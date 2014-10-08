/*
 * Copyright (c) 2014 Hewlett-Packard Development Company, L.P.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package monasca.thresh.infrastructure.thresholding;

import com.hpcloud.mon.common.event.AlarmDefinitionDeletedEvent;
import com.hpcloud.mon.common.model.alarm.AlarmState;
import com.hpcloud.mon.common.model.alarm.AlarmSubExpression;
import com.hpcloud.mon.common.model.metric.MetricDefinition;
import com.hpcloud.streaming.storm.Logging;
import com.hpcloud.util.Injector;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import monasca.thresh.domain.model.Alarm;
import monasca.thresh.domain.model.AlarmDefinition;
import monasca.thresh.domain.model.MetricDefinitionAndTenantId;
import monasca.thresh.domain.model.SubAlarm;
import monasca.thresh.domain.model.TenantIdAndMetricName;
import monasca.thresh.domain.service.AlarmDAO;
import monasca.thresh.domain.service.AlarmDefinitionDAO;
import monasca.thresh.infrastructure.persistence.PersistenceModule;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Handles creation of Alarms and Alarmed Metrics.
 * 
 * MUST be only one of these bolts in the storm application
 */
public class AlarmCreationBolt extends BaseRichBolt {
  private static final long serialVersionUID = 1096706128973976599L;

  public static final String ALARM_CREATION_STREAM = "alarm-creation-stream";
  public static final String[] ALARM_CREATION_FIELDS = new String[] {"control",
      "tenantIdAndMetricName", "metricDefinitionAndTenantId", "alarmDefinitionId", "subAlarm"};

  private transient Logger logger;
  private DataSourceFactory dbConfig;
  private transient AlarmDefinitionDAO alarmDefDAO;
  private transient AlarmDAO alarmDAO;
  private OutputCollector collector;
  private final Map<String, List<Alarm>> waitingAlarms = new HashMap<>();
  private static final List<Alarm> EMPTY_LIST = Collections.<Alarm>emptyList();

  public AlarmCreationBolt(DataSourceFactory dbConfig) {
    this.dbConfig = dbConfig;
  }

  public AlarmCreationBolt(AlarmDefinitionDAO alarmDefDAO,
                           AlarmDAO alarmDAO) {
    this.alarmDefDAO = alarmDefDAO;
    this.alarmDAO = alarmDAO;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declareStream(ALARM_CREATION_STREAM, new Fields(ALARM_CREATION_FIELDS));
  }

  @Override
  public void execute(Tuple tuple) {
    logger.debug("tuple: {}", tuple);
    try {
      if (MetricFilteringBolt.NEW_METRIC_FOR_ALARM_DEFINITION_STREAM.equals(tuple.getSourceStreamId())) {
        final MetricDefinitionAndTenantId metricDefinitionAndTenantId =
            (MetricDefinitionAndTenantId) tuple.getValue(0);
        handleNewMetricDefinition(metricDefinitionAndTenantId, tuple.getString(1));
      } else {
        final String eventType = tuple.getString(0);
        logger.debug("Received {} Event", eventType);
        if (EventProcessingBolt.ALARM_DEFINITION_EVENT_STREAM_ID.equals(tuple.getSourceStreamId())) {
          if (EventProcessingBolt.DELETED.equals(eventType)) {
            final AlarmDefinitionDeletedEvent event =
                (AlarmDefinitionDeletedEvent) tuple.getValue(1);
            final List<Alarm> waiting = waitingAlarms.remove(event.alarmDefinitionId);
            if (waiting != null && !waiting.isEmpty()) {
              logger.info("{} waiting alarms removed for Alarm Definition Id {}", waiting != null
                  && !waiting.isEmpty() ? waiting.size() : "No", event.alarmDefinitionId);
            }
          }
        }
      }
    } catch (Exception e) {
      logger.error("Error processing tuple {}", tuple, e);
    } finally {
      collector.ack(tuple);
    }
  }

  protected void handleNewMetricDefinition(
      final MetricDefinitionAndTenantId metricDefinitionAndTenantId, final String alarmDefinitionId) {
    final AlarmDefinition alarmDefinition = lookUpAlarmDefinition(alarmDefinitionId);
    if (alarmDefinition == null) {
      return;
    }

    if (!validMetricDefinition(alarmDefinition, metricDefinitionAndTenantId)) {
      return;
    }

    final List<Alarm> existingAlarms = alarmDAO.findForAlarmDefinitionId(alarmDefinitionId);
    if (alreadyCreated(existingAlarms, metricDefinitionAndTenantId)) {
      logger.warn("MetricDefinition {} is already in existing Alarm", metricDefinitionAndTenantId);
      return;
    }

    if (alreadyCreated(getWaitingAlarmsForAlarmDefinition(alarmDefinition),
        metricDefinitionAndTenantId)) {
      logger.warn("MetricDefinition {} is already in waiting Alarm", metricDefinitionAndTenantId);
      return;
    }

    final Alarm existingAlarm =
        fitsInExistingAlarm(metricDefinitionAndTenantId, alarmDefinition, existingAlarms);

    if (existingAlarm != null) {
      logger.info("Metric {} fits into existing alarm {}", metricDefinitionAndTenantId,
          existingAlarm);
      addToExistingAlarm(existingAlarm, metricDefinitionAndTenantId);
      sendNewMetricDefinition(existingAlarm, metricDefinitionAndTenantId);
    } else {
      final List<Alarm> newAlarms =
          finishesAlarm(alarmDefinition, metricDefinitionAndTenantId, existingAlarms);
      for (final Alarm newAlarm : newAlarms) {
        logger.info("Metric {} finishes waiting alarm {}", metricDefinitionAndTenantId, newAlarm);
        for (final MetricDefinitionAndTenantId md : newAlarm.getAlarmedMetrics()) {
          sendNewMetricDefinition(newAlarm, md);
        }
      }
    }
  }

  private Alarm fitsInExistingAlarm(final MetricDefinitionAndTenantId metricDefinitionAndTenantId,
      final AlarmDefinition alarmDefinition, final List<Alarm> existingAlarms) {
    Alarm existingAlarm = null;
    if (alarmDefinition.getMatchBy().isEmpty()) {
      if (!existingAlarms.isEmpty()) {
        existingAlarm = existingAlarms.get(0);
      }
    }
    else {
      for (final Alarm alarm : existingAlarms) {
        if (metricFitsInAlarm(alarm, alarmDefinition, metricDefinitionAndTenantId)) {
          existingAlarm = alarm;
          break;
        }
      }
    }
    return existingAlarm;
  }

  private void addToExistingAlarm(Alarm existingAlarm,
      MetricDefinitionAndTenantId metricDefinitionAndTenantId) {
    existingAlarm.addAlarmedMetric(metricDefinitionAndTenantId);
    alarmDAO.addAlarmedMetric(existingAlarm.getId(), metricDefinitionAndTenantId);
  }

  private void sendNewMetricDefinition(Alarm existingAlarm,
      MetricDefinitionAndTenantId metricDefinitionAndTenantId) {
    for (final SubAlarm subAlarm : existingAlarm.getSubAlarms()) {
      if (metricFitsInAlarmSubExpr(subAlarm.getExpression(),
          metricDefinitionAndTenantId.metricDefinition)) {
        final TenantIdAndMetricName timn = new TenantIdAndMetricName(metricDefinitionAndTenantId);
        final Values values =
            new Values(EventProcessingBolt.CREATED, timn, metricDefinitionAndTenantId,
                existingAlarm.getAlarmDefinitionId(), subAlarm);
        logger.debug("Emitting new SubAlarm {}", values);
        collector.emit(ALARM_CREATION_STREAM, values);
      }
    }
  }

  public static boolean metricFitsInAlarmSubExpr(AlarmSubExpression subExpr,
      MetricDefinition check) {
    final MetricDefinition md = subExpr.getMetricDefinition();
    if (!md.name.equals(check.name)) {
      return false;
    }
    if ((md.dimensions != null) && !md.dimensions.isEmpty()) {
      for (final Map.Entry<String, String> entry : md.dimensions.entrySet()) {
        if (!entry.getValue().equals(check.dimensions.get(entry.getKey()))) {
          return false;
        }
      }
    }
    return true;
  }

  protected boolean validMetricDefinition(AlarmDefinition alarmDefinition,
      MetricDefinitionAndTenantId check) {
    if (!alarmDefinition.getTenantId().equals(check.tenantId)) {
      return false;
    }
    for (final AlarmSubExpression subExpr : alarmDefinition.getAlarmExpression()
        .getSubExpressions()) {
      if (metricFitsInAlarmSubExpr(subExpr, check.metricDefinition)) {
        return true;
      }
    }
    return false;
  }

  protected String getNextId() {
    return UUID.randomUUID().toString(); 
  }

  /**
   * This is only used for testing
   *
   * @param alarmDefinitionId
   * @return
   */
  protected Integer countWaitingAlarms(final String alarmDefinitionId) {
    final List<Alarm> waiting = waitingAlarms.get(alarmDefinitionId);
    return waiting == null ? null: Integer.valueOf(waiting.size());
  }

  private List<Alarm> finishesAlarm(AlarmDefinition alarmDefinition,
      MetricDefinitionAndTenantId metricDefinitionAndTenantId, List<Alarm> existingAlarms) {
    final List<Alarm> waitingAlarms =
        findMatchingWaitingAlarms(getWaitingAlarmsForAlarmDefinition(alarmDefinition),
            alarmDefinition, metricDefinitionAndTenantId);
    final List<Alarm> result = new LinkedList<>();
    if (waitingAlarms.isEmpty()) {
      final String alarmId = getNextId();
      final Alarm newAlarm = new Alarm(alarmId, alarmDefinition, AlarmState.UNDETERMINED);
      newAlarm.addAlarmedMetric(metricDefinitionAndTenantId);
      if (alarmIsComplete(newAlarm)) {
        logger.debug("New alarm is complete. Saving");
        saveAlarm(newAlarm);
        result.add(newAlarm);
      } else {
        if (reuseExistingMetric(newAlarm, alarmDefinition, existingAlarms)) {
          logger.debug("New alarm is complete reusing existing metric. Saving");
          saveAlarm(newAlarm);
          result.add(newAlarm);        }
        else {
          logger.debug("Adding new alarm to the waiting list");
          addToWaitingAlarms(newAlarm, alarmDefinition);
        }
      }
    } else {
      for (final Alarm waiting : waitingAlarms) {
        waiting.addAlarmedMetric(metricDefinitionAndTenantId);
        if (alarmIsComplete(waiting)) {
          removeFromWaitingAlarms(waiting, alarmDefinition);
          saveAlarm(waiting);
          result.add(waiting);
        }
      }
    }
    return result;
  }

  private boolean reuseExistingMetric(Alarm newAlarm, final AlarmDefinition alarmDefinition,
      List<Alarm> existingAlarms) {
    boolean addedOne = false;
    for (final Alarm existingAlarm : existingAlarms) {
      for (final MetricDefinitionAndTenantId mtid : existingAlarm.getAlarmedMetrics()) {
        if (metricFitsInAlarm(newAlarm, alarmDefinition, mtid)) {
          newAlarm.addAlarmedMetric(mtid);
          addedOne = true;
        }
      }
    }
    if (!addedOne) {
      return false;
    }
    return alarmIsComplete(newAlarm);
  }

  private void saveAlarm(Alarm newAlarm) {
    alarmDAO.createAlarm(newAlarm);
  }

  private List<Alarm> findMatchingWaitingAlarms(List<Alarm> waiting, AlarmDefinition alarmDefinition,
        MetricDefinitionAndTenantId check) {
    final List<Alarm> result = new LinkedList<>();
    for (final Alarm alarm : waiting) {
      if (metricFitsInAlarm(alarm, alarmDefinition, check)) {
        result.add(alarm);
      }
    }
    return result;
  }

  protected boolean metricFitsInAlarm(final Alarm alarm, AlarmDefinition alarmDefinition,
      MetricDefinitionAndTenantId check) {
    final Map<String, String> matchesByValues = getMatchesByValues(alarmDefinition, alarm);
    boolean result = false;
    for (final SubAlarm subAlarm : alarm.getSubAlarms()) {
      if (metricFitsInAlarmSubExpr(subAlarm.getExpression(), check.metricDefinition)) {
        result = true;
        if (!matchesByValues.isEmpty()) {
          boolean foundOne = false;
          for (final Map.Entry<String, String> entry : matchesByValues.entrySet()) {
            final String value = check.metricDefinition.dimensions.get(entry.getKey());
            if (value != null) {
              if (!value.equals(entry.getValue())) {
                return false;
              }
              foundOne = true;
            }
          }
          if (!foundOne) {
            return false;
          }
        }
      }
    }
    return result;
  }

  private Map<String, String> getMatchesByValues(AlarmDefinition alarmDefinition, final Alarm alarm) {
    final Map<String, String> matchesByValues = new HashMap<>();
    if (!alarmDefinition.getMatchBy().isEmpty()) {
      for (final MetricDefinitionAndTenantId md : alarm.getAlarmedMetrics()) {
        for (final String matchBy : alarmDefinition.getMatchBy()) {
          final String value = md.metricDefinition.dimensions.get(matchBy);
          if (value != null) {
            matchesByValues.put(matchBy, value);
          }
        }
      }
    }
    return matchesByValues;
  }

  private void removeFromWaitingAlarms(Alarm toRemove, AlarmDefinition alarmDefinition) {
    final List<Alarm> waiting = waitingAlarms.get(alarmDefinition.getId());
    if ((waiting == null) || !waiting.remove(toRemove)) {
      logger.error("Did not find Alarm to remove");
    }
  }

  private void addToWaitingAlarms(Alarm newAlarm, AlarmDefinition alarmDefinition) {
    List<Alarm> waiting = waitingAlarms.get(alarmDefinition.getId());
    if (waiting == null) {
      waiting = new LinkedList<>();
      waitingAlarms.put(alarmDefinition.getId(), waiting);
    }
    waiting.add(newAlarm);
  }

  private List<Alarm> getWaitingAlarmsForAlarmDefinition(AlarmDefinition alarmDefinition) {
    final List<Alarm> waiting = waitingAlarms.get(alarmDefinition.getId());
    if (waiting == null) {
      return EMPTY_LIST;
    }
    return waiting;
  }

  private boolean alarmIsComplete(Alarm newAlarm) {
    for (final SubAlarm subAlarm : newAlarm.getSubAlarms()) {
      boolean found = false;
      for (final MetricDefinitionAndTenantId md : newAlarm.getAlarmedMetrics()) {
        if (metricFitsInAlarmSubExpr(subAlarm.getExpression(), md.metricDefinition)) {
          found = true;
          break;
        }
      }
      if (!found) {
        return false;
      }
    }
    return true;
  }

  private boolean alreadyCreated(List<Alarm> existingAlarms,
      MetricDefinitionAndTenantId metricDefinitionAndTenantId) {
    for (final Alarm alarm : existingAlarms) {
      for (final MetricDefinitionAndTenantId md : alarm.getAlarmedMetrics()) {
        if (md.equals(metricDefinitionAndTenantId)) {
          return true;
        }
      }
    }
    return false;
  }

  private AlarmDefinition lookUpAlarmDefinition(String alarmDefinitionId) {
    final AlarmDefinition found = alarmDefDAO.findById(alarmDefinitionId);
    if (found == null) {
      logger.warn("Did not find AlarmDefinition for ID {}", alarmDefinitionId);
      return null;
    }

    return found;
  }

  @Override
  @SuppressWarnings("rawtypes")
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    logger = LoggerFactory.getLogger(Logging.categoryFor(getClass(), context));
    logger.info("Preparing");
    this.collector = collector;

    if (alarmDefDAO == null) {
      Injector.registerIfNotBound(AlarmDefinitionDAO.class, new PersistenceModule(dbConfig));
      alarmDefDAO = Injector.getInstance(AlarmDefinitionDAO.class);
    }

    if (alarmDAO == null) {
      Injector.registerIfNotBound(AlarmDAO.class, new PersistenceModule(dbConfig));
      alarmDAO = Injector.getInstance(AlarmDAO.class);
    }
  }

  /**
   * Allow override of current time for testing.
   */
  protected long getCurrentTime() {
    return System.currentTimeMillis() / 1000;
  }
}
