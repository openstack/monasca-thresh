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

import com.hpcloud.mon.common.event.AlarmDefinitionCreatedEvent;
import com.hpcloud.mon.common.model.alarm.AlarmExpression;
import com.hpcloud.mon.common.model.alarm.AlarmSubExpression;
import com.hpcloud.mon.common.model.metric.Metric;
import com.hpcloud.streaming.storm.Logging;
import com.hpcloud.streaming.storm.Streams;
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
import monasca.thresh.domain.model.MetricDefinitionAndTenantIdMatcher;
import monasca.thresh.domain.model.SubAlarm;
import monasca.thresh.domain.model.TenantIdAndMetricName;
import monasca.thresh.domain.service.AlarmDAO;
import monasca.thresh.domain.service.AlarmDefinitionDAO;
import monasca.thresh.infrastructure.persistence.PersistenceModule;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Filters metrics for which there is no associated alarm and forwards metrics for which there is an
 * alarm. Receives metric alarm and metric sub-alarm events to update metric definitions.
 *
 * METRIC_DEFS table and the matcher are shared between any bolts in the same worker process so that
 * all of the MetricDefinitionAndTenantIds for existing SubAlarms only have to be read once and
 * because it is not possible to predict which bolt gets which Metrics so all Bolts know about all
 * starting MetricDefinitionAndTenantIds.
 *
 * The current topology uses shuffleGrouping for the incoming Metrics and allGrouping for the
 * events. So, any Bolt may get any Metric so the METRIC_DEFS table and the matcher must be kept up
 * to date for all MetricDefinitionAndTenantIds.
 *
 * The METRIC_DEFS table contains a List of SubAlarms IDs that reference the same
 * MetricDefinitionAndTenantId so if a SubAlarm is deleted, the MetricDefinitionAndTenantId will
 * only be deleted from it and the matcher if no more SubAlarms reference it. Incrementing and
 * decrementing the count is done under the static lock SENTINAL to ensure it is correct across all
 * Bolts sharing the same METRIC_DEFS table and the matcher. The amount of adds and deletes will be
 * very small compared to the number of Metrics so it shouldn't block the Metric handling.
 *
 * <ul>
 * <li>Input: MetricDefinition metricDefinition, Metric metric
 * <li>Input metric-alarm-events: String eventType, MetricDefinitionAndTenantId
 * metricDefinitionAndTenantId, String alarmId
 * <li>Input metric-sub-alarm-events: String eventType, MetricDefinitionAndTenantId
 * metricDefinitionAndTenantId, SubAlarm subAlarm
 * <li>Output: MetricDefinitionAndTenantId metricDefinitionAndTenantId, Metric metric
 * </ul>
 */
public class MetricFilteringBolt extends BaseRichBolt {
  private static final long serialVersionUID = 1096706128973976599L;

  public static final String NEW_METRIC_FOR_ALARM_DEFINITION_STREAM = "newMetricForAlarmDefinitionStream";
  public static final String[] NEW_METRIC_FOR_ALARM_DEFINITION_FIELDS =
      new String[] {"metricDefinitionAndTenantId", "alarmDefinitionId"};
  public static final String MIN_LAG_VALUE_KEY = "monasca.thresh.filtering.minLagValue";
  public static final int MIN_LAG_VALUE_DEFAULT = 10;
  public static final String MAX_LAG_MESSAGES_KEY = "monasca.thresh.filtering.maxLagMessages";
  public static final int MAX_LAG_MESSAGES_DEFAULT = 10;
  public static final String LAG_MESSAGE_PERIOD_KEY = "monasca.thresh.filtering.lagMessagePeriod";
  public static final int LAG_MESSAGE_PERIOD_DEFAULT = 30;
  public static final String[] FIELDS = new String[] {"tenantIdAndMetricName", "metric"};

  private static final int MIN_LAG_VALUE = PropertyFinder.getIntProperty(MIN_LAG_VALUE_KEY,
      MIN_LAG_VALUE_DEFAULT, 0, Integer.MAX_VALUE);
  private static final int MAX_LAG_MESSAGES = PropertyFinder.getIntProperty(MAX_LAG_MESSAGES_KEY,
      MAX_LAG_MESSAGES_DEFAULT, 0, Integer.MAX_VALUE);
  private static final int LAG_MESSAGE_PERIOD = PropertyFinder.getIntProperty(
      LAG_MESSAGE_PERIOD_KEY, LAG_MESSAGE_PERIOD_DEFAULT, 1, 600);
  private static final Map<MetricDefinitionAndTenantId, Set<String>> METRIC_DEFS =
      new ConcurrentHashMap<>();
  private static final MetricDefinitionAndTenantIdMatcher matcher =
      new MetricDefinitionAndTenantIdMatcher();
  private static final Object SENTINAL = new Object();
  private static final Map<String, AlarmDefinition> alarmDefinitions = new ConcurrentHashMap<>();

  private transient Logger logger;
  private DataSourceFactory dbConfig;
  private transient AlarmDAO alarmDAO;
  private transient AlarmDefinitionDAO alarmDefDAO;
  private OutputCollector collector;
  private long minLag = Long.MAX_VALUE;
  private long lastMinLagMessageSent = 0;
  private long minLagMessageSent = 0;
  private boolean lagging = true;

  public MetricFilteringBolt(DataSourceFactory dbConfig) {
    this.dbConfig = dbConfig;
  }

  public MetricFilteringBolt(AlarmDefinitionDAO alarmDefDAO, AlarmDAO alarmDAO) {
    this.alarmDefDAO = alarmDefDAO;
    this.alarmDAO = alarmDAO;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields(FIELDS));
    declarer.declareStream(NEW_METRIC_FOR_ALARM_DEFINITION_STREAM, new Fields(
        NEW_METRIC_FOR_ALARM_DEFINITION_FIELDS));
    declarer.declareStream(MetricAggregationBolt.METRIC_AGGREGATION_CONTROL_STREAM, new Fields(
        MetricAggregationBolt.METRIC_AGGREGATION_CONTROL_FIELDS));
    declarer.declareStream(AlarmCreationBolt.ALARM_CREATION_STREAM, new Fields(
        AlarmCreationBolt.ALARM_CREATION_FIELDS));
  }

  @Override
  public void execute(Tuple tuple) {
    logger.debug("tuple: {}", tuple);
    try {
      if (Streams.DEFAULT_STREAM_ID.equals(tuple.getSourceStreamId())) {
        final TenantIdAndMetricName timn = (TenantIdAndMetricName)tuple.getValue(0);
        final Long timestamp = (Long) tuple.getValue(1);
        final Metric metric = (Metric) tuple.getValue(2);
        final MetricDefinitionAndTenantId metricDefinitionAndTenantId =
            new MetricDefinitionAndTenantId(metric.definition(), timn.getTenantId());
        checkLag(timestamp);

        logger.debug("metric definition and tenant id: {}", metricDefinitionAndTenantId);
        if (checkForMatch(metricDefinitionAndTenantId)) {
          collector.emit(new Values(timn, metric));
        }
      } else {
        String eventType = tuple.getString(0);

        logger.debug("Received {} on {}", eventType, tuple.getSourceStreamId());
        // UPDATED events can be ignored because the MetricDefinitionAndTenantId doesn't change
        if (EventProcessingBolt.METRIC_ALARM_EVENT_STREAM_ID.equals(tuple.getSourceStreamId())) {
          if (EventProcessingBolt.DELETED.equals(eventType)) {
            MetricDefinitionAndTenantId metricDefinitionAndTenantId =
                (MetricDefinitionAndTenantId) tuple.getValue(1);
            removeAlarm(metricDefinitionAndTenantId, tuple.getString(2));
          }
        } else if (EventProcessingBolt.ALARM_DEFINITION_EVENT_STREAM_ID.equals(tuple
            .getSourceStreamId())) {
          if (EventProcessingBolt.CREATED.equals(eventType)) {
            synchronized (SENTINAL) {
              final AlarmDefinitionCreatedEvent event =
                  (AlarmDefinitionCreatedEvent) tuple.getValue(1);
              final AlarmDefinition alarmDefinition =
                  new AlarmDefinition(event.alarmDefinitionId, event.tenantId, event.alarmName,
                      event.alarmDescription, new AlarmExpression(event.alarmExpression), true,
                      event.matchBy);
              newAlarmDefinition(alarmDefinition);
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

  private void newAlarmDefinition(final AlarmDefinition alarmDefinition) {
    alarmDefinitions.put(alarmDefinition.getId(), alarmDefinition);
    for (final AlarmSubExpression subExpr : alarmDefinition.getAlarmExpression()
        .getSubExpressions()) {
      final MetricDefinitionAndTenantId mtid =
          new MetricDefinitionAndTenantId(subExpr.getMetricDefinition(),
              alarmDefinition.getTenantId());
      matcher.add(mtid, alarmDefinition.getId());
    }
  }

  private boolean checkForMatch(MetricDefinitionAndTenantId metricDefinitionAndTenantId) {
    final Set<String> alarmDefinitionIds = matcher.match(metricDefinitionAndTenantId);
    if (alarmDefinitionIds.isEmpty()) {
      return false;
    }
    final Set<String> existing = METRIC_DEFS.get(metricDefinitionAndTenantId);
    if (existing != null) {
      alarmDefinitionIds.removeAll(existing);
    }
    if (!alarmDefinitionIds.isEmpty()) {
      for (final String alarmDefinitionId : alarmDefinitionIds) {
        final AlarmDefinition alarmDefinition = alarmDefinitions.get(alarmDefinitionId);
        logger.info("Should add metric for id = {} name = {}", alarmDefinitionId,
            alarmDefinition.getName());
        collector.emit(NEW_METRIC_FOR_ALARM_DEFINITION_STREAM,
            new Values(metricDefinitionAndTenantId, alarmDefinitionId));
        addMetricDef(metricDefinitionAndTenantId, alarmDefinitionId);
      }
    }
    return true;
  }

  private void checkLag(Long apiTimeStamp) {
    if (!lagging) {
      return;
    }
    if ((apiTimeStamp == null) || (apiTimeStamp.longValue() == 0)) {
      return; // Remove this code at some point, just to handle old metrics without a NPE
    }
    final long now = getCurrentTime();
    final long lag = now - apiTimeStamp.longValue();
    if (lag < minLag) {
      minLag = lag;
    }
    if (minLag <= MIN_LAG_VALUE) {
      lagging = false;
      logger.info("Metrics no longer lagging, minLag = {}", minLag);
    } else if (minLagMessageSent >= MAX_LAG_MESSAGES) {
      logger.info("Waited for {} seconds for Metrics to catch up. Giving up. minLag = {}",
          MAX_LAG_MESSAGES * LAG_MESSAGE_PERIOD, minLag);
      lagging = false;
    } else if (lastMinLagMessageSent == 0) {
      lastMinLagMessageSent = now;
    } else if ((now - lastMinLagMessageSent) >= LAG_MESSAGE_PERIOD) {
      logger.info("Sending {} message, minLag = {}", MetricAggregationBolt.METRICS_BEHIND, minLag);
      collector.emit(MetricAggregationBolt.METRIC_AGGREGATION_CONTROL_STREAM, new Values(
          MetricAggregationBolt.METRICS_BEHIND));
      lastMinLagMessageSent = now;
      minLagMessageSent++;
    }
  }

  private void removeAlarm(MetricDefinitionAndTenantId metricDefinitionAndTenantId,
      String alarmDefinitionId) {
    synchronized (SENTINAL) {
      matcher.remove(metricDefinitionAndTenantId, alarmDefinitionId);
      final Set<String> alarms = METRIC_DEFS.get(metricDefinitionAndTenantId);
      if (alarms != null) {
        if (alarms.remove(alarmDefinitionId) && alarms.isEmpty()) {
          METRIC_DEFS.remove(metricDefinitionAndTenantId);
        }
      }
    }
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

    // DCL
    if (METRIC_DEFS.isEmpty()) {
      synchronized (SENTINAL) {
        if (METRIC_DEFS.isEmpty()) {
          for (AlarmDefinition alarmcDef : alarmDefDAO.listAll()) {
            newAlarmDefinition(alarmcDef);
          }

          // Load the existing Alarms
          for (Alarm alarm : alarmDAO.listAll()) {
            final AlarmDefinition alarmDefinition =
                alarmDefinitions.get(alarm.getAlarmDefinitionId());
            if (alarmDefinition == null) {
              logger.error("AlarmDefinition {} does not exist for Alarm {}, ignoring",
                  alarm.getAlarmDefinitionId(), alarm.getId());
              continue;
            }
            for (final MetricDefinitionAndTenantId mtid : alarm.getAlarmedMetrics()) {
              addMetricDef(mtid, alarm.getAlarmDefinitionId());
              for (final SubAlarm subAlarm : alarm.getSubAlarms()) {
                if (AlarmCreationBolt.metricFitsInSubAlarm(subAlarm, mtid.metricDefinition)) {
                  final TenantIdAndMetricName timn = new TenantIdAndMetricName(mtid);
                  final Values values =
                      new Values(EventProcessingBolt.CREATED, timn, mtid,
                          alarm.getAlarmDefinitionId(), subAlarm);
                  logger.debug("Emitting new SubAlarm {}", values);
                  collector.emit(AlarmCreationBolt.ALARM_CREATION_STREAM, values);
                }
              }
            }
          }

          logger.info("Found {} Metric Definitions", METRIC_DEFS.size());
          // Just output these here so they are only output once per JVM
          logger.info("MIN_LAG_VALUE set to {} seconds", MIN_LAG_VALUE);
          logger.info("MAX_LAG_MESSAGES set to {}", MAX_LAG_MESSAGES);
          logger.info("LAG_MESSAGE_PERIOD set to {} seconds", LAG_MESSAGE_PERIOD);
        }
      }
    }
    lastMinLagMessageSent = 0;
  }

  /**
   * Allow override of current time for testing.
   */
  protected long getCurrentTime() {
    return System.currentTimeMillis() / 1000;
  }

  private void addMetricDef(MetricDefinitionAndTenantId metricDefinitionAndTenantId,
      String alarmDefinitionId) {
    Set<String> subAlarmIds = METRIC_DEFS.get(metricDefinitionAndTenantId);
    if (subAlarmIds == null) {
      subAlarmIds = new HashSet<>();
      METRIC_DEFS.put(metricDefinitionAndTenantId, subAlarmIds);
    } else if (subAlarmIds.contains(alarmDefinitionId)) {
      return; // Make sure it is only added once. Multiple bolts process the same AlarmCreatedEvent
    }
    subAlarmIds.add(alarmDefinitionId);
    matcher.add(metricDefinitionAndTenantId, alarmDefinitionId);
  }

  /**
   * Only use for testing.
   */
  static void clearMetricDefinitions() {
    METRIC_DEFS.clear();
    matcher.clear();
  }

  /**
   * Only use for testing.
   */
  static int sizeMetricDefinitions() {
    return METRIC_DEFS.size();
  }
}
