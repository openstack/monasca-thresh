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

import com.hpcloud.configuration.KafkaProducerConfiguration;
import com.hpcloud.mon.common.event.AlarmDefinitionUpdatedEvent;
import com.hpcloud.mon.common.event.AlarmStateTransitionedEvent;
import com.hpcloud.mon.common.event.AlarmUpdatedEvent;
import com.hpcloud.mon.common.model.alarm.AlarmState;
import com.hpcloud.mon.common.model.metric.MetricDefinition;
import com.hpcloud.streaming.storm.Logging;
import com.hpcloud.streaming.storm.Streams;
import com.hpcloud.util.Injector;
import com.hpcloud.util.Serialization;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import monasca.thresh.ThresholdingConfiguration;
import monasca.thresh.domain.model.Alarm;
import monasca.thresh.domain.model.AlarmDefinition;
import monasca.thresh.domain.model.MetricDefinitionAndTenantId;
import monasca.thresh.domain.model.SubAlarm;
import monasca.thresh.domain.service.AlarmDAO;
import monasca.thresh.domain.service.AlarmDefinitionDAO;
import monasca.thresh.infrastructure.persistence.PersistenceModule;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Determines whether an alarm threshold has been exceeded.
 * <p/>
 * <p/>
 * Receives alarm state changes and events.
 * <p/>
 * <ul>
 * <li>Input: String alarmId, SubAlarm subAlarm
 * <li>Input alarm-events: String eventType, String alarmId
 * </ul>
 */
public class AlarmThresholdingBolt extends BaseRichBolt {
  private static final long serialVersionUID = -4126465124017857754L;

  private transient Logger logger;
  private DataSourceFactory dbConfig;
  private KafkaProducerConfiguration producerConfiguration;
  final Map<String, Alarm> alarms = new HashMap<String, Alarm>();
  final Map<String, AlarmDefinition> alarmDefinitions = new HashMap<>();
  private String alertExchange;
  private String alertRoutingKey;
  private transient AlarmDAO alarmDAO;
  private transient AlarmDefinitionDAO alarmDefinitionDAO;
  private transient AlarmEventForwarder alarmEventForwarder;
  private OutputCollector collector;

  public AlarmThresholdingBolt(DataSourceFactory dbConfig, KafkaProducerConfiguration producerConfig) {
    this.dbConfig = dbConfig;
    this.producerConfiguration = producerConfig;
  }

  public AlarmThresholdingBolt(final AlarmDAO alarmDAO, final AlarmDefinitionDAO alarmDefinitionDAO,
      final AlarmEventForwarder alarmEventForwarder) {
    this.alarmDAO = alarmDAO;
    this.alarmDefinitionDAO = alarmDefinitionDAO;
    this.alarmEventForwarder = alarmEventForwarder;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {}

  @Override
  public void execute(Tuple tuple) {

    logger.debug("tuple: {}", tuple);
    try {
      if (Streams.DEFAULT_STREAM_ID.equals(tuple.getSourceStreamId())) {
        String alarmId = tuple.getString(0);
        Alarm alarm = getOrCreateAlarm(alarmId);
        if (alarm == null) {
          return;
        }

        SubAlarm subAlarm = (SubAlarm) tuple.getValue(1);
        evaluateThreshold(alarm, subAlarm);
      } else if (EventProcessingBolt.ALARM_EVENT_STREAM_ID.equals(tuple.getSourceStreamId())) {
        String eventType = tuple.getString(0);
        String alarmId = tuple.getString(1);

        if (EventProcessingBolt.DELETED.equals(eventType)) {
          handleAlarmDeleted(alarmId);
        } else if (EventProcessingBolt.UPDATED.equals(eventType)) {
          handleAlarmUpdated(alarmId, (AlarmUpdatedEvent) tuple.getValue(2));
        }
      }
      else if (EventProcessingBolt.ALARM_DEFINITION_EVENT_STREAM_ID.equals(tuple.getSourceStreamId())) {
        String eventType = tuple.getString(0);
        if (EventProcessingBolt.UPDATED.equals(eventType)) {
          handle((AlarmDefinitionUpdatedEvent) tuple.getValue(1));
        }
      }
    } catch (Exception e) {
      logger.error("Error processing tuple {}", tuple, e);
    } finally {
      collector.ack(tuple);
    }
  }

  private void handle(AlarmDefinitionUpdatedEvent event) {
    final AlarmDefinition alarmDefinition = alarmDefinitions.get(event.alarmDefinitionId);
    if (alarmDefinition == null) {
      // This is OK. No Alarms are using this AlarmDefinition
      logger.info("Update of AlarmDefinition {} skipped. Not in use by this bolt",
          event.alarmDefinitionId);
      return;
    }
    logger.info("Updating AlarmDefinition {}", event.alarmDefinitionId);
    alarmDefinition.setName(event.alarmName);
    alarmDefinition.setDescription(event.alarmDescription);
    alarmDefinition.setSeverity(event.severity);
    alarmDefinition.setActionsEnabled(event.alarmActionsEnabled);
  }

  @Override
  @SuppressWarnings("rawtypes")
  public void prepare(Map config, TopologyContext context, OutputCollector collector) {
    logger = LoggerFactory.getLogger(Logging.categoryFor(getClass(), context));
    logger.info("Preparing");
    this.collector = collector;
    alertExchange = (String) config.get(ThresholdingConfiguration.ALERTS_EXCHANGE);
    alertRoutingKey = (String) config.get(ThresholdingConfiguration.ALERTS_ROUTING_KEY);

    if (alarmDAO == null) {
      Injector.registerIfNotBound(AlarmDAO.class, new PersistenceModule(dbConfig));
      alarmDAO = Injector.getInstance(AlarmDAO.class);
    }

    if (alarmDefinitionDAO == null) {
      Injector.registerIfNotBound(AlarmDefinitionDAO.class, new PersistenceModule(dbConfig));
      alarmDefinitionDAO = Injector.getInstance(AlarmDefinitionDAO.class);
    }
    if (alarmEventForwarder == null) {
      Injector.registerIfNotBound(AlarmEventForwarder.class, new ProducerModule(
          this.producerConfiguration));
      alarmEventForwarder = Injector.getInstance(AlarmEventForwarder.class);
    }
  }

  void evaluateThreshold(Alarm alarm, SubAlarm subAlarm) {
    logger.debug("Received state change for {}", subAlarm);
    subAlarm.setNoState(false);
    alarm.updateSubAlarm(subAlarm);

    AlarmState initialState = alarm.getState();
    // Wait for all sub alarms to have a state before evaluating to prevent flapping on startup
    if (allSubAlarmsHaveState(alarm)
        && alarm.evaluate(alarmDefinitions.get(alarm.getAlarmDefinitionId()).getAlarmExpression())) {
      changeAlarmState(alarm, initialState, alarm.getStateChangeReason());
    }
  }

  private boolean allSubAlarmsHaveState(final Alarm alarm) {
    for (SubAlarm subAlarm : alarm.getSubAlarms()) {
      if (subAlarm.isNoState()) {
        return false;
      }
    }
    return true;
  }

  private void changeAlarmState(Alarm alarm, AlarmState initialState, String stateChangeReason) {
    alarmDAO.updateState(alarm.getId(), alarm.getState());
    final AlarmDefinition alarmDefinition = alarmDefinitions.get(alarm.getAlarmDefinitionId());
    final List<MetricDefinition> alarmedMetrics = new ArrayList<>(alarm.getAlarmedMetrics().size());
    for (final MetricDefinitionAndTenantId mdtid : alarm.getAlarmedMetrics()) {
      alarmedMetrics.add(mdtid.metricDefinition);
    }
    logger.debug("Alarm {} transitioned from {} to {}", alarm, initialState, alarm.getState());
    AlarmStateTransitionedEvent event =
        new AlarmStateTransitionedEvent(alarmDefinition.getTenantId(), alarm.getId(),
            alarmDefinition.getId(), alarmedMetrics, alarmDefinition.getName(),
            alarmDefinition.getDescription(), initialState, alarm.getState(),
            alarmDefinition.getSeverity(), alarmDefinition.isActionsEnabled(), stateChangeReason,
            getTimestamp());
    try {
      alarmEventForwarder.send(alertExchange, alertRoutingKey, Serialization.toJson(event));
    } catch (Exception ignore) {
      logger.debug("Failure sending alarm", ignore);
    }
  }

  protected long getTimestamp() {
    return System.currentTimeMillis() / 1000;
  }

  void handleAlarmDeleted(String alarmId) {
    logger.debug("Received AlarmDeletedEvent for alarm id {}", alarmId);
    alarms.remove(alarmId);
  }

  void handleAlarmUpdated(String alarmId, AlarmUpdatedEvent alarmUpdatedEvent) {
    final Alarm oldAlarm = alarms.get(alarmId);
    if (oldAlarm == null) {
      logger.debug("Updated Alarm {} not loaded, ignoring");
      return;
    }

    oldAlarm.setState(alarmUpdatedEvent.alarmState);

  }

  void handleAlarmDefinitionUpdated(String alarmDefId, AlarmDefinitionUpdatedEvent event) {
    final AlarmDefinition oldAlarmDef = alarmDefinitions.get(alarmDefId);
    if (oldAlarmDef == null) {
      logger.debug("Updated Alarm Definition {} not loaded, ignoring", alarmDefId);
      return;
    }

    oldAlarmDef.setName(event.alarmName);
    oldAlarmDef.setDescription(event.alarmDescription);
    oldAlarmDef.setExpression(event.alarmExpression);
    oldAlarmDef.setActionsEnabled(event.alarmActionsEnabled);

    /* Have to figure out how to handle this
    // Now handle the SubAlarms
    // First remove the deleted SubAlarms so we don't have to consider them later
    for (Map.Entry<String, AlarmSubExpression> entry : event.oldAlarmSubExpressions
        .entrySet()) {
      logger.debug("Removing deleted SubAlarm {}", entry.getValue());
      if (!oldAlarmDef.removeSubAlarmById(entry.getKey())) {
        logger.error("Did not find removed SubAlarm {}", entry.getValue());
      }
    }

    // Reuse what we can from the changed SubAlarms
    for (Map.Entry<String, AlarmSubExpression> entry : event.changedSubExpressions
        .entrySet()) {
      final SubAlarm oldSubAlarm = oldAlarmDef.getSubAlarm(entry.getKey());
      if (oldSubAlarm == null) {
        logger.error("Did not find changed SubAlarm {}", entry.getValue());
        continue;
      }
      final SubAlarm newSubAlarm = new SubAlarm(entry.getKey(), oldAlarmDef.getId(), entry.getValue());
      newSubAlarm.setState(oldSubAlarm.getState());
      if (!oldSubAlarm.isCompatible(newSubAlarm)) {
        newSubAlarm.setNoState(true);
      }
      logger.debug("Changing SubAlarm from {} to {}", oldSubAlarm, newSubAlarm);
      oldAlarmDef.updateSubAlarm(newSubAlarm);
    }

    // Add the new SubAlarms
    for (Map.Entry<String, AlarmSubExpression> entry : event.newAlarmSubExpressions
        .entrySet()) {
      final SubAlarm newSubAlarm = new SubAlarm(entry.getKey(), oldAlarmDef.getId(), entry.getValue());
      newSubAlarm.setNoState(true);
      logger.debug("Adding SubAlarm {}", newSubAlarm);
      oldAlarmDef.updateSubAlarm(newSubAlarm);
    }
    */
  }

  String buildStateChangeReason() {
    return null;
  }

  private Alarm getOrCreateAlarm(String alarmId) {
    Alarm alarm = alarms.get(alarmId);
    if (alarm == null) {
      alarm = alarmDAO.findById(alarmId);
      if (alarm == null) {
        logger.error("Failed to locate alarm for id {}", alarmId);
        return null;
      } else {
        if (alarmDefinitions.get(alarm.getAlarmDefinitionId()) == null) {
          final AlarmDefinition alarmDefinition = alarmDefinitionDAO.findById(alarm.getAlarmDefinitionId());
          if (alarmDefinition == null) {
            logger.error("Failed to locate alarm definition for id {}", alarm.getAlarmDefinitionId());
            return null;
          }
          alarmDefinitions.put(alarmDefinition.getId(), alarmDefinition);
        }
        for (final SubAlarm subAlarm : alarm.getSubAlarms()) {
          subAlarm.setNoState(true);
        }
        alarms.put(alarmId, alarm);
      }
    }

    return alarm;
  }
}
