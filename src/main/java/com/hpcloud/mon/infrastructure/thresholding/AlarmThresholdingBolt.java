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

package com.hpcloud.mon.infrastructure.thresholding;

import com.hpcloud.configuration.KafkaProducerConfiguration;
import com.hpcloud.mon.ThresholdingConfiguration;
import com.hpcloud.mon.common.event.AlarmStateTransitionedEvent;
import com.hpcloud.mon.common.event.AlarmUpdatedEvent;
import com.hpcloud.mon.common.model.alarm.AlarmState;
import com.hpcloud.mon.common.model.alarm.AlarmSubExpression;
import com.hpcloud.mon.domain.model.Alarm;
import com.hpcloud.mon.domain.model.SubAlarm;
import com.hpcloud.mon.domain.service.AlarmDAO;
import com.hpcloud.mon.infrastructure.persistence.PersistenceModule;
import com.hpcloud.streaming.storm.Logging;
import com.hpcloud.streaming.storm.Streams;
import com.hpcloud.util.Injector;
import com.hpcloud.util.Serialization;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
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
  private String alertExchange;
  private String alertRoutingKey;
  private transient AlarmDAO alarmDAO;
  private transient AlarmEventForwarder alarmEventForwarder;
  private OutputCollector collector;

  public AlarmThresholdingBolt(DataSourceFactory dbConfig, KafkaProducerConfiguration producerConfig) {
    this.dbConfig = dbConfig;
    this.producerConfiguration = producerConfig;
  }

  public AlarmThresholdingBolt(final AlarmDAO alarmDAO,
      final AlarmEventForwarder alarmEventForwarder) {
    this.alarmDAO = alarmDAO;
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
    } catch (Exception e) {
      logger.error("Error processing tuple {}", tuple, e);
    } finally {
      collector.ack(tuple);
    }
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
    if (allSubAlarmsHaveState(alarm) && alarm.evaluate()) {
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

    logger.debug("Alarm {} transitioned from {} to {}", alarm, initialState, alarm.getState());
    AlarmStateTransitionedEvent event =
        new AlarmStateTransitionedEvent(alarm.getTenantId(), alarm.getId(), alarm.getName(),
            alarm.getDescription(), initialState, alarm.getState(), alarm.isActionsEnabled(),
            stateChangeReason, getTimestamp());
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

    oldAlarm.setName(alarmUpdatedEvent.alarmName);
    oldAlarm.setDescription(alarmUpdatedEvent.alarmDescription);
    oldAlarm.setExpression(alarmUpdatedEvent.alarmExpression);
    oldAlarm.setState(alarmUpdatedEvent.alarmState);
    oldAlarm.setActionsEnabled(alarmUpdatedEvent.alarmActionsEnabled);

    // Now handle the SubAlarms
    // First remove the deleted SubAlarms so we don't have to consider them later
    for (Map.Entry<String, AlarmSubExpression> entry : alarmUpdatedEvent.oldAlarmSubExpressions
        .entrySet()) {
      logger.debug("Removing deleted SubAlarm {}", entry.getValue());
      if (!oldAlarm.removeSubAlarmById(entry.getKey())) {
        logger.error("Did not find removed SubAlarm {}", entry.getValue());
      }
    }

    // Reuse what we can from the changed SubAlarms
    for (Map.Entry<String, AlarmSubExpression> entry : alarmUpdatedEvent.changedSubExpressions
        .entrySet()) {
      final SubAlarm oldSubAlarm = oldAlarm.getSubAlarm(entry.getKey());
      if (oldSubAlarm == null) {
        logger.error("Did not find changed SubAlarm {}", entry.getValue());
        continue;
      }
      final SubAlarm newSubAlarm = new SubAlarm(entry.getKey(), oldAlarm.getId(), entry.getValue());
      newSubAlarm.setState(oldSubAlarm.getState());
      if (!oldSubAlarm.isCompatible(newSubAlarm)) {
        newSubAlarm.setNoState(true);
      }
      logger.debug("Changing SubAlarm from {} to {}", oldSubAlarm, newSubAlarm);
      oldAlarm.updateSubAlarm(newSubAlarm);
    }

    // Add the new SubAlarms
    for (Map.Entry<String, AlarmSubExpression> entry : alarmUpdatedEvent.newAlarmSubExpressions
        .entrySet()) {
      final SubAlarm newSubAlarm = new SubAlarm(entry.getKey(), oldAlarm.getId(), entry.getValue());
      newSubAlarm.setNoState(true);
      logger.debug("Adding SubAlarm {}", newSubAlarm);
      oldAlarm.updateSubAlarm(newSubAlarm);
    }

    alarms.put(alarmId, oldAlarm);
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
      } else {
        for (final SubAlarm subAlarm : alarm.getSubAlarms()) {
          subAlarm.setNoState(true);
        }
        alarms.put(alarmId, alarm);
      }
    }

    return alarm;
  }
}
