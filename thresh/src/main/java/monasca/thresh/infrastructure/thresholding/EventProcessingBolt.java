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

import com.hpcloud.mon.common.event.AlarmCreatedEvent;
import com.hpcloud.mon.common.event.AlarmDeletedEvent;
import com.hpcloud.mon.common.event.AlarmUpdatedEvent;
import com.hpcloud.mon.common.model.alarm.AlarmSubExpression;
import com.hpcloud.mon.common.model.metric.MetricDefinition;
import monasca.thresh.domain.model.MetricDefinitionAndTenantId;
import monasca.thresh.domain.model.SubAlarm;
import com.hpcloud.streaming.storm.Logging;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Processes events by emitting tuples related to the event.
 *
 * <ul>
 * <li>Input: Object event
 * <li>Output alarm-events: String eventType, String alarmId
 * <li>Output metric-alarm-events: String eventType, MetricDefinition metricDefinition, String
 * alarmId, String subAlarmId
 * <li>Output metric-sub-alarm-events: String eventType, MetricDefinition metricDefinition, SubAlarm
 * subAlarm
 * </ul>
 */
public class EventProcessingBolt extends BaseRichBolt {
  private static final long serialVersionUID = 897171858708109378L;

  /** Stream for alarm specific events. */
  public static final String ALARM_EVENT_STREAM_ID = "alarm-events";
  /** Stream for metric and alarm specific events. */
  public static final String METRIC_ALARM_EVENT_STREAM_ID = "metric-alarm-events";
  /** Stream for metric and sub-alarm specific events. */
  public static final String METRIC_SUB_ALARM_EVENT_STREAM_ID = "metric-sub-alarm-events";

  public static final String[] ALARM_EVENT_STREAM_FIELDS = new String[] {"eventType", "alarmId",
      "alarm"};
  public static final String[] METRIC_ALARM_EVENT_STREAM_FIELDS = new String[] {"eventType",
      "metricDefinitionAndTenantId", "subAlarmId"};
  public static final String[] METRIC_SUB_ALARM_EVENT_STREAM_FIELDS = new String[] {"eventType",
      "metricDefinitionAndTenantId", "subAlarm"};

  public static final String CREATED = "created";
  public static final String DELETED = "deleted";
  public static final String UPDATED = "updated";
  public static final String RESEND = "resend";

  private transient Logger logger;
  private OutputCollector collector;

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declareStream(ALARM_EVENT_STREAM_ID, new Fields(ALARM_EVENT_STREAM_FIELDS));
    declarer.declareStream(METRIC_ALARM_EVENT_STREAM_ID, new Fields(
        METRIC_ALARM_EVENT_STREAM_FIELDS));
    declarer.declareStream(METRIC_SUB_ALARM_EVENT_STREAM_ID, new Fields(
        METRIC_SUB_ALARM_EVENT_STREAM_FIELDS));
  }

  @Override
  public void execute(Tuple tuple) {
    try {
      Object event = tuple.getValue(0);
      logger.trace("Received event for processing {}", event);
      if (event instanceof AlarmCreatedEvent) {
        handle((AlarmCreatedEvent) event);
      } else if (event instanceof AlarmDeletedEvent) {
        handle((AlarmDeletedEvent) event);
      } else if (event instanceof AlarmUpdatedEvent) {
        handle((AlarmUpdatedEvent) event);
      }
    } catch (Exception e) {
      logger.error("Error processing tuple {}", tuple, e);
    } finally {
      collector.ack(tuple);
    }
  }

  @Override
  @SuppressWarnings("rawtypes")
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    logger = LoggerFactory.getLogger(Logging.categoryFor(getClass(), context));
    logger.info("Preparing");
    this.collector = collector;
  }

  void handle(AlarmCreatedEvent event) {
    for (Map.Entry<String, AlarmSubExpression> subExpressionEntry : event.alarmSubExpressions
        .entrySet()) {
      sendAddSubAlarm(event.alarmId, subExpressionEntry.getKey(), event.tenantId,
          subExpressionEntry.getValue());
    }
  }

  private void sendAddSubAlarm(String alarmId, String subAlarmId, String tenantId,
      AlarmSubExpression alarmSubExpression) {
    sendSubAlarm(CREATED, alarmId, subAlarmId, tenantId, alarmSubExpression);
  }

  private void sendUpdateSubAlarm(String alarmId, String subAlarmId, String tenantId,
      AlarmSubExpression alarmSubExpression) {
    sendSubAlarm(UPDATED, alarmId, subAlarmId, tenantId, alarmSubExpression);
  }

  private void sendResendSubAlarm(String alarmId, String subAlarmId, String tenantId,
      AlarmSubExpression alarmSubExpression) {
    sendSubAlarm(RESEND, alarmId, subAlarmId, tenantId, alarmSubExpression);
  }

  private void sendSubAlarm(String eventType, String alarmId, String subAlarmId, String tenantId,
      AlarmSubExpression alarmSubExpression) {
    MetricDefinition metricDef = alarmSubExpression.getMetricDefinition();
    collector.emit(METRIC_SUB_ALARM_EVENT_STREAM_ID, new Values(eventType,
        new MetricDefinitionAndTenantId(metricDef, tenantId), new SubAlarm(subAlarmId, alarmId,
            alarmSubExpression)));
  }

  void handle(AlarmDeletedEvent event) {
    for (Map.Entry<String, MetricDefinition> entry : event.subAlarmMetricDefinitions.entrySet()) {
      sendDeletedSubAlarm(entry.getKey(), event.tenantId, entry.getValue());
    }

    collector.emit(ALARM_EVENT_STREAM_ID, new Values(DELETED, event.alarmId, event));
  }

  private void sendDeletedSubAlarm(String subAlarmId, String tenantId, MetricDefinition metricDef) {
    collector.emit(METRIC_ALARM_EVENT_STREAM_ID, new Values(DELETED,
        new MetricDefinitionAndTenantId(metricDef, tenantId), subAlarmId));
  }

  void handle(AlarmUpdatedEvent event) {
    if ((!event.oldAlarmState.equals(event.alarmState) || !event.oldAlarmSubExpressions.isEmpty())
        && event.changedSubExpressions.isEmpty() && event.newAlarmSubExpressions.isEmpty()) {
      for (Map.Entry<String, AlarmSubExpression> entry : event.unchangedSubExpressions.entrySet()) {
        sendResendSubAlarm(event.alarmId, entry.getKey(), event.tenantId, entry.getValue());
      }
    }
    for (Map.Entry<String, AlarmSubExpression> entry : event.oldAlarmSubExpressions.entrySet()) {
      sendDeletedSubAlarm(entry.getKey(), event.tenantId, entry.getValue().getMetricDefinition());
    }
    for (Map.Entry<String, AlarmSubExpression> entry : event.changedSubExpressions.entrySet()) {
      sendUpdateSubAlarm(event.alarmId, entry.getKey(), event.tenantId, entry.getValue());
    }
    for (Map.Entry<String, AlarmSubExpression> entry : event.newAlarmSubExpressions.entrySet()) {
      sendAddSubAlarm(event.alarmId, entry.getKey(), event.tenantId, entry.getValue());
    }
    collector.emit(ALARM_EVENT_STREAM_ID, new Values(UPDATED, event.alarmId, event));
  }
}
