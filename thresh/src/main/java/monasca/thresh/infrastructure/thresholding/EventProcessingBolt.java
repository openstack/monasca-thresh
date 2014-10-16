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

import monasca.common.model.event.AlarmDefinitionCreatedEvent;
import monasca.common.model.event.AlarmDefinitionDeletedEvent;
import monasca.common.model.event.AlarmDefinitionUpdatedEvent;
import monasca.common.model.event.AlarmDeletedEvent;
import monasca.common.model.event.AlarmUpdatedEvent;
import monasca.common.model.alarm.AlarmSubExpression;
import monasca.common.model.metric.MetricDefinition;
import monasca.thresh.domain.model.MetricDefinitionAndTenantId;
import monasca.thresh.domain.model.SubAlarm;
import monasca.thresh.domain.model.SubExpression;
import monasca.thresh.domain.model.TenantIdAndMetricName;
import monasca.thresh.domain.service.AlarmDAO;
import monasca.thresh.infrastructure.persistence.PersistenceModule;
import monasca.common.streaming.storm.Logging;
import monasca.common.util.Injector;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
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
  /** Stream for alarm definition specific events. */
  public static final String ALARM_DEFINITION_EVENT_STREAM_ID = "alarm-definition-events";

  public static final String[] ALARM_EVENT_STREAM_FIELDS = new String[] {"eventType", "alarmId",
      "alarm"};
  public static final String[] METRIC_ALARM_EVENT_STREAM_FIELDS = new String[] {"eventType",
    "tenantIdAndMetricName", "metricDefinitionAndTenantId", "alarmDefinitionId", "subAlarmId"};
  public static final String[] METRIC_SUB_ALARM_EVENT_STREAM_FIELDS = new String[] {"eventType",
      "subExpression", "alarmDefinitionId"};
  public static final String[] ALARM_DEFINITION_EVENT_FIELDS = new String[] {"eventType", "argument"};

  public static final String CREATED = "created";
  public static final String DELETED = "deleted";
  public static final String UPDATED = "updated";
  public static final String RESEND = "resend";

  private transient Logger logger;
  private OutputCollector collector;
  private AlarmDAO alarmDAO;
  private DataSourceFactory dbConfig;

  public EventProcessingBolt(DataSourceFactory dbConfig) {
    this.dbConfig = dbConfig;
  }

  public EventProcessingBolt(AlarmDAO alarmDAO) {
    this.alarmDAO = alarmDAO;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declareStream(ALARM_EVENT_STREAM_ID, new Fields(ALARM_EVENT_STREAM_FIELDS));
    declarer.declareStream(METRIC_ALARM_EVENT_STREAM_ID, new Fields(
        METRIC_ALARM_EVENT_STREAM_FIELDS));
    declarer.declareStream(METRIC_SUB_ALARM_EVENT_STREAM_ID, new Fields(
        METRIC_SUB_ALARM_EVENT_STREAM_FIELDS));
    declarer.declareStream(ALARM_DEFINITION_EVENT_STREAM_ID, new Fields(
        ALARM_DEFINITION_EVENT_FIELDS));
  }

  @Override
  public void execute(Tuple tuple) {
    try {
      Object event = tuple.getValue(0);
      logger.trace("Received event for processing {}", event);
      if (event instanceof AlarmDefinitionCreatedEvent) {
        handle((AlarmDefinitionCreatedEvent) event);
      } else if (event instanceof AlarmDefinitionUpdatedEvent) {
          handle((AlarmDefinitionUpdatedEvent) event);
      } else if (event instanceof AlarmDefinitionDeletedEvent) {
        handle((AlarmDefinitionDeletedEvent) event);
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
    if (alarmDAO == null) {
      Injector.registerIfNotBound(AlarmDAO.class, new PersistenceModule(dbConfig));
      alarmDAO = Injector.getInstance(AlarmDAO.class);
    }
  }

  void handle(AlarmDefinitionCreatedEvent event) {
    collector.emit(ALARM_DEFINITION_EVENT_STREAM_ID, new Values(CREATED, event));
  }

  void handle(AlarmDefinitionDeletedEvent event) {
    collector.emit(ALARM_DEFINITION_EVENT_STREAM_ID, new Values(DELETED, event));
  }

  private void sendUpdateSubAlarm(String alarmId, String subAlarmId, String tenantId,
      SubExpression subExpression) {
    sendSubAlarm(UPDATED, alarmId, subAlarmId, tenantId, subExpression);
  }

  private void sendSubAlarm(String eventType, String alarmId, String subAlarmId, String tenantId,
      SubExpression subExpression) {
    MetricDefinition metricDef = subExpression.getAlarmSubExpression().getMetricDefinition();
    collector.emit(METRIC_SUB_ALARM_EVENT_STREAM_ID, new Values(eventType,
        new MetricDefinitionAndTenantId(metricDef, tenantId), new SubAlarm(subAlarmId, alarmId,
            subExpression)));
  }

  void handle(AlarmDeletedEvent event) {
    processSubAlarms(DELETED, event.tenantId, event.alarmDefinitionId, event.alarmMetrics,
        event.subAlarms);

    collector.emit(ALARM_EVENT_STREAM_ID, new Values(DELETED, event.alarmId, event));
  }

  private void processSubAlarms(String command, final String tenantId,
      final String alarmDefinitionId, final List<MetricDefinition> alarmMetrics,
      final Map<String, AlarmSubExpression> subAlarms) {
    for (final MetricDefinition alarmedMetric : alarmMetrics) {
      for (Map.Entry<String, AlarmSubExpression> entry : subAlarms.entrySet()) {
        if (isSuperSet(alarmedMetric, entry.getValue().getMetricDefinition())) {
          sendSubAlarmMsg(command, entry.getKey(), tenantId, alarmDefinitionId, alarmedMetric);
        }
      }
    }
  }

  private boolean isSuperSet(MetricDefinition toMatch, MetricDefinition match) {
    if (!toMatch.name.equals(match.name)) {
      return false;
    }
    if (match.dimensions == null || match.dimensions.isEmpty()) {
      return true;
    }
    for (final Map.Entry<String, String> entry : toMatch.dimensions.entrySet()) {
      final String value = match.dimensions.get(entry.getKey());
      if (value != null) {
        if (!value.equals(entry.getValue())) {
          return false;
        }
      }
    }
    return true;
  }

  private void sendSubAlarmMsg(String command, String subAlarmId, String tenantId,
      String alarmDefinitionId, MetricDefinition metricDef) {
    collector.emit(METRIC_ALARM_EVENT_STREAM_ID, new Values(command, new TenantIdAndMetricName(
        tenantId, metricDef.name), new MetricDefinitionAndTenantId(metricDef, tenantId),
        alarmDefinitionId, subAlarmId));
  }

  void handle(AlarmDefinitionUpdatedEvent event) {
    // Update SubAlarms
    for (final Map.Entry<String, AlarmSubExpression> entry : event.changedSubExpressions.entrySet()) {
      final int updated = alarmDAO.updateSubAlarmExpressions(entry.getKey(), entry.getValue());
      logger.info("Updated {} SubAlarms with new AlarmSubExpression {} {}", updated, entry.getKey(),
          entry.getValue());
      collector.emit(METRIC_SUB_ALARM_EVENT_STREAM_ID,
          new Values(UPDATED, new SubExpression(entry.getKey(), entry.getValue()),
              event.alarmDefinitionId));
    }
    /* Not sure what this should do
    for (Map.Entry<String, AlarmSubExpression> entry : event.oldAlarmSubExpressions.entrySet()) {
      sendDeletedSubAlarm(entry.getKey(), event.tenantId, entry.getValue().getMetricDefinition());
    }
    // There won't be any way to add Sub Alarms any more. The alarms will have to be destroyed
    // and then created again
    for (Map.Entry<String, AlarmSubExpression> entry : event.newAlarmSubExpressions.entrySet()) {
      sendAddSubAlarm(event.alarmId, entry.getKey(), event.tenantId, entry.getValue());
    }
    */
    collector.emit(ALARM_DEFINITION_EVENT_STREAM_ID, new Values(UPDATED, event));
  }

  void handle(AlarmUpdatedEvent event) {
    if (event.oldAlarmState.equals(event.alarmState)) {
      logger.info("No state change for {}, ignoring", event.alarmId);
    }
    logger.info("Received AlarmUpdatedEvent {}", event);
    processSubAlarms(RESEND, event.tenantId, event.alarmDefinitionId, event.alarmMetrics,
        event.subAlarms);
    collector.emit(ALARM_EVENT_STREAM_ID, new Values(UPDATED, event.alarmId, event));
  }
}
