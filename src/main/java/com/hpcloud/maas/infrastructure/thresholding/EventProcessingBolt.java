package com.hpcloud.maas.infrastructure.thresholding;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.hpcloud.maas.common.event.AlarmCreatedEvent;
import com.hpcloud.maas.common.event.AlarmDeletedEvent;
import com.hpcloud.maas.common.model.alarm.AlarmSubExpression;
import com.hpcloud.maas.common.model.metric.CollectdMetrics;
import com.hpcloud.maas.common.model.metric.MetricDefinition;
import com.hpcloud.maas.domain.model.SubAlarm;
import com.hpcloud.maas.infrastructure.storm.Logging;

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
 * 
 * @author Jonathan Halterman
 */
public class EventProcessingBolt extends BaseRichBolt {
  private static final long serialVersionUID = 897171858708109378L;

  /** Stream for alarm specific events. */
  public static final String ALARM_EVENT_STREAM_ID = "alarm-events";
  /** Stream for metric and alarm specific events. */
  public static final String METRIC_ALARM_EVENT_STREAM_ID = "metric-alarm-events";
  /** Stream for metric and sub-alarm specific events. */
  public static final String METRIC_SUB_ALARM_EVENT_STREAM_ID = "metric-sub-alarm-events";

  private Logger LOG;
  private OutputCollector collector;

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declareStream(ALARM_EVENT_STREAM_ID, new Fields("eventType", "alarmId"));
    declarer.declareStream(METRIC_ALARM_EVENT_STREAM_ID, new Fields("eventType",
        "metricDefinition", "subAlarmId"));
    declarer.declareStream(METRIC_SUB_ALARM_EVENT_STREAM_ID, new Fields("eventType",
        "metricDefinition", "subAlarm"));
  }

  @Override
  public void execute(Tuple tuple) {
    try {
      Object event = tuple.getValue(0);
      LOG.trace("Received event for processing {}", event);
      if (event instanceof AlarmCreatedEvent)
        handle((AlarmCreatedEvent) event);
      else if (event instanceof AlarmDeletedEvent)
        handle((AlarmDeletedEvent) event);
    } catch (Exception e) {
      LOG.error("Error processing tuple {}", tuple, e);
    } finally {
      collector.ack(tuple);
    }
  }

  @Override
  @SuppressWarnings("rawtypes")
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    LOG = LoggerFactory.getLogger(Logging.categoryFor(context));
    LOG.info("Preparing");
    this.collector = collector;
  }

  void handle(AlarmCreatedEvent event) {
    String eventType = event.getClass().getSimpleName();
    for (Map.Entry<String, AlarmSubExpression> subExpressionEntry : event.alarmSubExpressions.entrySet()) {
      MetricDefinition metricDef = subExpressionEntry.getValue().getMetricDefinition();
      // TODO remove in the future
      CollectdMetrics.removeUnsupportedDimensions(metricDef);
      collector.emit(METRIC_SUB_ALARM_EVENT_STREAM_ID, new Values(eventType, metricDef,
          new SubAlarm(subExpressionEntry.getKey(), event.alarmId, subExpressionEntry.getValue())));
    }
  }

  void handle(AlarmDeletedEvent event) {
    String eventType = event.getClass().getSimpleName();
    for (Map.Entry<String, MetricDefinition> entry : event.subAlarmMetricDefinitions.entrySet()) {
      MetricDefinition metricDef = entry.getValue();
      // TODO remove in the future
      CollectdMetrics.removeUnsupportedDimensions(metricDef);
      collector.emit(METRIC_ALARM_EVENT_STREAM_ID, new Values(eventType, metricDef, entry.getKey()));
    }

    collector.emit(ALARM_EVENT_STREAM_ID, new Values(eventType, event.alarmId));
  }
}
