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
import com.hpcloud.maas.common.model.metric.MetricDefinition;

/**
 * Processes events by emitting tuples related to the event.
 * 
 * <ul>
 * <li>Input: Object event
 * <li>Output alarm-events: MetricDefinition metricDefinition, String compositeAlarmId, String
 * eventType, [AlarmSubExpression alarm]
 * <li>Output composite-alarm-events: String compositeAlarmId, String eventType
 * </ul>
 * 
 * @author Jonathan Halterman
 */
public class EventProcessingBolt extends BaseRichBolt {
  private static final long serialVersionUID = 897171858708109378L;
  private static final Logger LOG = LoggerFactory.getLogger(EventProcessingBolt.class);
  public static final String ALARM_EVENT_STREAM_ID = "alarm-events";
  public static final String COMPOSITE_ALARM_EVENT_STREAM_ID = "composite-alarm-events";

  private TopologyContext context;
  private OutputCollector collector;

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declareStream(ALARM_EVENT_STREAM_ID, new Fields("metricDefinition",
        "compositeAlarmId", "eventType", "alarm"));
    declarer.declareStream(COMPOSITE_ALARM_EVENT_STREAM_ID, new Fields("compositeAlarmId",
        "eventType"));
  }

  @Override
  public void execute(Tuple tuple) {
    Object event = tuple.getValue(0);

    LOG.trace("{} Received event for processing {}", context.getThisTaskId(), event);
    if (event instanceof AlarmCreatedEvent)
      handle((AlarmCreatedEvent) event);
    else if (event instanceof AlarmDeletedEvent)
      handle((AlarmDeletedEvent) event);
  }

  @Override
  @SuppressWarnings("rawtypes")
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    this.context = context;
    this.collector = collector;
  }

  void handle(AlarmCreatedEvent event) {
    for (AlarmSubExpression alarm : event.expression.getSubExpressions())
      collector.emit(ALARM_EVENT_STREAM_ID, new Values(alarm.getMetricDefinition(), event.id,
          event.getClass().getSimpleName(), alarm));
  }

  void handle(AlarmDeletedEvent event) {
    for (MetricDefinition metricDef : event.metricDefinitions)
      collector.emit(ALARM_EVENT_STREAM_ID, new Values(metricDef, event.id, event.getClass()
          .getSimpleName()));
    collector.emit(COMPOSITE_ALARM_EVENT_STREAM_ID, new Values(event.id, event));
  }
}
