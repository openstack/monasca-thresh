package com.hpcloud.maas.infrastructure.thresholding;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.hpcloud.maas.common.event.AlarmCreatedEvent;
import com.hpcloud.maas.common.event.AlarmDeletedEvent;
import com.hpcloud.maas.common.model.alarm.AlarmSubExpression;
import com.hpcloud.maas.common.model.metric.Metric;
import com.hpcloud.maas.common.model.metric.MetricDefinition;
import com.hpcloud.maas.domain.model.MetricData;
import com.hpcloud.maas.domain.model.SubAlarm;
import com.hpcloud.maas.domain.model.SubAlarmData;
import com.hpcloud.maas.domain.service.AlarmDAO;
import com.hpcloud.maas.infrastructure.storm.Streams;
import com.hpcloud.maas.infrastructure.storm.Tuples;
import com.hpcloud.util.Injector;

/**
 * Aggregates metrics for individual alarms. Receives metric/alarm tuples and tick tuples, and
 * outputs alarm information whenever an alarm's state changes. Concerned with alarms that relate to
 * a specific metric.
 * 
 * <ul>
 * <li>Input: MetricDefinition metricDefinition, Metric metric
 * <li>Input alarm-events: MetricDefinition metricDefinition, String compositeAlarmId, String
 * eventType, [AlarmSubExpression alarm]
 * <li>Output: String compositeAlarmId, Alarm alarm
 * </ul>
 * 
 * @author Jonathan Halterman
 */
public class MetricAggregationBolt extends BaseRichBolt {
  private static final Logger LOG = LoggerFactory.getLogger(MetricAggregationBolt.class);
  private static final long serialVersionUID = 5624314196838090726L;

  private final Map<MetricDefinition, MetricData> metricsData = new HashMap<MetricDefinition, MetricData>();
  private final Multimap<String, String> compositeAlarms = ArrayListMultimap.create();
  private transient AlarmDAO alarmDAO;
  private TopologyContext context;
  private OutputCollector collector;

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("compositeAlarmId", "alarm"));
  }

  @Override
  public void execute(Tuple tuple) {
    if (Tuples.isTickTuple(tuple)) {
      evaluateAlarmsAndAdvanceWindows();
    } else {
      if (Streams.DEFAULT_STREAM_ID.equals(tuple.getSourceStreamId())) {
        Metric metric = (Metric) tuple.getValueByField("metric");
        aggregateValues(metric);
      } else if (EventProcessingBolt.ALARM_EVENT_STREAM_ID.equals(tuple.getSourceStreamId())) {
        MetricDefinition metricDefinition = (MetricDefinition) tuple.getValue(0);
        MetricData metricData = getOrCreateMetricData(metricDefinition);
        if (metricData == null)
          return;

        String compositeAlarmId = tuple.getString(1);
        String eventType = tuple.getString(2);

        if (AlarmCreatedEvent.class.getSimpleName().equals(eventType)) {
          AlarmSubExpression alarmSubExpression = (AlarmSubExpression) tuple.getValueByField("alarm");
          handleAlarmCreated(metricData, compositeAlarmId, alarmSubExpression);
        } else if (AlarmDeletedEvent.class.getSimpleName().equals(eventType)) {
          handleAlarmDeleted(metricData, compositeAlarmId);
        }
      }
    }
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    Map<String, Object> conf = new HashMap<String, Object>();
    conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 5);// TODO undo to 60
    return conf;
  }

  @Override
  @SuppressWarnings("rawtypes")
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    this.context = context;
    this.collector = collector;
    alarmDAO = Injector.getInstance(AlarmDAO.class);
  }

  /**
   * Aggregates values for the {@code metric} that are within the periods defined for the alarm.
   */
  void aggregateValues(Metric metric) {
    LOG.debug("{} Aggregating values for {}", context.getThisTaskId(), metric);
    MetricData metricData = getOrCreateMetricData(metric.definition);
    if (metricData == null)
      return;

    metricData.addValue(metric.value, metric.timestamp);
  }

  void evaluateAlarmsAndAdvanceWindows() {
    LOG.debug("{} Evaluating alarms and advancing windows", context.getThisTaskId());
    long initialTimestamp = System.currentTimeMillis();
    for (MetricData metricData : metricsData.values())
      for (SubAlarmData alarmData : metricData.getAlarmData()) {
        LOG.debug("{} Evaluating {}", context.getThisTaskId(), alarmData.getStats());
        if (alarmData.evaluate(initialTimestamp)) {
          LOG.debug("Alarm state changed for {}", alarmData.getAlarm());
          collector.emit(new Values(alarmData.getAlarm().getCompositeId(), alarmData.getAlarm()));
        }

        alarmData.getStats().advanceWindowTo(initialTimestamp);
      }
  }

  MetricData getOrCreateMetricData(MetricDefinition metricDefinition) {
    MetricData metricData = metricsData.get(metricDefinition);
    if (metricData == null) {
      List<SubAlarm> alarms = alarmDAO.find(metricDefinition);
      if (alarms.isEmpty())
        LOG.warn("Failed to find alarm data for {}", metricDefinition);
      else {
        metricData = new MetricData(alarms);
        metricsData.put(metricDefinition, metricData);
        for (SubAlarm alarm : alarms)
          compositeAlarms.put(alarm.getCompositeId(), alarm.getExpression().getId());
      }
    }

    return metricData;
  }

  void handleAlarmCreated(MetricData metricData, String compositeAlarmId,
      AlarmSubExpression alarmSubExpression) {
    LOG.debug("{} Received AlarmCreatedEvent for composite alarm id {}, {}",
        context.getThisTaskId(), compositeAlarmId, alarmSubExpression);
    long initialTimestamp = System.currentTimeMillis();
    metricData.addAlarm(new SubAlarm(compositeAlarmId, alarmSubExpression), initialTimestamp);
    compositeAlarms.put(compositeAlarmId, alarmSubExpression.getId());
  }

  void handleAlarmDeleted(MetricData metricData, String compositeAlarmId) {
    LOG.debug("{} Received AlarmDeletedEvent for composite alarm id {}", context.getThisTaskId(),
        compositeAlarmId);
    for (String alarmId : compositeAlarms.removeAll(compositeAlarmId))
      metricData.removeAlarm(alarmId);
  }
}
