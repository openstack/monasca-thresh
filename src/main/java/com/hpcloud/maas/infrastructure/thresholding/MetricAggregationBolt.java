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
import com.hpcloud.maas.common.event.AlarmCreatedEvent.NewAlarm;
import com.hpcloud.maas.common.event.AlarmDeletedEvent;
import com.hpcloud.maas.common.model.metric.Metric;
import com.hpcloud.maas.common.model.metric.MetricDefinition;
import com.hpcloud.maas.domain.model.Alarm;
import com.hpcloud.maas.domain.model.AlarmData;
import com.hpcloud.maas.domain.model.MetricData;
import com.hpcloud.maas.domain.service.AlarmDAO;
import com.hpcloud.maas.infrastructure.storm.Streams;
import com.hpcloud.maas.infrastructure.storm.Tuples;
import com.hpcloud.maas.util.time.Times;
import com.hpcloud.util.Injector;

/**
 * Aggregates metrics for individual alarms. Receives metric/alarm tuples and tick tuples, and
 * outputs alarm information whenever an alarm's state changes. Concerned with alarms that relate to
 * a specific metric.
 * 
 * <ul>
 * <li>Input: MetricDefinition metricDefinition, Metric metric
 * <li>Input alarm-events: MetricDefinition metricDefinition, String compositeAlarmId, String
 * eventType, [NewAlarm alarm]
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
  private final AlarmDAO alarmDAO;
  private TopologyContext context;
  private OutputCollector collector;

  public MetricAggregationBolt() {
    alarmDAO = Injector.getInstance(AlarmDAO.class);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("compositeAlarmId", "alarmId", "alarmState"));
  }

  @Override
  public void execute(Tuple tuple) {
    if (Tuples.isTickTuple(tuple))
      evaluateAlarms();
    else {
      if (Streams.DEFAULT_STREAM_ID.equals(tuple.getSourceStreamId())) {
        Metric metric = (Metric) tuple.getValueByField("metric");
        LOG.trace("{} Received metric for aggregation {}", context.getThisTaskId(), metric);
        aggregateValues(metric);
      } else if (EventProcessingBolt.ALARM_EVENT_STREAM_ID.equals(tuple.getSourceStreamId())) {
        MetricDefinition metricDefinition = (MetricDefinition) tuple.getValue(0);
        MetricData metricData = getOrCreateMetricData(metricDefinition);
        if (metricData == null)
          return;

        String compositeAlarmId = tuple.getString(1);
        String eventType = tuple.getString(2);

        LOG.debug("{} Received {} event for composite alarm {}", context.getThisTaskId(),
            eventType, compositeAlarmId);
        if (AlarmCreatedEvent.class.getSimpleName().equals(eventType)) {
          NewAlarm newAlarm = (NewAlarm) tuple.getValueByField("alarm");
          handleAlarmCreated(metricData, compositeAlarmId, newAlarm);
        } else if (AlarmDeletedEvent.class.getSimpleName().equals(eventType)) {
          handleAlarmDeleted(metricData, compositeAlarmId);
        }
      }
    }
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    Map<String, Object> conf = new HashMap<String, Object>();
    conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 60);
    return conf;
  }

  @Override
  @SuppressWarnings("rawtypes")
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    this.context = context;
    this.collector = collector;
  }

  /**
   * Aggregates values for the {@code metric} that are within the periods defined for the alarm.
   */
  void aggregateValues(Metric metric) {
    MetricData metricData = getOrCreateMetricData(metric.definition);
    if (metricData == null)
      return;

    metricData.addValue(metric.value, Times.roundDownToNearestSecond(metric.timestamp));
  }

  void evaluateAlarms() {
    long initialTimestamp = Times.roundDownToNearestSecond(System.currentTimeMillis());
    for (MetricData metricData : metricsData.values())
      for (AlarmData alarmData : metricData.getAlarmData())
        if (alarmData.evaluate(initialTimestamp))
          collector.emit(new Values(alarmData.getAlarm().getCompositeId(), alarmData.getAlarm()));
  }

  MetricData getOrCreateMetricData(MetricDefinition metricDefinition) {
    MetricData metricData = metricsData.get(metricDefinition);
    if (metricData == null) {
      List<Alarm> alarms = alarmDAO.find(metricDefinition);
      if (alarms.isEmpty())
        LOG.warn("Failed to find alarm data for {}", metricDefinition);
      else {
        metricData = new MetricData(alarms);
        metricsData.put(metricDefinition, metricData);
        for (Alarm alarm : alarms)
          compositeAlarms.put(alarm.getCompositeId(), alarm.getId());
      }
    }

    return metricData;
  }

  void handleAlarmCreated(MetricData metricData, String compositeAlarmId, NewAlarm newAlarm) {
    metricData.addAlarm(new Alarm(compositeAlarmId, newAlarm));
    compositeAlarms.put(compositeAlarmId, newAlarm.id);
  }

  void handleAlarmDeleted(MetricData metricData, String compositeAlarmId) {
    for (String alarmId : compositeAlarms.removeAll(compositeAlarmId))
      metricData.removeAlarm(alarmId);
  }
}
