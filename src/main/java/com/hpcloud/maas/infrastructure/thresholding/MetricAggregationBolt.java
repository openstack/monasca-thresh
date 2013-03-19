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

import com.hpcloud.maas.common.model.AlarmState;
import com.hpcloud.maas.common.model.Metric;
import com.hpcloud.maas.domain.model.Alarm;
import com.hpcloud.maas.domain.model.SlidingWindowStats;
import com.hpcloud.maas.domain.model.Statistic;
import com.hpcloud.maas.domain.model.Statistics;
import com.hpcloud.maas.infrastructure.storm.Tuples;

/**
 * Aggregates metrics for individual alarms. Receives metric/alarm tuples and tick tuples, and
 * outputs alarm information whenever an alarm's state changes.
 * 
 * <ul>
 * <li>Input - "metric" : Metric, "alarm" : Alarm
 * <li>Output - "compositeAlarmId" : String, "alarmId" : String, "alarmState" : String
 * </ul>
 * 
 * @author Jonathan Halterman
 */
public class MetricAggregationBolt extends BaseRichBolt {
  private static final Logger LOG = LoggerFactory.getLogger(MetricAggregationBolt.class);
  private static final long serialVersionUID = 5624314196838090726L;

  private OutputCollector collector;
  private List<Alarm> alarms;
  private Map<Alarm, SlidingWindowStats> alarmStats = new HashMap<Alarm, SlidingWindowStats>();

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("compositeAlarmId", "alarmId", "alarmState"));
  }

  @Override
  public void execute(Tuple tuple) {
    if (!Tuples.isTickTuple(tuple))
      aggregateMetric(tuple);
    else
      evaluateAlarms();
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
    this.collector = collector;
  }

  /**
   * Aggregates metrics for values that are within the periods defined for the alarm.
   */
  private void aggregateMetric(Tuple tuple) {
    Metric metric = (Metric) tuple.getValue(0);
    Alarm alarm = (Alarm) tuple.getValue(1);

    SlidingWindowStats stats = alarmStats.get(alarm);
    if (stats == null) {
      Class<? extends Statistic> statType = Statistics.statTypeFor(alarm.getFunction());
      if (statType == null)
        LOG.warn("Unknown statistic type {}", alarm.getFunction());
      else {
        stats = new SlidingWindowStats(alarm.getPeriodSeconds(), alarm.getPeriods(), statType,
            metric.timestamp);
        alarmStats.put(alarm, stats);
      }
    }

    stats.addValue((int) metric.value, metric.timestamp);
  }

  private void evaluateAlarm(Alarm alarm) {
    SlidingWindowStats stats = alarmStats.get(alarm);
    AlarmState initialState = alarm.getState();
    AlarmState newState = null;

    if (stats == null)
      newState = AlarmState.UNDETERMINED;
    else {
      // Evaluate and update state of alarm
      String finalState = null;

      // We may want to track each alarm's state and only emit when the state changes. that means
      // we'd
      // evaluate the alarm each time a metric hits the alarm.

      // we must wait till evaluationPeriod periods have passed before even bothering to evaluate
      // the
      // alarm.
      // after that we can evaluate every 1 second or whatever

      // actually, this isn't good enough since we have to detect insufficient data things on a 60
      // sec
      // timer
    }

    if (stats == null || !newState.equals(initialState))
      collector.emit(new Values(alarm.getCompositeId(), alarm.getId(), alarm.getState().toString()));
  }

  private void evaluateAlarms() {
    for (Alarm alarm : alarms)
      evaluateAlarm(alarm);
  }
}
