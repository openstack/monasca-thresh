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
import com.hpcloud.maas.common.model.metric.Metric;
import com.hpcloud.maas.common.model.metric.MetricDefinition;
import com.hpcloud.maas.domain.model.SubAlarm;
import com.hpcloud.maas.domain.model.SubAlarmStats;
import com.hpcloud.maas.domain.service.SubAlarmDAO;
import com.hpcloud.maas.domain.service.SubAlarmStatsRepository;
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
 * <li>Input metric-alarm-events: String eventType, MetricDefinition metricDefinition, String
 * alarmId
 * <li>Input metric-sub-alarm-events: String eventType, MetricDefinition metricDefinition, SubAlarm
 * subAlarm
 * <li>Output: String alarmId, SubAlarm subAlarm
 * </ul>
 * 
 * @author Jonathan Halterman
 */
public class MetricAggregationBolt extends BaseRichBolt {
  private static final Logger LOG = LoggerFactory.getLogger(MetricAggregationBolt.class);
  private static final long serialVersionUID = 5624314196838090726L;
  public static final String TICK_TUPLE_SECONDS_KEY = "maas.aggregation.tick.seconds";

  private final Map<MetricDefinition, SubAlarmStatsRepository> subAlarmStatsRepos = new HashMap<MetricDefinition, SubAlarmStatsRepository>();
  private final Multimap<String, String> alarmSubAlarms = ArrayListMultimap.create();
  private transient SubAlarmDAO subAlarmDAO;
  private TopologyContext context;
  private OutputCollector collector;

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("alarmId", "subAlarm"));
  }

  @Override
  public void execute(Tuple tuple) {
    if (Tuples.isTickTuple(tuple)) {
      evaluateAlarmsAndAdvanceWindows();
    } else {
      if (Streams.DEFAULT_STREAM_ID.equals(tuple.getSourceStreamId())) {
        Metric metric = (Metric) tuple.getValueByField("metric");
        aggregateValues(metric);
      } else {
        MetricDefinition metricDefinition = (MetricDefinition) tuple.getValue(1);
        SubAlarmStatsRepository metricData = getOrCreateSubAlarmStatsRepo(metricDefinition);
        if (metricData == null)
          return;

        String eventType = tuple.getString(0);

        if (EventProcessingBolt.METRIC_ALARM_EVENT_STREAM_ID.equals(tuple.getSourceStreamId())) {
          String alarmId = tuple.getString(2);
          if (AlarmDeletedEvent.class.getSimpleName().equals(eventType))
            handleAlarmDeleted(metricData, alarmId);
        } else if (EventProcessingBolt.METRIC_SUB_ALARM_EVENT_STREAM_ID.equals(tuple.getSourceStreamId())) {
          SubAlarm subAlarm = (SubAlarm) tuple.getValue(2);
          if (AlarmCreatedEvent.class.getSimpleName().equals(eventType))
            handleAlarmCreated(metricData, subAlarm);
        }
      }
    }
    
    collector.ack(tuple);
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    Map<String, Object> conf = new HashMap<String, Object>();
    conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,
        Integer.valueOf(System.getProperty(TICK_TUPLE_SECONDS_KEY, "60")).intValue());
    return conf;
  }

  @Override
  @SuppressWarnings("rawtypes")
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    this.context = context;
    this.collector = collector;
    subAlarmDAO = Injector.getInstance(SubAlarmDAO.class);
  }

  /**
   * Aggregates values for the {@code metric} that are within the periods defined for the alarm.
   */
  void aggregateValues(Metric metric) {
    LOG.debug("{} Aggregating values for {}", context.getThisTaskId(), metric);
    SubAlarmStatsRepository subAlarmStatsRepo = getOrCreateSubAlarmStatsRepo(metric.definition);
    if (subAlarmStatsRepo == null)
      return;

    for (SubAlarmStats stats : subAlarmStatsRepo.get())
      stats.getStats().addValue(metric.value, metric.timestamp);
  }

  void evaluateAlarmsAndAdvanceWindows() {
    LOG.debug("{} Evaluating alarms and advancing windows", context.getThisTaskId());
    long initialTimestamp = System.currentTimeMillis();
    for (SubAlarmStatsRepository subAlarmStatsRepo : subAlarmStatsRepos.values())
      for (SubAlarmStats subAlarmStats : subAlarmStatsRepo.get()) {
        LOG.debug("{} Evaluating {}", context.getThisTaskId(), subAlarmStats.getStats());
        if (subAlarmStats.evaluateAndAdvanceWindow(initialTimestamp)) {
          LOG.debug("Alarm state changed for {}", subAlarmStats.getSubAlarm());
          collector.emit(new Values(subAlarmStats.getSubAlarm().getAlarmId(),
              subAlarmStats.getSubAlarm()));
        }

        LOG.debug("Updated {}", subAlarmStats);
      }
  }

  SubAlarmStatsRepository getOrCreateSubAlarmStatsRepo(MetricDefinition metricDefinition) {
    SubAlarmStatsRepository subAlarmStatsRepo = subAlarmStatsRepos.get(metricDefinition);
    if (subAlarmStatsRepo == null) {
      List<SubAlarm> subAlarms = subAlarmDAO.find(metricDefinition);
      if (subAlarms.isEmpty())
        LOG.warn("Failed to find sub alarms for {}", metricDefinition);
      else {
        subAlarmStatsRepo = new SubAlarmStatsRepository(subAlarms);
        subAlarmStatsRepos.put(metricDefinition, subAlarmStatsRepo);
        for (SubAlarm subAlarm : subAlarms)
          alarmSubAlarms.put(subAlarm.getAlarmId(), subAlarm.getId());
      }
    }

    return subAlarmStatsRepo;
  }

  void handleAlarmCreated(SubAlarmStatsRepository metricData, SubAlarm subAlarm) {
    LOG.debug("{} Received AlarmCreatedEvent for {}", context.getThisTaskId(), subAlarm);
    long initialTimestamp = System.currentTimeMillis();
    metricData.add(subAlarm, initialTimestamp);
    alarmSubAlarms.put(subAlarm.getAlarmId(), subAlarm.getId());
  }

  void handleAlarmDeleted(SubAlarmStatsRepository metricData, String alarmId) {
    LOG.debug("{} Received AlarmDeletedEvent for alarm id {}", context.getThisTaskId(), alarmId);
    for (String subAlarmId : alarmSubAlarms.removeAll(alarmId))
      metricData.remove(subAlarmId);
  }
}
