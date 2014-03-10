package com.hpcloud.mon.infrastructure.thresholding;

import io.dropwizard.db.DataSourceFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.fileupload.util.Streams;
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

import com.hpcloud.mon.common.event.AlarmCreatedEvent;
import com.hpcloud.mon.common.event.AlarmDeletedEvent;
import com.hpcloud.mon.common.model.metric.Metric;
import com.hpcloud.mon.common.model.metric.MetricDefinition;
import com.hpcloud.mon.domain.model.SubAlarm;
import com.hpcloud.mon.domain.model.SubAlarmStats;
import com.hpcloud.mon.domain.service.SubAlarmDAO;
import com.hpcloud.mon.domain.service.SubAlarmStatsRepository;
import com.hpcloud.mon.infrastructure.persistence.PersistenceModule;
import com.hpcloud.streaming.storm.Logging;
import com.hpcloud.streaming.storm.Tuples;
import com.hpcloud.util.Injector;

/**
 * Aggregates metrics for individual alarms. Receives metric/alarm tuples and tick tuples, and
 * outputs alarm information whenever an alarm's state changes. Concerned with alarms that relate to
 * a specific metric.
 * 
 * The TICK_TUPLE_SECONDS_KEY value should be no greater than the smallest possible window width.
 * This ensures that the window slides in time with the expected metrics.
 * 
 * <ul>
 * <li>Input: MetricDefinition metricDefinition, Metric metric
 * <li>Input metric-alarm-events: String eventType, MetricDefinition metricDefinition, String
 * subAlarmId
 * <li>Input metric-sub-alarm-events: String eventType, MetricDefinition metricDefinition, SubAlarm
 * subAlarm
 * <li>Output: String alarmId, SubAlarm subAlarm
 * </ul>
 * 
 * @author Jonathan Halterman
 */
public class MetricAggregationBolt extends BaseRichBolt {
  private static final long serialVersionUID = 5624314196838090726L;
  public static final String TICK_TUPLE_SECONDS_KEY = "maas.aggregation.tick.seconds";

  final Map<MetricDefinition, SubAlarmStatsRepository> subAlarmStatsRepos = new HashMap<MetricDefinition, SubAlarmStatsRepository>();
  private transient Logger LOG;
  private DataSourceFactory dbConfig;
  private transient SubAlarmDAO subAlarmDAO;
  /** Namespaces for which metrics are received sporadically */
  private Set<String> sporadicMetricNamespaces = Collections.emptySet();
  private OutputCollector collector;
  private int evaluationTimeOffset;

  public MetricAggregationBolt(SubAlarmDAO subAlarmDAO) {
    this.subAlarmDAO = subAlarmDAO;
  }

  public MetricAggregationBolt(DataSourceFactory dbConfig, Set<String> sporadicMetricNamespaces) {
    this.dbConfig = dbConfig;
    this.sporadicMetricNamespaces = sporadicMetricNamespaces;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("alarmId", "subAlarm"));
  }

  @Override
  public void execute(Tuple tuple) {
    try {
      if (Tuples.isTickTuple(tuple)) {
        evaluateAlarmsAndSlideWindows();
      } else {
        if (Streams.DEFAULT_STREAM_ID.equals(tuple.getSourceStreamId())) {
          MetricDefinition metricDefinition = (MetricDefinition) tuple.getValue(0);
          Metric metric = (Metric) tuple.getValueByField("metric");
          aggregateValues(metricDefinition, metric);
        } else {
          String eventType = tuple.getString(0);
          MetricDefinition metricDefinition = (MetricDefinition) tuple.getValue(1);

          if (EventProcessingBolt.METRIC_ALARM_EVENT_STREAM_ID.equals(tuple.getSourceStreamId())) {
            String subAlarmId = tuple.getString(2);
            if (AlarmDeletedEvent.class.getSimpleName().equals(eventType))
              handleAlarmDeleted(metricDefinition, subAlarmId);
          } else if (EventProcessingBolt.METRIC_SUB_ALARM_EVENT_STREAM_ID.equals(tuple.getSourceStreamId())) {
            SubAlarm subAlarm = (SubAlarm) tuple.getValue(2);
            if (AlarmCreatedEvent.class.getSimpleName().equals(eventType))
              handleAlarmCreated(metricDefinition, subAlarm);
          }
        }
      }
    } catch (Exception e) {
      LOG.error("Error processing tuple {}", tuple, e);
    } finally {
      collector.ack(tuple);
    }
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
    LOG = LoggerFactory.getLogger(Logging.categoryFor(getClass(), context));
    LOG.info("Preparing");
    this.collector = collector;
    evaluationTimeOffset = Integer.valueOf(System.getProperty(TICK_TUPLE_SECONDS_KEY, "60"))
        .intValue();

    if (subAlarmDAO == null) {
      Injector.registerIfNotBound(SubAlarmDAO.class, new PersistenceModule(dbConfig));
      subAlarmDAO = Injector.getInstance(SubAlarmDAO.class);
    }
  }

  /**
   * Aggregates values for the {@code metric} that are within the periods defined for the alarm.
   */
  void aggregateValues(MetricDefinition metricDefinition, Metric metric) {
    SubAlarmStatsRepository subAlarmStatsRepo = getOrCreateSubAlarmStatsRepo(metricDefinition);
    if (subAlarmStatsRepo == null || metric == null)
      return;

    for (SubAlarmStats stats : subAlarmStatsRepo.get()) {
      if (stats.getStats().addValue(metric.value, metric.timestamp))
        LOG.trace("Aggregated value {} at {} for {}. Updated {}", metric.value, metric.timestamp,
            metricDefinition, stats.getStats());
      else
        LOG.warn("Invalid metric timestamp {} for {}, {}", metric.timestamp, metricDefinition,
            stats.getStats());
    }
  }

  /**
   * Evaluates all SubAlarms for all SubAlarmStatsRepositories using an evaluation time of 1 minute
   * ago, then sliding the window to the current time.
   */
  void evaluateAlarmsAndSlideWindows() {
    long newWindowTimestamp = System.currentTimeMillis() / 1000;
    for (SubAlarmStatsRepository subAlarmStatsRepo : subAlarmStatsRepos.values())
      for (SubAlarmStats subAlarmStats : subAlarmStatsRepo.get()) {
        LOG.debug("Evaluating {}", subAlarmStats);
        if (subAlarmStats.evaluateAndSlideWindow(newWindowTimestamp)) {
          LOG.debug("Alarm state changed for {}", subAlarmStats);
          collector.emit(new Values(subAlarmStats.getSubAlarm().getAlarmId(),
              subAlarmStats.getSubAlarm()));
        }
      }
  }

  /**
   * Returns an existing or newly created SubAlarmStatsRepository for the {@code metricDefinition}.
   * Newly created SubAlarmStatsRepositories are initialized with stats whose view ends one minute
   * from now.
   */
  SubAlarmStatsRepository getOrCreateSubAlarmStatsRepo(MetricDefinition metricDefinition) {
    SubAlarmStatsRepository subAlarmStatsRepo = subAlarmStatsRepos.get(metricDefinition);
    if (subAlarmStatsRepo == null) {
      List<SubAlarm> subAlarms = subAlarmDAO.find(metricDefinition);
      if (subAlarms.isEmpty())
        LOG.warn("Failed to find sub alarms for {}", metricDefinition);
      else {
        LOG.debug("Creating SubAlarmStats for {}", metricDefinition);
        for (SubAlarm subAlarm : subAlarms)
          subAlarm.setSporadicMetric(sporadicMetricNamespaces.contains(metricDefinition.namespace));
        long viewEndTimestamp = (System.currentTimeMillis() / 1000) + evaluationTimeOffset;
        subAlarmStatsRepo = new SubAlarmStatsRepository(subAlarms, viewEndTimestamp);
        subAlarmStatsRepos.put(metricDefinition, subAlarmStatsRepo);
      }
    }

    return subAlarmStatsRepo;
  }

  /**
   * Adds the {@code subAlarm} subAlarmStatsRepo for the {@code metricDefinition}.
   */
  void handleAlarmCreated(MetricDefinition metricDefinition, SubAlarm subAlarm) {
    LOG.debug("Received AlarmCreatedEvent for {}", subAlarm);
    SubAlarmStatsRepository subAlarmStatsRepo = getOrCreateSubAlarmStatsRepo(metricDefinition);
    if (subAlarmStatsRepo == null)
      return;

    long viewEndTimestamp = (System.currentTimeMillis() / 1000) + evaluationTimeOffset;
    subAlarmStatsRepo.add(subAlarm, viewEndTimestamp);
  }

  /**
   * Removes the sub-alarm for the {@code subAlarmId} from the subAlarmStatsRepo for the
   * {@code metricDefinition}.
   */
  void handleAlarmDeleted(MetricDefinition metricDefinition, String subAlarmId) {
    LOG.debug("Received AlarmDeletedEvent for subAlarm id {}", subAlarmId);
    SubAlarmStatsRepository subAlarmStatsRepo = subAlarmStatsRepos.get(metricDefinition);
    if (subAlarmStatsRepo != null)
      subAlarmStatsRepo.remove(subAlarmId);
    if (subAlarmStatsRepo.isEmpty())
      subAlarmStatsRepos.remove(metricDefinition);
  }
}
