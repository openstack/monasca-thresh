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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

import com.hpcloud.mon.common.model.metric.Metric;
import com.hpcloud.mon.domain.model.MetricDefinitionAndTenantId;
import com.hpcloud.mon.domain.model.SubAlarm;
import com.hpcloud.mon.domain.model.SubAlarmStats;
import com.hpcloud.mon.domain.service.SubAlarmDAO;
import com.hpcloud.mon.domain.service.SubAlarmStatsRepository;
import com.hpcloud.mon.infrastructure.persistence.PersistenceModule;
import com.hpcloud.streaming.storm.Logging;
import com.hpcloud.streaming.storm.Streams;
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
 */
public class MetricAggregationBolt extends BaseRichBolt {
  private static final long serialVersionUID = 5624314196838090726L;
  public static final String TICK_TUPLE_SECONDS_KEY = "com.hpcloud.mon.aggregation.tick.seconds";
  public static final String[] FIELDS = new String[] { "alarmId", "subAlarm" };
  public static final String METRIC_AGGREGATION_CONTROL_STREAM = "MetricAggregationControl";
  public static final String[] METRIC_AGGREGATION_CONTROL_FIELDS = new String[] { "directive" };
  public static final String METRICS_BEHIND = "MetricsBehind";

  final Map<MetricDefinitionAndTenantId, SubAlarmStatsRepository> subAlarmStatsRepos = new HashMap<>();
  private transient Logger LOG;
  private DataSourceFactory dbConfig;
  private transient SubAlarmDAO subAlarmDAO;
  /** Namespaces for which metrics are received sporadically */
  private Set<String> sporadicMetricNamespaces = Collections.emptySet();
  private OutputCollector collector;
  private boolean upToDate = true;

  public MetricAggregationBolt(SubAlarmDAO subAlarmDAO) {
    this.subAlarmDAO = subAlarmDAO;
  }

  public MetricAggregationBolt(DataSourceFactory dbConfig, Set<String> sporadicMetricNamespaces) {
    this.dbConfig = dbConfig;
    this.sporadicMetricNamespaces = sporadicMetricNamespaces;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields(FIELDS));
  }

  @Override
  public void execute(Tuple tuple) {
    LOG.debug("tuple: {}", tuple);
    try {
      if (Tuples.isTickTuple(tuple)) {
        evaluateAlarmsAndSlideWindows();
      } else {
        if (Streams.DEFAULT_STREAM_ID.equals(tuple.getSourceStreamId())) {
          MetricDefinitionAndTenantId metricDefinitionAndTenantId = (MetricDefinitionAndTenantId) tuple.getValue(0);
          Metric metric = (Metric) tuple.getValueByField("metric");
          aggregateValues(metricDefinitionAndTenantId, metric);
        } else if (METRIC_AGGREGATION_CONTROL_STREAM.equals(tuple.getSourceStreamId())) {
          processControl(tuple.getString(0));
        } else {
          String eventType = tuple.getString(0);
          MetricDefinitionAndTenantId metricDefinitionAndTenantId = (MetricDefinitionAndTenantId) tuple.getValue(1);

          if (EventProcessingBolt.METRIC_ALARM_EVENT_STREAM_ID.equals(tuple.getSourceStreamId())) {
            String subAlarmId = tuple.getString(2);
            if (EventProcessingBolt.DELETED.equals(eventType))
              handleAlarmDeleted(metricDefinitionAndTenantId, subAlarmId);
          } else if (EventProcessingBolt.METRIC_SUB_ALARM_EVENT_STREAM_ID.equals(tuple.getSourceStreamId())) {
            SubAlarm subAlarm = (SubAlarm) tuple.getValue(2);
            if (EventProcessingBolt.CREATED.equals(eventType))
              handleAlarmCreated(metricDefinitionAndTenantId, subAlarm);
            else if (EventProcessingBolt.UPDATED.equals(eventType))
              handleAlarmUpdated(metricDefinitionAndTenantId, subAlarm);
          }
        }
      }
    } catch (Exception e) {
      LOG.error("Error processing tuple {}", tuple, e);
    } finally {
      collector.ack(tuple);
    }
  }

  private void processControl(final String directive) {
    if (METRICS_BEHIND.equals(directive)) {
      LOG.debug("Received {}", directive);
      this.upToDate = false;
    }
    else
      LOG.error("Unknown directive '{}'", directive);
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

    if (subAlarmDAO == null) {
      Injector.registerIfNotBound(SubAlarmDAO.class, new PersistenceModule(dbConfig));
      subAlarmDAO = Injector.getInstance(SubAlarmDAO.class);
    }
  }

  /**
   * Aggregates values for the {@code metric} that are within the periods defined for the alarm.
   */
  void aggregateValues(MetricDefinitionAndTenantId metricDefinitionAndTenantId, Metric metric) {
    SubAlarmStatsRepository subAlarmStatsRepo = getOrCreateSubAlarmStatsRepo(metricDefinitionAndTenantId);
    if (subAlarmStatsRepo == null || metric == null)
      return;

    for (SubAlarmStats stats : subAlarmStatsRepo.get()) {
      if (stats.getStats().addValue(metric.value, metric.timestamp))
        LOG.trace("Aggregated value {} at {} for {}. Updated {}", metric.value, metric.timestamp,
            metricDefinitionAndTenantId, stats.getStats());
      else
        LOG.warn("Invalid metric timestamp {} for {}, {}", metric.timestamp, metricDefinitionAndTenantId,
            stats.getStats());
    }
  }

  /**
   * Evaluates all SubAlarms for all SubAlarmStatsRepositories using an evaluation time of 1 minute
   * ago, then sliding the window to the current time.
   */
  void evaluateAlarmsAndSlideWindows() {
    if (!upToDate) {
        LOG.info("Not evaluating SubAlarms because Metrics are not up to date");
        upToDate = true;
        return;
    }
    long newWindowTimestamp = currentTimeSeconds();
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
   * Only used for testing.
   * @return
   */
  protected long currentTimeSeconds() {
    return System.currentTimeMillis() / 1000;
  }

  /**
   * Returns an existing or newly created SubAlarmStatsRepository for the {@code metricDefinitionAndTenantId}.
   * Newly created SubAlarmStatsRepositories are initialized with stats whose view ends one minute
   * from now.
   */
  SubAlarmStatsRepository getOrCreateSubAlarmStatsRepo(MetricDefinitionAndTenantId metricDefinitionAndTenantId) {
    SubAlarmStatsRepository subAlarmStatsRepo = subAlarmStatsRepos.get(metricDefinitionAndTenantId);
    if (subAlarmStatsRepo == null) {
      List<SubAlarm> subAlarms = subAlarmDAO.find(metricDefinitionAndTenantId);
      if (subAlarms.isEmpty())
        LOG.warn("Failed to find sub alarms for {}", metricDefinitionAndTenantId);
      else {
        LOG.debug("Creating SubAlarmStats for {}", metricDefinitionAndTenantId);
        for (SubAlarm subAlarm : subAlarms) {
          // TODO should treat metric def name prefix like a namespace
          subAlarm.setSporadicMetric(sporadicMetricNamespaces.contains(metricDefinitionAndTenantId.metricDefinition.name));
        }
        subAlarmStatsRepo = new SubAlarmStatsRepository();
        for (SubAlarm subAlarm : subAlarms) {
            long viewEndTimestamp = currentTimeSeconds() + subAlarm.getExpression().getPeriod();
            subAlarmStatsRepo.add(subAlarm, viewEndTimestamp);
        }
        subAlarmStatsRepos.put(metricDefinitionAndTenantId, subAlarmStatsRepo);
      }
    }

    return subAlarmStatsRepo;
  }

  /**
   * Adds the {@code subAlarm} subAlarmStatsRepo for the {@code metricDefinitionAndTenantId}.
   */
  void handleAlarmCreated(MetricDefinitionAndTenantId metricDefinitionAndTenantId, SubAlarm subAlarm) {
    LOG.debug("Received AlarmCreatedEvent for {}", subAlarm);
    addSubAlarm(metricDefinitionAndTenantId, subAlarm);
  }

  private void addSubAlarm(MetricDefinitionAndTenantId metricDefinitionAndTenantId, SubAlarm subAlarm) {
    SubAlarmStatsRepository subAlarmStatsRepo = getOrCreateSubAlarmStatsRepo(metricDefinitionAndTenantId);
    if (subAlarmStatsRepo == null)
      return;

    long viewEndTimestamp = currentTimeSeconds() + subAlarm.getExpression().getPeriod();
    subAlarmStatsRepo.add(subAlarm, viewEndTimestamp);
  }

  /**
   * Adds the {@code subAlarm} subAlarmStatsRepo for the {@code metricDefinition}.
   *
   * MetricDefinition can't have changed, just how it is evaluated
   */
  void handleAlarmUpdated(MetricDefinitionAndTenantId metricDefinitionAndTenantId, SubAlarm subAlarm) {
    LOG.debug("Received AlarmUpdatedEvent for {}", subAlarm);
    // Clear the old SubAlarm, but save the SubAlarm state
    final SubAlarmStatsRepository oldSubAlarmStatsRepo = subAlarmStatsRepos.get(metricDefinitionAndTenantId);
    if (oldSubAlarmStatsRepo == null) {
      LOG.error("Did not find SubAlarmStatsRepository for MetricDefinition {}", metricDefinitionAndTenantId);
    }
    else {
      final SubAlarmStats oldSubAlarmStats = oldSubAlarmStatsRepo.get(subAlarm.getId());
      if (oldSubAlarmStats == null)
        LOG.error("Did not find existing SubAlarm {} in SubAlarmStatsRepository", subAlarm);
      else {
        final SubAlarm oldSubAlarm = oldSubAlarmStats.getSubAlarm();
        subAlarm.setState(oldSubAlarm.getState());
        subAlarm.setNoState(true); // Doesn't hurt to send too many state changes, just too few
        if (oldSubAlarm.isCompatible(subAlarm)) {
            LOG.debug("Changing SubAlarm {} to SubAlarm {} and keeping measurements", oldSubAlarm, subAlarm);
            oldSubAlarmStats.updateSubAlarm(subAlarm);
            return;
        }
        // Have to completely change the SubAlarmStats
        LOG.debug("Changing SubAlarm {} to SubAlarm {} and flushing measurements", oldSubAlarm, subAlarm);
        oldSubAlarmStatsRepo.remove(subAlarm.getId());
      }
    }
    addSubAlarm(metricDefinitionAndTenantId, subAlarm);
  }

/**
   * Removes the sub-alarm for the {@code subAlarmId} from the subAlarmStatsRepo for the
   * {@code metricDefinitionAndTenantId}.
   */
  void handleAlarmDeleted(MetricDefinitionAndTenantId metricDefinitionAndTenantId, String subAlarmId) {
    LOG.debug("Received AlarmDeletedEvent for subAlarm id {}", subAlarmId);
    SubAlarmStatsRepository subAlarmStatsRepo = subAlarmStatsRepos.get(metricDefinitionAndTenantId);
    if (subAlarmStatsRepo != null) {
      subAlarmStatsRepo.remove(subAlarmId);
      if (subAlarmStatsRepo.isEmpty())
        subAlarmStatsRepos.remove(metricDefinitionAndTenantId);
    }
  }
}
