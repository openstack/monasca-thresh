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

import monasca.common.model.metric.Metric;
import monasca.common.streaming.storm.Logging;
import monasca.common.streaming.storm.Streams;
import monasca.common.streaming.storm.Tuples;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import monasca.thresh.domain.model.MetricDefinitionAndTenantId;
import monasca.thresh.domain.model.SubAlarm;
import monasca.thresh.domain.model.SubAlarmStats;
import monasca.thresh.domain.model.TenantIdAndMetricName;
import monasca.thresh.domain.service.SubAlarmStatsRepository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
  public static final String TICK_TUPLE_SECONDS_KEY = "monasca.thresh.aggregation.tick.seconds";
  public static final String[] FIELDS = new String[] {"alarmId", "subAlarm"};
  public static final String METRIC_AGGREGATION_CONTROL_STREAM = "MetricAggregationControl";
  public static final String[] METRIC_AGGREGATION_CONTROL_FIELDS = new String[] {"directive"};
  public static final String METRICS_BEHIND = "MetricsBehind";

  final Map<MetricDefinitionAndTenantId, SubAlarmStatsRepository> metricDefToSubAlarmStatsRepos =
      new HashMap<>();
  private final Set<SubAlarmStats> subAlarmStatsSet = new HashSet<>();
  private final Map<String, SubAlarmStats> subAlarmToSubAlarmStats = new HashMap<>();

  private transient Logger logger;
  /** Namespaces for which metrics are received sporadically */
  private Set<String> sporadicMetricNamespaces = Collections.emptySet();
  private OutputCollector collector;
  private boolean upToDate = true;

  public MetricAggregationBolt() {
  }

  public MetricAggregationBolt(Set<String> sporadicMetricNamespaces) {
    this.sporadicMetricNamespaces = sporadicMetricNamespaces;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields(FIELDS));
  }

  @Override
  public void execute(Tuple tuple) {
    logger.debug("tuple: {}", tuple);
    try {
      if (Tuples.isTickTuple(tuple)) {
        evaluateAlarmsAndSlideWindows();
      } else {
        if (Streams.DEFAULT_STREAM_ID.equals(tuple.getSourceStreamId())) {
          TenantIdAndMetricName timn = (TenantIdAndMetricName) tuple.getValue(0);
          Metric metric = (Metric) tuple.getValueByField("metric");
          MetricDefinitionAndTenantId metricDefinitionAndTenantId =
              new MetricDefinitionAndTenantId(metric.definition(), timn.getTenantId());
          aggregateValues(metricDefinitionAndTenantId, metric);
        } else if (METRIC_AGGREGATION_CONTROL_STREAM.equals(tuple.getSourceStreamId())) {
          processControl(tuple.getString(0));
        } else {
          String eventType = tuple.getString(0);

          if (EventProcessingBolt.METRIC_ALARM_EVENT_STREAM_ID.equals(tuple.getSourceStreamId())) {
            final MetricDefinitionAndTenantId metricDefinitionAndTenantId =
                (MetricDefinitionAndTenantId) tuple.getValue(2);
            final String subAlarmId = tuple.getString(4);
            if (EventProcessingBolt.DELETED.equals(eventType)) {
              handleAlarmDeleted(metricDefinitionAndTenantId, subAlarmId);
            } else if (EventProcessingBolt.RESEND.equals(eventType)) {
              handleAlarmResend(metricDefinitionAndTenantId, subAlarmId);
            }
          } else if (EventProcessingBolt.METRIC_SUB_ALARM_EVENT_STREAM_ID.equals(tuple
              .getSourceStreamId())) {
            MetricDefinitionAndTenantId metricDefinitionAndTenantId =
                (MetricDefinitionAndTenantId) tuple.getValue(2);
            SubAlarm subAlarm = (SubAlarm) tuple.getValue(3);
            if (EventProcessingBolt.UPDATED.equals(eventType)) {
              handleAlarmUpdated(metricDefinitionAndTenantId, subAlarm);
            }
          } else if (AlarmCreationBolt.ALARM_CREATION_STREAM.equals(tuple
              .getSourceStreamId())) {
            MetricDefinitionAndTenantId metricDefinitionAndTenantId =
                (MetricDefinitionAndTenantId) tuple.getValue(2);
            final SubAlarm subAlarm = (SubAlarm) tuple.getValue(4);
            if (EventProcessingBolt.CREATED.equals(eventType)) {
              handleAlarmCreated(metricDefinitionAndTenantId, subAlarm);
            }
          }
        }
      }
    } catch (Exception e) {
      logger.error("Error processing tuple {}", tuple, e);
    } finally {
      collector.ack(tuple);
    }
  }

  private void processControl(final String directive) {
    if (METRICS_BEHIND.equals(directive)) {
      logger.debug("Received {}", directive);
      this.upToDate = false;
    } else {
      logger.error("Unknown directive '{}'", directive);
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
    logger = LoggerFactory.getLogger(Logging.categoryFor(getClass(), context));
    logger.info("Preparing");
    this.collector = collector;
  }

  /**
   * Aggregates values for the {@code metric} that are within the periods defined for the alarm.
   */
  void aggregateValues(MetricDefinitionAndTenantId metricDefinitionAndTenantId, Metric metric) {
    SubAlarmStatsRepository subAlarmStatsRepo =
        getOrCreateSubAlarmStatsRepo(metricDefinitionAndTenantId);
    if (subAlarmStatsRepo == null || metric == null) {
      return;
    }

    for (SubAlarmStats stats : subAlarmStatsRepo.get()) {
      if (stats.getStats().addValue(metric.value, metric.timestamp)) {
        logger.trace("Aggregated value {} at {} for {}. Updated {}", metric.value,
            metric.timestamp, metricDefinitionAndTenantId, stats.getStats());
      } else {
        logger.warn("Metric is too old, age {} seconds: timestamp {} for {}, {}",
            currentTimeSeconds() - metric.timestamp, metric.timestamp, metricDefinitionAndTenantId,
            stats.getStats());
      }
    }
  }

  /**
   * Evaluates all SubAlarms for all SubAlarmStatsRepositories using an evaluation time of 1 minute
   * ago, then sliding the window to the current time.
   */
  void evaluateAlarmsAndSlideWindows() {
    logger.debug("evaluateAlarmsAndSlideWindows called");
    long newWindowTimestamp = currentTimeSeconds();
    for (SubAlarmStats subAlarmStats : subAlarmStatsSet) {
      if (upToDate) {
        logger.debug("Evaluating {}", subAlarmStats);
        if (subAlarmStats.evaluateAndSlideWindow(newWindowTimestamp)) {
          logger.debug("Alarm state changed for {}", subAlarmStats);
          collector.emit(new Values(subAlarmStats.getSubAlarm().getAlarmId(), subAlarmStats
              .getSubAlarm()));
        }
      } else {
        subAlarmStats.slideWindow(newWindowTimestamp);
      }
    }
    if (!upToDate) {
      logger.info("Did not evaluate SubAlarms because Metrics are not up to date");
      upToDate = true;
    }
  }

  /**
   * Only used for testing.
   *
   * @return
   */
  protected long currentTimeSeconds() {
    return System.currentTimeMillis() / 1000;
  }

  /**
   * Returns an existing or newly created SubAlarmStatsRepository for the
   * {@code metricDefinitionAndTenantId}. Newly created SubAlarmStatsRepositories are initialized
   * with stats whose view ends one minute from now.
   */
  SubAlarmStatsRepository getOrCreateSubAlarmStatsRepo(
      MetricDefinitionAndTenantId metricDefinitionAndTenantId) {
    SubAlarmStatsRepository subAlarmStatsRepo = metricDefToSubAlarmStatsRepos.get(metricDefinitionAndTenantId);
    if (subAlarmStatsRepo == null) {
      // This likely happened because MetricFilteringBolt is sending a MetricDefinitionAndTenantId
      // before the AlarmCreationBolt has created a sub alarm for it
      logger.debug("Failed to find sub alarms for {}", metricDefinitionAndTenantId);
    }

    return subAlarmStatsRepo;
  }

  /**
   * Adds the {@code subAlarm} subAlarmStatsRepo for the {@code metricDefinitionAndTenantId}.
   */
  void handleAlarmCreated(MetricDefinitionAndTenantId metricDefinitionAndTenantId, SubAlarm subAlarm) {
    logger.info("Received AlarmCreatedEvent for {}", subAlarm);
    addSubAlarm(metricDefinitionAndTenantId, subAlarm);
  }

  void handleAlarmResend(MetricDefinitionAndTenantId metricDefinitionAndTenantId,
      final String subAlarmId) {
    final RepoAndStats repoAndStats =
        findExistingSubAlarmStats(metricDefinitionAndTenantId, subAlarmId);
    if (repoAndStats == null) {
      return;
    }

    final SubAlarmStats oldSubAlarmStats = repoAndStats.subAlarmStats;
    final SubAlarm oldSubAlarm = oldSubAlarmStats.getSubAlarm();
    oldSubAlarm.setNoState(true); // Have it send its state again so the Alarm can be evaluated
    logger.info("Forcing SubAlarm {} to send state at next evaluation", oldSubAlarm);
  }

  private RepoAndStats findExistingSubAlarmStats(
      MetricDefinitionAndTenantId metricDefinitionAndTenantId, String subAlarmId) {
    final SubAlarmStatsRepository oldSubAlarmStatsRepo =
        metricDefToSubAlarmStatsRepos.get(metricDefinitionAndTenantId);
    if (oldSubAlarmStatsRepo == null) {
      logger.error("Did not find SubAlarmStatsRepository for MetricDefinition {}",
          metricDefinitionAndTenantId);
      return null;
    }
    final SubAlarmStats oldSubAlarmStats = oldSubAlarmStatsRepo.get(subAlarmId);
    if (oldSubAlarmStats == null) {
      logger.error("Did not find existing SubAlarm {} in SubAlarmStatsRepository", subAlarmId);
      return null;
    }
    return new RepoAndStats(oldSubAlarmStatsRepo, oldSubAlarmStats);
  }

  private void addSubAlarm(MetricDefinitionAndTenantId metricDefinitionAndTenantId,
      SubAlarm subAlarm) {
    SubAlarmStats subAlarmStats = subAlarmToSubAlarmStats.get(subAlarm.getId());
    if (subAlarmStats == null) {
      long viewEndTimestamp = currentTimeSeconds() + subAlarm.getExpression().getPeriod();
      subAlarmStats = new SubAlarmStats(subAlarm, viewEndTimestamp);
      subAlarmToSubAlarmStats.put(subAlarm.getId(), subAlarmStats);
      subAlarmStatsSet.add(subAlarmStats);
    }
    SubAlarmStatsRepository subAlarmStatsRepo = metricDefToSubAlarmStatsRepos.get(metricDefinitionAndTenantId);
    if (subAlarmStatsRepo == null) {
      subAlarmStatsRepo = new SubAlarmStatsRepository();
      metricDefToSubAlarmStatsRepos.put(metricDefinitionAndTenantId, subAlarmStatsRepo);
    }
    subAlarmStatsRepo.add(subAlarm.getId(), subAlarmStats);
  }

  protected boolean subAlarmRemoved(final String subAlarmId, MetricDefinitionAndTenantId metricDefinitionAndTenantId) {
    if (subAlarmToSubAlarmStats.containsKey(subAlarmId)) {
      return false;
    }
    SubAlarmStatsRepository subAlarmStatsRepo = metricDefToSubAlarmStatsRepos.get(metricDefinitionAndTenantId);
    if (subAlarmStatsRepo != null) {
      if (metricDefToSubAlarmStatsRepos.containsKey(subAlarmId)) {
        return false;
      }
    }
    for (final SubAlarmStats subAlarmStats : subAlarmStatsSet) {
      if (subAlarmStats.getSubAlarm().getId().equals(subAlarmId)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Adds the {@code subAlarm} subAlarmStatsRepo for the {@code metricDefinition}.
   *
   * MetricDefinition can't have changed, just how it is evaluated
   */
  void handleAlarmUpdated(MetricDefinitionAndTenantId metricDefinitionAndTenantId, SubAlarm subAlarm) {
    logger.debug("Received AlarmUpdatedEvent for {}", subAlarm);
    final RepoAndStats repoAndStats =
        findExistingSubAlarmStats(metricDefinitionAndTenantId, subAlarm.getId());
    if (repoAndStats != null) {
      // Clear the old SubAlarm, but save the SubAlarm state
      final SubAlarmStats oldSubAlarmStats = repoAndStats.subAlarmStats;
      final SubAlarm oldSubAlarm = oldSubAlarmStats.getSubAlarm();
      subAlarm.setState(oldSubAlarm.getState());
      subAlarm.setNoState(true); // Doesn't hurt to send too many state changes, just too few
      if (oldSubAlarm.isCompatible(subAlarm)) {
        logger.debug("Changing {} to {} and keeping measurements", oldSubAlarm,
            subAlarm);
        oldSubAlarmStats.updateSubAlarm(subAlarm);
        return;
      }
      // Have to completely change the SubAlarmStats
      logger.debug("Changing {} to {} and flushing measurements", oldSubAlarm,
          subAlarm);
      repoAndStats.subAlarmStatsRepository.remove(subAlarm.getId());
      subAlarmToSubAlarmStats.remove(subAlarm.getId());
      subAlarmStatsSet.remove(repoAndStats.subAlarmStats);
    }
    addSubAlarm(metricDefinitionAndTenantId, subAlarm);
  }

  /**
   * Removes the sub-alarm for the {@code subAlarmId} from the subAlarmStatsRepo for the
   * {@code metricDefinitionAndTenantId}.
   */
  void handleAlarmDeleted(MetricDefinitionAndTenantId metricDefinitionAndTenantId, String subAlarmId) {
    logger.debug("Received AlarmDeletedEvent for subAlarm id {}", subAlarmId);
    SubAlarmStatsRepository subAlarmStatsRepo = metricDefToSubAlarmStatsRepos.get(metricDefinitionAndTenantId);
    if (subAlarmStatsRepo != null) {
      subAlarmStatsRepo.remove(subAlarmId);
      if (subAlarmStatsRepo.isEmpty()) {
        metricDefToSubAlarmStatsRepos.remove(metricDefinitionAndTenantId);
      }
    }
    final SubAlarmStats subAlarmStats = subAlarmToSubAlarmStats.remove(subAlarmId);
    if (subAlarmStats != null) {
      subAlarmStatsSet.remove(subAlarmStats);
    }
  }

  private static class RepoAndStats {
    public final SubAlarmStatsRepository subAlarmStatsRepository;
    public final SubAlarmStats subAlarmStats;

    public RepoAndStats(SubAlarmStatsRepository subAlarmStatsRepository, SubAlarmStats subAlarmStats) {
      this.subAlarmStatsRepository = subAlarmStatsRepository;
      this.subAlarmStats = subAlarmStats;
    }
  }
}
