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

import com.hpcloud.mon.common.model.metric.Metric;
import com.hpcloud.mon.domain.model.MetricDefinitionAndTenantId;
import com.hpcloud.mon.domain.model.MetricDefinitionAndTenantIdMatcher;
import com.hpcloud.mon.domain.model.SubAlarm;
import com.hpcloud.mon.domain.service.MetricDefinitionDAO;
import com.hpcloud.mon.domain.service.SubAlarmDAO;
import com.hpcloud.mon.domain.service.SubAlarmMetricDefinition;
import com.hpcloud.mon.infrastructure.persistence.PersistenceModule;
import com.hpcloud.streaming.storm.Logging;
import com.hpcloud.streaming.storm.Streams;
import com.hpcloud.util.Injector;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Filters metrics for which there is no associated alarm and forwards metrics for which there is an
 * alarm. Receives metric alarm and metric sub-alarm events to update metric definitions.
 *
 * METRIC_DEFS table and the matcher are shared between any bolts in the same worker process so that
 * all of the MetricDefinitionAndTenantIds for existing SubAlarms only have to be read once and
 * because it is not possible to predict which bolt gets which Metrics so all Bolts know about all
 * starting MetricDefinitionAndTenantIds.
 *
 * The current topology uses shuffleGrouping for the incoming Metrics and allGrouping for the
 * events. So, any Bolt may get any Metric so the METRIC_DEFS table and the matcher must be kept up
 * to date for all MetricDefinitionAndTenantIds.
 *
 * The METRIC_DEFS table contains a List of SubAlarms IDs that reference the same
 * MetricDefinitionAndTenantId so if a SubAlarm is deleted, the MetricDefinitionAndTenantId will
 * only be deleted from it and the matcher if no more SubAlarms reference it. Incrementing and
 * decrementing the count is done under the static lock SENTINAL to ensure it is correct across all
 * Bolts sharing the same METRIC_DEFS table and the matcher. The amount of adds and deletes will be
 * very small compared to the number of Metrics so it shouldn't block the Metric handling.
 *
 * <ul>
 * <li>Input: MetricDefinition metricDefinition, Metric metric
 * <li>Input metric-alarm-events: String eventType, MetricDefinitionAndTenantId
 * metricDefinitionAndTenantId, String alarmId
 * <li>Input metric-sub-alarm-events: String eventType, MetricDefinitionAndTenantId
 * metricDefinitionAndTenantId, SubAlarm subAlarm
 * <li>Output: MetricDefinitionAndTenantId metricDefinitionAndTenantId, Metric metric
 * </ul>
 */
public class MetricFilteringBolt extends BaseRichBolt {
  private static final long serialVersionUID = 1096706128973976599L;

  public static final String MIN_LAG_VALUE_KEY = "com.hpcloud.mon.filtering.minLagValue";
  public static final int MIN_LAG_VALUE_DEFAULT = 10;
  public static final String MAX_LAG_MESSAGES_KEY = "com.hpcloud.mon.filtering.maxLagMessages";
  public static final int MAX_LAG_MESSAGES_DEFAULT = 10;
  public static final String LAG_MESSAGE_PERIOD_KEY = "com.hpcloud.mon.filtering.lagMessagePeriod";
  public static final int LAG_MESSAGE_PERIOD_DEFAULT = 30;
  public static final String[] FIELDS = new String[] {"metricDefinitionAndTenantId", "metric"};

  private static final int MIN_LAG_VALUE = PropertyFinder.getIntProperty(MIN_LAG_VALUE_KEY,
      MIN_LAG_VALUE_DEFAULT, 0, Integer.MAX_VALUE);
  private static final int MAX_LAG_MESSAGES = PropertyFinder.getIntProperty(MAX_LAG_MESSAGES_KEY,
      MAX_LAG_MESSAGES_DEFAULT, 0, Integer.MAX_VALUE);
  private static final int LAG_MESSAGE_PERIOD = PropertyFinder.getIntProperty(
      LAG_MESSAGE_PERIOD_KEY, LAG_MESSAGE_PERIOD_DEFAULT, 1, 600);
  private static final Map<MetricDefinitionAndTenantId, List<String>> METRIC_DEFS =
      new ConcurrentHashMap<>();
  private static final MetricDefinitionAndTenantIdMatcher matcher =
      new MetricDefinitionAndTenantIdMatcher();
  private static final Object SENTINAL = new Object();

  private transient Logger logger;
  private DataSourceFactory dbConfig;
  private transient MetricDefinitionDAO metricDefDAO;
  private OutputCollector collector;
  private long minLag = Long.MAX_VALUE;
  private long lastMinLagMessageSent = 0;
  private long minLagMessageSent = 0;
  private boolean lagging = true;

  public MetricFilteringBolt(DataSourceFactory dbConfig) {
    this.dbConfig = dbConfig;
  }

  public MetricFilteringBolt(MetricDefinitionDAO metricDefDAO) {
    this.metricDefDAO = metricDefDAO;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields(FIELDS));
    declarer.declareStream(MetricAggregationBolt.METRIC_AGGREGATION_CONTROL_STREAM, new Fields(
        MetricAggregationBolt.METRIC_AGGREGATION_CONTROL_FIELDS));
  }

  @Override
  public void execute(Tuple tuple) {
    logger.debug("tuple: {}", tuple);
    try {
      if (Streams.DEFAULT_STREAM_ID.equals(tuple.getSourceStreamId())) {
        final MetricDefinitionAndTenantId metricDefinitionAndTenantId =
            (MetricDefinitionAndTenantId) tuple.getValue(0);
        final Long timestamp = (Long) tuple.getValue(1);
        final Metric metric = (Metric) tuple.getValue(2);
        checkLag(timestamp);

        logger.debug("metric definition and tenant id: {}", metricDefinitionAndTenantId);
        // Check for exact matches as well as inexact matches
        final List<MetricDefinitionAndTenantId> matches =
            matcher.match(metricDefinitionAndTenantId);
        for (final MetricDefinitionAndTenantId match : matches) {
          collector.emit(new Values(match, metric));
        }
      } else {
        String eventType = tuple.getString(0);
        MetricDefinitionAndTenantId metricDefinitionAndTenantId =
            (MetricDefinitionAndTenantId) tuple.getValue(1);

        logger.debug("Received {} for {}", eventType, metricDefinitionAndTenantId);
        // UPDATED events can be ignored because the MetricDefinitionAndTenantId doesn't change
        if (EventProcessingBolt.METRIC_ALARM_EVENT_STREAM_ID.equals(tuple.getSourceStreamId())) {
          if (EventProcessingBolt.DELETED.equals(eventType)) {
            removeSubAlarm(metricDefinitionAndTenantId, tuple.getString(2));
          }
        } else if (EventProcessingBolt.METRIC_SUB_ALARM_EVENT_STREAM_ID.equals(tuple
            .getSourceStreamId())) {
          if (EventProcessingBolt.CREATED.equals(eventType)) {
            synchronized (SENTINAL) {
              final SubAlarm subAlarm = (SubAlarm) tuple.getValue(2);
              addMetricDef(metricDefinitionAndTenantId, subAlarm.getId());
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

  private void checkLag(Long apiTimeStamp) {
    if (!lagging) {
      return;
    }
    if ((apiTimeStamp == null) || (apiTimeStamp.longValue() == 0)) {
      return; // Remove this code at some point, just to handle old metrics without a NPE
    }
    final long now = getCurrentTime();
    final long lag = now - apiTimeStamp.longValue();
    if (lag < minLag) {
      minLag = lag;
    }
    if (minLag <= MIN_LAG_VALUE) {
      lagging = false;
      logger.info("Metrics no longer lagging, minLag = {}", minLag);
    } else if (minLagMessageSent >= MAX_LAG_MESSAGES) {
      logger.info("Waited for {} seconds for Metrics to catch up. Giving up. minLag = {}",
          MAX_LAG_MESSAGES * LAG_MESSAGE_PERIOD, minLag);
      lagging = false;
    } else if (lastMinLagMessageSent == 0) {
      lastMinLagMessageSent = now;
    } else if ((now - lastMinLagMessageSent) >= LAG_MESSAGE_PERIOD) {
      logger.info("Sending {} message, minLag = {}", MetricAggregationBolt.METRICS_BEHIND, minLag);
      collector.emit(MetricAggregationBolt.METRIC_AGGREGATION_CONTROL_STREAM, new Values(
          MetricAggregationBolt.METRICS_BEHIND));
      lastMinLagMessageSent = now;
      minLagMessageSent++;
    }
  }

  private void removeSubAlarm(MetricDefinitionAndTenantId metricDefinitionAndTenantId,
      String subAlarmId) {
    synchronized (SENTINAL) {
      final List<String> subAlarmIds = METRIC_DEFS.get(metricDefinitionAndTenantId);
      if (subAlarmIds != null) {
        if (subAlarmIds.remove(subAlarmId) && subAlarmIds.isEmpty()) {
          METRIC_DEFS.remove(metricDefinitionAndTenantId);
          matcher.remove(metricDefinitionAndTenantId);
        }
      }
    }
  }

  @Override
  @SuppressWarnings("rawtypes")
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    logger = LoggerFactory.getLogger(Logging.categoryFor(getClass(), context));
    logger.info("Preparing");
    this.collector = collector;

    if (metricDefDAO == null) {
      Injector.registerIfNotBound(SubAlarmDAO.class, new PersistenceModule(dbConfig));
      metricDefDAO = Injector.getInstance(MetricDefinitionDAO.class);
    }

    // DCL
    if (METRIC_DEFS.isEmpty()) {
      synchronized (SENTINAL) {
        if (METRIC_DEFS.isEmpty()) {
          for (SubAlarmMetricDefinition subAlarmMetricDef : metricDefDAO.findForAlarms()) {
            addMetricDef(subAlarmMetricDef.getMetricDefinitionAndTenantId(),
                subAlarmMetricDef.getSubAlarmId());
          }
          // Iterate again to ensure we only emit each metricDef once
          for (MetricDefinitionAndTenantId metricDefinitionAndTenantId : METRIC_DEFS.keySet()) {
            collector.emit(new Values(metricDefinitionAndTenantId, null));
          }
          logger.info("Found {} Metric Definitions", METRIC_DEFS.size());
          // Just output these here so they are only output once per JVM
          logger.info("MIN_LAG_VALUE set to {} seconds", MIN_LAG_VALUE);
          logger.info("MAX_LAG_MESSAGES set to {}", MAX_LAG_MESSAGES);
          logger.info("LAG_MESSAGE_PERIOD set to {} seconds", LAG_MESSAGE_PERIOD);
        }
      }
    }
    lastMinLagMessageSent = 0;
  }

  /**
   * Allow override of current time for testing.
   */
  protected long getCurrentTime() {
    return System.currentTimeMillis() / 1000;
  }

  private void addMetricDef(MetricDefinitionAndTenantId metricDefinitionAndTenantId,
      String subAlarmId) {
    List<String> subAlarmIds = METRIC_DEFS.get(metricDefinitionAndTenantId);
    if (subAlarmIds == null) {
      subAlarmIds = new LinkedList<>();
      METRIC_DEFS.put(metricDefinitionAndTenantId, subAlarmIds);
      matcher.add(metricDefinitionAndTenantId);
    } else if (subAlarmIds.contains(subAlarmId)) {
      return; // Make sure it is only added once. Multiple bolts process the same AlarmCreatedEvent
    }
    subAlarmIds.add(subAlarmId);
  }

  /**
   * Only use for testing.
   */
  static void clearMetricDefinitions() {
    METRIC_DEFS.clear();
    matcher.clear();
  }

  /**
   * Only use for testing.
   */
  static int sizeMetricDefinitions() {
    return METRIC_DEFS.size();
  }
}
