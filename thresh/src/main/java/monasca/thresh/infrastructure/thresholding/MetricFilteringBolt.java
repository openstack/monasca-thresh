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

import monasca.common.model.event.AlarmDefinitionCreatedEvent;
import monasca.common.model.event.AlarmDefinitionDeletedEvent;
import monasca.common.model.alarm.AlarmExpression;
import monasca.common.model.alarm.AlarmSubExpression;
import monasca.common.model.metric.Metric;
import monasca.common.streaming.storm.Logging;
import monasca.common.streaming.storm.Streams;
import monasca.common.util.Injector;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import monasca.thresh.domain.model.Alarm;
import monasca.thresh.domain.model.AlarmDefinition;
import monasca.thresh.domain.model.MetricDefinitionAndTenantId;
import monasca.thresh.domain.model.MetricDefinitionAndTenantIdMatcher;
import monasca.thresh.domain.model.SubAlarm;
import monasca.thresh.domain.model.TenantIdAndMetricName;
import monasca.thresh.domain.service.AlarmDAO;
import monasca.thresh.domain.service.AlarmDefinitionDAO;
import monasca.thresh.infrastructure.persistence.PersistenceModule;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Filters metrics for which there is no associated alarm and forwards metrics for which there is an
 * alarm. Receives metric alarm and metric sub-alarm events to update metric definitions.
 *
 * The alreadyFound and the matcher are shared between any bolts in the same worker process so that
 * all of the MetricDefinitionAndTenantIds for existing SubAlarms only have to be read once and
 * because it is not possible to predict which bolt gets which Metrics so all Bolts know about all
 * starting MetricDefinitionAndTenantIds.
 *
 * The current topology uses fieldGrouping for the incoming Metrics and allGrouping for the
 * events. So, a Bolt will always get the same Metrics it just can't be predicted which ones.
 *
 * The alreadyFound contains a Set of AlarmDefinition IDs that reference the same
 * MetricDefinitionAndTenantId so if a AlarmDefinition is deleted, the MetricDefinitionAndTenantId will
 * only be deleted from it and the matcher if no more AlarmDefinitions reference it. Adding and
 * deleting to list is done under the static lock SENTINAL to ensure it is correct across all
 * Bolts sharing the same alreadyFound and the matcher. The amount of adds and deletes will be
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

  public static final String NEW_METRIC_FOR_ALARM_DEFINITION_STREAM = "newMetricForAlarmDefinitionStream";
  public static final String[] NEW_METRIC_FOR_ALARM_DEFINITION_FIELDS =
      new String[] {"metricDefinitionAndTenantId", "alarmDefinitionId"};
  public static final String MIN_LAG_VALUE_KEY = "monasca.thresh.filtering.minLagValue";
  public static final int MIN_LAG_VALUE_DEFAULT = 10;
  public static final String MAX_LAG_MESSAGES_KEY = "monasca.thresh.filtering.maxLagMessages";
  public static final int MAX_LAG_MESSAGES_DEFAULT = 10;
  public static final String LAG_MESSAGE_PERIOD_KEY = "monasca.thresh.filtering.lagMessagePeriod";
  public static final int LAG_MESSAGE_PERIOD_DEFAULT = 30;
  public static final String[] FIELDS = new String[] {"tenantIdAndMetricName", "metric"};

  private static final int MIN_LAG_VALUE = PropertyFinder.getIntProperty(MIN_LAG_VALUE_KEY,
      MIN_LAG_VALUE_DEFAULT, 0, Integer.MAX_VALUE);
  private static final int MAX_LAG_MESSAGES = PropertyFinder.getIntProperty(MAX_LAG_MESSAGES_KEY,
      MAX_LAG_MESSAGES_DEFAULT, 0, Integer.MAX_VALUE);
  private static final int LAG_MESSAGE_PERIOD = PropertyFinder.getIntProperty(
      LAG_MESSAGE_PERIOD_KEY, LAG_MESSAGE_PERIOD_DEFAULT, 1, 600);
  private static final MetricDefinitionAndTenantIdMatcher matcher =
      new MetricDefinitionAndTenantIdMatcher();
  private static final ExistingHolder alreadyFound = new ExistingHolder();
  private static final Object SENTINAL = new Object();
  private static final Map<String, AlarmDefinition> alarmDefinitions = new ConcurrentHashMap<>();
  private transient Logger logger;
  private DataSourceFactory dbConfig;
  private transient AlarmDAO alarmDAO;
  private transient AlarmDefinitionDAO alarmDefDAO;
  private OutputCollector collector;
  private long minLag = Long.MAX_VALUE;
  private long lastMinLagMessageSent = 0;
  private long minLagMessageSent = 0;
  private boolean lagging = true;

  public MetricFilteringBolt(DataSourceFactory dbConfig) {
    this.dbConfig = dbConfig;
  }

  public MetricFilteringBolt(AlarmDefinitionDAO alarmDefDAO, AlarmDAO alarmDAO) {
    this.alarmDefDAO = alarmDefDAO;
    this.alarmDAO = alarmDAO;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields(FIELDS));
    declarer.declareStream(NEW_METRIC_FOR_ALARM_DEFINITION_STREAM, new Fields(
        NEW_METRIC_FOR_ALARM_DEFINITION_FIELDS));
    declarer.declareStream(MetricAggregationBolt.METRIC_AGGREGATION_CONTROL_STREAM, new Fields(
        MetricAggregationBolt.METRIC_AGGREGATION_CONTROL_FIELDS));
    declarer.declareStream(AlarmCreationBolt.ALARM_CREATION_STREAM, new Fields(
        AlarmCreationBolt.ALARM_CREATION_FIELDS));
  }

  @Override
  public void execute(Tuple tuple) {
    logger.debug("tuple: {}", tuple);
    try {
      if (Streams.DEFAULT_STREAM_ID.equals(tuple.getSourceStreamId())) {
        final TenantIdAndMetricName timn = (TenantIdAndMetricName)tuple.getValue(0);
        final Long timestamp = (Long) tuple.getValue(1);
        final Metric metric = (Metric) tuple.getValue(2);
        final MetricDefinitionAndTenantId metricDefinitionAndTenantId =
            new MetricDefinitionAndTenantId(metric.definition(), timn.getTenantId());
        checkLag(timestamp);

        logger.debug("metric definition and tenant id: {}", metricDefinitionAndTenantId);
        if (checkForMatch(metricDefinitionAndTenantId)) {
          collector.emit(new Values(timn, metric));
        }
      } else {
        String eventType = tuple.getString(0);

        logger.debug("Received {} on {}", eventType, tuple.getSourceStreamId());
        // UPDATED events can be ignored because the MetricDefinitionAndTenantId doesn't change
        if (EventProcessingBolt.METRIC_ALARM_EVENT_STREAM_ID.equals(tuple.getSourceStreamId())) {
          if (EventProcessingBolt.DELETED.equals(eventType)) {
            removeAlarm((MetricDefinitionAndTenantId) tuple.getValue(2), tuple.getString(3));
          }
        } else if (EventProcessingBolt.ALARM_DEFINITION_EVENT_STREAM_ID.equals(tuple
            .getSourceStreamId())) {
          if (EventProcessingBolt.CREATED.equals(eventType)) {
            synchronized (SENTINAL) {
              final AlarmDefinitionCreatedEvent event =
                  (AlarmDefinitionCreatedEvent) tuple.getValue(1);
              final AlarmDefinition alarmDefinition =
                  new AlarmDefinition(event.alarmDefinitionId, event.tenantId, event.alarmName,
                      event.alarmDescription, new AlarmExpression(event.alarmExpression), "LOW",
                      true, event.matchBy);
              newAlarmDefinition(alarmDefinition);
            }
          }
          else if (EventProcessingBolt.DELETED.equals(eventType)) {
            final AlarmDefinitionDeletedEvent event = (AlarmDefinitionDeletedEvent) tuple.getValue(1);
            deleteAlarmDefinition(event.alarmDefinitionId);
          }
        }
      }
    } catch (Exception e) {
      logger.error("Error processing tuple {}", tuple, e);
    } finally {
      collector.ack(tuple);
    }
  }

  private void deleteAlarmDefinition(final String alarmDefinitionId) {
    synchronized (SENTINAL) {
      final AlarmDefinition alarmDefinition = alarmDefinitions.get(alarmDefinitionId);
      if (alarmDefinition != null) {
        logger.info("Deleting Alarm Definition {}", alarmDefinitionId);
        alarmDefinitions.remove(alarmDefinitionId);
        alreadyFound.removeAlarmDefinition(alarmDefinitionId);
        for (final MetricDefinitionAndTenantId mtid : getAllMetricDefinitions(alarmDefinition)) {
          matcher.remove(mtid, alarmDefinition.getId());
        }
      }
    }
  }

  private Set<MetricDefinitionAndTenantId> getAllMetricDefinitions(
      final AlarmDefinition alarmDefinition) {
    final Set<MetricDefinitionAndTenantId> result =
        new HashSet<>(alarmDefinition.getAlarmExpression().getSubExpressions().size());
    for (final AlarmSubExpression subExpr : alarmDefinition.getAlarmExpression()
        .getSubExpressions()) {
      final MetricDefinitionAndTenantId mtid =
          new MetricDefinitionAndTenantId(subExpr.getMetricDefinition(),
              alarmDefinition.getTenantId());
      result.add(mtid);
    }
    return result;
  }

  private void newAlarmDefinition(final AlarmDefinition alarmDefinition) {
    alarmDefinitions.put(alarmDefinition.getId(), alarmDefinition);
    for (final MetricDefinitionAndTenantId mtid : getAllMetricDefinitions(alarmDefinition)) {
      matcher.add(mtid, alarmDefinition.getId());
    }
  }

  private boolean checkForMatch(MetricDefinitionAndTenantId metricDefinitionAndTenantId) {
    final Set<String> alarmDefinitionIds = matcher.match(metricDefinitionAndTenantId);
    if (alarmDefinitionIds.isEmpty()) {
      return false;
    }
    final Set<String> existing = alreadyFound.matches(metricDefinitionAndTenantId);
    if (existing != null) {
      alarmDefinitionIds.removeAll(existing);
    }

    if (!alarmDefinitionIds.isEmpty()) {
      for (final String alarmDefinitionId : alarmDefinitionIds) {
        final AlarmDefinition alarmDefinition = alarmDefinitions.get(alarmDefinitionId);
        logger.info("Add metric {} for Alarm Definition id = {} name = {}",
            metricDefinitionAndTenantId, alarmDefinitionId, alarmDefinition.getName());
        collector.emit(NEW_METRIC_FOR_ALARM_DEFINITION_STREAM,
            new Values(metricDefinitionAndTenantId, alarmDefinitionId));
        synchronized (SENTINAL) {
          alreadyFound.add(metricDefinitionAndTenantId, alarmDefinitionId);
        }
      }
    }
    return true;
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

  private void removeAlarm(MetricDefinitionAndTenantId metricDefinitionAndTenantId,
      String alarmDefinitionId) {
    final AlarmDefinition alarmDefinition = alarmDefinitions.get(alarmDefinitionId);
    if (alarmDefinition != null) {
      synchronized (SENTINAL) {
        alreadyFound.remove(metricDefinitionAndTenantId, alarmDefinitionId);
      }
    }
  }

  @Override
  @SuppressWarnings("rawtypes")
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    logger = LoggerFactory.getLogger(Logging.categoryFor(getClass(), context));
    logger.info("Preparing");
    this.collector = collector;

    if (alarmDefDAO == null) {
      Injector.registerIfNotBound(AlarmDefinitionDAO.class, new PersistenceModule(dbConfig));
      alarmDefDAO = Injector.getInstance(AlarmDefinitionDAO.class);
    }

    if (alarmDAO == null) {
      Injector.registerIfNotBound(AlarmDAO.class, new PersistenceModule(dbConfig));
      alarmDAO = Injector.getInstance(AlarmDAO.class);
    }

    // DCL
    if (alreadyFound.isEmpty()) {
      synchronized (SENTINAL) {
        if (alreadyFound.isEmpty()) {
          for (AlarmDefinition alarmcDef : alarmDefDAO.listAll()) {
            newAlarmDefinition(alarmcDef);
          }

          // Load the existing Alarms
          for (Alarm alarm : alarmDAO.listAll()) {
            final AlarmDefinition alarmDefinition =
                alarmDefinitions.get(alarm.getAlarmDefinitionId());
            if (alarmDefinition == null) {
              logger.error("AlarmDefinition {} does not exist for Alarm {}, ignoring",
                  alarm.getAlarmDefinitionId(), alarm.getId());
              continue;
            }
            for (final MetricDefinitionAndTenantId mtid : alarm.getAlarmedMetrics()) {
              alreadyFound.add(mtid, alarm.getAlarmDefinitionId());
              for (final SubAlarm subAlarm : alarm.getSubAlarms()) {
                if (AlarmCreationBolt.metricFitsInAlarmSubExpr(subAlarm.getExpression(),
                    mtid.metricDefinition)) {
                  final TenantIdAndMetricName timn = new TenantIdAndMetricName(mtid);
                  final Values values =
                      new Values(EventProcessingBolt.CREATED, timn, mtid,
                          alarm.getAlarmDefinitionId(), subAlarm);
                  logger.debug("Emitting new SubAlarm {}", values);
                  collector.emit(AlarmCreationBolt.ALARM_CREATION_STREAM, values);
                }
              }
            }
          }

          logger.info("Found {} Alarmed Metrics", alreadyFound.size());
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

  /**
   * Only use for testing.
   */
  static void clearMetricDefinitions() {
    alreadyFound.clear();
    matcher.clear();
    alarmDefinitions.clear();
  }

  /**
   * Only use for testing.
   */
  static int sizeMetricDefinitions() {
    return alreadyFound.size();
  }

  private static class ExistingHolder {
    private final Map<MetricDefinitionAndTenantId, Set<String>> metricDefs =
        new ConcurrentHashMap<>();

    /** Have to track which metric defs are used by which Alarm Definition Id
     *  so deletion of an Alarm Definition will work
     */
    private static final Map<String, List<MetricDefinitionAndTenantId>> usedMetrics = new ConcurrentHashMap<>();

    public void add(MetricDefinitionAndTenantId metricDefinitionAndTenantId,
        String alarmDefinitionId) {

      Set<String> alarmDefinitionIds = metricDefs.get(metricDefinitionAndTenantId);
      if (alarmDefinitionIds == null) {
        alarmDefinitionIds = new HashSet<>();
        metricDefs.put(metricDefinitionAndTenantId, alarmDefinitionIds);
      } else if (alarmDefinitionIds.contains(alarmDefinitionId)) {
        return; // Make sure it is only added once. Multiple bolts process the same
                // AlarmCreatedEvent
      }
      alarmDefinitionIds.add(alarmDefinitionId);
      List<MetricDefinitionAndTenantId> metrics = usedMetrics.get(alarmDefinitionId);
      if (metrics == null) {
        metrics = new LinkedList<>();
        usedMetrics.put(alarmDefinitionId, metrics);
      }
      metrics.add(metricDefinitionAndTenantId);
    }

    public void removeAlarmDefinition(String alarmDefinitionId) {
      final List<MetricDefinitionAndTenantId> metrics = usedMetrics.get(alarmDefinitionId);
      if (metrics != null) {
        for (final MetricDefinitionAndTenantId mtid : metrics) {
          removeFromMetricDefs(mtid, alarmDefinitionId);
        }
        usedMetrics.remove(alarmDefinitionId);
      }
    }

    public void remove(MetricDefinitionAndTenantId metricDefinitionAndTenantId,
        String alarmDefinitionId) {
      removeFromMetricDefs(metricDefinitionAndTenantId, alarmDefinitionId);
      final List<MetricDefinitionAndTenantId> metrics = usedMetrics.get(alarmDefinitionId);
      if (metrics != null) {
        if (metrics.remove(metricDefinitionAndTenantId) && metrics.isEmpty()) {
          usedMetrics.remove(alarmDefinitionId);
        }
      }
    }

    private void removeFromMetricDefs(MetricDefinitionAndTenantId metricDefinitionAndTenantId,
        String alarmDefinitionId) {
      final Set<String> alarmDefinitionIds = matches(metricDefinitionAndTenantId);
      if (alarmDefinitionIds != null) {
        if (alarmDefinitionIds.remove(alarmDefinitionId) && alarmDefinitionIds.isEmpty()) {
          metricDefs.remove(metricDefinitionAndTenantId);
        }
      }
    }

    public Set<String> matches(MetricDefinitionAndTenantId mtid) {
      return metricDefs.get(mtid);
    }

    public int size() {
      return metricDefs.size();
    }

    public boolean isEmpty() {
      return metricDefs.isEmpty();
    }

    public void clear() {
      metricDefs.clear();
      usedMetrics.clear();
    }
  }
}
