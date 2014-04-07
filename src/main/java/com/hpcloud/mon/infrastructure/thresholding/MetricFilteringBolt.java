package com.hpcloud.mon.infrastructure.thresholding;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.hpcloud.mon.common.model.metric.MetricDefinition;
import com.hpcloud.mon.domain.model.SubAlarm;
import com.hpcloud.mon.domain.service.MetricDefinitionDAO;
import com.hpcloud.mon.domain.service.SubAlarmDAO;
import com.hpcloud.mon.domain.service.SubAlarmMetricDefinition;
import com.hpcloud.mon.infrastructure.persistence.PersistenceModule;
import com.hpcloud.streaming.storm.Logging;
import com.hpcloud.streaming.storm.Streams;
import com.hpcloud.util.Injector;

/**
 * Filters metrics for which there is no associated alarm and forwards metrics for which there is an
 * alarm. Receives metric alarm and metric sub-alarm events to update metric definitions.
 *
 * METRIC_DEFS table is shared between any bolts in the same worker process so that all of the
 * Metric Definitions for existing SubAlarms only have to be read once and because it is not
 * possible to predict which bolt gets which Metrics so all Bolts know about all starting
 * MetricDefinitions.
 * 
 * The current topology uses shuffleGrouping for the incoming Metrics and allGrouping for the
 * events. So, any Bolt may get any Metric so the METRIC_DEFS table must be kept up to date
 * for all MetricDefinitions.
 * 
 * The METRIC_DEFS table contains a List of SubAlarms IDs that reference the same MetricDefinition
 * so if a SubAlarm is deleted, the MetricDefinition will only be deleted if no more SubAlarms
 * reference it. Incrementing and decrementing the count is done under the static lock SENTINAL
 * to ensure it is correct across all Bolts sharing the same METRIC_DEFS table. The
 * amount of adds and deletes will be very small compared to the number of Metrics so it shouldn't
 * block the Metric handling.
 * 
 * <ul>
 * <li>Input: MetricDefinition metricDefinition, Metric metric
 * <li>Input metric-alarm-events: String eventType, MetricDefinition metricDefinition, String
 * alarmId
 * <li>Input metric-sub-alarm-events: String eventType, MetricDefinition metricDefinition, SubAlarm
 * subAlarm
 * <li>Output: MetricDefinition metricDefinition, Metric metric
 * </ul>
 * 
 * @author Jonathan Halterman
 */
public class MetricFilteringBolt extends BaseRichBolt {
  private static final long serialVersionUID = 1096706128973976599L;
  private static final Map<MetricDefinition, List<String>> METRIC_DEFS = new ConcurrentHashMap<>();
  private static final Object SENTINAL = new Object();
  public static final String[] FIELDS = new String[] { "metricDefinition", "metric" };

  private transient Logger LOG;
  private DataSourceFactory dbConfig;
  private transient MetricDefinitionDAO metricDefDAO;
  private OutputCollector collector;

  public MetricFilteringBolt(DataSourceFactory dbConfig) {
    this.dbConfig = dbConfig;
  }

  public MetricFilteringBolt(MetricDefinitionDAO metricDefDAO) {
    this.metricDefDAO = metricDefDAO;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields(FIELDS));
  }

  @Override
  public void execute(Tuple tuple) {
      LOG.debug("tuple: {}", tuple);
    try {
      if (Streams.DEFAULT_STREAM_ID.equals(tuple.getSourceStreamId())) {
        MetricDefinition metricDef = (MetricDefinition) tuple.getValue(0);

        LOG.debug("metric definition: {}", metricDef);
        if (METRIC_DEFS.containsKey(metricDef))
          collector.emit(tuple, tuple.getValues());
      } else {
        String eventType = tuple.getString(0);
        MetricDefinition metricDefinition = (MetricDefinition) tuple.getValue(1);

        LOG.debug("Received {} for {}", eventType, metricDefinition);
        if (EventProcessingBolt.METRIC_ALARM_EVENT_STREAM_ID.equals(tuple.getSourceStreamId())) {
          if (EventProcessingBolt.DELETED.equals(eventType))
            removeSubAlarm(metricDefinition, tuple.getString(2));
        } else if (EventProcessingBolt.METRIC_SUB_ALARM_EVENT_STREAM_ID.equals(tuple.getSourceStreamId())) {
          if (EventProcessingBolt.CREATED.equals(eventType))
            synchronized(SENTINAL) {
              final SubAlarm subAlarm = (SubAlarm) tuple.getValue(2);
              addMetricDef(metricDefinition, subAlarm.getId());
            }
        }
      }
    } catch (Exception e) {
      LOG.error("Error processing tuple {}", tuple, e);
    } finally {
      collector.ack(tuple);
    }
  }

  private void removeSubAlarm(MetricDefinition metricDefinition, String subAlarmId) {
    synchronized(SENTINAL) {
      final List<String> count = METRIC_DEFS.get(metricDefinition);
      if (count != null) {
        if (count.remove(subAlarmId) && count.isEmpty()) {
           METRIC_DEFS.remove(metricDefinition);
        }
      }
    }
  }

  @Override
  @SuppressWarnings("rawtypes")
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    LOG = LoggerFactory.getLogger(Logging.categoryFor(getClass(), context));
    LOG.info("Preparing");
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
            addMetricDef(subAlarmMetricDef.getMetricDefinition(), subAlarmMetricDef.getSubAlarmId());
          }
          // Iterate again to ensure we only emit each metricDef once
          for (MetricDefinition metricDef : METRIC_DEFS.keySet())
            collector.emit(new Values(metricDef, null));
        }
      }
    }
  }

  private void addMetricDef(MetricDefinition metricDef, String subAlarmId) {
    List<String> subAlarmIds = METRIC_DEFS.get(metricDef);
    if (subAlarmIds == null) {
      subAlarmIds = new LinkedList<>();
      METRIC_DEFS.put(metricDef, subAlarmIds);
    }
    else if (subAlarmIds.contains(subAlarmId))
      return; // Make sure it only gets added once. Multiple bolts process the same AlarmCreatedEvent
    subAlarmIds.add(subAlarmId);
  }

  /**
   * Only use for testing.
   */
  static void clearMetricDefinitions() {
      METRIC_DEFS.clear();
  }

  /**
   * Only use for testing.
   */
  static int sizeMetricDefinitions() {
      return METRIC_DEFS.size();
  }
}
