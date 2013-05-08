package com.hpcloud.maas.infrastructure.thresholding;

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

import com.hpcloud.maas.common.event.AlarmCreatedEvent;
import com.hpcloud.maas.common.event.AlarmDeletedEvent;
import com.hpcloud.maas.common.model.metric.MetricDefinition;
import com.hpcloud.maas.domain.service.MetricDefinitionDAO;
import com.hpcloud.maas.domain.service.SubAlarmDAO;
import com.hpcloud.maas.infrastructure.persistence.PersistenceModule;
import com.hpcloud.maas.infrastructure.storm.Streams;
import com.hpcloud.persistence.DatabaseConfiguration;
import com.hpcloud.util.Injector;

/**
 * Filters metrics for which there is no associated alarm and forwards metrics for which there is an
 * alarm. Receives metric alarm and metric sub-alarm events to update metric definitions.
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
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(MetricFilteringBolt.class);
  private static final Map<MetricDefinition, Object> METRIC_DEFS = new ConcurrentHashMap<MetricDefinition, Object>();
  private static final Object SENTINAL = new Object();

  private final DatabaseConfiguration dbConfig;
  private transient MetricDefinitionDAO metricDefDAO;
  private TopologyContext context;
  private OutputCollector collector;

  public MetricFilteringBolt(DatabaseConfiguration dbConfig) {
    this.dbConfig = dbConfig;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("metricDefinition", "metric"));
  }

  @Override
  public void execute(Tuple tuple) {
    try {
      if (Streams.DEFAULT_STREAM_ID.equals(tuple.getSourceStreamId())) {
        MetricDefinition metricDef = (MetricDefinition) tuple.getValue(0);

        if (METRIC_DEFS.containsKey(metricDef))
          collector.emit(tuple, tuple.getValues());
      } else {
        String eventType = tuple.getString(0);
        MetricDefinition metricDefinition = (MetricDefinition) tuple.getValue(1);

        if (EventProcessingBolt.METRIC_ALARM_EVENT_STREAM_ID.equals(tuple.getSourceStreamId())) {
          if (AlarmDeletedEvent.class.getSimpleName().equals(eventType)) {
            LOG.debug("{} Received AlarmDeletedEvent for {}", context.getThisTaskId(),
                metricDefinition);
            METRIC_DEFS.remove(metricDefinition);
          }
        } else if (EventProcessingBolt.METRIC_SUB_ALARM_EVENT_STREAM_ID.equals(tuple.getSourceStreamId())) {
          if (AlarmCreatedEvent.class.getSimpleName().equals(eventType)) {
            LOG.debug("{} Received AlarmCreatedEvent for {}", context.getThisTaskId(),
                metricDefinition);
            METRIC_DEFS.put(metricDefinition, SENTINAL);
          }
        }
      }
    } catch (Exception e) {
      LOG.error("{} Error processing tuple {}", context.getThisTaskId(), tuple, e);
    } finally {
      collector.ack(tuple);
    }
  }

  @Override
  @SuppressWarnings("rawtypes")
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    LOG.info("{} Preparing {}", context.getThisTaskId(), context.getThisComponentId());
    this.context = context;
    this.collector = collector;

    if (metricDefDAO == null) {
      Injector.registerIfNotBound(SubAlarmDAO.class, new PersistenceModule(dbConfig));
      metricDefDAO = Injector.getInstance(MetricDefinitionDAO.class);
    }

    // DCL
    if (METRIC_DEFS.isEmpty()) {
      synchronized (SENTINAL) {
        if (METRIC_DEFS.isEmpty()) {
          for (MetricDefinition metricDef : metricDefDAO.findForAlarms())
            METRIC_DEFS.put(metricDef, SENTINAL);
        }
      }
    }
  }
}
