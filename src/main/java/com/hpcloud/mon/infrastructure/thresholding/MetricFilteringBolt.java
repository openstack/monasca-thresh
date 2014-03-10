package com.hpcloud.mon.infrastructure.thresholding;

import io.dropwizard.db.DataSourceFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.fileupload.util.Streams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.hpcloud.mon.common.event.AlarmCreatedEvent;
import com.hpcloud.mon.common.event.AlarmDeletedEvent;
import com.hpcloud.mon.common.model.metric.MetricDefinition;
import com.hpcloud.mon.domain.service.MetricDefinitionDAO;
import com.hpcloud.mon.domain.service.SubAlarmDAO;
import com.hpcloud.mon.infrastructure.persistence.PersistenceModule;
import com.hpcloud.streaming.storm.Logging;
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
  private static final long serialVersionUID = 1096706128973976599L;
  private static final Map<MetricDefinition, Object> METRIC_DEFS = new ConcurrentHashMap<MetricDefinition, Object>();
  private static final Object SENTINAL = new Object();

  private transient Logger LOG;
  private final DataSourceFactory dbConfig;
  private transient MetricDefinitionDAO metricDefDAO;
  private OutputCollector collector;

  public MetricFilteringBolt(DataSourceFactory dbConfig) {
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

        LOG.debug("Received {} for {}", eventType, metricDefinition);
        if (EventProcessingBolt.METRIC_ALARM_EVENT_STREAM_ID.equals(tuple.getSourceStreamId())) {
          if (AlarmDeletedEvent.class.getSimpleName().equals(eventType))
            METRIC_DEFS.remove(metricDefinition);
        } else if (EventProcessingBolt.METRIC_SUB_ALARM_EVENT_STREAM_ID.equals(tuple.getSourceStreamId())) {
          if (AlarmCreatedEvent.class.getSimpleName().equals(eventType))
            METRIC_DEFS.put(metricDefinition, SENTINAL);
        }
      }
    } catch (Exception e) {
      LOG.error("Error processing tuple {}", tuple, e);
    } finally {
      collector.ack(tuple);
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
          for (MetricDefinition metricDef : metricDefDAO.findForAlarms())
            METRIC_DEFS.put(metricDef, SENTINAL);
          // Iterate again to ensure we only emit each metricDef once
          for (MetricDefinition metricDef : METRIC_DEFS.keySet())
            collector.emit(new Values(metricDef, null));
        }
      }
    }
  }
}
