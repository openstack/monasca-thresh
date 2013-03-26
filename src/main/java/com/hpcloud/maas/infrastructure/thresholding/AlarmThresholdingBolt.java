package com.hpcloud.maas.infrastructure.thresholding;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import com.hpcloud.maas.common.event.AlarmDeletedEvent;
import com.hpcloud.maas.domain.model.Alarm;
import com.hpcloud.maas.domain.model.CompositeAlarm;
import com.hpcloud.maas.domain.service.AlarmDAO;
import com.hpcloud.maas.infrastructure.storm.Streams;
import com.hpcloud.util.Injector;

/**
 * Determines whether an alarm threshold has been exceeded.
 * 
 * <p>
 * Receives composite alarm state changes and events.
 * 
 * <ul>
 * <li>Input: String compositeAlarmId, Object alarm
 * <li>Input composite-alarm-events: String compositeAlarmId, String eventType
 * </ul>
 * 
 * @author Jonathan Halterman
 */
public class AlarmThresholdingBolt extends BaseRichBolt {
  private static final Logger LOG = LoggerFactory.getLogger(AlarmThresholdingBolt.class);
  private static final long serialVersionUID = -4126465124017857754L;

  private final Map<String, CompositeAlarm> compositeAlarms = new HashMap<String, CompositeAlarm>();
  private transient AlarmDAO alarmDAO;
  private TopologyContext context;
  private OutputCollector collector;

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
  }

  @Override
  public void execute(Tuple tuple) {
    if (Streams.DEFAULT_STREAM_ID.equals(tuple.getSourceStreamId())) {
      String compositeAlarmId = tuple.getString(0);
      CompositeAlarm compositeAlarm = getOrCreateCompositeAlarm(compositeAlarmId);
      if (compositeAlarm == null)
        return;

      Alarm alarm = (Alarm) tuple.getValue(1);
      LOG.debug("{} Received state change for composite alarm {}, alarm {}",
          context.getThisTaskId(), compositeAlarmId, alarm);
      evaluateThreshold(compositeAlarm, alarm);
    } else if (EventProcessingBolt.COMPOSITE_ALARM_EVENT_STREAM_ID.equals(tuple.getSourceStreamId())) {
      String compositeAlarmId = tuple.getString(0);
      String eventType = tuple.getString(1);

      LOG.debug("{} Received {} event for composite alarm {}", context.getThisTaskId(), eventType,
          compositeAlarmId);
      if (AlarmDeletedEvent.class.getSimpleName().equals(eventType))
        handleAlarmDeleted(compositeAlarmId);
    }
  }

  @Override
  @SuppressWarnings("rawtypes")
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    this.context = context;
    this.collector = collector;
    alarmDAO = Injector.getInstance(AlarmDAO.class);
  }

  void evaluateThreshold(CompositeAlarm compositeAlarm, Alarm alarm) {
    compositeAlarm.updateAlarm(alarm);
    if (compositeAlarm.evaluate()) {
      // Emit notification
      // Update persistent alarm state
    }
  }

  void handleAlarmDeleted(String compositeAlarmId) {
    compositeAlarms.remove(compositeAlarmId);
  }

  private CompositeAlarm getOrCreateCompositeAlarm(String compositeAlarmId) {
    CompositeAlarm compositeAlarm = compositeAlarms.get(compositeAlarmId);
    if (compositeAlarm == null) {
      compositeAlarm = alarmDAO.findByCompositeId(compositeAlarmId);
      if (compositeAlarm == null)
        LOG.error("Failed to locate composite alarm for id {}", compositeAlarmId);
      else {
        compositeAlarms.put(compositeAlarmId, compositeAlarm);
      }
    }

    return compositeAlarm;
  }
}
