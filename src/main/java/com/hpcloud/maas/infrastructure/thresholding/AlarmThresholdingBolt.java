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
import com.hpcloud.maas.domain.model.SubAlarm;
import com.hpcloud.maas.domain.service.AlarmDAO;
import com.hpcloud.maas.infrastructure.storm.Streams;
import com.hpcloud.util.Injector;

/**
 * Determines whether an alarm threshold has been exceeded.
 * 
 * <p>
 * Receives alarm state changes and events.
 * 
 * <ul>
 * <li>Input: String alarmId, SubAlarm subAlarm
 * <li>Input alarm-events: String eventType, String alarmId
 * </ul>
 * 
 * @author Jonathan Halterman
 */
public class AlarmThresholdingBolt extends BaseRichBolt {
  private static final Logger LOG = LoggerFactory.getLogger(AlarmThresholdingBolt.class);
  private static final long serialVersionUID = -4126465124017857754L;

  private final Map<String, Alarm> alarms = new HashMap<String, Alarm>();
  private transient AlarmDAO alarmDAO;
  private TopologyContext context;
  private OutputCollector collector;

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
  }

  @Override
  public void execute(Tuple tuple) {
    try {
      if (Streams.DEFAULT_STREAM_ID.equals(tuple.getSourceStreamId())) {
        String alarmId = tuple.getString(0);
        Alarm alarm = getOrCreateAlarm(alarmId);
        if (alarm == null)
          return;

        SubAlarm subAlarm = (SubAlarm) tuple.getValue(1);
        evaluateThreshold(alarm, subAlarm);
      } else if (EventProcessingBolt.ALARM_EVENT_STREAM_ID.equals(tuple.getSourceStreamId())) {
        String eventType = tuple.getString(0);
        String alarmId = tuple.getString(1);

        if (AlarmDeletedEvent.class.getSimpleName().equals(eventType))
          handleAlarmDeleted(alarmId);
      }

      collector.ack(tuple);
    } catch (Exception e) {
      LOG.error("{} Error processing tuple {}", context.getThisTaskId(), tuple, e);
    }
  }

  @Override
  @SuppressWarnings("rawtypes")
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    this.context = context;
    this.collector = collector;
    alarmDAO = Injector.getInstance(AlarmDAO.class);
  }

  void evaluateThreshold(Alarm alarm, SubAlarm subAlarm) {
    LOG.debug("{} Received state change for {}", context.getThisTaskId(), subAlarm);
    alarm.updateSubAlarm(subAlarm);
    if (alarm.evaluate()) {
      alarmDAO.updateState(alarm.getState());
      // Emit notification
    }
  }

  void handleAlarmDeleted(String alarmId) {
    LOG.debug("{} Received AlarmDeletedEvent for alarm id {}", context.getThisTaskId(), alarmId);
    alarms.remove(alarmId);
  }

  private Alarm getOrCreateAlarm(String alarmId) {
    Alarm alarm = alarms.get(alarmId);
    if (alarm == null) {
      alarm = alarmDAO.findById(alarmId);
      if (alarm == null)
        LOG.error("{} Failed to locate alarm for id {}", context.getThisTaskId(), alarmId);
      else {
        alarms.put(alarmId, alarm);
      }
    }

    return alarm;
  }
}
