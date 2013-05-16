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

import com.hpcloud.maas.ThresholdingConfiguration;
import com.hpcloud.maas.common.event.AlarmDeletedEvent;
import com.hpcloud.maas.common.model.alarm.AlarmState;
import com.hpcloud.maas.domain.model.Alarm;
import com.hpcloud.maas.domain.model.AlarmStateTransitionEvent;
import com.hpcloud.maas.domain.model.SubAlarm;
import com.hpcloud.maas.domain.service.AlarmDAO;
import com.hpcloud.maas.infrastructure.messaging.MessagingModule;
import com.hpcloud.maas.infrastructure.persistence.PersistenceModule;
import com.hpcloud.maas.infrastructure.storm.Streams;
import com.hpcloud.messaging.rabbitmq.RabbitMQConfiguration;
import com.hpcloud.messaging.rabbitmq.RabbitMQService;
import com.hpcloud.persistence.DatabaseConfiguration;
import com.hpcloud.supervision.SupervisionModule;
import com.hpcloud.util.Injector;
import com.hpcloud.util.Serialization;

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

  private final DatabaseConfiguration dbConfig;
  private final RabbitMQConfiguration rabbitConfig;
  private final Map<String, Alarm> alarms = new HashMap<String, Alarm>();
  private String alertExchange;
  private String alertRoutingKey;
  private transient AlarmDAO alarmDAO;
  private transient RabbitMQService rabbitService;
  private TopologyContext ctx;
  private OutputCollector collector;

  public AlarmThresholdingBolt(DatabaseConfiguration dbConfig, RabbitMQConfiguration rabbitConfig) {
    this.dbConfig = dbConfig;
    this.rabbitConfig = rabbitConfig;
  }

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
    } catch (Exception e) {
      LOG.error("{} Error processing tuple {}", ctx.getThisTaskId(), tuple, e);
    } finally {
      collector.ack(tuple);
    }
  }

  @Override
  @SuppressWarnings("rawtypes")
  public void prepare(Map config, TopologyContext context, OutputCollector collector) {
    LOG.info("{} Preparing {}", context.getThisTaskId(), context.getThisComponentId());
    this.ctx = context;
    this.collector = collector;
    alertExchange = (String) config.get(ThresholdingConfiguration.ALERTS_EXCHANGE);
    alertRoutingKey = (String) config.get(ThresholdingConfiguration.ALERTS_ROUTING_KEY);

    Injector.registerIfNotBound(AlarmDAO.class, new PersistenceModule(dbConfig));
    Injector.registerIfNotBound(RabbitMQService.class, new MessagingModule(rabbitConfig),
        new SupervisionModule());

    alarmDAO = Injector.getInstance(AlarmDAO.class);
    rabbitService = Injector.getInstance(RabbitMQService.class);
  }

  void evaluateThreshold(Alarm alarm, SubAlarm subAlarm) {
    LOG.debug("{} Received state change for {}", ctx.getThisTaskId(), subAlarm);
    alarm.updateSubAlarm(subAlarm);

    AlarmState initialState = alarm.getState();
    if (alarm.evaluate()) {
      alarmDAO.updateState(alarm.getId(), alarm.getState());

      if (AlarmState.ALARM.equals(alarm.getState())) {
        LOG.debug("{} ALARM triggered for {}", ctx.getThisTaskId(), alarm);
        AlarmStateTransitionEvent event = new AlarmStateTransitionEvent(alarm.getTenantId(),
            alarm.getId(), alarm.getName(), initialState, alarm.getState(),
            alarm.getStateChangeReason(), System.currentTimeMillis() / 1000);
        rabbitService.send(alertExchange, alertRoutingKey, Serialization.toJson(event));
      } else
        LOG.debug("{} State changed for {}", ctx.getThisTaskId(), alarm);
    }
  }

  void handleAlarmDeleted(String alarmId) {
    LOG.debug("{} Received AlarmDeletedEvent for alarm id {}", ctx.getThisTaskId(), alarmId);
    alarms.remove(alarmId);
  }

  String buildStateChangeReason() {
    return null;
  }

  private Alarm getOrCreateAlarm(String alarmId) {
    Alarm alarm = alarms.get(alarmId);
    if (alarm == null) {
      alarm = alarmDAO.findById(alarmId);
      if (alarm == null)
        LOG.error("{} Failed to locate alarm for id {}", ctx.getThisTaskId(), alarmId);
      else {
        alarms.put(alarmId, alarm);
      }
    }

    return alarm;
  }
}
