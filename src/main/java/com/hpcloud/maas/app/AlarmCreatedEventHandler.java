package com.hpcloud.maas.app;

import javax.inject.Inject;

import com.hpcloud.maas.common.event.AlarmCreatedEvent;
import com.hpcloud.maas.domain.service.AlarmingService;
import com.hpcloud.messaging.Message;
import com.hpcloud.messaging.MessageHandler;

/**
 * Handles AlarmCreated events.
 * 
 * @author Jonathan Halterman
 */
public class AlarmCreatedEventHandler implements MessageHandler<AlarmCreatedEvent> {
  private @Inject AlarmingService service;

  @Override
  public void handle(Message<AlarmCreatedEvent> msg) {
   // service.startAlarmingFor(msg.body.tenantId, msg.body.primaryDimension);
  }
}
