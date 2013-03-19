package com.hpcloud.maas.app;

import javax.inject.Inject;

import com.hpcloud.maas.common.event.AlarmDeletedEvent;
import com.hpcloud.maas.domain.service.AlarmingService;
import com.hpcloud.messaging.Message;
import com.hpcloud.messaging.MessageHandler;

/**
 * Handles AlarmDeleted events.
 * 
 * @author Jonathan Halterman
 */
public class AlarmDeletedEventHandler implements MessageHandler<AlarmDeletedEvent> {
  private @Inject AlarmingService service;

  @Override
  public void handle(Message<AlarmDeletedEvent> msg) {
    service.stopAlarmingFor(msg.body.tenantId, msg.body.primaryDimension);
  }
}
