package com.hpcloud.maas.app;

import javax.inject.Inject;

import com.hpcloud.maas.common.event.EndpointDeletedEvent;
import com.hpcloud.maas.domain.service.AlarmingService;
import com.hpcloud.messaging.Message;
import com.hpcloud.messaging.MessageHandler;

/**
 * Handles EndpointDeleted events.
 * 
 * @author Jonathan Halterman
 */
public class EndpointDeletedEventHandler implements MessageHandler<EndpointDeletedEvent> {
  private @Inject AlarmingService service;

  @Override
  public void handle(Message<EndpointDeletedEvent> msg) {
    service.stopAlarmingFor(msg.body.tenantId);
  }
}
