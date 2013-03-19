package com.hpcloud.maas.infrastructure.alarming;

import javax.inject.Inject;
import javax.inject.Named;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hpcloud.maas.ThresholdingConfiguration;
import com.hpcloud.maas.domain.service.AlarmingService;
import com.hpcloud.messaging.rabbitmq.RabbitMQConfiguration;

/**
 * Alarming service implementation.
 * 
 * @author Jonathan Halterman
 */
public class AlarmingServiceImpl implements AlarmingService {
  private static final Logger LOG = LoggerFactory.getLogger(AlarmingServiceImpl.class);

  private final ThresholdingConfiguration config;

  @Inject
  public AlarmingServiceImpl(ThresholdingConfiguration config,
      @Named("internal") RabbitMQConfiguration internalConfig,
      @Named("external") RabbitMQConfiguration externalConfig) {
    this.config = config;

  }

  @Override
  public void start() throws Exception {
    LOG.info("Starting alarming service");

  }

  @Override
  public void stop() throws Exception {
    LOG.info("Stopping alarming service");

  }

  @Override
  public void startAlarmingFor(String tenantId, String primaryDimension) {
  }

  @Override
  public void stopAlarmingFor(String tenantId) {
  }

  @Override
  public void stopAlarmingFor(String tenantId, String primaryDimension) {
  }
}
