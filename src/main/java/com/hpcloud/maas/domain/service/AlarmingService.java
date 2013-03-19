package com.hpcloud.maas.domain.service;

import com.yammer.dropwizard.lifecycle.Managed;

/**
 * Services alarming related requests.
 * 
 * @author Jonathan Halterman
 */
public interface AlarmingService extends Managed {
  /**
   * Starts alarming for the criteria.
   */
  void startAlarmingFor(String tenantId, String primaryDimension);

  /**
   * Stops all alarming for the {@code tenantId}.
   */
  void stopAlarmingFor(String tenantId);

  /**
   * Stops alarming for the criteria.
   */
  void stopAlarmingFor(String tenantId, String primaryDimension);
}
