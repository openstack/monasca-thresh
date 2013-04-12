package com.hpcloud.maas.domain.service;

import com.hpcloud.maas.common.model.alarm.AlarmState;
import com.hpcloud.maas.domain.model.Alarm;

/**
 * Alarm DAO.
 * 
 * @author Jonathan Halterman
 */
public interface AlarmDAO {
  /** Finds and returns the Alarm for the {@code id}. */
  Alarm findById(String id);

  /** Updates the alarm state. */
  void updateState(String id, AlarmState state);
}
