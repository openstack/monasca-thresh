package com.hpcloud.mon.domain.service;

import com.hpcloud.mon.common.model.alarm.AlarmState;
import com.hpcloud.mon.domain.model.Alarm;

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
