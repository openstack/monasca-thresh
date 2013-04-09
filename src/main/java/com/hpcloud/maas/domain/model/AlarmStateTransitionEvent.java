package com.hpcloud.maas.domain.model;

import com.fasterxml.jackson.annotation.JsonRootName;
import com.hpcloud.maas.common.model.alarm.AlarmState;

/**
 * Represents an alarm state transition having occurred.
 * 
 * @author Jonathan Halterman
 */
@JsonRootName(value = "alarm-transitioned")
public class AlarmStateTransitionEvent {
  public String tenantId;
  public String alarmId;
  public String alarmName;
  public AlarmState oldState;
  public AlarmState newState;
  public String stateChangeReason;
  public long timestamp;

  public AlarmStateTransitionEvent() {
  }

  public AlarmStateTransitionEvent(String tenantId, String alarmId, String alarmName,
      AlarmState oldState, AlarmState newState, String stateChangeReason, long timestamp) {
    this.tenantId = tenantId;
    this.alarmId = alarmId;
    this.alarmName = alarmName;
    this.oldState = oldState;
    this.newState = newState;
    this.stateChangeReason = stateChangeReason;
    this.timestamp = timestamp;
  }
}
