package com.hpcloud.maas.domain.model;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.hpcloud.maas.common.model.alarm.AlarmState;
import com.hpcloud.maas.domain.common.AbstractEntity;

/**
 * An alarm composed of alarms according to an evaluatable expression.
 * 
 * @author Jonathan Halterman
 */
public class CompositeAlarm extends AbstractEntity {
  private String tenantId;
  private String name;
  private Map<String, Alarm> alarms;
  private AlarmState state;

  public CompositeAlarm(String id, String tenantId, String name, List<Alarm> newAlarms,
      AlarmState state) {
    this.id = id;
    this.tenantId = tenantId;
    this.name = name;
    alarms = new HashMap<String, Alarm>();
    for (Alarm alarm : newAlarms)
      alarms.put(alarm.getId(), alarm);
    this.state = state;
  }

  /**
   * Evaluates the {@code alarm}, updating the alarm's state if necessary and returning true if the
   * alarm's state changed, else false.
   */
  public boolean evaluate() {
    if (!AlarmState.UNDETERMINED.equals(state)) {
      for (Alarm alarm : alarms.values())
        if (AlarmState.UNDETERMINED.equals(alarm.getState())) {
          state = AlarmState.UNDETERMINED;
          return true;
        }
    }

    AlarmState initialState = state;

    // TODO Calculate threshold and update state if needed

    return false;
  }

  public Collection<Alarm> getAlarms() {
    return alarms.values();
  }

  public Alarm getAlarm(String alarmId) {
    return alarms.get(alarmId);
  }

  public String getName() {
    return name;
  }

  public AlarmState getState() {
    return state;
  }

  public String getTenantId() {
    return tenantId;
  }

  public void setAlarms(Map<String, Alarm> alarms) {
    this.alarms = alarms;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setState(AlarmState state) {
    this.state = state;
  }

  public void setTenantId(String tenantId) {
    this.tenantId = tenantId;
  }

  @Override
  public String toString() {
    return name;
  }

  public void updateAlarm(Alarm alarm) {
    alarms.put(alarm.getId(), alarm);
  }
}
