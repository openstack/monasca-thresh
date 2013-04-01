package com.hpcloud.maas.domain.model;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.hpcloud.maas.common.model.alarm.AlarmExpression;
import com.hpcloud.maas.common.model.alarm.AlarmState;
import com.hpcloud.maas.common.model.alarm.AlarmSubExpression;
import com.hpcloud.maas.domain.common.AbstractEntity;

/**
 * An alarm comprised of sub-alarms.
 * 
 * @author Jonathan Halterman
 */
public class Alarm extends AbstractEntity {
  private String tenantId;
  private String name;
  private AlarmExpression expression;
  private Map<String, SubAlarm> subAlarms;
  private AlarmState state;

  public Alarm(String id, String tenantId, String name, AlarmExpression expression,
      List<SubAlarm> subAlarms, AlarmState state) {
    this.id = id;
    this.tenantId = tenantId;
    this.name = name;
    this.expression = expression;
    this.subAlarms = new HashMap<String, SubAlarm>();
    for (SubAlarm subAlarm : subAlarms)
      this.subAlarms.put(subAlarm.getId(), subAlarm);
    this.state = state;
  }

  /**
   * Evaluates the {@code alarm}, updating the alarm's state if necessary and returning true if the
   * alarm's state changed, else false.
   */
  public boolean evaluate() {
    AlarmState initialState = state;
    for (SubAlarm subAlarm : subAlarms.values()) {
      if (AlarmState.UNDETERMINED.equals(subAlarm.getState())) {
        state = AlarmState.UNDETERMINED;
        return !AlarmState.UNDETERMINED.equals(initialState);
      }
    }

    Map<AlarmSubExpression, Boolean> subExpressionValues = new HashMap<AlarmSubExpression, Boolean>();
    for (SubAlarm subAlarm : subAlarms.values())
      subExpressionValues.put(subAlarm.getExpression(),
          AlarmState.ALARM.equals(subAlarm.getState()));

    if (expression.evaluate(subExpressionValues)) {
      if (AlarmState.ALARM.equals(initialState))
        return false;
      state = AlarmState.ALARM;
      return true;
    }

    if (AlarmState.OK.equals(initialState))
      return false;
    state = AlarmState.OK;
    return true;
  }

  public AlarmExpression getExpression() {
    return expression;
  }

  public String getName() {
    return name;
  }

  public AlarmState getState() {
    return state;
  }

  public SubAlarm getSubAlarm(String subAlarmId) {
    return subAlarms.get(subAlarmId);
  }

  public Collection<SubAlarm> getSubAlarms() {
    return subAlarms.values();
  }

  public String getTenantId() {
    return tenantId;
  }

  public void setExpression(AlarmExpression expression) {
    this.expression = expression;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setState(AlarmState state) {
    this.state = state;
  }

  public void setSubAlarms(Map<String, SubAlarm> subAlarms) {
    this.subAlarms = subAlarms;
  }

  public void setTenantId(String tenantId) {
    this.tenantId = tenantId;
  }

  @Override
  public String toString() {
    return String.format("Alarm [name=%s]", name);
  }

  public void updateSubAlarm(SubAlarm subAlarm) {
    subAlarms.put(subAlarm.getId(), subAlarm);
  }
}
