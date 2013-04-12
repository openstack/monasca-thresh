package com.hpcloud.maas.domain.model;

import java.util.ArrayList;
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
  private String stateChangeReason;

  public Alarm() {
  }

  public Alarm(String id, String tenantId, String name, AlarmExpression expression,
      List<SubAlarm> subAlarms, AlarmState state) {
    this.id = id;
    this.tenantId = tenantId;
    this.name = name;
    this.expression = expression;
    setSubAlarms(subAlarms);
    this.state = state;
  }

  static String buildStateChangeReason(AlarmState alarmState, List<String> subAlarmExpressions) {
    if (AlarmState.UNDETERMINED.equals(alarmState))
      return String.format("No data was present for the sub-alarms: %s", subAlarmExpressions);
    else if (AlarmState.ALARM.equals(alarmState))
      return String.format("Thresholds were exceeded for the sub-alarms: %s", subAlarmExpressions);
    else
      return "The alarm threshold(s) have not been exceeded";
  }

  /**
   * Evaluates the {@code alarm}, updating the alarm's state if necessary and returning true if the
   * alarm's state changed, else false.
   */
  public boolean evaluate() {
    AlarmState initialState = state;
    List<String> subAlarmExpressions = null;
    for (SubAlarm subAlarm : subAlarms.values()) {
      if (AlarmState.UNDETERMINED.equals(subAlarm.getState())) {
        if (subAlarmExpressions == null)
          subAlarmExpressions = new ArrayList<String>();
        subAlarmExpressions.add(subAlarm.getExpression().toString());
      }
    }

    // Handle UNDETERMINED state
    if (subAlarmExpressions != null) {
      if (AlarmState.UNDETERMINED.equals(initialState))
        return false;
      state = AlarmState.UNDETERMINED;
      stateChangeReason = buildStateChangeReason(state, subAlarmExpressions);
      return true;
    }

    Map<AlarmSubExpression, Boolean> subExpressionValues = new HashMap<AlarmSubExpression, Boolean>();
    for (SubAlarm subAlarm : subAlarms.values())
      subExpressionValues.put(subAlarm.getExpression(),
          AlarmState.ALARM.equals(subAlarm.getState()));

    // Handle ALARM state
    if (expression.evaluate(subExpressionValues)) {
      if (AlarmState.ALARM.equals(initialState))
        return false;

      subAlarmExpressions = new ArrayList<String>();
      for (SubAlarm subAlarm : subAlarms.values())
        if (AlarmState.ALARM.equals(subAlarm.getState()))
          subAlarmExpressions.add(subAlarm.getExpression().toString());

      state = AlarmState.ALARM;
      stateChangeReason = buildStateChangeReason(state, subAlarmExpressions);
      return true;
    }

    if (AlarmState.OK.equals(initialState))
      return false;
    state = AlarmState.OK;
    stateChangeReason = buildStateChangeReason(state, null);
    return true;
  }

  public AlarmExpression getAlarmExpression() {
    return expression;
  }

  public String getName() {
    return name;
  }

  public AlarmState getState() {
    return state;
  }

  public String getStateChangeReason() {
    return stateChangeReason;
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

  public void setExpression(String expression) {
    this.expression = AlarmExpression.of(expression);
  }

  public void setId(String id) {
    this.id = id;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setState(AlarmState state) {
    this.state = state;
  }

  public void setSubAlarms(List<SubAlarm> subAlarms) {
    this.subAlarms = new HashMap<String, SubAlarm>();
    for (SubAlarm subAlarm : subAlarms)
      this.subAlarms.put(subAlarm.getId(), subAlarm);
  }

  public void setTenantId(String tenantId) {
    this.tenantId = tenantId;
  }

  @Override
  public String toString() {
    return String.format("Alarm [tenantId=%s, name=%s, state=%s]", tenantId, name, state);
  }

  public void updateSubAlarm(SubAlarm subAlarm) {
    subAlarms.put(subAlarm.getId(), subAlarm);
  }
}
