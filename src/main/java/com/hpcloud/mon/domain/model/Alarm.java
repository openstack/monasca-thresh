package com.hpcloud.mon.domain.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.hpcloud.mon.common.model.alarm.AlarmExpression;
import com.hpcloud.mon.common.model.alarm.AlarmState;
import com.hpcloud.mon.common.model.alarm.AlarmSubExpression;
import com.hpcloud.mon.domain.common.AbstractEntity;

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

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (!super.equals(obj))
      return false;
    if (getClass() != obj.getClass())
      return false;
    Alarm other = (Alarm) obj;
    if (expression == null) {
      if (other.expression != null)
        return false;
    } else if (!expression.equals(other.expression))
      return false;
    if (name == null) {
      if (other.name != null)
        return false;
    } else if (!name.equals(other.name))
      return false;
    if (state != other.state)
      return false;
    if (subAlarms == null) {
      if (other.subAlarms != null)
        return false;
    } else if (!subAlarms.equals(other.subAlarms))
      return false;
    if (tenantId == null) {
      if (other.tenantId != null)
        return false;
    } else if (!tenantId.equals(other.tenantId))
      return false;
    return true;
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

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((expression == null) ? 0 : expression.hashCode());
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    result = prime * result + ((state == null) ? 0 : state.hashCode());
    result = prime * result + ((subAlarms == null) ? 0 : subAlarms.hashCode());
    result = prime * result + ((tenantId == null) ? 0 : tenantId.hashCode());
    return result;
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
