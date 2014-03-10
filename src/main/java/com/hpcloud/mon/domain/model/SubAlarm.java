package com.hpcloud.mon.domain.model;

import java.io.Serializable;

import com.hpcloud.maas.common.model.alarm.AlarmState;
import com.hpcloud.maas.common.model.alarm.AlarmSubExpression;
import com.hpcloud.maas.domain.common.AbstractEntity;

/**
 * Sub-alarm. Decorates an AlarmSubExpression.
 * 
 * @author Jonathan Halterman
 */
public class SubAlarm extends AbstractEntity implements Serializable {
  private static final long serialVersionUID = -3946708553723868124L;

  private final String alarmId;
  private final AlarmSubExpression expression;
  private AlarmState state;
  /** Whether metrics for this sub-alarm are received sporadically. */
  private boolean sporadicMetric;

  public SubAlarm(String id, String alarmId, AlarmSubExpression expression) {
    this(id, alarmId, expression, AlarmState.UNDETERMINED);
  }

  public SubAlarm(String id, String alarmId, AlarmSubExpression expression, AlarmState state) {
    this.id = id;
    this.alarmId = alarmId;
    this.expression = expression;
    this.state = state;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (!super.equals(obj))
      return false;
    if (getClass() != obj.getClass())
      return false;
    SubAlarm other = (SubAlarm) obj;
    if (alarmId == null) {
      if (other.alarmId != null)
        return false;
    } else if (!alarmId.equals(other.alarmId))
      return false;
    if (expression == null) {
      if (other.expression != null)
        return false;
    } else if (!expression.equals(other.expression))
      return false;
    if (state != other.state)
      return false;
    return true;
  }

  public String getAlarmId() {
    return alarmId;
  }

  public AlarmSubExpression getExpression() {
    return expression;
  }

  public AlarmState getState() {
    return state;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((alarmId == null) ? 0 : alarmId.hashCode());
    result = prime * result + ((expression == null) ? 0 : expression.hashCode());
    result = prime * result + ((state == null) ? 0 : state.hashCode());
    return result;
  }

  public boolean isSporadicMetric() {
    return sporadicMetric;
  }

  public void setSporadicMetric(boolean sporadicMetric) {
    this.sporadicMetric = sporadicMetric;
  }

  public void setState(AlarmState state) {
    this.state = state;
  }

  @Override
  public String toString() {
    return String.format("SubAlarm [id=%s, alarmId=%s, expression=%s, state=%s]", id, alarmId,
        expression, state);
  }
}
