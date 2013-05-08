package com.hpcloud.maas.domain.model;

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

  public SubAlarm(String id, String alarmId, AlarmSubExpression expression) {
    this(id, alarmId, expression, AlarmState.UNDETERMINED);
  }

  public SubAlarm(String id, String alarmId, AlarmSubExpression expression, AlarmState state) {
    this.id = id;
    this.alarmId = alarmId;
    this.expression = expression;
    this.state = state;
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

  public void setState(AlarmState state) {
    this.state = state;
  }

  @Override
  public String toString() {
    return String.format("SubAlarm [id=%s, expression=%s, state=%s]", id, expression, state);
  }
}
