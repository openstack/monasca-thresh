package com.hpcloud.maas.domain.model;

import com.hpcloud.maas.common.model.alarm.AlarmState;
import com.hpcloud.maas.common.model.alarm.AlarmSubExpression;
import com.hpcloud.maas.domain.common.AbstractEntity;

/**
 * Sub-alarm. Decorates an AlarmSubExpression.
 * 
 * @author Jonathan Halterman
 */
public class SubAlarm extends AbstractEntity {
  private String alarmId;
  private AlarmSubExpression expression;
  private AlarmState state;

  public SubAlarm(String alarmId, String id, AlarmSubExpression expression) {
    this.alarmId = alarmId;
    this.id = id;
    this.expression = expression;
    this.state = AlarmState.UNDETERMINED;
  }

  public SubAlarm(String alarmId, String id, AlarmSubExpression expression, AlarmState state) {
    this.alarmId = alarmId;
    this.id = id;
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
    return String.format("SubAlarm [alarmId=%s, id=%s, expression=%s, state=%s]", alarmId, id,
        expression, state);
  }
}
