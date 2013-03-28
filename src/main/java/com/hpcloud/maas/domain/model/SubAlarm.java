package com.hpcloud.maas.domain.model;

import com.hpcloud.maas.common.model.alarm.AlarmState;
import com.hpcloud.maas.common.model.alarm.AlarmSubExpression;

/**
 * Sub-alarm. Decorates an AlarmSubExpression.
 * 
 * @author Jonathan Halterman
 */
public class SubAlarm {
  private String compositeId;
  private AlarmSubExpression expression;
  private AlarmState state;

  public SubAlarm(String compositeId, AlarmSubExpression expression) {
    this.compositeId = compositeId;
    this.expression = expression;
    this.state = AlarmState.UNDETERMINED;
  }

  public SubAlarm(String compositeId, AlarmSubExpression expression, AlarmState state) {
    this.compositeId = compositeId;
    this.expression = expression;
    this.state = state;
  }

  public String getCompositeId() {
    return compositeId;
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
}
