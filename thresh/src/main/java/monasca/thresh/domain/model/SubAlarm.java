/*
 * (C) Copyright 2014-2016 Hewlett Packard Enterprise Development LP
 * Copyright 2016 FUJITSU LIMITED
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package monasca.thresh.domain.model;

import monasca.common.model.alarm.AlarmState;
import monasca.common.model.alarm.AlarmSubExpression;
import monasca.common.model.domain.common.AbstractEntity;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Sub-alarm. Decorates an AlarmSubExpression.
 */
public class SubAlarm extends AbstractEntity implements Serializable {
  private static final long serialVersionUID = -3946708553723868124L;

  private String alarmId;
  private String alarmSubExpressionId;
  private AlarmSubExpression expression;
  private AlarmState state;
  private boolean noState;
  private List<Double> currentValues;

  public SubAlarm(String id, String alarmId, SubExpression expression) {
    this(id, alarmId, expression, SubAlarm.initialStateFromExpression(expression));
  }

  // Need this for kryo serialization/deserialization. Fixes a bug in default java
  // serialization/deserialization where id was not being set. See resources/storm.yaml
  // file for how to handle serialization/deserialization with kryo.
  public SubAlarm() {
  }

  public SubAlarm(String id, String alarmId, SubExpression expression, AlarmState state) {
    this.id = id;
    this.alarmId = alarmId;
    this.expression = expression.getAlarmSubExpression();
    this.alarmSubExpressionId = expression.getId();
    this.state = state;
    this.currentValues = new ArrayList<>();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    SubAlarm other = (SubAlarm) obj;
    if (alarmId == null) {
      if (other.alarmId != null) {
        return false;
      }
    } else if (!alarmId.equals(other.alarmId)) {
      return false;
    }
    if (alarmSubExpressionId == null) {
      if (other.alarmSubExpressionId != null) {
        return false;
      }
    } else if (!alarmSubExpressionId.equals(other.alarmSubExpressionId)) {
      return false;
    }
    if (expression == null) {
       if (other.expression != null) {
        return false;
       }
    } else if (!expression.equals(other.expression)) {
      return false;
    }
    if (state != other.state) {
      return false;
    }
    return true;
  }

  public String getAlarmId() {
    return alarmId;
  }

  public AlarmSubExpression getExpression() {
    return expression;
  }

  public void setExpression(AlarmSubExpression expression) {
    this.expression = expression;
  }

  public AlarmState getState() {
    return state;
  }

  public String getAlarmSubExpressionId() {
    return alarmSubExpressionId;
  }

  public List<Double> getCurrentValues() {
    return currentValues;
  }

  public void setCurrentValues(List<Double> currentValues) {
    this.currentValues = currentValues;
  }

  public void addCurrentValue(Double currentValue) {
    this.currentValues.add(currentValue);
  }

  public void clearCurrentValues() {
    this.currentValues.clear();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((alarmId == null) ? 0 : alarmId.hashCode());
    result = prime * result + ((expression == null) ? 0 : expression.hashCode());
    result = prime * result + ((alarmSubExpressionId == null) ? 0 : alarmSubExpressionId.hashCode());
    result = prime * result + ((state == null) ? 0 : state.hashCode());
    return result;
  }

  /**
   * Determines if {@link SubAlarm} is deterministic.
   *
   * Is {@link SubAlarm} deterministic or not depends
   * on underlying expression.
   *
   * @return true/false
   * @see AlarmSubExpression#isDeterministic()
   */
  public boolean isDeterministic() {
    return this.expression.isDeterministic();
  }

  public void setState(AlarmState state) {
    this.state = state;
  }

  public boolean isNoState() {
    return noState;
  }

  public void setNoState(boolean noState) {
    this.noState = noState;
  }

  @Override
  public String toString() {
    return String.format("SubAlarm [id=%s, alarmId=%s, alarmSubExpressionId=%s, expression=%s, " +
        "state=%s, noState=%s, currentValues:[", id,
        alarmId, alarmSubExpressionId, expression, state, noState) + currentValues + "]]";
  }

  /**
   * Determine if this SubAlarm and 'other' could reuse saved measurements. Only possible only
   * operator and/or threshold are the only properties from the expression that are different
   *
   * @param other SubAlarm to compare to
   * @return true if 'other' is "compatible", false otherwise
   */
  public boolean isCompatible(final AlarmSubExpression other) {
    if (!this.expression.getMetricDefinition().equals(other.getMetricDefinition())) {
      return false;
    }
    if (!this.expression.getFunction().equals(other.getFunction())) {
      return false;
    }
    if (this.expression.getPeriod() != other.getPeriod()) {
      return false;
    }
    if (this.expression.getPeriods() != other.getPeriods()) {
      return false;
    }
    // Operator and Threshold can vary
    return true;
  }

  public boolean canEvaluateAlarmImmediately() {
    switch (this.getExpression().getFunction()) {
      // LAST must be evaluated immediately
      case LAST:
       return true;
      // MIN never gets larger so if the operator is < or <=,
      // then they can be immediately evaluated
      case MIN:
        switch(this.getExpression().getOperator()) {
          case LT:
          case LTE:
            return true;
          default:
            return false;
        }
      // These two never get smaller so if the operator is > or >=,
      // then they can be immediately evaluated
      case MAX:
      case COUNT:
        switch(this.getExpression().getOperator()) {
          case GT:
          case GTE:
            return true;
          default:
            return false;
        }
      // SUM can increase on a positive measurement or decrease on a negative
      // AVG can't be computed until all the metrics have come in
      default:
        return false;
    }
  }

  public boolean canEvaluateOkImmediately() {
    switch (this.getExpression().getFunction()) {
      // LAST must be evaluated immediately
      case LAST:
        return true;
      // MIN never gets larger so if the operator is > or >=,
      // then they can be immediately evaluated
      case MIN:
        switch(this.getExpression().getOperator()) {
          case GT:
          case GTE:
            return true;
          default:
            return false;
        }
      // These two never get smaller so if the operator is < or <=,
      // then they can be immediately evaluated
      case MAX:
      case COUNT:
        switch(this.getExpression().getOperator()) {
          case LT:
          case LTE:
            return true;
          default:
            return false;
        }
      // SUM can increase on a positive measurement or decrease on a negative
      // AVG can't be computed until all the metrics have come in
      default:
        return false;
    }
  }

  public boolean onlyImmediateEvaluation() {
    switch (this.getExpression().getFunction()) {
      // LAST must be evaluated immediately
      case LAST:
        return true;
      // All others at this time can't be evaluated immediately
      default:
        return false;
    }
  }

  /**
   * Computes initial state for an {@link SubAlarm} based on
   * underlying {@link SubExpression}.
   *
   * @param expr sub expression
   *
   * @return initial state for an sub alarm
   *
   * @see SubExpression#getAlarmSubExpression()
   * @see #getDefaultState(boolean)
   */
  private static AlarmState initialStateFromExpression(final SubExpression expr) {
    final AlarmSubExpression subExpression = expr.getAlarmSubExpression();
    return getDefaultState(subExpression.isDeterministic());
  }

  /**
   * Returns default {@link AlarmState} for {@link AlarmSubExpression#DEFAULT_DETERMINISTIC}
   * value ({@value AlarmSubExpression#DEFAULT_DETERMINISTIC}).
   *
   * @return default state
   */
  public static AlarmState getDefaultState() {
    return getDefaultState(AlarmSubExpression.DEFAULT_DETERMINISTIC);
  }

  /**
   * Returns default {@link AlarmState} sub alarm should fallback to
   *
   * If <b>deterministic</b> is equal to {@link Boolean#TRUE}, {@link AlarmState#OK} is returned,
   * otherwise default state is {@link AlarmState#UNDETERMINED}.
   *
   * @param deterministic is sub alarm deterministic
   *
   * @return default state according to <b>deterministic</b> flag
   */
  public static AlarmState getDefaultState(final boolean deterministic) {
    return deterministic ? AlarmState.OK : AlarmState.UNDETERMINED;
  }

}
