/*
 * Copyright (c) 2014 Hewlett-Packard Development Company, L.P.
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

import com.hpcloud.mon.common.model.alarm.AlarmExpression;
import com.hpcloud.mon.common.model.alarm.AlarmState;
import com.hpcloud.mon.common.model.alarm.AlarmSubExpression;
import com.hpcloud.mon.domain.common.AbstractEntity;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An alarm comprised of sub-alarms.
 */
public class Alarm extends AbstractEntity {
  private String tenantId;
  private String name;
  private String description;
  private AlarmExpression expression;
  private Map<String, SubAlarm> subAlarms;
  private AlarmState state;
  private boolean actionsEnabled = true;
  private String stateChangeReason;

  public Alarm() {
  }

  public Alarm(String id, String tenantId, String name, String description,
      AlarmExpression expression, List<SubAlarm> subAlarms, AlarmState state, boolean actionsEnabled) {
    this.id = id;
    this.tenantId = tenantId;
    this.name = name;
    this.description = description;
    this.expression = expression;
    setSubAlarms(subAlarms);
    this.state = state;
    this.actionsEnabled = actionsEnabled;
  }

  static String buildStateChangeReason(AlarmState alarmState, List<String> subAlarmExpressions) {
    if (AlarmState.UNDETERMINED.equals(alarmState)) {
      return String.format("No data was present for the sub-alarms: %s", subAlarmExpressions);
    } else if (AlarmState.ALARM.equals(alarmState)) {
      return String.format("Thresholds were exceeded for the sub-alarms: %s", subAlarmExpressions);
    } else {
      return "The alarm threshold(s) have not been exceeded";
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!super.equals(obj)) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    Alarm other = (Alarm) obj;
    if (!compareObjects(expression, other.expression)) {
      return false;
    }
    if (!compareObjects(name, other.name)) {
      return false;
    }
    if (!compareObjects(description, other.description)) {
      return false;
    }
    if (state != other.state) {
      return false;
    }
    if (actionsEnabled != other.actionsEnabled) {
      return false;
    }
    if (!compareObjects(subAlarms, other.subAlarms)) {
      return false;
    }
    if (!compareObjects(tenantId, other.tenantId)) {
      return false;
    }
    return true;
  }

  private boolean compareObjects(final Object o1, final Object o2) {
    if (o1 == null) {
      if (o2 != null) {
        return false;
      }
    } else if (!o1.equals(o2)) {
      return false;
    }
    return true;
  }

  /**
   * Evaluates the {@code alarm}, updating the alarm's state if necessary and returning true if the
   * alarm's state changed, else false.
   */
  public boolean evaluate() {
    AlarmState initialState = state;
    List<String> unitializedSubAlarms = new ArrayList<String>();
    for (SubAlarm subAlarm : subAlarms.values()) {
      if (AlarmState.UNDETERMINED.equals(subAlarm.getState())) {
        unitializedSubAlarms.add(subAlarm.getExpression().toString());
      }
    }

    // Handle UNDETERMINED state
    if (!unitializedSubAlarms.isEmpty()) {
      if (AlarmState.UNDETERMINED.equals(initialState)) {
        return false;
      }
      state = AlarmState.UNDETERMINED;
      stateChangeReason = buildStateChangeReason(state, unitializedSubAlarms);
      return true;
    }

    Map<AlarmSubExpression, Boolean> subExpressionValues =
        new HashMap<AlarmSubExpression, Boolean>();
    for (SubAlarm subAlarm : subAlarms.values()) {
      subExpressionValues.put(subAlarm.getExpression(),
          AlarmState.ALARM.equals(subAlarm.getState()));
    }

    // Handle ALARM state
    if (expression.evaluate(subExpressionValues)) {
      if (AlarmState.ALARM.equals(initialState)) {
        return false;
      }

      List<String> subAlarmExpressions = new ArrayList<String>();
      for (SubAlarm subAlarm : subAlarms.values()) {
        if (AlarmState.ALARM.equals(subAlarm.getState())) {
          subAlarmExpressions.add(subAlarm.getExpression().toString());
        }
      }

      state = AlarmState.ALARM;
      stateChangeReason = buildStateChangeReason(state, subAlarmExpressions);
      return true;
    }

    if (AlarmState.OK.equals(initialState)) {
      return false;
    }
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

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public AlarmState getState() {
    return state;
  }

  public boolean isActionsEnabled() {
    return actionsEnabled;
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
    result = prime * result + (actionsEnabled ? 1783 : 0);
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

  public void setActionsEnabled(boolean actionsEnabled) {
    this.actionsEnabled = actionsEnabled;
  }

  public void setSubAlarms(List<SubAlarm> subAlarms) {
    this.subAlarms = new HashMap<String, SubAlarm>();
    for (SubAlarm subAlarm : subAlarms) {
      this.subAlarms.put(subAlarm.getId(), subAlarm);
    }
  }

  public void setTenantId(String tenantId) {
    this.tenantId = tenantId;
  }

  @Override
  public String toString() {
    return String.format(
        "Alarm [tenantId=%s, name=%s, description=%s, state=%s, actionsEnabled=%s]", tenantId,
        name, description, state, actionsEnabled);
  }

  public void updateSubAlarm(SubAlarm subAlarm) {
    subAlarms.put(subAlarm.getId(), subAlarm);
  }

  public boolean removeSubAlarmById(String toDeleteId) {
    return subAlarms.remove(toDeleteId) != null;
  }
}
