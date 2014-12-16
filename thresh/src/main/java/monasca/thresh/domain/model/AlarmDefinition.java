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

import monasca.common.model.alarm.AlarmExpression;
import monasca.common.model.alarm.AlarmSubExpression;
import monasca.common.model.domain.common.AbstractEntity;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Defines the "policy" for creating alarms
 */
public class AlarmDefinition extends AbstractEntity {
  private String tenantId;
  private String name;
  private String description;
  private AlarmExpression expression;
  private List<String> matchBy = new ArrayList<>();
  private boolean actionsEnabled = true;
  private String severity;
  private List<SubExpression> subExpressions = new ArrayList<>();

  public AlarmDefinition() {
  }

  public AlarmDefinition(String id, String tenantId, String name, String description,
      AlarmExpression expression, String severity, boolean actionsEnabled, List<SubExpression> subExpressions, List<String> matchBy) {
    this.id = id;
    this.tenantId = tenantId;
    this.name = name;
    this.description = description;
    this.expression = expression;
    this.severity = severity;
    this.actionsEnabled = actionsEnabled;
    this.subExpressions = subExpressions;
    this.setMatchBy(matchBy);
  }

  public AlarmDefinition(String tenantId, String name, String description,
      AlarmExpression expression, String severity, boolean actionsEnabled, List<String> matchBy) {
    this(UUID.randomUUID().toString(), tenantId, name, description, expression, severity, actionsEnabled,
        new ArrayList<SubExpression>(expression.getSubExpressions().size()), matchBy);
    for (final AlarmSubExpression alarmSubExpression : this.expression.getSubExpressions()) {
      subExpressions.add(new SubExpression(UUID.randomUUID().toString(), alarmSubExpression));
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
    AlarmDefinition other = (AlarmDefinition) obj;
    if (!compareObjects(expression, other.expression)) {
      return false;
    }
    if (!compareObjects(name, other.name)) {
      return false;
    }
    if (!compareObjects(severity, other.severity)) {
      return false;
    }
    if (!compareObjects(description, other.description)) {
      return false;
    }
    if (actionsEnabled != other.actionsEnabled) {
      return false;
    }
    if (!compareObjects(tenantId, other.tenantId)) {
      return false;
    }
    if (!compareObjects(matchBy, other.matchBy)) {
      return false;
    }
    if (!compareObjects(subExpressions, other.subExpressions)) {
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

  public String getSeverity() {
    return severity;
  }

  public boolean isActionsEnabled() {
    return actionsEnabled;
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
    result = prime * result + ((description == null) ? 0 : description.hashCode());
    result = prime * result + ((severity == null) ? 0 : severity.hashCode());
    result = prime * result + (actionsEnabled ? 1783 : 0);
    result = prime * result + ((tenantId == null) ? 0 : tenantId.hashCode());
    result = prime * result + ((subExpressions == null) ? 0 : subExpressions.hashCode());
    result = prime * result + matchBy.hashCode();
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

  public void setSeverity(String severity) {
    this.severity = severity;
  }

  public void setActionsEnabled(boolean actionsEnabled) {
    this.actionsEnabled = actionsEnabled;
  }

  public void setTenantId(String tenantId) {
    this.tenantId = tenantId;
  }

  @Override
  public String toString() {
    final StringBuilder builder = new StringBuilder('[');
    for (final String name : matchBy) {
      if (builder.length() > 1) {
        builder.append(',');
      }
      builder.append(name);
    }
    builder.append(']');
    return String.format(
            "Alarm [id=%s. tenantId=%s, name=%s, description=%s, expression=%s, severity=%s, actionsEnabled=%s, subExpressions=%s, matchBy=%s]",
            id, tenantId, name, description, expression.getExpression(), severity, actionsEnabled,
            subExpressions, builder);
  }

  public List<String> getMatchBy() {
    return matchBy;
  }

  public void setMatchBy(List<String> matchBy) {
    if (matchBy == null) {
      this.matchBy = new ArrayList<>();
    }
    else {
      this.matchBy = matchBy;
    }
  }

  public List<SubExpression> getSubExpressions() {
    return subExpressions;
  }

  public void setSubExpressions(List<SubExpression> subExpressions) {
    this.subExpressions = subExpressions;
  }

  public boolean updateSubExpression(final String id, final AlarmSubExpression alarmSubExpression) {
    for (final SubExpression subExpression : this.subExpressions) {
      if (subExpression.getId().equals(id)) {
        subExpression.setAlarmSubExpression(alarmSubExpression);
        return true;
      }
    }
    return false;
  }
}
