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
import com.hpcloud.mon.domain.common.AbstractEntity;

import java.util.ArrayList;
import java.util.List;

/**
 * An alarm comprised of sub-alarms.
 */
public class AlarmDefinition extends AbstractEntity {
  private String tenantId;
  private String name;
  private String description;
  private AlarmExpression expression;
  private List<String> matchBy = new ArrayList<>();
  private boolean actionsEnabled = true;

  public AlarmDefinition() {
  }

  public AlarmDefinition(String id, String tenantId, String name, String description,
      AlarmExpression expression, boolean actionsEnabled, List<String> matchBy) {
    this.id = id;
    this.tenantId = tenantId;
    this.name = name;
    this.description = description;
    this.expression = expression;
    this.actionsEnabled = actionsEnabled;
    this.setMatchBy(matchBy);
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
    result = prime * result + (actionsEnabled ? 1783 : 0);
    result = prime * result + ((tenantId == null) ? 0 : tenantId.hashCode());
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
        "Alarm [tenantId=%s, name=%s, description=%s, actionsEnabled=%s, matchBy=%s]", tenantId,
        name, description, actionsEnabled, builder);
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
}
