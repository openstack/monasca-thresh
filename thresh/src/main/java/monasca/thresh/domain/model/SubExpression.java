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

import java.io.Serializable;

import monasca.common.model.alarm.AlarmSubExpression;
import monasca.common.model.domain.common.AbstractEntity;

public class SubExpression extends AbstractEntity implements Serializable {
  private static final long serialVersionUID = -5485473840376995997L;

  private AlarmSubExpression alarmSubExpression;

  public SubExpression() {
  }

  public SubExpression(String id, AlarmSubExpression alarmSubExpression) {
    this.id = id;
    this.alarmSubExpression = alarmSubExpression;
  }

  public AlarmSubExpression getAlarmSubExpression() {
    return alarmSubExpression;
  }

  public void setAlarmSubExpression(AlarmSubExpression alarmSubExpression) {
    this.alarmSubExpression = alarmSubExpression;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((alarmSubExpression == null) ? 0 : alarmSubExpression.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (!super.equals(obj))
      return false;
    if (getClass() != obj.getClass())
      return false;
    SubExpression other = (SubExpression) obj;
    if (alarmSubExpression == null) {
      if (other.alarmSubExpression != null)
        return false;
    } else if (!alarmSubExpression.equals(other.alarmSubExpression))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return String.format("SubExpression [id=%s. alarmSubExpression=%s]", id, alarmSubExpression);
  }
}
