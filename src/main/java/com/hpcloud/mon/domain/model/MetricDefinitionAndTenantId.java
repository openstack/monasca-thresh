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

package com.hpcloud.mon.domain.model;

import com.hpcloud.mon.common.model.metric.MetricDefinition;

import java.io.Serializable;

public class MetricDefinitionAndTenantId implements Serializable {

  private static final long serialVersionUID = -4224596705186481749L;

  public MetricDefinition metricDefinition;
  public String tenantId;

  public MetricDefinitionAndTenantId(MetricDefinition metricDefinition, String tenantId) {
    this.metricDefinition = metricDefinition;
    this.tenantId = tenantId;
  }

  @Override
  public int hashCode() {
    int result = 0;
    if (this.metricDefinition != null) {
      result += this.metricDefinition.hashCode();
    }
    if (this.tenantId != null) {
      result = result * 31 + this.tenantId.hashCode();
    }
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final MetricDefinitionAndTenantId other = (MetricDefinitionAndTenantId) obj;

    if (!compareObjects(this.tenantId, other.tenantId)) {
      return false;
    }
    if (!compareObjects(this.metricDefinition, other.metricDefinition)) {
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

  @Override
  public String toString() {
    return String.format("MetricDefinitionAndTenantId tenantId=%s metricDefinition=%s",
        this.tenantId, this.metricDefinition);
  }
}
