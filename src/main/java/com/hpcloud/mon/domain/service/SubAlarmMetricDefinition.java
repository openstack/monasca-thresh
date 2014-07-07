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

package com.hpcloud.mon.domain.service;

import com.hpcloud.mon.domain.model.MetricDefinitionAndTenantId;

public class SubAlarmMetricDefinition {
  private final String subAlarmId;
  private final MetricDefinitionAndTenantId metricDefinitionAndTenantId;

  public SubAlarmMetricDefinition(String subAlarmId,
      MetricDefinitionAndTenantId metricDefinitionAndTenantId) {
    this.subAlarmId = subAlarmId;
    this.metricDefinitionAndTenantId = metricDefinitionAndTenantId;
  }

  public String getSubAlarmId() {
    return subAlarmId;
  }

  public MetricDefinitionAndTenantId getMetricDefinitionAndTenantId() {
    return metricDefinitionAndTenantId;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((subAlarmId == null) ? 0 : subAlarmId.hashCode());
    result =
        prime * result
            + ((metricDefinitionAndTenantId == null) ? 0 : metricDefinitionAndTenantId.hashCode());
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
    SubAlarmMetricDefinition other = (SubAlarmMetricDefinition) obj;
    return compareObjects(subAlarmId, other.subAlarmId)
        && compareObjects(metricDefinitionAndTenantId, other.metricDefinitionAndTenantId);
  }

  private boolean compareObjects(final Object o1, final Object o2) {
    if (o1 == o2) {
      return true;
    }
    if (o1 == null) {
      return false;
    }
    return o1.equals(o2);
  }

  @Override
  public String toString() {
    return String.format("SubAlarmMetricDefinition subAlarmId=%s metricDefinitionAndTenantId=%s",
        subAlarmId, metricDefinitionAndTenantId);
  }
}
