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

/**
 * This class is used for routing
 * 
 * @author craigbr
 * 
 */
public class TenantIdAndMetricName implements Serializable {
  private static final long serialVersionUID = -2213662242536216424L;

  private final String tenantId;
  private final String metricName;

  public TenantIdAndMetricName(String tenantId, String metricName) {
    this.tenantId = tenantId;
    this.metricName = metricName;
  }

  public TenantIdAndMetricName(MetricDefinitionAndTenantId metricDefinitionAndTenantId) {
    this(metricDefinitionAndTenantId.tenantId, metricDefinitionAndTenantId.metricDefinition.name);
  }

  public String getTenantId() {
    return tenantId;
  }

  public String getMetricName() {
    return metricName;
  }

  @Override
  public int hashCode() {
    return tenantId.hashCode() + metricName.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof TenantIdAndMetricName)) {
      return false;
    }
    final TenantIdAndMetricName other = (TenantIdAndMetricName) obj;
    return this.metricName.equals(other.metricName) && this.tenantId.equals(other.tenantId);
  }

  @Override
  public String toString() {
    return String.format("TenantIdAndMetricName{tenantId=%s,metricName=%s}", this.tenantId,
        this.metricName);
  }
}
