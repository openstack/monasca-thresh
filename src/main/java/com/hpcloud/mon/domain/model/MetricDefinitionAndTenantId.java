package com.hpcloud.mon.domain.model;

import java.io.Serializable;

import com.hpcloud.mon.common.model.metric.MetricDefinition;

public class MetricDefinitionAndTenantId implements Serializable {

    private static final long serialVersionUID = -4224596705186481749L;

    public MetricDefinition metricDefinition;
    public String tenantId;

    public MetricDefinitionAndTenantId(MetricDefinition metricDefinition,
            String tenantId) {
        this.metricDefinition = metricDefinition;
        this.tenantId = tenantId;
    }

    @Override
    public int hashCode() {
        int result = 0;
        if (this.metricDefinition != null)
            result += this.metricDefinition.hashCode();
        if (this.tenantId != null)
            result = result * 31 + this.tenantId.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        final MetricDefinitionAndTenantId other = (MetricDefinitionAndTenantId) obj;
        
        if (!compareObjects(this.tenantId, other.tenantId))
             return false;
        if (!compareObjects(this.metricDefinition, other.metricDefinition))
            return false;
        return true;
    }

    private boolean compareObjects(final Object o1,
                                   final Object o2) {
      if (o1 == null) {
         if (o2 != null)
            return false;
      } else if (!o1.equals(o2))
          return false;
      return true;
    }

    @Override
    public String toString() {
        return String.format("MetricDefinitionAndTenantId tenantId=%s metricDefinition=%s", this.tenantId, this.metricDefinition);
    }
}
