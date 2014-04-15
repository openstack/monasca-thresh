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
        result = prime * result + ((metricDefinitionAndTenantId == null) ? 0 : metricDefinitionAndTenantId.hashCode());
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
        SubAlarmMetricDefinition other = (SubAlarmMetricDefinition) obj;
        return compareObjects(subAlarmId, other.subAlarmId) &&
               compareObjects(metricDefinitionAndTenantId, other.metricDefinitionAndTenantId);
    }

    private boolean compareObjects(final Object o1, final Object o2) {
        if (o1 == o2)
            return true;
        if (o1 == null)
            return false;
        return o1.equals(o2);
    }

    @Override
    public String toString() {
        return String.format("SubAlarmMetricDefinition subAlarmId=%s metricDefinitionAndTenantId=%s", subAlarmId,
                metricDefinitionAndTenantId);
    }
}
