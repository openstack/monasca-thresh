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
}
