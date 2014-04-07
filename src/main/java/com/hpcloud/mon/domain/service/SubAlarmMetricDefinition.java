package com.hpcloud.mon.domain.service;

import com.hpcloud.mon.common.model.metric.MetricDefinition;

public class SubAlarmMetricDefinition {
    private final String subAlarmId;
    private final MetricDefinition metricDefinition;

    public SubAlarmMetricDefinition(String subAlarmId,
            MetricDefinition metricDefinition) {
        this.subAlarmId = subAlarmId;
        this.metricDefinition = metricDefinition;
    }

    public String getSubAlarmId() {
        return subAlarmId;
    }

    public MetricDefinition getMetricDefinition() {
        return metricDefinition;
    }
}
