package com.hpcloud.mon.domain.model;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

public class MetricDefinitionAndTenantIdMatcher {
    final Map<String, Map<String, List<MetricDefinitionAndTenantId>>> byTenantId = new ConcurrentHashMap<>();

    public void add(MetricDefinitionAndTenantId metricDefinitionAndTenantId) {
        Map<String, List<MetricDefinitionAndTenantId>> byMetricName = byTenantId.get(metricDefinitionAndTenantId.tenantId);
        if (byMetricName == null) {
            byMetricName = new ConcurrentHashMap<>();
            byTenantId.put(metricDefinitionAndTenantId.tenantId, byMetricName);
        }
        List<MetricDefinitionAndTenantId> defsList = byMetricName.get(metricDefinitionAndTenantId.metricDefinition.name);
        if (defsList == null) {
            defsList = new LinkedList<>();
            byMetricName.put(metricDefinitionAndTenantId.metricDefinition.name, defsList);
        }
        defsList.add(metricDefinitionAndTenantId);
    }

    public boolean remove(MetricDefinitionAndTenantId metricDefinitionAndTenantId) {
        Map<String, List<MetricDefinitionAndTenantId>> byMetricName = byTenantId.get(metricDefinitionAndTenantId.tenantId);
        if (byMetricName == null) {
            return false;
        }
        List<MetricDefinitionAndTenantId> defsList = byMetricName.get(metricDefinitionAndTenantId.metricDefinition.name);
        if (defsList == null) {
            return false;
        }
        final boolean result = defsList.remove(metricDefinitionAndTenantId);
        if (result) {
            if (defsList.isEmpty()) {
                byMetricName.remove(metricDefinitionAndTenantId.metricDefinition.name);
                if (byMetricName.isEmpty())
                    byTenantId.remove(metricDefinitionAndTenantId.tenantId);
            }
        }
        return result;
    }

    public boolean match(final MetricDefinitionAndTenantId toMatch,
                         final List<MetricDefinitionAndTenantId> matches) {
        Map<String, List<MetricDefinitionAndTenantId>> byMetricName = byTenantId.get(toMatch.tenantId);
        if (byMetricName == null)
            return false;
        List<MetricDefinitionAndTenantId> defsList = byMetricName.get(toMatch.metricDefinition.name);
        if (defsList == null)
            return false;
        matches.clear();
        for (final MetricDefinitionAndTenantId existing : defsList) {
            if (toMatch.metricDefinition.dimensions.size() >= existing.metricDefinition.dimensions.size()) {
                boolean isMatch = true;
                for (final Entry<String, String> entry : existing.metricDefinition.dimensions.entrySet()) {
                    if (!compareStrings(entry.getValue(), toMatch.metricDefinition.dimensions.get(entry.getKey()))) {
                        isMatch = false;
                        break;
                    }
                }
                if (isMatch)
                    matches.add(existing);
            }
        }
        return !matches.isEmpty();
    }

    public boolean isEmpty() {
        return byTenantId.isEmpty();
    }

    public void clear() {
        byTenantId.clear();
    }

    private boolean compareStrings(final String s1,
                                   final String s2) {
      if (s1 == s2)
          return true;
      if (s1 == null)
          return false;
      return s1.equals(s2);
    }
}
