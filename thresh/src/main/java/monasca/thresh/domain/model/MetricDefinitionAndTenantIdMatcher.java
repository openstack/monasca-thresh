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

import monasca.common.model.metric.MetricDefinition;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class is used to find any matching MetricDefinitionAndTenantId instances that match the
 * MetricDefinitionAndTenantIds and their associated Alarm Definition IDs that have been loaded.
 * 
 * The actual MetricDefinitionAndTenantId is not kept in the AlarmDefinitionDimensions in order to
 * save heap space. It is expected that possibly millions of metrics may be stored in the Matcher
 * and so by only storing the Dimensions instead of the whole MetricDefinitionAndTenantId, a
 * significant amount of heap space will be saved thus reducing swapping.
 */
public class MetricDefinitionAndTenantIdMatcher {
  final Map<String, Map<String, List<AlarmDefinitionDimensions>>> byTenantId = new ConcurrentHashMap<>();
  @SuppressWarnings("unchecked")
  private final static Set<String> EMPTY_SET = Collections.EMPTY_SET;

  public void add(MetricDefinitionAndTenantId metricDefinitionAndTenantId, String alarmDefinitionId) {
    Map<String, List<AlarmDefinitionDimensions>> byMetricName =
        byTenantId.get(metricDefinitionAndTenantId.tenantId);
    if (byMetricName == null) {
      byMetricName = new ConcurrentHashMap<>();
      byTenantId.put(metricDefinitionAndTenantId.tenantId, byMetricName);
    }
    List<AlarmDefinitionDimensions> alarmDefDimensions =
        byMetricName.get(metricDefinitionAndTenantId.metricDefinition.name);
    if (alarmDefDimensions == null) {
      alarmDefDimensions = new LinkedList<>();
      byMetricName.put(metricDefinitionAndTenantId.metricDefinition.name, alarmDefDimensions);
    }
    final AlarmDefinitionDimensions dimensionSet =
        createDimensionSet(metricDefinitionAndTenantId.metricDefinition, alarmDefinitionId);
    if (!alarmDefDimensions.contains(dimensionSet)) {
      alarmDefDimensions.add(dimensionSet);
    }
  }

  private AlarmDefinitionDimensions createDimensionSet(MetricDefinition metricDefinition,
                                          final String alarmDefinitionId) {
    return new AlarmDefinitionDimensions(metricDefinition.dimensions, alarmDefinitionId);
  }

  public boolean remove(MetricDefinitionAndTenantId metricDefinitionAndTenantId,
                        final String alarmDefinitionId) {
    final Map<String, List<AlarmDefinitionDimensions>> byMetricName =
        byTenantId.get(metricDefinitionAndTenantId.tenantId);
    if (byMetricName == null) {
      return false;
    }

    final List<AlarmDefinitionDimensions> alarmDefDimensions =
        byMetricName.get(metricDefinitionAndTenantId.metricDefinition.name);
    if (alarmDefDimensions == null) {
      return false;
    }

    final AlarmDefinitionDimensions toFind =
        createDimensionSet(metricDefinitionAndTenantId.metricDefinition, alarmDefinitionId);
    final boolean result = alarmDefDimensions.remove(toFind);
    if (result && alarmDefDimensions.isEmpty()) {
      byMetricName.remove(metricDefinitionAndTenantId.metricDefinition.name);
      if (byMetricName.isEmpty()) {
        byTenantId.remove(metricDefinitionAndTenantId.tenantId);
      }
    }
    return result;
  }

  public Set<String> match(final MetricDefinitionAndTenantId toMatch) {
    final Map<String, List<AlarmDefinitionDimensions>> byMetricName = byTenantId.get(toMatch.tenantId);
    if (byMetricName == null) {
      return EMPTY_SET;
    }

    final List<AlarmDefinitionDimensions> alarmDefDimensions = byMetricName.get(toMatch.metricDefinition.name);
    if (alarmDefDimensions == null) {
      return EMPTY_SET;
    }
    Set<String> matches = null;
    for (final AlarmDefinitionDimensions alarmDefDimension : alarmDefDimensions) {
      if (alarmDefDimension.isContainedIn(toMatch.metricDefinition.dimensions)) {
        if (matches == null) {
          matches = new HashSet<>();
        }
        matches.add(alarmDefDimension.alarmDefinitionId);
      }
    }
    return matches == null ? EMPTY_SET : matches;
  }

  public boolean isEmpty() {
    return byTenantId.isEmpty();
  }

  public void clear() {
    byTenantId.clear();
  }

  private static class AlarmDefinitionDimensions {
    final Map<String, String> dimensions;
    final String alarmDefinitionId;

    public AlarmDefinitionDimensions(Map<String, String> dimensions, final String alarmDefinitionId) {
      if (dimensions != null) {
        this.dimensions = dimensions;
      }
      else {
        this.dimensions = new HashMap<>();
      }
      this.alarmDefinitionId = alarmDefinitionId;
    }

    public boolean isContainedIn(Map<String, String> metricDimensions) {
      // All of the dimensions in this AlarmDefinitionDimensions must also be in the metric
      // dimensions
      if (metricDimensions.size() < this.dimensions.size()) {
        return false;
      }
      for (final Map.Entry<String, String> entry : this.dimensions.entrySet()) {
        final String value = metricDimensions.get(entry.getKey());
        if (entry.getValue() == null) {
          if (value != null) {
            return false;
          }
        } else {
          if (!entry.getValue().equals(value)) {
            return false;
          }
        }
      }
      return true;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + alarmDefinitionId.hashCode();
      result = prime * result + dimensions.hashCode();
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
      final AlarmDefinitionDimensions other = (AlarmDefinitionDimensions) obj;
      if (!this.alarmDefinitionId.equals(other.alarmDefinitionId)) {
        return false;
      }
      return this.dimensions.equals(other.dimensions);
    }

    @Override
    public String toString() {
      final StringBuilder builder = new StringBuilder(256);
      builder.append("AlarmDefinitionDimensions [alarmDefinitionId=");
      builder.append(alarmDefinitionId);
      builder.append(",dimensions=");
      builder.append(this.dimensions);
      builder.append("]");

      return builder.toString();
    }
  }
}
