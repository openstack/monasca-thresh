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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class is used to find any matching MetricDefinitionAndTenantId instances that match a given
 * MetricDefinitionAndTenantId. This class has no way of handling duplicate
 * MetricDefinitionAndTenantIds so it assume some other handles that issue.
 *
 * The actual MetricDefinitionAndTenantId is not kept in the last Map in order to save heap space.
 * It is expected that possibly millions of metrics may be stored in the Matcher and so by only
 * storing the DiminsionPairs instead of the whole MetricDefinitionAndTenantId, a significant amount
 * of heap space will be saved thus reducing swapping. The MetricDefinitionAndTenantId is recreated
 * when returned but since it will be just sent on and then the reference dropped, the object will
 * be quickly and easily garbage collected. Testing shows that this algorithm is faster than keeping
 * the whole MetricDefinitionAndTenantId in the Map.
 */
public class MetricDefinitionAndTenantIdMatcher {
  final Map<String, Map<String, Map<DimensionSet, Set<String>>>> byTenantId = new ConcurrentHashMap<>();
  private final static DimensionSet EMPTY_DIMENSION_SET = new DimensionSet(new DimensionPair[0]);
  @SuppressWarnings("unchecked")
  private final static Set<String> EMPTY_SET = Collections.EMPTY_SET;

  public void add(MetricDefinitionAndTenantId metricDefinitionAndTenantId, String alarmDefinitionId) {
    Map<String, Map<DimensionSet, Set<String>>> byMetricName =
        byTenantId.get(metricDefinitionAndTenantId.tenantId);
    if (byMetricName == null) {
      byMetricName = new ConcurrentHashMap<>();
      byTenantId.put(metricDefinitionAndTenantId.tenantId, byMetricName);
    }
    Map<DimensionSet, Set<String>> byDimensionSet =
        byMetricName.get(metricDefinitionAndTenantId.metricDefinition.name);
    if (byDimensionSet == null) {
      byDimensionSet = new ConcurrentHashMap<>();
      byMetricName.put(metricDefinitionAndTenantId.metricDefinition.name, byDimensionSet);
    }
    final DimensionSet dimensionSet =
        createDimensionSet(metricDefinitionAndTenantId.metricDefinition);
    Set<String> alarmDefIds = byDimensionSet.get(dimensionSet);
    if (alarmDefIds == null) {
      alarmDefIds = new HashSet<>();
      byDimensionSet.put(dimensionSet, alarmDefIds);
    }
    alarmDefIds.add(alarmDefinitionId);
  }

  private DimensionSet createDimensionSet(MetricDefinition metricDefinition) {
    return new DimensionSet(createPairs(metricDefinition));
  }

  public boolean remove(MetricDefinitionAndTenantId metricDefinitionAndTenantId,
                        final String alarmDefinitionId) {
    final Map<String, Map<DimensionSet, Set<String>>> byMetricName =
        byTenantId.get(metricDefinitionAndTenantId.tenantId);
    if (byMetricName == null) {
      return false;
    }

    final Map<DimensionSet, Set<String>> byDimensionSet =
        byMetricName.get(metricDefinitionAndTenantId.metricDefinition.name);
    if (byDimensionSet == null) {
      return false;
    }

    final DimensionSet dimensionSet =
        createDimensionSet(metricDefinitionAndTenantId.metricDefinition);
    final Set<String> alarmDefinitionIds = byDimensionSet.get(dimensionSet);
    boolean result = false;
    if (alarmDefinitionIds != null && !alarmDefinitionIds.isEmpty()) {
      if (alarmDefinitionIds.remove(alarmDefinitionId)) {
        result = true;
        if (alarmDefinitionIds.isEmpty()) {
          byDimensionSet.remove(dimensionSet);
          if (byDimensionSet.isEmpty()) {
            byMetricName.remove(metricDefinitionAndTenantId.metricDefinition.name);
            if (byMetricName.isEmpty()) {
              byTenantId.remove(metricDefinitionAndTenantId.tenantId);
            }
          }
        }
      }
    }
    return result;
  }

  public Set<String> match(final MetricDefinitionAndTenantId toMatch) {
    final Map<String, Map<DimensionSet, Set<String>>> byMetricName = byTenantId.get(toMatch.tenantId);
    if (byMetricName == null) {
      return EMPTY_SET;
    }

    final Map<DimensionSet, Set<String>> byDimensionSet =
        byMetricName.get(toMatch.metricDefinition.name);
    if (byDimensionSet == null) {
      return EMPTY_SET;
    }
    final DimensionSet[] possibleDimensionSets =
        createPossibleDimensionPairs(toMatch.metricDefinition);
    Set<String> matches = null;
    for (final DimensionSet dimensionSet : possibleDimensionSets) {
      final Set<String> alarmDefinitionIds = byDimensionSet.get(dimensionSet);
      if (alarmDefinitionIds != null) {
        if (matches == null) {
          matches = new HashSet<>();
        }
        matches.addAll(alarmDefinitionIds);
      }
    }
    return matches == null ? EMPTY_SET : matches;
  }

  protected DimensionSet[] createPossibleDimensionPairs(MetricDefinition metricDefinition) {
    final int dimensionSize =
        metricDefinition.dimensions == null ? 0 : metricDefinition.dimensions.size();
    final int size = (int) Math.pow(2, dimensionSize);
    final DimensionSet[] result = new DimensionSet[size];
    int index = 0;
    result[index++] = EMPTY_DIMENSION_SET;
    if (dimensionSize == 0) {
      return result;
    }
    final DimensionPair[] pairs = createPairs(metricDefinition);
    for (int i = 0; i < pairs.length; i++) {
      index = addMore(pairs, i, EMPTY_DIMENSION_SET, result, index);
    }
    return result;
  }

  private int addMore(DimensionPair[] pairs, int start, DimensionSet dimensionSet,
      DimensionSet[] result, int index) {
    final DimensionPair[] newPairs = new DimensionPair[dimensionSet.pairs.length + 1];
    if (dimensionSet.pairs.length > 0) {
      System.arraycopy(dimensionSet.pairs, 0, newPairs, 0, dimensionSet.pairs.length);
    }
    newPairs[dimensionSet.pairs.length] = pairs[start];
    final DimensionSet thisDimensionSet = new DimensionSet(newPairs);
    result[index++] = thisDimensionSet;
    for (int i = start + 1; i < pairs.length; i++) {
      index = addMore(pairs, i, thisDimensionSet, result, index);
    }
    return index;
  }

  private DimensionPair[] createPairs(MetricDefinition metricDefinition) {
    final int dimensionSize =
        metricDefinition.dimensions == null ? 0 : metricDefinition.dimensions.size();
    final DimensionPair[] pairs = new DimensionPair[dimensionSize];
    if (dimensionSize > 0) { // metricDefinition.dimensions can be null
      int index = 0;
      for (final Map.Entry<String, String> entry : metricDefinition.dimensions.entrySet()) {
        pairs[index++] = new DimensionPair(entry.getKey(), entry.getValue());
      }
    }
    return pairs;
  }

  public boolean isEmpty() {
    return byTenantId.isEmpty();
  }

  public void clear() {
    byTenantId.clear();
  }

  protected static class DimensionSet {
    final DimensionPair[] pairs;

    public DimensionSet(DimensionPair... pairs) {
      Arrays.sort(pairs);
      this.pairs = pairs;
    }

    @Override
    public int hashCode() {
      int result = 1;
      final int prime = 31;
      for (DimensionPair pair : pairs) {
        result = result * prime + pair.hashCode();
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
      final DimensionSet other = (DimensionSet) obj;
      if (this.pairs.length != other.pairs.length) {
        return false;
      }
      for (int i = 0; i < this.pairs.length; i++) {
        if (!this.pairs[i].equals(other.pairs[i])) {
          return false;
        }
      }
      return true;
    }

    @Override
    public String toString() {
      final StringBuilder builder = new StringBuilder(256);
      builder.append("DimensionSet [");
      boolean first = true;
      for (DimensionPair pair : pairs) {
        if (!first) {
          builder.append(", ");
        }
        builder.append(pair.toString());
        first = false;
      }
      builder.append("]");

      return builder.toString();
    }
  }

  protected static class DimensionPair implements Comparable<DimensionPair> {
    private String key;
    private String value;

    public DimensionPair(String key, String value) {
      this.key = key;
      this.value = value;
    }

    @Override
    public int hashCode() {
      int result = 1;
      final int prime = 31;
      result = prime * result + key.hashCode();
      result = prime * result + ((value == null) ? 0 : value.hashCode());
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
      DimensionPair other = (DimensionPair) obj;
      return compareStrings(key, other.key) && compareStrings(value, other.value);
    }

    private boolean compareStrings(final String s1, final String s2) {
      if (s1 == s2) {
        return true;
      }
      if (s1 == null) {
        return false;
      }
      return s1.equals(s2);
    }

    @Override
    public int compareTo(DimensionPair o) {
      int c = this.key.compareTo(o.key);
      if (c != 0) {
        return c;
      }
      // Handle possible null values. A actual value is bigger than a null
      if (this.value == null) {
        return o.value == null ? 0 : 1;
      }
      return this.value.compareTo(o.value);
    }

    @Override
    public String toString() {
      return String.format("DimensionPair %s=%s", key, value);
    }
  }
}
