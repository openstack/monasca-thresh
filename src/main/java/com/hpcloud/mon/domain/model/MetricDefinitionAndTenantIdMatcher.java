package com.hpcloud.mon.domain.model;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.hpcloud.mon.common.model.metric.MetricDefinition;

/**
 * This class is used to find any matching MetricDefinitionAndTenantId instances that match a given MetricDefinitionAndTenantId. This class
 * has no way of handling duplicate MetricDefinitionAndTenantIds so it assume some other handles that issue.
 * 
 * The actual MetricDefinitionAndTenantId is not kept in the last Map in order to save heap space. It is expected that possibly millions
 * of metrics may be stored in the Matcher and so by only storing the DiminsionPairs instead of the whole MetricDefinitionAndTenantId,
 * a significant amount of heap space will be saved thus reducing swapping. The MetricDefinitionAndTenantId is recreated when returned but
 * since it will be just sent on and then the reference dropped, the object will be quickly and easily garbage collected. Testing shows
 * that this algorithm is faster than keeping the whole MetricDefinitionAndTenantId in the Map.
 */
public class MetricDefinitionAndTenantIdMatcher {
    final Map<String, Map<String, Map<DimensionSet, Object>>> byTenantId = new ConcurrentHashMap<>();
    private final static DimensionSet EMPTY_DIMENSION_SET = new DimensionSet(new DimensionPair[0]);
    private final static Object placeHolder = new Object();

    public void add(MetricDefinitionAndTenantId metricDefinitionAndTenantId) {
        Map<String, Map<DimensionSet, Object>> byMetricName = byTenantId.get(metricDefinitionAndTenantId.tenantId);
        if (byMetricName == null) {
            byMetricName = new ConcurrentHashMap<>();
            byTenantId.put(metricDefinitionAndTenantId.tenantId, byMetricName);
        }
        Map<DimensionSet, Object> byDimensionSet = byMetricName.get(metricDefinitionAndTenantId.metricDefinition.name);
        if (byDimensionSet == null) {
            byDimensionSet = new ConcurrentHashMap<>();
            byMetricName.put(metricDefinitionAndTenantId.metricDefinition.name, byDimensionSet);
        }
        final DimensionSet dimensionSet = createDimensionSet(metricDefinitionAndTenantId.metricDefinition);
        byDimensionSet.put(dimensionSet, placeHolder);
    }

    private DimensionSet createDimensionSet(MetricDefinition metricDefinition) {
        return new DimensionSet(createPairs(metricDefinition));
    }

    public boolean remove(MetricDefinitionAndTenantId metricDefinitionAndTenantId) {
        final Map<String, Map<DimensionSet, Object>> byMetricName = byTenantId.get(metricDefinitionAndTenantId.tenantId);
        if (byMetricName == null)
            return false;

        final Map<DimensionSet, Object> byDimensionSet = byMetricName.get(metricDefinitionAndTenantId.metricDefinition.name);
        if (byDimensionSet == null)
            return false;

        final DimensionSet dimensionSet = createDimensionSet(metricDefinitionAndTenantId.metricDefinition);
        final boolean result = byDimensionSet.remove(dimensionSet) != null;
        if (result) {
            if (byDimensionSet.isEmpty()) {
                byMetricName.remove(metricDefinitionAndTenantId.metricDefinition.name);
                if (byMetricName.isEmpty())
                    byTenantId.remove(metricDefinitionAndTenantId.tenantId);
            }
        }
        return result;
    }

    public boolean match(final MetricDefinitionAndTenantId toMatch,
                         final List<MetricDefinitionAndTenantId> matches) {
        final Map<String, Map<DimensionSet, Object>> byMetricName = byTenantId.get(toMatch.tenantId);
        if (byMetricName == null)
            return false;

        final Map<DimensionSet, Object> byDimensionSet = byMetricName.get(toMatch.metricDefinition.name);
        if (byDimensionSet == null)
            return false;
        final DimensionSet[] possibleDimensionSets = createPossibleDimensionPairs(toMatch.metricDefinition);
        matches.clear();
        for (final DimensionSet dimensionSet : possibleDimensionSets) {
            if (byDimensionSet.containsKey(dimensionSet))
                matches.add(createFromDimensionSet(toMatch, dimensionSet));
        }
        return !matches.isEmpty();
    }

    private MetricDefinitionAndTenantId createFromDimensionSet(
            MetricDefinitionAndTenantId toMatch,
            DimensionSet dimensionSet) {
        final Map<String, String> dimensions = new HashMap<>(dimensionSet.pairs.length);
        for (final DimensionPair pair : dimensionSet.pairs)
            dimensions.put(pair.key, pair.value);
        return new MetricDefinitionAndTenantId(new MetricDefinition(toMatch.metricDefinition.name, dimensions), toMatch.tenantId);
    }

    protected DimensionSet[] createPossibleDimensionPairs(MetricDefinition metricDefinition) {
        final int dimensionSize = metricDefinition.dimensions == null ? 0 : metricDefinition.dimensions.size();
        final int size = (int)Math.pow(2, dimensionSize);
        final DimensionSet[] result = new DimensionSet[size];
        int index = 0;
        result[index++] = EMPTY_DIMENSION_SET;
        if (dimensionSize == 0)
            return result;
        final DimensionPair[] pairs = createPairs(metricDefinition);
        for (int i = 0; i < pairs.length; i++)
            index = addMore(pairs, i, EMPTY_DIMENSION_SET, result, index);
        return result;
    }

    private int addMore(DimensionPair[] pairs, int start,
            DimensionSet dimensionSet, DimensionSet[] result, int index) {
        final DimensionPair[] newPairs = new DimensionPair[dimensionSet.pairs.length + 1];
        if (dimensionSet.pairs.length > 0)
            System.arraycopy(dimensionSet.pairs, 0, newPairs, 0, dimensionSet.pairs.length);
        newPairs[dimensionSet.pairs.length] = pairs[start];
        final DimensionSet thisDimensionSet = new DimensionSet(newPairs);
        result[index++] = thisDimensionSet;
        for (int i = start + 1; i < pairs.length; i++)
           index = addMore(pairs, i, thisDimensionSet, result, index);
        return index;
    }

    private DimensionPair[] createPairs(MetricDefinition metricDefinition) {
        final int dimensionSize = metricDefinition.dimensions == null ? 0 : metricDefinition.dimensions.size();
        final DimensionPair[] pairs = new DimensionPair[dimensionSize];
        if (dimensionSize > 0) { // metricDefinition.dimensions can be null
            int index = 0;
            for (final Map.Entry<String, String> entry : metricDefinition.dimensions.entrySet())
                pairs[index++] = new DimensionPair(entry.getKey(), entry.getValue());
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

        public DimensionSet(DimensionPair ... pairs) {
            Arrays.sort(pairs);
            this.pairs = pairs;
        }

        @Override
        public int hashCode() {
            int result = 1;
            final int prime = 31;
            for (DimensionPair pair : pairs)
                result = result * prime + pair.hashCode();
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (getClass() != obj.getClass())
                return false;
            final DimensionSet other = (DimensionSet) obj;
            if (this.pairs.length != other.pairs.length)
                return false;
            for (int i = 0; i < this.pairs.length; i++)
                if (!this.pairs[i].equals(other.pairs[i]))
                    return false;
            return true;
        }

        @Override
        public String toString() {
            final StringBuilder builder = new StringBuilder(256);
            builder.append("DimensionSet [");
            boolean first = true;
            for (DimensionPair pair : pairs) {
                if (!first)
                    builder.append(", ");
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
            if (this == obj)
                return true;
            if (getClass() != obj.getClass())
                return false;
            DimensionPair other = (DimensionPair) obj;
            return compareStrings(key, other.key) &&
                    compareStrings(value, other.value);
        }

        private boolean compareStrings(final String s1,
                                       final String s2) {
          if (s1 == s2)
              return true;
          if (s1 == null)
              return false;
          return s1.equals(s2);
        }

        @Override
        public int compareTo(DimensionPair o) {
            int c = this.key.compareTo(o.key);
            if (c != 0)
                return c;
            // Handle possible null values. A actual value is bigger than a null
            if (this.value == null)
                return o.value == null ? 0: 1;
            return this.value.compareTo(o.value);
        }

        @Override
        public String toString() {
            return String.format("DimensionPair %s=%s", key, value);
        }
    }
}
