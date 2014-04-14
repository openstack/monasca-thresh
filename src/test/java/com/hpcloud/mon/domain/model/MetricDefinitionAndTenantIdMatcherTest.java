package com.hpcloud.mon.domain.model;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertEqualsNoOrder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.hpcloud.mon.common.model.metric.MetricDefinition;
import com.hpcloud.mon.domain.model.MetricDefinitionAndTenantIdMatcher.DimensionPair;
import com.hpcloud.mon.domain.model.MetricDefinitionAndTenantIdMatcher.DimensionSet;

@Test
public class MetricDefinitionAndTenantIdMatcherTest {

    private static final String HOST = "host";
    private static final String LOAD_BALANCER_GROUP = "loadBalancerGroup";
    private static final String CPU_METRIC_NAME = "cpu";
    private MetricDefinitionAndTenantIdMatcher matcher;
    private List<MetricDefinitionAndTenantId> matches = new ArrayList<>();
    private final String tenantId = "4242";
    private MetricDefinition metricDef;
    private Map<String, String> dimensions;

    @BeforeMethod
    protected void beforeMethod() {
        matches.clear();
        matcher = new MetricDefinitionAndTenantIdMatcher();
        dimensions = new HashMap<>();
        dimensions.put(HOST, "CloudAmI");
        dimensions.put(LOAD_BALANCER_GROUP, "GroupA");
        metricDef = new MetricDefinition(CPU_METRIC_NAME, dimensions);
    }

    public void shouldNotFind() {
        assertTrue(matcher.isEmpty());
        final MetricDefinitionAndTenantId toMatch = new MetricDefinitionAndTenantId(metricDef, tenantId);
        assertFalse(matcher.match(toMatch, matches));

        final MetricDefinitionAndTenantId diffTenantId = new MetricDefinitionAndTenantId(metricDef, "Different");
        matcher.add(diffTenantId);
        assertFalse(matcher.match(toMatch, matches));

        matcher.add(toMatch);
        assertTrue(matcher.match(toMatch, matches));

        assertEquals(matches, Arrays.asList(toMatch));
        matches.clear();

        final MetricDefinitionAndTenantId noMatchOnName = new MetricDefinitionAndTenantId(
                new MetricDefinition("NotCpu", dimensions), tenantId);
        assertFalse(matcher.match(noMatchOnName, matches));

        final Map<String, String> hostDimensions = new HashMap<>(dimensions);
        hostDimensions.put(HOST, "OtherHost");
        final MetricDefinitionAndTenantId noMatchOnDimensions = new MetricDefinitionAndTenantId(
                new MetricDefinition(CPU_METRIC_NAME, hostDimensions), tenantId);
        assertFalse(matcher.match(noMatchOnDimensions, matches));

        matcher.remove(toMatch);
        assertFalse(matcher.match(toMatch, matches));
        matcher.remove(diffTenantId);
        assertTrue(matcher.isEmpty());
    }

    public void shouldFind() {
        assertTrue(matcher.isEmpty());
        final MetricDefinitionAndTenantId toMatch = new MetricDefinitionAndTenantId(metricDef, tenantId);

        final Map<String, String> nullDimensions = new HashMap<>(dimensions);
        nullDimensions.put(HOST, null);
        final MetricDefinitionAndTenantId nullMatch = new MetricDefinitionAndTenantId(
                new MetricDefinition(CPU_METRIC_NAME, nullDimensions), tenantId);
        matcher.add(nullMatch);
        assertTrue(matcher.match(nullMatch, matches));
        assertEqualsNoOrder(matches.toArray(), new MetricDefinitionAndTenantId[] {
            nullMatch});

        final Map<String, String> noDimensions = new HashMap<>();
        final MetricDefinitionAndTenantId noMatch = new MetricDefinitionAndTenantId(
                new MetricDefinition(CPU_METRIC_NAME, noDimensions), tenantId);
        matcher.add(noMatch);
        assertTrue(matcher.match(noMatch, matches));
        assertEqualsNoOrder(matches.toArray(), new MetricDefinitionAndTenantId[] {
            noMatch});

        final Map<String, String> hostDimensions = new HashMap<>();
        hostDimensions.put(HOST, dimensions.get(HOST));
        final MetricDefinitionAndTenantId hostMatch = new MetricDefinitionAndTenantId(
                new MetricDefinition(CPU_METRIC_NAME, hostDimensions), tenantId);
        matcher.add(hostMatch);

        final Map<String, String> groupDimensions = new HashMap<>();
        groupDimensions.put(LOAD_BALANCER_GROUP, dimensions.get(LOAD_BALANCER_GROUP));
        final MetricDefinitionAndTenantId groupMatch = new MetricDefinitionAndTenantId(
                new MetricDefinition(CPU_METRIC_NAME, groupDimensions), tenantId);
        matcher.add(groupMatch);

        assertTrue(matcher.match(toMatch, matches));
        assertEqualsNoOrder(matches.toArray(), new MetricDefinitionAndTenantId[] {
            noMatch, hostMatch, groupMatch});
        matches.clear();

        matcher.add(toMatch);
        assertTrue(matcher.match(toMatch, matches));
        assertEqualsNoOrder(matches.toArray(), new MetricDefinitionAndTenantId[] {
            noMatch, hostMatch, groupMatch, toMatch});
        matches.clear();

        matcher.remove(groupMatch);
        assertTrue(matcher.match(toMatch, matches));
        assertEqualsNoOrder(matches.toArray(), new MetricDefinitionAndTenantId[] {
            noMatch, hostMatch, toMatch});
        matches.clear();

        matcher.remove(noMatch);
        assertTrue(matcher.match(toMatch, matches));
        assertEqualsNoOrder(matches.toArray(), new MetricDefinitionAndTenantId[] {
            hostMatch, toMatch});
        matches.clear();

        matcher.remove(toMatch);
        assertTrue(matcher.match(toMatch, matches));
        assertEqualsNoOrder(matches.toArray(), new MetricDefinitionAndTenantId[] {
            hostMatch});
        matches.clear();

        // Remove it again to ensure it won't throw an exception if the MetricDefinitionAndTenantId
        // doesn't exist
        matcher.remove(toMatch);

        final MetricDefinitionAndTenantId loadMetric = new MetricDefinitionAndTenantId(
                new MetricDefinition("load", new HashMap<String, String>(dimensions)), tenantId);
        matcher.add(loadMetric);

        matcher.remove(hostMatch);
        assertFalse(matcher.match(toMatch, matches));

        // Remove it again to ensure it won't throw an exception if the MetricDefinitionAndTenantId
        // doesn't exist
        matcher.remove(hostMatch);

        matcher.remove(loadMetric);
        matcher.remove(nullMatch);
        assertTrue(matcher.isEmpty());
        assertFalse(matcher.match(toMatch, matches));
    }

    public void shouldCreatePossiblePairs() {
        final Map<String, String> dimensions = new HashMap<>();
        DimensionSet[] actual = matcher.createPossibleDimensionPairs(new MetricDefinition(CPU_METRIC_NAME, dimensions));
        DimensionSet[] expected = { new DimensionSet() };
        assertEqualsNoOrder(actual, expected);

        dimensions.put("1", "a");
        actual = matcher.createPossibleDimensionPairs(new MetricDefinition(CPU_METRIC_NAME, dimensions));
        expected = new DimensionSet[] { new DimensionSet(), new DimensionSet(new DimensionPair("1", "a")) };
        assertEqualsNoOrder(actual, expected);

        dimensions.put("2", "b");
        actual = matcher.createPossibleDimensionPairs(new MetricDefinition(CPU_METRIC_NAME, dimensions));
        expected = new DimensionSet[] { new DimensionSet(), new DimensionSet(new DimensionPair("1", "a")),
                       new DimensionSet(new DimensionPair("2", "b")),
                       new DimensionSet(new DimensionPair("1", "a"), new DimensionPair("2", "b")) };
        assertEqualsNoOrder(actual, expected);

        dimensions.put("3", "c");
        actual = matcher.createPossibleDimensionPairs(new MetricDefinition(CPU_METRIC_NAME, dimensions));
        expected = new DimensionSet[] { new DimensionSet(),
                       new DimensionSet(new DimensionPair("1", "a")),
                       new DimensionSet(new DimensionPair("2", "b")),
                       new DimensionSet(new DimensionPair("3", "c")),
                       new DimensionSet(new DimensionPair("1", "a"), new DimensionPair("2", "b")),
                       new DimensionSet(new DimensionPair("1", "a"), new DimensionPair("3", "c")),
                       new DimensionSet(new DimensionPair("2", "b"), new DimensionPair("3", "c")),
                       new DimensionSet(new DimensionPair("1", "a"), new DimensionPair("2", "b"), new DimensionPair("3", "c"))
        };

        dimensions.put("4", "d");
        actual = matcher.createPossibleDimensionPairs(new MetricDefinition(CPU_METRIC_NAME, dimensions));
        expected = new DimensionSet[] { new DimensionSet(),
                       new DimensionSet(new DimensionPair("1", "a")),
                       new DimensionSet(new DimensionPair("2", "b")),
                       new DimensionSet(new DimensionPair("3", "c")),
                       new DimensionSet(new DimensionPair("4", "d")),
                       new DimensionSet(new DimensionPair("1", "a"), new DimensionPair("2", "b")),
                       new DimensionSet(new DimensionPair("1", "a"), new DimensionPair("3", "c")),
                       new DimensionSet(new DimensionPair("1", "a"), new DimensionPair("4", "d")),
                       new DimensionSet(new DimensionPair("2", "b"), new DimensionPair("3", "c")),
                       new DimensionSet(new DimensionPair("2", "b"), new DimensionPair("4", "d")),
                       new DimensionSet(new DimensionPair("3", "c"), new DimensionPair("4", "d")),
                       new DimensionSet(new DimensionPair("1", "a"), new DimensionPair("2", "b"), new DimensionPair("3", "c")),
                       new DimensionSet(new DimensionPair("1", "a"), new DimensionPair("2", "b"), new DimensionPair("4", "d")),
                       new DimensionSet(new DimensionPair("1", "a"), new DimensionPair("3", "c"), new DimensionPair("4", "d")),
                       new DimensionSet(new DimensionPair("2", "b"), new DimensionPair("3", "c"), new DimensionPair("4", "d")),
                       new DimensionSet(new DimensionPair("1", "a"), new DimensionPair("2", "b"), new DimensionPair("3", "c"), new DimensionPair("4", "d"))
        };
        assertEqualsNoOrder(actual, expected);
    }
}
