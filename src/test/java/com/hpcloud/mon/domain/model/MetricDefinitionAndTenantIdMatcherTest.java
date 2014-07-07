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

package com.hpcloud.mon.domain.model;

import static org.testng.Assert.assertEqualsNoOrder;
import static org.testng.Assert.assertTrue;

import com.hpcloud.mon.common.model.metric.MetricDefinition;
import com.hpcloud.mon.domain.model.MetricDefinitionAndTenantIdMatcher.DimensionPair;
import com.hpcloud.mon.domain.model.MetricDefinitionAndTenantIdMatcher.DimensionSet;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Test
public class MetricDefinitionAndTenantIdMatcherTest {

  private static final String HOST = "host";
  private static final String LOAD_BALANCER_GROUP = "loadBalancerGroup";
  private static final String CPU_METRIC_NAME = "cpu";
  private MetricDefinitionAndTenantIdMatcher matcher;
  private final String tenantId = "4242";
  private MetricDefinition metricDef;
  private Map<String, String> dimensions;

  @BeforeMethod
  protected void beforeMethod() {
    matcher = new MetricDefinitionAndTenantIdMatcher();
    dimensions = new HashMap<>();
    dimensions.put(HOST, "CloudAmI");
    dimensions.put(LOAD_BALANCER_GROUP, "GroupA");
    metricDef = new MetricDefinition(CPU_METRIC_NAME, dimensions);
  }

  public void shouldNotFind() {
    assertTrue(matcher.isEmpty());
    final MetricDefinitionAndTenantId toMatch =
        new MetricDefinitionAndTenantId(metricDef, tenantId);
    verifyNoMatch(toMatch);

    final MetricDefinitionAndTenantId diffTenantId =
        new MetricDefinitionAndTenantId(metricDef, "Different");
    matcher.add(diffTenantId);
    verifyNoMatch(toMatch);

    matcher.add(toMatch);
    verifyMatch(toMatch, toMatch);

    final MetricDefinitionAndTenantId noMatchOnName =
        new MetricDefinitionAndTenantId(new MetricDefinition("NotCpu", dimensions), tenantId);
    verifyNoMatch(noMatchOnName);

    final Map<String, String> hostDimensions = new HashMap<>(dimensions);
    hostDimensions.put(HOST, "OtherHost");
    final MetricDefinitionAndTenantId noMatchOnDimensions =
        new MetricDefinitionAndTenantId(new MetricDefinition(CPU_METRIC_NAME, hostDimensions),
            tenantId);
    verifyNoMatch(noMatchOnDimensions);

    matcher.remove(toMatch);
    verifyNoMatch(toMatch);
    matcher.remove(diffTenantId);
    assertTrue(matcher.isEmpty());
  }

  private void verifyNoMatch(final MetricDefinitionAndTenantId toMatch) {
    verifyMatch(toMatch);
  }

  private void verifyMatch(final MetricDefinitionAndTenantId toMatch,
      final MetricDefinitionAndTenantId... expected) {
    final List<MetricDefinitionAndTenantId> matches = matcher.match(toMatch);
    assertEqualsNoOrder(matches.toArray(), expected);
  }

  public void shouldFind() {
    assertTrue(matcher.isEmpty());
    final MetricDefinitionAndTenantId toMatch =
        new MetricDefinitionAndTenantId(metricDef, tenantId);

    final Map<String, String> nullDimensions = new HashMap<>(dimensions);
    nullDimensions.put(HOST, null);
    final MetricDefinitionAndTenantId nullMatch =
        new MetricDefinitionAndTenantId(new MetricDefinition(CPU_METRIC_NAME, nullDimensions),
            tenantId);
    matcher.add(nullMatch);
    verifyMatch(nullMatch, nullMatch);

    final Map<String, String> noDimensions = new HashMap<>();
    final MetricDefinitionAndTenantId noMatch =
        new MetricDefinitionAndTenantId(new MetricDefinition(CPU_METRIC_NAME, noDimensions),
            tenantId);
    matcher.add(noMatch);
    verifyMatch(noMatch, noMatch);

    final Map<String, String> hostDimensions = new HashMap<>();
    hostDimensions.put(HOST, dimensions.get(HOST));
    final MetricDefinitionAndTenantId hostMatch =
        new MetricDefinitionAndTenantId(new MetricDefinition(CPU_METRIC_NAME, hostDimensions),
            tenantId);
    matcher.add(hostMatch);

    final Map<String, String> groupDimensions = new HashMap<>();
    groupDimensions.put(LOAD_BALANCER_GROUP, dimensions.get(LOAD_BALANCER_GROUP));
    final MetricDefinitionAndTenantId groupMatch =
        new MetricDefinitionAndTenantId(new MetricDefinition(CPU_METRIC_NAME, groupDimensions),
            tenantId);
    matcher.add(groupMatch);

    verifyMatch(toMatch, noMatch, hostMatch, groupMatch);

    matcher.add(toMatch);
    verifyMatch(toMatch, noMatch, hostMatch, groupMatch, toMatch);

    matcher.remove(groupMatch);
    verifyMatch(toMatch, noMatch, hostMatch, toMatch);

    matcher.remove(noMatch);
    verifyMatch(toMatch, hostMatch, toMatch);

    matcher.remove(toMatch);
    verifyMatch(toMatch, hostMatch);

    // Remove it again to ensure it won't throw an exception if the MetricDefinitionAndTenantId
    // doesn't exist
    matcher.remove(toMatch);

    final MetricDefinitionAndTenantId loadMetric =
        new MetricDefinitionAndTenantId(new MetricDefinition("load", new HashMap<String, String>(
            dimensions)), tenantId);
    matcher.add(loadMetric);

    matcher.remove(hostMatch);
    verifyNoMatch(toMatch);

    // Remove it again to ensure it won't throw an exception if the MetricDefinitionAndTenantId
    // doesn't exist
    matcher.remove(hostMatch);

    matcher.remove(loadMetric);
    matcher.remove(nullMatch);
    assertTrue(matcher.isEmpty());
    verifyNoMatch(toMatch);
  }

  public void shouldCreatePossiblePairs() {
    final Map<String, String> dimensions = new HashMap<>();
    DimensionSet[] actual =
        matcher.createPossibleDimensionPairs(new MetricDefinition(CPU_METRIC_NAME, dimensions));
    DimensionSet[] expected = {new DimensionSet()};
    assertEqualsNoOrder(actual, expected);

    dimensions.put("1", "a");
    actual =
        matcher.createPossibleDimensionPairs(new MetricDefinition(CPU_METRIC_NAME, dimensions));
    expected =
        new DimensionSet[] {new DimensionSet(), new DimensionSet(new DimensionPair("1", "a"))};
    assertEqualsNoOrder(actual, expected);

    dimensions.put("2", "b");
    actual =
        matcher.createPossibleDimensionPairs(new MetricDefinition(CPU_METRIC_NAME, dimensions));
    expected =
        new DimensionSet[] {new DimensionSet(), new DimensionSet(new DimensionPair("1", "a")),
            new DimensionSet(new DimensionPair("2", "b")),
            new DimensionSet(new DimensionPair("1", "a"), new DimensionPair("2", "b"))};
    assertEqualsNoOrder(actual, expected);

    dimensions.put("3", "c");
    actual =
        matcher.createPossibleDimensionPairs(new MetricDefinition(CPU_METRIC_NAME, dimensions));
    expected =
        new DimensionSet[] {
            new DimensionSet(),
            new DimensionSet(new DimensionPair("1", "a")),
            new DimensionSet(new DimensionPair("2", "b")),
            new DimensionSet(new DimensionPair("3", "c")),
            new DimensionSet(new DimensionPair("1", "a"), new DimensionPair("2", "b")),
            new DimensionSet(new DimensionPair("1", "a"), new DimensionPair("3", "c")),
            new DimensionSet(new DimensionPair("2", "b"), new DimensionPair("3", "c")),
            new DimensionSet(new DimensionPair("1", "a"), new DimensionPair("2", "b"),
                new DimensionPair("3", "c"))};

    dimensions.put("4", "d");
    actual =
        matcher.createPossibleDimensionPairs(new MetricDefinition(CPU_METRIC_NAME, dimensions));
    expected =
        new DimensionSet[] {
            new DimensionSet(),
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
            new DimensionSet(new DimensionPair("1", "a"), new DimensionPair("2", "b"),
                new DimensionPair("3", "c")),
            new DimensionSet(new DimensionPair("1", "a"), new DimensionPair("2", "b"),
                new DimensionPair("4", "d")),
            new DimensionSet(new DimensionPair("1", "a"), new DimensionPair("3", "c"),
                new DimensionPair("4", "d")),
            new DimensionSet(new DimensionPair("2", "b"), new DimensionPair("3", "c"),
                new DimensionPair("4", "d")),
            new DimensionSet(new DimensionPair("1", "a"), new DimensionPair("2", "b"),
                new DimensionPair("3", "c"), new DimensionPair("4", "d"))};
    assertEqualsNoOrder(actual, expected);
  }
}

