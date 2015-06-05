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

import static org.testng.Assert.assertEqualsNoOrder;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertFalse;

import monasca.common.model.metric.MetricDefinition;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Test
public class MetricDefinitionAndTenantIdMatcherTest {

  private static final String HOST = "host";
  private static final String LOAD_BALANCER_GROUP = "loadBalancerGroup";
  private static final String CPU_METRIC_NAME = "cpu";
  private MetricDefinitionAndTenantIdMatcher matcher;
  private final String tenantId = "4242";
  private MetricDefinition metricDef;
  private Map<String, String> dimensions;
  private int nextId = 42;

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
    final String toMatchId = getNextId();
    verifyNoMatch(toMatch);

    final MetricDefinitionAndTenantId diffTenantId =
        new MetricDefinitionAndTenantId(metricDef, "Different");
    final String diffTenantIdId = getNextId();
    matcher.add(diffTenantId, diffTenantIdId);
    verifyNoMatch(toMatch);

    matcher.add(toMatch, toMatchId);
    verifyMatch(toMatch, toMatchId);

    final MetricDefinitionAndTenantId noMatchOnName =
        new MetricDefinitionAndTenantId(new MetricDefinition("NotCpu", dimensions), tenantId);
    verifyNoMatch(noMatchOnName);

    final Map<String, String> hostDimensions = new HashMap<>(dimensions);
    hostDimensions.put(HOST, "OtherHost");
    final MetricDefinitionAndTenantId noMatchOnDimensions =
        new MetricDefinitionAndTenantId(new MetricDefinition(CPU_METRIC_NAME, hostDimensions),
            tenantId);
    verifyNoMatch(noMatchOnDimensions);

    matcher.remove(toMatch, toMatchId);
    verifyNoMatch(toMatch);
    matcher.remove(diffTenantId, diffTenantIdId);
    assertTrue(matcher.isEmpty());
  }

  private void verifyNoMatch(final MetricDefinitionAndTenantId toMatch) {
    verifyMatch(toMatch);
  }

  private void verifyMatch(final MetricDefinitionAndTenantId toMatch,
      final String... expected) {
    final Set<String> matches = matcher.match(toMatch);
    assertEqualsNoOrder(matches.toArray(), expected);
  }

  public void shouldFind() {
    assertTrue(matcher.isEmpty());
    final MetricDefinitionAndTenantId toMatch =
        new MetricDefinitionAndTenantId(metricDef, tenantId);
    final String toMatchId = getNextId();

    final Map<String, String> nullDimensions = new HashMap<>(dimensions);
    nullDimensions.put(HOST, null);
    final MetricDefinitionAndTenantId nullMatch =
        new MetricDefinitionAndTenantId(new MetricDefinition(CPU_METRIC_NAME, nullDimensions),
            tenantId);
    final String nullMatchId = getNextId();
    matcher.add(nullMatch, nullMatchId);
    verifyMatch(nullMatch, nullMatchId);

    final Map<String, String> noDimensions = new HashMap<>();
    final MetricDefinitionAndTenantId noMatch =
        new MetricDefinitionAndTenantId(new MetricDefinition(CPU_METRIC_NAME, noDimensions),
            tenantId);
    final String noMatchId = getNextId();
    matcher.add(noMatch, noMatchId);
    verifyMatch(noMatch, noMatchId);

    final Map<String, String> hostDimensions = new HashMap<>();
    hostDimensions.put(HOST, dimensions.get(HOST));
    final MetricDefinitionAndTenantId hostMatch =
        new MetricDefinitionAndTenantId(new MetricDefinition(CPU_METRIC_NAME, hostDimensions),
            tenantId);
    final String hostMatchId = getNextId();
    matcher.add(hostMatch, hostMatchId);

    final Map<String, String> groupDimensions = new HashMap<>();
    groupDimensions.put(LOAD_BALANCER_GROUP, dimensions.get(LOAD_BALANCER_GROUP));
    final MetricDefinitionAndTenantId groupMatch =
        new MetricDefinitionAndTenantId(new MetricDefinition(CPU_METRIC_NAME, groupDimensions),
            tenantId);
    final String groupMatchId = getNextId();
    matcher.add(groupMatch, groupMatchId);

    // Add it twice just to make sure that doesn't cause problems
    matcher.add(groupMatch, groupMatchId);

    verifyMatch(toMatch, noMatchId, hostMatchId, groupMatchId);

    matcher.add(toMatch, toMatchId);
    verifyMatch(toMatch, noMatchId, hostMatchId, groupMatchId, toMatchId);

    assertTrue(matcher.remove(groupMatch, groupMatchId));
    verifyMatch(toMatch, noMatchId, hostMatchId, toMatchId);

    assertTrue(matcher.remove(noMatch, noMatchId));
    verifyMatch(toMatch, hostMatchId, toMatchId);

    assertTrue(matcher.remove(toMatch, toMatchId));
    verifyMatch(toMatch, hostMatchId);

    // Remove it again to ensure it won't throw an exception if the MetricDefinitionAndTenantId
    // doesn't exist
    assertFalse(matcher.remove(toMatch, toMatchId));

    final MetricDefinitionAndTenantId loadMetric =
        new MetricDefinitionAndTenantId(new MetricDefinition("load", new HashMap<String, String>(
            dimensions)), tenantId);
    final String loadMetricId = getNextId();
    matcher.add(loadMetric, loadMetricId);

    matcher.remove(hostMatch, hostMatchId);
    verifyNoMatch(toMatch);

    // Remove it again to ensure it won't throw an exception if the MetricDefinitionAndTenantId
    // doesn't exist
    matcher.remove(hostMatch, hostMatchId);

    matcher.remove(loadMetric, loadMetricId);
    matcher.remove(nullMatch, nullMatchId);
    assertTrue(matcher.isEmpty());
    verifyNoMatch(toMatch);
  }

  private String getNextId() {
    return String.valueOf(this.nextId++);
  }
}

