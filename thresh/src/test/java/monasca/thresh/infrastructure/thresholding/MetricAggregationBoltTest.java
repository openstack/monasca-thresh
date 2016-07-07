/*
 * (C) Copyright 2014,2016 Hewlett Packard Enterprise Development Company LP.
 * Copyright 2016 FUJITSU LIMITED
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

package monasca.thresh.infrastructure.thresholding;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import monasca.common.model.alarm.AlarmOperator;
import monasca.common.model.alarm.AlarmState;

import monasca.common.model.alarm.AlarmSubExpression;
import monasca.common.model.metric.Metric;
import monasca.common.model.metric.MetricDefinition;
import monasca.thresh.ThresholdingConfiguration;
import monasca.thresh.domain.model.MetricDefinitionAndTenantId;
import monasca.thresh.domain.model.SubAlarm;
import monasca.thresh.domain.model.SubAlarmStats;
import monasca.thresh.domain.model.SubExpression;
import monasca.thresh.domain.model.TenantIdAndMetricName;
import monasca.thresh.domain.service.SubAlarmStatsRepository;
import monasca.thresh.utils.Streams;

import org.apache.storm.Constants;
import org.apache.storm.Testing;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.testing.MkTupleParam;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Test
public class MetricAggregationBoltTest {
  private static final String TENANT_ID = "42";
  private static final String ALARM_ID_1 = "123";
  private MockMetricAggregationBolt bolt;
  private TopologyContext context;
  private OutputCollector collector;
  private List<SubAlarm> subAlarms;
  private SubAlarm subAlarm1;
  private SubAlarm subAlarm2;
  private SubAlarm subAlarm3;
  private SubAlarm subAlarm4;
  private SubExpression subExpr1;
  private SubExpression subExpr2;
  private SubExpression subExpr3;
  private SubExpression subExpr4;
  private MetricDefinition metricDef1;
  private MetricDefinition metricDef2;
  private MetricDefinition metricDef3;
  private MetricDefinition metricDef4;

  @BeforeClass
  protected void beforeClass() {
    // Other tests set this and that can cause problems when the test is run from Maven
    System.clearProperty(MetricAggregationBolt.TICK_TUPLE_SECONDS_KEY);
    subExpr1 = new SubExpression("444", AlarmSubExpression.of("avg(hpcs.compute.cpu{id=5}, 60) >= 90 times 3"));
    subExpr2 = new SubExpression("555", AlarmSubExpression.of("avg(hpcs.compute.mem{id=5}, 60) >= 90"));
    subExpr3 = new SubExpression("666", AlarmSubExpression.of("max(hpcs.compute.mem{id=5}, 60) >= 96"));
    subExpr4 = new SubExpression("777", AlarmSubExpression.of(
        "count(log.error{id=5},deterministic,60) >= 5")
    );
    metricDef1 = subExpr1.getAlarmSubExpression().getMetricDefinition();
    metricDef2 = subExpr2.getAlarmSubExpression().getMetricDefinition();
    metricDef3 = subExpr3.getAlarmSubExpression().getMetricDefinition();
    metricDef4 = subExpr4.getAlarmSubExpression().getMetricDefinition();
  }

  @BeforeMethod
  protected void beforeMethod() {
    // Fixtures
    subAlarm1 = new SubAlarm(ALARM_ID_1, "1", subExpr1);
    subAlarm2 = new SubAlarm("456", "1", subExpr2);
    subAlarm3 = new SubAlarm("789", "2", subExpr3);
    subAlarm4 = new SubAlarm("666", "3", subExpr4);
    subAlarms = new ArrayList<>();
    subAlarms.add(subAlarm1);
    subAlarms.add(subAlarm2);
    subAlarms.add(subAlarm3);
    subAlarms.add(subAlarm4);

    final ThresholdingConfiguration config = new ThresholdingConfiguration();
    config.alarmDelay = 10;
    bolt = new MockMetricAggregationBolt(config);
    context = mock(TopologyContext.class);
    collector = mock(OutputCollector.class);
    bolt.prepare(null, context, collector);
  }

  public void shouldAggregateValues() {

    sendSubAlarmCreated(metricDef1, subAlarm1);
    sendSubAlarmCreated(metricDef2, subAlarm2);
    sendSubAlarmCreated(metricDef4, subAlarm4);

    long t1 = System.currentTimeMillis();

    bolt.aggregateValues(new MetricDefinitionAndTenantId(metricDef1, TENANT_ID), new Metric(
        metricDef1.name, metricDef1.dimensions, t1, 100, null));
    bolt.aggregateValues(new MetricDefinitionAndTenantId(metricDef1, TENANT_ID), new Metric(
        metricDef1.name, metricDef1.dimensions, t1, 80, null));
    bolt.aggregateValues(new MetricDefinitionAndTenantId(metricDef2, TENANT_ID), new Metric(
        metricDef2.name, metricDef2.dimensions, t1, 50, null));
    bolt.aggregateValues(new MetricDefinitionAndTenantId(metricDef2, TENANT_ID), new Metric(
        metricDef2.name, metricDef2.dimensions, t1, 40, null));
    bolt.aggregateValues(new MetricDefinitionAndTenantId(metricDef4, TENANT_ID), new Metric(
        metricDef4.name, metricDef4.dimensions, t1, 1, null));
    bolt.aggregateValues(new MetricDefinitionAndTenantId(metricDef4, TENANT_ID), new Metric(
        metricDef4.name, metricDef4.dimensions, t1, 1, null));

    SubAlarmStatsRepository orCreateSubAlarmStatsRepo = bolt.getOrCreateSubAlarmStatsRepo(new MetricDefinitionAndTenantId(metricDef1, TENANT_ID));
    SubAlarmStats alarmData =
        orCreateSubAlarmStatsRepo
            .get(subAlarm1.getId());
    assertEquals(alarmData.getStats().getValue(t1/1000), 90.0);

    alarmData =
        bolt.getOrCreateSubAlarmStatsRepo(new MetricDefinitionAndTenantId(metricDef2, TENANT_ID))
            .get(subAlarm2.getId());
    assertEquals(alarmData.getStats().getValue(t1/1000), 45.0);

    alarmData =
        bolt.getOrCreateSubAlarmStatsRepo(new MetricDefinitionAndTenantId(metricDef4, TENANT_ID))
            .get(subAlarm4.getId());
    assertEquals(alarmData.getStats().getValue(t1/1000), 2.0);
  }

  public void shouldEvaluateAlarms() {
    // Ensure subAlarm2 and subAlarm3 map to the same Metric Definition
    assertEquals(metricDef3, metricDef2);

    long t1 = 170000;
    bolt.setCurrentTime(t1);
    sendSubAlarmCreated(metricDef1, subAlarm1);
    sendSubAlarmCreated(metricDef2, subAlarm2);
    sendSubAlarmCreated(metricDef3, subAlarm3);
    sendSubAlarmCreated(metricDef4, subAlarm4);

    // Send metrics for subAlarm1
    bolt.execute(createMetricTuple(metricDef1, new Metric(metricDef1, t1, 100000, null)));
    bolt.execute(createMetricTuple(metricDef1, new Metric(metricDef1, t1 - 60000, 95, null)));
    bolt.execute(createMetricTuple(metricDef1, new Metric(metricDef1, t1 - 120000, 88, null)));

    t1 += 25000;
    bolt.setCurrentTime(t1);
    sendTickTuple();

    assertEquals(subAlarm1.getState(), AlarmState.OK);
    assertEquals(subAlarm2.getState(), AlarmState.UNDETERMINED);
    assertEquals(subAlarm3.getState(), AlarmState.UNDETERMINED);
    assertEquals(subAlarm4.getState(), AlarmState.OK); // deterministic

    verify(collector, times(1)).emit(new Values(subAlarm1.getAlarmId(), subAlarm1));
    verify(collector, never()).emit(new Values(subAlarm2.getAlarmId(), subAlarm2));
    verify(collector, never()).emit(new Values(subAlarm3.getAlarmId(), subAlarm3));
    verify(collector, times(1)).emit(new Values(subAlarm4.getAlarmId(), subAlarm4));
    // Have to reset the mock so it can tell the difference when subAlarm2 and subAlarm3 are emitted
    reset(collector);

    // Drive subAlarm1 to ALARM
    bolt.execute(createMetricTuple(metricDef1, new Metric(metricDef1, t1, 99, null)));
    // Drive subAlarm2 to ALARM and subAlarm3 to OK since they use the same MetricDefinition
    t1 += 10000;
    bolt.execute(createMetricTuple(metricDef2, new Metric(metricDef2, t1, 94, null)));
    t1 += 50000;
    bolt.setCurrentTime(t1);
    sendTickTuple();

    assertEquals(subAlarm1.getState(), AlarmState.ALARM);
    assertEquals(subAlarm2.getState(), AlarmState.ALARM);
    assertEquals(subAlarm3.getState(), AlarmState.OK);
    assertEquals(subAlarm4.getState(), AlarmState.OK);
    verify(collector, times(1)).emit(new Values(subAlarm1.getAlarmId(), subAlarm1));
    verify(collector, times(1)).emit(new Values(subAlarm2.getAlarmId(), subAlarm2));
    verify(collector, times(1)).emit(new Values(subAlarm3.getAlarmId(), subAlarm3));
    verify(collector, never()).emit(new Values(subAlarm4.getAlarmId(), subAlarm4));
  }

  public void shouldImmediatelyEvaluateSubAlarm() {
    // Ensure subAlarm2 and subAlarm3 map to the same Metric Definition
    assertEquals(metricDef3, metricDef2);

    long t1 = 170000;
    bolt.setCurrentTime(t1);
    sendSubAlarmCreated(metricDef2, subAlarm2);
    sendSubAlarmCreated(metricDef3, subAlarm3);
    sendSubAlarmCreated(metricDef4, subAlarm4);

    // Send metric for subAlarm2 and subAlarm3
    bolt.execute(createMetricTuple(metricDef3, new Metric(metricDef3, t1 + 1000, 100000, null)));
    for (int i = 0; i < 5; i++) {
      bolt.execute(createMetricTuple(metricDef4, new Metric(metricDef4, t1 + 1000 + i, 1, null)));
    }

    // subAlarm2 is AVG so it can't be evaluated immediately like the MAX for subalarm3
    // or count for subAlarm4
    assertEquals(subAlarm2.getState(), AlarmState.UNDETERMINED);
    assertEquals(subAlarm3.getState(), AlarmState.ALARM);
    assertEquals(subAlarm4.getState(), AlarmState.ALARM);

    verify(collector, never()).emit(new Values(subAlarm2.getAlarmId(), subAlarm2));
    verify(collector, times(1)).emit(new Values(subAlarm3.getAlarmId(), subAlarm3));
    verify(collector, times(1)).emit(new Values(subAlarm4.getAlarmId(), subAlarm4));

    // Have to reset the mock so it can tell the difference when subAlarm2 and subAlarm3 are emitted
    // again.
    reset(collector);

    t1 = 195000;
    bolt.setCurrentTime(t1);
    sendTickTuple();

    assertEquals(subAlarm2.getState(), AlarmState.ALARM);
    assertEquals(subAlarm3.getState(), AlarmState.ALARM);
    assertEquals(subAlarm4.getState(), AlarmState.ALARM);

    verify(collector, times(1)).emit(new Values(subAlarm2.getAlarmId(), subAlarm2));
    verify(collector, never()).emit(new Values(subAlarm3.getAlarmId(), subAlarm3));
    verify(collector, never()).emit(new Values(subAlarm4.getAlarmId(), subAlarm4));

    // Have to reset the mock so it can tell the difference when subAlarm2 and subAlarm3 are emitted
    // again.
    reset(collector);

    // Now drive SubAlarms back to OK
    t1 = 235000;
    bolt.setCurrentTime(t1);
    bolt.execute(createMetricTuple(metricDef3, new Metric(metricDef3, t1 + 1000, 20, null)));

    t1 = 315000;
    bolt.setCurrentTime(t1);

    bolt.execute(createMetricTuple(metricDef3, new Metric(metricDef3, t1 + 1000, 20, null)));

    sendTickTuple();

    assertEquals(subAlarm2.getState(), AlarmState.OK);
    assertEquals(subAlarm3.getState(), AlarmState.OK);
    // subAlarm4 goes to OK because of missing metrics
    assertEquals(subAlarm4.getState(), AlarmState.OK);
    verify(collector, times(1)).emit(new Values(subAlarm2.getAlarmId(), subAlarm2));
    verify(collector, times(1)).emit(new Values(subAlarm3.getAlarmId(), subAlarm3));
    verify(collector, times(1)).emit(new Values(subAlarm4.getAlarmId(), subAlarm4));

    // Have to reset the mock so it can tell the difference when subAlarm2 and subAlarm3 are emitted
    // again.
    reset(collector);

    // Now send a metric that is after the window end time but within alarm delay
    t1 = 365000;
    bolt.setCurrentTime(t1);
    bolt.execute(createMetricTuple(metricDef3, new Metric(metricDef3, t1 + 1000, 100000, null)));

    // subAlarm2 is AVG so it can't be evaluated immediately like the MAX for subalarm3
    assertEquals(subAlarm2.getState(), AlarmState.OK);
    assertEquals(subAlarm3.getState(), AlarmState.ALARM);

    verify(collector, never()).emit(new Values(subAlarm2.getAlarmId(), subAlarm2));
    verify(collector, times(1)).emit(new Values(subAlarm3.getAlarmId(), subAlarm3));

    // Have to reset the mock so it can tell the difference when subAlarm2 and subAlarm3 are emitted
    // again.
    reset(collector);

    t1 = 375000;
    bolt.setCurrentTime(t1);

    sendTickTuple();

    // Ensure that subAlarm3 is still ALARM. subAlarm2 is still OK but because the metric
    // that triggered ALARM is in the future bucket
    // subAlarm4 is still OK due to missing metrics
    assertEquals(subAlarm2.getState(), AlarmState.OK);
    assertEquals(subAlarm3.getState(), AlarmState.ALARM);
    assertEquals(subAlarm4.getState(), AlarmState.OK);
    verify(collector, never()).emit(new Values(subAlarm2.getAlarmId(), subAlarm2));
    verify(collector, never()).emit(new Values(subAlarm3.getAlarmId(), subAlarm3));
    verify(collector, never()).emit(new Values(subAlarm4.getAlarmId(), subAlarm4));
  }

  private void sendTickTuple() {
    final Tuple tickTuple = createTickTuple();
    bolt.execute(tickTuple);
    verify(collector, times(1)).ack(tickTuple);
  }

  public void shouldSendAlarmAgain() {

    long t1 = 12000;
    bolt.setCurrentTime(t1);
    sendSubAlarmCreated(metricDef2, subAlarm2);

    bolt.execute(createMetricTuple(metricDef2, new Metric(metricDef2, t1, 100, null)));
    bolt.execute(createMetricTuple(metricDef2, new Metric(metricDef2, t1 += 1000, 95, null)));
    bolt.execute(createMetricTuple(metricDef2, new Metric(metricDef2, t1 += 1000, 88, null)));

    t1 += 60000;
    bolt.setCurrentTime(t1);
    sendTickTuple();
    verify(collector, times(1)).emit(new Values(subAlarm2.getAlarmId(), subAlarm2));
    assertEquals(subAlarm2.getState(), AlarmState.ALARM);

    sendSubAlarmResend(metricDef2, subAlarm2);

    bolt.execute(createMetricTuple(metricDef2, new Metric(metricDef2, t1, 100, null)));
    bolt.execute(createMetricTuple(metricDef2, new Metric(metricDef2, t1 += 1000, 95, null)));
    bolt.execute(createMetricTuple(metricDef2, new Metric(metricDef2, t1 += 1000, 88, null)));

    t1 += 60000;
    bolt.setCurrentTime(t1);
    sendTickTuple();
    assertEquals(subAlarm2.getState(), AlarmState.ALARM);
    verify(collector, times(2)).emit(new Values(subAlarm2.getAlarmId(), subAlarm2));
  }

  private void sendSubAlarmCreated(MetricDefinition metricDef, SubAlarm subAlarm) {
    final MkTupleParam tupleParam = new MkTupleParam();
    tupleParam.setFields(AlarmCreationBolt.ALARM_CREATION_FIELDS);
    tupleParam.setStream(AlarmCreationBolt.ALARM_CREATION_STREAM);
    final String alarmDefinitionString = ""; // TODO - Figure out what this needs to be
    final Tuple tuple =
        Testing.testTuple(Arrays.asList(EventProcessingBolt.CREATED, new TenantIdAndMetricName(
            TENANT_ID, metricDef.name), new MetricDefinitionAndTenantId(metricDef, TENANT_ID),
            alarmDefinitionString, subAlarm), tupleParam);
    bolt.execute(tuple);
  }

  private void sendSubAlarmResend(MetricDefinition metricDef, SubAlarm subAlarm) {
    sendSubAlarmMsg(EventProcessingBolt.RESEND, metricDef, subAlarm);
  }

  private void sendSubAlarmMsg(String command, MetricDefinition metricDef, SubAlarm subAlarm) {
    final MkTupleParam tupleParam = new MkTupleParam();
    tupleParam.setFields(EventProcessingBolt.METRIC_ALARM_EVENT_STREAM_FIELDS);
    tupleParam.setStream(EventProcessingBolt.METRIC_ALARM_EVENT_STREAM_ID);
    final String alarmDefinitionId = "";
    final MetricDefinitionAndTenantId mdtid = new MetricDefinitionAndTenantId(metricDef, TENANT_ID);
    final Tuple tuple =
        Testing.testTuple(Arrays.asList(command, new TenantIdAndMetricName(mdtid), mdtid,
            alarmDefinitionId, subAlarm.getId()), tupleParam);
    bolt.execute(tuple);
  }

  public void shouldSendUndeterminedIfStateChanges() {
    long t1 = 50000;
    bolt.setCurrentTime(t1);
    sendSubAlarmCreated(metricDef2, subAlarm2);
    bolt.execute(createMetricTuple(metricDef2, new Metric(metricDef2, t1, 1.0, null)));
    t1 += 1000;
    bolt.execute(createMetricTuple(metricDef2, new Metric(metricDef2, t1, 1.0, null)));

    bolt.setCurrentTime(t1 += 60000);
    sendTickTuple();
    assertEquals(subAlarm2.getState(), AlarmState.OK);

    bolt.setCurrentTime(t1 += 60000);
    sendTickTuple();
    assertEquals(subAlarm2.getState(), AlarmState.OK);
    verify(collector, times(1)).emit(new Values(subAlarm2.getAlarmId(), subAlarm2));

    // Have to reset the mock so it can tell the difference when subAlarm2 is emitted again.
    reset(collector);

    bolt.setCurrentTime(t1 += 60000);
    sendTickTuple();
    assertEquals(subAlarm2.getState(), AlarmState.UNDETERMINED);
    verify(collector, times(1)).emit(new Values(subAlarm2.getAlarmId(), subAlarm2));
  }

  public void shouldSendOkAfterAlarmIfNoMetrics() {
    long t1 = 50000;
    bolt.setCurrentTime(t1);
    sendSubAlarmCreated(metricDef4, subAlarm4);
    bolt.execute(createMetricTuple(metricDef4, new Metric(metricDef4, t1, 1.0, null)));
    t1 += 1000;
    bolt.execute(createMetricTuple(metricDef4, new Metric(metricDef4, t1, 1.0, null)));
    t1 += 1000;
    bolt.execute(createMetricTuple(metricDef4, new Metric(metricDef4, t1, 1.0, null)));
    t1 += 1000;
    bolt.execute(createMetricTuple(metricDef4, new Metric(metricDef4, t1, 1.0, null)));
    t1 += 1000;
    bolt.execute(createMetricTuple(metricDef4, new Metric(metricDef4, t1, 1.0, null)));

    bolt.setCurrentTime(t1 += 60000);
    sendTickTuple();
    assertEquals(subAlarm4.getState(), AlarmState.ALARM);
    verify(collector, times(1)).emit(new Values(subAlarm4.getAlarmId(), subAlarm4));

    // Have to reset the mock so it can tell the difference when subAlarm4 is emitted again.
    reset(collector);

    bolt.setCurrentTime(t1 += 60000);
    sendTickTuple();
    assertEquals(subAlarm4.getState(), AlarmState.OK);
    verify(collector, times(1)).emit(new Values(subAlarm4.getAlarmId(), subAlarm4));
  }

  public void shouldNeverLeaveOkIfThresholdNotExceededForDeterministic() {
    long t1 = 50000;
    bolt.setCurrentTime(t1);
    sendSubAlarmCreated(metricDef4, subAlarm4);
    bolt.execute(createMetricTuple(metricDef4, new Metric(metricDef4, t1, 1.0, null)));
    t1 += 1000;
    bolt.execute(createMetricTuple(metricDef4, new Metric(metricDef4, t1, 1.0, null)));

    bolt.setCurrentTime(t1 += 60000);
    sendTickTuple();
    assertEquals(subAlarm4.getState(), AlarmState.OK);

    bolt.setCurrentTime(t1 += 60000);
    sendTickTuple();
    assertEquals(subAlarm4.getState(), AlarmState.OK);
    verify(collector, times(1)).emit(new Values(subAlarm4.getAlarmId(), subAlarm4));

    // Have to reset the mock so it can tell the difference when subAlarm4 is emitted again.
    reset(collector);

    bolt.setCurrentTime(t1 += 60000);
    sendTickTuple();
    assertEquals(subAlarm4.getState(), AlarmState.OK);
    verify(collector, never()).emit(new Values(subAlarm4.getAlarmId(), subAlarm4));
  }

  public void shouldSendUndeterminedOnStartup() {
    long t1 = 14000;
    bolt.setCurrentTime(t1);
    sendSubAlarmCreated(metricDef2, subAlarm2);

    final MkTupleParam tupleParam = new MkTupleParam();
    tupleParam.setStream(MetricAggregationBolt.METRIC_AGGREGATION_CONTROL_STREAM);
    final Tuple lagTuple =
        Testing.testTuple(Arrays.asList(MetricAggregationBolt.METRICS_BEHIND), tupleParam);
    bolt.execute(lagTuple);
    verify(collector, times(1)).ack(lagTuple);

    t1 += 60000;
    bolt.setCurrentTime(t1);
    sendTickTuple();
    verify(collector, never()).emit(new Values(subAlarm2.getAlarmId(), subAlarm2));

    t1 += 60000;
    bolt.setCurrentTime(t1);
    sendTickTuple();
    verify(collector, never()).emit(new Values(subAlarm2.getAlarmId(), subAlarm2));

    t1 += 60000;
    bolt.setCurrentTime(t1);
    sendTickTuple();
    assertEquals(subAlarm2.getState(), AlarmState.UNDETERMINED);

    verify(collector, times(1)).emit(new Values(subAlarm2.getAlarmId(), subAlarm2));
  }

  public void shouldSendOKOnStartupForDeterministic() {
    long t1 = 14000;
    bolt.setCurrentTime(t1);
    sendSubAlarmCreated(metricDef4, subAlarm4);

    final MkTupleParam tupleParam = new MkTupleParam();
    tupleParam.setStream(MetricAggregationBolt.METRIC_AGGREGATION_CONTROL_STREAM);
    final Tuple lagTuple =
        Testing.testTuple(Arrays.asList(MetricAggregationBolt.METRICS_BEHIND), tupleParam);
    bolt.execute(lagTuple);
    verify(collector, times(1)).ack(lagTuple);

    // Won't send OK on this Tick Tuple because of the METRIC_BEHIND message sent above
    t1 += 60000;
    bolt.setCurrentTime(t1);
    sendTickTuple();
    verify(collector, never()).emit(new Values(subAlarm4.getAlarmId(), subAlarm4));

    t1 += 60000;
    bolt.setCurrentTime(t1);
    sendTickTuple();
    assertEquals(subAlarm4.getState(), AlarmState.OK);

    verify(collector, times(1)).emit(new Values(subAlarm4.getAlarmId(), subAlarm4));
  }

  private Tuple createTickTuple() {
    final MkTupleParam tupleParam = new MkTupleParam();
    tupleParam.setComponent(Constants.SYSTEM_COMPONENT_ID);
    tupleParam.setStream(Constants.SYSTEM_TICK_STREAM_ID);
    final Tuple tickTuple = Testing.testTuple(Arrays.asList(), tupleParam);
    return tickTuple;
  }

  public void validateMetricDefAdded() {

    MetricDefinitionAndTenantId metricDefinitionAndTenantId =
        new MetricDefinitionAndTenantId(metricDef1, TENANT_ID);
    assertNull(bolt.metricDefToSubAlarmStatsRepos.get(metricDefinitionAndTenantId));

    sendSubAlarmCreated(metricDef1, subAlarm1);

    assertNotNull(bolt.metricDefToSubAlarmStatsRepos.get(metricDefinitionAndTenantId).get(ALARM_ID_1));
  }

  public void validateMetricDefUpdatedThreshold() {
    final SubAlarmStats stats =
        updateEnsureMeasurementsKept(subExpr2, "avg(hpcs.compute.mem{id=5}, 60) >= 80");
    assertEquals(stats.getSubAlarm().getExpression().getThreshold(), 80.0);
  }

  public void validateMetricDefUpdatedOperator() {
    final SubAlarmStats stats =
        updateEnsureMeasurementsKept(subExpr2, "avg(hpcs.compute.mem{id=5}, 60) < 80");
    assertEquals(stats.getSubAlarm().getExpression().getOperator(), AlarmOperator.LT);
  }

  private SubAlarmStats updateEnsureMeasurementsKept(SubExpression subExpr,
      String newSubExpression) {
    final SubAlarmStats stats = updateSubAlarmsStats(subExpr, newSubExpression);
    final double[] values = stats.getStats().getWindowValues();
    assertFalse(Double.isNaN(values[0])); // Ensure old measurements weren't flushed
    return stats;
  }

  public void validateMetricDefReplacedFunction() {
    final SubAlarmStats stats =
        updateEnsureMeasurementsFlushed(subExpr2, "max(hpcs.compute.mem{id=5}, 60) < 80");
    assertEquals(stats.getSubAlarm().getExpression().getOperator(), AlarmOperator.LT);
  }

  public void validateMetricDefReplacedPeriods() {
    final SubAlarmStats stats =
        updateEnsureMeasurementsFlushed(subExpr2, "avg(hpcs.compute.mem{id=5}, 60) >= 80 times 7");
    assertEquals(stats.getSubAlarm().getExpression().getPeriods(), 7);
  }

  public void validateMetricDefReplacedPeriod() {
    final SubAlarmStats stats =
        updateEnsureMeasurementsFlushed(subExpr2, "avg(hpcs.compute.mem{id=5}, 120) >= 80");
    assertEquals(stats.getSubAlarm().getExpression().getPeriod(), 120);
  }

  private SubAlarmStats updateEnsureMeasurementsFlushed(SubExpression subExpr,
      String newSubExpression) {
    final SubAlarmStats stats = updateSubAlarmsStats(subExpr, newSubExpression);
    final double[] values = stats.getStats().getWindowValues();
    assertTrue(Double.isNaN(values[0])); // Ensure old measurements were flushed
    return stats;
  }

  private SubAlarmStats updateSubAlarmsStats(SubExpression subExpr, String newSubExpression) {

    final MetricDefinitionAndTenantId metricDefinitionAndTenantId =
        new MetricDefinitionAndTenantId(subExpr.getAlarmSubExpression().getMetricDefinition(), TENANT_ID);
    assertNull(bolt.metricDefToSubAlarmStatsRepos.get(metricDefinitionAndTenantId));

    SubAlarm subAlarm = new SubAlarm(ALARM_ID_1, "1", subExpr);
    sendSubAlarmCreated(metricDefinitionAndTenantId.metricDefinition, subAlarm);

    final SubAlarmStats oldStats =
        bolt.metricDefToSubAlarmStatsRepos.get(metricDefinitionAndTenantId).get(ALARM_ID_1);
    assertEquals(oldStats.getSubAlarm().getExpression().getThreshold(), 90.0);
    assertTrue(oldStats.getStats().addValue(80.0, System.currentTimeMillis() / 1000));
    assertFalse(Double.isNaN(oldStats.getStats().getWindowValues()[0]));
    assertNotNull(bolt.metricDefToSubAlarmStatsRepos.get(metricDefinitionAndTenantId).get(ALARM_ID_1));

    final SubExpression newExpr =
        new SubExpression(subExpr.getId(), AlarmSubExpression.of(newSubExpression));

    final MkTupleParam tupleParam = new MkTupleParam();
    tupleParam.setFields(EventProcessingBolt.METRIC_SUB_ALARM_EVENT_STREAM_FIELDS);
    tupleParam.setStream(EventProcessingBolt.METRIC_SUB_ALARM_EVENT_STREAM_ID);
    final String alarmDefinitionId = "42"; // Not used by MetricAggregationBolt
    bolt.execute(Testing.testTuple(Arrays.asList(EventProcessingBolt.UPDATED,
        newExpr, alarmDefinitionId), tupleParam));

    return bolt.metricDefToSubAlarmStatsRepos.get(metricDefinitionAndTenantId).get(ALARM_ID_1);
  }

  public void validateMetricDefDeleted() {
    MkTupleParam tupleParam = new MkTupleParam();
    tupleParam.setFields(EventProcessingBolt.METRIC_ALARM_EVENT_STREAM_FIELDS);
    tupleParam.setStream(EventProcessingBolt.METRIC_ALARM_EVENT_STREAM_ID);
    MetricDefinitionAndTenantId metricDefinitionAndTenantId =
        new MetricDefinitionAndTenantId(metricDef1, TENANT_ID);
    bolt.getOrCreateSubAlarmStatsRepo(metricDefinitionAndTenantId);

    sendSubAlarmCreated(metricDef1, subAlarm1);

    assertNotNull(bolt.metricDefToSubAlarmStatsRepos.get(metricDefinitionAndTenantId).get(ALARM_ID_1));

    // We don't have an AlarmDefinition so no id, but the MetricAggregationBolt doesn't use this
    // field anyways
    final String alarmDefinitionId = "";
    bolt.execute(Testing.testTuple(Arrays.asList(EventProcessingBolt.DELETED,
        new TenantIdAndMetricName(metricDefinitionAndTenantId), metricDefinitionAndTenantId,
        alarmDefinitionId, ALARM_ID_1), tupleParam));

    assertNull(bolt.metricDefToSubAlarmStatsRepos.get(metricDefinitionAndTenantId));
    assertTrue(bolt.subAlarmRemoved(ALARM_ID_1, metricDefinitionAndTenantId));
  }

  private Tuple createMetricTuple(final MetricDefinition metricDef, final Metric metric) {
    final MkTupleParam tupleParam = new MkTupleParam();
    tupleParam.setFields(MetricFilteringBolt.FIELDS);
    tupleParam.setStream(Streams.DEFAULT_STREAM_ID);
    return Testing.testTuple(
        Arrays.asList(new TenantIdAndMetricName(TENANT_ID, metric.name), metric), tupleParam);
  }

  private static class MockMetricAggregationBolt extends MetricAggregationBolt {
    private static final long serialVersionUID = 1L;

    private long currentTime;

    public MockMetricAggregationBolt(ThresholdingConfiguration config) {
      super(config);
    }

    @Override
    protected long currentTimeSeconds() {
      if (currentTime != 0) {
        return currentTime;
      }
      return super.currentTimeSeconds();
    }

    public void setCurrentTime(long millis) {
      this.currentTime = millis/1000;
    }
  }
}
