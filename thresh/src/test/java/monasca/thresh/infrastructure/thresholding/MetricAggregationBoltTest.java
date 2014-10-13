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
import monasca.common.streaming.storm.Streams;

import backtype.storm.Constants;
import backtype.storm.Testing;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.MkTupleParam;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import monasca.thresh.domain.model.MetricDefinitionAndTenantId;
import monasca.thresh.domain.model.SubAlarm;
import monasca.thresh.domain.model.SubAlarmStats;
import monasca.thresh.domain.model.TenantIdAndMetricName;
import monasca.thresh.domain.service.SubAlarmStatsRepository;

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
  private AlarmSubExpression subExpr1;
  private AlarmSubExpression subExpr2;
  private AlarmSubExpression subExpr3;
  private MetricDefinition metricDef1;
  private MetricDefinition metricDef2;
  private MetricDefinition metricDef3;

  @BeforeClass
  protected void beforeClass() {
    // Other tests set this and that can cause problems when the test is run from Maven
    System.clearProperty(MetricAggregationBolt.TICK_TUPLE_SECONDS_KEY);
    subExpr1 = AlarmSubExpression.of("avg(hpcs.compute.cpu{id=5}, 60) >= 90 times 3");
    subExpr2 = AlarmSubExpression.of("avg(hpcs.compute.mem{id=5}, 60) >= 90");
    subExpr3 = AlarmSubExpression.of("avg(hpcs.compute.mem{id=5}, 60) >= 96");
    metricDef1 = subExpr1.getMetricDefinition();
    metricDef2 = subExpr2.getMetricDefinition();
    metricDef3 = subExpr3.getMetricDefinition();
  }

  @BeforeMethod
  protected void beforeMethod() {
    // Fixtures
    subAlarm1 = new SubAlarm(ALARM_ID_1, "1", subExpr1, AlarmState.UNDETERMINED);
    subAlarm2 = new SubAlarm("456", "1", subExpr2, AlarmState.UNDETERMINED);
    subAlarm3 = new SubAlarm("789", "2", subExpr3, AlarmState.UNDETERMINED);
    subAlarms = new ArrayList<>();
    subAlarms.add(subAlarm1);
    subAlarms.add(subAlarm2);
    subAlarms.add(subAlarm3);

    bolt = new MockMetricAggregationBolt();
    context = mock(TopologyContext.class);
    collector = mock(OutputCollector.class);
    bolt.prepare(null, context, collector);
  }

  public void shouldAggregateValues() {

    sendSubAlarmCreated(metricDef1, subAlarm1);
    sendSubAlarmCreated(metricDef2, subAlarm2);

    long t1 = System.currentTimeMillis() / 1000;

    bolt.aggregateValues(new MetricDefinitionAndTenantId(metricDef1, TENANT_ID), new Metric(
        metricDef1.name, metricDef1.dimensions, t1, 100));
    bolt.aggregateValues(new MetricDefinitionAndTenantId(metricDef1, TENANT_ID), new Metric(
        metricDef1.name, metricDef1.dimensions, t1, 80));
    bolt.aggregateValues(new MetricDefinitionAndTenantId(metricDef2, TENANT_ID), new Metric(
        metricDef2.name, metricDef2.dimensions, t1, 50));
    bolt.aggregateValues(new MetricDefinitionAndTenantId(metricDef2, TENANT_ID), new Metric(
        metricDef2.name, metricDef2.dimensions, t1, 40));

    SubAlarmStatsRepository orCreateSubAlarmStatsRepo = bolt.getOrCreateSubAlarmStatsRepo(new MetricDefinitionAndTenantId(metricDef1, TENANT_ID));
    SubAlarmStats alarmData =
        orCreateSubAlarmStatsRepo
            .get(subAlarm1.getId());
    assertEquals(alarmData.getStats().getValue(t1), 90.0);

    alarmData =
        bolt.getOrCreateSubAlarmStatsRepo(new MetricDefinitionAndTenantId(metricDef2, TENANT_ID))
            .get(subAlarm2.getId());
    assertEquals(alarmData.getStats().getValue(t1), 45.0);
  }

  public void shouldEvaluateAlarms() {
    // Ensure subAlarm2 and subAlarm3 map to the same Metric Definition
    assertEquals(metricDef3, metricDef2);

    sendSubAlarmCreated(metricDef1, subAlarm1);
    sendSubAlarmCreated(metricDef2, subAlarm2);
    sendSubAlarmCreated(metricDef3, subAlarm3);

    // Send metrics for subAlarm1
    long t1 = System.currentTimeMillis() / 1000;
    bolt.execute(createMetricTuple(metricDef1, new Metric(metricDef1, t1, 100)));
    bolt.execute(createMetricTuple(metricDef1, new Metric(metricDef1, t1 -= 60, 95)));
    bolt.execute(createMetricTuple(metricDef1, new Metric(metricDef1, t1 -= 60, 88)));

    final Tuple tickTuple = createTickTuple();
    bolt.execute(tickTuple);
    verify(collector, times(1)).ack(tickTuple);

    assertEquals(subAlarm1.getState(), AlarmState.OK);
    assertEquals(subAlarm2.getState(), AlarmState.UNDETERMINED);
    assertEquals(subAlarm3.getState(), AlarmState.UNDETERMINED);

    verify(collector, times(1)).emit(new Values(subAlarm1.getAlarmId(), subAlarm1));
    // Have to reset the mock so it can tell the difference when subAlarm2 and subAlarm3 are emitted
    // again.
    reset(collector);

    // Drive subAlarm1 to ALARM
    bolt.execute(createMetricTuple(metricDef1, new Metric(metricDef1, t1, 99)));
    // Drive subAlarm2 to ALARM and subAlarm3 to OK since they use the same MetricDefinition
    bolt.execute(createMetricTuple(metricDef2, new Metric(metricDef2,
        System.currentTimeMillis() / 1000, 94)));
    bolt.execute(tickTuple);
    verify(collector, times(1)).ack(tickTuple);

    assertEquals(subAlarm1.getState(), AlarmState.ALARM);
    assertEquals(subAlarm2.getState(), AlarmState.ALARM);
    assertEquals(subAlarm3.getState(), AlarmState.OK);
    verify(collector, times(1)).emit(new Values(subAlarm1.getAlarmId(), subAlarm1));
    verify(collector, times(1)).emit(new Values(subAlarm2.getAlarmId(), subAlarm2));
    verify(collector, times(1)).emit(new Values(subAlarm3.getAlarmId(), subAlarm3));
  }

  public void shouldSendAlarmAgain() {

    long t1 = 10;
    bolt.setCurrentTime(t1);
    sendSubAlarmCreated(metricDef2, subAlarm2);

    bolt.execute(createMetricTuple(metricDef2, new Metric(metricDef2, t1, 100)));
    bolt.execute(createMetricTuple(metricDef2, new Metric(metricDef2, t1++, 95)));
    bolt.execute(createMetricTuple(metricDef2, new Metric(metricDef2, t1++, 88)));

    t1 += 60;
    bolt.setCurrentTime(t1);
    final Tuple tickTuple = createTickTuple();
    bolt.execute(tickTuple);
    verify(collector, times(1)).emit(new Values(subAlarm2.getAlarmId(), subAlarm2));
    assertEquals(subAlarm2.getState(), AlarmState.ALARM);
    verify(collector, times(1)).ack(tickTuple);

    sendSubAlarmResend(metricDef2, subAlarm2);

    bolt.execute(createMetricTuple(metricDef2, new Metric(metricDef2, t1, 100)));
    bolt.execute(createMetricTuple(metricDef2, new Metric(metricDef2, t1++, 95)));
    bolt.execute(createMetricTuple(metricDef2, new Metric(metricDef2, t1++, 88)));

    t1 += 60;
    bolt.setCurrentTime(t1);
    bolt.execute(tickTuple);
    verify(collector, times(2)).ack(tickTuple);

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
    sendSubAlarmCreated(metricDef2, subAlarm2);
    long t1 = System.currentTimeMillis() / 1000;
    bolt.setCurrentTime(t1);
    bolt.execute(createMetricTuple(metricDef2, new Metric(metricDef2, t1, 1.0)));
    t1 += 1;
    bolt.execute(createMetricTuple(metricDef2, new Metric(metricDef2, t1, 1.0)));

    bolt.setCurrentTime(t1 += 60);
    final Tuple tickTuple = createTickTuple();
    bolt.execute(tickTuple);
    assertEquals(subAlarm2.getState(), AlarmState.OK);

    bolt.setCurrentTime(t1 += 60);
    bolt.execute(tickTuple);
    assertEquals(subAlarm2.getState(), AlarmState.OK);
    verify(collector, times(1)).emit(new Values(subAlarm2.getAlarmId(), subAlarm2));

    // Have to reset the mock so it can tell the difference when subAlarm2 is emitted again.
    reset(collector);

    bolt.setCurrentTime(t1 += 60);
    bolt.execute(tickTuple);
    assertEquals(subAlarm2.getState(), AlarmState.UNDETERMINED);
    verify(collector, times(1)).emit(new Values(subAlarm2.getAlarmId(), subAlarm2));
  }

  public void shouldSendUndeterminedOnStartup() {
    sendSubAlarmCreated(metricDef2, subAlarm2);

    final MkTupleParam tupleParam = new MkTupleParam();
    tupleParam.setStream(MetricAggregationBolt.METRIC_AGGREGATION_CONTROL_STREAM);
    final Tuple lagTuple =
        Testing.testTuple(Arrays.asList(MetricAggregationBolt.METRICS_BEHIND), tupleParam);
    bolt.execute(lagTuple);
    verify(collector, times(1)).ack(lagTuple);

    final Tuple tickTuple = createTickTuple();
    bolt.execute(tickTuple);
    verify(collector, times(1)).ack(tickTuple);
    verify(collector, never()).emit(new Values(subAlarm2.getAlarmId(), subAlarm2));

    bolt.execute(tickTuple);
    verify(collector, times(2)).ack(tickTuple);
    verify(collector, never()).emit(new Values(subAlarm2.getAlarmId(), subAlarm2));

    bolt.execute(tickTuple);
    verify(collector, times(3)).ack(tickTuple);
    assertEquals(subAlarm2.getState(), AlarmState.UNDETERMINED);

    verify(collector, times(1)).emit(new Values(subAlarm2.getAlarmId(), subAlarm2));
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

  private SubAlarmStats updateEnsureMeasurementsKept(AlarmSubExpression subExpr,
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

  private SubAlarmStats updateEnsureMeasurementsFlushed(AlarmSubExpression subExpr,
      String newSubExpression) {
    final SubAlarmStats stats = updateSubAlarmsStats(subExpr, newSubExpression);
    final double[] values = stats.getStats().getWindowValues();
    assertTrue(Double.isNaN(values[0])); // Ensure old measurements were flushed
    return stats;
  }

  private SubAlarmStats updateSubAlarmsStats(AlarmSubExpression subExpr, String newSubExpression) {

    final MkTupleParam tupleParam = new MkTupleParam();
    tupleParam.setFields(EventProcessingBolt.METRIC_SUB_ALARM_EVENT_STREAM_FIELDS);
    tupleParam.setStream(EventProcessingBolt.METRIC_SUB_ALARM_EVENT_STREAM_ID);

    final MetricDefinitionAndTenantId metricDefinitionAndTenantId =
        new MetricDefinitionAndTenantId(subExpr.getMetricDefinition(), TENANT_ID);
    assertNull(bolt.metricDefToSubAlarmStatsRepos.get(metricDefinitionAndTenantId));

    SubAlarm subAlarm = new SubAlarm(ALARM_ID_1, "1", subExpr);
    sendSubAlarmCreated(metricDefinitionAndTenantId.metricDefinition, subAlarm);

    bolt.execute(Testing.testTuple(Arrays.asList(EventProcessingBolt.CREATED,
        new TenantIdAndMetricName(metricDefinitionAndTenantId), metricDefinitionAndTenantId,
        subAlarm), tupleParam));
    final SubAlarmStats oldStats =
        bolt.metricDefToSubAlarmStatsRepos.get(metricDefinitionAndTenantId).get(ALARM_ID_1);
    assertEquals(oldStats.getSubAlarm().getExpression().getThreshold(), 90.0);
    assertTrue(oldStats.getStats().addValue(80.0, System.currentTimeMillis() / 1000));
    assertFalse(Double.isNaN(oldStats.getStats().getWindowValues()[0]));
    assertNotNull(bolt.metricDefToSubAlarmStatsRepos.get(metricDefinitionAndTenantId).get(ALARM_ID_1));

    final AlarmSubExpression newExpr = AlarmSubExpression.of(newSubExpression);
    bolt.execute(Testing.testTuple(Arrays.asList(EventProcessingBolt.UPDATED,
        new TenantIdAndMetricName(metricDefinitionAndTenantId), metricDefinitionAndTenantId,
        new SubAlarm(ALARM_ID_1, "1", newExpr)), tupleParam));

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

    public MockMetricAggregationBolt() {
      super();
    }

    @Override
    protected long currentTimeSeconds() {
      if (currentTime != 0) {
        return currentTime;
      }
      return super.currentTimeSeconds();
    }

    public void setCurrentTime(long currentTime) {
      this.currentTime = currentTime;
    }
  }
}
