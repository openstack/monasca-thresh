package com.hpcloud.mon.infrastructure.thresholding;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.reset;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import backtype.storm.Constants;
import backtype.storm.Testing;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.MkTupleParam;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.hpcloud.mon.common.model.alarm.AlarmState;
import com.hpcloud.mon.common.model.alarm.AlarmSubExpression;
import com.hpcloud.mon.common.model.metric.Metric;
import com.hpcloud.mon.common.model.metric.MetricDefinition;
import com.hpcloud.mon.domain.model.MetricDefinitionAndTenantId;
import com.hpcloud.mon.domain.model.SubAlarm;
import com.hpcloud.mon.domain.model.SubAlarmStats;
import com.hpcloud.mon.domain.service.SubAlarmDAO;
import com.hpcloud.mon.domain.service.SubAlarmStatsRepository;
import com.hpcloud.streaming.storm.Streams;

/**
 * @author Jonathan Halterman
 */
@Test
public class MetricAggregationBoltTest {
  private static final String TENANT_ID = "42";
  private MetricAggregationBolt bolt;
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
    subAlarm1 = new SubAlarm("123", "1", subExpr1, AlarmState.OK);
    subAlarm2 = new SubAlarm("456", "1", subExpr2, AlarmState.OK);
    subAlarm3 = new SubAlarm("789", "2", subExpr3, AlarmState.ALARM);
    subAlarms = new ArrayList<>();
    subAlarms.add(subAlarm1);
    subAlarms.add(subAlarm2);
    subAlarms.add(subAlarm3);

    final SubAlarmDAO dao = mock(SubAlarmDAO.class);
    when(dao.find(any(MetricDefinitionAndTenantId.class))).thenAnswer(new Answer<List<SubAlarm>>() {
      @Override
      public List<SubAlarm> answer(InvocationOnMock invocation) throws Throwable {
        final MetricDefinitionAndTenantId metricDefinitionAndTenantId = (MetricDefinitionAndTenantId) invocation.getArguments()[0];
        final List<SubAlarm> result = new ArrayList<>();
        for (final SubAlarm subAlarm : subAlarms)
          if (subAlarm.getExpression().getMetricDefinition().equals(metricDefinitionAndTenantId.metricDefinition))
            result.add(subAlarm);
        return result;
      }
    });

    bolt = new MetricAggregationBolt(dao);
    context = mock(TopologyContext.class);
    collector = mock(OutputCollector.class);
    bolt.prepare(null, context, collector);
  }

  public void shouldAggregateValues() {
    long t1 = System.currentTimeMillis() / 1000;

    bolt.aggregateValues(new MetricDefinitionAndTenantId(metricDef1, TENANT_ID), new Metric(metricDef1.name, metricDef1.dimensions, t1, 100));
    bolt.aggregateValues(new MetricDefinitionAndTenantId(metricDef1, TENANT_ID), new Metric(metricDef1.name, metricDef1.dimensions, t1, 80));
    bolt.aggregateValues(new MetricDefinitionAndTenantId(metricDef2, TENANT_ID), new Metric(metricDef2.name, metricDef2.dimensions, t1, 50));
    bolt.aggregateValues(new MetricDefinitionAndTenantId(metricDef2, TENANT_ID), new Metric(metricDef2.name, metricDef2.dimensions, t1, 40));

    SubAlarmStats alarmData = bolt.getOrCreateSubAlarmStatsRepo(new MetricDefinitionAndTenantId(metricDef1, TENANT_ID)).get("123");
    assertEquals(alarmData.getStats().getValue(t1), 90.0);

    alarmData = bolt.getOrCreateSubAlarmStatsRepo(new MetricDefinitionAndTenantId(metricDef2, TENANT_ID)).get("456");
    assertEquals(alarmData.getStats().getValue(t1), 45.0);
  }

  public void shouldEvaluateAlarms() {
    // Ensure subAlarm2 and subAlarm3 map to the same Metric Definition
    assertEquals(metricDef3, metricDef2);

    bolt.execute(createMetricTuple(metricDef2, null));

    // Send metrics for subAlarm1
    long t1 = System.currentTimeMillis() / 1000;
    bolt.execute(createMetricTuple(metricDef1, new Metric(metricDef1, t1, 100)));
    bolt.execute(createMetricTuple(metricDef1, new Metric(metricDef1, t1 -= 60, 95)));
    bolt.execute(createMetricTuple(metricDef1, new Metric(metricDef1, t1 -= 60, 88)));

    final Tuple tickTuple = createTickTuple();
    bolt.execute(tickTuple);

    assertEquals(subAlarm1.getState(), AlarmState.OK);
    assertEquals(subAlarm2.getState(), AlarmState.UNDETERMINED);
    assertEquals(subAlarm3.getState(), AlarmState.UNDETERMINED);

    verify(collector, times(1)).emit(new Values(subAlarm2.getAlarmId(), subAlarm2));
    verify(collector, times(1)).emit(new Values(subAlarm3.getAlarmId(), subAlarm3));
    // Have to reset the mock so it can tell the difference when subAlarm2 and subAlarm3 are emitted again.
    reset(collector);

    bolt.execute(createMetricTuple(metricDef1, new Metric(metricDef1, t1, 99)));
    bolt.execute(createMetricTuple(metricDef2, new Metric(metricDef2, System.currentTimeMillis() / 1000, 94)));
    bolt.execute(tickTuple);

    assertEquals(subAlarm1.getState(), AlarmState.ALARM);
    assertEquals(subAlarm2.getState(), AlarmState.ALARM);
    assertEquals(subAlarm3.getState(), AlarmState.OK);
    verify(collector, times(1)).emit(new Values(subAlarm1.getAlarmId(), subAlarm1));
    verify(collector, times(1)).emit(new Values(subAlarm2.getAlarmId(), subAlarm2));
    verify(collector, times(1)).emit(new Values(subAlarm3.getAlarmId(), subAlarm3));
  }

  public void shouldSendUndeterminedIfStateChanges() {

    assertNotEquals(AlarmState.UNDETERMINED, subAlarm2.getState());
    bolt.execute(createMetricTuple(metricDef2, null));

    final Tuple tickTuple = createTickTuple();
    bolt.execute(tickTuple);

    assertEquals(AlarmState.UNDETERMINED, subAlarm2.getState());
    verify(collector, times(1)).emit(new Values(subAlarm2.getAlarmId(), subAlarm2));
  }

  public void shouldSendUndeterminedOnStartup() {

    subAlarm2.setNoState(true);
    subAlarm2.setState(AlarmState.UNDETERMINED);
    bolt.execute(createMetricTuple(metricDef2, null));

    final Tuple tickTuple = createTickTuple();
    bolt.execute(tickTuple);

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
    MkTupleParam tupleParam = new MkTupleParam();
    tupleParam.setFields(EventProcessingBolt.METRIC_SUB_ALARM_EVENT_STREAM_FIELDS);
    tupleParam.setStream(EventProcessingBolt.METRIC_SUB_ALARM_EVENT_STREAM_ID);

    MetricDefinitionAndTenantId metricDefinitionAndTenantId = new MetricDefinitionAndTenantId(metricDef1, TENANT_ID);
    assertNull(bolt.subAlarmStatsRepos.get(metricDefinitionAndTenantId));

    bolt.execute(Testing.testTuple(Arrays.asList(EventProcessingBolt.CREATED,
            metricDefinitionAndTenantId, new SubAlarm("123", "1", subExpr1)), tupleParam));

    assertNotNull(bolt.subAlarmStatsRepos.get(metricDefinitionAndTenantId).get("123"));
  }

  public void validateMetricDefDeleted() {
    MkTupleParam tupleParam = new MkTupleParam();
    tupleParam.setFields(EventProcessingBolt.METRIC_ALARM_EVENT_STREAM_FIELDS);
    tupleParam.setStream(EventProcessingBolt.METRIC_ALARM_EVENT_STREAM_ID);
    MetricDefinitionAndTenantId metricDefinitionAndTenantId = new MetricDefinitionAndTenantId(metricDef1, TENANT_ID);
    bolt.getOrCreateSubAlarmStatsRepo(metricDefinitionAndTenantId);

    assertNotNull(bolt.subAlarmStatsRepos.get(metricDefinitionAndTenantId).get("123"));

    bolt.execute(Testing.testTuple(
        Arrays.asList(EventProcessingBolt.DELETED, metricDefinitionAndTenantId, "123"), tupleParam));

    assertNull(bolt.subAlarmStatsRepos.get(metricDefinitionAndTenantId));
  }

  public void shouldGetOrCreateSameMetricData() {
    SubAlarmStatsRepository data = bolt.getOrCreateSubAlarmStatsRepo(new MetricDefinitionAndTenantId(metricDef1, TENANT_ID));
    assertNotNull(data);
    assertEquals(bolt.getOrCreateSubAlarmStatsRepo(new MetricDefinitionAndTenantId(metricDef1, TENANT_ID)), data);
  }

  private Tuple createMetricTuple(final MetricDefinition metricDef,
        final Metric metric) {
    final MkTupleParam tupleParam = new MkTupleParam();
    tupleParam.setFields(MetricFilteringBolt.FIELDS);
    tupleParam.setStream(Streams.DEFAULT_STREAM_ID);
    return Testing.testTuple(Arrays.asList(new MetricDefinitionAndTenantId(metricDef, TENANT_ID), metric), tupleParam);
  }
}
