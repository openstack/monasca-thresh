package com.hpcloud.mon.infrastructure.thresholding;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import backtype.storm.Testing;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.MkTupleParam;

import com.hpcloud.mon.common.model.alarm.AlarmState;
import com.hpcloud.mon.common.model.alarm.AlarmSubExpression;
import com.hpcloud.mon.common.model.metric.Metric;
import com.hpcloud.mon.common.model.metric.MetricDefinition;
import com.hpcloud.mon.domain.model.SubAlarm;
import com.hpcloud.mon.domain.model.SubAlarmStats;
import com.hpcloud.mon.domain.service.SubAlarmDAO;
import com.hpcloud.mon.domain.service.SubAlarmStatsRepository;

/**
 * @author Jonathan Halterman
 */
@Test
public class MetricAggregationBoltTest {
  private MetricAggregationBolt bolt;
  private TopologyContext context;
  private OutputCollector collector;
  private List<AlarmSubExpression> subExpressions;
  private Map<MetricDefinition, SubAlarm> subAlarms;
  private SubAlarm subAlarm1;
  private SubAlarm subAlarm2;
  private AlarmSubExpression subExpr1;
  private AlarmSubExpression subExpr2;
  private MetricDefinition metricDef1;
  private MetricDefinition metricDef2;

  @BeforeClass
  protected void beforeClass() {
    subExpr1 = AlarmSubExpression.of("avg(hpcs.compute.cpu{id=5}, 60) >= 90 times 3");
    subExpr2 = AlarmSubExpression.of("avg(hpcs.compute.mem{id=5}, 60) >= 90 times 3");
    subExpressions = Arrays.asList(subExpr1, subExpr2);
    metricDef1 = subExpr1.getMetricDefinition();
    metricDef2 = subExpr2.getMetricDefinition();
  }

  @BeforeMethod
  protected void beforeMethod() {
    // Fixtures
    subAlarm1 = new SubAlarm("123", "1", subExpr1, AlarmState.OK);
    subAlarm2 = new SubAlarm("456", "1", subExpr2, AlarmState.OK);
    subAlarms = new HashMap<>();
    subAlarms.put(subExpr1.getMetricDefinition(), subAlarm1);
    subAlarms.put(subExpr2.getMetricDefinition(), subAlarm2);

    final SubAlarmDAO dao = mock(SubAlarmDAO.class);
    when(dao.find(any(MetricDefinition.class))).thenAnswer(new Answer<List<SubAlarm>>() {
      @Override
      public List<SubAlarm> answer(InvocationOnMock invocation) throws Throwable {
        return Arrays.asList(subAlarms.get((MetricDefinition) invocation.getArguments()[0]));
      }
    });

    bolt = new MetricAggregationBolt(dao);
    context = mock(TopologyContext.class);
    collector = mock(OutputCollector.class);
    bolt.prepare(null, context, collector);
  }

  public void shouldAggregateValues() {
    long t1 = System.currentTimeMillis() / 1000;

    bolt.aggregateValues(metricDef1, new Metric(metricDef1.name, metricDef1.dimensions, t1, 100));
    bolt.aggregateValues(metricDef1, new Metric(metricDef1.name, metricDef1.dimensions, t1, 80));
    bolt.aggregateValues(metricDef2, new Metric(metricDef2.name, metricDef2.dimensions, t1, 50));
    bolt.aggregateValues(metricDef2, new Metric(metricDef2.name, metricDef2.dimensions, t1, 40));

    SubAlarmStats alarmData = bolt.getOrCreateSubAlarmStatsRepo(metricDef1).get("123");
    assertEquals(alarmData.getStats().getValue(t1), 90.0);

    alarmData = bolt.getOrCreateSubAlarmStatsRepo(metricDef2).get("456");
    assertEquals(alarmData.getStats().getValue(t1), 45.0);
  }

  @SuppressWarnings("unchecked")
  public void shouldEvaluateAlarms() {
    for (AlarmSubExpression subExp : subExpressions)
      bolt.getOrCreateSubAlarmStatsRepo(subExp.getMetricDefinition());

    // Given
    long t1 = System.currentTimeMillis() / 1000;
    bolt.aggregateValues(metricDef1, new Metric(metricDef1.name, metricDef1.dimensions, t1, 100));
    bolt.aggregateValues(metricDef1, new Metric(metricDef1.name, metricDef1.dimensions, t1 -= 60, 95));
    bolt.aggregateValues(metricDef1, new Metric(metricDef1.name, metricDef1.dimensions, t1 -= 60, 88));

    bolt.evaluateAlarmsAndSlideWindows();
    assertEquals(subAlarm2.getState(), AlarmState.UNDETERMINED);

    bolt.aggregateValues(metricDef1, new Metric(metricDef1.name, metricDef1.dimensions, t1, 99));

    bolt.evaluateAlarmsAndSlideWindows();
    assertEquals(subAlarm1.getState(), AlarmState.ALARM);
    verify(collector, times(2)).emit(any(List.class));
  }

  public void validateMetricDefAdded() {
    MkTupleParam tupleParam = new MkTupleParam();
    tupleParam.setFields(EventProcessingBolt.METRIC_SUB_ALARM_EVENT_STREAM_FIELDS);
    tupleParam.setStream(EventProcessingBolt.METRIC_SUB_ALARM_EVENT_STREAM_ID);

    assertNull(bolt.subAlarmStatsRepos.get(metricDef1));

    bolt.execute(Testing.testTuple(Arrays.asList(EventProcessingBolt.CREATED,
        metricDef1, new SubAlarm("123", "1", subExpr1)), tupleParam));

    assertNotNull(bolt.subAlarmStatsRepos.get(metricDef1).get("123"));
  }

  public void validateMetricDefDeleted() {
    MkTupleParam tupleParam = new MkTupleParam();
    tupleParam.setFields(EventProcessingBolt.METRIC_ALARM_EVENT_STREAM_FIELDS);
    tupleParam.setStream(EventProcessingBolt.METRIC_ALARM_EVENT_STREAM_ID);
    bolt.getOrCreateSubAlarmStatsRepo(metricDef1);

    assertNotNull(bolt.subAlarmStatsRepos.get(metricDef1).get("123"));

    bolt.execute(Testing.testTuple(
        Arrays.asList(EventProcessingBolt.DELETED, metricDef1, "123"), tupleParam));

    assertNull(bolt.subAlarmStatsRepos.get(metricDef1));
  }

  public void shouldGetOrCreateSameMetricData() {
    SubAlarmStatsRepository data = bolt.getOrCreateSubAlarmStatsRepo(metricDef1);
    assertNotNull(data);
    assertEquals(bolt.getOrCreateSubAlarmStatsRepo(metricDef1), data);
  }
}
