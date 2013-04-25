package com.hpcloud.maas.infrastructure.thresholding;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;

import com.hpcloud.maas.common.model.alarm.AlarmState;
import com.hpcloud.maas.common.model.alarm.AlarmSubExpression;
import com.hpcloud.maas.common.model.metric.Metric;
import com.hpcloud.maas.common.model.metric.MetricDefinition;
import com.hpcloud.maas.domain.model.SubAlarm;
import com.hpcloud.maas.domain.model.SubAlarmStats;
import com.hpcloud.maas.domain.service.SubAlarmDAO;
import com.hpcloud.maas.domain.service.SubAlarmStatsRepository;

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

  @BeforeClass
  protected void beforeClass() {
    AlarmSubExpression subExpr1 = AlarmSubExpression.of("avg(compute:cpu:{id=5}, 60) >= 90 times 3");
    AlarmSubExpression subExpr2 = AlarmSubExpression.of("avg(compute:mem:{id=5}, 60) >= 90 times 3");
    subExpressions = Arrays.asList(subExpr1, subExpr2);
  }

  @BeforeMethod
  protected void beforeMethod() {
    // Fixtures
    subAlarm1 = new SubAlarm("123", "1", subExpressions.get(0), AlarmState.OK);
    subAlarm2 = new SubAlarm("456", "1", subExpressions.get(1), AlarmState.OK);
    subAlarms = new HashMap<MetricDefinition, SubAlarm>();
    subAlarms.put(subAlarm1.getExpression().getMetricDefinition(), subAlarm1);
    subAlarms.put(subAlarm2.getExpression().getMetricDefinition(), subAlarm2);

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

    // Register subalarm stats repo for metric def
    for (AlarmSubExpression subExp : subExpressions)
      bolt.getOrCreateSubAlarmStatsRepo(subExp.getMetricDefinition());
  }

  public void shouldAggregateValues() {
    long t1 = System.currentTimeMillis();

    bolt.aggregateValues(new Metric(subExpressions.get(0).getMetricDefinition(), t1, 100));
    bolt.aggregateValues(new Metric(subExpressions.get(0).getMetricDefinition(), t1, 80));
    bolt.aggregateValues(new Metric(subExpressions.get(1).getMetricDefinition(), t1, 50));
    bolt.aggregateValues(new Metric(subExpressions.get(1).getMetricDefinition(), t1, 40));

    SubAlarmStats alarmData = bolt.getOrCreateSubAlarmStatsRepo(
        subExpressions.get(0).getMetricDefinition()).get("123");
    assertEquals(alarmData.getStats().getValue(t1), 90.0);

    alarmData = bolt.getOrCreateSubAlarmStatsRepo(subExpressions.get(1).getMetricDefinition()).get(
        "456");
    assertEquals(alarmData.getStats().getValue(t1), 45.0);
  }

  @SuppressWarnings("unchecked")
  public void shouldEvaluateAlarms() {
    // Given
    long t1 = System.currentTimeMillis();
    bolt.aggregateValues(new Metric(subExpressions.get(0).getMetricDefinition(), t1, 100));
    bolt.aggregateValues(new Metric(subExpressions.get(0).getMetricDefinition(), t1 -= 60000, 95));
    bolt.aggregateValues(new Metric(subExpressions.get(0).getMetricDefinition(), t1 -= 60000, 88));

    bolt.evaluateAlarmsAndSlideWindows();
    assertEquals(subAlarm2.getState(), AlarmState.UNDETERMINED);

    bolt.aggregateValues(new Metric(subExpressions.get(0).getMetricDefinition(), t1, 99));

    bolt.evaluateAlarmsAndSlideWindows();
    assertEquals(subAlarm1.getState(), AlarmState.ALARM);
    verify(collector, times(2)).emit(any(List.class));
  }

  public void shouldHandleAlarmCreated() {
  }

  public void shouldHandleAlarmDeleted() {
  }

  public void shouldGetOrCreateSameMetricData() {
    SubAlarmStatsRepository data = bolt.getOrCreateSubAlarmStatsRepo(subExpressions.get(0)
        .getMetricDefinition());
    assertNotNull(data);
    assertEquals(bolt.getOrCreateSubAlarmStatsRepo(subExpressions.get(0).getMetricDefinition()),
        data);
  }
}
