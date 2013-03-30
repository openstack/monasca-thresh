package com.hpcloud.maas.infrastructure.thresholding;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
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

import com.google.inject.AbstractModule;
import com.hpcloud.maas.common.model.alarm.AlarmState;
import com.hpcloud.maas.common.model.alarm.AlarmSubExpression;
import com.hpcloud.maas.common.model.metric.Metric;
import com.hpcloud.maas.common.model.metric.MetricDefinition;
import com.hpcloud.maas.domain.model.SubAlarm;
import com.hpcloud.maas.domain.model.SubAlarmStats;
import com.hpcloud.maas.domain.service.SubAlarmDAO;
import com.hpcloud.maas.domain.service.SubAlarmStatsRepository;
import com.hpcloud.util.Injector;

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

  @BeforeClass
  protected void beforeClass() {
    AlarmSubExpression subExpr1 = AlarmSubExpression.of("avg(compute:cpu:{id=5}, 2, 3) >= 90");
    AlarmSubExpression subExpr2 = AlarmSubExpression.of("avg(compute:mem:{id=5}, 2, 3) >= 90");
    subExpressions = Arrays.asList(subExpr1, subExpr2);
  }

  @BeforeMethod
  protected void beforeMethod() {
    // Fixtures
    SubAlarm subAlarm1 = new SubAlarm("1", "123", subExpressions.get(0), AlarmState.OK);
    SubAlarm subAlarm2 = new SubAlarm("1", "456", subExpressions.get(1), AlarmState.OK);
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

    Injector.reset();
    Injector.registerModules(new AbstractModule() {
      protected void configure() {
        bind(SubAlarmDAO.class).toInstance(dao);
      }
    });

    bolt = new MetricAggregationBolt();
    context = mock(TopologyContext.class);
    collector = mock(OutputCollector.class);
    bolt.prepare(null, context, collector);
  }

  public void shouldAggregateValues() {
    long t1 = System.currentTimeMillis() - 2000;

    bolt.aggregateValues(new Metric(subExpressions.get(0).getMetricDefinition(), 100, t1));
    bolt.aggregateValues(new Metric(subExpressions.get(0).getMetricDefinition(), 80, t1));
    bolt.aggregateValues(new Metric(subExpressions.get(1).getMetricDefinition(), 50, t1));
    bolt.aggregateValues(new Metric(subExpressions.get(1).getMetricDefinition(), 40, t1));

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
    bolt.aggregateValues(new Metric(subExpressions.get(0).getMetricDefinition(), 100, t1));
    bolt.aggregateValues(new Metric(subExpressions.get(0).getMetricDefinition(), 95, t1 - 2000));
    bolt.aggregateValues(new Metric(subExpressions.get(0).getMetricDefinition(), 88, t1 - 4000));

    bolt.evaluateAlarmsAndAdvanceWindows();
    verify(collector, never()).emit(any(List.class));

    bolt.aggregateValues(new Metric(subExpressions.get(0).getMetricDefinition(), 99, t1 - 4000));

    bolt.evaluateAlarmsAndAdvanceWindows();
    verify(collector, times(1)).emit(any(List.class));
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
