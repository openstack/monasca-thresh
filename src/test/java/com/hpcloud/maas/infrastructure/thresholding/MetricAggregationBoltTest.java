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

import com.google.inject.AbstractModule;
import com.hpcloud.maas.common.model.alarm.AggregateFunction;
import com.hpcloud.maas.common.model.alarm.AlarmOperator;
import com.hpcloud.maas.common.model.alarm.AlarmState;
import com.hpcloud.maas.common.model.metric.Metric;
import com.hpcloud.maas.common.model.metric.MetricDefinition;
import com.hpcloud.maas.domain.model.Alarm;
import com.hpcloud.maas.domain.model.AlarmData;
import com.hpcloud.maas.domain.model.MetricData;
import com.hpcloud.maas.domain.service.AlarmDAO;
import com.hpcloud.util.Injector;

/**
 * @author Jonathan Halterman
 */
@Test
public class MetricAggregationBoltTest {
  private MetricAggregationBolt bolt;
  private OutputCollector collector;
  private List<MetricDefinition> metricDefs;
  private Map<MetricDefinition, Alarm> alarms;

  @BeforeClass
  protected void beforeClass() {
    MetricDefinition metricDef1 = new MetricDefinition("compute", "cpu", null, null);
    MetricDefinition metricDef2 = new MetricDefinition("compute", "mem", null, null);
    metricDefs = Arrays.asList(metricDef1, metricDef2);
  }

  @BeforeMethod
  protected void beforeMethod() {
    // Fixtures
    Alarm alarm1 = new Alarm("1", "123", AggregateFunction.AVERAGE, metricDefs.get(0),
        AlarmOperator.GTE, 90, 2, 3, AlarmState.OK);
    Alarm alarm2 = new Alarm("1", "456", AggregateFunction.AVERAGE, metricDefs.get(1),
        AlarmOperator.GTE, 90, 2, 3, AlarmState.OK);
    alarms = new HashMap<MetricDefinition, Alarm>();
    alarms.put(metricDefs.get(0), alarm1);
    alarms.put(metricDefs.get(1), alarm2);

    final AlarmDAO dao = mock(AlarmDAO.class);
    when(dao.find(any(MetricDefinition.class))).thenAnswer(new Answer<List<Alarm>>() {
      @Override
      public List<Alarm> answer(InvocationOnMock invocation) throws Throwable {
        return Arrays.asList(alarms.get((MetricDefinition) invocation.getArguments()[0]));
      }
    });

    Injector.reset();
    Injector.registerModules(new AbstractModule() {
      protected void configure() {
        bind(AlarmDAO.class).toInstance(dao);
      }
    });

    bolt = new MetricAggregationBolt();
    collector = mock(OutputCollector.class);
    bolt.prepare(null, null, collector);
  }

  public void shouldAggregateValues() {
    long t1 = System.currentTimeMillis() - 2000;

    bolt.aggregateValues(new Metric(metricDefs.get(0), 100, t1));
    bolt.aggregateValues(new Metric(metricDefs.get(0), 80, t1));
    bolt.aggregateValues(new Metric(metricDefs.get(1), 50, t1));
    bolt.aggregateValues(new Metric(metricDefs.get(1), 40, t1));

    AlarmData alarmData = bolt.getOrCreateMetricData(metricDefs.get(0)).alarmDataFor("123");
    assertEquals(alarmData.getStats().getValue(t1), 90.0);

    alarmData = bolt.getOrCreateMetricData(metricDefs.get(1)).alarmDataFor("456");
    assertEquals(alarmData.getStats().getValue(t1), 45.0);
  }

  @SuppressWarnings("unchecked")
  public void shouldEvaluateAlarms() {
    // Given
    long t1 = System.currentTimeMillis();
    bolt.aggregateValues(new Metric(metricDefs.get(0), 100, t1));
    bolt.aggregateValues(new Metric(metricDefs.get(0), 95, t1 - 2000));
    bolt.aggregateValues(new Metric(metricDefs.get(0), 88, t1 - 4000));

    bolt.evaluateAlarms();
    verify(collector, never()).emit(any(List.class));

    bolt.aggregateValues(new Metric(metricDefs.get(0), 99, t1 - 4000));

    bolt.evaluateAlarms();
    verify(collector, times(1)).emit(any(List.class));
  }

  public void shouldHandleAlarmCreated() {

  }

  public void shouldHandleAlarmDeleted() {

  }

  public void shouldGetOrCreateSameMetricData() {
    MetricData data = bolt.getOrCreateMetricData(metricDefs.get(0));
    assertNotNull(data);
    assertEquals(bolt.getOrCreateMetricData(metricDefs.get(0)), data);
  }
}
