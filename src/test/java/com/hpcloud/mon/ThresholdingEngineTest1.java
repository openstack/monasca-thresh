package com.hpcloud.mon;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

import backtype.storm.Config;
import backtype.storm.testing.FeederSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.hpcloud.mon.common.event.AlarmCreatedEvent;
import com.hpcloud.mon.common.event.AlarmDeletedEvent;
import com.hpcloud.mon.common.model.alarm.AlarmExpression;
import com.hpcloud.mon.common.model.alarm.AlarmState;
import com.hpcloud.mon.common.model.alarm.AlarmSubExpression;
import com.hpcloud.mon.common.model.metric.Metric;
import com.hpcloud.mon.common.model.metric.MetricDefinition;
import com.hpcloud.mon.domain.model.Alarm;
import com.hpcloud.mon.domain.model.MetricDefinitionAndTenantId;
import com.hpcloud.mon.domain.model.SubAlarm;
import com.hpcloud.mon.domain.service.AlarmDAO;
import com.hpcloud.mon.domain.service.MetricDefinitionDAO;
import com.hpcloud.mon.domain.service.SubAlarmDAO;
import com.hpcloud.mon.domain.service.SubAlarmMetricDefinition;
import com.hpcloud.mon.infrastructure.thresholding.AlarmEventForwarder;
import com.hpcloud.mon.infrastructure.thresholding.MetricAggregationBolt;
import com.hpcloud.mon.infrastructure.thresholding.MetricSpout;
import com.hpcloud.mon.infrastructure.thresholding.ProducerModule;
import com.hpcloud.streaming.storm.TopologyTestCase;
import com.hpcloud.util.Injector;

/**
 * Simulates a real'ish run of the thresholding engine, using seconds instead of minutes for the
 * evaluation timescale.
 * 
 * @author Jonathan Halterman
 */
@Test(groups = "integration")
public class ThresholdingEngineTest1 extends TopologyTestCase {
  private static final String JOE_TENANT_ID = "joe";
  private static final String BOB_TENANT_ID = "bob";
  private FeederSpout metricSpout;
  private FeederSpout eventSpout;
  private AlarmDAO alarmDAO;
  private SubAlarmDAO subAlarmDAO;
  private MetricDefinition cpuMetricDef;
  private MetricDefinition memMetricDef;
  private MetricDefinition customMetricDef;
  private MetricDefinitionDAO metricDefinitionDAO;

  private AlarmExpression expression;
  private AlarmExpression customExpression;
  private AlarmSubExpression customSubExpression;

  public ThresholdingEngineTest1() {
    // Fixtures
    expression = new AlarmExpression(
        "avg(hpcs.compute.cpu{id=5}, 3) >= 3 times 2 and avg(hpcs.compute.mem{id=5}, 3) >= 5 times 2");
    customExpression = AlarmExpression.of("avg(my.test{id=4}, 3) > 10");
    customSubExpression = customExpression.getSubExpressions().get(0);

    cpuMetricDef = expression.getSubExpressions().get(0).getMetricDefinition();
    memMetricDef = expression.getSubExpressions().get(1).getMetricDefinition();
    customMetricDef = customSubExpression.getMetricDefinition();

    // Mocks
    alarmDAO = mock(AlarmDAO.class);
    when(alarmDAO.findById(anyString())).thenAnswer(new Answer<Alarm>() {
      @Override
      public Alarm answer(InvocationOnMock invocation) throws Throwable {
        if (invocation.getArguments()[0].equals("1"))
          return new Alarm("1", BOB_TENANT_ID, "test-alarm", "Descr of test-alarm", expression, Arrays.asList(createCpuSubAlarm(),
              createMemSubAlarm()), AlarmState.OK, Boolean.TRUE);
        else if (invocation.getArguments()[0].equals("2"))
          return new Alarm("2", JOE_TENANT_ID, "joes-alarm", "Descr of joes-alarm", customExpression,
              Arrays.asList(createCustomSubAlarm()), AlarmState.OK, Boolean.TRUE);
        return null;
      }
    });

    subAlarmDAO = mock(SubAlarmDAO.class);
    when(subAlarmDAO.find(any(MetricDefinitionAndTenantId.class))).thenAnswer(new Answer<List<SubAlarm>>() {
      @Override
      public List<SubAlarm> answer(InvocationOnMock invocation) throws Throwable {
        MetricDefinitionAndTenantId metricDefinitionAndTenantId = (MetricDefinitionAndTenantId) invocation.getArguments()[0];
        MetricDefinition metricDef = metricDefinitionAndTenantId.metricDefinition;
        if (metricDef.equals(cpuMetricDef))
          return Arrays.asList(createCpuSubAlarm());
        else if (metricDef.equals(memMetricDef))
          return Arrays.asList(createMemSubAlarm());
        else if (metricDef.equals(customMetricDef))
          return Arrays.asList(createCustomSubAlarm());
        return Collections.emptyList();
      }
    });

    metricDefinitionDAO = mock(MetricDefinitionDAO.class);
    final List<SubAlarmMetricDefinition> metricDefs = Arrays.asList(
            new SubAlarmMetricDefinition(createCpuSubAlarm().getId(),
                    new MetricDefinitionAndTenantId(cpuMetricDef, BOB_TENANT_ID)),
            new SubAlarmMetricDefinition(createMemSubAlarm().getId(),
                    new MetricDefinitionAndTenantId(memMetricDef, BOB_TENANT_ID)),
            new SubAlarmMetricDefinition(createCustomSubAlarm().getId(),
                    new MetricDefinitionAndTenantId(customMetricDef, JOE_TENANT_ID)));
    when(metricDefinitionDAO.findForAlarms()).thenReturn(metricDefs);

    // Bindings
    Injector.reset();
    Injector.registerModules(new AbstractModule() {
      protected void configure() {
        bind(AlarmDAO.class).toInstance(alarmDAO);
        bind(SubAlarmDAO.class).toInstance(subAlarmDAO);
        bind(MetricDefinitionDAO.class).toInstance(metricDefinitionDAO);
      }
    });

    // Config
    ThresholdingConfiguration threshConfig = new ThresholdingConfiguration();
    Config stormConfig = new Config();
    stormConfig.setMaxTaskParallelism(5);

    metricSpout = new FeederSpout(new Fields(MetricSpout.FIELDS));
    eventSpout = new FeederSpout(new Fields("event"));
    final AlarmEventForwarder alarmEventForwarder = mock(AlarmEventForwarder.class);

    Injector.registerModules(new TopologyModule(threshConfig, stormConfig,
        metricSpout, eventSpout));
    Injector.registerModules(new ProducerModule(alarmEventForwarder));

    // Evaluate alarm stats every 1 seconds
    System.setProperty(MetricAggregationBolt.TICK_TUPLE_SECONDS_KEY, "1");
  }

  private SubAlarm createCpuSubAlarm() {
    return new SubAlarm("111", "1", expression.getSubExpressions().get(0));
  }

  private SubAlarm createMemSubAlarm() {
    return new SubAlarm("222", "1", expression.getSubExpressions().get(1));
  }

  private SubAlarm createCustomSubAlarm() {
    return new SubAlarm("333", "2", customSubExpression);
  }

  public void shouldThreshold() throws Exception {
    int count = 0;
    int eventCounter = 0;

    while (true) {
      long time = System.currentTimeMillis();
      metricSpout.feed(new Values(new MetricDefinitionAndTenantId(cpuMetricDef, BOB_TENANT_ID), new Metric(cpuMetricDef.name,
          cpuMetricDef.dimensions, time, count % 10 == 0 ? 555 : 1)));
      metricSpout.feed(new Values(new MetricDefinitionAndTenantId(memMetricDef, BOB_TENANT_ID), new Metric(memMetricDef.name,
              cpuMetricDef.dimensions, time, count % 10 == 0 ? 555 : 1)));
      metricSpout.feed(new Values(new MetricDefinitionAndTenantId(customMetricDef, JOE_TENANT_ID), new Metric(customMetricDef.name,
              cpuMetricDef.dimensions, time, count % 20 == 0 ? 1 : 123)));

      if (count % 5 == 0) {
        Object event = null;
        if (++eventCounter % 2 == 0)
          event = new AlarmDeletedEvent(JOE_TENANT_ID, "2",
              ImmutableMap.<String, MetricDefinition>builder().put("444", customMetricDef).build());
        else
          event = new AlarmCreatedEvent(JOE_TENANT_ID, "2", "foo", customSubExpression.getExpression(),
              ImmutableMap.<String, AlarmSubExpression>builder()
                  .put("444", customSubExpression)
                  .build());

        eventSpout.feed(new Values(event));
      }

      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        return;
      }

      count++;
    }
  }
}
