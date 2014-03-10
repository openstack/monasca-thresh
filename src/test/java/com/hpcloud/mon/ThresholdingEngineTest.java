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
import backtype.storm.topology.IRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.google.inject.AbstractModule;
import com.hpcloud.messaging.rabbitmq.RabbitMQService;
import com.hpcloud.mon.common.model.alarm.AlarmExpression;
import com.hpcloud.mon.common.model.alarm.AlarmState;
import com.hpcloud.mon.common.model.metric.Metric;
import com.hpcloud.mon.common.model.metric.MetricDefinition;
import com.hpcloud.mon.domain.model.Alarm;
import com.hpcloud.mon.domain.model.SubAlarm;
import com.hpcloud.mon.domain.service.AlarmDAO;
import com.hpcloud.mon.domain.service.MetricDefinitionDAO;
import com.hpcloud.mon.domain.service.SubAlarmDAO;
import com.hpcloud.mon.infrastructure.thresholding.MetricAggregationBolt;
import com.hpcloud.streaming.storm.NoopSpout;
import com.hpcloud.streaming.storm.TopologyTestCase;
import com.hpcloud.util.Injector;

/**
 * Simulates a real'ish run of the thresholding engine, using seconds instead of minutes for the
 * evaluation timescale.
 * 
 * @author Jonathan Halterman
 */
@Test(groups = "integration")
public class ThresholdingEngineTest extends TopologyTestCase {
  private FeederSpout collectdMetricSpout;
  private IRichSpout maasMetricSpout;
  private FeederSpout eventSpout;
  private AlarmDAO alarmDAO;
  private SubAlarmDAO subAlarmDAO;
  private MetricDefinition cpuMetricDef;
  private MetricDefinition memMetricDef;
  private MetricDefinitionDAO metricDefinitionDAO;

  public ThresholdingEngineTest() {
    // Fixtures
    final AlarmExpression expression = new AlarmExpression(
        "avg(hpcs.compute:cpu:{id=5}, 3) >= 3 times 2 and avg(hpcs.compute:mem:{id=5}, 3) >= 5 times 2");

    cpuMetricDef = expression.getSubExpressions().get(0).getMetricDefinition();
    memMetricDef = expression.getSubExpressions().get(1).getMetricDefinition();

    // Mocks
    alarmDAO = mock(AlarmDAO.class);
    when(alarmDAO.findById(anyString())).thenAnswer(new Answer<Alarm>() {
      @Override
      public Alarm answer(InvocationOnMock invocation) throws Throwable {
        return new Alarm("1", "bob", "test-alarm", expression, subAlarmsFor(expression),
            AlarmState.OK);
      }
    });

    subAlarmDAO = mock(SubAlarmDAO.class);
    when(subAlarmDAO.find(any(MetricDefinition.class))).thenAnswer(new Answer<List<SubAlarm>>() {
      @Override
      public List<SubAlarm> answer(InvocationOnMock invocation) throws Throwable {
        MetricDefinition metricDef = (MetricDefinition) invocation.getArguments()[0];
        if (metricDef.equals(cpuMetricDef))
          return Arrays.asList(new SubAlarm("123", "1", expression.getSubExpressions().get(0)));
        else if (metricDef.equals(memMetricDef))
          return Arrays.asList(new SubAlarm("456", "1", expression.getSubExpressions().get(1)));
        return Collections.emptyList();
      }
    });

    metricDefinitionDAO = mock(MetricDefinitionDAO.class);
    List<MetricDefinition> metricDefs = Arrays.asList(cpuMetricDef, memMetricDef);
    when(metricDefinitionDAO.findForAlarms()).thenReturn(metricDefs);

    final RabbitMQService rabbitMQService = mock(RabbitMQService.class);

    // Bindings
    Injector.reset();
    Injector.registerModules(new AbstractModule() {
      protected void configure() {
        bind(AlarmDAO.class).toInstance(alarmDAO);
        bind(SubAlarmDAO.class).toInstance(subAlarmDAO);
        bind(MetricDefinitionDAO.class).toInstance(metricDefinitionDAO);
        bind(RabbitMQService.class).toInstance(rabbitMQService);
      }
    });

    // Config
    ThresholdingConfiguration threshConfig = new ThresholdingConfiguration();
    Config stormConfig = new Config();
    stormConfig.setMaxTaskParallelism(1);
    collectdMetricSpout = new FeederSpout(new Fields("metricDefinition", "metric"));
    maasMetricSpout = new NoopSpout(new Fields("metricDefinition", "metric"));
    eventSpout = new FeederSpout(new Fields("event"));
    Injector.registerModules(new TopologyModule(threshConfig, stormConfig, collectdMetricSpout,
        maasMetricSpout, eventSpout));

    // Evaluate alarm stats every 1 seconds
    System.setProperty(MetricAggregationBolt.TICK_TUPLE_SECONDS_KEY, "1");
  }

  private List<SubAlarm> subAlarmsFor(AlarmExpression expression) {
    SubAlarm subAlarm1 = new SubAlarm("123", "1", expression.getSubExpressions().get(0));
    SubAlarm subAlarm2 = new SubAlarm("456", "1", expression.getSubExpressions().get(1));
    return Arrays.asList(subAlarm1, subAlarm2);
  }

  public void shouldThreshold() throws Exception {
    int waitCount = 0;
    int feedCount = 5;
    int goodValueCount = 0;
    for (int i = 1; i < 40; i++) {
      if (feedCount > 0) {
        System.out.println("Feeding metrics...");

        long time = System.currentTimeMillis();
        collectdMetricSpout.feed(new Values(cpuMetricDef, new Metric(cpuMetricDef.namespace,
                cpuMetricDef.dimensions, time, (double) (++goodValueCount == 15 ? 1 : 555))));
        collectdMetricSpout.feed(new Values(memMetricDef, new Metric(memMetricDef.namespace,
                memMetricDef.dimensions, time, (double) (goodValueCount == 15 ? 1 : 555))));

        if (--feedCount == 0)
          waitCount = 3;

        if (goodValueCount == 15)
          goodValueCount = 0;
      } else {
        System.out.println("Waiting...");
        if (--waitCount == 0)
          feedCount = 5;
      }

      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
}
