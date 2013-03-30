package com.hpcloud.maas;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

import backtype.storm.Config;
import backtype.storm.testing.FeederSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.google.inject.AbstractModule;
import com.hpcloud.maas.common.model.alarm.AlarmExpression;
import com.hpcloud.maas.common.model.alarm.AlarmState;
import com.hpcloud.maas.common.model.metric.Metric;
import com.hpcloud.maas.common.model.metric.MetricDefinition;
import com.hpcloud.maas.domain.model.Alarm;
import com.hpcloud.maas.domain.model.SubAlarm;
import com.hpcloud.maas.domain.service.AlarmDAO;
import com.hpcloud.maas.domain.service.SubAlarmDAO;
import com.hpcloud.maas.infrastructure.storm.TopologyTestCase;
import com.hpcloud.maas.infrastructure.thresholding.MetricAggregationBolt;
import com.hpcloud.util.Injector;

/**
 * @author Jonathan Halterman
 */
@Test
public class ThresholdingEngineTest extends TopologyTestCase {
  private FeederSpout metricSpout;
  private FeederSpout eventSpout;
  private AlarmDAO alarmDAO;
  private SubAlarmDAO subAlarmDAO;
  private Map<String, Alarm> alarms;
  private List<MetricDefinition> metricDefs;
  private Map<MetricDefinition, SubAlarm> subAlarms;

  public ThresholdingEngineTest() {
    // Fixtures
    AlarmExpression expression = new AlarmExpression(
        "count(compute:cpu:{id=5}, 3, 3) >= 3 and count(compute:mem:{id=5}, 2, 4) >= 3");

    metricDefs = Arrays.asList(expression.getSubExpressions().get(0).getMetricDefinition(),
        expression.getSubExpressions().get(1).getMetricDefinition());
    SubAlarm subAlarm1 = new SubAlarm("1", "123", expression.getSubExpressions().get(0));
    SubAlarm subAlarm2 = new SubAlarm("1", "456", expression.getSubExpressions().get(1));
    subAlarms = new HashMap<MetricDefinition, SubAlarm>();
    subAlarms.put(subAlarm1.getExpression().getMetricDefinition(), subAlarm1);
    subAlarms.put(subAlarm2.getExpression().getMetricDefinition(), subAlarm2);

    alarms = new HashMap<String, Alarm>();
    alarms.put("1",
        new Alarm("1", "bob", "test-alarm", expression, Arrays.asList(subAlarm1, subAlarm2),
            AlarmState.OK));

    // Mocks
    alarmDAO = mock(AlarmDAO.class);
    when(alarmDAO.findById(anyString())).thenAnswer(new Answer<Alarm>() {
      @Override
      public Alarm answer(InvocationOnMock invocation) throws Throwable {
        return alarms.get((String) invocation.getArguments()[0]);
      }
    });

    subAlarmDAO = mock(SubAlarmDAO.class);
    when(subAlarmDAO.find(any(MetricDefinition.class))).thenAnswer(new Answer<List<SubAlarm>>() {
      @Override
      public List<SubAlarm> answer(InvocationOnMock invocation) throws Throwable {
        MetricDefinition metricDef = (MetricDefinition) invocation.getArguments()[0];
        return Arrays.asList(subAlarms.get(metricDef));
      }
    });

    // Bindings
    Injector.reset();
    Injector.registerModules(new AbstractModule() {
      protected void configure() {
        bind(AlarmDAO.class).toInstance(alarmDAO);
        bind(SubAlarmDAO.class).toInstance(subAlarmDAO);
      }
    });

    // Config
    ThresholdingConfiguration threshConfig = new ThresholdingConfiguration();
    Config stormConfig = new Config();
    stormConfig.setMaxTaskParallelism(1);
    // stormConfig.setDebug(true);
    metricSpout = new FeederSpout(new Fields("metricDefinition", "metric"));
    eventSpout = new FeederSpout(new Fields("event"));
    Injector.registerModules(new TopologyModule(threshConfig, stormConfig, metricSpout, eventSpout));

    // Evaluate alarm stats every 5 seconds
    System.setProperty(MetricAggregationBolt.TICK_TUPLE_SECONDS_KEY, "5");
  }

  public void shouldThreshold() throws Exception {
    int waitCount = 0;
    int feedCount = 5;
    for (int i = 1; i < 40; i++) {
      if (feedCount > 0) {
        System.out.println("Feeding metrics...");

        long time = System.currentTimeMillis();
        metricSpout.feed(new Values(metricDefs.get(0), new Metric(metricDefs.get(0), 333, time)));
        metricSpout.feed(new Values(metricDefs.get(1), new Metric(metricDefs.get(1), 333, time)));

        if (--feedCount == 0)
          waitCount = 3;
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
