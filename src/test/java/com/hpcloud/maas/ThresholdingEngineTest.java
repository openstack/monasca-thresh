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
import com.hpcloud.maas.infrastructure.storm.TopologyTestCase;
import com.hpcloud.util.Injector;

/**
 * @author Jonathan Halterman
 */
@Test
public class ThresholdingEngineTest extends TopologyTestCase {
  private FeederSpout metricSpout;
  private FeederSpout eventSpout;
  private AlarmDAO alarmDAO;
  private List<MetricDefinition> metricDefs;
  private Map<String, Alarm> compositeAlarms;
  private Map<MetricDefinition, SubAlarm> alarms;

  public ThresholdingEngineTest() {
    // Fixtures
    MetricDefinition metricDef1 = new MetricDefinition("compute", "cpu", null, null);
    MetricDefinition metricDef2 = new MetricDefinition("compute", "mem", null, null);
    metricDefs = Arrays.asList(metricDef1, metricDef2);
    AlarmExpression expression = new AlarmExpression(
        "avg(compute:cpu, 2, 3) >= 90 and avg(compute:mem, 2, 3) >= 90");
    SubAlarm subAlarm1 = new SubAlarm("1", expression.getSubExpressions().get(0));
    SubAlarm subAlarm2 = new SubAlarm("1", expression.getSubExpressions().get(1));
    alarms = new HashMap<MetricDefinition, SubAlarm>();
    alarms.put(metricDefs.get(0), subAlarm1);
    alarms.put(metricDefs.get(1), subAlarm2);
    compositeAlarms = new HashMap<String, Alarm>();
    compositeAlarms.put("1",
        new Alarm("1", "bob", "test-alarm", expression, Arrays.asList(subAlarm1, subAlarm2),
            AlarmState.OK));

    // Mocks
    alarmDAO = mock(AlarmDAO.class);
    when(alarmDAO.find(any(MetricDefinition.class))).thenAnswer(new Answer<List<SubAlarm>>() {
      @Override
      public List<SubAlarm> answer(InvocationOnMock invocation) throws Throwable {
        return Arrays.asList(alarms.get((MetricDefinition) invocation.getArguments()[0]));
      }
    });

    when(alarmDAO.findByCompositeId(anyString())).thenAnswer(new Answer<Alarm>() {
      @Override
      public Alarm answer(InvocationOnMock invocation) throws Throwable {
        return compositeAlarms.get((String) invocation.getArguments()[0]);
      }
    });

    Injector.reset();
    Injector.registerModules(new AbstractModule() {
      protected void configure() {
        bind(AlarmDAO.class).toInstance(alarmDAO);
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
  }

  public void shouldThreshold() throws Exception {
    for (int i = 0; i < 20; i++) {
      MetricDefinition metricDef = new MetricDefinition("compute", "cpu", null, null);
      metricSpout.feed(new Values(metricDef, new Metric(metricDef, 95, System.currentTimeMillis())));

      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
}
