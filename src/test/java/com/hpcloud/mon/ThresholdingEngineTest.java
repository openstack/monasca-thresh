package com.hpcloud.mon;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doAnswer;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import backtype.storm.Config;
import backtype.storm.testing.FeederSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.google.inject.AbstractModule;
import com.hpcloud.configuration.KafkaProducerConfiguration;
import com.hpcloud.mon.common.model.alarm.AlarmExpression;
import com.hpcloud.mon.common.model.alarm.AlarmState;
import com.hpcloud.mon.common.model.metric.Metric;
import com.hpcloud.mon.common.model.metric.MetricDefinition;
import com.hpcloud.mon.domain.model.Alarm;
import com.hpcloud.mon.domain.model.AlarmStateTransitionEvent;
import com.hpcloud.mon.domain.model.SubAlarm;
import com.hpcloud.mon.domain.service.AlarmDAO;
import com.hpcloud.mon.domain.service.MetricDefinitionDAO;
import com.hpcloud.mon.domain.service.SubAlarmDAO;
import com.hpcloud.mon.infrastructure.thresholding.AlarmEventForwarder;
import com.hpcloud.mon.infrastructure.thresholding.MetricAggregationBolt;
import com.hpcloud.streaming.storm.TopologyTestCase;
import com.hpcloud.util.Injector;
import com.hpcloud.util.Serialization;

/**
 * Simulates a real'ish run of the thresholding engine, using seconds instead of minutes for the
 * evaluation timescale.
 * 
 * @author Jonathan Halterman
 */
@Test(groups = "integration")
public class ThresholdingEngineTest extends TopologyTestCase {
  private static final String TEST_ALARM_TENANT_ID = "bob";
  private static final String TEST_ALARM_ID = "1";
  private static final String TEST_ALARM_NAME = "test-alarm";
  private FeederSpout metricSpout;
  private FeederSpout eventSpout;
  private AlarmDAO alarmDAO;
  private SubAlarmDAO subAlarmDAO;
  private MetricDefinition cpuMetricDef;
  private MetricDefinition memMetricDef;
  private MetricDefinitionDAO metricDefinitionDAO;
  private final AlarmEventForwarder alarmEventForwarder;

  private AlarmState previousState = AlarmState.UNDETERMINED;
  private AlarmState expectedState = AlarmState.ALARM;
  private volatile int alarmsSent = 0;

  public ThresholdingEngineTest() {
    // Fixtures
    final AlarmExpression expression = new AlarmExpression(
        "max(hpcs.compute.cpu{id=5}) >= 3 or max(hpcs.compute.mem{id=5}) >= 5 times 2");

    cpuMetricDef = expression.getSubExpressions().get(0).getMetricDefinition();
    memMetricDef = expression.getSubExpressions().get(1).getMetricDefinition();

    // Mocks
    alarmDAO = mock(AlarmDAO.class);
    when(alarmDAO.findById(anyString())).thenAnswer(new Answer<Alarm>() {
      @Override
      public Alarm answer(InvocationOnMock invocation) throws Throwable {
        return new Alarm(TEST_ALARM_ID, TEST_ALARM_TENANT_ID, TEST_ALARM_NAME, expression, subAlarmsFor(expression),
            AlarmState.UNDETERMINED);
      }
    });

    subAlarmDAO = mock(SubAlarmDAO.class);
    when(subAlarmDAO.find(any(MetricDefinition.class))).thenAnswer(new Answer<List<SubAlarm>>() {
      @Override
      public List<SubAlarm> answer(InvocationOnMock invocation) throws Throwable {
        MetricDefinition metricDef = (MetricDefinition) invocation.getArguments()[0];
        if (metricDef.equals(cpuMetricDef))
          return Arrays.asList(new SubAlarm("123", TEST_ALARM_ID, expression.getSubExpressions().get(0)));
        else if (metricDef.equals(memMetricDef))
          return Arrays.asList(new SubAlarm("456", TEST_ALARM_ID, expression.getSubExpressions().get(1)));
        return Collections.emptyList();
      }
    });

    metricDefinitionDAO = mock(MetricDefinitionDAO.class);
    List<MetricDefinition> metricDefs = Arrays.asList(cpuMetricDef, memMetricDef);
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
    threshConfig.sporadicMetricNamespaces = new HashSet<String>();
    Serialization.registerTarget(KafkaProducerConfiguration.class);

    threshConfig.kafkaProducerConfig = Serialization.fromJson("{\"KafkaProducerConfiguration\":{\"topic\":\"alarm-state-transitions\",\"metadataBrokerList\":\"192.168.10.10:9092\",\"requestRequiredAcks\":1,\"requestTimeoutMs\":10000,\"producerType\":\"sync\",\"serializerClass\":\"kafka.serializer.StringEncoder\",\"keySerializerClass\":\"\",\"partitionerClass\":\"\",\"compressionCodec\":\"none\",\"compressedTopics\":\"\",\"messageSendMaxRetries\":3,\"retryBackoffMs\":100,\"topicMetadataRefreshIntervalMs\":600000,\"queueBufferingMaxMs\":5000,\"queueBufferingMaxMessages\":10000,\"queueEnqueueTimeoutMs\":-1,\"batchNumMessages\":200,\"sendBufferBytes\":102400,\"clientId\":\"Threshold_Engine\"}}");
    Config stormConfig = new Config();
    stormConfig.setMaxTaskParallelism(1);
    metricSpout = new FeederSpout(new Fields("metricDefinition", "metric"));
    eventSpout = new FeederSpout(new Fields("event"));
    alarmEventForwarder = mock(AlarmEventForwarder.class);
    Injector.registerModules(new TopologyModule(threshConfig, stormConfig,
        metricSpout, eventSpout, alarmEventForwarder));

    // Evaluate alarm stats every 1 seconds
    System.setProperty(MetricAggregationBolt.TICK_TUPLE_SECONDS_KEY, "1");
  }

  private List<SubAlarm> subAlarmsFor(AlarmExpression expression) {
    SubAlarm subAlarm1 = new SubAlarm("123", TEST_ALARM_ID, expression.getSubExpressions().get(0));
    SubAlarm subAlarm2 = new SubAlarm("456", TEST_ALARM_ID, expression.getSubExpressions().get(1));
    return Arrays.asList(subAlarm1, subAlarm2);
  }

  public void shouldThreshold() throws Exception {
    doAnswer(new Answer<Object>() {
          public Object answer(InvocationOnMock invocation) {
              final Object[] args = invocation.getArguments();
              AlarmStateTransitionEvent event = Serialization.fromJson((String)args[2]);
              alarmsSent++;
              System.out.printf("Alarm transitioned from %s to %s%n", event.oldState, event.newState);
              assertEquals(event.alarmName, TEST_ALARM_NAME);
              assertEquals(event.alarmId, TEST_ALARM_ID);
              assertEquals(event.tenantId, TEST_ALARM_TENANT_ID);
              assertEquals(event.oldState, previousState);
              assertEquals(event.newState, expectedState);
              previousState = event.newState;
              if (event.newState == AlarmState.UNDETERMINED) {
                  expectedState = AlarmState.ALARM;
              }
              else if (event.newState == AlarmState.ALARM) {
                  expectedState = AlarmState.UNDETERMINED;
              }
              return null;
          }
      }
    )
    .when(alarmEventForwarder).send(anyString(), anyString(), anyString());
    int waitCount = 0;
    int feedCount = 5;
    int goodValueCount = 0;
    for (int i = 1; i < 40 && alarmsSent == 0; i++) {
      if (feedCount > 0) {
        System.out.println("Feeding metrics...");

        long time = System.currentTimeMillis() / 1000;
        metricSpout.feed(new Values(cpuMetricDef, new Metric(cpuMetricDef.name,
                cpuMetricDef.dimensions, time, (double) (++goodValueCount == 15 ? 1 : 555))));
        metricSpout.feed(new Values(memMetricDef, new Metric(memMetricDef.name,
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

    // Give it some extra time if it needs it for the alarm to come out
    for (int i = 0; i < 30 && alarmsSent == 0; i++) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
    }
    assertTrue(alarmsSent > 0, "Not enough alarms");
  }
}
