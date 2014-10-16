/*
 * Copyright (c) 2014 Hewlett-Packard Development Company, L.P.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package monasca.thresh;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import backtype.storm.Config;
import backtype.storm.testing.FeederSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.google.inject.AbstractModule;

import monasca.common.configuration.KafkaProducerConfiguration;
import monasca.common.model.alarm.AlarmExpression;
import monasca.common.model.alarm.AlarmState;
import monasca.common.model.alarm.AlarmSubExpression;
import monasca.common.model.event.AlarmDefinitionCreatedEvent;
import monasca.common.model.event.AlarmStateTransitionedEvent;
import monasca.common.model.metric.Metric;
import monasca.common.model.metric.MetricDefinition;
import monasca.common.streaming.storm.TopologyTestCase;
import monasca.common.util.Injector;
import monasca.common.util.Serialization;
import monasca.thresh.domain.model.Alarm;
import monasca.thresh.domain.model.AlarmDefinition;
import monasca.thresh.domain.model.MetricDefinitionAndTenantId;
import monasca.thresh.domain.model.TenantIdAndMetricName;
import monasca.thresh.domain.service.AlarmDAO;
import monasca.thresh.domain.service.AlarmDefinitionDAO;
import monasca.thresh.infrastructure.thresholding.AlarmEventForwarder;
import monasca.thresh.infrastructure.thresholding.MetricFilteringBolt;
import monasca.thresh.infrastructure.thresholding.MetricSpout;
import monasca.thresh.infrastructure.thresholding.ProducerModule;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;

/**
 * Simulates a real'ish run of the thresholding engine, using seconds instead of minutes for the
 * evaluation timescale.
 *
 * The Thresholding Engine starts with one existing Alarm Definition. Then metrics are sent to the
 * system that cause an alarm to be created and for that alarm to transition to the ALARM state
 */
@Test(groups = "integration")
public class ThresholdingEngineTest extends TopologyTestCase {
  private static final String TEST_ALARM_TENANT_ID = "bob";
  private static final String TEST_ALARM_NAME = "test-alarm";
  private static final String TEST_ALARM_DESCRIPTION = "Description of test-alarm";
  private AlarmDefinition alarmDefinition;
  private FeederSpout metricSpout;
  private FeederSpout eventSpout;
  private AlarmDAO alarmDAO;
  private AlarmDefinitionDAO alarmDefinitionDAO;
  private MetricDefinition cpuMetricDef;
  private MetricDefinition memMetricDef;
  private Map<String, String> extraMemMetricDefDimensions;
  private AlarmEventForwarder alarmEventForwarder;

  private AlarmState previousState = AlarmState.UNDETERMINED;
  private AlarmState expectedState = AlarmState.ALARM;
  private volatile int alarmsSent = 0;

  @BeforeMethod
  public void befortMethod() throws Exception {
    // Fixtures
    final AlarmExpression expression =
        new AlarmExpression("max(cpu{id=5}) >= 3 or max(mem{id=5}) >= 5");

    cpuMetricDef = expression.getSubExpressions().get(0).getMetricDefinition();
    memMetricDef = expression.getSubExpressions().get(1).getMetricDefinition();

    extraMemMetricDefDimensions = new HashMap<>(memMetricDef.dimensions);
    extraMemMetricDefDimensions.put("Group", "group A");

    alarmDefinition =
        new AlarmDefinition(TEST_ALARM_TENANT_ID, TEST_ALARM_NAME,
            TEST_ALARM_DESCRIPTION, expression, "LOW", true, new ArrayList<String>());

    // Mocks
    alarmDAO = mock(AlarmDAO.class);
    alarmDefinitionDAO = mock(AlarmDefinitionDAO.class);

    // Bindings
    Injector.reset();
    Injector.registerModules(new AbstractModule() {
      protected void configure() {
        bind(AlarmDAO.class).toInstance(alarmDAO);
        bind(AlarmDefinitionDAO.class).toInstance(alarmDefinitionDAO);
      }
    });

    // Config
    ThresholdingConfiguration threshConfig = new ThresholdingConfiguration();
    threshConfig.sporadicMetricNamespaces = new HashSet<String>();
    Serialization.registerTarget(KafkaProducerConfiguration.class);

    threshConfig.kafkaProducerConfig =
        Serialization
            .fromJson("{\"KafkaProducerConfiguration\":{\"topic\":\"alarm-state-transitions\",\"metadataBrokerList\":\"192.168.10.10:9092\",\"requestRequiredAcks\":1,\"requestTimeoutMs\":10000,\"producerType\":\"sync\",\"serializerClass\":\"kafka.serializer.StringEncoder\",\"keySerializerClass\":\"\",\"partitionerClass\":\"\",\"compressionCodec\":\"none\",\"compressedTopics\":\"\",\"messageSendMaxRetries\":3,\"retryBackoffMs\":100,\"topicMetadataRefreshIntervalMs\":600000,\"queueBufferingMaxMs\":5000,\"queueBufferingMaxMessages\":10000,\"queueEnqueueTimeoutMs\":-1,\"batchNumMessages\":200,\"sendBufferBytes\":102400,\"clientId\":\"Threshold_Engine\"}}");
    Config stormConfig = new Config();
    stormConfig.setMaxTaskParallelism(1);
    metricSpout = new FeederSpout(new Fields(MetricSpout.FIELDS));
    eventSpout = new FeederSpout(new Fields("event"));
    alarmEventForwarder = mock(AlarmEventForwarder.class);
    Injector
        .registerModules(new TopologyModule(threshConfig, stormConfig, metricSpout, eventSpout));
    Injector.registerModules(new ProducerModule(alarmEventForwarder));

  }

  @AfterMethod
  public void afterMethod() throws Exception {
    System.out.println("Stopping topology");
    stopTopology();
    cluster = null;
  }

  public void testWithInitialAlarmDefinition() throws Exception {
    when(alarmDefinitionDAO.findById(alarmDefinition.getId())).thenReturn(alarmDefinition);
    when(alarmDefinitionDAO.listAll()).thenReturn(Arrays.asList(alarmDefinition));
    shouldThreshold(null, false);
  }

  public void testWithInitialAlarm() throws Exception {
    when(alarmDefinitionDAO.findById(alarmDefinition.getId())).thenReturn(alarmDefinition);
    when(alarmDefinitionDAO.listAll()).thenReturn(Arrays.asList(alarmDefinition));
    final Alarm alarm = new Alarm(alarmDefinition, AlarmState.UNDETERMINED);
    alarm.addAlarmedMetric(new MetricDefinitionAndTenantId(cpuMetricDef, TEST_ALARM_TENANT_ID));
    alarm.addAlarmedMetric(new MetricDefinitionAndTenantId(memMetricDef, TEST_ALARM_TENANT_ID));
    when(alarmDAO.listAll()).thenReturn(Arrays.asList(alarm));
    when(alarmDAO.findById(alarm.getId())).thenReturn(alarm);
    when(alarmDAO.findForAlarmDefinitionId(alarmDefinition.getId())).thenReturn(Arrays.asList(alarm));
    shouldThreshold(alarm.getId(), true);  
  }

  public void testWithAlarmDefinitionCreatedEvent() throws Exception {
    when(alarmDefinitionDAO.listAll()).thenReturn(new ArrayList<AlarmDefinition>());
    when(alarmDefinitionDAO.findById(alarmDefinition.getId())).thenReturn(alarmDefinition);
    final AlarmDefinitionCreatedEvent event =
        new AlarmDefinitionCreatedEvent(alarmDefinition.getTenantId(), alarmDefinition.getId(),
            alarmDefinition.getName(), alarmDefinition.getDescription(), alarmDefinition
                .getAlarmExpression().getExpression(),
            createSubExpressionMap(alarmDefinition.getAlarmExpression()), Arrays.asList("id"));
    eventSpout.feed(new Values(event));
    shouldThreshold(null, false);
  }

  private Map<String, AlarmSubExpression> createSubExpressionMap(AlarmExpression alarmExpression) {
    final Map<String, AlarmSubExpression> subExprMap = new HashMap<>();
    for (final AlarmSubExpression subExpr : alarmExpression.getSubExpressions()) {
      subExprMap.put(getNextId(), subExpr);
    }
    return subExprMap;
  }

  private String getNextId() {
    return UUID.randomUUID().toString();
  }

  private void shouldThreshold(final String expectedAlarmId,
                               final boolean hasExtraMetric) throws Exception {
    System.out.println("Starting topology");
    startTopology();
    previousState = AlarmState.UNDETERMINED;
    expectedState = AlarmState.ALARM;
    alarmsSent = 0;
    MetricFilteringBolt.clearMetricDefinitions();
    doAnswer(new Answer<Object>() {
      public Object answer(InvocationOnMock invocation) {
        final Object[] args = invocation.getArguments();
        AlarmStateTransitionedEvent event = Serialization.fromJson((String) args[2]);
        alarmsSent++;
        System.out.printf("Alarm transitioned from %s to %s%n", event.oldState, event.newState);
        assertEquals(event.alarmDefinitionId, alarmDefinition.getId());
        assertEquals(event.alarmName, TEST_ALARM_NAME);
        assertEquals(event.tenantId, TEST_ALARM_TENANT_ID);
        if (expectedAlarmId != null) {
          assertEquals(event.alarmId, expectedAlarmId);
        }
        assertEquals(event.oldState, previousState);
        assertEquals(event.newState, expectedState);
        assertEquals(event.metrics.size(), hasExtraMetric ? 3 : 2);
        for (MetricDefinition md : event.metrics) {
          if (md.name.equals(cpuMetricDef.name)) {
            assertEquals(cpuMetricDef, md);
          }
          else if (md.name.equals(memMetricDef.name)) {
            if (md.dimensions.size() == extraMemMetricDefDimensions.size()) {
              assertEquals(extraMemMetricDefDimensions, md.dimensions);
            }
            else if (hasExtraMetric) {
              assertEquals(memMetricDef, md);
            }
            else {
              fail("Incorrect mem Alarmed Metric");
            }
          }
          else {
            fail(String.format("Unrecognized MetricDefinition %s", md));
          }
        }
        previousState = event.newState;
        return null;
      }
    }).when(alarmEventForwarder).send(anyString(), anyString(), anyString());

    doAnswer(new Answer<Object>() {
      public Object answer(InvocationOnMock invocation) {
        final Object[] args = invocation.getArguments();
        final Alarm alarm = (Alarm) args[0];
        when(alarmDAO.findById(alarm.getId())).thenReturn(alarm);
        System.out.printf("Alarm %s created\n", alarm.getId());
        return null;
      }
    }).when(alarmDAO).createAlarm((Alarm)any());
    int waitCount = 0;
    int feedCount = 5;
    int goodValueCount = 0;

    for (int i = 1; i < 40 && alarmsSent == 0; i++) {
      if (feedCount > 0) {
        System.out.println("Feeding metrics...");

        long time = System.currentTimeMillis() / 1000;
        final MetricDefinitionAndTenantId cpuMtid = new MetricDefinitionAndTenantId(cpuMetricDef,
            TEST_ALARM_TENANT_ID);
        metricSpout.feed(new Values(new TenantIdAndMetricName(cpuMtid), time, new Metric(cpuMetricDef.name, cpuMetricDef.dimensions,
            time, (double) (++goodValueCount == 15 ? 1 : 555))));
        final MetricDefinitionAndTenantId memMtid = new MetricDefinitionAndTenantId(memMetricDef,
            TEST_ALARM_TENANT_ID);
        metricSpout.feed(new Values(new TenantIdAndMetricName(memMtid), time, new Metric(memMetricDef.name, extraMemMetricDefDimensions,
            time, (double) (goodValueCount == 15 ? 1 : 555))));

        if (--feedCount == 0) {
          waitCount = 3;
        }

        if (goodValueCount == 15) {
          goodValueCount = 0;
        }
      } else {
        System.out.println("Waiting...");
        if (--waitCount == 0) {
          feedCount = 5;
        }
      }

      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    // Give it some extra time if it needs it for the alarm to come out
    final int maxWait = 30;
    for (int i = 0; i < maxWait && alarmsSent == 0; i++) {
      if ((i % 5) == 0) {
        System.out.printf("Waiting %d more seconds for alarms to be sent\n", maxWait - i);
      }
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    assertTrue(alarmsSent > 0, "Not enough alarms");
    System.out.println("All expected Alarms received");
  }
}
