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

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

import com.hpcloud.configuration.KafkaProducerConfiguration;
import com.hpcloud.mon.common.event.AlarmDefinitionCreatedEvent;
import com.hpcloud.mon.common.event.AlarmStateTransitionedEvent;
import com.hpcloud.mon.common.event.AlarmUpdatedEvent;
import com.hpcloud.mon.common.model.alarm.AlarmExpression;
import com.hpcloud.mon.common.model.alarm.AlarmState;
import com.hpcloud.mon.common.model.alarm.AlarmSubExpression;
import com.hpcloud.mon.common.model.metric.Metric;
import com.hpcloud.streaming.storm.TopologyTestCase;
import com.hpcloud.util.Injector;
import com.hpcloud.util.Serialization;

import backtype.storm.Config;
import backtype.storm.testing.FeederSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.google.inject.AbstractModule;

import monasca.thresh.domain.model.Alarm;
import monasca.thresh.domain.model.AlarmDefinition;
import monasca.thresh.domain.model.MetricDefinitionAndTenantId;
import monasca.thresh.domain.model.SubAlarm;
import monasca.thresh.domain.service.AlarmDAO;
import monasca.thresh.domain.service.AlarmDefinitionDAO;
import monasca.thresh.infrastructure.thresholding.AlarmEventForwarder;
import monasca.thresh.infrastructure.thresholding.EventProcessingBoltTest;
import monasca.thresh.infrastructure.thresholding.MetricAggregationBolt;
import monasca.thresh.infrastructure.thresholding.MetricSpout;
import monasca.thresh.infrastructure.thresholding.ProducerModule;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Simulates a real'ish run of the thresholding engine with alarms being created, updated and
 * deleted
 */
@Test(groups = "integration")
public class ThresholdingEngineAlarmTest extends TopologyTestCase {
  private static final String TEST_ALARM_TENANT_ID = "bob";
  private static final String TEST_ALARM_ID = "1";
  private static final String TEST_ALARM_DEFINITION_ID = "1";
  private static final String TEST_ALARM_NAME = "test-alarm";
  private static final String TEST_ALARM_DESCRIPTION = "Description of test-alarm";
  private FeederSpout metricSpout;
  private FeederSpout eventSpout;
  private AlarmDAO alarmDAO;
  private AlarmDefinitionDAO alarmDefinitionDAO;
  private final AlarmEventForwarder alarmEventForwarder;
  private int nextSubAlarmId = 4242;
  private List<SubAlarm> subAlarms;
  private AlarmExpression expression = new AlarmExpression(
      "max(hpcs.compute.cpu{id=5}) >= 3 or max(hpcs.compute.mem{id=5}) >= 557");

  private AlarmState currentState = AlarmState.UNDETERMINED;
  private volatile int alarmsSent = 0;

  public ThresholdingEngineAlarmTest() {
    // Fixtures
    subAlarms = subAlarmsFor(TEST_ALARM_ID, expression);

    // Mocks
    alarmDAO = mock(AlarmDAO.class);
    /* FIX THIS TO MATCH THE CORRECT METHOD
    when(alarmDAO.findById(anyString())).thenAnswer(new Answer<Alarm>() {
      @Override
      public Alarm answer(InvocationOnMock invocation) throws Throwable {
        return new Alarm(TEST_ALARM_ID, TEST_ALARM_TENANT_ID, TEST_ALARM_NAME,
            TEST_ALARM_DESCRIPTION, expression, subAlarms, currentState, Boolean.TRUE);
      }
    });
    */

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

    // Evaluate alarm stats every 1 seconds
    System.setProperty(MetricAggregationBolt.TICK_TUPLE_SECONDS_KEY, "5");
  }

  private List<SubAlarm> subAlarmsFor(final String alarmId, final AlarmExpression expression,
      final String... ids) {
    final List<SubAlarm> result = new ArrayList<SubAlarm>(expression.getSubExpressions().size());
    int index = 0;
    for (final AlarmSubExpression expr : expression.getSubExpressions()) {
      final String id;
      if ((index >= ids.length) || (ids[index] == null)) {
        id = String.valueOf(nextSubAlarmId++);
      } else {
        id = ids[index];
      }
      index++;
      result.add(new SubAlarm(id, TEST_ALARM_ID, expr));
    }
    return result;
  }

  final AlarmState[] expectedStates = {AlarmState.ALARM, AlarmState.OK, AlarmState.ALARM,
      AlarmState.OK};

  public void shouldThreshold() throws Exception {
    doAnswer(new Answer<Object>() {
      public Object answer(InvocationOnMock invocation) {
        final Object[] args = invocation.getArguments();
        AlarmStateTransitionedEvent event = Serialization.fromJson((String) args[2]);
        System.out.printf("Alarm transitioned from %s to %s%n", event.oldState, event.newState);
        assertEquals(event.alarmName, TEST_ALARM_NAME);
        assertEquals(event.alarmId, TEST_ALARM_ID);
        assertEquals(event.tenantId, TEST_ALARM_TENANT_ID);
        assertEquals(event.oldState, currentState);
        currentState = event.newState;
        assertEquals(event.newState, expectedStates[alarmsSent++]);
        return null;
      }
    }).when(alarmEventForwarder).send(anyString(), anyString(), anyString());
    int goodValueCount = 0;
    boolean firstUpdate = true;
    boolean secondUpdate = true;
    boolean thirdUpdate = true;
    final AlarmDefinition initialAlarmDefinition =
        new AlarmDefinition(TEST_ALARM_DEFINITION_ID, TEST_ALARM_TENANT_ID, TEST_ALARM_NAME,
            TEST_ALARM_DESCRIPTION, expression, Boolean.TRUE, new ArrayList<String>());
    final Alarm initialAlarm =
        new Alarm(TEST_ALARM_ID, subAlarms, initialAlarmDefinition.getId(), AlarmState.UNDETERMINED);
    final int expectedAlarms = expectedStates.length;
    AlarmExpression savedAlarmExpression = null;
    for (int i = 1; alarmsSent != expectedAlarms && i < 300; i++) {
      if (i == 5) {
        final Map<String, AlarmSubExpression> exprs = createSubExpressionMap();
        final AlarmDefinitionCreatedEvent event =
            new AlarmDefinitionCreatedEvent(TEST_ALARM_TENANT_ID, TEST_ALARM_DEFINITION_ID, TEST_ALARM_NAME, TEST_ALARM_DESCRIPTION,
                expression.getExpression(), exprs, Arrays.asList("hostname"));
        eventSpout.feed(new Values(event));
        System.out.printf("Send AlarmCreatedEvent for expression %s%n", expression.getExpression());
      } else if (alarmsSent == 1 && firstUpdate) {
        firstUpdate = false;
        final String originalExpression = expression.getExpression();
        expression = new AlarmExpression(originalExpression.replace(">= 3", ">= 556"));
        assertNotEquals(expression.getExpression(), originalExpression);
        final List<SubAlarm> updatedSubAlarms = new ArrayList<>();
        updatedSubAlarms.add(new SubAlarm(subAlarms.get(0).getId(), initialAlarm.getId(),
            expression.getSubExpressions().get(0)));
        for (int index = 1; index < subAlarms.size(); index++) {
          final SubAlarm subAlarm = subAlarms.get(index);
          updatedSubAlarms.add(new SubAlarm(subAlarm.getId(), initialAlarm.getId(), subAlarm
              .getExpression()));
        }

        initialAlarm.setState(currentState);
        final AlarmUpdatedEvent event =
            EventProcessingBoltTest.createAlarmUpdatedEvent(initialAlarmDefinition, initialAlarm, initialAlarm.getState(),
                expression, updatedSubAlarms);
        subAlarms = updatedSubAlarms;
        initialAlarm.setSubAlarms(updatedSubAlarms);
        eventSpout.feed(new Values(event));

        System.out.printf("Send AlarmUpdatedEvent for expression %s%n", expression.getExpression());
      } else if (alarmsSent == 2 && secondUpdate) {
        secondUpdate = false;
        savedAlarmExpression = expression;
        expression =
            new AlarmExpression("max(hpcs.compute.load{id=5}) > 551 and ("
                + expression.getExpression().replace("556", "554") + ")");
        final List<SubAlarm> updatedSubAlarms = new ArrayList<>();
        updatedSubAlarms.add(new SubAlarm(UUID.randomUUID().toString(), initialAlarm.getId(),
            expression.getSubExpressions().get(0)));
        for (int index = 0; index < subAlarms.size(); index++) {
          updatedSubAlarms.add(new SubAlarm(subAlarms.get(index).getId(), initialAlarm.getId(),
              expression.getSubExpressions().get(index + 1)));
        }

        initialAlarm.setState(currentState);
        final AlarmUpdatedEvent event =
            EventProcessingBoltTest.createAlarmUpdatedEvent(initialAlarmDefinition, initialAlarm, initialAlarm.getState(),
                expression, updatedSubAlarms);
        subAlarms = updatedSubAlarms;
        initialAlarm.setSubAlarms(updatedSubAlarms);
        eventSpout.feed(new Values(event));

        System.out.printf("Send AlarmUpdatedEvent for expression %s%n", expression.getExpression());
      } else if (alarmsSent == 3 && thirdUpdate) {
        thirdUpdate = false;
        expression = savedAlarmExpression;
        final List<SubAlarm> updatedSubAlarms = new ArrayList<>();
        int index = 1;
        for (AlarmSubExpression subExpression : expression.getSubExpressions()) {
          updatedSubAlarms.add(new SubAlarm(subAlarms.get(index).getId(), initialAlarm.getId(),
              subExpression));
          index++;
        }

        initialAlarm.setState(currentState);
        final AlarmUpdatedEvent event =
            EventProcessingBoltTest.createAlarmUpdatedEvent(initialAlarmDefinition, initialAlarm, initialAlarm.getState(),
                expression, updatedSubAlarms);
        subAlarms = updatedSubAlarms;
        initialAlarm.setSubAlarms(updatedSubAlarms);
        eventSpout.feed(new Values(event));

        System.out.printf("Send AlarmUpdatedEvent for expression %s%n", expression.getExpression());
      } else {
        System.out.println("Feeding metrics...");

        long time = System.currentTimeMillis() / 1000;
        ++goodValueCount;
        for (final SubAlarm subAlarm : subAlarms) {
          final MetricDefinitionAndTenantId metricDefinitionAndTenantId =
              new MetricDefinitionAndTenantId(subAlarm.getExpression().getMetricDefinition(),
                  TEST_ALARM_TENANT_ID);
          metricSpout.feed(new Values(metricDefinitionAndTenantId, time, new Metric(
              metricDefinitionAndTenantId.metricDefinition, time,
              (double) (goodValueCount == 15 ? 1 : 555))));
        }
      }
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    for (int i = 0; alarmsSent != expectedAlarms && i < 60; i++) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    assertEquals(alarmsSent, expectedAlarms);
    assertEquals(currentState, expectedStates[expectedStates.length - 1]);
  }

  private Map<String, AlarmSubExpression> createSubExpressionMap() {
    final Map<String, AlarmSubExpression> exprs = new HashMap<>();
    for (final SubAlarm subAlarm : subAlarms) {
      exprs.put(subAlarm.getId(), subAlarm.getExpression());
    }
    return exprs;
  }
}
