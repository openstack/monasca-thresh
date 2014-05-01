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
package com.hpcloud.mon;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doAnswer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import backtype.storm.Config;
import backtype.storm.testing.FeederSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.google.inject.AbstractModule;
import com.hpcloud.configuration.KafkaProducerConfiguration;
import com.hpcloud.mon.common.event.AlarmCreatedEvent;
import com.hpcloud.mon.common.event.AlarmStateTransitionedEvent;
import com.hpcloud.mon.common.event.AlarmUpdatedEvent;
import com.hpcloud.mon.common.model.alarm.AlarmExpression;
import com.hpcloud.mon.common.model.alarm.AlarmState;
import com.hpcloud.mon.common.model.alarm.AlarmSubExpression;
import com.hpcloud.mon.common.model.metric.Metric;
import com.hpcloud.mon.domain.model.Alarm;
import com.hpcloud.mon.domain.model.MetricDefinitionAndTenantId;
import com.hpcloud.mon.domain.model.SubAlarm;
import com.hpcloud.mon.domain.service.AlarmDAO;
import com.hpcloud.mon.domain.service.MetricDefinitionDAO;
import com.hpcloud.mon.domain.service.SubAlarmDAO;
import com.hpcloud.mon.domain.service.SubAlarmMetricDefinition;
import com.hpcloud.mon.infrastructure.thresholding.AlarmEventForwarder;
import com.hpcloud.mon.infrastructure.thresholding.EventProcessingBoltTest;
import com.hpcloud.mon.infrastructure.thresholding.MetricAggregationBolt;
import com.hpcloud.mon.infrastructure.thresholding.MetricSpout;
import com.hpcloud.mon.infrastructure.thresholding.ProducerModule;
import com.hpcloud.streaming.storm.TopologyTestCase;
import com.hpcloud.util.Injector;
import com.hpcloud.util.Serialization;

/**
 * Simulates a real'ish run of the thresholding engine with alarms being created, updated and deleted
 */
@Test(groups = "integration")
public class ThresholdingEngineAlarmTest extends TopologyTestCase {
  private static final String TEST_ALARM_TENANT_ID = "bob";
  private static final String TEST_ALARM_ID = "1";
  private static final String TEST_ALARM_NAME = "test-alarm";
  private static final String TEST_ALARM_DESCRIPTION = "Description of test-alarm";
  private FeederSpout metricSpout;
  private FeederSpout eventSpout;
  private AlarmDAO alarmDAO;
  private SubAlarmDAO subAlarmDAO;
  private MetricDefinitionDAO metricDefinitionDAO;
  private final AlarmEventForwarder alarmEventForwarder;
  private int nextSubAlarmId = 4242;
  private List<SubAlarm> subAlarms;
  private AlarmExpression expression = new AlarmExpression(
          "max(hpcs.compute.cpu{id=5}) >= 3 or max(hpcs.compute.mem{id=5}) >= 557");

  private AlarmState currentState = AlarmState.OK;
  private volatile int alarmsSent = 0;

  public ThresholdingEngineAlarmTest() {
    // Fixtures

    subAlarms = subAlarmsFor(TEST_ALARM_ID, expression);

    // Mocks
    alarmDAO = mock(AlarmDAO.class);
    when(alarmDAO.findById(anyString())).thenAnswer(new Answer<Alarm>() {
      @Override
      public Alarm answer(InvocationOnMock invocation) throws Throwable {
        return new Alarm(TEST_ALARM_ID, TEST_ALARM_TENANT_ID, TEST_ALARM_NAME,
                TEST_ALARM_DESCRIPTION, expression, subAlarms, currentState, Boolean.TRUE);
      }
    });

    subAlarmDAO = mock(SubAlarmDAO.class);
    when(subAlarmDAO.find(any(MetricDefinitionAndTenantId.class))).thenAnswer(new Answer<List<SubAlarm>>() {
      @Override
      public List<SubAlarm> answer(InvocationOnMock invocation) throws Throwable {
        MetricDefinitionAndTenantId metricDefinitionAndTenantId = (MetricDefinitionAndTenantId) invocation.getArguments()[0];
        for (final SubAlarm subAlarm : subAlarms) {
            if (metricDefinitionAndTenantId.metricDefinition.equals(subAlarm.getExpression().getMetricDefinition())) {
                return Arrays.asList(subAlarm);
            }
        }
        return Collections.emptyList();
      }
    });

    metricDefinitionDAO = mock(MetricDefinitionDAO.class);
    List<SubAlarmMetricDefinition> metricDefs = new ArrayList<>(0);
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
    metricSpout = new FeederSpout(new Fields(MetricSpout.FIELDS));
    eventSpout = new FeederSpout(new Fields("event"));
    alarmEventForwarder = mock(AlarmEventForwarder.class);
    Injector.registerModules(new TopologyModule(threshConfig, stormConfig,
        metricSpout, eventSpout));
    Injector.registerModules(new ProducerModule(alarmEventForwarder));

    // Evaluate alarm stats every 1 seconds
    System.setProperty(MetricAggregationBolt.TICK_TUPLE_SECONDS_KEY, "5");
  }

  private List<SubAlarm> subAlarmsFor(final String alarmId,
                                      final AlarmExpression expression,
                                      final String ... ids) {
    final List<SubAlarm> result = new ArrayList<SubAlarm>(expression.getSubExpressions().size());
    int index = 0;
    for (final AlarmSubExpression expr : expression.getSubExpressions()) {
      final String id;
      if ((index >= ids.length) || (ids[index] == null)) {
          id = String.valueOf(nextSubAlarmId++);
      }
      else {
          id = ids[index];
      }
      index++;
      result.add(new SubAlarm(id, TEST_ALARM_ID, expr));
    }
    return result;
  }

  final AlarmState[] expectedStates = { AlarmState.ALARM, AlarmState.OK, AlarmState.ALARM };
  public void shouldThreshold() throws Exception {
    doAnswer(new Answer<Object>() {
          public Object answer(InvocationOnMock invocation) {
              final Object[] args = invocation.getArguments();
              AlarmStateTransitionedEvent event = Serialization.fromJson((String)args[2]);
              System.out.printf("Alarm transitioned from %s to %s%n", event.oldState, event.newState);
              assertEquals(event.alarmName, TEST_ALARM_NAME);
              assertEquals(event.alarmId, TEST_ALARM_ID);
              assertEquals(event.tenantId, TEST_ALARM_TENANT_ID);
              assertEquals(event.oldState, currentState);
              currentState = event.newState;
              assertEquals(event.newState, expectedStates[alarmsSent++]);
              return null;
          }
      }
    )
    .when(alarmEventForwarder).send(anyString(), anyString(), anyString());
    int goodValueCount = 0;
    boolean firstUpdate = true;
    boolean secondUpdate = true;
    final Alarm initialAlarm = new Alarm(TEST_ALARM_ID, TEST_ALARM_TENANT_ID, TEST_ALARM_NAME,
            TEST_ALARM_DESCRIPTION, expression, subAlarms, AlarmState.UNDETERMINED, Boolean.TRUE);
    final int expectedAlarms = expectedStates.length;
    for (int i = 1; alarmsSent != expectedAlarms && i < 300; i++) {
      if (i == 5) {
          final Map<String, AlarmSubExpression> exprs = createSubExpressionMap();
          final AlarmCreatedEvent event = new AlarmCreatedEvent(TEST_ALARM_TENANT_ID, TEST_ALARM_ID, TEST_ALARM_NAME,
                  expression.getExpression(), exprs);
          eventSpout.feed(new Values(event));
          System.out.printf("Send AlarmCreatedEvent for expression %s%n", expression.getExpression());
      }
      else if (alarmsSent == 1 && firstUpdate) {
          firstUpdate = false;
          final String originalExpression = expression.getExpression();
          expression = new AlarmExpression(originalExpression.replace(">= 3", ">= 556"));
          assertNotEquals(expression.getExpression(), originalExpression);
          final List<SubAlarm> updatedSubAlarms = new ArrayList<>();
          updatedSubAlarms.add(new SubAlarm(subAlarms.get(0).getId(), initialAlarm.getId(), expression.getSubExpressions().get(0)));
          for (int index = 1; index < subAlarms.size(); index++) {
              final SubAlarm subAlarm = subAlarms.get(index);
              updatedSubAlarms.add(new SubAlarm(subAlarm.getId(), initialAlarm.getId(), subAlarm.getExpression()));
          }

          final AlarmUpdatedEvent event = EventProcessingBoltTest.createAlarmUpdatedEvent(initialAlarm, expression, updatedSubAlarms);
          event.alarmState = currentState;
          subAlarms = updatedSubAlarms;
          eventSpout.feed(new Values(event));

          System.out.printf("Send AlarmUpdatedEvent for expression %s%n", expression.getExpression());
      }
      else if (alarmsSent == 2 && secondUpdate) {
          secondUpdate = false;
          expression = new AlarmExpression("max(hpcs.compute.load{id=5}) > 551 and (" + expression.getExpression().replace("556", "554") + ")");
          final List<SubAlarm> updatedSubAlarms = new ArrayList<>();
          updatedSubAlarms.add(new SubAlarm(UUID.randomUUID().toString(), initialAlarm.getId(), expression.getSubExpressions().get(0)));
          for (int index = 0; index < subAlarms.size(); index++) {
              updatedSubAlarms.add(new SubAlarm(subAlarms.get(index).getId(), initialAlarm.getId(), expression.getSubExpressions().get(index+1)));
          }

          final AlarmUpdatedEvent event = EventProcessingBoltTest.createAlarmUpdatedEvent(initialAlarm, expression, updatedSubAlarms);
          event.alarmState = currentState;
          subAlarms = updatedSubAlarms;
          eventSpout.feed(new Values(event));

          System.out.printf("Send AlarmUpdatedEvent for expression %s%n", expression.getExpression());
      }
      else {
        System.out.println("Feeding metrics...");

        long time = System.currentTimeMillis() / 1000;
        ++goodValueCount;
        for (final SubAlarm subAlarm : subAlarms) {
          final MetricDefinitionAndTenantId metricDefinitionAndTenantId =
                new MetricDefinitionAndTenantId(subAlarm.getExpression().getMetricDefinition(), TEST_ALARM_TENANT_ID);
          metricSpout.feed(new Values(metricDefinitionAndTenantId,
                    new Metric(metricDefinitionAndTenantId.metricDefinition, time, (double) (goodValueCount == 15 ? 1 : 555))));
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
    assertEquals(currentState, AlarmState.ALARM);
  }

  private Map<String, AlarmSubExpression> createSubExpressionMap() {
    final Map<String, AlarmSubExpression> exprs = new HashMap<>();
      for (final SubAlarm subAlarm : subAlarms) {
          exprs.put(subAlarm.getId(), subAlarm.getExpression());
      }
    return exprs;
  }
}
