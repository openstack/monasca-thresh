/*
 * (C) Copyright 2014,2016 Hewlett Packard Enterprise Development Company LP.
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
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import monasca.common.configuration.KafkaProducerConfiguration;
import monasca.common.model.event.AlarmDefinitionCreatedEvent;
import monasca.common.model.event.AlarmDefinitionDeletedEvent;
import monasca.common.model.event.AlarmDefinitionUpdatedEvent;
import monasca.common.model.event.AlarmDeletedEvent;
import monasca.common.model.event.AlarmStateTransitionedEvent;
import monasca.common.model.event.AlarmUpdatedEvent;
import monasca.common.model.alarm.AlarmExpression;
import monasca.common.model.alarm.AlarmState;
import monasca.common.model.alarm.AlarmSubExpression;
import monasca.common.model.metric.Metric;
import monasca.common.model.metric.MetricDefinition;
import monasca.common.util.Injector;
import monasca.common.util.Serialization;
import monasca.thresh.domain.model.Alarm;
import monasca.thresh.domain.model.AlarmDefinition;
import monasca.thresh.domain.model.MetricDefinitionAndTenantId;
import monasca.thresh.domain.model.SubAlarm;
import monasca.thresh.domain.model.SubExpression;
import monasca.thresh.domain.model.TenantIdAndMetricName;
import monasca.thresh.domain.service.AlarmDAO;
import monasca.thresh.domain.service.AlarmDefinitionDAO;
import monasca.thresh.infrastructure.thresholding.AlarmEventForwarder;
import monasca.thresh.infrastructure.thresholding.EventProcessingBoltTest;
import monasca.thresh.infrastructure.thresholding.MetricAggregationBolt;
import monasca.thresh.infrastructure.thresholding.MetricFilteringBoltTest;
import monasca.thresh.infrastructure.thresholding.MetricSpout;
import monasca.thresh.infrastructure.thresholding.ProducerModule;

import com.google.inject.AbstractModule;

import org.apache.storm.Config;
import org.apache.storm.testing.FeederSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Simulates a real'ish run of the thresholding engine with alarms being created, updated and
 * deleted
 *
 * This doesn't currently do everything it used to because thresh doesn't handle updating
 * expressions of Alarm Definitions. So lots of stuff commented
 */
@Test(groups = "integration")
public class ThresholdingEngineAlarmTest extends TopologyTestCase {
  private static final String TEST_ALARM_TENANT_ID = "bob";
  private static final String TEST_ALARM_NAME = "test-alarm";
  private static final String TEST_ALARM_DESCRIPTION = "Description of test-alarm";
  private static final String TEST_ALARM_SEVERITY = "LOW";
  private FeederSpout metricSpout;
  private FeederSpout eventSpout;
  private MockAlarmDAO alarmDAO;
  private AlarmDefinitionDAO alarmDefinitionDAO;
  private AlarmEventForwarder alarmEventForwarder;

  private AlarmState currentState = AlarmState.UNDETERMINED;
  private AlarmState expectedState;
  private volatile int alarmsSent = 0;

  @BeforeMethod
  public void beforeMethod() throws Exception {
    // Fixtures
    alarmDefinitionDAO = mock(AlarmDefinitionDAO.class);

    // Mocks
    alarmDAO = new MockAlarmDAO();

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
    threshConfig.alarmDelay = 1;
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

    // Evaluate alarm stats every 5 seconds
    System.setProperty(MetricAggregationBolt.TICK_TUPLE_SECONDS_KEY, "5");

    startTopology();
  }

  @AfterMethod
  public void afterMethod() {
    stopTopology();
  }

  private String expectedAlarmDefinitionId;
  private String expectedAlarmName = TEST_ALARM_NAME;
  private String expectedAlarmDescription = TEST_ALARM_DESCRIPTION;
  private String expectedAlarmSeverity = TEST_ALARM_SEVERITY;

  private enum Stages {
    INITIAL_WAIT,
    ALARM_DEFINITION_CREATED_SENT,
    FIRST_ALARM_CREATED,
    WAITING_FOR_FIRST_STATUS_UPDATE,
    ALARM_DEFINITION_UPDATED_SENT,
    ALARM_UPDATED,
    ALARM_DELETED,
    ALARM_RECREATED,
    WAITING_FOR_SECOND_STATUS_UPDATE,
    ALARM_DEFINITION_DELETED,
    FINISHED
  }

  public void shouldThreshold() throws Exception {
    doAnswer(new Answer<Object>() {
      public Object answer(InvocationOnMock invocation) {
        final Object[] args = invocation.getArguments();
        AlarmStateTransitionedEvent event = Serialization.fromJson((String) args[0]);
        System.out.printf("Alarm transitioned from %s to %s%n", event.oldState, event.newState);
        assertEquals(event.alarmName, expectedAlarmName);
        assertEquals(event.alarmDefinitionId, expectedAlarmDefinitionId);
        assertEquals(event.alarmDescription, expectedAlarmDescription);
        assertEquals(event.severity, expectedAlarmSeverity);
        assertEquals(event.tenantId, TEST_ALARM_TENANT_ID);
        assertEquals(event.oldState, currentState);
        currentState = event.newState;
        assertEquals(event.newState, expectedState);
        // TODO Check Alarmed Metrics
        alarmsSent++;
        return null;
      }
    }).when(alarmEventForwarder).send(anyString());
    final AlarmExpression initialExpression = new AlarmExpression(
        "max(hpcs.compute.cpu{id=5}) >= 556 or max(hpcs.compute.mem{id=5}) >= 557");

    final AlarmDefinition alarmDefinition =
        new AlarmDefinition(TEST_ALARM_TENANT_ID, TEST_ALARM_NAME,
            TEST_ALARM_DESCRIPTION, initialExpression, "LOW", Boolean.TRUE, new ArrayList<String>());
    expectedAlarmDefinitionId = alarmDefinition.getId();

    final Set<MetricDefinitionAndTenantId> mtids = new HashSet<MetricDefinitionAndTenantId>();
    for (final AlarmSubExpression subExpr : initialExpression.getSubExpressions()) {
      final Map<String, String> dimensions = new HashMap<>(subExpr.getMetricDefinition().dimensions);
      dimensions.put("hostname", "eleanore");
      final MetricDefinition metricDefinition = new MetricDefinition(subExpr.getMetricDefinition().name, dimensions);
      final MetricDefinitionAndTenantId metricDefinitionAndTenantId =
          new MetricDefinitionAndTenantId(metricDefinition, TEST_ALARM_TENANT_ID);
      mtids.add(metricDefinitionAndTenantId);
    }

    Alarm alarm = null;
    Stages stage = Stages.INITIAL_WAIT;
    int finishAt = 0;
    for (int i = 1; i < 600 && stage != Stages.FINISHED; i++) {
      switch (stage) {
        case INITIAL_WAIT:
          if (i == 5) {
            final Map<String, AlarmSubExpression> exprs =
                MetricFilteringBoltTest.createSubExpressionMap(alarmDefinition);
            final AlarmDefinitionCreatedEvent event =
                new AlarmDefinitionCreatedEvent(alarmDefinition.getTenantId(),
                    alarmDefinition.getId(), alarmDefinition.getName(),
                    alarmDefinition.getDescription(), initialExpression.getExpression(), exprs,
                    Arrays.asList("hostname"));
            when(alarmDefinitionDAO.findById(alarmDefinition.getId())).thenReturn(alarmDefinition);
            eventSpout.feed(new Values(event));
            System.out.printf("Sent AlarmDefinitionCreatedEvent for expression %s%n",
                initialExpression.getExpression());
            stage = Stages.ALARM_DEFINITION_CREATED_SENT;
          }
          break;
        case ALARM_DEFINITION_CREATED_SENT: {
            final List<Alarm> alarms = alarmDAO.listAll();
            if (alarms.size() > 0) {
              assertEquals(1, alarms.size());
              alarm = alarms.get(0);
              System.out.printf("Alarm %s created with state %s\n", alarm.getId(), alarm.getState());
              System.out.println("Waiting for state change");
              stage = Stages.FIRST_ALARM_CREATED;
              expectedState = AlarmState.OK;
            }
          }
          break;
        case FIRST_ALARM_CREATED:
          if (alarmsSent == 1) {
            Map<String, AlarmSubExpression> empty = new HashMap<>();
            Map<String, AlarmSubExpression> unchangedSubExpressions = new HashMap<>();
            Map<String, AlarmSubExpression> changedSubExpressions = new HashMap<>();
            final SubExpression changed = alarmDefinition.getSubExpressions().get(0);
            final int original = (int)changed.getAlarmSubExpression().getThreshold();
            final int newThreshold = 554;
            changed.getAlarmSubExpression().setThreshold(newThreshold);
            alarmDefinition.getAlarmExpression().getSubExpressions().get(0).setThreshold(newThreshold);
            final String newAlarmExpression =
                alarmDefinition.getAlarmExpression().getExpression()
                    .replace(String.valueOf(original), String.valueOf(newThreshold));
            assertNotEquals(alarmDefinition.getAlarmExpression().getExpression(), newAlarmExpression);
            final SubExpression unchanged = alarmDefinition.getSubExpressions().get(1);
            changedSubExpressions.put(changed.getId(), changed.getAlarmSubExpression());
            unchangedSubExpressions.put(unchanged.getId(), unchanged.getAlarmSubExpression());
            alarmDefinition.setName(expectedAlarmName = "New Alarm Name");
            alarmDefinition.setDescription(expectedAlarmDescription = "New Alarm Description");
            alarmDefinition.setSeverity(expectedAlarmSeverity = "HIGH");
            alarmDefinition.setExpression(newAlarmExpression);
            expectedState = AlarmState.ALARM;
            final AlarmDefinitionUpdatedEvent alarmDefinitionUpdatedEvent =
                new AlarmDefinitionUpdatedEvent(TEST_ALARM_TENANT_ID, alarmDefinition.getId(),
                    alarmDefinition.getName(), alarmDefinition.getDescription(),
                    newAlarmExpression, alarmDefinition.getMatchBy(), false,
                    alarmDefinition.getSeverity(), empty, changedSubExpressions,
                    unchangedSubExpressions, empty);
            eventSpout.feed(new Values(alarmDefinitionUpdatedEvent));
            System.out.println("Sent AlarmDefinitionUpdatedEvent");
            stage = Stages.ALARM_DEFINITION_UPDATED_SENT;
          }
          break;
        case ALARM_DEFINITION_UPDATED_SENT:
          if (alarmsSent == 2) {
            final AlarmUpdatedEvent alarmUpdatedEvent =
                EventProcessingBoltTest.createAlarmUpdatedEvent(alarmDefinition, alarm,
                    AlarmState.OK);
            // An AlarmStateTransitionedEvent doesn't get generated for this so change the current state
            // manually
            currentState = AlarmState.OK;
            expectedState = AlarmState.ALARM;
            eventSpout.feed(new Values(alarmUpdatedEvent));
            System.out.println("Sent AlarmUpdatedEvent to " + alarmUpdatedEvent.alarmState);
            stage = Stages.ALARM_UPDATED;
          }
          break;
        case ALARM_UPDATED:
          if (alarmsSent == 3) {
            currentState = AlarmState.UNDETERMINED;
            expectedState = AlarmState.ALARM;
            // Delete Alarm
            alarmDAO.deleteAlarm(alarm);
            Map<String, AlarmSubExpression> subAlarmMap = new HashMap<>();
            for (final SubAlarm subAlarm : alarm.getSubAlarms()) {
              subAlarmMap.put(subAlarm.getId(), subAlarm.getExpression());
            }
            List<MetricDefinition> alarmedMetrics = new ArrayList<>();
            for (final MetricDefinitionAndTenantId mdtid : alarm.getAlarmedMetrics()) {
              alarmedMetrics.add(mdtid.metricDefinition);
            }
            final AlarmDeletedEvent alarmDeletedEvent =
                new AlarmDeletedEvent(alarmDefinition.getTenantId(), alarm.getId(),
                    alarmedMetrics, alarmDefinition.getId(), subAlarmMap);
            eventSpout.feed(new Values(alarmDeletedEvent));
            System.out.println("Deleted initial Alarm, waiting for recreation");
            stage = Stages.ALARM_DELETED;
          }
          break;
        case ALARM_DELETED:
          {
            final List<Alarm> alarms = alarmDAO.listAll();
            if (alarms.size() > 0) {
              assertEquals(1, alarms.size());
              alarm = alarms.get(0);
              System.out.printf("Alarm %s created with state %s\n", alarm.getId(), alarm.getState());
              System.out.printf("Waiting for state change\n");
              stage = Stages.ALARM_RECREATED;
            }
          }
          break;
        case ALARM_RECREATED:
          if (alarmsSent == 4) {
            // Delete Alarm definition
            // Delete Alarm
            stage = Stages.ALARM_DEFINITION_DELETED;
            final AlarmDefinitionDeletedEvent alarmDefinitionDeletedEvent =
                EventProcessingBoltTest.createAlarmDefinitionDeletedEvent(alarmDefinition);

            when(alarmDefinitionDAO.findById(alarmDefinition.getId())).thenReturn(null);
            eventSpout.feed(new Values(alarmDefinitionDeletedEvent));
            System.out.println("Sent AlarmDefinitionDeletedEvent");

            final List<MetricDefinition> alarmedMetrics = new ArrayList<>();
            for (final MetricDefinitionAndTenantId mtid : alarm.getAlarmedMetrics()) {
              alarmedMetrics.add(mtid.metricDefinition);
            }
            final Map<String, AlarmSubExpression> subAlarms = new HashMap<>();
            for (final SubAlarm subAlarm : alarm.getSubAlarms()) {
              subAlarms.put(subAlarm.getId(), subAlarm.getExpression());
            }
            final AlarmDeletedEvent deleteEvent =
                new AlarmDeletedEvent(TEST_ALARM_TENANT_ID, alarm.getId(), alarmedMetrics,
                    alarm.getAlarmDefinitionId(), subAlarms);
            assertTrue(alarmDAO.deleteAlarm(alarm));
            eventSpout.feed(new Values(deleteEvent));
            System.out.println("Sent AlarmDeletedEvent");
            // Wait for 5 or so iterations
            finishAt = i + 5;
          }
          break;
        case ALARM_DEFINITION_DELETED:
          if (i >= finishAt) {
            stage = Stages.FINISHED;
          }
          break;
        default:
          fail(String.format("Unknown stage %s", stage));
          break; // Never reached
      }
      System.out.printf("Feeding metrics...%d Stage %s\n", i, stage);

      long time = System.currentTimeMillis();
      for (final MetricDefinitionAndTenantId metricDefinitionAndTenantId : mtids) {
        metricSpout.feed(new Values(new TenantIdAndMetricName(metricDefinitionAndTenantId), time,
            new Metric(metricDefinitionAndTenantId.metricDefinition, time,
                (double) 555, null)));
      }
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    assertEquals(stage, Stages.FINISHED);
    assertEquals(alarmsSent, 4);
    assertEquals(0, alarmDAO.listAll().size());
  }

  final class MockAlarmDAO implements AlarmDAO {

    final List<Alarm> alarms = new LinkedList<>();

    @Override
    public Alarm findById(String id) {
      for (final Alarm alarm : alarms) {
        if (alarm.getId().equals(id)) {
          return alarm;
        }
      }
      fail("Did not find Alarm for id=" + id);
      return null;
    }

    @Override
    public List<Alarm> findForAlarmDefinitionId(String alarmDefinitionId) {
      final List<Alarm> result = new LinkedList<>();
      for (final Alarm alarm : alarms) {
        if (alarm.getAlarmDefinitionId().equals(alarmDefinitionId)) {
          result.add(alarm);
        }
      }
      return result;
    }

    @Override
    public List<Alarm> listAll() {
      return alarms;
    }

    @Override
    public void updateState(String id, AlarmState state) {
      findById(id).setState(state);
    }

    @Override
    public void addAlarmedMetric(String id, MetricDefinitionAndTenantId metricDefinition) {
      findById(id).addAlarmedMetric(metricDefinition);
    }

    @Override
    public void createAlarm(Alarm newAlarm) {
      alarms.add(newAlarm);
    }

    @Override
    public int updateSubAlarmExpressions(String alarmSubExpressionId,
        AlarmSubExpression alarmSubExpression) {
      int updated = 0;
      for (final Alarm alarm : alarms) {
        for (final SubAlarm subAlarm : alarm.getSubAlarms()) {
          if (subAlarm.getAlarmSubExpressionId().equals(alarmSubExpressionId)) {
            subAlarm.setExpression(alarmSubExpression);
            updated++;
          }
        }
      }
      return updated;
    }

    public boolean deleteAlarm(final Alarm toDelete) {
      for (final Alarm alarm : alarms) {
        if (alarm.getId().equals(toDelete.getId())) {
           alarms.remove(alarm);
           return true;
        }
      }
      return false;
    }

    @Override
    public void deleteByDefinitionId(String alarmDefinitionId) {}
  }
}
