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

package monasca.thresh.infrastructure.thresholding;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertNotEquals;

import monasca.common.model.event.AlarmDefinitionCreatedEvent;
import monasca.common.model.event.AlarmDefinitionDeletedEvent;
import monasca.common.model.event.AlarmDefinitionUpdatedEvent;
import monasca.common.model.event.AlarmDeletedEvent;
import monasca.common.model.event.AlarmUpdatedEvent;
import monasca.common.model.alarm.AlarmExpression;
import monasca.common.model.alarm.AlarmState;
import monasca.common.model.alarm.AlarmSubExpression;
import monasca.common.model.metric.MetricDefinition;
import monasca.common.streaming.storm.Streams;

import backtype.storm.Testing;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.MkTupleParam;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Sets;

import monasca.thresh.domain.model.Alarm;
import monasca.thresh.domain.model.AlarmDefinition;
import monasca.thresh.domain.model.MetricDefinitionAndTenantId;
import monasca.thresh.domain.model.SubAlarm;
import monasca.thresh.domain.model.TenantIdAndMetricName;

import org.mockito.verification.VerificationMode;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@Test
public class EventProcessingBoltTest {

  private static final String TENANT_ID = "AAAAABBBBBBCCCCC";
  private EventProcessingBolt bolt;
  private OutputCollector collector;
  private AlarmExpression alarmExpression;
  private Alarm alarm;
  private AlarmDefinition alarmDefinition;
  private List<SubAlarm> subAlarms;
  private final Map<String, AlarmSubExpression> subAlarmMap = new HashMap<>();

  @BeforeMethod
  protected void beforeMethod() {
    collector = mock(OutputCollector.class);
    bolt = new EventProcessingBolt();

    final Map<String, String> config = new HashMap<>();
    final TopologyContext context = mock(TopologyContext.class);
    bolt.prepare(config, context, collector);

    final String alarmId = "111111112222222222233333333334";
    final String name = "Test CPU Alarm";
    final String description = "Description of " + name;
    final String expression =
        "avg(hpcs.compute.cpu{instance_id=123,device=42}, 1) > 5 "
            + "and max(hpcs.compute.mem{instance_id=123,device=42}) > 80 "
            + "and max(hpcs.compute.load{instance_id=123,device=42}) > 5";
    alarmExpression = new AlarmExpression(expression);
    alarmDefinition =
        new AlarmDefinition("424242", TENANT_ID, name, description, alarmExpression, "LOW",
            Boolean.TRUE, Arrays.asList("hostname"));
    alarm = new Alarm(alarmId, alarmDefinition, AlarmState.UNDETERMINED);
    for (final AlarmSubExpression subExpr : alarmDefinition.getAlarmExpression()
        .getSubExpressions()) {
      final Map<String, String> newDimensions =
          new HashMap<>(subExpr.getMetricDefinition().dimensions);
      newDimensions.put("hostname", "vivi");
      alarm.addAlarmedMetric(new MetricDefinitionAndTenantId(new MetricDefinition(subExpr
          .getMetricDefinition().name, newDimensions), alarmDefinition.getTenantId()));
    }
    subAlarms = new ArrayList<SubAlarm>(alarm.getSubAlarms());
    for (final SubAlarm subAlarm : alarm.getSubAlarms()) {
      subAlarmMap.put(subAlarm.getId(), subAlarm.getExpression());
    }
  }

  public void testAlarmDefinitionCreatedEvent() {
    final Map<String, AlarmSubExpression> expressions = createAlarmSubExpressionMap(alarm);
    final AlarmDefinitionCreatedEvent event =
        new AlarmDefinitionCreatedEvent(alarmDefinition.getTenantId(), alarmDefinition.getId(),
            alarmDefinition.getName(), alarmDefinition.getDescription(), alarmDefinition
                .getAlarmExpression().getExpression(), expressions, Arrays.asList("hostname"));
    final Tuple tuple = createTuple(event);
    bolt.execute(tuple);
    verify(collector, times(1)).ack(tuple);
    verify(collector, times(1)).emit(
        EventProcessingBolt.ALARM_DEFINITION_EVENT_STREAM_ID,
        new Values(EventProcessingBolt.CREATED, event));
  }

  public void testAlarmDefinitionDeletedEvent() {
    final AlarmDefinitionDeletedEvent event =
        new AlarmDefinitionDeletedEvent(alarmDefinition.getId(), null);
    final Tuple tuple = createTuple(event);
    bolt.execute(tuple);
    verify(collector, times(1)).ack(tuple);
    verify(collector, times(1)).emit(
        EventProcessingBolt.ALARM_DEFINITION_EVENT_STREAM_ID,
        new Values(EventProcessingBolt.DELETED, event));
  }

  private Tuple createTuple(final Object event) {
    MkTupleParam tupleParam = new MkTupleParam();
    tupleParam.setFields("event");
    tupleParam.setStream(Streams.DEFAULT_STREAM_ID);
    final Tuple tuple = Testing.testTuple(Arrays.asList(event), tupleParam);
    return tuple;
  }

  public void testAlarmDeletedEvent() {
    final Map<String, MetricDefinition> subAlarmMetricDefs = new HashMap<>();
    for (final SubAlarm subAlarm : alarm.getSubAlarms()) {
      subAlarmMetricDefs.put(subAlarm.getId(), subAlarm.getExpression().getMetricDefinition());
    }
    final List<MetricDefinition> alarmedMetricsDefs = new LinkedList<>();
    for (final MetricDefinitionAndTenantId mtid : alarm.getAlarmedMetrics()) {
      alarmedMetricsDefs.add(mtid.metricDefinition);
    }
    final AlarmDeletedEvent event =
        new AlarmDeletedEvent(alarmDefinition.getTenantId(), alarm.getId(), alarmedMetricsDefs,
            alarmDefinition.getId(), subAlarmMap);
    final Tuple tuple = createTuple(event);
    bolt.execute(tuple);
    for (final SubAlarm subAlarm : alarm.getSubAlarms()) {
      for (final MetricDefinitionAndTenantId mtid : alarm.getAlarmedMetrics()) {
        // This is not the real check but it is sufficient for this test
        if (mtid.metricDefinition.name.equals(subAlarm.getExpression().getMetricDefinition().name)) {
          verifyDeletedSubAlarm(mtid, alarm.getAlarmDefinitionId(), subAlarm);
        }
      }
    }
    verify(collector, times(1)).emit(EventProcessingBolt.ALARM_EVENT_STREAM_ID,
        new Values(EventProcessingBolt.DELETED, event.alarmId, event));
    verify(collector, times(1)).ack(tuple);
  }

  private void verifyDeletedSubAlarm(MetricDefinitionAndTenantId mtid, String alarmDefinitionId,
      final SubAlarm subAlarm) {
    verify(collector, times(1)).emit(
        EventProcessingBolt.METRIC_ALARM_EVENT_STREAM_ID,
        new Values(EventProcessingBolt.DELETED, new TenantIdAndMetricName(mtid), mtid,
            alarmDefinitionId, subAlarm.getId()));
  }

  public static AlarmUpdatedEvent createAlarmUpdatedEvent(final AlarmDefinition alarmDefinition,
      final Alarm alarm, final AlarmState newState) {

    final List<MetricDefinition> alarmedMetrics = new ArrayList<>();
    for (final MetricDefinitionAndTenantId mdtid : alarm.getAlarmedMetrics()) {
      alarmedMetrics.add(mdtid.metricDefinition);
    }
    final Map<String, MetricDefinition> subAlarmMetricDefinitions = new HashMap<>();
    for (final SubAlarm subAlarm : alarm.getSubAlarms()) {
      subAlarmMetricDefinitions.put(subAlarm.getId(), subAlarm.getExpression().getMetricDefinition());
    }
    final Map<String, AlarmSubExpression> subAlarmMap = new HashMap<>();
    for (final SubAlarm subAlarm : alarm.getSubAlarms()) {
      subAlarmMap.put(subAlarm.getId(), subAlarm.getExpression());
    }
    final AlarmUpdatedEvent event =
        new AlarmUpdatedEvent(alarm.getId(), alarmDefinition.getId(),
            alarmDefinition.getTenantId(), alarmedMetrics, subAlarmMap, newState, alarm.getState());
   return event;
  }

  public static AlarmDefinitionUpdatedEvent createAlarmDefinitionUpdatedEvent(final AlarmDefinition alarmDefinition,
      final Alarm alarm, final AlarmState newState, final AlarmExpression updatedAlarmExpression,
      List<SubAlarm> updatedSubAlarms) {
    final Map<String, AlarmSubExpression> oldAlarmSubExpressions = new HashMap<>();
    for (final SubAlarm subAlarm : alarm.getSubAlarms()) {
      oldAlarmSubExpressions.put(subAlarm.getId(), subAlarm.getExpression());
    }
    BiMap<String, AlarmSubExpression> oldExpressions = HashBiMap.create(oldAlarmSubExpressions);
    Set<AlarmSubExpression> oldSet = oldExpressions.inverse().keySet();
    Set<AlarmSubExpression> newSet = new HashSet<>();
    for (final SubAlarm subAlarm : updatedSubAlarms) {
      newSet.add(subAlarm.getExpression());
    }

    // Identify old or changed expressions
    Set<AlarmSubExpression> oldOrChangedExpressions =
        new HashSet<>(Sets.difference(oldSet, newSet));

    // Identify new or changed expressions
    Set<AlarmSubExpression> newOrChangedExpressions =
        new HashSet<>(Sets.difference(newSet, oldSet));

    // Find changed expressions
    Map<String, AlarmSubExpression> changedExpressions = new HashMap<>();
    for (Iterator<AlarmSubExpression> oldIt = oldOrChangedExpressions.iterator(); oldIt.hasNext();) {
      AlarmSubExpression oldExpr = oldIt.next();
      for (Iterator<AlarmSubExpression> newIt = newOrChangedExpressions.iterator(); newIt.hasNext();) {
        AlarmSubExpression newExpr = newIt.next();
        if (sameKeyFields(oldExpr, newExpr)) {
          oldIt.remove();
          newIt.remove();
          changedExpressions.put(oldExpressions.inverse().get(oldExpr), newExpr);
          break;
        }
      }
    }

    BiMap<String, AlarmSubExpression> unchangedExpressions = HashBiMap.create(oldExpressions);
    unchangedExpressions.values().removeAll(oldOrChangedExpressions);
    unchangedExpressions.keySet().removeAll(changedExpressions.keySet());

    // Remove old sub expressions
    oldExpressions.values().retainAll(oldOrChangedExpressions);

    // Create IDs for new expressions
    Map<String, AlarmSubExpression> newExpressions = new HashMap<>();
    for (AlarmSubExpression expression : newOrChangedExpressions) {
      for (final SubAlarm subAlarm : updatedSubAlarms) {
        if (subAlarm.getExpression().equals(expression)) {
          newExpressions.put(subAlarm.getId(), expression);
        }
      }
    }

    final AlarmDefinitionUpdatedEvent event =
        /* Get the right constructor
        new AlarmUpdatedEvent(alarm.getId(), alarmDefinition.getId(), newState, alarm.getState());
        */ null;
    return event;
  }

  /**
   * Returns whether all of the fields of {@code a} and {@code b} are the same except the operator
   * and threshold.
   */
  private static boolean sameKeyFields(AlarmSubExpression a, AlarmSubExpression b) {
    return a.getMetricDefinition().equals(b.getMetricDefinition())
        && a.getFunction().equals(b.getFunction()) && a.getPeriod() == b.getPeriod()
        && a.getPeriods() == b.getPeriods();
  }

  public void testAlarmDefinitionUpdatedEvent() {
    /* Current code does not allow updates of the expressions
    final String updatedExpression =
        "avg(hpcs.compute.cpu{instance_id=123,device=42}, 1) > 5 "
            + "and max(hpcs.compute.mem{instance_id=123,device=42}) > 90 "
            + "and max(hpcs.compute.newLoad{instance_id=123,device=42}) > 5";

    final AlarmExpression updatedAlarmExpression = new AlarmExpression(updatedExpression);

    final List<SubAlarm> updatedSubAlarms = new ArrayList<>();
    updatedSubAlarms.add(subAlarms.get(0));
    updatedSubAlarms.add(new SubAlarm(subAlarms.get(1).getId(), alarm.getId(),
        updatedAlarmExpression.getSubExpressions().get(1)));
    updatedSubAlarms.add(new SubAlarm(UUID.randomUUID().toString(), alarm.getId(),
        updatedAlarmExpression.getSubExpressions().get(2)));

    */
    Map<String, AlarmSubExpression> unchangedSubExpressions = new HashMap<>();
    for (final AlarmSubExpression subExpr : alarmDefinition.getAlarmExpression().getSubExpressions()) {
      unchangedSubExpressions.put(UUID.randomUUID().toString(), subExpr);
    }
    Map<String, AlarmSubExpression> emptySubExpressions = new HashMap<>();
    final AlarmDefinitionUpdatedEvent event =
        new AlarmDefinitionUpdatedEvent(alarmDefinition.getTenantId(), alarmDefinition.getId(),
            "New Name", "New Description", alarmDefinition.getAlarmExpression().getExpression(), alarmDefinition.getMatchBy(),
            false, "HIGH", emptySubExpressions, emptySubExpressions, unchangedSubExpressions, emptySubExpressions);

    final Tuple tuple = createTuple(event);
    bolt.execute(tuple);
    verify(collector, times(1)).ack(tuple);

    // TODO - FIX THIS IF WE EVER SUPPORT ALARM EXPRESSION UPDATES
    // verifyDeletedSubAlarm(subAlarms.get(2));
    // verifyUpdatedSubAlarm(updatedSubAlarms.get(1));
    // verifyAddedSubAlarm(updatedSubAlarms.get(2));
    verify(collector, times(1)).emit(EventProcessingBolt.ALARM_DEFINITION_EVENT_STREAM_ID,
        new Values(EventProcessingBolt.UPDATED, event));
  }

  public void testAlarmUpdatedEvent() {
    
    final AlarmState newState = AlarmState.OK;
    assertNotEquals(alarm.getState(), newState);
    final AlarmUpdatedEvent event =
        createAlarmUpdatedEvent(alarmDefinition, alarm, newState);

    final Tuple tuple = createTuple(event);
    bolt.execute(tuple);
    verify(collector, times(1)).ack(tuple);

    verifyResendsSent(alarm, alarmDefinition);
    verify(collector, times(1)).emit(EventProcessingBolt.ALARM_EVENT_STREAM_ID,
        new Values(EventProcessingBolt.UPDATED, event.alarmId, event));
  }

  private void verifyResendsSent(Alarm alarm, AlarmDefinition alarmDefinition) {
    for (final MetricDefinitionAndTenantId mdtid : alarm.getAlarmedMetrics()) {
      for (final SubAlarm subAlarm : alarm.getSubAlarms()) {
        // This is not the real check but is sufficient for this test
        final VerificationMode wanted;
        if (subAlarm.getExpression().getMetricDefinition().name.equals(mdtid.metricDefinition.name)) {
          wanted = times(1);
        } else {
          wanted = never();
        }
        verify(collector, wanted).emit(
            EventProcessingBolt.METRIC_ALARM_EVENT_STREAM_ID,
            new Values(EventProcessingBolt.RESEND, new TenantIdAndMetricName(mdtid), mdtid,
                alarmDefinition.getId(), subAlarm.getId()));
      }
    }
  }

  private void verifyAddedSubAlarm(final SubAlarm subAlarm) {
    sendSubAlarm(subAlarm, EventProcessingBolt.CREATED);
  }

  private void verifyUpdatedSubAlarm(final SubAlarm subAlarm) {
    sendSubAlarm(subAlarm, EventProcessingBolt.UPDATED);
  }

  private void sendSubAlarm(final SubAlarm subAlarm, String eventType) {
    verify(collector, times(1)).emit(
        EventProcessingBolt.METRIC_SUB_ALARM_EVENT_STREAM_ID,
        new Values(eventType, new MetricDefinitionAndTenantId(subAlarm.getExpression()
            .getMetricDefinition(), TENANT_ID), subAlarm));
  }

  private static Map<String, AlarmSubExpression> createAlarmSubExpressionMap(Alarm alarm) {
    final Map<String, AlarmSubExpression> oldAlarmSubExpressions = new HashMap<>();
    for (final SubAlarm subAlarm : alarm.getSubAlarms()) {
      oldAlarmSubExpressions.put(subAlarm.getId(), subAlarm.getExpression());
    }
    return oldAlarmSubExpressions;
  }
}
