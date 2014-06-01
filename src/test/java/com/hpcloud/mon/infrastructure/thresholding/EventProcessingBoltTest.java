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
package com.hpcloud.mon.infrastructure.thresholding;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import backtype.storm.Testing;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.MkTupleParam;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Sets;
import com.hpcloud.mon.common.event.AlarmCreatedEvent;
import com.hpcloud.mon.common.event.AlarmDeletedEvent;
import com.hpcloud.mon.common.event.AlarmUpdatedEvent;
import com.hpcloud.mon.common.model.alarm.AlarmExpression;
import com.hpcloud.mon.common.model.alarm.AlarmState;
import com.hpcloud.mon.common.model.alarm.AlarmSubExpression;
import com.hpcloud.mon.common.model.metric.MetricDefinition;
import com.hpcloud.mon.domain.model.Alarm;
import com.hpcloud.mon.domain.model.MetricDefinitionAndTenantId;
import com.hpcloud.mon.domain.model.SubAlarm;
import com.hpcloud.streaming.storm.Streams;

@Test
public class EventProcessingBoltTest {

    private static final String TENANT_ID = "AAAAABBBBBBCCCCC";
    private EventProcessingBolt bolt;
    private OutputCollector collector;
    private AlarmExpression alarmExpression;
    private Alarm alarm;
    private List<SubAlarm> subAlarms;

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
        final String expression = "avg(hpcs.compute.cpu{instance_id=123,device=42}, 1) > 5 " +
                "and max(hpcs.compute.mem{instance_id=123,device=42}) > 80 " +
              "and max(hpcs.compute.load{instance_id=123,device=42}) > 5";
        alarmExpression = new AlarmExpression(expression);
        subAlarms = createSubAlarms(alarmId, alarmExpression);
        alarm = new Alarm(alarmId, TENANT_ID, name, description, alarmExpression, subAlarms,
                AlarmState.UNDETERMINED, Boolean.TRUE);
    }

    private List<SubAlarm> createSubAlarms(final String alarmId,
                                    final AlarmExpression alarmExpression,
                                    String ... ids) {
        final List<AlarmSubExpression> subExpressions = alarmExpression.getSubExpressions();
        final List<SubAlarm> subAlarms = new ArrayList<SubAlarm>(subExpressions.size());
        for (int i = 0; i < subExpressions.size(); i++) {
            final String id;
            if (i >= ids.length) {
                id = UUID.randomUUID().toString();
            }
            else {
                id = ids[i];
            }
            final SubAlarm subAlarm = new SubAlarm(id, alarmId, subExpressions.get(i));
            subAlarms.add(subAlarm);
        }
        return subAlarms;
    }

    public void testAlarmCreatedEvent() {
        final Map<String, AlarmSubExpression> expressions = createAlarmSubExpressionMap(alarm);
        final AlarmCreatedEvent event = new AlarmCreatedEvent(alarm.getTenantId(), alarm.getId(),
                alarm.getName(), alarm.getAlarmExpression().getExpression(), expressions);
        final Tuple tuple = createTuple(event);
        bolt.execute(tuple);
        for (final SubAlarm subAlarm : subAlarms) {
            verifyAddedSubAlarm(subAlarm);
        }
        verify(collector, times(1)).ack(tuple);
    }

    private Tuple createTuple(final Object event) {
        MkTupleParam tupleParam = new MkTupleParam();
        tupleParam.setFields("event");
        tupleParam.setStream(Streams.DEFAULT_STREAM_ID);
        final Tuple tuple = Testing.testTuple(Arrays.asList(event), tupleParam);
        return tuple;
    }

    public void testAlarmDeletedEvent() {
        final Map<String, MetricDefinition> metricDefinitions = new HashMap<>();
        for (final SubAlarm subAlarm : alarm.getSubAlarms()) {
            metricDefinitions.put(subAlarm.getId(), subAlarm.getExpression().getMetricDefinition());
        }
        final AlarmDeletedEvent event = new AlarmDeletedEvent(alarm.getTenantId(), alarm.getId(),
                metricDefinitions);
        final Tuple tuple = createTuple(event);
        bolt.execute(tuple);
        for (final SubAlarm subAlarm : subAlarms) {
            verifyDeletedSubAlarm(subAlarm);
        }
        verify(collector, times(1)).emit(EventProcessingBolt.ALARM_EVENT_STREAM_ID,
                new Values(EventProcessingBolt.DELETED, event.alarmId, event));
        verify(collector, times(1)).ack(tuple);
    }

    private void verifyDeletedSubAlarm(final SubAlarm subAlarm) {
        verify(collector, times(1)).emit(EventProcessingBolt.METRIC_ALARM_EVENT_STREAM_ID,
            new Values(EventProcessingBolt.DELETED,
                    new MetricDefinitionAndTenantId(
                            subAlarm.getExpression().getMetricDefinition(), TENANT_ID), subAlarm.getId()));
    }

    public static AlarmUpdatedEvent createAlarmUpdatedEvent(final Alarm alarm,
                                                            final AlarmState newState,
                                                            final AlarmExpression updatedAlarmExpression,
                                                            List<SubAlarm> updatedSubAlarms) {
        final Map<String, AlarmSubExpression> oldAlarmSubExpressions = new HashMap<>();
        for (final SubAlarm subAlarm : alarm.getSubAlarms())
            oldAlarmSubExpressions.put(subAlarm.getId(), subAlarm.getExpression());
        BiMap<String, AlarmSubExpression> oldExpressions = HashBiMap.create(oldAlarmSubExpressions);
        Set<AlarmSubExpression> oldSet = oldExpressions.inverse().keySet();
        Set<AlarmSubExpression> newSet = new HashSet<>();
        for (final SubAlarm subAlarm : updatedSubAlarms)
          newSet.add(subAlarm.getExpression());

        // Identify old or changed expressions
        Set<AlarmSubExpression> oldOrChangedExpressions = new HashSet<>(Sets.difference(oldSet, newSet));

        // Identify new or changed expressions
        Set<AlarmSubExpression> newOrChangedExpressions = new HashSet<>(Sets.difference(newSet, oldSet));

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
        for (AlarmSubExpression expression : newOrChangedExpressions)
          for (final SubAlarm subAlarm : updatedSubAlarms)
              if (subAlarm.getExpression().equals(expression))
                  newExpressions.put(subAlarm.getId(), expression);

        final AlarmUpdatedEvent event = new AlarmUpdatedEvent(alarm.getTenantId(), alarm.getId(),
                alarm.getName(), alarm.getDescription(), updatedAlarmExpression.getExpression(), newState, alarm.getState(),
                true, oldExpressions,
                changedExpressions, unchangedExpressions, newExpressions);
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

    public void testAlarmUpdatedEvent() {
        final String updatedExpression = "avg(hpcs.compute.cpu{instance_id=123,device=42}, 1) > 5 " +
                "and max(hpcs.compute.mem{instance_id=123,device=42}) > 90 " +
                "and max(hpcs.compute.newLoad{instance_id=123,device=42}) > 5";

        final AlarmExpression updatedAlarmExpression = new AlarmExpression(updatedExpression);

        final List<SubAlarm> updatedSubAlarms = new ArrayList<>();
        updatedSubAlarms.add(subAlarms.get(0));
        updatedSubAlarms.add(new SubAlarm(subAlarms.get(1).getId(), alarm.getId(), updatedAlarmExpression.getSubExpressions().get(1)));
        updatedSubAlarms.add(new SubAlarm(UUID.randomUUID().toString(), alarm.getId(), updatedAlarmExpression.getSubExpressions().get(2)));

        final AlarmUpdatedEvent event = createAlarmUpdatedEvent(alarm, alarm.getState(), updatedAlarmExpression,
                                                                updatedSubAlarms);

        final Tuple tuple = createTuple(event);
        bolt.execute(tuple);
        verify(collector, times(1)).ack(tuple);

        verifyDeletedSubAlarm(subAlarms.get(2));
        verifyUpdatedSubAlarm(updatedSubAlarms.get(1));
        verifyAddedSubAlarm(updatedSubAlarms.get(2));
        verify(collector, times(1)).emit(EventProcessingBolt.ALARM_EVENT_STREAM_ID,
                new Values(EventProcessingBolt.UPDATED, event.alarmId, event));
    }

    private void verifyAddedSubAlarm(final SubAlarm subAlarm) {
        sendSubAlarm(subAlarm, EventProcessingBolt.CREATED);
    }

    private void verifyUpdatedSubAlarm(final SubAlarm subAlarm) {
        sendSubAlarm(subAlarm, EventProcessingBolt.UPDATED);
    }

    private void sendSubAlarm(final SubAlarm subAlarm, String eventType) {
        verify(collector, times(1)).emit(EventProcessingBolt.METRIC_SUB_ALARM_EVENT_STREAM_ID,
            new Values(eventType,
                    new MetricDefinitionAndTenantId(
                            subAlarm.getExpression().getMetricDefinition(), TENANT_ID), subAlarm));
    }

   private static Map<String, AlarmSubExpression> createAlarmSubExpressionMap(
         Alarm alarm) {
      final Map<String, AlarmSubExpression> oldAlarmSubExpressions = new HashMap<>();
        for (final SubAlarm subAlarm : alarm.getSubAlarms()) {
            oldAlarmSubExpressions.put(subAlarm.getId(), subAlarm.getExpression());
        }
      return oldAlarmSubExpressions;
   }
}
