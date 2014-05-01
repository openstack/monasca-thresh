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
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import backtype.storm.Testing;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.MkTupleParam;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

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
                                                            final AlarmExpression updatedAlarmExpression, List<SubAlarm> updatedSubAlarms) {
        final Alarm updatedAlarm = new Alarm();
        updatedAlarm.setId(alarm.getId());
        updatedAlarm.setTenantId(alarm.getTenantId());
        updatedAlarm.setName(alarm.getName());
        updatedAlarm.setExpression(updatedAlarmExpression.getExpression());
        updatedAlarm.setDescription(alarm.getDescription());
        updatedAlarm.setState(alarm.getState());

        final List<SubAlarm> toDelete = new ArrayList<>(alarm.getSubAlarms());
        final Map<String, AlarmSubExpression> newAlarmSubExpressions = new HashMap<>();
        final Map<String, AlarmSubExpression> updatedSubExpressions = new HashMap<>();
        for (final SubAlarm newSubAlarm : updatedSubAlarms) {
            final SubAlarm oldSubAlarm = alarm.getSubAlarm(newSubAlarm.getId());
            if (oldSubAlarm == null) {
                newAlarmSubExpressions.put(newSubAlarm.getId(), newSubAlarm.getExpression());
            }
            else {
                toDelete.remove(oldSubAlarm);
                if (!newSubAlarm.getExpression().equals(oldSubAlarm.getExpression())) {
                    updatedSubExpressions.put(newSubAlarm.getId(), newSubAlarm.getExpression());
                }
            }
        }
        final Map<String, AlarmSubExpression> deletedSubExpressions = new HashMap<>(toDelete.size());
        for (final SubAlarm oldSubAlarm : toDelete) {
            deletedSubExpressions.put(oldSubAlarm.getId(), oldSubAlarm.getExpression());
        }
        final AlarmUpdatedEvent event = new AlarmUpdatedEvent(updatedAlarm.getTenantId(), updatedAlarm.getId(),
              updatedAlarm.getName(), updatedAlarm.getDescription(), updatedAlarm.getAlarmExpression().getExpression(), alarm.getState(), true, deletedSubExpressions,
              updatedSubExpressions, newAlarmSubExpressions);
        return event;
    }

    public void testAlarmUpdatedEvent() {
        final String updatedExpression = "avg(hpcs.compute.cpu{instance_id=123,device=42}, 1) > 5 " +
                "and max(hpcs.compute.Mem{instance_id=123,device=42}) > 90 " +
                "and max(hpcs.compute.newLoad{instance_id=123,device=42}) > 5";

        final AlarmExpression updatedAlarmExpression = new AlarmExpression(updatedExpression);

        final List<SubAlarm> updatedSubAlarms = new ArrayList<>();
        updatedSubAlarms.add(subAlarms.get(0));
        updatedSubAlarms.add(new SubAlarm(subAlarms.get(1).getId(), alarm.getId(), updatedAlarmExpression.getSubExpressions().get(1)));
        updatedSubAlarms.add(new SubAlarm(UUID.randomUUID().toString(), alarm.getId(), updatedAlarmExpression.getSubExpressions().get(2)));

        final AlarmUpdatedEvent event = createAlarmUpdatedEvent(alarm, updatedAlarmExpression, updatedSubAlarms);

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
