package com.hpcloud.mon.infrastructure.thresholding;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
                new Values(EventProcessingBolt.DELETED, event.alarmId));
        verify(collector, times(1)).ack(tuple);
    }

    private void verifyDeletedSubAlarm(final SubAlarm subAlarm) {
        verify(collector, times(1)).emit(EventProcessingBolt.METRIC_ALARM_EVENT_STREAM_ID,
            new Values(EventProcessingBolt.DELETED,
                    new MetricDefinitionAndTenantId(
                            subAlarm.getExpression().getMetricDefinition(), TENANT_ID), subAlarm.getId()));
    }

    public static AlarmUpdatedEvent createAlarmUpdatedEvent(final Alarm alarm,
                                                            final AlarmExpression updatedAlarmExpression) {
        final Alarm updatedAlarm = new Alarm();
        updatedAlarm.setId(alarm.getId());
        updatedAlarm.setTenantId(alarm.getTenantId());
        updatedAlarm.setName(alarm.getName());
        updatedAlarm.setExpression(updatedAlarmExpression.getExpression());
        Entry<Map<String, AlarmSubExpression>, Map<String, AlarmSubExpression>> entry =
                oldAndNewSubExpressionsFor(createAlarmSubExpressionMap(alarm), updatedAlarmExpression);

        final Map<String, AlarmSubExpression> newAlarmSubExpressions = entry.getValue();
        Map<String, AlarmSubExpression> changedSubExpressions = new HashMap<>();
        final AlarmUpdatedEvent event = new AlarmUpdatedEvent(updatedAlarm.getTenantId(), updatedAlarm.getId(),
              updatedAlarm.getName(), updatedAlarm.getAlarmExpression().getExpression(), alarm.getState(), true, entry.getKey(),
              changedSubExpressions, newAlarmSubExpressions);
        return event;
    }

    public void testAlarmUpdatedEvent() {
        final String updatedExpression = "avg(hpcs.compute.cpu{instance_id=123,device=42}, 1) > 5 " +
                "and max(hpcs.compute.newMem{instance_id=123,device=42}) > 80 " +
                "and max(hpcs.compute.newLoad{instance_id=123,device=42}) > 5";
        final AlarmExpression updatedAlarmExpression = new AlarmExpression(updatedExpression);
        final AlarmUpdatedEvent event = createAlarmUpdatedEvent(alarm, updatedAlarmExpression);
        final Tuple tuple = createTuple(event);
        bolt.execute(tuple);
        verify(collector, times(1)).emit(EventProcessingBolt.ALARM_EVENT_STREAM_ID,
                new Values(EventProcessingBolt.UPDATED, event.alarmId));

        final List<SubAlarm> updatedSubAlarms = new ArrayList<>();
        final Map<String, AlarmSubExpression> oldAlarmSubExpressionMap = createAlarmSubExpressionMap(alarm);
        for (final AlarmSubExpression alarmExpression : updatedAlarmExpression.getSubExpressions()) {
            String id = find(oldAlarmSubExpressionMap, alarmExpression);
            if (id == null) {
                id = find(event.newAlarmSubExpressions, alarmExpression);
            }
            final SubAlarm subAlarm = new SubAlarm(id, alarm.getId(), alarmExpression);
            updatedSubAlarms.add(subAlarm);
        }

        verifyDeletedSubAlarm(subAlarms.get(1));
        verifyDeletedSubAlarm(subAlarms.get(2));
        verifyAddedSubAlarm(updatedSubAlarms.get(1));
        verifyAddedSubAlarm(updatedSubAlarms.get(2));
        verify(collector, times(1)).ack(tuple);
    }

    private String find(
            final Map<String, AlarmSubExpression> newAlarmSubExpressions,
            final AlarmSubExpression alarmExpression) {
        String id = null;
        for (Entry<String, AlarmSubExpression> entry2 : newAlarmSubExpressions.entrySet()) {
            if (entry2.getValue().equals(alarmExpression)) {
                id = entry2.getKey();
                break;
            }
        }
        return id;
    }

    private static Entry<Map<String, AlarmSubExpression>, Map<String, AlarmSubExpression>> oldAndNewSubExpressionsFor(
            Map<String, AlarmSubExpression> oldSubAlarms,
            final AlarmExpression alarmExpression) {
        Set<AlarmSubExpression> oldSet = new HashSet<>(oldSubAlarms.values());
        Set<AlarmSubExpression> newSet = new HashSet<>(alarmExpression.getSubExpressions());

        // Filter old sub expressions
        Set<AlarmSubExpression> oldExpressions = Sets.difference(oldSet, newSet);
        oldSubAlarms.values().retainAll(oldExpressions);

        // Identify new sub expressions
        Map<String, AlarmSubExpression> newSubAlarms = new HashMap<>();
        Set<AlarmSubExpression> newExpressions = Sets.difference(newSet, oldSet);
        for (AlarmSubExpression expression : newExpressions)
            newSubAlarms.put(UUID.randomUUID().toString(), expression);

        return new SimpleEntry<>(oldSubAlarms, newSubAlarms);
    }

    private void verifyAddedSubAlarm(final SubAlarm subAlarm) {
        verify(collector, times(1)).emit(EventProcessingBolt.METRIC_SUB_ALARM_EVENT_STREAM_ID,
            new Values(EventProcessingBolt.CREATED,
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
