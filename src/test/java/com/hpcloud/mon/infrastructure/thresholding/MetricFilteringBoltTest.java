package com.hpcloud.mon.infrastructure.thresholding;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.mockito.verification.VerificationMode;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import backtype.storm.Testing;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.MkTupleParam;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.hpcloud.mon.common.model.alarm.AlarmExpression;
import com.hpcloud.mon.common.model.alarm.AlarmSubExpression;
import com.hpcloud.mon.common.model.metric.Metric;
import com.hpcloud.mon.common.model.metric.MetricDefinition;
import com.hpcloud.mon.domain.model.MetricDefinitionAndTenantId;
import com.hpcloud.mon.domain.model.SubAlarm;
import com.hpcloud.mon.domain.service.MetricDefinitionDAO;
import com.hpcloud.mon.domain.service.SubAlarmMetricDefinition;
import com.hpcloud.streaming.storm.Streams;

@Test
public class MetricFilteringBoltTest {
    private List<SubAlarm> subAlarms;
    private List<SubAlarm> duplicateMetricSubAlarms;
    private final static String TEST_TENANT_ID = "42";

    @BeforeMethod
    protected void beforeMethod() {

        final String expression = "avg(hpcs.compute.cpu{instance_id=123,device=42}, 1) > 5 " +
                "and max(hpcs.compute.mem{instance_id=123,device=42}) > 80 " +
              "and max(hpcs.compute.load{instance_id=123,device=42}) > 5";
        subAlarms = createSubAlarmsForAlarm("111111112222222222233333333334", expression);

        duplicateMetricSubAlarms = createSubAlarmsForAlarm(UUID.randomUUID().toString(),
                "max(hpcs.compute.load{instance_id=123,device=42}) > 8");
        subAlarms.addAll(duplicateMetricSubAlarms);
    }

    private List<SubAlarm> createSubAlarmsForAlarm(final String alarmId,
            final String expression) {
        final AlarmExpression alarmExpression = new AlarmExpression(expression);
        final List<AlarmSubExpression> subExpressions = alarmExpression.getSubExpressions();
        final List<SubAlarm> result = new ArrayList<SubAlarm>(subExpressions.size());
        for (int i = 0; i < subExpressions.size(); i++) {
            final SubAlarm subAlarm = new SubAlarm(UUID.randomUUID().toString(), alarmId, subExpressions.get(i));
            result.add(subAlarm);
        }
        return result;
    }

    private MetricFilteringBolt createBolt(List<SubAlarmMetricDefinition> initialMetricDefinitions,
                                           final OutputCollector collector, boolean willEmit) {
        final MetricDefinitionDAO dao = mock(MetricDefinitionDAO.class);
        when(dao.findForAlarms()).thenReturn(initialMetricDefinitions);
        MetricFilteringBolt bolt = new MetricFilteringBolt(dao);

        final Map<String, String> config = new HashMap<>();
        final TopologyContext context = mock(TopologyContext.class);
        bolt.prepare(config, context, collector);

        if (willEmit) {
            // Validate the prepare emits the initial Metric Definitions
            for (final SubAlarmMetricDefinition metricDefinition : initialMetricDefinitions) {
                verify(collector, times(1)).emit(new Values(metricDefinition.getMetricDefinitionAndTenantId(), null));
            }
        }
        return bolt;
    }

    public void testNoInitial() {
        MetricFilteringBolt.clearMetricDefinitions();
        final OutputCollector collector1 = mock(OutputCollector.class);

        final MetricFilteringBolt bolt1 = createBolt(new ArrayList<SubAlarmMetricDefinition>(0), collector1, true);

        final OutputCollector collector2 = mock(OutputCollector.class);

        final MetricFilteringBolt bolt2 = createBolt(new ArrayList<SubAlarmMetricDefinition>(0), collector2, false);

        // First ensure metrics don't pass the filter
        verifyMetricFiltered(collector1, bolt1);
        verifyMetricFiltered(collector2, bolt2);

        sendMetricCreation(collector1, bolt1);
        sendMetricCreation(collector2, bolt2);

        testDeleteSubAlarms(bolt1, collector1, bolt2, collector2);
    }

    private void sendMetricCreation(final OutputCollector collector1,
            final MetricFilteringBolt bolt1) {
        for (final SubAlarm subAlarm : subAlarms) {
            final Tuple tuple = createMetricDefinitionTuple(subAlarm);
            bolt1.execute(tuple);
            verify(collector1, times(1)).ack(tuple);
        }
    }

    private void verifyMetricFiltered(final OutputCollector collector1,
            final MetricFilteringBolt bolt1) {
        sendMetricsAndVerify(collector1, bolt1, never());
    }

    private void verifyMetricPassed(final OutputCollector collector1,
            final MetricFilteringBolt bolt1) {
        sendMetricsAndVerify(collector1, bolt1, times(1));
    }

    private void sendMetricsAndVerify(final OutputCollector collector1,
            final MetricFilteringBolt bolt1, VerificationMode howMany) {
        for (final SubAlarm subAlarm : subAlarms) {
            // First do a MetricDefinition that is an exact match
            final MetricDefinition metricDefinition = subAlarm.getExpression().getMetricDefinition();
            final Tuple exactTuple = createMetricTuple(metricDefinition, new Metric(metricDefinition, System.currentTimeMillis()/1000, 42.0));
            bolt1.execute(exactTuple);
            verify(collector1, times(1)).ack(exactTuple);
            verify(collector1, howMany).emit(exactTuple, exactTuple.getValues());

            // Now do a MetricDefinition with an extra dimension that should still match the SubAlarm
            final Map<String, String> extraDimensions = new HashMap<>(metricDefinition.dimensions);
            extraDimensions.put("group", "group_a");
            final MetricDefinition inexactMetricDef = new MetricDefinition(metricDefinition.name, extraDimensions);
            Metric inexactMetric = new Metric(inexactMetricDef, System.currentTimeMillis()/1000, 42.0);
            final Tuple inexactTuple = createMetricTuple(metricDefinition, inexactMetric);
            bolt1.execute(inexactTuple);
            verify(collector1, times(1)).ack(inexactTuple);
            // We want the MetricDefinitionAndTenantId from the exact tuple, but the inexactMetric
            verify(collector1, howMany).emit(inexactTuple, new Values(exactTuple.getValue(0), inexactMetric));
        }
    }

    public void testAllInitial() {
        MetricFilteringBolt.clearMetricDefinitions();
        final List<SubAlarmMetricDefinition> initialMetricDefinitions = new ArrayList<>(subAlarms.size());
        for (final SubAlarm subAlarm : subAlarms) {
            initialMetricDefinitions.add(new SubAlarmMetricDefinition(subAlarm.getId(),
                    new MetricDefinitionAndTenantId(subAlarm.getExpression().getMetricDefinition(), TEST_TENANT_ID)));
        }
        final OutputCollector collector1 = mock(OutputCollector.class);

        final MetricFilteringBolt bolt1 = createBolt(initialMetricDefinitions, collector1, true);

        final OutputCollector collector2 = mock(OutputCollector.class);

        final MetricFilteringBolt bolt2 = createBolt(initialMetricDefinitions, collector2, false);

        testDeleteSubAlarms(bolt1, collector1, bolt2, collector2);
    }

    private void testDeleteSubAlarms(MetricFilteringBolt bolt1, OutputCollector collector1, MetricFilteringBolt bolt2, OutputCollector collector2) {
        // Now ensure metrics pass the filter
        verifyMetricPassed(collector1, bolt1);
        verifyMetricPassed(collector2, bolt2);

        // Now delete the SubAlarm that duplicated a MetricDefinition
        deleteSubAlarms(bolt1, collector1, duplicateMetricSubAlarms);
        deleteSubAlarms(bolt2, collector2, duplicateMetricSubAlarms);

        // Ensure metrics still pass the filter
        verifyMetricPassed(collector1, bolt1);
        verifyMetricPassed(collector2, bolt2);

        deleteSubAlarms(bolt1, collector1, subAlarms);
        // All MetricDefinitions should be deleted
        assertEquals(MetricFilteringBolt.sizeMetricDefinitions(), 0);
        deleteSubAlarms(bolt2, collector2, subAlarms);

        verifyMetricFiltered(collector1, bolt1);
        verifyMetricFiltered(collector2, bolt2);
    }

    private void deleteSubAlarms(MetricFilteringBolt bolt, OutputCollector collector, final List<SubAlarm> otherSubAlarms) {
        for (final SubAlarm subAlarm : otherSubAlarms) {
            final Tuple tuple = createMetricDefinitionDeletionTuple(subAlarm);
            bolt.execute(tuple);
            verify(collector, times(1)).ack(tuple);
        }
    }

    private Tuple createMetricDefinitionTuple(final SubAlarm subAlarm) {
        final MkTupleParam tupleParam = new MkTupleParam();
        tupleParam.setFields(EventProcessingBolt.METRIC_SUB_ALARM_EVENT_STREAM_FIELDS);
        tupleParam.setStream(EventProcessingBolt.METRIC_SUB_ALARM_EVENT_STREAM_ID);
        final Tuple tuple = Testing.testTuple(Arrays.asList(EventProcessingBolt.CREATED,
                new MetricDefinitionAndTenantId(
                        subAlarm.getExpression().getMetricDefinition(), TEST_TENANT_ID),
                subAlarm), tupleParam);
        return tuple;
    }

    private Tuple createMetricDefinitionDeletionTuple(final SubAlarm subAlarm) {
        final MkTupleParam tupleParam = new MkTupleParam();
        tupleParam.setFields(EventProcessingBolt.METRIC_ALARM_EVENT_STREAM_FIELDS);
        tupleParam.setStream(EventProcessingBolt.METRIC_ALARM_EVENT_STREAM_ID);
        final Tuple tuple = Testing.testTuple(Arrays.asList(EventProcessingBolt.DELETED,
                new MetricDefinitionAndTenantId(
                        subAlarm.getExpression().getMetricDefinition(), TEST_TENANT_ID),
                subAlarm.getId()), tupleParam);

        return tuple;
    }

    private Tuple createMetricTuple(final MetricDefinition metricDefinition,
            final Metric metric) {
        final MkTupleParam tupleParam = new MkTupleParam();
        tupleParam.setFields(MetricFilteringBolt.FIELDS);
        tupleParam.setStream(Streams.DEFAULT_STREAM_ID);        final Tuple tuple = Testing.testTuple(Arrays.asList(
                new MetricDefinitionAndTenantId(metricDefinition, TEST_TENANT_ID), metric), tupleParam);
        return tuple;
    }
}
