package com.hpcloud.mon.infrastructure.thresholding;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import com.hpcloud.mon.domain.model.SubAlarm;
import com.hpcloud.mon.domain.service.MetricDefinitionDAO;
import com.hpcloud.streaming.storm.Streams;

@Test
public class MetricFilteringBoltTest {
    private OutputCollector collector;
    private AlarmExpression alarmExpression;
    private List<SubAlarm> subAlarms;
    private MetricFilteringBolt bolt;

    @BeforeMethod
    protected void beforeMethod() {
        collector = mock(OutputCollector.class);

        final String alarmId = "111111112222222222233333333334";
        final String expression = "avg(hpcs.compute.cpu{instance_id=123,device=42}, 1) > 5 " +
                "and max(hpcs.compute.mem{instance_id=123,device=42}) > 80 " +
              "and max(hpcs.compute.load{instance_id=123,device=42}) > 5";
        alarmExpression = new AlarmExpression(expression);
        subAlarms = createSubAlarms(alarmId, alarmExpression);
    }

    private void createBolt(List<MetricDefinition> initialMetricDefinitions) {
        final MetricDefinitionDAO dao = mock(MetricDefinitionDAO.class);
        when(dao.findForAlarms()).thenReturn(initialMetricDefinitions);
        bolt = new MetricFilteringBolt(dao);
        bolt.clearMetricDefinitions();

        final Map<String, String> config = new HashMap<>();
        final TopologyContext context = mock(TopologyContext.class);
        bolt.prepare(config, context, collector);

        // Validate the prepare emits the initial Metric Definitions
        for (final MetricDefinition metricDefinition : initialMetricDefinitions) {
            verify(collector, times(1)).emit(new Values(metricDefinition, null));
        }
    }

    private List<SubAlarm> createSubAlarms(final String alarmId,
                                    final AlarmExpression alarmExpression,
                                    String ... ids) {
        final List<AlarmSubExpression> subExpressions = alarmExpression.getSubExpressions();
        final List<SubAlarm> subAlarms = new ArrayList<SubAlarm>(subExpressions.size());
        int subAlarmId = 4242;
        for (int i = 0; i < subExpressions.size(); i++) {
            final String id;
            if (i >= ids.length) {
                id = String.valueOf(subAlarmId++);
            }
            else {
                id = ids[i];
            }
            final SubAlarm subAlarm = new SubAlarm(id, alarmId, subExpressions.get(i));
            subAlarms.add(subAlarm);
        }
        return subAlarms;
    }

    public void testAddFilter() {
        createBolt(new ArrayList<MetricDefinition>(0));

        // First ensure metrics don't pass the filter
        for (final SubAlarm subAlarm : subAlarms) {
            final Tuple tuple = createMetricTuple(subAlarm);
            bolt.execute(tuple);
            verify(collector, times(1)).ack(tuple);
            verify(collector, never()).emit(tuple, tuple.getValues());
        }
        for (final SubAlarm subAlarm : subAlarms) {
            final Tuple tuple = createMetricDefinitionTuple(subAlarm);
            bolt.execute(tuple);
            verify(collector, times(1)).ack(tuple);
        }
        // Now ensure metrics pass the filter
        for (final SubAlarm subAlarm : subAlarms) {
            final Tuple tuple = createMetricTuple(subAlarm);
            bolt.execute(tuple);
            verify(collector, times(1)).ack(tuple);
            verify(collector, times(1)).emit(tuple, tuple.getValues());
        }
    }

    public void testDeleteFilter() {
        final List<MetricDefinition> initialMetricDefinitions = new ArrayList<MetricDefinition>(subAlarms.size());
        for (final SubAlarm subAlarm : subAlarms) {
            initialMetricDefinitions.add(subAlarm.getExpression().getMetricDefinition());
        }
        createBolt(initialMetricDefinitions);

        // First ensure metrics pass the filter
        for (final SubAlarm subAlarm : subAlarms) {
            final Tuple tuple = createMetricTuple(subAlarm);
            bolt.execute(tuple);
            verify(collector, times(1)).ack(tuple);
            verify(collector, times(1)).emit(tuple, tuple.getValues());
        }
        for (final SubAlarm subAlarm : subAlarms) {
            final Tuple tuple = createMetricDefinitionDeletionTuple(subAlarm);
            bolt.execute(tuple);
            verify(collector, times(1)).ack(tuple);
        }
        // Now ensure metrics don't pass the filter
        for (final SubAlarm subAlarm : subAlarms) {
            final Tuple tuple = createMetricTuple(subAlarm);
            bolt.execute(tuple);
            verify(collector, times(1)).ack(tuple);
            verify(collector, never()).emit(tuple, tuple.getValues());
        }
    }

    private Tuple createMetricDefinitionTuple(final SubAlarm subAlarm) {
        final MkTupleParam tupleParam = new MkTupleParam();
        tupleParam.setFields("eventType", "metricDefinition", "subAlarm");
        tupleParam.setStream(EventProcessingBolt.METRIC_SUB_ALARM_EVENT_STREAM_ID);
        final Tuple tuple = Testing.testTuple(Arrays.asList(EventProcessingBolt.CREATED,
                subAlarm.getExpression().getMetricDefinition(), subAlarm), tupleParam);
        return tuple;
    }

    private Tuple createMetricDefinitionDeletionTuple(final SubAlarm subAlarm) {
        final MkTupleParam tupleParam = new MkTupleParam();
        tupleParam.setFields("eventType", "metricDefinition", "subAlarm");
        tupleParam.setStream(EventProcessingBolt.METRIC_ALARM_EVENT_STREAM_ID);
        final Tuple tuple = Testing.testTuple(Arrays.asList(EventProcessingBolt.DELETED,
                subAlarm.getExpression().getMetricDefinition(), subAlarm.getId()), tupleParam);

        return tuple;
    }

    private Tuple createMetricTuple(final SubAlarm subAlarm) {
        final MkTupleParam tupleParam = new MkTupleParam();
        tupleParam.setFields("metricDefinition", "metric");
        tupleParam.setStream(Streams.DEFAULT_STREAM_ID);
        MetricDefinition metricDefinition = subAlarm.getExpression().getMetricDefinition();
        final Metric metric = new Metric(metricDefinition, System.currentTimeMillis()/1000, 42.0);
        final Tuple tuple = Testing.testTuple(Arrays.asList(metricDefinition, metric), tupleParam);
        return tuple;
    }
}
