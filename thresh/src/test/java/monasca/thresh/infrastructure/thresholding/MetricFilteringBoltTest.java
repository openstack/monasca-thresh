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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.hpcloud.mon.common.event.AlarmDefinitionCreatedEvent;
import com.hpcloud.mon.common.model.alarm.AlarmExpression;
import com.hpcloud.mon.common.model.alarm.AlarmState;
import com.hpcloud.mon.common.model.alarm.AlarmSubExpression;
import com.hpcloud.mon.common.model.metric.Metric;
import com.hpcloud.mon.common.model.metric.MetricDefinition;
import com.hpcloud.streaming.storm.Streams;

import backtype.storm.Testing;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.MkTupleParam;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import monasca.thresh.domain.model.Alarm;
import monasca.thresh.domain.model.AlarmDefinition;
import monasca.thresh.domain.model.MetricDefinitionAndTenantId;
import monasca.thresh.domain.model.SubAlarm;
import monasca.thresh.domain.model.TenantIdAndMetricName;
import monasca.thresh.domain.service.AlarmDAO;
import monasca.thresh.domain.service.AlarmDefinitionDAO;

import org.mockito.verification.VerificationMode;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Test
public class MetricFilteringBoltTest {
  private final static String TEST_TENANT_ID = "42";
  private long metricTimestamp = System.currentTimeMillis() / 1000; // Make sure the metric

  private AlarmDefinition alarmDef1;
  private AlarmDefinition dupMetricAlarmDef;

  @BeforeMethod
  protected void beforeMethod() {

    final String expression =
        "avg(hpcs.compute.cpu{instance_id=123,device=42}, 1) > 5 "
            + "and max(hpcs.compute.mem{instance_id=123,device=42}) > 80 "
            + "and max(hpcs.compute.load{instance_id=123,device=42}) > 5";
    alarmDef1 = createAlarmDefinition(expression, "Alarm Def 1");

    String dupMetricExpression = "max(hpcs.compute.load{instance_id=123,device=42}) > 8";
    dupMetricAlarmDef = createAlarmDefinition(dupMetricExpression, "Dup Metric Alarm Def");
  }

  private AlarmDefinition createAlarmDefinition(final String expression, String name) {
    final AlarmExpression alarm1Expression = new AlarmExpression(expression);
    final String alarmId = getNextId();
    return new AlarmDefinition(alarmId, TEST_TENANT_ID, name, "Alarm1 Description", alarm1Expression, true,
        Arrays.asList("hostname"));
  }

  private String getNextId() {
    return UUID.randomUUID().toString();
  }

  private MockMetricFilteringBolt createBolt(List<AlarmDefinition> initialAlarmDefinitions,
      final List<Alarm> initialAlarms, final OutputCollector collector, final boolean willEmit) {
    final AlarmDefinitionDAO alarmDefDao = mock(AlarmDefinitionDAO.class);
    when(alarmDefDao.listAll()).thenReturn(initialAlarmDefinitions);
    final AlarmDAO alarmDao = mock(AlarmDAO.class);
    when(alarmDao.listAll()).thenReturn(initialAlarms);
    MockMetricFilteringBolt bolt = new MockMetricFilteringBolt(alarmDefDao, alarmDao);

    final Map<String, String> config = new HashMap<>();
    final TopologyContext context = mock(TopologyContext.class);
    bolt.prepare(config, context, collector);

    if (willEmit) {
      final Map<String, AlarmDefinition> alarmDefinitions = new HashMap<>();
      for (final AlarmDefinition alarmDefinition : initialAlarmDefinitions) {
        alarmDefinitions.put(alarmDefinition.getId(), alarmDefinition);
      }

      // Validate the prepare emits the initial Metric Definitions
      for (final Alarm alarm : initialAlarms) {
        final AlarmDefinition alarmDefinition = alarmDefinitions.get(alarm.getAlarmDefinitionId());
        for (final SubAlarm subAlarm : alarm.getSubAlarms()) {
          final MetricDefinitionAndTenantId mtid =
              new MetricDefinitionAndTenantId(subAlarm.getExpression().getMetricDefinition(),
                  alarmDefinition.getTenantId());
          final TenantIdAndMetricName timn = new TenantIdAndMetricName(mtid);
          verify(collector, times(1))
              .emit(
                  AlarmCreationBolt.ALARM_CREATION_STREAM,
                  new Values(EventProcessingBolt.CREATED, timn, mtid, alarmDefinition.getId(),
                      subAlarm));
        }
      }
    }
    return bolt;
  }

  public void testLagging() {
    final OutputCollector collector = mock(OutputCollector.class);

    final MockMetricFilteringBolt bolt =
        createBolt(new ArrayList<AlarmDefinition>(0), new ArrayList<Alarm>(0), collector, true);

    final long prepareTime = bolt.getCurrentTime();
    final MetricDefinition metricDefinition =
        alarmDef1.getAlarmExpression().getSubExpressions().get(0).getMetricDefinition();
    final long oldestTimestamp = prepareTime - MetricFilteringBolt.LAG_MESSAGE_PERIOD_DEFAULT;
    final Tuple lateMetricTuple =
        createMetricTuple(metricDefinition, oldestTimestamp, new Metric(metricDefinition,
            oldestTimestamp, 42.0));
    bolt.execute(lateMetricTuple);
    verify(collector, times(1)).ack(lateMetricTuple);
    bolt.setCurrentTime(prepareTime + MetricFilteringBolt.LAG_MESSAGE_PERIOD_DEFAULT);
    final Tuple lateMetricTuple2 =
        createMetricTuple(metricDefinition, prepareTime, new Metric(metricDefinition, prepareTime,
            42.0));
    bolt.execute(lateMetricTuple2);
    verify(collector, times(1)).ack(lateMetricTuple2);
    verify(collector, times(1)).emit(MetricAggregationBolt.METRIC_AGGREGATION_CONTROL_STREAM,
        new Values(MetricAggregationBolt.METRICS_BEHIND));
    bolt.setCurrentTime(prepareTime + 2 * MetricFilteringBolt.LAG_MESSAGE_PERIOD_DEFAULT);
    long caughtUpTimestamp = bolt.getCurrentTime() - MetricFilteringBolt.MIN_LAG_VALUE_DEFAULT;
    final Tuple metricTuple =
        createMetricTuple(metricDefinition, caughtUpTimestamp, new Metric(metricDefinition,
            caughtUpTimestamp, 42.0));
    bolt.execute(metricTuple);
    // Metrics are caught up so there should not be another METRICS_BEHIND message
    verify(collector, times(1)).ack(metricTuple);
    verify(collector, times(1)).emit(MetricAggregationBolt.METRIC_AGGREGATION_CONTROL_STREAM,
        new Values(MetricAggregationBolt.METRICS_BEHIND));
  }

  public void testLaggingTooLong() {
    final OutputCollector collector = mock(OutputCollector.class);

    final MockMetricFilteringBolt bolt =
        createBolt(new ArrayList<AlarmDefinition>(0), new ArrayList<Alarm>(0), collector, true);

    long prepareTime = bolt.getCurrentTime();
    final MetricDefinition metricDefinition =
        alarmDef1.getAlarmExpression().getSubExpressions().get(0).getMetricDefinition();
    // Fake sending metrics for MetricFilteringBolt.MAX_LAG_MESSAGES_DEFAULT *
    // MetricFilteringBolt.LAG_MESSAGE_PERIOD_DEFAULT seconds
    boolean first = true;
    // Need to send MetricFilteringBolt.MAX_LAG_MESSAGES_DEFAULT + 1 metrics because the lag message
    // is not
    // output on the first one.
    for (int i = 0; i < MetricFilteringBolt.MAX_LAG_MESSAGES_DEFAULT + 1; i++) {
      final Tuple lateMetricTuple =
          createMetricTuple(metricDefinition, prepareTime, new Metric(metricDefinition,
              prepareTime, 42.0));
      bolt.setCurrentTime(prepareTime + MetricFilteringBolt.LAG_MESSAGE_PERIOD_DEFAULT);
      bolt.execute(lateMetricTuple);
      verify(collector, times(1)).ack(lateMetricTuple);
      if (!first) {
        verify(collector, times(i)).emit(MetricAggregationBolt.METRIC_AGGREGATION_CONTROL_STREAM,
            new Values(MetricAggregationBolt.METRICS_BEHIND));
      }
      first = false;
      prepareTime = bolt.getCurrentTime();
    }
    // One more
    long timestamp = bolt.getCurrentTime() - MetricFilteringBolt.LAG_MESSAGE_PERIOD_DEFAULT;
    final Tuple metricTuple =
        createMetricTuple(metricDefinition, timestamp,
            new Metric(metricDefinition, timestamp, 42.0));
    bolt.execute(metricTuple);
    verify(collector, times(1)).ack(metricTuple);
    // Won't be any more of these
    verify(collector, times(MetricFilteringBolt.MAX_LAG_MESSAGES_DEFAULT)).emit(
        MetricAggregationBolt.METRIC_AGGREGATION_CONTROL_STREAM,
        new Values(MetricAggregationBolt.METRICS_BEHIND));
  }

  private static class MockMetricFilteringBolt extends MetricFilteringBolt {
    private static final long serialVersionUID = 1L;
    private long currentTimeMillis = System.currentTimeMillis();

    public MockMetricFilteringBolt(AlarmDefinitionDAO alarmDefDAO, AlarmDAO alarmDAO) {
      super(alarmDefDAO, alarmDAO);
    }

    @Override
    protected long getCurrentTime() {
      return currentTimeMillis;
    }

    public void setCurrentTime(final long currentTimeMillis) {
      this.currentTimeMillis = currentTimeMillis;
    }
  }

  public void testNoInitial() {
    MetricFilteringBolt.clearMetricDefinitions();
    final OutputCollector collector1 = mock(OutputCollector.class);

    final MetricFilteringBolt bolt1 =
        createBolt(new ArrayList<AlarmDefinition>(0), new ArrayList<Alarm>(0), collector1, true);

    final OutputCollector collector2 = mock(OutputCollector.class);

    final MetricFilteringBolt bolt2 =
        createBolt(new ArrayList<AlarmDefinition>(0), new ArrayList<Alarm>(0), collector2, false);

    // First ensure metrics don't pass the filter
    verifyMetricFiltered(collector1, bolt1);
    verifyMetricFiltered(collector2, bolt2);

    sendAlarmDefinitionCreation(collector1, bolt1);
    sendAlarmDefinitionCreation(collector2, bolt2);

    // Now ensure metrics pass the filter
    verifyMetricPassed(collector1, bolt1);
    verifyNewMetricDefinitionMessagesSent(collector1, bolt1);
    verifyMetricPassed(collector2, bolt2);
    verifyNoNewMetricDefinitionMessagesSent(collector2, bolt2);

    testDeleteSubAlarms(bolt1, collector1, bolt2, collector2);
  }

  private void sendAlarmDefinitionCreation(final OutputCollector collector1, final MetricFilteringBolt bolt1) {
    for (final AlarmDefinition alarmDef : Arrays.asList(alarmDef1, dupMetricAlarmDef)) {
      final Tuple tuple = createAlarmDefinitionTuple(alarmDef);
      bolt1.execute(tuple);
      verify(collector1, times(1)).ack(tuple);
    }
  }

  private void verifyMetricFiltered(final OutputCollector collector1,
      final MetricFilteringBolt bolt1) {
    sendMetricsAndVerify(collector1, bolt1, never());
  }

  private void verifyMetricPassed(final OutputCollector collector1, final MetricFilteringBolt bolt1) {
    sendMetricsAndVerify(collector1, bolt1, times(1));
  }

  private List<AlarmSubExpression> getAllSubAlarms() {
    final List<AlarmSubExpression> result = new ArrayList<>(alarmDef1.getAlarmExpression().getSubExpressions());
    result.addAll(dupMetricAlarmDef.getAlarmExpression().getSubExpressions());
    return result;
  }

  private void sendMetricsAndVerify(final OutputCollector collector1,
      final MetricFilteringBolt bolt1, VerificationMode howMany) {
    for (final AlarmSubExpression subExpr : getAllSubAlarms()) {
      // First do a MetricDefinition that is an exact match
      final MetricDefinition metricDefinition = subExpr.getMetricDefinition();
      final Tuple exactTuple =
          createMetricTuple(metricDefinition, metricTimestamp++, new Metric(metricDefinition,
              metricTimestamp, 42.0));
      bolt1.execute(exactTuple);
      verify(collector1, times(1)).ack(exactTuple);
      verify(collector1, howMany).emit(new Values(exactTuple.getValue(0), exactTuple.getValue(2)));

      // Now do a MetricDefinition with an extra dimension that should still match the SubExpression
      final MetricDefinition inexactMetricDef = addExtraDimension(metricDefinition);
      final Metric inexactMetric = new Metric(inexactMetricDef, metricTimestamp, 42.0);
      final Tuple inexactTuple =
          createMetricTuple(metricDefinition, metricTimestamp++, inexactMetric);
      bolt1.execute(inexactTuple);
      verify(collector1, times(1)).ack(inexactTuple);
      verify(collector1, howMany).emit(new Values(exactTuple.getValue(0), inexactMetric));
    }
  }

  private MetricDefinition addExtraDimension(final MetricDefinition metricDefinition) {
    final Map<String, String> extraDimensions = new HashMap<>(metricDefinition.dimensions);
    extraDimensions.put("group", "group_a");
    final MetricDefinition inexactMetricDef =
        new MetricDefinition(metricDefinition.name, extraDimensions);
    return inexactMetricDef;
  }

  private void verifyNewMetricDefinitionMessagesSent(final OutputCollector collector,
      final MetricFilteringBolt bolt) {
    verifyNewMetricDefinitionMessages(collector, bolt, times(1));
  }

  private void verifyNoNewMetricDefinitionMessagesSent(final OutputCollector collector,
      final MetricFilteringBolt bolt) {
    verifyNewMetricDefinitionMessages(collector, bolt, never());
  }

  private void verifyNewMetricDefinitionMessages(final OutputCollector collector,
        final MetricFilteringBolt bolt, VerificationMode howMany) {
    for (final AlarmDefinition alarmDefinition : Arrays.asList(alarmDef1, dupMetricAlarmDef)) {
      for (final AlarmSubExpression subExpr : alarmDefinition.getAlarmExpression()
          .getSubExpressions()) {
        final MetricDefinitionAndTenantId mtid =
            new MetricDefinitionAndTenantId(subExpr.getMetricDefinition(),
                alarmDefinition.getTenantId()); // First do a MetricDefinition that is an exact
                                                // match
        verify(collector, howMany)
            .emit(MetricFilteringBolt.NEW_METRIC_FOR_ALARM_DEFINITION_STREAM, new Values(mtid, alarmDefinition.getId()));

        // Now do a MetricDefinition with an extra dimension that should still match the
        // SubExpression
        final MetricDefinition inexactMetricDef = addExtraDimension(subExpr.getMetricDefinition());
        verify(collector, howMany).emit(
            MetricFilteringBolt.NEW_METRIC_FOR_ALARM_DEFINITION_STREAM,
            new Values(new MetricDefinitionAndTenantId(inexactMetricDef, alarmDefinition
                .getTenantId()), alarmDefinition.getId()));
      }
    }
  }

  public void testAllInitial() {
    MetricFilteringBolt.clearMetricDefinitions();
    final List<AlarmDefinition> initialAlarmDefinitions = Arrays.asList(alarmDef1, dupMetricAlarmDef);
    final List<Alarm> initialAlarms = createMatchingAlarms(initialAlarmDefinitions);

    final OutputCollector collector1 = mock(OutputCollector.class);

    final MetricFilteringBolt bolt1 = createBolt(initialAlarmDefinitions, initialAlarms, collector1, true);

    final OutputCollector collector2 = mock(OutputCollector.class);

    final MetricFilteringBolt bolt2 = createBolt(initialAlarmDefinitions, initialAlarms, collector2, false);

    // Now ensure metrics pass the filter
    verifyMetricPassed(collector1, bolt1);
    verifyNoNewMetricDefinitionMessagesSent(collector1, bolt1);
    verifyMetricPassed(collector2, bolt2);
    verifyNoNewMetricDefinitionMessagesSent(collector2, bolt2);

    testDeleteSubAlarms(bolt1, collector1, bolt2, collector2);
  }

  private List<Alarm> createMatchingAlarms(List<AlarmDefinition> alarmDefinitions) {
    final List<Alarm> alarms = new LinkedList<>();
    for (final AlarmDefinition alarmDef : alarmDefinitions) {
      final Alarm alarm = new Alarm(getNextId(), alarmDef, AlarmState.UNDETERMINED);
      for (final AlarmSubExpression subExpr : getAllSubAlarms()) {
        // First do a MetricDefinition that is an exact match
        final MetricDefinition metricDefinition = subExpr.getMetricDefinition();
        alarm.addAlarmedMetric(new MetricDefinitionAndTenantId(metricDefinition, alarmDef
            .getTenantId()));

        // Now do a MetricDefinition with an extra dimension that should still match the
        // SubExpression
        final MetricDefinition inexactMetricDef = addExtraDimension(metricDefinition);
        alarm.addAlarmedMetric(new MetricDefinitionAndTenantId(inexactMetricDef, alarmDef
            .getTenantId()));
      }

      alarms.add(alarm);
    }
    return alarms;
  }

  private void testDeleteSubAlarms(MetricFilteringBolt bolt1, OutputCollector collector1,
      MetricFilteringBolt bolt2, OutputCollector collector2) {

    // Now delete the SubAlarm that duplicated a MetricDefinition
    deleteSubAlarms(bolt1, collector1, dupMetricAlarmDef);
    deleteSubAlarms(bolt2, collector2, dupMetricAlarmDef);

    // Ensure metrics still pass the filter
    verifyMetricPassed(collector1, bolt1);
    verifyMetricPassed(collector2, bolt2);

    /** TODO FIX ME, deleting sub alarms doesn't work yet
    deleteSubAlarms(bolt1, collector1, alarmDef1);
    // All MetricDefinitions should be deleted
    assertEquals(MetricFilteringBolt.sizeMetricDefinitions(), 0);
    deleteSubAlarms(bolt2, collector2, alarmDef1);

    verifyMetricFiltered(collector1, bolt1);
    verifyMetricFiltered(collector2, bolt2);
    */
  }

  private void deleteSubAlarms(MetricFilteringBolt bolt, OutputCollector collector,
      final AlarmDefinition alarmDefinition) {
    for (final AlarmSubExpression subAlarm : alarmDefinition.getAlarmExpression().getSubExpressions()) {
      final Tuple tuple = createMetricDefinitionDeletionTuple(alarmDefinition, subAlarm);
      bolt.execute(tuple);
      verify(collector, times(1)).ack(tuple);
    }
  }

  private Tuple createAlarmDefinitionTuple(final AlarmDefinition alarmDef) {
    final MkTupleParam tupleParam = new MkTupleParam();
    tupleParam.setFields(EventProcessingBolt.ALARM_DEFINITION_EVENT_FIELDS);
    tupleParam.setStream(EventProcessingBolt.ALARM_DEFINITION_EVENT_STREAM_ID);
    final AlarmDefinitionCreatedEvent event =
        new AlarmDefinitionCreatedEvent(alarmDef.getTenantId(), alarmDef.getId(),
            alarmDef.getName(), alarmDef.getDescription(), alarmDef.getAlarmExpression()
                .getExpression(), null, alarmDef.getMatchBy());
    final Tuple tuple =
        Testing.testTuple(Arrays.asList(EventProcessingBolt.CREATED, event), tupleParam);
    return tuple;
  }

  private Tuple createMetricDefinitionDeletionTuple(final AlarmDefinition alarmDef, final AlarmSubExpression subAlarm) {
    final MkTupleParam tupleParam = new MkTupleParam();
    tupleParam.setFields(EventProcessingBolt.METRIC_ALARM_EVENT_STREAM_FIELDS);
    tupleParam.setStream(EventProcessingBolt.METRIC_ALARM_EVENT_STREAM_ID);
    final Tuple tuple =
        Testing.testTuple(Arrays.asList(EventProcessingBolt.DELETED,
            new MetricDefinitionAndTenantId(subAlarm.getMetricDefinition(),
                TEST_TENANT_ID), alarmDef.getId()), tupleParam);

    return tuple;
  }

  private Tuple createMetricTuple(final MetricDefinition metricDefinition, final long timestamp,
      final Metric metric) {
    final MkTupleParam tupleParam = new MkTupleParam();
    tupleParam.setFields(MetricSpout.FIELDS);
    tupleParam.setStream(Streams.DEFAULT_STREAM_ID);
    final Tuple tuple =
        Testing.testTuple(Arrays.asList(new TenantIdAndMetricName(TEST_TENANT_ID,
            metricDefinition.name), timestamp, metric), tupleParam);
    return tuple;
  }
}
