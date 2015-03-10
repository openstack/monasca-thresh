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
import static org.testng.Assert.assertEquals;
import monasca.common.model.event.AlarmDefinitionCreatedEvent;
import monasca.common.model.event.AlarmDefinitionDeletedEvent;
import monasca.common.model.alarm.AlarmExpression;
import monasca.common.model.alarm.AlarmState;
import monasca.common.model.alarm.AlarmSubExpression;
import monasca.common.model.metric.Metric;
import monasca.common.model.metric.MetricDefinition;
import monasca.common.streaming.storm.Streams;

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
import monasca.thresh.domain.model.SubExpression;
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
  private long metricTimestamp = System.currentTimeMillis(); // Make sure the metric

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
    return new AlarmDefinition(TEST_TENANT_ID, name, "Alarm1 Description",
        alarm1Expression, "LOW", true, Arrays.asList("hostname"));
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
          for (MetricDefinitionAndTenantId mtid : alarm.getAlarmedMetrics()) {
            // Skip metrics that don't match this sub alarm. The real check checks dimensions but
            // for this test, the name match is sufficient
            if (!mtid.metricDefinition.name.equals(subAlarm.getExpression().getMetricDefinition().name)) {
              continue;
            }
            final TenantIdAndMetricName timn = new TenantIdAndMetricName(mtid);
            verify(collector, times(1)).emit(
                AlarmCreationBolt.ALARM_CREATION_STREAM,
                new Values(EventProcessingBolt.CREATED, timn, mtid, alarmDefinition.getId(),
                    subAlarm));
          }
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
            oldestTimestamp, 42.0, null));
    bolt.execute(lateMetricTuple);
    verify(collector, times(1)).ack(lateMetricTuple);
    bolt.setCurrentTime(prepareTime + MetricFilteringBolt.LAG_MESSAGE_PERIOD_DEFAULT);
    final Tuple lateMetricTuple2 =
        createMetricTuple(metricDefinition, prepareTime, new Metric(metricDefinition, prepareTime,
            42.0, null));
    bolt.execute(lateMetricTuple2);
    verify(collector, times(1)).ack(lateMetricTuple2);
    verify(collector, times(1)).emit(MetricAggregationBolt.METRIC_AGGREGATION_CONTROL_STREAM,
        new Values(MetricAggregationBolt.METRICS_BEHIND));
    bolt.setCurrentTime(prepareTime + 2 * MetricFilteringBolt.LAG_MESSAGE_PERIOD_DEFAULT);
    long caughtUpTimestamp = bolt.getCurrentTime() - MetricFilteringBolt.MIN_LAG_VALUE_DEFAULT;
    final Tuple metricTuple =
        createMetricTuple(metricDefinition, caughtUpTimestamp, new Metric(metricDefinition,
            caughtUpTimestamp, 42.0, null));
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
              prepareTime, 42.0, null));
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
            new Metric(metricDefinition, timestamp, 42.0, null));
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

    final List<Alarm> alarms = createMatchingAlarms(Arrays.asList(alarmDef1, dupMetricAlarmDef));

    // First ensure metrics don't pass the filter
    verifyMetricFiltered(alarms, collector1, bolt1);
    verifyMetricFiltered(alarms, collector2, bolt2);

    sendAlarmDefinitionCreation(collector1, bolt1);
    sendAlarmDefinitionCreation(collector2, bolt2);

    // Now ensure metrics pass the filter
    verifyMetricPassed(alarms, collector1, bolt1);
    verifyNewMetricDefinitionMessagesSent(alarms, collector1, bolt1);
    verifyMetricPassed(alarms, collector2, bolt2);
    verifyNoNewMetricDefinitionMessagesSent(alarms, collector2, bolt2);

    testDeleteAlarms(alarms, bolt1, collector1, bolt2, collector2, true);
  }

  private void sendAlarmDefinitionCreation(final OutputCollector collector1, final MetricFilteringBolt bolt1) {
    for (final AlarmDefinition alarmDef : Arrays.asList(alarmDef1, dupMetricAlarmDef)) {
      final Tuple tuple = createNewAlarmDefinitionTuple(alarmDef);
      bolt1.execute(tuple);
      verify(collector1, times(1)).ack(tuple);
    }
  }

  private void verifyMetricFiltered(List<Alarm> alarms, final OutputCollector collector1,
      final MetricFilteringBolt bolt1) {
    sendMetricsAndVerify(alarms, collector1, bolt1, never());
  }

  private void verifyMetricPassed(List<Alarm> alarms, final OutputCollector collector1, final MetricFilteringBolt bolt1) {
    sendMetricsAndVerify(alarms, collector1, bolt1, times(1));
  }

  private void sendMetricsAndVerify(List<Alarm> alarms, final OutputCollector collector1,
      final MetricFilteringBolt bolt1, VerificationMode howMany) {
    for (final Alarm alarm : alarms) {
      for (MetricDefinitionAndTenantId mtid : alarm.getAlarmedMetrics()) {
        final Tuple exactTuple =
            createMetricTuple(mtid.metricDefinition, metricTimestamp++, new Metric(
                mtid.metricDefinition, metricTimestamp, 42.0, null));
        bolt1.execute(exactTuple);
        verify(collector1, times(1)).ack(exactTuple);
        verify(collector1, howMany)
            .emit(new Values(exactTuple.getValue(0), exactTuple.getValue(2)));
      }
    }
  }

  private MetricDefinition addExtraDimension(final MetricDefinition metricDefinition) {
    final Map<String, String> extraDimensions = new HashMap<>(metricDefinition.dimensions);
    extraDimensions.put("group", "group_a");
    final MetricDefinition inexactMetricDef =
        new MetricDefinition(metricDefinition.name, extraDimensions);
    return inexactMetricDef;
  }

  private void verifyNewMetricDefinitionMessagesSent(List<Alarm> alarms, final OutputCollector collector,
      final MetricFilteringBolt bolt) {
    verifyNewMetricDefinitionMessages(alarms, collector, bolt, times(1));
  }

  private void verifyNoNewMetricDefinitionMessagesSent(List<Alarm> alarms, final OutputCollector collector,
      final MetricFilteringBolt bolt) {
    verifyNewMetricDefinitionMessages(alarms, collector, bolt, never());
  }

  private void verifyNewMetricDefinitionMessages(List<Alarm> alarms, final OutputCollector collector,
        final MetricFilteringBolt bolt, VerificationMode howMany) {
    for (final Alarm alarm : alarms) {
      for (MetricDefinitionAndTenantId mtid : alarm.getAlarmedMetrics()) {
        verify(collector, howMany)
            .emit(MetricFilteringBolt.NEW_METRIC_FOR_ALARM_DEFINITION_STREAM, new Values(mtid, alarm.getAlarmDefinitionId()));
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
    verifyMetricPassed(initialAlarms, collector1, bolt1);
    verifyNoNewMetricDefinitionMessagesSent(initialAlarms, collector1, bolt1);
    verifyMetricPassed(initialAlarms, collector2, bolt2);
    verifyNoNewMetricDefinitionMessagesSent(initialAlarms, collector2, bolt2);

    testDeleteAlarms(initialAlarms, bolt1, collector1, bolt2, collector2, false);
  }

  private List<Alarm> createMatchingAlarms(List<AlarmDefinition> alarmDefinitions) {
    final List<Alarm> alarms = new LinkedList<>();
    for (final AlarmDefinition alarmDef : alarmDefinitions) {
      final Alarm alarm = new Alarm(alarmDef, AlarmState.UNDETERMINED);
      for (final AlarmSubExpression subExpr : alarmDef.getAlarmExpression().getSubExpressions()) {
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

  private void testDeleteAlarms(List<Alarm> alarms, MetricFilteringBolt bolt1, OutputCollector collector1,
      MetricFilteringBolt bolt2, OutputCollector collector2, boolean newMetricsAlreadySent) {

    final List<Alarm> deletedAlarms = new LinkedList<>();
    final List<Alarm> notDeletedAlarms = new LinkedList<>();
    // Now delete the alarm that duplicated a MetricDefinition
    for (final Alarm alarm : alarms) {
      if (alarm.getAlarmDefinitionId().equals(dupMetricAlarmDef.getId())) {
        deleteSubAlarms(bolt1, collector1, alarm);
        deleteSubAlarms(bolt2, collector2, alarm);
        deletedAlarms.add(alarm);
      }
      else {
        notDeletedAlarms.add(alarm);
      }
    }

    // Ensure metrics still pass the filter
    verifyMetricPassed(alarms, collector1, bolt1);
    verifyMetricPassed(alarms, collector2, bolt2);

    int expected = newMetricsAlreadySent ? 1 : 0;
    // New alarms will be created for dupMetricAlarmDef because it still exists
    verifyNewMetricDefinitionMessages(deletedAlarms, collector1, bolt1, times(expected + 1));
    verifyNoNewMetricDefinitionMessagesSent(deletedAlarms, collector2, bolt2);
    verifyNewMetricDefinitionMessages(notDeletedAlarms, collector1, bolt1, times(expected));
    verifyNoNewMetricDefinitionMessagesSent(notDeletedAlarms, collector2, bolt2);

    // Now delete all the alarms
    for (final Alarm alarm : alarms) {
      deleteSubAlarms(bolt1, collector1, alarm);
    }

    // All MetricDefinitions should be deleted
    assertEquals(MetricFilteringBolt.sizeMetricDefinitions(), 0);

    // Ensure bolt2 handles the fact that bolt1 deleted the metrics
    for (final Alarm alarm : alarms) {
      deleteSubAlarms(bolt2, collector2, alarm);
    }

    for (final AlarmDefinition alarmDefinition : Arrays.asList(alarmDef1, dupMetricAlarmDef)) {
      deleteAlarmDefinition(alarmDefinition, bolt1, collector1);
      deleteAlarmDefinition(alarmDefinition, bolt2, collector2);
    }

    verifyMetricFiltered(alarms, collector1, bolt1);
    verifyMetricFiltered(alarms, collector2, bolt2);

    verifyNewMetricDefinitionMessages(deletedAlarms, collector1, bolt1, times(expected + 1));
    verifyNoNewMetricDefinitionMessagesSent(deletedAlarms, collector2, bolt2);
    verifyNewMetricDefinitionMessages(notDeletedAlarms, collector1, bolt1, times(expected));
    verifyNoNewMetricDefinitionMessagesSent(notDeletedAlarms, collector2, bolt2);
  }

  private void deleteAlarmDefinition(final AlarmDefinition alarmDefinition,
      MetricFilteringBolt bolt, OutputCollector collector) {
    final Tuple tuple = createDeleteAlarmDefinitionTuple(alarmDefinition);
    bolt.execute(tuple);
    verify(collector, times(1)).ack(tuple);
  }

  private void deleteSubAlarms(MetricFilteringBolt bolt, OutputCollector collector,
      final Alarm alarm) {
    for (final MetricDefinitionAndTenantId mtid : alarm.getAlarmedMetrics()) {
      final Tuple tuple = createMetricDefinitionDeletionTuple(mtid, alarm.getAlarmDefinitionId());
      bolt.execute(tuple);
      verify(collector, times(1)).ack(tuple);
    }
  }

  private Tuple createNewAlarmDefinitionTuple(final AlarmDefinition alarmDef) {
    final MkTupleParam tupleParam = new MkTupleParam();
    tupleParam.setFields(EventProcessingBolt.ALARM_DEFINITION_EVENT_FIELDS);
    tupleParam.setStream(EventProcessingBolt.ALARM_DEFINITION_EVENT_STREAM_ID);
    final AlarmDefinitionCreatedEvent event =
        new AlarmDefinitionCreatedEvent(alarmDef.getTenantId(), alarmDef.getId(),
            alarmDef.getName(), alarmDef.getDescription(), alarmDef.getAlarmExpression()
                .getExpression(), createSubExpressionMap(alarmDef), alarmDef.getMatchBy());
    final Tuple tuple =
        Testing.testTuple(Arrays.asList(EventProcessingBolt.CREATED, event), tupleParam);
    return tuple;
  }


  public static Map<String, AlarmSubExpression> createSubExpressionMap(AlarmDefinition alarmDef) {
    final Map<String, AlarmSubExpression> exprs = new HashMap<>();
    for (final SubExpression subExpr : alarmDef.getSubExpressions()) {
      exprs.put(subExpr.getId(), subExpr.getAlarmSubExpression());
    }
    return exprs;
  }

  private Tuple createDeleteAlarmDefinitionTuple(final AlarmDefinition alarmDef) {
    final MkTupleParam tupleParam = new MkTupleParam();
    tupleParam.setFields(EventProcessingBolt.ALARM_DEFINITION_EVENT_FIELDS);
    tupleParam.setStream(EventProcessingBolt.ALARM_DEFINITION_EVENT_STREAM_ID);
    final Map<String, MetricDefinition> subAlarmMetricDefinitions = new HashMap<>();
    for (final AlarmSubExpression subExpr : alarmDef.getAlarmExpression().getSubExpressions()) {
      subAlarmMetricDefinitions.put(getNextId(), subExpr.getMetricDefinition());
    }
    final AlarmDefinitionDeletedEvent event =
        new AlarmDefinitionDeletedEvent(alarmDef.getId(), subAlarmMetricDefinitions);
    final Tuple tuple =
        Testing.testTuple(Arrays.asList(EventProcessingBolt.DELETED, event), tupleParam);
    return tuple;
  }

  private Tuple createMetricDefinitionDeletionTuple(final MetricDefinitionAndTenantId mtid,
      final String alarmDefinitionId) {
    final MkTupleParam tupleParam = new MkTupleParam();
    tupleParam.setFields(EventProcessingBolt.METRIC_ALARM_EVENT_STREAM_FIELDS);
    tupleParam.setStream(EventProcessingBolt.METRIC_ALARM_EVENT_STREAM_ID);
    // This code doesn't know the sub alarm id and the metric filtering bolt doesn't care
    final String subAlarmId = "";
    final Tuple tuple =
        Testing.testTuple(Arrays.asList(EventProcessingBolt.DELETED,
            new TenantIdAndMetricName(mtid), mtid, alarmDefinitionId, subAlarmId), tupleParam);

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
