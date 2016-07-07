/*
 * (C) Copyright 2014,2016 Hewlett Packard Enterprise Development Company LP.
 * Copyright 2016 FUJITSU LIMITED
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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import monasca.common.model.alarm.AlarmExpression;
import monasca.common.model.alarm.AlarmState;
import monasca.common.model.alarm.AlarmSubExpression;
import monasca.common.model.event.AlarmDefinitionDeletedEvent;
import monasca.common.model.event.AlarmDeletedEvent;
import monasca.common.model.metric.MetricDefinition;
import monasca.thresh.domain.model.Alarm;
import monasca.thresh.domain.model.AlarmDefinition;
import monasca.thresh.domain.model.MetricDefinitionAndTenantId;
import monasca.thresh.domain.model.SubAlarm;
import monasca.thresh.domain.model.SubExpression;
import monasca.thresh.domain.model.TenantIdAndMetricName;
import monasca.thresh.domain.service.AlarmDAO;
import monasca.thresh.domain.service.AlarmDefinitionDAO;

import org.apache.storm.Testing;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.testing.MkTupleParam;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Test
public class AlarmCreationBoltTest {

  private static final String TENANT_ID = "42";

  private final AlarmDAO alarmDAO = mock(AlarmDAO.class);
  private final AlarmDefinitionDAO alarmDefDAO = mock(AlarmDefinitionDAO.class);
  private final AlarmCreationBolt bolt = new AlarmCreationBolt(alarmDefDAO, alarmDAO);
  final OutputCollector collector = mock(OutputCollector.class);

  final private List<Alarm> createdAlarms = new LinkedList<>();
  final private Map<String, List<Alarm>> existingAlarms = new HashMap<>();

  @BeforeMethod
  public void beforeMethod() {
    final Map<String, String> config = new HashMap<>();
    final TopologyContext context = mock(TopologyContext.class);
    bolt.prepare(config, context, collector);
    this.createdAlarms.clear();
    doAnswer(new Answer<Object>() {
      public Object answer(InvocationOnMock invocation) {
        final Object[] args = invocation.getArguments();
        final Alarm newAlarm = (Alarm) args[0];
        createdAlarms.add(newAlarm);
        // Return this and any previously created alarms when queried for this alarm definition IDs
        List<Alarm> alarmList = existingAlarms.get(newAlarm.getAlarmDefinitionId());
        if (alarmList == null) {
          alarmList = new LinkedList<>();
          existingAlarms.put(newAlarm.getAlarmDefinitionId(), alarmList);
        }
        alarmList.add(newAlarm);
        when(alarmDAO.findForAlarmDefinitionId(newAlarm.getAlarmDefinitionId())).thenReturn(alarmList);
        return null;
      }
    }).when(alarmDAO).createAlarm((Alarm) any());
  }

  public void testmetricFitsInAlarmSubExpr() {
    final String expression =
        "max(cpu{hostname=eleanore}) > 90 and max(load_avg{hostname=eleanore,cpu=1}) > 10";
    final AlarmExpression alarmExpression = new AlarmExpression(expression);
    final String alarmId = getNextId();
    final SubAlarm cpu =
        new SubAlarm(getNextId(), alarmId, new SubExpression(UUID.randomUUID().toString(),
            alarmExpression.getSubExpressions().get(0)));
    final SubAlarm load_avg =
        new SubAlarm(getNextId(), alarmId, new SubExpression(UUID.randomUUID().toString(),
            alarmExpression.getSubExpressions().get(1)));

    assertTrue(AlarmCreationBolt.metricFitsInAlarmSubExpr(cpu.getExpression(), cpu.getExpression()
        .getMetricDefinition()));
    assertTrue(AlarmCreationBolt.metricFitsInAlarmSubExpr(load_avg.getExpression(), load_avg
        .getExpression().getMetricDefinition()));
    assertFalse(AlarmCreationBolt.metricFitsInAlarmSubExpr(load_avg.getExpression(), cpu
        .getExpression().getMetricDefinition()));
    assertFalse(AlarmCreationBolt.metricFitsInAlarmSubExpr(cpu.getExpression(), load_avg
        .getExpression().getMetricDefinition()));
    MetricDefinition load_avgSuperSet =
        build(load_avg.getExpression().getMetricDefinition().name, "hostname", "eleanore", "cpu",
            "1", "other", "vivi");
    assertTrue(AlarmCreationBolt.metricFitsInAlarmSubExpr(load_avg.getExpression(),
        load_avgSuperSet));
    assertFalse(AlarmCreationBolt.metricFitsInAlarmSubExpr(
        load_avg.getExpression(),
        build(cpu.getExpression().getMetricDefinition().name, "hostname", "eleanore", "cpu", "2",
            "other", "vivi")));
  }

  public void testmetricFitsInAlarmDefinition() {
    final AlarmDefinition alarmDefinition =
        createAlarmDefinition("max(cpu{service=2}) > 90 and max(load_avg) > 10", "hostname");

    final MetricDefinitionAndTenantId goodCpu =
        new MetricDefinitionAndTenantId(build("cpu", "hostname", "eleanore", "service", "2",
            "other", "vivi"), TENANT_ID);

    assertTrue(bolt.validMetricDefinition(alarmDefinition, goodCpu));

    final MetricDefinitionAndTenantId goodLoad =
        new MetricDefinitionAndTenantId(build("load_avg", "hostname", "eleanore", "service", "2",
            "other", "vivi"), TENANT_ID);

    assertTrue(bolt.validMetricDefinition(alarmDefinition, goodLoad));

    final MetricDefinitionAndTenantId goodLoadNoDim =
        new MetricDefinitionAndTenantId(build("load_avg"), TENANT_ID);

    assertTrue(bolt.validMetricDefinition(alarmDefinition, goodLoadNoDim));

    final MetricDefinitionAndTenantId badCpuDim =
        new MetricDefinitionAndTenantId(build("cpu", "hostname", "eleanore", "service", "1",
            "other", "vivi"), TENANT_ID);
    assertFalse(bolt.validMetricDefinition(alarmDefinition, badCpuDim));

    final MetricDefinitionAndTenantId wrongMetricName =
        new MetricDefinitionAndTenantId(build("mem", "hostname", "eleanore", "service", "1",
            "other", "vivi"), TENANT_ID);
    assertFalse(bolt.validMetricDefinition(alarmDefinition, wrongMetricName));

    final MetricDefinitionAndTenantId badCpuNoDim =
        new MetricDefinitionAndTenantId(build("cpu"), TENANT_ID);
    assertFalse(bolt.validMetricDefinition(alarmDefinition, badCpuNoDim));

    final MetricDefinitionAndTenantId badCpuWrongTenant =
        new MetricDefinitionAndTenantId(build("cpu"), TENANT_ID + "2");
    assertFalse(bolt.validMetricDefinition(alarmDefinition, badCpuWrongTenant));

    // check deterministic
    final AlarmDefinition deterministicAlarmDefinition =
        createAlarmDefinition("count(log.error{},deterministic) > 2", "hostname");

    // deterministic, same tenant, with dimensions
    MetricDefinitionAndTenantId validLogError =
        new MetricDefinitionAndTenantId(build("log.error", "hostname", "eleanore", "path",
            "/var/log/test.log"), TENANT_ID);
    assertTrue(bolt.validMetricDefinition(deterministicAlarmDefinition, validLogError));

    // deterministic, same tenant, no dimensions
    MetricDefinitionAndTenantId invalidLogError =
        new MetricDefinitionAndTenantId(build("log.error"), TENANT_ID);
    assertTrue(bolt.validMetricDefinition(deterministicAlarmDefinition, invalidLogError));

    // deterministic, different tenant
    invalidLogError =
        new MetricDefinitionAndTenantId(build("log.error"), TENANT_ID + "234");
    assertFalse(bolt.validMetricDefinition(deterministicAlarmDefinition, invalidLogError));
  }

  public void testMetricFitsInAlarm() {
    final AlarmDefinition alarmDefinition =
        createAlarmDefinition("max(cpu{service=2}) > 90 and max(load_avg{service=2}) > 10",
            "hostname");

    final Alarm alarm = new Alarm(alarmDefinition);
    alarm.setState(AlarmState.ALARM);

    final Iterator<SubAlarm> iterator = alarm.getSubAlarms().iterator();
    final SubAlarm cpu = iterator.next();
    final SubAlarm disk = iterator.next();
    final MetricDefinition alarmedMetric =
        build(cpu.getExpression().getMetricDefinition().name, "hostname", "eleanore", "service",
            "2");
    alarm.addAlarmedMetric(new MetricDefinitionAndTenantId(alarmedMetric, TENANT_ID));

    final MetricDefinition check =
        build(disk.getExpression().getMetricDefinition().name, "hostname", "eleanore", "service",
            "2", "other", "vivi");
    assertTrue(bolt.metricFitsInAlarm(alarm, alarmDefinition, new MetricDefinitionAndTenantId(
        check, TENANT_ID)));

    final MetricDefinition check2 =
        build(disk.getExpression().getMetricDefinition().name, "hostname", "vivi", "service", "2",
            "other", "eleanore");
    assertFalse(bolt.metricFitsInAlarm(alarm, alarmDefinition, new MetricDefinitionAndTenantId(
        check2, TENANT_ID)));

    final MetricDefinition check3 =
        build(disk.getExpression().getMetricDefinition().name, "service", "2", "other", "eleanore");
    assertFalse(bolt.metricFitsInAlarm(alarm, alarmDefinition, new MetricDefinitionAndTenantId(
        check3, TENANT_ID)));

    final MetricDefinition check4 =
        build(disk.getExpression().getMetricDefinition().name, "hostname", "eleanore", "service",
            "1", "other", "vivi");
    assertFalse(bolt.metricFitsInAlarm(alarm, alarmDefinition, new MetricDefinitionAndTenantId(
        check4, TENANT_ID)));
  }

  public void testAlarmDefinitionDeleted() {
    final AlarmDefinition alarmDefinition =
        createAlarmDefinition("max(cpu{service=2}) > 90 and max(load{service=2}) > 2", "hostname");

    // Create some waiting alarms.
    final AlarmSubExpression subExpr =
        alarmDefinition.getAlarmExpression().getSubExpressions().get(0);
    final List<String> hostnames = Arrays.asList("eleanore", "vivi", "maddyie");
    for (final String hostname : hostnames) {
      final MetricDefinition metric =
          build(subExpr.getMetricDefinition().name, "hostname", hostname, "service", "2");
      sendNewMetric(new MetricDefinitionAndTenantId(metric, TENANT_ID), alarmDefinition.getId());
    }

    assertEquals(bolt.countWaitingAlarms(alarmDefinition.getId()), Integer.valueOf(hostnames.size()));

    sendAlarmDefinitionDeleted(alarmDefinition);

    // Ensure they are gone
    assertNull(bolt.countWaitingAlarms(alarmDefinition.getId()));

    final AlarmDefinition alarmDefinition2 =
        createAlarmDefinition("max(cpu{service=2}) > 90 and max(load{service=2}) > 3", "hostname");

    sendAlarmDefinitionDeleted(alarmDefinition2);

    // Ensure there are no waiting alarms
    assertNull(bolt.countWaitingAlarms(alarmDefinition2.getId()));
  }

  public void testAlarmDefinitionUpdated() {
    final AlarmDefinition alarmDefinition =
        createAlarmDefinition("max(cpu{service=2}) > 90 and max(load{service=2}) > 2", "hostname");

    // Create some waiting alarms.
    final AlarmSubExpression subExpr =
        alarmDefinition.getAlarmExpression().getSubExpressions().get(0);
    final List<String> hostnames = Arrays.asList("eleanore", "vivi", "maddyie");
    for (final String hostname : hostnames) {
      final MetricDefinition metric =
          build(subExpr.getMetricDefinition().name, "hostname", hostname, "service", "2");
      sendNewMetric(new MetricDefinitionAndTenantId(metric, TENANT_ID), alarmDefinition.getId());
    }

    assertEquals(bolt.countWaitingAlarms(alarmDefinition.getId()), Integer.valueOf(hostnames.size()));

    // Update the Alarm Definition
    final SubExpression first = alarmDefinition.getSubExpressions().get(0);

    // We make a copy of the SubExpression because the actual SubExpression from the AlarmDefinition is
    // in the Alarm and updating first updates the SubAlarm's SubExpresssion directly
    final SubExpression copy = new SubExpression(first.getId(), AlarmSubExpression.of(first.getAlarmSubExpression().getExpression()));
    copy.getAlarmSubExpression().setThreshold(42.0);
    final MkTupleParam tupleParam = new MkTupleParam();
    tupleParam.setFields(EventProcessingBolt.METRIC_SUB_ALARM_EVENT_STREAM_FIELDS);
    tupleParam.setStream(EventProcessingBolt.METRIC_SUB_ALARM_EVENT_STREAM_ID);
    final Tuple tuple =
        Testing.testTuple(
            Arrays.asList(EventProcessingBolt.UPDATED, copy, alarmDefinition.getId()), tupleParam);

    bolt.execute(tuple);

    final AlarmSubExpression subExpr2 =
        alarmDefinition.getAlarmExpression().getSubExpressions().get(1);

    // Now finish the Alarms
    for (final String hostname : hostnames) {
      final MetricDefinition metric =
          build(subExpr2.getMetricDefinition().name, "hostname", hostname, "service", "2");
      sendNewMetric(new MetricDefinitionAndTenantId(metric, TENANT_ID), alarmDefinition.getId());
    }

    assertEquals(this.createdAlarms.size(), hostnames.size());

    // Can't use verifyCreatedAlarm because then the AlarmDefinition must be updated which
    // might update the SubAlarms directly because of reuse of AlarmSubExpressions
    for (final Alarm alarm : this.createdAlarms) {
      boolean found = false;
      for (SubAlarm subAlarm : alarm.getSubAlarms()) {
        if (subAlarm.getAlarmSubExpressionId().equals(first.getId())) {
          assertEquals(subAlarm.getExpression().getThreshold(),
                       copy.getAlarmSubExpression().getThreshold());
          found = true;
          break;
        }
      }
      assertTrue(found, "Did not find expected sub alarm");
    }
  }

  private void sendAlarmDefinitionDeleted(final AlarmDefinition alarmDefinition) {
    final Map<String, MetricDefinition> subAlarmMetricDefinitions = new HashMap<>();
    for (final AlarmSubExpression subExpr : alarmDefinition.getAlarmExpression().getSubExpressions()) {
      subAlarmMetricDefinitions.put(getNextId(), subExpr.getMetricDefinition());
    }
    // Delete the Alarm Definition
    final AlarmDefinitionDeletedEvent event =
        new AlarmDefinitionDeletedEvent(alarmDefinition.getId(), subAlarmMetricDefinitions);
    final MkTupleParam tupleParam = new MkTupleParam();
    tupleParam.setFields(EventProcessingBolt.ALARM_DEFINITION_EVENT_FIELDS);
    tupleParam.setStream(EventProcessingBolt.ALARM_DEFINITION_EVENT_STREAM_ID);
    final Tuple tuple =
        Testing.testTuple(Arrays.asList(EventProcessingBolt.DELETED, event), tupleParam);

    bolt.execute(tuple);
  }

  private void sendNewMetric(MetricDefinitionAndTenantId metricDefinitionAndTenantId,
                             String alarmDefinitionId) {
    final MkTupleParam tupleParam = new MkTupleParam();
    tupleParam.setFields(MetricFilteringBolt.NEW_METRIC_FOR_ALARM_DEFINITION_FIELDS);
    tupleParam.setStream(MetricFilteringBolt.NEW_METRIC_FOR_ALARM_DEFINITION_STREAM);
    final Tuple tuple =
        Testing.testTuple(Arrays.asList(metricDefinitionAndTenantId, alarmDefinitionId), tupleParam);

    bolt.execute(tuple);
  }

  public void testCreateSimpleDeterministicAlarm() {
    this.runCreateComplexAlarm(true);
  }

  public void testCreateSimpleDeterministicAlarmWithMatchBy() {
    this.runCreateComplexAlarm(true, "hostname");
  }

  public void testCreateSimpleAlarmWithMatchBy() {
    this.runCreateSimpleAlarm(false, "hostname");
  }

  public void testCreateSimpleAlarm() {
    this.runCreateSimpleAlarm(false);
  }

  public void testCreateComplexAlarmWithMatchBy() {
    this.runCreateComplexAlarm(false, "hostname");
  }

  public void testCreateComplexAlarm() {
    this.runCreateComplexAlarm(false);
  }

  public void testFinishesMultipleAlarms() {
    final List<MetricDefinition> metricDefinitionsToSend = new LinkedList<>();
    final int numDevs = 4;
    for (int i = 0; i < numDevs; i++) {
      final String dev = String.format("dev%d", i);
      final MetricDefinition diskMetric =
          build("disk.io", "hostname", "eleanore", "service", "2", "dev", dev);
      metricDefinitionsToSend.add(diskMetric);
    }
    final MetricDefinition cpuMetric =
        build("cpu", "hostname", "eleanore", "service", "2", "other", "vivi");
    metricDefinitionsToSend.add(cpuMetric);

    testMultipleExpressions(metricDefinitionsToSend, numDevs);
  }

  public void testMetricReusedInMultipleAlarms() {
    final List<MetricDefinition> metricDefinitionsToSend = new LinkedList<>();
    final MetricDefinition cpuMetric =
        build("cpu", "hostname", "eleanore", "service", "2", "other", "vivi");
    metricDefinitionsToSend.add(cpuMetric);
    final int numDevs = 4;
    for (int i = 0; i < numDevs; i++) {
      final String dev = String.format("dev%d", i);
      final MetricDefinition diskMetric =
          build("disk.io", "hostname", "eleanore", "service", "2", "dev", dev);
      metricDefinitionsToSend.add(diskMetric);
    }

    testMultipleExpressions(metricDefinitionsToSend, numDevs);
  }

  public void testReuseMetricFromExistingAlarm() {
    final boolean deterministic = false;
    final String expression = "max(cpu{service=vivi}) > 90";
    final String[] matchBy = new String[] { "hostname", "amplifier" };
    final AlarmDefinition alarmDefinition = createAlarmDefinition(expression, matchBy);

    final MetricDefinition metric =
        build("cpu", "hostname", "eleanore", "amplifier", "2", "service", "vivi");

    bolt.handleNewMetricDefinition(new MetricDefinitionAndTenantId(metric, TENANT_ID),
        alarmDefinition.getId());

    assertEquals(this.createdAlarms.size(), 1);
    verifyCreatedAlarm(this.createdAlarms.get(0), alarmDefinition, deterministic, collector,
        new MetricDefinitionAndTenantId(metric, TENANT_ID));

    final MetricDefinition metric2 =
        build("cpu", "hostname", "eleanore", "service", "vivi");

    sendNewMetric(new MetricDefinitionAndTenantId(metric2, TENANT_ID), alarmDefinition.getId());

    assertEquals(this.createdAlarms.size(), 1,
          "A second alarm was created instead of the metric fitting into the first");

    verifyCreatedAlarm(this.createdAlarms.get(0), alarmDefinition, deterministic, collector,
        new MetricDefinitionAndTenantId(metric, TENANT_ID),
        new MetricDefinitionAndTenantId(metric2, TENANT_ID));

    final MetricDefinition metric3 =
        build("cpu", "hostname", "eleanore", "amplifier", "3", "service", "vivi");

    bolt.handleNewMetricDefinition(new MetricDefinitionAndTenantId(metric3, TENANT_ID),
        alarmDefinition.getId());

    assertEquals(this.createdAlarms.size(), 2);

    verifyCreatedAlarm(this.createdAlarms.get(1), alarmDefinition, deterministic, collector,
        new MetricDefinitionAndTenantId(metric3, TENANT_ID),
        new MetricDefinitionAndTenantId(metric2, TENANT_ID));
  }

  public void testUseMetricInExistingAlarm() {
    final boolean deterministic = false;
    final String expression = "max(cpu{service=vivi}) > 90";
    final String[] matchBy = new String[] { "hostname", "amplifier" };
    final AlarmDefinition alarmDefinition = createAlarmDefinition(expression, matchBy);

    final MetricDefinition metric =
        build("cpu", "hostname", "eleanore", "amplifier", "2", "service", "vivi");

    bolt.handleNewMetricDefinition(new MetricDefinitionAndTenantId(metric, TENANT_ID),
        alarmDefinition.getId());

    assertEquals(this.createdAlarms.size(), 1);
    verifyCreatedAlarm(this.createdAlarms.get(0), alarmDefinition, deterministic, collector,
        new MetricDefinitionAndTenantId(metric, TENANT_ID));

    final MetricDefinition metric3 =
        build("cpu", "hostname", "eleanore", "amplifier", "3", "service", "vivi");

    bolt.handleNewMetricDefinition(new MetricDefinitionAndTenantId(metric3, TENANT_ID),
        alarmDefinition.getId());

    assertEquals(this.createdAlarms.size(), 2);

    verifyCreatedAlarm(this.createdAlarms.get(1), alarmDefinition, deterministic, collector,
        new MetricDefinitionAndTenantId(metric3, TENANT_ID));

    final MetricDefinition metric2 =
        build("cpu", "hostname", "eleanore", "service", "vivi");

    sendNewMetric(new MetricDefinitionAndTenantId(metric2, TENANT_ID), alarmDefinition.getId());

    assertEquals(this.createdAlarms.size(), 2,
          "A third alarm was created instead of the metric fitting into the first two");

    verifyCreatedAlarm(this.createdAlarms.get(0), alarmDefinition, deterministic, collector,
        new MetricDefinitionAndTenantId(metric, TENANT_ID),
        new MetricDefinitionAndTenantId(metric2, TENANT_ID));

    verifyCreatedAlarm(this.createdAlarms.get(1), alarmDefinition, deterministic, collector,
        new MetricDefinitionAndTenantId(metric3, TENANT_ID),
        new MetricDefinitionAndTenantId(metric2, TENANT_ID));
  }

  public void testDeletedAlarm() {
    final AlarmDefinition alarmDefinition = runCreateSimpleAlarm(false);
    assertEquals(this.createdAlarms.size(), 1);
    final Alarm alarmToDelete = this.createdAlarms.get(0);
    this.createdAlarms.clear();
    final Map<String, AlarmSubExpression> subAlarms = new HashMap<>();
    for (final SubAlarm subAlarm : alarmToDelete.getSubAlarms()) {
      subAlarms.put(subAlarm.getId(), subAlarm.getExpression());
    }
    final List<MetricDefinition> alarmedMetrics = new ArrayList<>();
    for (final MetricDefinitionAndTenantId mdtid : alarmToDelete.getAlarmedMetrics()) {
      alarmedMetrics.add(mdtid.metricDefinition);
    }
    final AlarmDeletedEvent event = new AlarmDeletedEvent(TENANT_ID, alarmToDelete.getId(),
        alarmedMetrics, alarmToDelete.getAlarmDefinitionId(), subAlarms);

    final MkTupleParam tupleParam = new MkTupleParam();
    tupleParam.setFields(EventProcessingBolt.ALARM_EVENT_STREAM_FIELDS);
    tupleParam.setStream(EventProcessingBolt.ALARM_EVENT_STREAM_ID);
    final Tuple tuple =
        Testing.testTuple(Arrays.asList(EventProcessingBolt.DELETED, alarmToDelete.getId(), event),
            tupleParam);

    bolt.execute(tuple);

    // Make sure the alarm gets created again
    createAlarms(alarmDefinition, false);
  }

  private void testMultipleExpressions(final List<MetricDefinition> metricDefinitionsToSend,
      final int numAlarms) {
    final AlarmDefinition alarmDefinition =
        createAlarmDefinition("max(cpu) > 90 and max(disk.io) > 10", "hostname", "dev");

    for (final MetricDefinition md : metricDefinitionsToSend) {
      sendNewMetric(new MetricDefinitionAndTenantId(md, TENANT_ID), alarmDefinition.getId());
    }

    assertEquals(this.createdAlarms.size(), numAlarms);
  }

  private AlarmDefinition runCreateSimpleAlarm(final Boolean deterministic, final String... matchBy) {
    final String expression = String.format(
        "max(cpu{service=2}%s) > 90", (deterministic ? ",deterministic" : "")
    );
    final AlarmDefinition alarmDefinition = createAlarmDefinition(expression, matchBy);
    this.createAlarms(alarmDefinition, deterministic, matchBy);
    return alarmDefinition;
  }

  private void createAlarms(final AlarmDefinition alarmDefinition,
                            final Boolean deterministic,
                            final String... matchBy) {
    final MetricDefinition metric =
        build("cpu", "hostname", "eleanore", "service", "2", "other", "vivi");

    bolt.handleNewMetricDefinition(new MetricDefinitionAndTenantId(metric, TENANT_ID),
        alarmDefinition.getId());

    assertEquals(this.createdAlarms.size(), 1);
    this.verifyCreatedAlarm(
        this.createdAlarms.get(0),
        alarmDefinition,
        deterministic,
        collector,
        new MetricDefinitionAndTenantId(metric, TENANT_ID)
    );

    final MetricDefinition metric2 =
        build("cpu", "hostname", "vivi", "service", "2", "other", "eleanore");

    sendNewMetric(new MetricDefinitionAndTenantId(metric2, TENANT_ID), alarmDefinition.getId());
    if (matchBy.length == 0) {
      assertEquals(this.createdAlarms.size(), 1,
          "A second alarm was created instead of the metric fitting into the first");
    } else {
      assertEquals(this.createdAlarms.size(), 2,
          "The metric was fitted into the first alarm instead of creating a new alarm");

      verifyCreatedAlarm(this.createdAlarms.get(1), alarmDefinition, deterministic, collector,
          new MetricDefinitionAndTenantId(metric2, TENANT_ID));

      // Now send a metric that must fit into the just created alarm to test that
      // code path
      final MetricDefinition metric3 =
          build("cpu", "hostname", "vivi", "service", "2", "other", "maddyie");

      sendNewMetric(new MetricDefinitionAndTenantId(metric3, TENANT_ID), alarmDefinition.getId());

      assertEquals(this.createdAlarms.size(), 2,
          "The metric created a new alarm instead of fitting into the second");

      verifyCreatedAlarm(this.createdAlarms.get(1), alarmDefinition, deterministic, collector,
          new MetricDefinitionAndTenantId(metric2, TENANT_ID), new MetricDefinitionAndTenantId(metric3, TENANT_ID));
    }
  }

  private void runCreateComplexAlarm(final Boolean deterministic, final String... matchBy) {
    final String rawExpression = "max(cpu{service=2}%s) > 90 or max(load.avg{service=2}%s) > 5";
    final String expression;
    final AlarmDefinition alarmDefinition;

    if (deterministic) {
      expression = String.format(rawExpression, ",deterministic", ",deterministic");
    } else {
      expression = String.format(rawExpression, "", "");
    }

    alarmDefinition = this.createAlarmDefinition(expression, matchBy);

    final MetricDefinition cpuMetric =
        build("cpu", "hostname", "eleanore", "service", "2", "other", "vivi");

    MetricDefinitionAndTenantId cpuMtid = new MetricDefinitionAndTenantId(cpuMetric, TENANT_ID);
    bolt.handleNewMetricDefinition(cpuMtid, alarmDefinition.getId());

    // Send it again to ensure it handles case where the metric is sent twice.
    // Should not happen but make sure bolt handles it
    bolt.handleNewMetricDefinition(cpuMtid, alarmDefinition.getId());

    final MetricDefinition loadAvgMetric =
        build("load.avg", "hostname", "eleanore", "service", "2", "other", "vivi");

    MetricDefinitionAndTenantId loadAvgMtid =
        new MetricDefinitionAndTenantId(loadAvgMetric, TENANT_ID);
    bolt.handleNewMetricDefinition(loadAvgMtid, alarmDefinition.getId());

    assertEquals(this.createdAlarms.size(), 1);
    verifyCreatedAlarm(this.createdAlarms.get(0), alarmDefinition, deterministic, collector,
        cpuMtid, loadAvgMtid);

    // Send it again to ensure it handles case where the metric is sent after
    // the alarm has been created.
    // Should not happen but make sure bolt handles it
    bolt.handleNewMetricDefinition(cpuMtid, alarmDefinition.getId());

    assertEquals(this.createdAlarms.size(), 1);
    // Make sure it did not get added to the existing alarm
    verifyCreatedAlarm(this.createdAlarms.get(0), alarmDefinition, deterministic, collector,
        cpuMtid, loadAvgMtid);
  }

  private AlarmDefinition createAlarmDefinition(final String expression, final String... matchBy) {
    final AlarmExpression alarmExpression = new AlarmExpression(expression);

    final AlarmDefinition alarmDefinition =
        new AlarmDefinition(TENANT_ID, "max cpu", "", alarmExpression, "LOW", true,
            Arrays.asList(matchBy));
    when(alarmDefDAO.findById(alarmDefinition.getId())).thenReturn(alarmDefinition);
    return alarmDefinition;
  }

  private void verifyCreatedAlarm(final Alarm newAlarm,
                                  final AlarmDefinition alarmDefinition,
                                  final Boolean deterministic,
                                  final OutputCollector collector,
                                  MetricDefinitionAndTenantId... mtids) {

    final AlarmState expectedState = deterministic ? AlarmState.OK : AlarmState.UNDETERMINED;
    assertEquals(newAlarm.getState(), expectedState);

    final String alarmId = newAlarm.getId();
    final Alarm expectedAlarm = new Alarm(alarmDefinition);
    expectedAlarm.setId(alarmId);
    final List<SubAlarm> expectedSubAlarms = new LinkedList<>();
    for (final SubAlarm expectedSubAlarm : expectedAlarm.getSubAlarms()) {
      boolean found = false;
      for (final SubAlarm newSubAlarm : newAlarm.getSubAlarms()) {
        if (expectedSubAlarm.getExpression().equals(newSubAlarm.getExpression())) {
          found = true;
          expectedSubAlarms.add(new SubAlarm(newSubAlarm.getId(), alarmId, new SubExpression(
              expectedSubAlarm.getAlarmSubExpressionId(), expectedSubAlarm.getExpression())));
          break;
        }
      }
      assertTrue(found, "SubAlarms for created Alarm don't match the Alarm Definition");
    }
    expectedAlarm.setSubAlarms(expectedSubAlarms);

    assertEquals(newAlarm.getAlarmedMetrics().size(), mtids.length);

    for (final SubAlarm subAlarm : expectedAlarm.getSubAlarms()) {
      // Have to do it this way because order of sub alarms is not deterministic
      MetricDefinitionAndTenantId mtid = null;
      for (final MetricDefinitionAndTenantId check : mtids) {
        if (subAlarm.getExpression().getMetricDefinition().name.equals(check.metricDefinition.name)) {
          mtid = check;
          break;
        }
      }
      assertNotNull(mtid, String.format("Did not find metric for %s", subAlarm.getExpression()
          .getMetricDefinition().name));

      verify(collector, times(1)).emit(
          AlarmCreationBolt.ALARM_CREATION_STREAM,
          new Values(EventProcessingBolt.CREATED, new TenantIdAndMetricName(mtid), mtid,
              alarmDefinition.getId(), subAlarm));
    }
  }

  private MetricDefinition build(final String name, String... dimensions) {
    final Map<String, String> dimensionsMap = new HashMap<String, String>();
    for (int i = 0; i < dimensions.length; i += 2) {
      dimensionsMap.put(dimensions[i], dimensions[i + 1]);
    }
    return new MetricDefinition(name, dimensionsMap);
  }

  private String getNextId() {
    return UUID.randomUUID().toString();
  }
}
