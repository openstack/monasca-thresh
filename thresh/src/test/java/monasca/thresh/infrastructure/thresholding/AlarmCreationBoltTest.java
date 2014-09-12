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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.hpcloud.mon.common.model.alarm.AlarmExpression;
import com.hpcloud.mon.common.model.alarm.AlarmState;
import com.hpcloud.mon.common.model.alarm.AlarmSubExpression;
import com.hpcloud.mon.common.model.metric.MetricDefinition;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Values;

import monasca.thresh.domain.model.Alarm;
import monasca.thresh.domain.model.AlarmDefinition;
import monasca.thresh.domain.model.MetricDefinitionAndTenantId;
import monasca.thresh.domain.model.SubAlarm;
import monasca.thresh.domain.model.TenantIdAndMetricName;
import monasca.thresh.domain.service.AlarmDAO;
import monasca.thresh.domain.service.AlarmDefinitionDAO;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
  final private Map<String, List<Alarm>> existingAlarms = new HashMap<>();;

  private final String ALARM_DEF_ID = getNextId();

  @BeforeMethod
  public void beforeMethod() {
    final Map<String, String> config = new HashMap<>();
    final TopologyContext context = mock(TopologyContext.class);
    bolt.prepare(config, context, collector);
    this.createdAlarms.clear();;
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

  public void testMetricFitsInSubAlarm() {
    final String expression =
        "max(cpu{hostname=eleanore}) > 90 and max(load_avg{hostname=eleanore,cpu=1}) > 10";
    final AlarmExpression alarmExpression = new AlarmExpression(expression);
    final String alarmId = getNextId();
    final SubAlarm cpu =
        new SubAlarm(getNextId(), alarmId, alarmExpression.getSubExpressions().get(0));
    final SubAlarm load_avg =
        new SubAlarm(getNextId(), alarmId, alarmExpression.getSubExpressions().get(1));

    assertTrue(AlarmCreationBolt.metricFitsInSubAlarm(cpu, cpu.getExpression()
        .getMetricDefinition()));
    assertTrue(AlarmCreationBolt.metricFitsInSubAlarm(load_avg, load_avg.getExpression()
        .getMetricDefinition()));
    assertFalse(AlarmCreationBolt.metricFitsInSubAlarm(load_avg, cpu.getExpression()
        .getMetricDefinition()));
    assertFalse(AlarmCreationBolt.metricFitsInSubAlarm(cpu, load_avg.getExpression()
        .getMetricDefinition()));
    MetricDefinition load_avgSuperSet =
        build(load_avg.getExpression().getMetricDefinition().name, "hostname", "eleanore", "cpu",
            "1", "other", "vivi");
    assertTrue(AlarmCreationBolt.metricFitsInSubAlarm(load_avg, load_avgSuperSet));
    assertFalse(AlarmCreationBolt.metricFitsInSubAlarm(
        load_avg,
        build(cpu.getExpression().getMetricDefinition().name, "hostname", "eleanore", "cpu", "2",
            "other", "vivi")));

    final Alarm alarm =
        new Alarm(getNextId(), Arrays.asList(cpu, load_avg), ALARM_DEF_ID, AlarmState.UNDETERMINED);
    assertTrue(bolt.fitsInAlarm(alarm, load_avgSuperSet));
  }

  public void testMetricFitsInAlarm() {
    final String expression = "max(cpu{service=2}) > 90 and max(load_avg{service=2}) > 10";
    final AlarmExpression alarmExpression = new AlarmExpression(expression);

    final AlarmDefinition alarmDefinition =
        new AlarmDefinition(getNextId(), TENANT_ID, "max cpu", "", alarmExpression, true,
            Arrays.asList("hostname"));

    String alarmId = getNextId();
    final Alarm alarm = new Alarm(alarmId, alarmDefinition, AlarmState.ALARM);

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

  public void testCreateSimpleAlarmWithMatchBy() {
    runCreateSimpleAlarm("hostname");
  }

  public void testCreateSimpleAlarm() {
    runCreateSimpleAlarm();
  }

  public void testCreateComplexAlarmWithMatchBy() {
    runCreateComplexAlarm("hostname");
  }

  public void testCreateComplexAlarm() {
    runCreateComplexAlarm();
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

  private void testMultipleExpressions(final List<MetricDefinition> metricDefinitionsToSend,
      final int numAlarms) {
    final String expression = "max(cpu) > 90 and max(disk.io) > 10";
    final AlarmExpression alarmExpression = new AlarmExpression(expression);

    final AlarmDefinition alarmDefinition =
        new AlarmDefinition(getNextId(), TENANT_ID, "max cpu and disk", "", alarmExpression, true,
            Arrays.asList("hostname", "dev"));

    when(alarmDefDAO.findById(alarmDefinition.getId())).thenReturn(alarmDefinition);

    for (final MetricDefinition md : metricDefinitionsToSend) {
      bolt.handleNewMetricDefinition(new MetricDefinitionAndTenantId(md, TENANT_ID),
          alarmDefinition.getId());
    }

    assertEquals(this.createdAlarms.size(), numAlarms);
  }

  private void runCreateSimpleAlarm(final String... matchBy) {

    final String expression = "max(cpu{service=2}) > 90";
    final AlarmExpression alarmExpression = new AlarmExpression(expression);

    final AlarmDefinition alarmDefinition =
        new AlarmDefinition(getNextId(), TENANT_ID, "max cpu", "", alarmExpression, true,
            Arrays.asList(matchBy));
    when(alarmDefDAO.findById(alarmDefinition.getId())).thenReturn(alarmDefinition);
    final MetricDefinition metric =
        build("cpu", "hostname", "eleanore", "service", "2", "other", "vivi");

    bolt.handleNewMetricDefinition(new MetricDefinitionAndTenantId(metric, TENANT_ID),
        alarmDefinition.getId());

    assertEquals(this.createdAlarms.size(), 1);
    verifyCreatedAlarm(this.createdAlarms.get(0), alarmDefinition, collector,
        new MetricDefinitionAndTenantId(metric, TENANT_ID));

    this.createdAlarms.clear();;
    final MetricDefinition metric2 =
        build("cpu", "hostname", "vivi", "service", "2", "other", "eleanore");

    bolt.handleNewMetricDefinition(new MetricDefinitionAndTenantId(metric2, TENANT_ID),
        alarmDefinition.getId());
    if (matchBy.length == 0) {
      assertEquals(this.createdAlarms.size(), 0,
          "A second alarm was created instead of the metric fitting into the first");
    } else {
      assertEquals(this.createdAlarms.size(), 1,
          "The metric was fitted into the first alarm instead of creating a new alarm");

      verifyCreatedAlarm(this.createdAlarms.get(0), alarmDefinition, collector,
          new MetricDefinitionAndTenantId(metric2, TENANT_ID));
    }
  }

  private void runCreateComplexAlarm(final String... matchBy) {
    final String expression = "max(cpu{service=2}) > 90 or max(load.avg{service=2}) > 5";
    final AlarmExpression alarmExpression = new AlarmExpression(expression);

    final AlarmDefinition alarmDefinition =
        new AlarmDefinition(getNextId(), TENANT_ID, "max cpu", "", alarmExpression, true,
            Arrays.asList(matchBy));
    when(alarmDefDAO.findById(alarmDefinition.getId())).thenReturn(alarmDefinition);

    final MetricDefinition cpuMetric =
        build("cpu", "hostname", "eleanore", "service", "2", "other", "vivi");

    MetricDefinitionAndTenantId cpuMtid = new MetricDefinitionAndTenantId(cpuMetric, TENANT_ID);
    bolt.handleNewMetricDefinition(cpuMtid, alarmDefinition.getId());

    final MetricDefinition loadAvgMetric =
        build("load.avg", "hostname", "eleanore", "service", "2", "other", "vivi");

    MetricDefinitionAndTenantId loadAvgMtid =
        new MetricDefinitionAndTenantId(loadAvgMetric, TENANT_ID);
    bolt.handleNewMetricDefinition(loadAvgMtid, alarmDefinition.getId());

    assertEquals(this.createdAlarms.size(), 1);
    verifyCreatedAlarm(this.createdAlarms.get(0), alarmDefinition, collector, cpuMtid, loadAvgMtid);
  }

  private void verifyCreatedAlarm(final Alarm newAlarm, final AlarmDefinition alarmDefinition,
      final OutputCollector collector, MetricDefinitionAndTenantId... mtids) {
    final String alarmId = newAlarm.getId();
    final Alarm expectedAlarm =
        new Alarm(alarmId, createSubAlarms(alarmDefinition, alarmId, newAlarm.getSubAlarms()),
            alarmDefinition.getId(), AlarmState.UNDETERMINED);
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

  private List<SubAlarm> createSubAlarms(AlarmDefinition alarmDefinition, final String alarmId,
      final Collection<SubAlarm> actualSubAlarms) {
    final List<SubAlarm> subAlarms =
        new ArrayList<>(alarmDefinition.getAlarmExpression().getSubExpressions().size());
    final Iterator<SubAlarm> iterator = actualSubAlarms.iterator();
    for (final AlarmSubExpression subExpr : alarmDefinition.getAlarmExpression()
        .getSubExpressions()) {
      subAlarms.add(new SubAlarm(iterator.next().getId(), alarmId, subExpr));
    }
    return subAlarms;
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
