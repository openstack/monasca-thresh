/*
 * (C) Copyright 2014,2016 Hewlett Packard Enterprise Development  LP
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

package monasca.thresh.infrastructure.persistence;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertNull;

import monasca.common.model.alarm.AggregateFunction;
import monasca.common.model.alarm.AlarmExpression;
import monasca.common.model.alarm.AlarmState;
import monasca.common.model.metric.MetricDefinition;
import monasca.thresh.domain.model.Alarm;
import monasca.thresh.domain.model.AlarmDefinition;
import monasca.thresh.domain.model.MetricDefinitionAndTenantId;
import monasca.thresh.domain.model.SubAlarm;
import monasca.thresh.domain.model.SubExpression;
import monasca.thresh.domain.service.AlarmDAO;

import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * These tests won't work without the real mysql database so use mini-mon.
 * Warning, this will truncate the alarms part of your mini-mon database
 * @author craigbr
 *
 */
@Test(groups = "database")
public class AlarmDAOImplTest {
  private static final String TENANT_ID = "bob";
  private static String ALARM_NAME = "90% CPU";
  private static String ALARM_DESCR = "Description for " + ALARM_NAME;
  private static Boolean ALARM_ENABLED = Boolean.TRUE;
  private static String DETERMINISTIC_ALARM_NAME = "count(log.error)";
  private static String DETERMINISTIC_ALARM_DESCRIPTION = "Description for " + ALARM_NAME;
  private static Boolean DETERMINISTIC_ALARM_ENABLED = Boolean.TRUE;
  private MetricDefinitionAndTenantId newMetric;

  private AlarmDefinition deterministicAlarmDef;
  private AlarmDefinition alarmDef;

  private DBI db;
  private Handle handle;
  private AlarmDAO dao;

  @BeforeClass
  protected void setupClass() throws Exception {
    // See class comment
    db = new DBI("jdbc:mysql://192.168.10.4/mon", "monapi", "password");
    handle = db.open();
    dao = new AlarmDAOImpl(db);
  }

  @AfterClass
  protected void afterClass() {
    handle.close();
  }

  @BeforeMethod
  protected void beforeMethod() {
    handle.execute("SET foreign_key_checks = 0;");
    handle.execute("truncate table alarm_definition");
    handle.execute("truncate table alarm");
    handle.execute("truncate table sub_alarm");
    handle.execute("truncate table sub_alarm_definition");
    handle.execute("truncate table sub_alarm_definition_dimension");
    handle.execute("truncate table alarm_metric");
    handle.execute("truncate table metric_definition");
    handle.execute("truncate table metric_definition_dimensions");
    handle.execute("truncate table metric_dimension");

    final String expr = "avg(load{first=first_value}) > 10 and max(cpu) < 90";
    alarmDef =
        new AlarmDefinition(TENANT_ID, ALARM_NAME, ALARM_DESCR, new AlarmExpression(
            expr), "LOW", ALARM_ENABLED, new ArrayList<String>());
    AlarmDefinitionDAOImplTest.insertAlarmDefinition(handle, alarmDef);

    final String deterministicExpr = "count(log.error{path=/var/log/test},deterministic,20) > 5";
    this.deterministicAlarmDef = new AlarmDefinition(
        TENANT_ID,
        DETERMINISTIC_ALARM_NAME,
        DETERMINISTIC_ALARM_DESCRIPTION,
        new AlarmExpression(deterministicExpr),
        "HIGH",
        DETERMINISTIC_ALARM_ENABLED,
        new ArrayList<String>()
    );
    AlarmDefinitionDAOImplTest.insertAlarmDefinition(handle, this.deterministicAlarmDef);

    final Map<String, String> dimensions = new HashMap<String, String>();
    dimensions.put("first", "first_value");
    dimensions.put("second", "second_value");
    final MetricDefinition md = new MetricDefinition("load", dimensions);
    newMetric = new MetricDefinitionAndTenantId(md, TENANT_ID);
  }

  public void shouldFindForAlarmDefinitionId() {
    verifyAlarmList(dao.findForAlarmDefinitionId(alarmDef.getId()));

    final Alarm firstAlarm = new Alarm(alarmDef, AlarmState.OK);
    firstAlarm.addAlarmedMetric(newMetric);

    dao.createAlarm(firstAlarm);

    final Alarm secondAlarm = new Alarm(alarmDef, AlarmState.OK);
    secondAlarm.addAlarmedMetric(newMetric);
    dao.createAlarm(secondAlarm);

    final AlarmDefinition secondAlarmDef =
        new AlarmDefinition(TENANT_ID, "Second", null, new AlarmExpression(
            "avg(cpu{disk=vda, instance_id=123}) > 10"), "LOW", true, Arrays.asList("dev"));
    AlarmDefinitionDAOImplTest.insertAlarmDefinition(handle, secondAlarmDef);

    final Alarm thirdAlarm = new Alarm(secondAlarmDef, AlarmState.OK);
    final Map<String, String> dims = new HashMap<>();
    dims.put("disk", "vda");
    dims.put("instance_id", "123");
    thirdAlarm.addAlarmedMetric(new MetricDefinitionAndTenantId(new MetricDefinition("cpu", dims),
        secondAlarmDef.getTenantId()));
    dao.createAlarm(thirdAlarm);

    verifyAlarmList(dao.findForAlarmDefinitionId(alarmDef.getId()), firstAlarm, secondAlarm);

    verifyAlarmList(dao.findForAlarmDefinitionId(secondAlarmDef.getId()), thirdAlarm);

    verifyAlarmList(dao.listAll(), firstAlarm, secondAlarm, thirdAlarm);
  }

  private void verifyAlarmList(final List<Alarm> found, Alarm... expected) {
    assertEquals(found.size(), expected.length);
    for (final Alarm alarm : expected) {
      assertTrue(found.contains(alarm));
    }
  }

  public void shouldFindById() {
    final Alarm newAlarm = new Alarm(alarmDef, AlarmState.OK);
    assertNull(dao.findById(newAlarm.getId()));

    dao.createAlarm(newAlarm);

    assertEquals(dao.findById(newAlarm.getId()), newAlarm);

    dao.addAlarmedMetric(newAlarm.getId(), newMetric);
    newAlarm.addAlarmedMetric(newMetric);

    assertEquals(dao.findById(newAlarm.getId()), newAlarm);

    // Make sure it can handle MetricDefinition with no dimensions
    final MetricDefinitionAndTenantId anotherMetric =
        new MetricDefinitionAndTenantId(new MetricDefinition("cpu", new HashMap<String, String>()),
            TENANT_ID);
    dao.addAlarmedMetric(newAlarm.getId(), anotherMetric);
    newAlarm.addAlarmedMetric(anotherMetric);

    assertEquals(dao.findById(newAlarm.getId()), newAlarm);
  }

  public void checkComplexMetrics() {
    final Alarm newAlarm = new Alarm(alarmDef, AlarmState.ALARM);

    for (final String hostname : Arrays.asList("vivi", "eleanore")) {
      for (final String metricName : Arrays.asList("cpu", "load")) {
        final Map<String, String> dimensions = new HashMap<String, String>();
        dimensions.put("first", "first_value");
        dimensions.put("second", "second_value");
        dimensions.put("hostname", hostname);
        final MetricDefinition md = new MetricDefinition(metricName, dimensions);
        newAlarm.addAlarmedMetric(new MetricDefinitionAndTenantId(md, TENANT_ID));
      }
    }
    dao.createAlarm(newAlarm);

    final Alarm found = dao.findById(newAlarm.getId());
    // Have to check both ways because there was a bug in AlarmDAOImpl and it showed up if both
    // ways were tested
    assertTrue(newAlarm.equals(found));
    assertTrue(found.equals(newAlarm));
  }

  public void shouldUpdateState() {
    final Alarm newAlarm = new Alarm(alarmDef, AlarmState.OK);

    dao.createAlarm(newAlarm);
    dao.updateState(newAlarm.getId(), AlarmState.ALARM, System.currentTimeMillis());
    assertEquals(dao.findById(newAlarm.getId()).getState(), AlarmState.ALARM);
  }

  public void shouldUpdate() {
    final Alarm newAlarm = new Alarm(alarmDef, AlarmState.OK);
    dao.createAlarm(newAlarm);

    final SubExpression first = alarmDef.getSubExpressions().get(0);
    final AggregateFunction newFunction = AggregateFunction.COUNT;
    first.getAlarmSubExpression().setFunction(newFunction);
    assertEquals(1, dao.updateSubAlarmExpressions(first.getId(), first.getAlarmSubExpression()));
    // Find the SubAlarm that was created from the changed SubExpression
    boolean found = false;
    for (final SubAlarm subAlarm : newAlarm.getSubAlarms()) {
      if (subAlarm.getAlarmSubExpressionId().equals(first.getId())) {
        found = true;
        // This is what dao.updateSubAlarmExpressions() should have changed
        subAlarm.getExpression().setFunction(newFunction);
        break;
      }
    }
    assertTrue(found);
    assertEquals(dao.findById(newAlarm.getId()), newAlarm);
  }

  public void validateNoDuplicates() {
    final Alarm alarm1 = new Alarm(alarmDef, AlarmState.OK);
    alarm1.addAlarmedMetric(newMetric);
    dao.createAlarm(alarm1);
    assertEquals(dao.findById(alarm1.getId()), alarm1);

    final Alarm alarm2 = new Alarm(alarmDef, AlarmState.OK);
    alarm2.addAlarmedMetric(newMetric);
    dao.createAlarm(alarm2);
    assertEquals(dao.findById(alarm2.getId()), alarm2);

    assertEquals(1, handle.select("select * from metric_definition").size());
    assertEquals(1, handle.select("select * from metric_definition_dimensions").size());
    List<Map<String, Object>> rows = handle.select("select * from metric_dimension");
    assertEquals(2, rows.size());
  }

  public void shouldPersistDeterministic() {
    final Alarm alarm1 = new Alarm(this.deterministicAlarmDef);
    final MetricDefinition definition = this.deterministicAlarmDef
        .getSubExpressions()
        .get(0)
        .getAlarmSubExpression()
        .getMetricDefinition();
    final MetricDefinitionAndTenantId mtid = new MetricDefinitionAndTenantId(
        definition,
        TENANT_ID
    );

    alarm1.addAlarmedMetric(mtid);
    dao.createAlarm(alarm1);

    final Alarm byId = dao.findById(alarm1.getId());

    assertEquals(byId, alarm1);
    assertEquals(1, byId.getDeterministicSubAlarms().size());
  }
}
