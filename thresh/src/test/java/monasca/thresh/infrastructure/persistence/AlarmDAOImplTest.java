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

package monasca.thresh.infrastructure.persistence;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.hpcloud.mon.common.model.alarm.AlarmExpression;
import com.hpcloud.mon.common.model.alarm.AlarmState;
import com.hpcloud.mon.common.model.metric.MetricDefinition;

import com.google.common.io.Resources;

import monasca.thresh.domain.model.Alarm;
import monasca.thresh.domain.model.AlarmDefinition;
import monasca.thresh.domain.model.MetricDefinitionAndTenantId;
import monasca.thresh.domain.service.AlarmDAO;

import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Test
public class AlarmDAOImplTest {
  private static final String TENANT_ID = "bob";
  private static String ALARM_NAME = "90% CPU";
  private static String ALARM_DESCR = "Description for " + ALARM_NAME;
  private static Boolean ALARM_ENABLED = Boolean.TRUE;
  private MetricDefinitionAndTenantId newMetric;

  private AlarmDefinition alarmDef;

  private DBI db;
  private Handle handle;
  private AlarmDAO dao;

  @BeforeClass
  protected void setupClass() throws Exception {
    db = new DBI("jdbc:h2:mem:test;MODE=MySQL");
    handle = db.open();
    handle
        .execute(Resources.toString(getClass().getResource("alarm.sql"), Charset.defaultCharset()));
    dao = new AlarmDAOImpl(db);
  }

  @AfterClass
  protected void afterClass() {
    handle.close();
  }

  @BeforeMethod
  protected void beforeMethod() {
    handle.execute("truncate table alarm");
    handle.execute("truncate table sub_alarm");
    handle.execute("truncate table alarm_metric");
    handle.execute("truncate table metric_definition");
    handle.execute("truncate table metric_definition_dimensions");
    handle.execute("truncate table metric_dimension");

    final String expr = "avg(load{first=first_value}) > 10 and max(cpu) < 90";
    alarmDef =
        new AlarmDefinition(getNextId(), TENANT_ID, ALARM_NAME, ALARM_DESCR, new AlarmExpression(
            expr), "LOW", ALARM_ENABLED, new ArrayList<String>());

    final Map<String, String> dimensions = new HashMap<String, String>();
    dimensions.put("first", "first_value");
    dimensions.put("second", "second_value");
    final MetricDefinition md = new MetricDefinition("load", dimensions);
    newMetric = new MetricDefinitionAndTenantId(md, TENANT_ID);
  }

  private String getNextId() {
    return UUID.randomUUID().toString();
  }

  public void shouldFindForAlarmDefinitionId() {
    verifyAlarmList(dao.findForAlarmDefinitionId(alarmDef.getId()));

    final Alarm firstAlarm = new Alarm(getNextId(), alarmDef, AlarmState.OK);
    firstAlarm.addAlarmedMetric(newMetric);

    dao.createAlarm(firstAlarm);

    final Alarm secondAlarm = new Alarm(getNextId(), alarmDef, AlarmState.OK);
    dao.createAlarm(secondAlarm);

    final AlarmDefinition secondAlarmDef =
        new AlarmDefinition(getNextId(), TENANT_ID, "Second", null, new AlarmExpression(
            "avg(cpu{disk=vda, instance_id=123}) > 10"), "LOW", true, Arrays.asList("dev"));

    final Alarm thirdAlarm = new Alarm(getNextId(), secondAlarmDef, AlarmState.OK);
    dao.createAlarm(thirdAlarm);

    verifyAlarmList(dao.findForAlarmDefinitionId(alarmDef.getId()), firstAlarm, secondAlarm);

    verifyAlarmList(dao.findForAlarmDefinitionId(secondAlarmDef.getId()), thirdAlarm);

    verifyAlarmList(dao.listAll(), firstAlarm, secondAlarm, thirdAlarm);
  }

  private void verifyAlarmList(final List<Alarm> found, Alarm... expected) {
    assertEquals(expected.length, found.size());
    for (final Alarm alarm : expected) {
      assertTrue(found.contains(alarm));
    }
  }

  public void shouldFindById() {
    String alarmId = getNextId();
    assertNull(dao.findById(alarmId));

    final Alarm newAlarm = new Alarm(alarmId, alarmDef, AlarmState.OK);
    dao.createAlarm(newAlarm);

    assertEquals(dao.findById(alarmId), newAlarm);

    dao.addAlarmedMetric(newAlarm.getId(), newMetric);
    newAlarm.addAlarmedMetric(newMetric);

    assertEquals(dao.findById(alarmId), newAlarm);

    // Make sure it can handle MetricDefinition with no dimensions
    final MetricDefinitionAndTenantId anotherMetric =
        new MetricDefinitionAndTenantId(new MetricDefinition("cpu", new HashMap<String, String>()),
            TENANT_ID);
    dao.addAlarmedMetric(newAlarm.getId(), anotherMetric);
    newAlarm.addAlarmedMetric(anotherMetric);

    assertEquals(dao.findById(alarmId), newAlarm);
  }

  public void checkComplexMetrics() {
    String alarmId = getNextId();

    final Alarm newAlarm = new Alarm(alarmId, alarmDef, AlarmState.ALARM);

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

    final Alarm found = dao.findById(alarmId);
    // Have to check both ways because there was a bug in AlarmDAOImpl and it showed up if both
    // ways were tested
    assertTrue(newAlarm.equals(found));
    assertTrue(found.equals(newAlarm));
  }

  public void shouldUpdateState() {
    String alarmId = getNextId();

    final Alarm newAlarm = new Alarm(alarmId, alarmDef, AlarmState.OK);

    dao.createAlarm(newAlarm);
    dao.updateState(alarmId, AlarmState.ALARM);
    assertEquals(dao.findById(alarmId).getState(), AlarmState.ALARM);
  }

  public void validateNoDuplicates() {
    final Alarm alarm1 = new Alarm(getNextId(), alarmDef, AlarmState.OK);
    alarm1.addAlarmedMetric(newMetric);
    dao.createAlarm(alarm1);
    assertEquals(dao.findById(alarm1.getId()), alarm1);

    final Alarm alarm2 = new Alarm(getNextId(), alarmDef, AlarmState.OK);
    alarm2.addAlarmedMetric(newMetric);
    dao.createAlarm(alarm2);
    assertEquals(dao.findById(alarm2.getId()), alarm2);

    assertEquals(1, handle.select("select * from metric_definition").size());
    assertEquals(1, handle.select("select * from metric_definition_dimensions").size());
    List<Map<String, Object>> rows = handle.select("select * from metric_dimension");
    assertEquals(2, rows.size());
  }
}
