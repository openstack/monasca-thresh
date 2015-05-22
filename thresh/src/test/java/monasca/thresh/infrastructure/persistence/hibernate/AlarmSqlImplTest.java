/*
 * Copyright 2015 FUJITSU LIMITED
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package monasca.thresh.infrastructure.persistence.hibernate;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.beust.jcommander.internal.Maps;
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

import org.hibernate.SessionFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Test scenarios for AlarmSqlRepoImpl.
 *
 * @author lukasz.zajaczkowski@ts.fujitsu.com
 * @author tomasz.trebski@ts.fujitsu.com
 */
@Test(groups = "orm")
public class AlarmSqlImplTest {
  private static final String TENANT_ID = "bob";
  private static String ALARM_NAME = "90% CPU";
  private static String ALARM_DESCR = "Description for " + ALARM_NAME;
  private static Boolean ALARM_ENABLED = Boolean.TRUE;
  private MetricDefinitionAndTenantId newMetric;
  private AlarmDefinition alarmDef;

  private SessionFactory sessionFactory;
  private AlarmDAO dao;

  @BeforeMethod
  protected void setupClass() throws Exception {
    sessionFactory = HibernateUtil.getSessionFactory();
    dao = new AlarmSqlImpl(sessionFactory);
    this.prepareData();
  }

  @AfterMethod
  protected void afterMethod() {
    this.sessionFactory.close();
    this.sessionFactory = null;
  }

  protected void prepareData() {
    final String expr = "avg(load{first=first_value}) > 10 and max(cpu) < 90";
    alarmDef = new AlarmDefinition(TENANT_ID, ALARM_NAME, ALARM_DESCR, new AlarmExpression(expr), "LOW", ALARM_ENABLED, new ArrayList<String>());
    HibernateUtil.insertAlarmDefinition(this.sessionFactory.openSession(), alarmDef);

    final Map<String, String> dimensions = new HashMap<String, String>();
    dimensions.put("first", "first_value");
    dimensions.put("second", "second_value");
    final MetricDefinition md = new MetricDefinition("load", dimensions);
    newMetric = new MetricDefinitionAndTenantId(md, TENANT_ID);
  }

  @Test(groups = "orm")
  public void shouldFindForAlarmDefinitionId() {
    verifyAlarmList(dao.findForAlarmDefinitionId(alarmDef.getId()));

    final Alarm firstAlarm = new Alarm(alarmDef, AlarmState.OK);
    firstAlarm.addAlarmedMetric(newMetric);

    dao.createAlarm(firstAlarm);

    final Alarm secondAlarm = new Alarm(alarmDef, AlarmState.OK);
    secondAlarm.addAlarmedMetric(newMetric);
    dao.createAlarm(secondAlarm);

    final AlarmDefinition secondAlarmDef =
        new AlarmDefinition(TENANT_ID, "Second", null, new AlarmExpression("avg(cpu{disk=vda, instance_id=123}) > 10"), "LOW", true,
            Arrays.asList("dev"));
    HibernateUtil.insertAlarmDefinition(sessionFactory.openSession(), secondAlarmDef);

    final Alarm thirdAlarm = new Alarm(secondAlarmDef, AlarmState.OK);
    final Map<String, String> dimensionMap = new HashMap<>();
    dimensionMap.put("disk", "vda");
    dimensionMap.put("instance_id", "123");
    thirdAlarm.addAlarmedMetric(new MetricDefinitionAndTenantId(new MetricDefinition("cpu", dimensionMap), secondAlarmDef.getTenantId()));
    dao.createAlarm(thirdAlarm);

    verifyAlarmList(dao.findForAlarmDefinitionId(alarmDef.getId()), firstAlarm, secondAlarm);

    verifyAlarmList(dao.findForAlarmDefinitionId(secondAlarmDef.getId()), thirdAlarm);

    verifyAlarmList(dao.listAll(), firstAlarm, secondAlarm, thirdAlarm);
  }

  @Test(groups = "orm")
  public void shouldUpdateState() {
    final Alarm newAlarm = new Alarm(alarmDef, AlarmState.OK);

    dao.createAlarm(newAlarm);
    dao.updateState(newAlarm.getId(), AlarmState.ALARM);
    assertEquals(dao.findById(newAlarm.getId()).getState(), AlarmState.ALARM);
  }

  private void verifyAlarmList(final List<Alarm> found, Alarm... expected) {
    assertEquals(found.size(), expected.length);
    for (final Alarm alarm : expected) {
      assertTrue(found.contains(alarm));
    }
  }

  @Test(groups = "orm")
  public void checkComplexMetrics() {
    final Alarm newAlarm = new Alarm(alarmDef, AlarmState.ALARM);

    for (final String hostname : Arrays.asList("vivi", "eleanore")) {
      for (final String metricName : Arrays.asList("cpu", "load")) {
        final Map<String, String> dimensions = Maps.newHashMap();
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

  @Test(groups = "orm")
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

  @Test(groups = "orm")
  public void shouldDelete() {
    final Alarm newAlarm = new Alarm(alarmDef, AlarmState.OK);
    dao.createAlarm(newAlarm);
    dao.deleteByDefinitionId(newAlarm.getAlarmDefinitionId());

    assertNull(dao.findById(newAlarm.getId()));
  }
}
