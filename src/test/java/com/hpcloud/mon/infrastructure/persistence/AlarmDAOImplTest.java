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
package com.hpcloud.mon.infrastructure.persistence;

import static org.testng.Assert.assertEquals;

import java.nio.charset.Charset;
import java.util.Arrays;

import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.io.Resources;
import com.hpcloud.mon.common.model.alarm.AlarmExpression;
import com.hpcloud.mon.common.model.alarm.AlarmState;
import com.hpcloud.mon.common.model.alarm.AlarmSubExpression;
import com.hpcloud.mon.domain.model.Alarm;
import com.hpcloud.mon.domain.model.SubAlarm;
import com.hpcloud.mon.domain.service.AlarmDAO;

@Test
public class AlarmDAOImplTest {
  private static final String TENANT_ID = "bob";
  private static final String ALARM_ID = "123";
  private static String ALARM_NAME = "90% CPU";
  private static String ALARM_DESCR = "Description for " + ALARM_NAME;
  private static Boolean ALARM_ENABLED = Boolean.TRUE;

  private DBI db;
  private Handle handle;
  private AlarmDAO dao;

  @BeforeClass
  protected void setupClass() throws Exception {
    db = new DBI("jdbc:h2:mem:test;MODE=MySQL");
    handle = db.open();
    handle.execute(Resources.toString(getClass().getResource("alarm.sql"), Charset.defaultCharset()));
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
    handle.execute("truncate table sub_alarm_dimension");
    handle.execute("truncate table alarm_action");

    String sql = String.format("insert into alarm (id, tenant_id, name, description, expression, state, actions_enabled, created_at, updated_at) "
        + "values ('%s', '%s', '%s', '%s', 'avg(hpcs.compute{disk=vda, instance_id=123, metric_name=cpu}) > 10', 'UNDETERMINED', %d, NOW(), NOW())",
        ALARM_ID, TENANT_ID, ALARM_NAME, ALARM_DESCR, ALARM_ENABLED ? 1 : 0);
    handle.execute(sql);
    handle.execute("insert into sub_alarm (id, alarm_id, function, metric_name, operator, threshold, period, periods, created_at, updated_at) "
        + "values ('111', '123', 'AVG', 'hpcs.compute', 'GT', 10, 60, 1, NOW(), NOW())");
    handle.execute("insert into sub_alarm_dimension values ('111', 'instance_id', '123')");
    handle.execute("insert into sub_alarm_dimension values ('111', 'disk', 'vda')");
    handle.execute("insert into sub_alarm_dimension values ('111', 'metric_name', 'cpu')");
    handle.execute("insert into alarm_action values ('123', '29387234')");
    handle.execute("insert into alarm_action values ('123', '77778687')");
  }

  public void shouldFindById() {
    String expr = "avg(hpcs.compute{disk=vda, instance_id=123, metric_name=cpu}) > 10";
    Alarm expected = new Alarm(ALARM_ID, TENANT_ID, ALARM_NAME, ALARM_DESCR, AlarmExpression.of(expr),
        Arrays.asList(new SubAlarm("111", ALARM_ID, AlarmSubExpression.of(expr))),
        AlarmState.UNDETERMINED, Boolean.TRUE);

    Alarm alarm = dao.findById(ALARM_ID);

    // Identity equality
    assertEquals(alarm, expected);
    assertEquals(alarm.getSubAlarms(), expected.getSubAlarms());
  }

  public void shouldUpdateState() {
    dao.updateState(ALARM_ID, AlarmState.ALARM);
    assertEquals(dao.findById(ALARM_ID).getState(), AlarmState.ALARM);
  }
}
