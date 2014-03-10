package com.hpcloud.mon.infrastructure.persistence;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.hpcloud.mon.common.model.alarm.AlarmState;
import com.hpcloud.mon.common.model.alarm.AlarmSubExpression;
import com.hpcloud.mon.common.model.metric.MetricDefinition;
import com.hpcloud.mon.domain.model.SubAlarm;
import com.hpcloud.mon.domain.service.SubAlarmDAO;

/**
 * @author Jonathan Halterman
 */
@Test
public class SubAlarmDAOImplTest {
  private DBI db;
  private Handle handle;
  private SubAlarmDAO dao;

  @BeforeClass
  protected void setupClass() throws Exception {
    db = new DBI("jdbc:h2:mem:test;MODE=MySQL");
    handle = db.open();
    handle.execute(Resources.toString(getClass().getResource("alarm.sql"), Charset.defaultCharset()));
    dao = new SubAlarmDAOImpl(db);
  }

  @AfterClass
  protected void afterClass() {
    handle.close();
  }

  @BeforeMethod
  protected void beforeMethod() {
    handle.execute("truncate table sub_alarm");
    handle.execute("truncate table sub_alarm_dimension");

    handle.execute("insert into sub_alarm (id, alarm_id, function, namespace, operator, threshold, period, periods, created_at, updated_at) "
        + "values ('111', '123', 'AVG', 'hpcs.compute', 'GT', 10, 60, 1, NOW(), NOW())");
    handle.execute("insert into sub_alarm_dimension values ('111', 'instance_id', '555')");
    handle.execute("insert into sub_alarm_dimension values ('111', 'az', '1')");
    handle.execute("insert into sub_alarm_dimension values ('111', 'instance_uuid', '555')");
    handle.execute("insert into sub_alarm_dimension values ('111', 'metric_name', 'cpu')");

    handle.execute("insert into sub_alarm (id, alarm_id, function, namespace, operator, threshold, period, periods, created_at, updated_at) "
        + "values ('222', '234', 'AVG', 'hpcs.compute', 'GT', 10, 60, 1, NOW(), NOW())");
    handle.execute("insert into sub_alarm_dimension values ('222', 'instance_id', '666')");
    handle.execute("insert into sub_alarm_dimension values ('222', 'az', '1')");
    handle.execute("insert into sub_alarm_dimension values ('222', 'instance_uuid', '666')");
    handle.execute("insert into sub_alarm_dimension values ('222', 'metric_name', 'cpu')");

    handle.execute("insert into sub_alarm (id, alarm_id, function, namespace, operator, threshold, period, periods, created_at, updated_at) "
        + "values ('333', '345', 'AVG', 'hpcs.compute', 'GT', 10, 60, 1, NOW(), NOW())");
    handle.execute("insert into sub_alarm_dimension values ('333', 'instance_id', '777')");
    handle.execute("insert into sub_alarm_dimension values ('333', 'az', '1')");
    handle.execute("insert into sub_alarm_dimension values ('333', 'instance_uuid', '777')");
    handle.execute("insert into sub_alarm_dimension values ('333', 'metric_name', 'disk')");
    handle.execute("insert into sub_alarm_dimension values ('333', 'device', 'vda')");

    handle.execute("insert into sub_alarm (id, alarm_id, function, namespace, operator, threshold, period, periods, created_at, updated_at) "
        + "values ('444', '456', 'AVG', 'hpcs.compute', 'GT', 10, 60, 1, NOW(), NOW())");
    handle.execute("insert into sub_alarm_dimension values ('444', 'metric_name', 'cpu')");
  }

  public void shouldFind() {
    List<SubAlarm> expected = Arrays.asList(new SubAlarm("111", "123",
        AlarmSubExpression.of("avg(hpcs.compute{instance_id=555,az=1,metric_name=cpu}) > 10"),
        AlarmState.UNDETERMINED));
    List<SubAlarm> subAlarms = dao.find(expected.get(0).getExpression().getMetricDefinition());
    assertEquals(subAlarms, expected);

    expected = Arrays.asList(new SubAlarm("222", "234",
        AlarmSubExpression.of("avg(hpcs.compute{instance_id=666,az=1,metric_name=cpu}) > 10"),
        AlarmState.UNDETERMINED));
    subAlarms = dao.find(expected.get(0).getExpression().getMetricDefinition());
    assertEquals(subAlarms, expected);
  }

  public void shouldFindWithSubject() {
    List<SubAlarm> expected = Arrays.asList(new SubAlarm(
        "333",
        "345",
        AlarmSubExpression.of("avg(hpcs.compute{instance_id=777,az=1,metric_name=disk,device=vda}) > 10"),
        AlarmState.UNDETERMINED));
    List<SubAlarm> subAlarms = dao.find(expected.get(0).getExpression().getMetricDefinition());
    assertEquals(subAlarms, expected);
  }

  public void shouldFailFindForNullDimensions() {
    List<SubAlarm> expected = Arrays.asList(new SubAlarm("444", "456",
        AlarmSubExpression.of("avg(hpcs.compute{metric_name=cpu}) > 10"), AlarmState.UNDETERMINED));
    List<SubAlarm> subAlarms = dao.find(new MetricDefinition("hpcs.compute",
        new ImmutableMap.Builder<String, String>().put("metric_name", "cpu").build()));
    assertNotEquals(subAlarms, expected);
  }
}
