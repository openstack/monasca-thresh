package com.hpcloud.maas.infrastructure.persistence;

import static org.testng.Assert.assertEquals;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.io.Resources;
import com.hpcloud.maas.common.model.alarm.AlarmState;
import com.hpcloud.maas.common.model.alarm.AlarmSubExpression;
import com.hpcloud.maas.common.model.metric.MetricDefinition;
import com.hpcloud.maas.domain.model.SubAlarm;
import com.hpcloud.maas.domain.service.SubAlarmDAO;

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

    handle.execute("insert into sub_alarm (id, alarm_id, function, namespace, metric_type, metric_subject, operator, threshold, period, periods, created_at, updated_at) "
        + "values ('111', '123', 'AVG', 'hpcs.compute', 'cpu', null, 'GT', 10, 60, 1, NOW(), NOW())");
    handle.execute("insert into sub_alarm_dimension values ('111', 'instance_id', '555')");
    handle.execute("insert into sub_alarm_dimension values ('111', 'az', '1')");
    handle.execute("insert into sub_alarm_dimension values ('111', 'instance_uuid', '555')");

    handle.execute("insert into sub_alarm (id, alarm_id, function, namespace, metric_type, metric_subject, operator, threshold, period, periods, created_at, updated_at) "
        + "values ('222', '234', 'AVG', 'hpcs.compute', 'cpu', null, 'GT', 10, 60, 1, NOW(), NOW())");
    handle.execute("insert into sub_alarm_dimension values ('222', 'instance_id', '666')");
    handle.execute("insert into sub_alarm_dimension values ('222', 'az', '1')");
    handle.execute("insert into sub_alarm_dimension values ('222', 'instance_uuid', '666')");

    handle.execute("insert into sub_alarm (id, alarm_id, function, namespace, metric_type, metric_subject, operator, threshold, period, periods, created_at, updated_at) "
        + "values ('333', '345', 'AVG', 'hpcs.compute', 'disk', 'vda', 'GT', 10, 60, 1, NOW(), NOW())");
    handle.execute("insert into sub_alarm_dimension values ('333', 'instance_id', '777')");
    handle.execute("insert into sub_alarm_dimension values ('333', 'az', '1')");
    handle.execute("insert into sub_alarm_dimension values ('333', 'instance_uuid', '777')");

    handle.execute("insert into sub_alarm (id, alarm_id, function, namespace, metric_type, metric_subject, operator, threshold, period, periods, created_at, updated_at) "
        + "values ('444', '456', 'AVG', 'hpcs.compute', 'cpu', null, 'GT', 10, 60, 1, NOW(), NOW())");
  }

  public void shouldFind() {
    List<SubAlarm> expected = Arrays.asList(new SubAlarm("111", "123",
        AlarmSubExpression.of("avg(hpcs.compute:cpu:{instance_id=555,az=1}) > 10"),
        AlarmState.UNDETERMINED));
    List<SubAlarm> subAlarms = dao.find(expected.get(0).getExpression().getMetricDefinition());
    assertEquals(subAlarms, expected);

    expected = Arrays.asList(new SubAlarm("222", "234",
        AlarmSubExpression.of("avg(hpcs.compute:cpu:{instance_id=666,az=1}) > 10"),
        AlarmState.UNDETERMINED));
    subAlarms = dao.find(expected.get(0).getExpression().getMetricDefinition());
    assertEquals(subAlarms, expected);
  }

  public void shouldFindWithSubject() {
    List<SubAlarm> expected = Arrays.asList(new SubAlarm("333", "345",
        AlarmSubExpression.of("avg(hpcs.compute:disk:vda:{instance_id=777,az=1}) > 10"),
        AlarmState.UNDETERMINED));
    List<SubAlarm> subAlarms = dao.find(expected.get(0).getExpression().getMetricDefinition());
    assertEquals(subAlarms, expected);
  }

  public void shouldFindForNullDimensions() {
    List<SubAlarm> expected = Arrays.asList(new SubAlarm("444", "456",
        AlarmSubExpression.of("avg(hpcs.compute:cpu) > 10"), AlarmState.UNDETERMINED));
    List<SubAlarm> subAlarms = dao.find(new MetricDefinition("hpcs.compute", "cpu", null, null));
    assertEquals(subAlarms, expected);
  }
}
