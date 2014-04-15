package com.hpcloud.mon.infrastructure.persistence;

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
import com.hpcloud.mon.common.model.alarm.AlarmState;
import com.hpcloud.mon.common.model.alarm.AlarmSubExpression;
import com.hpcloud.mon.common.model.metric.MetricDefinition;
import com.hpcloud.mon.domain.model.MetricDefinitionAndTenantId;
import com.hpcloud.mon.domain.model.SubAlarm;
import com.hpcloud.mon.domain.service.SubAlarmDAO;

/**
 * @author Jonathan Halterman
 */
@Test
public class SubAlarmDAOImplTest {
  private static final String TENANT_ID = "42";
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
    handle.execute("truncate table alarm");
    handle.execute("truncate table sub_alarm");
    handle.execute("truncate table sub_alarm_dimension");

    // These don't have the real Alarm expression because it doesn't matter for this test
    handle.execute("insert into alarm (id, tenant_id, name, description, expression, state, created_at, updated_at) "
            + "values ('123', '" + TENANT_ID + "', 'Test Alarm', 'Test Alarm Description', 'Not real expr', 'OK', NOW(), NOW())");
    handle.execute("insert into alarm (id, tenant_id, name, description, expression, state, created_at, updated_at) "
            + "values ('234', '" + TENANT_ID + "', 'Test Alarm2', 'Test Alarm2 Description', 'Not real expr', 'OK', NOW(), NOW())");
    handle.execute("insert into alarm (id, tenant_id, name, description, expression, state, created_at, updated_at) "
            + "values ('345', '" + TENANT_ID + "', 'Test Alarm3', 'Test Alarm3 Description', 'Not real expr', 'OK', NOW(), NOW())");
    handle.execute("insert into alarm (id, tenant_id, name, description, expression, state, created_at, updated_at) "
            + "values ('456', '" + TENANT_ID + "', 'Test Alarm4', 'Test Alarm4 Description', 'Not real expr', 'OK', NOW(), NOW())");

    handle.execute("insert into sub_alarm (id, alarm_id, function, metric_name, operator, threshold, period, periods, created_at, updated_at) "
        + "values ('111', '123', 'AVG', 'cpu', 'GT', 10, 60, 1, NOW(), NOW())");
    handle.execute("insert into sub_alarm_dimension values ('111', 'instance_id', '555')");
    handle.execute("insert into sub_alarm_dimension values ('111', 'az', '1')");
    handle.execute("insert into sub_alarm_dimension values ('111', 'instance_uuid', '555')");

    handle.execute("insert into sub_alarm (id, alarm_id, function, metric_name, operator, threshold, period, periods, created_at, updated_at) "
        + "values ('222', '234', 'AVG', 'cpu', 'GT', 10, 60, 1, NOW(), NOW())");
    handle.execute("insert into sub_alarm_dimension values ('222', 'instance_id', '666')");
    handle.execute("insert into sub_alarm_dimension values ('222', 'az', '1')");
    handle.execute("insert into sub_alarm_dimension values ('222', 'instance_uuid', '666')");

    handle.execute("insert into sub_alarm (id, alarm_id, function, metric_name, operator, threshold, period, periods, created_at, updated_at) "
        + "values ('333', '345', 'AVG', 'disk', 'GT', 10, 60, 1, NOW(), NOW())");
    handle.execute("insert into sub_alarm_dimension values ('333', 'instance_id', '777')");
    handle.execute("insert into sub_alarm_dimension values ('333', 'az', '1')");
    handle.execute("insert into sub_alarm_dimension values ('333', 'instance_uuid', '777')");
    handle.execute("insert into sub_alarm_dimension values ('333', 'device', 'vda')");

    handle.execute("insert into sub_alarm (id, alarm_id, function, metric_name, operator, threshold, period, periods, created_at, updated_at) "
        + "values ('444', '456', 'AVG', 'cpu', 'GT', 10, 60, 1, NOW(), NOW())");
  }

  public void shouldFind() {
    List<SubAlarm> expected = Arrays.asList(new SubAlarm("111", "123",
        AlarmSubExpression.of("avg(cpu{instance_id=555,az=1}) > 10"),
        AlarmState.UNDETERMINED));
    List<SubAlarm> subAlarms = dao.find(new MetricDefinitionAndTenantId(expected.get(0).getExpression().getMetricDefinition(), TENANT_ID));
    assertEquals(subAlarms, expected);

    expected = Arrays.asList(new SubAlarm("222", "234",
        AlarmSubExpression.of("avg(cpu{instance_id=666,az=1}) > 10"),
        AlarmState.UNDETERMINED));
    subAlarms = dao.find(new MetricDefinitionAndTenantId(expected.get(0).getExpression().getMetricDefinition(), TENANT_ID));
    assertEquals(subAlarms, expected);
  }

  public void shouldNotFind() {
    final String badTenantId = TENANT_ID + "42";
    List<SubAlarm> subAlarms = dao.find(new MetricDefinitionAndTenantId(AlarmSubExpression.of("avg(cpu{instance_id=555,az=1}) > 10").getMetricDefinition(), badTenantId));
    assertEquals(subAlarms.size(), 0);

    subAlarms = dao.find(new MetricDefinitionAndTenantId(AlarmSubExpression.of("avg(cpu{instance_id=666,az=1}) > 10").getMetricDefinition(), badTenantId));
    assertEquals(subAlarms.size(), 0);
  }

  public void shouldFindWithSubject() {
    List<SubAlarm> expected = Arrays.asList(new SubAlarm(
        "333",
        "345",
        AlarmSubExpression.of("avg(disk{instance_id=777,az=1,device=vda}) > 10"),
        AlarmState.UNDETERMINED));
    List<SubAlarm> subAlarms = dao.find(new MetricDefinitionAndTenantId(expected.get(0).getExpression().getMetricDefinition(), TENANT_ID));
    assertEquals(subAlarms, expected);
  }

  public void shouldFindForNullDimensions() {
    List<SubAlarm> expected = Arrays.asList(new SubAlarm("444", "456",
        AlarmSubExpression.of("avg(cpu{}) > 10"), AlarmState.UNDETERMINED));
    List<SubAlarm> subAlarms = dao.find(new MetricDefinitionAndTenantId(new MetricDefinition("cpu", null), TENANT_ID));
    assertEquals(subAlarms, expected);
  }
}
