package com.hpcloud.maas.infrastructure.persistence;

import static org.testng.Assert.assertEquals;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
        + "values ('111', '123', 'AVG', 'compute', 'cpu', '1', 'GT', 10, 60, 1, NOW(), NOW())");
    handle.execute("insert into sub_alarm_dimension values ('111', 'flavor_id', '777')");
    handle.execute("insert into sub_alarm_dimension values ('111', 'image_id', '888')");

    handle.execute("insert into sub_alarm (id, alarm_id, function, namespace, metric_type, metric_subject, operator, threshold, period, periods, created_at, updated_at) "
        + "values ('222', '456', 'AVG', 'compute', 'cpu', '1', 'GTE', 20, 60, 1, NOW(), NOW())");
    handle.execute("insert into sub_alarm_dimension values ('222', 'flavor_id', '777')");
    handle.execute("insert into sub_alarm_dimension values ('222', 'image_id', '888')");

    handle.execute("insert into sub_alarm (id, alarm_id, function, namespace, metric_type, metric_subject, operator, threshold, period, periods, created_at, updated_at) "
        + "values ('333', '456', 'AVG', 'compute', 'cpu', '1', 'LT', 10, 60, 1, NOW(), NOW())");
    handle.execute("insert into sub_alarm_dimension values ('333', 'flavor_id', '777')");
    handle.execute("insert into sub_alarm_dimension values ('333', 'image_id', '999999')");
  }

  public void shouldFind() {
    List<SubAlarm> expected = Arrays.asList(
        new SubAlarm("111", "123",
            AlarmSubExpression.of("avg(compute:cpu:1:{flavor_id=777,image_id=888}) > 10"),
            AlarmState.UNDETERMINED),
        new SubAlarm("222", "456",
            AlarmSubExpression.of("avg(compute:cpu:1:{flavor_id=777,image_id=888}) >= 20"),
            AlarmState.UNDETERMINED));

    Map<String, String> dimensions = new HashMap<String, String>();
    dimensions.put("flavor_id", "777");
    dimensions.put("image_id", "888");

    List<SubAlarm> subAlarms = dao.find(new MetricDefinition("compute", "cpu", "1", dimensions));

    assertEquals(subAlarms, expected);
  }
}
