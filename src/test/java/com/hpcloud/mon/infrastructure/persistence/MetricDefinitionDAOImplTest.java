package com.hpcloud.mon.infrastructure.persistence;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertFalse;

import java.util.Arrays;
import java.util.List;

import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.hpcloud.mon.common.model.metric.MetricDefinition;
import com.hpcloud.mon.domain.model.MetricDefinitionAndTenantId;
import com.hpcloud.mon.domain.service.MetricDefinitionDAO;
import com.hpcloud.mon.domain.service.SubAlarmMetricDefinition;

/**
 * Note: MySQL dependent test because of the group_concat() used in the SQL in MetricDefinitionDAOImpl.
 * Depends on the MySQL in mini-mon.
 * 
 * @author Jonathan Halterman
 */
@Test(groups = "database")
public class MetricDefinitionDAOImplTest {
  private static final String TENANT_ID = "42";
  private DBI db;
  private Handle handle;
  private MetricDefinitionDAO dao;
  private List<SubAlarmMetricDefinition> expected;

  @BeforeClass
  protected void setupClass() throws Exception {
    db = new DBI("jdbc:mysql://192.168.10.6/mon", "thresh", "password");
    handle = db.open();
    dao = new MetricDefinitionDAOImpl(db);
  }

  @AfterClass
  protected void afterClass() {
    cleanUp();
    handle.close();
  }

  @BeforeMethod
  protected void beforeMethod() {
    cleanUp();

    handle.execute("insert into alarm (id, tenant_id, name, description, expression, state, enabled, created_at, updated_at) "
            + "values ('123', '" + TENANT_ID + "', 'Test Alarm', 'Test Alarm Description', 'Not real expr', 'OK', '1', NOW(), NOW())");

    handle.execute("insert into sub_alarm (id, alarm_id, function, metric_name, operator, threshold, period, periods, state, created_at, updated_at) "
        + "values ('111', '123', 'AVG', 'cpu', 'GT', 10, 60, 1, 'OK', NOW(), NOW())");
    handle.execute("insert into sub_alarm_dimension values ('111', 'device', '1')");
    handle.execute("insert into sub_alarm_dimension values ('111', 'instance_id', '777')");
    handle.execute("insert into sub_alarm_dimension values ('111', 'image_id', '888')");

    handle.execute("insert into sub_alarm (id, alarm_id, function, metric_name, operator, threshold, period, periods, state, created_at, updated_at) "
        + "values ('222', '123', 'AVG', 'mem', 'GT', 10, 60, 1, 'OK', NOW(), NOW())");
    handle.execute("insert into sub_alarm_dimension values ('222', 'instance_id', '123')");
    handle.execute("insert into sub_alarm_dimension values ('222', 'az', '2')");

    handle.execute("insert into sub_alarm (id, alarm_id, function, metric_name, operator, threshold, period, periods, state, created_at, updated_at) "
        + "values ('333', '123', 'AVG', 'bar', 'GT', 10, 60, 1, 'OK', NOW(), NOW())");
    SubAlarmMetricDefinition metricDef1 = new SubAlarmMetricDefinition("111",
            new MetricDefinitionAndTenantId(new MetricDefinition("cpu",
                  ImmutableMap.<String, String>builder()
                      .put("device", "1")
                      .put("instance_id", "777")
                      .put("image_id", "888")
                      .build()), TENANT_ID));
  SubAlarmMetricDefinition metricDef2 = new SubAlarmMetricDefinition("222",
            new MetricDefinitionAndTenantId(new MetricDefinition("mem",
                  ImmutableMap.<String, String>builder()
                      .put("az", "2")
                      .put("instance_id", "123")
                      .build()), TENANT_ID));
  SubAlarmMetricDefinition metricDef3 = new SubAlarmMetricDefinition("333",
            new MetricDefinitionAndTenantId(new MetricDefinition("bar",
                    null), TENANT_ID));
    expected = Arrays.asList(metricDef1, metricDef2, metricDef3);
  }

  private void cleanUp() {
    handle.execute("delete from sub_alarm where id in (111, 222, 333)");
    handle.execute("delete from sub_alarm_dimension where sub_alarm_id in (111, 222, 333)");
    handle.execute("delete from alarm where id in (123)");
  }

  public void shouldFindForAlarms() {


    List<SubAlarmMetricDefinition> found = dao.findForAlarms();
    for (final SubAlarmMetricDefinition toFind : expected)
      assertTrue(found.contains(toFind), "Did not find " + toFind);
  }

  public void shouldNotFindDisabledAlarms() {
      handle.execute("update alarm set enabled=0 where id in ('123')");

      List<SubAlarmMetricDefinition> found = dao.findForAlarms();
      for (final SubAlarmMetricDefinition toFind : expected)
          assertFalse(found.contains(toFind), "Should not have found " + toFind);
  }

  public void shouldNotFindDeletedAlarms() {
      handle.execute("update alarm set deleted_at=NOW() where id in ('123')");

      List<SubAlarmMetricDefinition> found = dao.findForAlarms();
      for (final SubAlarmMetricDefinition toFind : expected)
          assertFalse(found.contains(toFind), "Should not have found " + toFind);
  }
}
