package com.hpcloud.mon.infrastructure.persistence;

import static org.testng.Assert.assertTrue;

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
import com.hpcloud.mon.domain.service.MetricDefinitionDAO;

/**
 * Note: MySQL dependent test.
 * 
 * @author Jonathan Halterman
 */
@Test(groups = "database")
public class MetricDefinitionDAOImplTest {
  private DBI db;
  private Handle handle;
  private MetricDefinitionDAO dao;

  @BeforeClass
  protected void setupClass() throws Exception {
    db = new DBI("jdbc:mysql://localhost/maas", "root", "password");
    handle = db.open();
    dao = new MetricDefinitionDAOImpl(db);
  }

  @AfterClass
  protected void afterClass() {
    handle.close();
  }

  @BeforeMethod
  protected void beforeMethod() {
    handle.execute("delete from sub_alarm where id in (111, 222, 333)");
    handle.execute("delete from sub_alarm_dimension where sub_alarm_id in (111, 222, 333)");

    handle.execute("insert into sub_alarm (id, alarm_id, function, namespace, operator, threshold, period, periods, state, created_at, updated_at) "
        + "values ('111', '123', 'AVG', 'hpcs.compute', 'GT', 10, 60, 1, 'OK', NOW(), NOW())");
    handle.execute("insert into sub_alarm_dimension values ('111', 'metric_name', 'cpu')");
    handle.execute("insert into sub_alarm_dimension values ('111', 'device', '1')");
    handle.execute("insert into sub_alarm_dimension values ('111', 'instance_id', '777')");
    handle.execute("insert into sub_alarm_dimension values ('111', 'image_id', '888')");

    handle.execute("insert into sub_alarm (id, alarm_id, function, namespace, operator, threshold, period, periods, state, created_at, updated_at) "
        + "values ('222', '123', 'AVG', 'hpcs.compute', 'GT', 10, 60, 1, 'OK', NOW(), NOW())");
    handle.execute("insert into sub_alarm_dimension values ('222', 'metric_name', 'mem')");
    handle.execute("insert into sub_alarm_dimension values ('222', 'instance_id', '123')");
    handle.execute("insert into sub_alarm_dimension values ('222', 'az', '2')");

    handle.execute("insert into sub_alarm (id, alarm_id, function, namespace, operator, threshold, period, periods, state, created_at, updated_at) "
        + "values ('333', '123', 'AVG', 'foo', 'GT', 10, 60, 1, 'OK', NOW(), NOW())");
    handle.execute("insert into sub_alarm_dimension values ('333', 'metric_name', 'bar')");
  }

  public void shouldFindForAlarms() {
    MetricDefinition metricDef1 = new MetricDefinition("hpcs.compute",
        ImmutableMap.<String, String>builder()
            .put("metric_name", "cpu")
            .put("device", "1")
            .put("instance_id", "777")
            .build());
    MetricDefinition metricDef2 = new MetricDefinition("hpcs.compute",
        ImmutableMap.<String, String>builder()
            .put("metric_name", "mem")
            .put("az", "2")
            .put("instance_id", "123")
            .build());
    MetricDefinition metricDef3 = new MetricDefinition("foo",
        ImmutableMap.<String, String>builder().put("metric_name", "bar").build());
    List<MetricDefinition> expected = Arrays.asList(metricDef1, metricDef2, metricDef3);

    assertTrue(dao.findForAlarms().containsAll(expected));
  }
}
