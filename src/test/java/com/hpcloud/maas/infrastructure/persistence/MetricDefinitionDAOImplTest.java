package com.hpcloud.maas.infrastructure.persistence;

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
import com.hpcloud.maas.common.model.metric.MetricDefinition;
import com.hpcloud.maas.domain.service.MetricDefinitionDAO;

/**
 * Note: MySQL dependent test.
 * 
 * @author Jonathan Halterman
 */
@Test(groups = "database", enabled = false)
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
    handle.execute("delete from alarm where id = 123");

    handle.execute("insert into alarm (id, tenant_id, name, expression, state, created_at, updated_at) "
        + "values ('123', 'bob', '90% CPU', 'avg(hpcs.compute:cpu:{flavor_id=777, image_id=888}) > 10', 'UNDETERMINED', NOW(), NOW())");
    handle.execute("insert into sub_alarm (id, alarm_id, function, namespace, metric_type, metric_subject, operator, threshold, period, periods, state, created_at, updated_at) "
        + "values ('111', '123', 'AVG', 'hpcs.compute', 'cpu', '1', 'GT', 10, 60, 1, 'OK', NOW(), NOW())");
    handle.execute("insert into sub_alarm (id, alarm_id, function, namespace, metric_type, metric_subject, operator, threshold, period, periods, state, created_at, updated_at) "
        + "values ('222', '123', 'AVG', 'hpcs.compute', 'mem', null, 'GT', 10, 60, 1, 'OK', NOW(), NOW())");
    handle.execute("insert into sub_alarm_dimension values ('111', 'flavor_id', '777')");
    handle.execute("insert into sub_alarm_dimension values ('111', 'image_id', '888')");
    handle.execute("insert into sub_alarm_dimension values ('222', 'instance_id', '123')");
    handle.execute("insert into sub_alarm_dimension values ('222', 'az', '2')");
  }

  public void shouldFindForAlarms() {
    List<MetricDefinition> expected = Arrays.asList(new MetricDefinition("hpcs.compute", "cpu", "1",
        ImmutableMap.<String, String>builder()
            .put("flavor_id", "777")
            .put("image_id", "888")
            .build()), new MetricDefinition("hpcs.compute", "mem", null,
        ImmutableMap.<String, String>builder().put("instance_id", "123").put("az", "2").build()));

    assertTrue(dao.findForAlarms().containsAll(expected));
  }
}
