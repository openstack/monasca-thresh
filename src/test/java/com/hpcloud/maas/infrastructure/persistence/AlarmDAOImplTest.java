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
import com.hpcloud.maas.domain.model.Alarm;
import com.hpcloud.maas.domain.service.AlarmDAO;

/**
 * @author Jonathan Halterman
 */
@Test
public class AlarmDAOImplTest {
  private DBI db;
  private Handle handle;
  private AlarmDAO repo;
  private Map<String, String> dimensions;

  @BeforeClass
  protected void setupClass() throws Exception {
    db = new DBI("jdbc:h2:mem:test;MODE=MySQL");
    handle = db.open();
    handle.execute(Resources.toString(getClass().getResource("alarm.sql"),
        Charset.defaultCharset()));
    repo = new AlarmDAOImpl(db);

    // Fixtures
    dimensions = new HashMap<String, String>();
    dimensions.put("flavor_id", "777");
    dimensions.put("image_id", "888");
  }

  @AfterClass
  protected void afterClass() {
    handle.close();
  }

  @BeforeMethod
  protected void beforeMethod() {
    handle.execute("truncate table alarm");
    handle.execute("truncate table alarm_dimension");
    handle.execute("truncate table alarm_action");

    handle.execute("insert into alarm (id, tenant_id, name, namespace, metric_type, metric_subject, operator, threshold, state, created_at, updated_at) "
        + "values ('123', '444', '90% CPU', 'compute', 'CPU', '3', 'GTE', 90, 'UNDETERMINED', NOW(), NOW())");
    handle.execute("insert into alarm_dimension values ('123', 'flavor_id', '777')");
    handle.execute("insert into alarm_dimension values ('123', 'image_id', '888')");
  }

  public void shouldFind() {
    List<Alarm> alarms = repo.find();

    Alarm alarm = new Alarm("123", "90% CPU", "compute", "CPU", "3", dimensions, "GTE", 90l);
    assertEquals(alarms, Arrays.asList(alarm));
  }
}
