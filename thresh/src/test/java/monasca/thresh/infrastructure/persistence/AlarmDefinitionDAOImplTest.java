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

package monasca.thresh.infrastructure.persistence;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.google.common.base.Joiner;
import com.google.common.io.Resources;

import monasca.common.model.alarm.AlarmExpression;
import monasca.common.model.alarm.AlarmSubExpression;
import monasca.thresh.domain.model.AlarmDefinition;
import monasca.thresh.domain.model.SubExpression;
import monasca.thresh.domain.service.AlarmDefinitionDAO;

import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Test
public class AlarmDefinitionDAOImplTest {
  private static final String TENANT_ID = "bob";
  private static final String ALARM_DEFINITION_ID = "123";
  private static String ALARM_NAME = "90% CPU";
  private static String ALARM_DESCR = "Description for " + ALARM_NAME;

  private static final Joiner COMMA_JOINER = Joiner.on(',');

  private DBI db;
  private Handle handle;
  private AlarmDefinitionDAO dao;

  @BeforeClass
  protected void setupClass() throws Exception {
    db = new DBI("jdbc:h2:mem:test;MODE=MySQL");
    handle = db.open();
    handle
        .execute(Resources.toString(getClass().getResource("alarm.sql"), Charset.defaultCharset()));
    dao = new AlarmDefinitionDAOImpl(db);
  }

  @AfterClass
  protected void afterClass() {
    handle.close();
  }

  @BeforeMethod
  protected void beforeMethod() {
    handle.execute("truncate table alarm_definition");
    handle.execute("truncate table sub_alarm_definition");
    handle.execute("truncate table sub_alarm_definition_dimension");
  }

  public void testGetById() {
    assertNull(dao.findById(ALARM_DEFINITION_ID));

    final AlarmExpression expression = new AlarmExpression("max(cpu{service=nova}) > 90");
    final AlarmDefinition alarmDefinition =
        new AlarmDefinition(TENANT_ID, ALARM_NAME, ALARM_DESCR, expression, "LOW",
            false, Arrays.asList("fred"));
    insertAndCheck(alarmDefinition);

    final AlarmExpression expression2 = new AlarmExpression("max(cpu{service=swift}) > 90");
    final AlarmDefinition alarmDefinition2 =
        new AlarmDefinition(TENANT_ID, ALARM_NAME, ALARM_DESCR, expression2, "LOW",
            false, Arrays.asList("hostname", "dev"));
    insertAndCheck(alarmDefinition2);

    // Make sure it works when there are no dimensions
    final AlarmExpression expression3 = new AlarmExpression("max(cpu) > 90");
    final AlarmDefinition alarmDefinition3 =
        new AlarmDefinition(TENANT_ID, ALARM_NAME, ALARM_DESCR, expression3, "LOW",
            false, Arrays.asList("hostname", "dev"));
    insertAndCheck(alarmDefinition3);

    final AlarmExpression expression4 = new AlarmExpression("max(cpu,deterministic) > 90");
    final AlarmDefinition alarmDefinition4 =
        new AlarmDefinition(TENANT_ID, ALARM_NAME, ALARM_DESCR, expression4, "LOW",
            false, Arrays.asList("hostname", "dev"));
    insertAndCheck(alarmDefinition4);
  }

  public void testListAll() {
    assertEquals(0, dao.listAll().size());

    final AlarmExpression expression = new AlarmExpression("max(cpu{service=nova}) > 90");
    final AlarmDefinition alarmDefinition =
        new AlarmDefinition(TENANT_ID, ALARM_NAME, ALARM_DESCR, expression, "LOW",
            false, Arrays.asList("fred", "barney"));
    insertAlarmDefinition(handle, alarmDefinition);

    verifyListAllMatches(alarmDefinition);
    final AlarmExpression expression2 = new AlarmExpression("max(cpu{service=swift}) > 90");
    final AlarmDefinition alarmDefinition2 =
        new AlarmDefinition(TENANT_ID, ALARM_NAME, ALARM_DESCR, expression2, "LOW",
            false, Arrays.asList("fred", "barney", "wilma", "betty"));
    insertAlarmDefinition(handle, alarmDefinition2);

    verifyListAllMatches(alarmDefinition, alarmDefinition2);

    final AlarmExpression expression3 = new AlarmExpression(
        "max(cpu{service=swift}, deterministic) > 90"
    );
    final AlarmDefinition alarmDefinition3 =
        new AlarmDefinition(TENANT_ID, ALARM_NAME, ALARM_DESCR, expression3, "LOW",
            false, Arrays.asList("fred", "barney", "wilma", "betty", "scooby", "doo"));
    insertAlarmDefinition(handle, alarmDefinition3);

    verifyListAllMatches(alarmDefinition, alarmDefinition2, alarmDefinition3);
  }

  private void insertAndCheck(final AlarmDefinition alarmDefinition) {
    insertAlarmDefinition(handle, alarmDefinition);

    assertEquals(dao.findById(alarmDefinition.getId()), alarmDefinition);
  }

  private void verifyListAllMatches(final AlarmDefinition... alarmDefinitions) {
    List<AlarmDefinition> found = dao.listAll();
    assertEquals(alarmDefinitions.length, found.size());

    for (AlarmDefinition alarmDef : alarmDefinitions) {
      assertTrue(found.contains(alarmDef));
    }
  }

  // This method is not a test but without this TestNG tries to run it
  @Test(enabled=false)
  public static void insertAlarmDefinition(Handle handle, AlarmDefinition alarmDefinition) {
    try {
      handle.begin();
      handle
          .insert(
              "insert into alarm_definition (id, tenant_id, name, description, severity, expression, match_by, actions_enabled, created_at, updated_at, deleted_at) values (?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW(), NULL)",
              alarmDefinition.getId(),
              alarmDefinition.getTenantId(),
              alarmDefinition.getName(),
              alarmDefinition.getDescription(),
              "LOW",
              alarmDefinition.getAlarmExpression().getExpression(),
              alarmDefinition.getMatchBy().isEmpty() ? null : COMMA_JOINER.join(alarmDefinition
                  .getMatchBy()), alarmDefinition.isActionsEnabled());

      for (final SubExpression subExpression : alarmDefinition.getSubExpressions()) {
        final AlarmSubExpression alarmSubExpr = subExpression.getAlarmSubExpression();
        handle
            .insert(
                "insert into sub_alarm_definition (id, alarm_definition_id, function, metric_name, operator, "
                    + "threshold, period, periods, is_deterministic, created_at, updated_at) "
                    + "values (?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())",
                subExpression.getId(), alarmDefinition.getId(), alarmSubExpr.getFunction().name(),
                alarmSubExpr.getMetricDefinition().name, alarmSubExpr.getOperator().name(),
                alarmSubExpr.getThreshold(), alarmSubExpr.getPeriod(), alarmSubExpr.getPeriods(),
                alarmSubExpr.isDeterministic()
            );
        for (final Map.Entry<String, String> entry : alarmSubExpr.getMetricDefinition().dimensions.entrySet()) {
          handle
              .insert(
                  "insert into sub_alarm_definition_dimension (sub_alarm_definition_id, dimension_name, value) values (?, ?, ?)",
                  subExpression.getId(), entry.getKey(), entry.getValue());
        }
      }
      handle.commit();
    } catch (RuntimeException e) {
      handle.rollback();
      throw e;
    }
  }
}
