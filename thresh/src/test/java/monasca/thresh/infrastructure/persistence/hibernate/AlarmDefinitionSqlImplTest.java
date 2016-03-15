/*
 * Copyright 2015 FUJITSU LIMITED
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package monasca.thresh.infrastructure.persistence.hibernate;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import monasca.common.model.alarm.AlarmExpression;
import monasca.thresh.domain.model.AlarmDefinition;
import monasca.thresh.domain.service.AlarmDefinitionDAO;

import org.hibernate.SessionFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Test scenarios for AlarmDefinitionSqlRepoImpl.
 *
 * @author lukasz.zajaczkowski@ts.fujitsu.com
 *
 */
@Test(groups = "orm")
public class AlarmDefinitionSqlImplTest {

  private static final String TENANT_ID = "bob";
  private static final String ALARM_DEFINITION_ID = "123";
  private static String ALARM_NAME = "90% CPU";
  private static String ALARM_DESCR = "Description for " + ALARM_NAME;

  private SessionFactory sessionFactory;
  private AlarmDefinitionDAO dao;

  @BeforeMethod
  protected void setupClass() throws Exception {
    sessionFactory = HibernateUtil.getSessionFactory();
    dao = new AlarmDefinitionSqlImpl(sessionFactory);
  }

  @AfterMethod
  protected void afterMethod() {
    this.sessionFactory.close();
    this.sessionFactory = null;
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
    HibernateUtil.insertAlarmDefinition(sessionFactory.openSession(), alarmDefinition);

    verifyListAllMatches(alarmDefinition);
    final AlarmExpression expression2 = new AlarmExpression("max(cpu{service=swift}) > 90");
    final AlarmDefinition alarmDefinition2 =
        new AlarmDefinition(TENANT_ID, ALARM_NAME, ALARM_DESCR, expression2, "LOW",
            false, Arrays.asList("fred", "barney", "wilma", "betty"));
    HibernateUtil.insertAlarmDefinition(sessionFactory.openSession(), alarmDefinition2);

    verifyListAllMatches(alarmDefinition, alarmDefinition2);

    final AlarmExpression expression3 = new AlarmExpression(
        "max(cpu{service=swift}, deterministic) > 90"
    );
    final AlarmDefinition alarmDefinition3 =
        new AlarmDefinition(TENANT_ID, ALARM_NAME, ALARM_DESCR, expression3, "LOW",
            false, Arrays.asList("fred", "barney", "wilma", "betty", "scooby", "doo"));
    HibernateUtil.insertAlarmDefinition(sessionFactory.openSession(), alarmDefinition3);

    verifyListAllMatches(alarmDefinition, alarmDefinition2, alarmDefinition3);
  }

  private void insertAndCheck(final AlarmDefinition alarmDefinition) {
    HibernateUtil.insertAlarmDefinition(sessionFactory.openSession(), alarmDefinition);
    AlarmDefinition fromDb = dao.findById(alarmDefinition.getId());
    assertEquals(fromDb, alarmDefinition);
  }

  private void verifyListAllMatches(final AlarmDefinition... alarmDefinitions) {
    List<AlarmDefinition> found = dao.listAll();
    assertEquals(alarmDefinitions.length, found.size());

    for (AlarmDefinition alarmDef : alarmDefinitions) {
      assertTrue(found.contains(alarmDef));
    }
  }
}
