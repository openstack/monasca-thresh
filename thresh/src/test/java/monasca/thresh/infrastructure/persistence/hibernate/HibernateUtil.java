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

import java.util.Map;
import java.util.Properties;

import com.google.common.base.Joiner;
import monasca.common.hibernate.db.AlarmActionDb;
import monasca.common.hibernate.db.AlarmDb;
import monasca.common.hibernate.db.AlarmDefinitionDb;
import monasca.common.hibernate.db.AlarmMetricDb;
import monasca.common.hibernate.db.MetricDefinitionDb;
import monasca.common.hibernate.db.MetricDefinitionDimensionsDb;
import monasca.common.hibernate.db.MetricDimensionDb;
import monasca.common.hibernate.db.NotificationMethodDb;
import monasca.common.hibernate.db.SubAlarmDb;
import monasca.common.hibernate.db.SubAlarmDefinitionDb;
import monasca.common.hibernate.db.SubAlarmDefinitionDimensionDb;
import monasca.common.model.alarm.AlarmSeverity;
import monasca.common.model.alarm.AlarmSubExpression;
import monasca.thresh.domain.model.AlarmDefinition;
import monasca.thresh.domain.model.SubExpression;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class HibernateUtil {

  public static final Joiner COMMA_JOINER = Joiner.on(',');
  private static final Logger logger = LoggerFactory.getLogger(HibernateUtil.class);
  private static Configuration CONFIGURATION = null;

  static {
    try {
      Configuration configuration = new Configuration();

      configuration.addAnnotatedClass(AlarmDb.class);
      configuration.addAnnotatedClass(AlarmDefinitionDb.class);
      configuration.addAnnotatedClass(AlarmMetricDb.class);
      configuration.addAnnotatedClass(MetricDefinitionDb.class);
      configuration.addAnnotatedClass(MetricDefinitionDimensionsDb.class);
      configuration.addAnnotatedClass(MetricDimensionDb.class);
      configuration.addAnnotatedClass(SubAlarmDefinitionDb.class);
      configuration.addAnnotatedClass(SubAlarmDefinitionDimensionDb.class);
      configuration.addAnnotatedClass(SubAlarmDb.class);
      configuration.addAnnotatedClass(AlarmActionDb.class);
      configuration.addAnnotatedClass(NotificationMethodDb.class);

      configuration.setProperties(getHikariH2Properties());

      HibernateUtil.CONFIGURATION = configuration;
    } catch (Throwable ex) {
      // Make sure you log the exception, as it might be swallowed
      System.err.println("Initial SessionFactory creation failed." + ex);
      throw new ExceptionInInitializerError(ex);
    }
  }

  private static Properties getHikariH2Properties() {
    Properties properties = new Properties();
    properties.put("hibernate.connection.provider_class", "com.zaxxer.hikari.hibernate.HikariConnectionProvider");
    properties.put("hibernate.hbm2ddl.auto", "create-drop");
    properties.put("show_sql", true);
    properties.put("hibernate.hikari.dataSourceClassName", "org.h2.jdbcx.JdbcDataSource");
    properties.put("hibernate.hikari.dataSource.url", "jdbc:h2:mem:mon;MODE=PostgreSQL");
    properties.put("hibernate.hikari.dataSource.user", "sa");
    properties.put("hibernate.hikari.dataSource.password", "");
    return properties;
  }

  private static Properties getHikariPostgresProperties() {
    Properties properties = new Properties();
    properties.put("hibernate.connection.provider_class", "com.zaxxer.hikari.hibernate.HikariConnectionProvider");
    properties.put("hibernate.hbm2ddl.auto", "validate");
    properties.put("show_sql", true);
    properties.put("hibernate.hikari.dataSourceClassName", "org.postgresql.ds.PGPoolingDataSource");
    properties.put("hibernate.hikari.dataSource.serverName", "localhost");
    properties.put("hibernate.hikari.dataSource.portNumber", "5432");
    properties.put("hibernate.hikari.dataSource.databaseName", "mon");
    properties.put("hibernate.hikari.dataSource.user", "mon");
    properties.put("hibernate.hikari.dataSource.password", "mon");
    properties.put("hibernate.hikari.dataSource.initialConnections", "25");
    properties.put("hibernate.hikari.dataSource.maxConnections", "100");
    properties.put("hibernate.hikari.connectionTestQuery", "SELECT 1");
    return properties;
  }

  public static void insertAlarmDefinition(Session session, AlarmDefinition ad) {
    AlarmDefinitionDb def;

    try {
      session.beginTransaction();
      final DateTime now = DateTime.now();

      def = new AlarmDefinitionDb();
      def.setId(ad.getId());
      def.setTenantId(ad.getTenantId());
      def.setName(ad.getName());
      def.setDescription(ad.getDescription());
      def.setSeverity(AlarmSeverity.LOW);
      def.setExpression(ad.getAlarmExpression().getExpression());
      def.setMatchBy(ad.getMatchBy().isEmpty() ? null : COMMA_JOINER.join(ad.getMatchBy()));
      def.setActionsEnabled(ad.isActionsEnabled());
      def.setCreatedAt(now);
      def.setUpdatedAt(now);
      def.setDeletedAt(null);

      session.save(def);

      for (final SubExpression subExpression : ad.getSubExpressions()) {
        final AlarmSubExpression alarmSubExpr = subExpression.getAlarmSubExpression();
        final SubAlarmDefinitionDb subAlarmDef = new SubAlarmDefinitionDb(
            subExpression.getId(),
            def,
            alarmSubExpr.getFunction().name(),
            alarmSubExpr.getMetricDefinition().name,
            alarmSubExpr.getOperator().name(),
            alarmSubExpr.getThreshold(),
            alarmSubExpr.getPeriod(),
            alarmSubExpr.getPeriods(),
            now,
            now,
            alarmSubExpr.isDeterministic()
        );
        session.save(subAlarmDef);

        for (final Map.Entry<String, String> entry : alarmSubExpr.getMetricDefinition().dimensions.entrySet()) {
          SubAlarmDefinitionDimensionDb dimension = new SubAlarmDefinitionDimensionDb(subAlarmDef, entry.getKey(), entry.getValue());
          session.save(dimension);
        }
      }
      session.getTransaction().commit();
    } catch (RuntimeException e) {
      try {
        session.getTransaction().rollback();
      } catch (RuntimeException rbe) {
        logger.error("Couldn’t roll back transaction", rbe);
      }
      throw e;
    } finally {
      if (session != null) {
        session.close();
      }
    }
  }

  public static SessionFactory getSessionFactory() {
    return CONFIGURATION.buildSessionFactory(
        new StandardServiceRegistryBuilder()
            .applySettings(CONFIGURATION.getProperties())
            .build()
    );
  }
}
