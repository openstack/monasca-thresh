/*
 * Copyright 2016 FUJITSU LIMITED
 * (C) Copyright 2016 Hewlett Packard Enterprise Development LP
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

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.inject.Inject;

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.codec.digest.DigestUtils;
import org.hibernate.Criteria;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.StatelessSession;
import org.hibernate.Transaction;
import org.hibernate.criterion.Order;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import monasca.common.hibernate.db.AlarmDb;
import monasca.common.hibernate.db.AlarmDefinitionDb;
import monasca.common.hibernate.db.AlarmMetricDb;
import monasca.common.hibernate.db.MetricDefinitionDb;
import monasca.common.hibernate.db.MetricDefinitionDimensionsDb;
import monasca.common.hibernate.db.MetricDimensionDb;
import monasca.common.hibernate.db.SubAlarmDb;
import monasca.common.hibernate.db.SubAlarmDefinitionDb;
import monasca.common.hibernate.type.BinaryId;
import monasca.common.model.alarm.AlarmState;
import monasca.common.model.alarm.AlarmSubExpression;
import monasca.common.model.metric.MetricDefinition;
import monasca.thresh.domain.model.Alarm;
import monasca.thresh.domain.model.MetricDefinitionAndTenantId;
import monasca.thresh.domain.model.SubAlarm;
import monasca.thresh.domain.model.SubExpression;
import monasca.thresh.domain.service.AlarmDAO;

/**
 * AlarmDAO hibernate implementation.
 *
 * @author lukasz.zajaczkowski@ts.fujitsu.com
 * @author tomasz.trebski@ts.fujitsu.com
 */
public class AlarmSqlImpl
    implements AlarmDAO {
  private static final Logger LOGGER = LoggerFactory.getLogger(AlarmSqlImpl.class);
  private static final int ALARM_ID = 0;
  private static final int ALARM_DEFINITION_ID = 1;
  private static final int ALARM_STATE = 2;
  private static final int SUB_ALARM_ID = 3;
  private static final int ALARM_EXPRESSION = 4;
  private static final int SUB_EXPRESSION_ID = 5;
  private static final int TENANT_ID = 6;
  private static final int MAX_COLUMN_LENGTH = 255;
  private final SessionFactory sessionFactory;

  @Inject
  public AlarmSqlImpl(SessionFactory sessionFactory) {
    this.sessionFactory = sessionFactory;
  }

  @Override
  public Alarm findById(final String id) {
    final List<Alarm> alarms = this
        .findAlarms(new LookupHelper() {

          @Override
          public Criteria apply(@Nonnull final Criteria input) {
            return input.add(Restrictions.eq("a.id", id));
          }

          @Override
          public Query apply(@Nonnull final Query query) {
            return query.setParameter("alarmId", id);
          }

          @Override
          public String formatHQL(@Nonnull final String hqlQuery) {
            return String.format(hqlQuery, "a.id=:alarmId");
          }
        });

    return alarms.isEmpty() ? null : alarms.get(0);
  }

  @Override
  public List<Alarm> findForAlarmDefinitionId(final String alarmDefinitionId) {
    return this.findAlarms(new LookupHelper() {
      @Override
      public Criteria apply(@Nonnull final Criteria input) {
        return input.add(Restrictions.eq("a.alarmDefinition.id", alarmDefinitionId));
      }

      @Override
      public Query apply(@Nonnull final Query query) {
        return query.setParameter("alarmDefinitionId", alarmDefinitionId);
      }

      @Override
      public String formatHQL(@Nonnull final String hqlQuery) {
        return String.format(hqlQuery, "a.alarmDefinition.id=:alarmDefinitionId");
      }
    });
  }

  @Override
  public List<Alarm> listAll() {
    return this.findAlarms(LookupHelper.NOOP_HELPER);
  }

  @Override
  public void updateState(String id, AlarmState state, long msTimestamp) {
    Transaction tx = null;
    Session session = null;
    try {

      session = sessionFactory.openSession();
      tx = session.beginTransaction();
      final DateTime dt = new DateTime(msTimestamp);

      final AlarmDb alarm = (AlarmDb) session.get(AlarmDb.class, id);
      alarm.setState(state);
      alarm.setUpdatedAt(dt);
      alarm.setStateUpdatedAt(dt);

      session.update(alarm);

      tx.commit();
      tx = null;

    } finally {
      this.rollbackIfNotNull(tx);
      if (session != null) {
        session.close();
      }
    }
  }

  @Override
  public void addAlarmedMetric(String id, MetricDefinitionAndTenantId metricDefinition) {
    Transaction tx = null;
    Session session = null;
    try {

      session = sessionFactory.openSession();
      tx = session.beginTransaction();
      this.createAlarmedMetric(session, metricDefinition, id);
      tx.commit();
      tx = null;

    } finally {
      this.rollbackIfNotNull(tx);
      if (session != null) {
        session.close();
      }
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public void createAlarm(Alarm newAlarm) {
    Transaction tx = null;
    Session session = null;
    try {
      session = sessionFactory.openSession();
      tx = session.beginTransaction();

      final DateTime now = DateTime.now();
      final AlarmDb alarm = new AlarmDb(
          newAlarm.getId(),
          session.get(AlarmDefinitionDb.class, newAlarm.getAlarmDefinitionId()),
          newAlarm.getState(),
          null,
          null,
          now,
          now,
          now
      );

      session.save(alarm);

      for (final SubAlarm subAlarm : newAlarm.getSubAlarms()) {
        session.save(new SubAlarmDb()
                .setAlarm(alarm)
                .setSubExpression(session.get(SubAlarmDefinitionDb.class, subAlarm.getAlarmSubExpressionId()))
                .setExpression(subAlarm.getExpression().getExpression())
                .setUpdatedAt(now)
                .setCreatedAt(now)
                .setId(subAlarm.getId())
        );
      }

      for (final MetricDefinitionAndTenantId md : newAlarm.getAlarmedMetrics()) {
        this.createAlarmedMetric(session, md, newAlarm.getId());
      }

      tx.commit();
      tx = null;

    } finally {
      this.rollbackIfNotNull(tx);
      if (session != null) {
        session.close();
      }
    }
  }

  @Override
  public int updateSubAlarmExpressions(String alarmSubExpressionId, AlarmSubExpression alarmSubExpression) {
    Transaction tx = null;
    Session session = null;
    int updatedItems;

    try {
      session = sessionFactory.openSession();
      tx = session.beginTransaction();

      updatedItems = session
          .getNamedQuery(SubAlarmDb.Queries.UPDATE_EXPRESSION_BY_SUBEXPRESSION_ID)
          .setString("expression", alarmSubExpression.getExpression())
          .setString("alarmSubExpressionId", alarmSubExpressionId)
          .executeUpdate();

      tx.commit();
      tx = null;

      return updatedItems;
    } finally {
      this.rollbackIfNotNull(tx);
      if (session != null) {
        session.close();
      }
    }

  }

  @Override
  public void deleteByDefinitionId(final String alarmDefinitionId) {
    Transaction tx = null;
    Session session = null;

    try {
      session = sessionFactory.openSession();
      tx = session.beginTransaction();

      session
          .getNamedQuery(AlarmDb.Queries.DELETE_BY_ALARMDEFINITION_ID)
          .setString("alarmDefinitionId", alarmDefinitionId)
          .executeUpdate();

      tx.commit();
      tx = null;

    } finally {
      this.rollbackIfNotNull(tx);
      if (session != null) {
        session.close();
      }
    }

  }

  @SuppressWarnings("unchecked")
  private List<Alarm> findAlarms(@Nonnull final LookupHelper lookupHelper) {
    StatelessSession session = null;

    try {
      session = sessionFactory.openStatelessSession();
      final Criteria criteria = lookupHelper.apply(session
              .createCriteria(AlarmDb.class, "a")
              .createAlias("a.subAlarms", "sa")
              .createAlias("a.alarmDefinition", "ad")
              .add(Restrictions.isNull("ad.deletedAt"))
              .addOrder(Order.asc("a.id"))
              .setProjection(
                  Projections.projectionList()
                      .add(Projections.property("a.id"))
                      .add(Projections.property("a.alarmDefinition.id"))
                      .add(Projections.property("a.state"))
                      .add(Projections.alias(Projections.property("sa.id"), "sub_alarm_id"))
                      .add(Projections.property("sa.expression"))
                      .add(Projections.property("sa.subExpression.id"))
                      .add(Projections.property("ad.tenantId"))
              )
              .setReadOnly(true)
      );
      assert criteria != null;
      return this.createAlarms(session, (List<Object[]>) criteria.list(), lookupHelper);
    } finally {
      if (session != null) {
        session.close();
      }
    }
  }

  private Map<String, List<MetricDefinition>> getAlarmedMetrics(List<Object[]> alarmList) {

    Map<String, List<MetricDefinition>> result = Maps.newHashMap();
    Map<BinaryId, List<MetricDefinition>> metricDefinitionList = Maps.newHashMap();
    Map<BinaryId, Map<String, Map<String, String>>> metricList = Maps.newHashMap();
    Map<String, Set<BinaryId>> mapAssociationIds = Maps.newHashMap();

    for (Object[] alarmRow : alarmList) {
      String alarmId = (String) alarmRow[0];
      String metric_name = (String) alarmRow[1];
      String dimension_name = (String) alarmRow[2];
      String dimension_value = (String) alarmRow[3];
      BinaryId dimensionSetId = (BinaryId) alarmRow[4];

      if (!metricList.containsKey(dimensionSetId)) {
        metricList.put(dimensionSetId, new HashMap<String, Map<String, String>>());
      }
      Map<String, Map<String, String>> dimensions = metricList.get(dimensionSetId);
      if (!dimensions.containsKey(metric_name)) {
        dimensions.put(metric_name, new HashMap<String, String>());
      }
      if (!mapAssociationIds.containsKey(alarmId)) {
        mapAssociationIds.put(alarmId, new HashSet<BinaryId>());
      }
      mapAssociationIds.get(alarmId).add(dimensionSetId);
      dimensions.get(metric_name).put(dimension_name, dimension_value);
    }

    for (BinaryId keyDimensionSetId : metricList.keySet()) {
      Map<String, Map<String, String>> metrics = metricList.get(keyDimensionSetId);
      List<MetricDefinition> valueList = Lists.newArrayListWithExpectedSize(metrics.size());
      for (String keyMetricName : metrics.keySet()) {
        MetricDefinition md = new MetricDefinition(keyMetricName, metrics.get(keyMetricName));
        valueList.add(md);
      }
      metricDefinitionList.put(keyDimensionSetId, valueList);
    }

    for (String keyAlarmId : mapAssociationIds.keySet()) {
      if (!result.containsKey(keyAlarmId)) {
        result.put(keyAlarmId, new LinkedList<MetricDefinition>());
      }
      Set<BinaryId> setDimensionId = mapAssociationIds.get(keyAlarmId);
      for (BinaryId keyDimensionId : setDimensionId) {
        List<MetricDefinition> metricDefList = metricDefinitionList.get(keyDimensionId);
        result.get(keyAlarmId).addAll(metricDefList);

      }
    }

    return result;
  }

  private List<Alarm> createAlarms(final StatelessSession session,
                                   final List<Object[]> alarmList,
                                   final LookupHelper lookupHelper) {
    final List<Alarm> alarms = Lists.newArrayListWithCapacity(alarmList.size());

    if (alarmList.isEmpty()) {
      return alarms;
    }

    List<SubAlarm> subAlarms = null;

    String prevAlarmId = null;
    Alarm alarm = null;

    final Map<String, Alarm> alarmMap = Maps.newHashMapWithExpectedSize(alarmList.size());
    final Map<String, String> tenantIdMap = Maps.newHashMapWithExpectedSize(alarmList.size());

    for (Object[] alarmRow : alarmList) {
      final String alarmId = (String) alarmRow[ALARM_ID];
      if (!alarmId.equals(prevAlarmId)) {
        if (alarm != null) {
          alarm.setSubAlarms(subAlarms);
        }
        alarm = new Alarm();
        alarm.setId(alarmId);
        alarm.setAlarmDefinitionId((String) alarmRow[ALARM_DEFINITION_ID]);
        alarm.setState((AlarmState) alarmRow[ALARM_STATE]);
        subAlarms = Lists.newArrayListWithExpectedSize(alarmList.size());
        alarms.add(alarm);
        alarmMap.put(alarmId, alarm);
        tenantIdMap.put(alarmId, (String) alarmRow[TENANT_ID]);
      }

      subAlarms.add(new SubAlarm(
          (String) alarmRow[SUB_ALARM_ID],
          alarmId,
          new SubExpression(
              (String) alarmRow[SUB_EXPRESSION_ID],
              AlarmSubExpression.of((String) alarmRow[ALARM_EXPRESSION])
          )
      ));

      prevAlarmId = alarmId;
    }

    if (alarm != null) {
      alarm.setSubAlarms(subAlarms);
    }

    if (!alarms.isEmpty()) {
      this.getAlarmedMetrics(session, alarmMap, tenantIdMap, lookupHelper);
    }

    return alarms;
  }

  @SuppressWarnings("unchecked")
  private void getAlarmedMetrics(final StatelessSession session,
                                 final Map<String, Alarm> alarmMap,
                                 final Map<String, String> tenantIdMap,
                                 final LookupHelper binder) {

    String rawHQLQuery =
        "select a.id, md.name as metric_def_name, mdg.id.name, mdg.value, mdg.id.dimensionSetId from MetricDefinitionDb as md, "
            + "MetricDefinitionDimensionsDb as mdd, " + "AlarmMetricDb as am, " + "AlarmDb as a, "
            + "MetricDimensionDb as mdg where md.id = mdd.metricDefinition.id and mdd.id = am.alarmMetricId.metricDefinitionDimensions.id and "
            + "am.alarmMetricId.alarm.id = a.id and mdg.id.dimensionSetId = mdd.metricDimensionSetId and %s";

    final Query query = binder.apply(session.createQuery(binder.formatHQL(rawHQLQuery)));
    final List<Object[]> metricRows = query.list();
    final HashSet<String> existingAlarmId = Sets.newHashSet();
    final Map<String, List<MetricDefinition>> alarmMetrics = this.getAlarmedMetrics(metricRows);

    for (final Object[] row : metricRows) {
      final String alarmId = (String) row[ALARM_ID];
      final Alarm alarm = alarmMap.get(alarmId);
      // This shouldn't happen but it is possible an Alarm gets created after the AlarmDefinition is
      // marked deleted and any existing alarms are deleted but before the Threshold Engine gets the
      // AlarmDefinitionDeleted message
      if (alarm == null) {
        continue;
      }
      if (!existingAlarmId.contains(alarmId)) {
        List<MetricDefinition> mdList = alarmMetrics.get(alarmId);
        for (MetricDefinition md : mdList) {
          alarm.addAlarmedMetric(new MetricDefinitionAndTenantId(md, tenantIdMap.get(alarmId)));
        }

      }
      existingAlarmId.add(alarmId);
    }
  }

  private AlarmMetricDb createAlarmedMetric(final Session session,
                                            final MetricDefinitionAndTenantId metricDefinition,
                                            final String alarmId) {
    final MetricDefinitionDimensionsDb metricDefinitionDimension = this.insertMetricDefinitionDimension(session, metricDefinition);
    final AlarmDb alarm = (AlarmDb) session.load(AlarmDb.class, alarmId);
    final AlarmMetricDb alarmMetric = new AlarmMetricDb(alarm, metricDefinitionDimension);

    session.save(alarmMetric);

    return alarmMetric;
  }

  private MetricDefinitionDimensionsDb insertMetricDefinitionDimension(final Session session,
                                                                       final MetricDefinitionAndTenantId mdtId) {
    final MetricDefinitionDb metricDefinition = this.insertMetricDefinition(session, mdtId);
    final BinaryId metricDimensionSetId = this.insertMetricDimensionSet(session, mdtId.metricDefinition.dimensions);
    final byte[] definitionDimensionsIdSha1Hash = DigestUtils.sha(
        metricDefinition.getId().toHexString() + metricDimensionSetId.toHexString()
    );
    final MetricDefinitionDimensionsDb metricDefinitionDimensions = new MetricDefinitionDimensionsDb(
        definitionDimensionsIdSha1Hash,
        metricDefinition,
        metricDimensionSetId
    );
    return (MetricDefinitionDimensionsDb) session.merge(metricDefinitionDimensions);
  }

  private BinaryId insertMetricDimensionSet(Session session, Map<String, String> dimensions) {
    final byte[] dimensionSetId = calculateDimensionSHA1(dimensions);
    for (final Map.Entry<String, String> entry : dimensions.entrySet()) {

      final MetricDimensionDb metricDimension = new MetricDimensionDb(dimensionSetId, entry.getKey(), entry.getValue());

      if (session.get(MetricDimensionDb.class, metricDimension.getId()) == null) {
        session.merge(metricDimension);
      }

    }

    return new BinaryId(dimensionSetId);
  }

  private MetricDefinitionDb insertMetricDefinition(final Session session,
                                                    final MetricDefinitionAndTenantId mdtid) {

    final String region = ""; // TODO We currently don't have region
    final String definitionIdStringToHash =
        truncateString(mdtid.metricDefinition.name, MAX_COLUMN_LENGTH) +
            truncateString(mdtid.tenantId, MAX_COLUMN_LENGTH) +
            truncateString(region, MAX_COLUMN_LENGTH);
    final byte[] id = DigestUtils.sha(definitionIdStringToHash);
    final MetricDefinitionDb metricDefinition = new MetricDefinitionDb(id, mdtid.metricDefinition.name, mdtid.tenantId, region);

    if (session.get(MetricDefinitionDb.class, metricDefinition.getId()) == null) {
      session.persist(metricDefinition);
      return metricDefinition;
    }

    session.merge(metricDefinition);
    return metricDefinition;
  }

  private String truncateString(String s, int l) {
    if (s == null) {
      return "";
    } else if (s.length() <= l) {
      return s;
    } else {
      String r = s.substring(0, l);
      LOGGER.warn("Input string exceeded max column length. Truncating input string {} to {} chars", s, l);
      LOGGER.warn("Resulting string {}", r);
      return r;
    }
  }

  private byte[] calculateDimensionSHA1(final Map<String, String> dimensions) {
    // Calculate dimensions sha1 hash id.
    final StringBuilder dimensionIdStringToHash = new StringBuilder("");
    if (dimensions != null && !dimensions.isEmpty()) {
      // Sort the dimensions on name and value.
      final Map<String, String> dimensionTreeMap = Maps.newTreeMap(ImmutableSortedMap.copyOf(dimensions));
      for (final String dimensionName : dimensionTreeMap.keySet()) {
        if (dimensionName != null && !dimensionName.isEmpty()) {
          final String dimensionValue = dimensionTreeMap.get(dimensionName);
          if (dimensionValue != null && !dimensionValue.isEmpty()) {
            dimensionIdStringToHash
                .append(this.truncateString(dimensionName, MAX_COLUMN_LENGTH))
                .append(this.truncateString(dimensionValue, MAX_COLUMN_LENGTH));
          }
        }
      }
    }
    return DigestUtils.sha(dimensionIdStringToHash.toString());
  }

  /**
   * Rollbacks passed {@code tx} transaction if such is not null.
   * Assumption is being made that {@code tx} being null means transaction
   * has been successfully comitted.
   *
   * @param tx {@link Transaction} object
   */
  private void rollbackIfNotNull(final Transaction tx) {
    if (tx != null) {
      try {
        tx.rollback();
      } catch (RuntimeException rbe) {
        LOGGER.error("Couldn’t roll back transaction", rbe);
      }
    }
  }

  private static class LookupHelper {

    static final LookupHelper NOOP_HELPER = new LookupHelper();


    public Criteria apply(@Nonnull final Criteria input) {
      return input; // by default we do nothing
    }

    public Query apply(@Nonnull final Query query) {
      return query;
    }

    public String formatHQL(@Nonnull final String hqlQuery) {
      return String.format(hqlQuery, "1=1"); // by default no formatting
    }

  }
}
