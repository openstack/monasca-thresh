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

import monasca.common.model.alarm.AlarmState;
import monasca.common.model.alarm.AlarmSubExpression;
import monasca.common.model.metric.MetricDefinition;
import monasca.thresh.domain.model.Alarm;
import monasca.thresh.domain.model.MetricDefinitionAndTenantId;
import monasca.thresh.domain.model.SubAlarm;
import monasca.thresh.domain.model.SubExpression;
import monasca.thresh.domain.service.AlarmDAO;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.inject.Inject;

/**
 * Alarm DAO implementation.
 */
public class AlarmDAOImpl implements AlarmDAO {
  private static final Logger logger = LoggerFactory.getLogger(AlarmDAOImpl.class);

  public static final int MAX_COLUMN_LENGTH = 255;

  private final DBI db;

  @Inject
  public AlarmDAOImpl(DBI db) {
    this.db = db;
  }

  @Override
  public List<Alarm> findForAlarmDefinitionId(String alarmDefinitionId) {
    return findAlarms("a.alarm_definition_id = :alarmDefinitionId ", "alarmDefinitionId",
        alarmDefinitionId);
  }

  @Override
  public List<Alarm> listAll() {
    return findAlarms("1=1"); // This is basically "true" and gets optimized out
  }

  private List<Alarm> findAlarms(final String additionalWhereClause, String ... params) {
    try (final Handle h = db.open()) {

      final String ALARMS_SQL =
            "select a.id, a.alarm_definition_id, a.state, sa.id as sub_alarm_id, sa.expression, sa.sub_expression_id, ad.tenant_id from alarm a "
          + "inner join sub_alarm sa on sa.alarm_id = a.id "
          + "inner join alarm_definition ad on a.alarm_definition_id = ad.id "
          + "where ad.deleted_at is null and %s "
          + "order by a.id";
      final String sql = String.format(ALARMS_SQL, additionalWhereClause);
      final Query<Map<String, Object>> query = h.createQuery(sql);
      addQueryParameters(query, params);
      final List<Map<String, Object>> rows = query.list();

      final List<Alarm> alarms = new ArrayList<>(rows.size());
      List<SubAlarm> subAlarms = new ArrayList<SubAlarm>();
      String prevAlarmId = null;
      Alarm alarm = null;
      final Map<String, Alarm> alarmMap = new HashMap<>();
      final Map<String, String> tenantIdMap = new HashMap<>();
      for (final Map<String, Object> row : rows) {
        final String alarmId = getString(row, "id");
        if (!alarmId.equals(prevAlarmId)) {
          if (alarm != null) {
            alarm.setSubAlarms(subAlarms);
          }
          alarm = new Alarm();
          alarm.setId(alarmId);
          alarm.setAlarmDefinitionId(getString(row, "alarm_definition_id"));
          alarm.setState(AlarmState.valueOf(getString(row, "state")));
          subAlarms = new ArrayList<SubAlarm>();
          alarms.add(alarm);
          alarmMap.put(alarmId, alarm);
          tenantIdMap.put(alarmId, getString(row, "tenant_id"));
        }
        final SubExpression subExpression =
            new SubExpression(getString(row, "sub_expression_id"), AlarmSubExpression.of(getString(
                row, "expression")));
        final SubAlarm subAlarm =
            new SubAlarm(getString(row, "sub_alarm_id"), alarmId, subExpression);
        subAlarms.add(subAlarm);
        prevAlarmId = alarmId;
      }
      if (alarm != null) {
        alarm.setSubAlarms(subAlarms);
      }
      if (!alarms.isEmpty()) {
        getAlarmedMetrics(h, alarmMap, tenantIdMap, additionalWhereClause, params);
      }
      return alarms;
    }
  }

  private void addQueryParameters(final Query<Map<String, Object>> query, String... params) {
    for (int i = 0; i < params.length;) {
      query.bind(params[i], params[i+1]);
      i += 2;
    }
  }

  private void getAlarmedMetrics(Handle h, final Map<String, Alarm> alarmMap,
      final Map<String, String> tenantIdMap, final String additionalWhereClause, String ... params) {
    final String baseSql = "select a.id, md.name, mdg.dimensions from metric_definition as md "
        + "inner join metric_definition_dimensions as mdd on md.id = mdd.metric_definition_id "
        + "inner join alarm_metric as am on mdd.id = am.metric_definition_dimensions_id "
        + "inner join alarm as a on am.alarm_id = a.id "
        + "left join (select dimension_set_id, name, value, group_concat(name, '=', value) as dimensions "
        + "       from metric_dimension group by dimension_set_id) as mdg on mdg.dimension_set_id = mdd.metric_dimension_set_id where %s";
    final String sql = String.format(baseSql, additionalWhereClause);
    final Query<Map<String, Object>> query = h.createQuery(sql);
    addQueryParameters(query, params);
    final List<Map<String, Object>> metricRows = query.list();
    for (final Map<String, Object> row : metricRows) {
      final String alarmId = getString(row, "id");
      final Alarm alarm = alarmMap.get(alarmId);
      // This shouldn't happen but it is possible an Alarm gets created after the AlarmDefinition is
      // marked deleted and any existing alarms are deleted but before the Threshold Engine gets the
      // AlarmDefinitionDeleted message
      if (alarm == null) {
        continue;
      }
      final MetricDefinition md = createMetricDefinitionFromRow(row);
      alarm.addAlarmedMetric(new MetricDefinitionAndTenantId(md, tenantIdMap.get(alarmId)));
    }
  }

  @Override
  public void addAlarmedMetric(String alarmId, MetricDefinitionAndTenantId metricDefinition) {
    Handle h = db.open();
    try {
      h.begin();
      createAlarmedMetric(h, metricDefinition, alarmId);
      h.commit();
    } catch (RuntimeException e) {
      h.rollback();
      throw e;
    } finally {
      h.close();
    }
  }

  private void createAlarmedMetric(Handle h, MetricDefinitionAndTenantId metricDefinition,
      String alarmId) {
    final Sha1HashId metricDefinitionDimensionId =
        insertMetricDefinitionDimension(h, metricDefinition);

    h.insert("insert into alarm_metric (alarm_id, metric_definition_dimensions_id) values (?, ?)",
        alarmId, metricDefinitionDimensionId.getSha1Hash());
  }

  private Sha1HashId insertMetricDefinitionDimension(Handle h, MetricDefinitionAndTenantId mdtid) {
    final Sha1HashId metricDefinitionId = insertMetricDefinition(h, mdtid);
    final Sha1HashId metricDimensionSetId =
        insertMetricDimensionSet(h, mdtid.metricDefinition.dimensions);
    final byte[] definitionDimensionsIdSha1Hash =
        DigestUtils.sha(metricDefinitionId.toHexString() + metricDimensionSetId.toHexString());
    h.insert(
        "insert into metric_definition_dimensions (id, metric_definition_id, metric_dimension_set_id) values (?, ?, ?)"
            + "on duplicate key update id=id", definitionDimensionsIdSha1Hash,
        metricDefinitionId.getSha1Hash(), metricDimensionSetId.getSha1Hash());
    return new Sha1HashId(definitionDimensionsIdSha1Hash);
  }

  private Sha1HashId insertMetricDimensionSet(Handle h, Map<String, String> dimensions) {
    final byte[] dimensionSetId = calculateDimensionSHA1(dimensions);
    for (final Map.Entry<String, String> entry : dimensions.entrySet()) {
      h.insert("insert into metric_dimension(dimension_set_id, name, value) values (?, ?, ?) "
          + "on duplicate key update dimension_set_id=dimension_set_id", dimensionSetId,
          entry.getKey(), entry.getValue());
    }
    return new Sha1HashId(dimensionSetId);
  }

  private byte[] calculateDimensionSHA1(final Map<String, String> dimensions) {
    // Calculate dimensions sha1 hash id.
    final StringBuilder dimensionIdStringToHash = new StringBuilder("");
    if (dimensions != null) {
      // Sort the dimensions on name and value.
      TreeMap<String, String> dimensionTreeMap = new TreeMap<>(dimensions);
      for (String dimensionName : dimensionTreeMap.keySet()) {
        if (dimensionName != null && !dimensionName.isEmpty()) {
          String dimensionValue = dimensionTreeMap.get(dimensionName);
          if (dimensionValue != null && !dimensionValue.isEmpty()) {
            dimensionIdStringToHash.append(trunc(dimensionName, MAX_COLUMN_LENGTH));
            dimensionIdStringToHash.append(trunc(dimensionValue, MAX_COLUMN_LENGTH));
          }
        }
      }
    }

    final byte[] dimensionIdSha1Hash = DigestUtils.sha(dimensionIdStringToHash.toString());
    return dimensionIdSha1Hash;
  }

  private Sha1HashId insertMetricDefinition(Handle h, MetricDefinitionAndTenantId mdtid) {
    final String region = ""; // TODO We currently don't have region
    final String definitionIdStringToHash =
        trunc(mdtid.metricDefinition.name, MAX_COLUMN_LENGTH)
            + trunc(mdtid.tenantId, MAX_COLUMN_LENGTH) + trunc(region, MAX_COLUMN_LENGTH);
    final byte[] id = DigestUtils.sha(definitionIdStringToHash);
    h.insert("insert into metric_definition(id, name, tenant_id) values (?, ?, ?) " +
             "on duplicate key update id=id", id, mdtid.metricDefinition.name, mdtid.tenantId);
    return new Sha1HashId(id);
  }

  @Override
  public void createAlarm(Alarm alarm) {
    Handle h = db.open();
    try {
      h.begin();
      h.insert(
          "insert into alarm (id, alarm_definition_id, state, created_at, updated_at) values (?, ?, ?, NOW(), NOW())",
          alarm.getId(), alarm.getAlarmDefinitionId(), alarm.getState().toString());

      for (final SubAlarm subAlarm : alarm.getSubAlarms()) {
        h.insert(
            "insert into sub_alarm (id, alarm_id, sub_expression_id, expression, created_at, updated_at) values (?, ?, ?, ?, NOW(), NOW())",
            subAlarm.getId(), subAlarm.getAlarmId(), subAlarm.getAlarmSubExpressionId(), subAlarm
                .getExpression().getExpression());
      }
      for (final MetricDefinitionAndTenantId md : alarm.getAlarmedMetrics()) {
        createAlarmedMetric(h, md, alarm.getId());
      }
      h.commit();
    } catch (RuntimeException e) {
      h.rollback();
      throw e;
    } finally {
      h.close();
    }
  }

  @Override
  public Alarm findById(String id) {
    final List<Alarm> alarms = findAlarms("a.id = :alarm_id ", "alarm_id", id);
    if (alarms.isEmpty()) {
      return null;
    }
    else {
      return alarms.get(0);
    }
  }

  @Override
  public void updateState(String id, AlarmState state) {

    try (final Handle h = db.open()) {
      h.createStatement("update alarm set state = :state, updated_at = NOW() where id = :id")
          .bind("id", id).bind("state", state.toString()).execute();
    }
  }

  @Override
  public int updateSubAlarmExpressions(String alarmSubExpressionId,
      AlarmSubExpression alarmSubExpression) {

    try (final Handle h = db.open()) {
      return h
          .createStatement(
              "update sub_alarm set expression=:expression where sub_expression_id=:alarmSubExpressionId")
          .bind("expression", alarmSubExpression.getExpression())
          .bind("alarmSubExpressionId", alarmSubExpressionId).execute();
    }
  }

  @Override
  public void deleteByDefinitionId(String alarmDefinitionId){
    try (Handle h = db.open()) {
      h.execute("delete from alarm where alarm_definition_id = :id", alarmDefinitionId);
    }
  }

  private MetricDefinition createMetricDefinitionFromRow(final Map<String, Object> row) {
    final Map<String, String> dimensionMap = new HashMap<>();
    final String dimensions = getString(row, "dimensions");
    if (dimensions != null) {
      for (String dimension : dimensions.split(",")) {
        final String[] parsed_dimension = dimension.split("=");
        dimensionMap.put(parsed_dimension[0], parsed_dimension[1]);
      }
    }
    final MetricDefinition md = new MetricDefinition(getString(row, "name"), dimensionMap);
    return md;
  }

  private String getString(final Map<String, Object> row, String fieldName) {
    return (String) row.get(fieldName);
  }
  
  private String trunc(String s, int l) {

    if (s == null) {
      return "";
    } else if (s.length() <= l) {
      return s;
    } else {
      String r = s.substring(0, l);
      logger.warn(
          "Input string exceeded max column length. Truncating input string {} to {} chars", s, l);
      logger.warn("Resulting string {}", r);
      return r;
    }
  }

  /**
   * This class is used when a binary id needs to be used in a map. Just using a byte[] as
   * a key fails because they are not considered as equal because the check is ==
   * @author craigbr
   *
   */

  private static class Sha1HashId {
    private final byte[] sha1Hash;

    public Sha1HashId(byte[] sha1Hash) {
      this.sha1Hash = sha1Hash;
    }

    @Override
    public String toString() {
      return "Sha1HashId{" + "sha1Hash=" + Hex.encodeHexString(sha1Hash) + "}";
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (!(o instanceof Sha1HashId))
        return false;

      Sha1HashId that = (Sha1HashId) o;

      if (!Arrays.equals(sha1Hash, that.sha1Hash))
        return false;

      return true;
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(sha1Hash);
    }

    public byte[] getSha1Hash() {
      return sha1Hash;
    }

    public String toHexString() {
      return Hex.encodeHexString(sha1Hash);
    }
  }
}
