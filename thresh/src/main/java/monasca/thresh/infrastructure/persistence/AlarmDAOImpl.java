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

import com.hpcloud.mon.common.model.alarm.AlarmState;
import com.hpcloud.mon.common.model.alarm.AlarmSubExpression;
import com.hpcloud.mon.common.model.metric.MetricDefinition;
import com.hpcloud.persistence.BeanMapper;

import monasca.thresh.domain.model.Alarm;
import monasca.thresh.domain.model.MetricDefinitionAndTenantId;
import monasca.thresh.domain.model.SubAlarm;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    Handle h = db.open();
    try {
      List<Alarm> alarms =
          h.createQuery("select * from alarm where alarm_definition_id = :id")
              .bind("id", alarmDefinitionId).map(new BeanMapper<Alarm>(Alarm.class)).list();

      for (final Alarm alarm : alarms) {
        alarm.setSubAlarms(subAlarmsForRows(h
            .createQuery("select * from sub_alarm where alarm_id = :alarmId")
            .bind("alarmId", alarm.getId())
            .map(new BeanMapper<SubAlarmCompact>(SubAlarmCompact.class)).list()));

        alarm.setAlarmedMetrics(findAlarmedMetrics(h, alarm.getId()));
      }
      return alarms;
    } finally {
      h.close();
    }
  }

  @Override
  public List<Alarm> listAll() {
    Handle h = db.open();
    try {
      List<Alarm> alarms =
          h.createQuery("select * from alarm").map(new BeanMapper<Alarm>(Alarm.class)).list();

      for (final Alarm alarm : alarms) {
        alarm.setSubAlarms(subAlarmsForRows(h
            .createQuery("select * from sub_alarm where alarm_id = :alarmId")
            .bind("alarmId", alarm.getId())
            .map(new BeanMapper<SubAlarmCompact>(SubAlarmCompact.class)).list()));

        alarm.setAlarmedMetrics(findAlarmedMetrics(h, alarm.getId()));
      }
      return alarms;
    } finally {
      h.close();
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
            "insert into sub_alarm (id, alarm_id, expression, created_at, updated_at) values (?, ?, ?, NOW(), NOW())",
            subAlarm.getId(), subAlarm.getAlarmId(), getExpression(subAlarm.getExpression()));
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

  /**
   * Returns the sub-alarm's expression.
   */
  private String getExpression(final AlarmSubExpression subExpression) {
    StringBuilder sb = new StringBuilder();
    sb.append(subExpression.getFunction()).append('(')
        .append(subExpression.getMetricDefinition().toExpression());
    if (subExpression.getPeriod() != 60)
      sb.append(", ").append(subExpression.getPeriod());
    sb.append(") ").append(subExpression.getOperator()).append(' ')
        .append((int) subExpression.getThreshold());
    if (subExpression.getPeriods() != 1)
      sb.append(" times ").append(subExpression.getPeriods());
    return sb.toString();
  }

  /**
   * Returns a list of SubAlarms for the complete (select *) set of {@code subAlarmRows}.
   */
  private static List<SubAlarm> subAlarmsForRows(List<SubAlarmCompact> rows) {
    List<SubAlarm> subAlarms = new ArrayList<SubAlarm>(rows.size());

    for (SubAlarmCompact row : rows) {
      AlarmSubExpression subExpression = AlarmSubExpression.of(row.expression);
      SubAlarm subAlarm = new SubAlarm(row.id, row.alarmId, subExpression);
      subAlarms.add(subAlarm);
    }

    return subAlarms;
  }

  public static class SubAlarmCompact {
    String id;
    String alarmId;
    String expression;

    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }

    public String getAlarmId() {
      return alarmId;
    }

    public void setAlarmId(String alarmId) {
      this.alarmId = alarmId;
    }

    public String getExpression() {
      return expression;
    }

    public void setExpression(String expression) {
      this.expression = expression;
    }
  }

  @Override
  public Alarm findById(String id) {
    Handle h = db.open();

    try {
      Alarm alarm =
          h.createQuery("select * from alarm where id = :id").bind("id", id)
              .map(new BeanMapper<Alarm>(Alarm.class)).first();
      if (alarm == null) {
        return null;
      }

      alarm.setSubAlarms(subAlarmsForRows(h
          .createQuery("select * from sub_alarm where alarm_id = :alarmId")
          .bind("alarmId", alarm.getId())
          .map(new BeanMapper<SubAlarmCompact>(SubAlarmCompact.class)).list()));

      alarm.setAlarmedMetrics(findAlarmedMetrics(h, id));
      return alarm;
    } finally {
      h.close();
    }
  }

  private Set<MetricDefinitionAndTenantId> findAlarmedMetrics(Handle h, String alarmId) {
    final List<Map<String, Object>> result =
        h.createQuery(
            "select metric_definition_id, metric_dimension_set_id from metric_definition_dimensions "
            + "where id in (select metric_definition_dimensions_id from alarm_metric where alarm_id=:alarm_id)")
            .bind("alarm_id", alarmId).list();
    if ((result == null) || result.isEmpty()) {
      return new HashSet<>(0);
    }
    final Set<byte[]> metricDefinitionIds = new HashSet<>();
    final Set<byte[]> metricDimensionSetIds = new HashSet<>();
    for (final Map<String, Object> row : result) {
      metricDefinitionIds.add((byte[]) row.get("metric_definition_id"));
      metricDimensionSetIds.add((byte[]) row.get("metric_dimension_set_id"));
    }

    final List<Map<String, Object>> metricDefinitionRows =
        queryForIds(h, "select * from metric_definition where id in (%s)", metricDefinitionIds);
    final Map<Sha1HashId, MetricDefinitionAndTenantId> mds = new HashMap<>(metricDefinitionRows.size());
    for (final Map<String, Object> row : metricDefinitionRows) {
      final byte[] id = (byte[]) row.get("id");
      MetricDefinition md = new MetricDefinition();
      md.name = (String) row.get("name");
      mds.put(new Sha1HashId(id), new MetricDefinitionAndTenantId(md, (String) row.get("tenant_id")));
    }

    final List<Map<String, Object>> metricDimensionRows =
        queryForIds(h, "select * from metric_dimension where dimension_set_id in (%s)",
            metricDimensionSetIds);
    final Map<Sha1HashId, Map<String, String>> dims = new HashMap<>(metricDimensionRows.size());
    for (final Map<String, Object> row : metricDimensionRows) {
      final Sha1HashId dimensionSetId = new Sha1HashId((byte[]) row.get("dimension_set_id"));
      Map<String, String> dim = dims.get(dimensionSetId);
      if (dim == null) {
        dim = new HashMap<>();
        dims.put(dimensionSetId, dim);
      }
      final String name = (String) row.get("name");
      final String value = (String) row.get("value");
      dim.put(name, value);
    }
    final Set<MetricDefinitionAndTenantId> alarmedMetrics = new HashSet<>(result.size());
    for (Map<String, Object> row : result) {
      final Sha1HashId metricDefinitionId = new Sha1HashId((byte[]) row.get("metric_definition_id"));
      final MetricDefinitionAndTenantId mdtid = mds.get(metricDefinitionId);
      final Map<String, String> dim =
          dims.get(new Sha1HashId((byte[]) row.get("metric_dimension_set_id")));
      if (dim != null) {
        mdtid.metricDefinition.dimensions = dim;
      } else {
        mdtid.metricDefinition.dimensions = new HashMap<>();
      }
      alarmedMetrics.add(mdtid);
    }
    return alarmedMetrics;
  }

  private List<Map<String, Object>> queryForIds(Handle h, String sql, final Set<byte[]> ids) {
    final String stmt = String.format(sql, createArgString(ids.size()));
    final Query<Map<String, Object>> q = h.createQuery(stmt);
    int index = 0;
    for (Object metric_definition_id : ids) {
      q.bind(String.format("i%d", index++), metric_definition_id);
    }
    final List<Map<String, Object>> metricDefinitionRows = q.list();
    return metricDefinitionRows;
  }

  private String createArgString(final int count) {
    final StringBuilder builder = new StringBuilder();
    for (int i = 0; i < count; i++) {
      if (builder.length() > 0) {
        builder.append(',');
      }
      builder.append(":i");
      builder.append(i);
    }
    return builder.toString();
  }

  @Override
  public void updateState(String id, AlarmState state) {
    Handle h = db.open();

    try {
      h.createStatement("update alarm set state = :state, updated_at = NOW() where id = :id")
          .bind("id", id).bind("state", state.toString()).execute();
    } finally {
      h.close();
    }
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
