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
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
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
        alarm.setSubAlarms(getSubAlarms(h, alarm.getId()));

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
        alarm.setSubAlarms(getSubAlarms(h, alarm.getId()));

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

      alarm.setSubAlarms(getSubAlarms(h, alarm.getId()));

      alarm.setAlarmedMetrics(findAlarmedMetrics(h, id));
      return alarm;
    } finally {
      h.close();
    }
  }

  private static class SubAlarmMapper implements ResultSetMapper<SubAlarm>
  {
    public SubAlarm map(int rowIndex, ResultSet rs, StatementContext ctxt) throws SQLException {
      AlarmSubExpression subExpression = AlarmSubExpression.of(rs.getString("expression"));
      return new SubAlarm(rs.getString("id"), rs.getString("alarm_id"), subExpression);
    }   
  }

  private List<SubAlarm> getSubAlarms(Handle h, String alarmId) {
    return h.createQuery("select * from sub_alarm where alarm_id = :alarmId")
        .bind("alarmId", alarmId).map(new SubAlarmMapper()).list();
  }

  private Set<MetricDefinitionAndTenantId> findAlarmedMetrics(Handle h, String alarmId) {
    final List<Map<String, Object>> result =
        h.createQuery(
            "select md.name as metric_name, md.tenant_id, md.region, mdi.name, mdi.value, mdd.id, mdd.metric_dimension_set_id " +
            "from metric_definition_dimensions as mdd left join metric_definition as md on md.id = mdd.metric_definition_id " +
            "left join metric_dimension as mdi on mdi.dimension_set_id = mdd.metric_dimension_set_id where mdd.id in " +
            "(select metric_definition_dimensions_id from alarm_metric where alarm_id=:alarm_id order by mdd.metric_dimension_set_id)")
            .bind("alarm_id", alarmId).list();
    if ((result == null) || result.isEmpty()) {
      return new HashSet<>(0);
    }

    final Set<MetricDefinitionAndTenantId> alarmedMetrics = new HashSet<>(result.size());
    Sha1HashId previous = null;
    MetricDefinitionAndTenantId mdtid = null;
    for (Map<String, Object> row : result) {
      final Sha1HashId next = new Sha1HashId((byte[]) row.get("id"));
      // The order by clause in the SQL guarantees this order
      if (!next.equals(previous)) {
        if (mdtid != null) {
          alarmedMetrics.add(mdtid);
        }
        final String name = (String) row.get("metric_name");
        final String tenantId = (String) row.get("tenant_id");
        mdtid = new MetricDefinitionAndTenantId(new MetricDefinition(name, new HashMap<String, String>()), tenantId);
        previous = next;
      }
      final String name = (String) row.get("name");
      final String value = (String) row.get("value");
      if ((name != null) && !name.isEmpty()) {
        mdtid.metricDefinition.dimensions.put(name, value);
      }
    }
    if (mdtid != null) {
      alarmedMetrics.add(mdtid);
    }
    return alarmedMetrics;
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
