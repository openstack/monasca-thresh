package com.hpcloud.maas.infrastructure.persistence;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;

import com.hpcloud.maas.common.model.alarm.AggregateFunction;
import com.hpcloud.maas.common.model.alarm.AlarmOperator;
import com.hpcloud.maas.common.model.alarm.AlarmSubExpression;
import com.hpcloud.maas.common.model.metric.MetricDefinition;
import com.hpcloud.maas.domain.model.SubAlarm;
import com.hpcloud.maas.domain.service.SubAlarmDAO;
import com.hpcloud.persistence.SqlQueries;
import com.hpcloud.persistence.SqlStatements;

/**
 * SubAlarm DAO implementation.
 * 
 * @author Jonathan Halterman
 */
public class SubAlarmDAOImpl implements SubAlarmDAO {
  /**
   * This query works by matching the set of actual dimension names and values to those in the
   * table, grouping by the dimension id and counting them to ensure that the number of matched
   * dimensions equals the number of actual dimensions in the table for the subscription.
   */
  private static final String FIND_BY_METRIC_DEF_SQL = "select sa.* from sub_alarm sa, sub_alarm_dimension d "
      + "join (%s) v on d.dimension_name = v.dimension_name and d.value = v.value "
      + "where sa.id = d.sub_alarm_id and sa.namespace = :namespace and sa.metric_type = :metricType and sa.metric_subject = :metricSubject "
      + "group by d.sub_alarm_id having count(d.sub_alarm_id) = (select count(*) from sub_alarm_dimension where sub_alarm_id = d.sub_alarm_id)";

  private final DBI db;

  @Inject
  public SubAlarmDAOImpl(DBI db) {
    this.db = db;
  }

  /**
   * Returns a list of SubAlarms for the complete (select *) set of {@code subAlarmRows}.
   */
  public static List<SubAlarm> subAlarmsForRows(Handle handle,
      List<Map<String, Object>> subAlarmRows) {
    List<SubAlarm> subAlarms = new ArrayList<SubAlarm>(subAlarmRows.size());

    for (Map<String, Object> row : subAlarmRows) {
      String subAlarmId = (String) row.get("id");
      Map<String, String> dimensions = findDimensionsById(handle, subAlarmId);
      AggregateFunction function = AggregateFunction.of((String) row.get("function"));
      MetricDefinition metricDef = new MetricDefinition((String) row.get("namespace"),
          (String) row.get("metric_type"), (String) row.get("metric_subject"), dimensions);
      AlarmOperator operator = AlarmOperator.valueOf((String) row.get("operator"));
      AlarmSubExpression subExpression = new AlarmSubExpression(function, metricDef, operator,
          (Double) row.get("threshold"), (Integer) row.get("period"), (Integer) row.get("periods"));
      SubAlarm subAlarm = new SubAlarm(subAlarmId, (String) row.get("alarm_id"), subExpression);
      subAlarms.add(subAlarm);
    }

    return subAlarms;
  }

  private static Map<String, String> findDimensionsById(Handle handle, String subAlarmId) {
    return SqlQueries.keyValuesFor(handle,
        "select dimension_name, value from sub_alarm_dimension where sub_alarm_id = ?", subAlarmId);
  }

  @Override
  public List<SubAlarm> find(MetricDefinition metricDefinition) {
    Handle h = db.open();

    try {
      String unionAllStatement = SqlStatements.unionAllStatementFor(metricDefinition.dimensions,
          "dimension_name", "value");
      String sql = String.format(FIND_BY_METRIC_DEF_SQL, unionAllStatement);

      return subAlarmsForRows(h, h.createQuery(sql)
          .bind("namespace", metricDefinition.namespace)
          .bind("metricType", metricDefinition.type)
          .bind("metricSubject", metricDefinition.subject)
          .list());
    } finally {
      h.close();
    }
  }
}
