package com.hpcloud.maas.infrastructure.persistence;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;

import com.hpcloud.maas.common.model.alarm.AggregateFunction;
import com.hpcloud.maas.common.model.alarm.AlarmOperator;
import com.hpcloud.maas.common.model.alarm.AlarmState;
import com.hpcloud.maas.common.model.alarm.AlarmSubExpression;
import com.hpcloud.maas.common.model.metric.MetricDefinition;
import com.hpcloud.maas.domain.model.Alarm;
import com.hpcloud.maas.domain.model.SubAlarm;
import com.hpcloud.maas.domain.service.AlarmDAO;
import com.hpcloud.persistence.BeanMapper;
import com.hpcloud.persistence.SqlQueries;

/**
 * Alarm DAO implementation.
 * 
 * @author Jonathan Halterman
 */
public class AlarmDAOImpl implements AlarmDAO {
  private final DBI db;

  @Inject
  public AlarmDAOImpl(DBI db) {
    this.db = db;
  }

  private static Map<String, String> findDimensionsById(Handle handle, String subAlarmId) {
    return SqlQueries.keyValuesFor(handle,
        "select dimension_name, value from sub_alarm_dimension where sub_alarm_id = ?", subAlarmId);
  }

  /**
   * Returns a list of SubAlarms for the complete (select *) set of {@code subAlarmRows}.
   */
  private static List<SubAlarm> subAlarmsForRows(Handle handle,
      List<Map<String, Object>> subAlarmRows) {
    List<SubAlarm> subAlarms = new ArrayList<SubAlarm>(subAlarmRows.size());

    for (Map<String, Object> row : subAlarmRows) {
      String subAlarmId = (String) row.get("id");
      Map<String, String> dimensions = findDimensionsById(handle, subAlarmId);
      AggregateFunction function = AggregateFunction.valueOf((String) row.get("function"));
      MetricDefinition metricDef = new MetricDefinition((String) row.get("namespace"), dimensions);
      // TODO remove later when collectd supports all dimensions
      // CollectdMetrics.removeUnsupportedDimensions(metricDef);
      AlarmOperator operator = AlarmOperator.valueOf((String) row.get("operator"));
      AlarmSubExpression subExpression = new AlarmSubExpression(function, metricDef, operator,
          (Double) row.get("threshold"), (Integer) row.get("period"), (Integer) row.get("periods"));
      SubAlarm subAlarm = new SubAlarm(subAlarmId, (String) row.get("alarm_id"), subExpression);
      subAlarms.add(subAlarm);
    }

    return subAlarms;
  }

  @Override
  public Alarm findById(String id) {
    Handle h = db.open();

    try {
      Alarm alarm = h.createQuery("select * from alarm where id = :id")
          .bind("id", id)
          .map(new BeanMapper<Alarm>(Alarm.class))
          .first();

      alarm.setSubAlarms(subAlarmsForRows(
          h,
          h.createQuery("select * from sub_alarm where alarm_id = :alarmId")
              .bind("alarmId", alarm.getId())
              .list()));

      return alarm;
    } finally {
      h.close();
    }
  }

  @Override
  public void updateState(String id, AlarmState state) {
    Handle h = db.open();

    try {
      h.createStatement("update alarm set state = :state, updated_at = NOW() where id = :id")
          .bind("id", id)
          .bind("state", state.toString())
          .execute();
    } finally {
      h.close();
    }
  }
}
