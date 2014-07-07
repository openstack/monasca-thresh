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

package com.hpcloud.mon.infrastructure.persistence;

import com.hpcloud.mon.common.model.alarm.AggregateFunction;
import com.hpcloud.mon.common.model.alarm.AlarmOperator;
import com.hpcloud.mon.common.model.alarm.AlarmState;
import com.hpcloud.mon.common.model.alarm.AlarmSubExpression;
import com.hpcloud.mon.common.model.metric.MetricDefinition;
import com.hpcloud.mon.domain.model.Alarm;
import com.hpcloud.mon.domain.model.SubAlarm;
import com.hpcloud.mon.domain.service.AlarmDAO;
import com.hpcloud.persistence.BeanMapper;
import com.hpcloud.persistence.SqlQueries;

import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

/**
 * Alarm DAO implementation.
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
      MetricDefinition metricDef =
          new MetricDefinition((String) row.get("metric_name"), dimensions);
      AlarmOperator operator = AlarmOperator.valueOf((String) row.get("operator"));
      AlarmSubExpression subExpression =
          new AlarmSubExpression(function, metricDef, operator, (Double) row.get("threshold"),
              (Integer) row.get("period"), (Integer) row.get("periods"));
      SubAlarm subAlarm = new SubAlarm(subAlarmId, (String) row.get("alarm_id"), subExpression);
      subAlarms.add(subAlarm);
    }

    return subAlarms;
  }

  @Override
  public Alarm findById(String id) {
    Handle h = db.open();

    try {
      Alarm alarm =
          h.createQuery("select * from alarm where id = :id and deleted_at is null").bind("id", id)
              .map(new BeanMapper<Alarm>(Alarm.class)).first();
      if (alarm == null) {
        return null;
      }

      alarm.setSubAlarms(subAlarmsForRows(
          h,
          h.createQuery("select * from sub_alarm where alarm_id = :alarmId")
              .bind("alarmId", alarm.getId()).list()));

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
          .bind("id", id).bind("state", state.toString()).execute();
    } finally {
      h.close();
    }
  }
}
