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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.Query;

import com.hpcloud.mon.common.model.alarm.AggregateFunction;
import com.hpcloud.mon.common.model.alarm.AlarmOperator;
import com.hpcloud.mon.common.model.alarm.AlarmSubExpression;
import com.hpcloud.mon.common.model.metric.MetricDefinition;
import com.hpcloud.mon.domain.model.MetricDefinitionAndTenantId;
import com.hpcloud.mon.domain.model.SubAlarm;
import com.hpcloud.mon.domain.service.SubAlarmDAO;
import com.hpcloud.persistence.SqlStatements;

/**
 * SubAlarm DAO implementation.
 */
public class SubAlarmDAOImpl implements SubAlarmDAO {
  /**
   * This query works by matching the set of actual dimension names and values to those in the
   * table, grouping by the dimension id and counting them to ensure that the number of matched
   * dimensions equals the number of actual dimensions in the table for the subscription.
   */
  private static final String FIND_BY_METRIC_DEF_SQL = "select sa.* from sub_alarm sa, alarm a, sub_alarm_dimension d "
      + "join (%s) v on d.dimension_name = v.dimension_name and d.value = v.value "
      + "where sa.id = d.sub_alarm_id and sa.metric_name = :metric_name and a.tenant_id = :tenant_id and a.id = sa.alarm_id and a.deleted_at is null "
      + "group by d.sub_alarm_id having count(d.sub_alarm_id) = %s";
  private static final String FIND_BY_METRIC_DEF_NO_DIMS_SQL = "select sa.* from sub_alarm sa, alarm a where sa.metric_name = :metric_name "
      + "and a.tenant_id = :tenant_id and a.id = sa.alarm_id and a.deleted_at is null and (select count(*) from sub_alarm_dimension where sub_alarm_id = sa.id) = 0";

  private final DBI db;

  @Inject
  public SubAlarmDAOImpl(DBI db) {
    this.db = db;
  }

  @Override
  public List<SubAlarm> find(MetricDefinitionAndTenantId metricDefinitionTenantId) {
    Handle h = db.open();

    try {
      final MetricDefinition metricDefinition = metricDefinitionTenantId.metricDefinition;
      final String sql;
      if (metricDefinition.dimensions == null || metricDefinition.dimensions.isEmpty())
        sql = FIND_BY_METRIC_DEF_NO_DIMS_SQL;
      else {
        String unionAllStatement = SqlStatements.unionAllStatementFor(metricDefinition.dimensions,
            "dimension_name", "value");
        sql = String.format(FIND_BY_METRIC_DEF_SQL, unionAllStatement,
            metricDefinition.dimensions.size());
      }

      Query<Map<String, Object>> query = h.createQuery(sql).bind("metric_name",
          metricDefinition.name).bind("tenant_id", metricDefinitionTenantId.tenantId);
      List<Map<String, Object>> rows = query.list();

      List<SubAlarm> subAlarms = new ArrayList<SubAlarm>(rows.size());
      for (Map<String, Object> row : rows) {
        String subAlarmId = (String) row.get("id");
        AggregateFunction function = AggregateFunction.valueOf((String) row.get("function"));
        AlarmOperator operator = AlarmOperator.valueOf((String) row.get("operator"));
        AlarmSubExpression subExpression = new AlarmSubExpression(function, metricDefinition,
            operator, (Double) row.get("threshold"), (Integer) row.get("period"),
            (Integer) row.get("periods"));
        SubAlarm subAlarm = new SubAlarm(subAlarmId, (String) row.get("alarm_id"), subExpression);

        subAlarms.add(subAlarm);
      }

      return subAlarms;
    } finally {
      h.close();
    }
  }
}
