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

import com.google.common.base.Function;

import monasca.common.model.alarm.AggregateFunction;
import monasca.common.model.alarm.AlarmExpression;
import monasca.common.model.alarm.AlarmOperator;
import monasca.common.model.alarm.AlarmSubExpression;
import monasca.common.model.metric.MetricDefinition;
import monasca.common.util.Conversions;
import monasca.thresh.domain.model.AlarmDefinition;
import monasca.thresh.domain.model.SubExpression;
import monasca.thresh.domain.service.AlarmDefinitionDAO;

import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.Query;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;
import javax.inject.Inject;

public class AlarmDefinitionDAOImpl implements AlarmDefinitionDAO {
  private static Function<Object, Boolean> BOOLEAN_MAPPER_FUNCTION = new Function<Object, Boolean>() {
    @Nullable
    @Override
    public Boolean apply(@Nullable final Object input) {
      /*
      For connection established with actual database boolean
      values are returned from database as actual Boolean,
      however unit test are written in H2 and there booleans
      are returned as Byte
       */
      assert input != null;
      if (input instanceof Boolean) {
        return (Boolean) input;
      }
      return "1".equals(input.toString());
    }
  };
  private static final String SUB_ALARM_SQL =
      "select sad.*, sadd.* from sub_alarm_definition sad " +
      "left outer join sub_alarm_definition_dimension sadd on sadd.sub_alarm_definition_id=sad.id " +
      "where sad.alarm_definition_id = :alarmDefId order by sad.id";

  private final DBI db;

  @Inject
  public AlarmDefinitionDAOImpl(DBI db) {
    this.db = db;
  }

  @Override
  public List<AlarmDefinition> listAll() {
    try (Handle h = db.open()) {
      final String sql =
          "select * from alarm_definition where deleted_at is NULL order by created_at";

      final Query<AlarmDefinition> q =
          h.createQuery(sql).map(new AlarmDefinitionMapper());

      final List<AlarmDefinition> alarmDefs = q.list();
      for (final AlarmDefinition alarmDef : alarmDefs) {
        alarmDef.setSubExpressions(findSubExpressions(h, alarmDef.getId()));
      }
      return alarmDefs;
    }
  }

  private List<SubExpression> findSubExpressions(Handle h, String alarmDefId) {
    List<Map<String, Object>> rows =
        h.createQuery(SUB_ALARM_SQL).bind("alarmDefId", alarmDefId).list();
    final List<SubExpression> subExpressions = new ArrayList<>(rows.size());
    int index = 0;
    while (index < rows.size()) {
      Map<String, Object> row = rows.get(index);
      String id = (String) row.get("id");
      AggregateFunction function = AggregateFunction.fromJson((String) row.get("function"));
      String metricName = (String) row.get("metric_name");
      AlarmOperator operator = AlarmOperator.fromJson((String) row.get("operator"));
      Double threshold = (Double) row.get("threshold");
      // MySQL connector returns an Integer, Drizzle returns a Long for period and periods.
      // Need to convert the results appropriately based on type.
      Integer period = Conversions.variantToInteger(row.get("period"));
      Integer periods = Conversions.variantToInteger(row.get("periods"));
      Boolean deterministic = BOOLEAN_MAPPER_FUNCTION.apply(row.get("is_deterministic"));
      Map<String, String> dimensions = new HashMap<>();
      while (addedDimension(dimensions, id, rows, index)) {
        index++;
      }
      subExpressions.add(
          new SubExpression(id,
              new AlarmSubExpression(
                  function,
                  new MetricDefinition(metricName, dimensions),
                  operator,
                  threshold,
                  period,
                  periods,
                  deterministic
              )
          )
      );
    }

    return subExpressions;
  }

  private boolean addedDimension(Map<String, String> dimensions, String id,
      List<Map<String, Object>> rows, int index) {
    if (index >= rows.size()) {
      return false;
    }
    final Map<String, Object> row = rows.get(index);
    if (!row.get("id").equals(id)) {
      return false;
    }
    final String name = (String)row.get("dimension_name");
    final String value = (String)row.get("value");
    if ((name != null) && !name.isEmpty()) {
      dimensions.put(name, value);
    }
    return true;
  }

  private static class AlarmDefinitionMapper implements ResultSetMapper<AlarmDefinition> {
    public AlarmDefinition map(int rowIndex, ResultSet rs, StatementContext ctxt)
        throws SQLException {
      final String matchByString = rs.getString("match_by");
      final List<String> matchBy;
      if (matchByString == null || matchByString.isEmpty()) {
        matchBy = new ArrayList<>(0);
      } else {
        matchBy = new ArrayList<String>(Arrays.asList(matchByString.split(",")));
      }
      final AlarmDefinition real =
          new AlarmDefinition(rs.getString("id"), rs.getString("tenant_id"), rs.getString("name"),
              rs.getString("description"), new AlarmExpression(rs.getString("expression")),
              rs.getString("severity"), rs.getBoolean("actions_enabled"), null, matchBy);
      return real;

    }
  }

  @Override
  public AlarmDefinition findById(String id) {
    try (Handle h = db.open()) {
      AlarmDefinition alarmDefinition =
          h.createQuery("select * from alarm_definition where id = :id and deleted_at is NULL")
              .bind("id", id).map(new AlarmDefinitionMapper()).first();
      if (alarmDefinition != null) {
        alarmDefinition.setSubExpressions(findSubExpressions(h, alarmDefinition.getId()));
      }

      return alarmDefinition;
    }
  }
}
