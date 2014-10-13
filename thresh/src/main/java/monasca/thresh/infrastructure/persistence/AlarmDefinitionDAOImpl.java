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

import monasca.common.model.alarm.AlarmExpression;

import monasca.thresh.domain.model.AlarmDefinition;
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
import java.util.List;

import javax.inject.Inject;

public class AlarmDefinitionDAOImpl implements AlarmDefinitionDAO {
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
      return alarmDefs;
    }
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
              rs.getString("severity"), rs.getBoolean("actions_enabled"), matchBy);
      return real;

    }
  }

  @Override
  public AlarmDefinition findById(String id) {
    try (Handle h = db.open()) {
      AlarmDefinition alarmDefinition =
          h.createQuery("select * from alarm_definition where id = :id and deleted_at is NULL")
              .bind("id", id).map(new AlarmDefinitionMapper()).first();

      return alarmDefinition;
    }
  }
}
