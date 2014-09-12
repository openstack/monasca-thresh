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

import com.hpcloud.mon.common.model.alarm.AlarmExpression;
import com.hpcloud.persistence.BeanMapper;

import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.Query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

import monasca.thresh.domain.model.AlarmDefinition;
import monasca.thresh.domain.service.AlarmDefinitionDAO;

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

      final Query<?> q =
          h.createQuery(sql).map(new BeanMapper<AlarmDefinitionHack>(AlarmDefinitionHack.class));

      @SuppressWarnings("unchecked")
      final List<AlarmDefinitionHack> alarmDefHacks = (List<AlarmDefinitionHack>) q.list();
      
      final List<AlarmDefinition> alarmDefs = new ArrayList<>(alarmDefHacks.size());
      for (final AlarmDefinitionHack alarmDefHack : alarmDefHacks) {
        alarmDefs.add(transform(alarmDefHack));
      }
      return alarmDefs;
    }
  }

  public static class AlarmDefinitionHack {
    private String id;
    private String tenantId;
    private String name;
    private String description;
    private String expression;
    private boolean actionsEnabled;
    private String matchBy;
    public String getId() {
      return id;
    }
    public void setId(String id) {
      this.id = id;
    }
    public String getTenantId() {
      return tenantId;
    }
    public void setTenantId(String tenantId) {
      this.tenantId = tenantId;
    }
    public String getName() {
      return name;
    }
    public void setName(String name) {
      this.name = name;
    }
    public String getDescription() {
      return description;
    }
    public void setDescription(String description) {
      this.description = description;
    }
    public String getExpression() {
      return expression;
    }
    public void setExpression(String expression) {
      this.expression = expression;
    }
    public boolean isActionsEnabled() {
      return actionsEnabled;
    }
    public void setActionsEnabled(boolean actionsEnabled) {
      this.actionsEnabled = actionsEnabled;
    }
    public String getMatchBy() {
      return matchBy;
    }
    public void setMatchBy(String matchBy) {
      this.matchBy = matchBy;
    }
  }

  @Override
  public AlarmDefinition findById(String id) {
    try (Handle h = db.open()) {
      AlarmDefinitionHack alarmDefHack =
          h.createQuery("select * from alarm_definition where id = :id and deleted_at is NULL")
              .bind("id", id).map(new BeanMapper<AlarmDefinitionHack>(AlarmDefinitionHack.class)).first();

      if (alarmDefHack == null) {
        return null;
      }
      return transform(alarmDefHack);
    }
  }

  private AlarmDefinition transform(AlarmDefinitionHack alarmDefHack) {
    final List<String> matchBy;
    if (alarmDefHack.getMatchBy() == null || alarmDefHack.getMatchBy().isEmpty()) {
      matchBy = new ArrayList<>(0);
    }
    else {
      matchBy = new ArrayList<String>(Arrays.asList(alarmDefHack.getMatchBy().split(",")));
    }
    final AlarmDefinition real =
        new AlarmDefinition(alarmDefHack.getId(), alarmDefHack.getTenantId(),
            alarmDefHack.getName(), alarmDefHack.getDescription(), new AlarmExpression(
                alarmDefHack.getExpression()), alarmDefHack.isActionsEnabled(), matchBy);
    return real;
  }
}
