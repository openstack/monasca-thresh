/*
 * Copyright 2015 FUJITSU LIMITED
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package monasca.thresh.infrastructure.persistence.hibernate;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import monasca.common.hibernate.db.AlarmDefinitionDb;
import monasca.common.hibernate.db.SubAlarmDefinitionDb;
import monasca.common.hibernate.db.SubAlarmDefinitionDimensionDb;
import monasca.common.hibernate.db.SubAlarmDefinitionDimensionId;
import monasca.common.model.alarm.AggregateFunction;
import monasca.common.model.alarm.AlarmExpression;
import monasca.common.model.alarm.AlarmOperator;
import monasca.common.model.alarm.AlarmSubExpression;
import monasca.common.model.metric.MetricDefinition;
import monasca.thresh.domain.model.AlarmDefinition;
import monasca.thresh.domain.model.SubExpression;
import monasca.thresh.domain.service.AlarmDefinitionDAO;
import org.hibernate.ScrollMode;
import org.hibernate.ScrollableResults;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.criterion.DetachedCriteria;
import org.hibernate.criterion.Order;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Property;
import org.hibernate.criterion.Restrictions;

/**
 * AlarmDefinitionDAO hibernate implementation.
 *
 * @author lukasz.zajaczkowski@ts.fujitsu.com
 */
public class AlarmDefinitionSqlImpl
    implements AlarmDefinitionDAO {

  private final SessionFactory sessionFactory;

  @Inject
  public AlarmDefinitionSqlImpl(SessionFactory sessionFactory) {
    this.sessionFactory = sessionFactory;
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<AlarmDefinition> listAll() {

    Session session = null;
    List<AlarmDefinition> alarmDefinitions = null;

    try {
      session = sessionFactory.openSession();
      List<AlarmDefinitionDb> alarmDefDbList = session
          .createCriteria(AlarmDefinitionDb.class)
          .add(Restrictions.isNull("deletedAt"))
          .addOrder(Order.asc("createdAt"))
          .setReadOnly(true)
          .list();

      if (alarmDefDbList != null) {
        alarmDefinitions = Lists.newArrayListWithExpectedSize(alarmDefDbList.size());

        for (final AlarmDefinitionDb alarmDefDb : alarmDefDbList) {
          final Collection<String> matchBy = alarmDefDb.getMatchByAsCollection();
          final boolean actionEnable = alarmDefDb.isActionsEnabled();

          alarmDefinitions.add(new AlarmDefinition(
              alarmDefDb.getId(),
              alarmDefDb.getTenantId(),
              alarmDefDb.getName(),
              alarmDefDb.getDescription(),
              AlarmExpression.of(alarmDefDb.getExpression()),
              alarmDefDb.getSeverity().name(),
              actionEnable,
              this.findSubExpressions(session, alarmDefDb.getId()),
              matchBy.isEmpty() ? Collections.<String>emptyList() : Lists.newArrayList(matchBy)
          ));

          session.evict(alarmDefDb);
        }

      }

      return alarmDefinitions == null ? Collections.<AlarmDefinition>emptyList() : alarmDefinitions;
    } finally {
      if (session != null) {
        session.close();
      }
    }
  }

  @Override
  public AlarmDefinition findById(String id) {
    Session session = null;
    AlarmDefinition alarmDefinition = null;

    try {
      session = sessionFactory.openSession();

      AlarmDefinitionDb alarmDefDb = session.get(AlarmDefinitionDb.class, id);

      if (alarmDefDb != null) {

        final Collection<String> matchBy = alarmDefDb.getMatchByAsCollection();
        final boolean actionEnabled = alarmDefDb.isActionsEnabled();
        final AlarmExpression expression = AlarmExpression.of(alarmDefDb.getExpression());

        alarmDefinition = new AlarmDefinition(
            alarmDefDb.getId(),
            alarmDefDb.getTenantId(),
            alarmDefDb.getName(),
            alarmDefDb.getDescription(),
            expression,
            alarmDefDb.getSeverity().name(),
            actionEnabled,
            this.findSubExpressions(session, id),
            matchBy.isEmpty() ? Collections.<String>emptyList() : Lists.newArrayList(matchBy)
        );

      }

      return alarmDefinition;

    } finally {
      if (session != null) {
        session.close();
      }
    }
  }

  @SuppressWarnings("unchecked")
  private List<SubExpression> findSubExpressions(final Session session, final String alarmDefId) {

    final List<SubExpression> subExpressions = Lists.newArrayList();
    Map<String, Map<String, String>> dimensionMap = Maps.newHashMap();

    final DetachedCriteria subAlarmDefinitionCriteria = DetachedCriteria
        .forClass(SubAlarmDefinitionDb.class, "sad")
        .createAlias("alarmDefinition", "ad")
        .add(
            Restrictions.conjunction(
                Restrictions.eqProperty("sad.alarmDefinition.id", "ad.id"),
                Restrictions.eq("sad.alarmDefinition.id", alarmDefId)
            )
        )
        .addOrder(Order.asc("sad.id"))
        .setProjection(Projections.property("sad.id"));

    final ScrollableResults subAlarmDefinitionDimensionResult = session
        .createCriteria(SubAlarmDefinitionDimensionDb.class)
        .add(
            Property
                .forName("subAlarmDefinitionDimensionId.subExpression.id")
                .in(subAlarmDefinitionCriteria)
        )
        .setReadOnly(true)
        .scroll(ScrollMode.FORWARD_ONLY);

    final ScrollableResults subAlarmDefinitionResult = session
        .getNamedQuery(SubAlarmDefinitionDb.Queries.BY_ALARMDEFINITION_ID)
        .setString("id", alarmDefId)
        .setReadOnly(true)
        .scroll(ScrollMode.FORWARD_ONLY);

    while (subAlarmDefinitionDimensionResult.next()) {

      final SubAlarmDefinitionDimensionDb dim = (SubAlarmDefinitionDimensionDb) subAlarmDefinitionDimensionResult.get()[0];
      final SubAlarmDefinitionDimensionId id = dim.getSubAlarmDefinitionDimensionId();

      final String subAlarmId = (String) session.getIdentifier(id.getSubExpression());
      final String name = id.getDimensionName();
      final String value = dim.getValue();

      if (!dimensionMap.containsKey(subAlarmId)) {
        dimensionMap.put(subAlarmId, Maps.<String, String>newTreeMap());
      }
      dimensionMap.get(subAlarmId).put(name, value);

      session.evict(dim);
    }

    while (subAlarmDefinitionResult.next()) {
      final SubAlarmDefinitionDb def = (SubAlarmDefinitionDb) subAlarmDefinitionResult.get()[0];

      final String id = def.getId();
      final AggregateFunction function = AggregateFunction.fromJson(def.getFunction());
      final String metricName = def.getMetricName();
      final AlarmOperator operator = AlarmOperator.fromJson(def.getOperator());
      final Double threshold = def.getThreshold();
      final Integer period = def.getPeriod();
      final Integer periods = def.getPeriods();
      final Boolean deterministic = def.isDeterministic();

      Map<String, String> dimensions = dimensionMap.get(id);

      if (dimensions == null) {
        dimensions = Collections.emptyMap();
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

      session.evict(def);
    }

    subAlarmDefinitionDimensionResult.close();
    subAlarmDefinitionResult.close();

    return subExpressions;
  }
}
