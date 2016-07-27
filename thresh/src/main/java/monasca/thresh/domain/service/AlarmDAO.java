/*
 * (C) Copyright 2014-2016 Hewlett Packard Enterprise Development LP
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

package monasca.thresh.domain.service;

import monasca.common.model.alarm.AlarmState;
import monasca.common.model.alarm.AlarmSubExpression;
import monasca.thresh.domain.model.Alarm;
import monasca.thresh.domain.model.MetricDefinitionAndTenantId;

import java.util.List;

/**
 * Alarm DAO.
 */
public interface AlarmDAO {
  /** Finds and returns the Alarm for the {@code id}. */
  Alarm findById(String id);

  /** Finds all Alarms for the given AlarmDefinition */
  List<Alarm> findForAlarmDefinitionId(String alarmDefinitionId);

  /** List all Alarms */
  public List<Alarm> listAll();

  /** Updates the alarm state. */
  void updateState(String id, AlarmState state, long msTimestamp);

  /** Adds a new AlarmedMetric to an Alarm */
  void addAlarmedMetric(String id, MetricDefinitionAndTenantId metricDefinition);

  /** Create a new Alarm */
  void createAlarm(Alarm newAlarm);

  /** Update SubAlarms when AlarmDefinition changes */
  int updateSubAlarmExpressions(final String alarmSubExpressionId, AlarmSubExpression alarmSubExpression);

  /** Deletes all alarms for the given AlarmDefinition */
  void deleteByDefinitionId(String alarmDefinitionId);

  /** Update the state of the given SubAlarm */
  void updateSubAlarmState(String subAlarmId, AlarmState subAlarmState);
}
