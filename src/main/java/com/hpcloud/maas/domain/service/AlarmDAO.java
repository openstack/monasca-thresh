package com.hpcloud.maas.domain.service;

import java.util.List;

import com.hpcloud.maas.common.model.metric.MetricDefinition;
import com.hpcloud.maas.domain.model.SubAlarm;
import com.hpcloud.maas.domain.model.Alarm;

/**
 * Alarm DAO.
 * 
 * @author Jonathan Halterman
 */
public interface AlarmDAO {
  /** Finds and returns all alarm components for the {@code metricDefinition}. */
  List<SubAlarm> find(MetricDefinition metricDefinition);

  /** Finds and returns the CompositeAlarm for the {@code compositeAlarmId}. */
  Alarm findByCompositeId(String compositeAlarmId);
}
