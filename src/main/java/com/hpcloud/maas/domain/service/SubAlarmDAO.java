package com.hpcloud.maas.domain.service;

import java.util.List;

import com.hpcloud.maas.common.model.metric.MetricDefinition;
import com.hpcloud.maas.domain.model.SubAlarm;

/**
 * SubAlarm DAO.
 * 
 * @author Jonathan Halterman
 */
public interface SubAlarmDAO {
  /** Finds and returns all sub alarms for the {@code metricDefinition}. */
  List<SubAlarm> find(MetricDefinition metricDefinition);
}
