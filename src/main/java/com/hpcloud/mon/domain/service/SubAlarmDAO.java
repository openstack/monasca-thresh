package com.hpcloud.mon.domain.service;

import java.util.List;

import com.hpcloud.mon.common.model.metric.MetricDefinition;
import com.hpcloud.mon.domain.model.SubAlarm;

/**
 * SubAlarm DAO.
 * 
 * @author Jonathan Halterman
 */
public interface SubAlarmDAO {
  /** Finds and returns all sub alarms for the {@code metricDefinition}. */
  List<SubAlarm> find(MetricDefinition metricDefinition);
}
