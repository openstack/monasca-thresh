package com.hpcloud.mon.domain.service;

import java.util.List;

/**
 * Metric definition data access object.
 * 
 * @author Jonathan Halterman
 */
public interface MetricDefinitionDAO {
  /** Finds all metric definitions SubAlarm IDs for all alarms. */
  List<SubAlarmMetricDefinition> findForAlarms();
}
