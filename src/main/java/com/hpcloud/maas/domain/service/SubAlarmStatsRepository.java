package com.hpcloud.maas.domain.service;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.hpcloud.maas.domain.model.SubAlarm;
import com.hpcloud.maas.domain.model.SubAlarmStats;

/**
 * SubAlarmStats repository.
 * 
 * @author Jonathan Halterman
 */
public class SubAlarmStatsRepository {
  private final Map<String, SubAlarmStats> subAlarmStats = new HashMap<String, SubAlarmStats>();

  /**
   * Creates a new SubAlarmStatsRepository initialized with SubAlarmStats for each of the
   * {@code subAlarms} with the {@code viewEndTimestamp}.
   */
  public SubAlarmStatsRepository(List<SubAlarm> subAlarms, long viewEndTimestamp) {
    for (SubAlarm subAlarm : subAlarms)
      add(subAlarm, viewEndTimestamp);
  }

  /**
   * Creates a new SubAlarmStats instance for the {@code subAlarm} and {@code viewEndTimestamp} and
   * adds it to the repository.
   */
  public void add(SubAlarm subAlarm, long viewEndTimestamp) {
    subAlarmStats.put(subAlarm.getId(), new SubAlarmStats(subAlarm, viewEndTimestamp));
  }

  public Collection<SubAlarmStats> get() {
    return subAlarmStats.values();
  }

  public SubAlarmStats get(String subAlarmId) {
    return subAlarmStats.get(subAlarmId);
  }

  public void remove(String subAlarmId) {
    subAlarmStats.remove(subAlarmId);
  }

  @Override
  public String toString() {
    return String.format("SubAlarmStatsRepository [subAlarmStats=%s]", subAlarmStats);
  }
}
