package com.hpcloud.maas.domain.model;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Data for a specific metric. Value object.
 * 
 * @author Jonathan Halterman
 */
public class MetricData {
  private final Map<String, SubAlarmData> subAlarmsData = new HashMap<String, SubAlarmData>();

  public MetricData(List<SubAlarm> subAlarms) {
    long initialTimestamp = System.currentTimeMillis();
    for (SubAlarm subAlarm : subAlarms)
      addAlarm(subAlarm, initialTimestamp);
  }

  public void addAlarm(SubAlarm subAlarm, long initialTimestamp) {
    subAlarmsData.put(subAlarm.getExpression().getId(),
        new SubAlarmData(subAlarm, initialTimestamp));
  }

  /**
   * Adds the {@code value} to the statistics for the slot associated with the {@code timestamp},
   * else <b>does nothing</b> if the {@code timestamp} is outside of the window.
   */
  public void addValue(double value, long timestamp) {
    for (SubAlarmData alarmData : subAlarmsData.values())
      alarmData.getStats().addValue(value, timestamp);
  }

  public SubAlarmData alarmDataFor(String alarmId) {
    return subAlarmsData.get(alarmId);
  }

  public Collection<SubAlarmData> getAlarmData() {
    return subAlarmsData.values();
  }

  public void removeAlarm(String alarmId) {
    subAlarmsData.remove(alarmId);
  }

  @Override
  public String toString() {
    return String.format("MetricData [alarmsData=%s]", subAlarmsData);
  }
}
