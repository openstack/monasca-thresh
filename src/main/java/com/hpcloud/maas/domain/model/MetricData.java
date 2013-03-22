package com.hpcloud.maas.domain.model;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.hpcloud.maas.util.time.Times;

/**
 * Data for a specific metric. Value object.
 * 
 * @author Jonathan Halterman
 */
public class MetricData {
  private final Map<String, AlarmData> alarmsData = new HashMap<String, AlarmData>();

  public MetricData(List<Alarm> alarms) {
    long initialTimestamp = Times.roundDownToNearestSecond(System.currentTimeMillis());
    for (Alarm alarm : alarms)
      alarmsData.put(alarm.getId(), new AlarmData(alarm, initialTimestamp));
  }

  /**
   * Adds the {@code value} to the statistics for the slot associated with the {@code timestamp},
   * else <b>does nothing</b> if the {@code timestamp} is outside of the window.
   */
  public void addValue(double value, long timestamp) {
    for (AlarmData alarmData : alarmsData.values())
      alarmData.getStats().addValue(value, timestamp);
  }

  public void addAlarm(Alarm alarm) {
    // TODO
  }

  public void removeAlarm(String alarmId) {
    alarmsData.remove(alarmId);
  }

  public Collection<AlarmData> getAlarmData() {
    return alarmsData.values();
  }

  @Override
  public String toString() {
    return String.format("MetricData [alarmsData=%s]", alarmsData);
  }
}
