package com.hpcloud.maas.domain.model;

import com.hpcloud.maas.common.model.alarm.AlarmState;
import com.hpcloud.maas.util.stats.SlidingWindowStats;
import com.hpcloud.maas.util.stats.Statistics;

/**
 * Data for a specific alarm. Value object.
 * 
 * @author Jonathan Halterman
 */
public class AlarmData {
  /**
   * Coefficient that, along with the window size, determines how many observations to wait for
   * before changing an alarm's state to insufficient data.
   */
  private static final int INSUFFICIENT_DATA_COEFFICIENT = 3;

  private final Alarm alarm;
  private final SlidingWindowStats stats;
  private final int emptySlotObservationThreshold;
  private int emptySlotObservations;

  public AlarmData(Alarm alarm, long initialTimestamp) {
    this.alarm = alarm;
    this.stats = new SlidingWindowStats(alarm.getPeriodSeconds(), alarm.getPeriods(),
        Statistics.statTypeFor(alarm.getFunction()), initialTimestamp);
    int slotWidthInMinutes = stats.getSlotWidthInSeconds() / 60;
    emptySlotObservationThreshold = (slotWidthInMinutes == 0 ? 1 : slotWidthInMinutes)
        * INSUFFICIENT_DATA_COEFFICIENT;
  }

  public SlidingWindowStats getStats() {
    return stats;
  }

  /**
   * Evaluates the {@code alarm}, updating the alarm's state if necessary and returning true if the
   * alarm's state changed, else false.
   */
  public boolean evaluate(long initialTimestamp) {
    if (stats.hasEmptySlots())
      emptySlotObservations++;
    else
      emptySlotObservations = 0;

    // TODO initialTimestamp should come into play here for selecting the appropraite portion of the
    // window. maybe? does that mean the window needs to be larger than it is by default?

    AlarmState initialState = alarm.getState();
    if (emptySlotObservations >= emptySlotObservationThreshold) {
      if (AlarmState.UNDETERMINED.equals(initialState))
        return false;
      alarm.setState(AlarmState.UNDETERMINED);
      return true;
    }

    boolean alarmed = true;
    for (double value : stats.getValues())
      if (!alarm.getOperator().evaluate(value, alarm.getThreshold())) {
        alarmed = false;
        break;
      }

    if (alarmed) {
      if (AlarmState.ALARM.equals(initialState))
        return false;
      alarm.setState(AlarmState.ALARM);
      return true;
    }

    if (AlarmState.OK.equals(initialState))
      return false;
    alarm.setState(AlarmState.OK);
    return true;
  }

  /**
   * Returns the alarm that data is being observed for.
   */
  public Alarm getAlarm() {
    return alarm;
  }

  @Override
  public String toString() {
    return String.format("AlarmData [alarm=%s, stats=%s]", alarm, stats);
  }
}
