package com.hpcloud.maas.domain.model;

import com.hpcloud.maas.common.model.alarm.AlarmState;
import com.hpcloud.maas.util.stats.SlidingWindowStats;
import com.hpcloud.maas.util.stats.Statistics;
import com.hpcloud.maas.util.time.Timescale;

/**
 * Data for a specific alarm. Value object.
 * 
 * @author Jonathan Halterman
 */
public class SubAlarmData {
  /**
   * Helps determine how many observations to wait for before changing an alarm's state to
   * insufficient data.
   */
  private static final int INSUFFICIENT_DATA_COEFFICIENT = 3;

  private final SubAlarm alarm;
  private final SlidingWindowStats stats;
  private final int emptySlotObservationThreshold;
  private int emptySlotObservations;

  public SubAlarmData(SubAlarm alarm, long initialTimestamp) {
    this.alarm = alarm;
    this.stats = new SlidingWindowStats(Timescale.SECONDS_SINCE_EPOCH, alarm.getExpression()
        .getPeriod(), alarm.getExpression().getPeriods(),
        Statistics.statTypeFor(alarm.getExpression().getFunction()), initialTimestamp);
    emptySlotObservationThreshold = (stats.getSlotWidthInMinutes() == 0 ? 1
        : stats.getSlotWidthInMinutes()) * INSUFFICIENT_DATA_COEFFICIENT;
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

    // TODO initialTimestamp should come into play here for selecting the appropriate portion of the
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
      if (!alarm.getExpression()
          .getOperator()
          .evaluate(value, alarm.getExpression().getThreshold())) {
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
  public SubAlarm getAlarm() {
    return alarm;
  }

  @Override
  public String toString() {
    return String.format("AlarmData [alarm=%s, stats=%s]", alarm, stats);
  }
}
