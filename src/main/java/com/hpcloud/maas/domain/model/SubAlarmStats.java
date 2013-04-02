package com.hpcloud.maas.domain.model;

import com.hpcloud.maas.common.model.alarm.AlarmState;
import com.hpcloud.maas.util.stats.SlidingWindowStats;
import com.hpcloud.maas.util.stats.Statistics;
import com.hpcloud.maas.util.time.Timescale;

/**
 * Aggregates statistics for a specific SubAlarm.
 * 
 * @author Jonathan Halterman
 */
public class SubAlarmStats {
  /** Number of slots for future minutes that we should collect metrics for. */
  private static final int FUTURE_SLOTS = 2;
  /** Determines how many observations to wait for before changing an alarm's state to undetermined. */
  private static final int INSUFFICIENT_DATA_COEFFICIENT = 3;

  private final SubAlarm subAlarm;
  private final SlidingWindowStats stats;
  private final int emptySlotObservationThreshold;
  private int emptySlotObservations;

  public SubAlarmStats(SubAlarm subAlarm, long initialTimestamp) {
    this.subAlarm = subAlarm;
    this.stats = new SlidingWindowStats(Statistics.statTypeFor(subAlarm.getExpression()
        .getFunction()), Timescale.MILLISECONDS, subAlarm.getExpression().getPeriod() * 1000,
        subAlarm.getExpression().getPeriods() + FUTURE_SLOTS, initialTimestamp);
    emptySlotObservationThreshold = (subAlarm.getExpression().getPeriod() == 0 ? 1
        : subAlarm.getExpression().getPeriod())
        * INSUFFICIENT_DATA_COEFFICIENT;
    emptySlotObservations = emptySlotObservationThreshold;
  }

  /**
   * Evaluates the {@code alarm}, updating the alarm's state if necessary and returning true if the
   * alarm's state changed, else false.
   */
  public boolean evaluateAndAdvanceWindow(long timestamp) {
    boolean result = evaluate(timestamp);
    stats.advanceWindowTo(timestamp);
    return result;
  }

  /**
   * Returns the stats.
   */
  public SlidingWindowStats getStats() {
    return stats;
  }

  /**
   * Returns the SubAlarm.
   */
  public SubAlarm getSubAlarm() {
    return subAlarm;
  }

  @Override
  public String toString() {
    return String.format("SubAlarmStats [subAlarm=%s, stats=%s]", subAlarm, stats);
  }

  private boolean evaluate(long timestamp) {
    if (stats.hasEmptySlots())
      emptySlotObservations++;
    else
      emptySlotObservations = 0;

    AlarmState initialState = subAlarm.getState();
    if (emptySlotObservations >= emptySlotObservationThreshold) {
      if (AlarmState.UNDETERMINED.equals(initialState))
        return false;
      subAlarm.setState(AlarmState.UNDETERMINED);
      return true;
    }

    boolean alarmed = true;
    for (double value : stats.getValuesUpTo(timestamp))
      if (!subAlarm.getExpression()
          .getOperator()
          .evaluate(value, subAlarm.getExpression().getThreshold())) {
        alarmed = false;
        break;
      }

    if (alarmed) {
      if (AlarmState.ALARM.equals(initialState))
        return false;
      subAlarm.setState(AlarmState.ALARM);
      return true;
    }

    if (AlarmState.OK.equals(initialState))
      return false;
    subAlarm.setState(AlarmState.OK);
    return true;
  }
}
