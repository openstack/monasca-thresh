package com.hpcloud.mon.domain.model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hpcloud.mon.common.model.alarm.AlarmState;
import com.hpcloud.util.stats.SlidingWindowStats;
import com.hpcloud.util.time.TimeResolution;

/**
 * Aggregates statistics for a specific SubAlarm.
 * 
 * @author Jonathan Halterman
 */
public class SubAlarmStats {
  private static final Logger LOG = LoggerFactory.getLogger(SubAlarmStats.class);
  /** Number of slots for future periods that we should collect metrics for. */
  private static final int FUTURE_SLOTS = 2;
  /** Helps determine how many empty window observations before transitioning to UNDETERMINED. */
  private static final int UNDETERMINED_COEFFICIENT = 2;

  private final int slotWidth;
  private SubAlarm subAlarm;
  private final SlidingWindowStats stats;
  /** The number of times we can observe an empty window before transitioning to UNDETERMINED state. */
  final int emptyWindowObservationThreshold;
  private int emptyWindowObservations;

  public SubAlarmStats(SubAlarm subAlarm, long viewEndTimestamp) {
    this(subAlarm, TimeResolution.MINUTES, viewEndTimestamp);
  }

  public SubAlarmStats(SubAlarm subAlarm, TimeResolution timeResolution, long viewEndTimestamp) {
    slotWidth = subAlarm.getExpression().getPeriod();
    this.subAlarm = subAlarm;
    this.subAlarm.setNoState(true);
    this.stats = new SlidingWindowStats(subAlarm.getExpression().getFunction().toStatistic(),
        timeResolution, slotWidth, subAlarm.getExpression().getPeriods(), FUTURE_SLOTS,
        viewEndTimestamp);
    int period = subAlarm.getExpression().getPeriod();
    int periodMinutes = period < 60 ? 1 : period / 60; // Assumes the period is in seconds so we
                                                       // convert to minutes
    emptyWindowObservationThreshold = periodMinutes * subAlarm.getExpression().getPeriods()
        * UNDETERMINED_COEFFICIENT;
    emptyWindowObservations = 0;
  }

  /**
   * Evaluates the {@link #subAlarm} for the current stats window, updating the sub-alarm's state if
   * necessary and sliding the window to the {@code slideToTimestamp}.
   * 
   * @return true if the alarm's state changed, else false.
   */
  public boolean evaluateAndSlideWindow(long slideToTimestamp) {
    try {
      return evaluate();
    } catch (Exception e) {
      LOG.error("Failed to evaluate {}", this, e);
      return false;
    } finally {
      stats.slideViewTo(slideToTimestamp);
    }
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
    return String.format(
        "SubAlarmStats [subAlarm=%s, stats=%s, emptyWindowObservations=%s, emptyWindowObservationThreshold=%s]",
        subAlarm, stats, emptyWindowObservations, emptyWindowObservationThreshold);
  }

  /**
   * @throws IllegalStateException if the {@code timestamp} is outside of the {@link #stats} window
   */
  boolean evaluate() {
    double[] values = stats.getViewValues();
    AlarmState initialState = subAlarm.getState();
    boolean thresholdExceeded = false;
    boolean hasEmptyWindows = false;
    for (double value : values) {
      if (Double.isNaN(value))
        hasEmptyWindows = true;
      else {
        emptyWindowObservations = 0;

        // Check if value is OK
        if (!subAlarm.getExpression()
            .getOperator()
            .evaluate(value, subAlarm.getExpression().getThreshold())) {
          if (AlarmState.OK.equals(initialState))
            return false;
          setSubAlarmState(AlarmState.OK);
          return true;
        } else
          thresholdExceeded = true;
      }
    }

    if (thresholdExceeded && !hasEmptyWindows) {
      if (AlarmState.ALARM.equals(initialState))
        return false;
      setSubAlarmState(AlarmState.ALARM);
      return true;
    }

    // Window is empty at this point
    emptyWindowObservations++;

    if ((emptyWindowObservations >= emptyWindowObservationThreshold) &&
         (subAlarm.isNoState() || !AlarmState.UNDETERMINED.equals(initialState)) &&
         !subAlarm.isSporadicMetric()) {
        setSubAlarmState(AlarmState.UNDETERMINED);
      return true;
    }

    return false;
  }

private void setSubAlarmState(AlarmState newState) {
    subAlarm.setState(newState);
    subAlarm.setNoState(false);
}

  /**
   * This MUST only be used for compatible SubAlarms, i.e. where
   * this.subAlarm.isCompatible(subAlarm) is true 
   * @param subAlarm
   */
  public void updateSubAlarm(final SubAlarm subAlarm) {
      this.subAlarm = subAlarm;
  }
}
