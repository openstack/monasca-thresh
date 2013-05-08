package com.hpcloud.maas.domain.model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hpcloud.maas.common.model.alarm.AlarmState;
import com.hpcloud.maas.util.stats.SlidingWindowStats;
import com.hpcloud.maas.util.stats.Statistics;
import com.hpcloud.maas.util.time.TimeResolution;

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
  private final SubAlarm subAlarm;
  private final SlidingWindowStats stats;
  /** The number of times we can observe an empty window before transitioning to UNDETERMINED state. */
  private final int emptyWindowObservationThreshold;
  private int emptyWindowObservations;

  public SubAlarmStats(SubAlarm subAlarm, long viewEndTimestamp) {
    this(subAlarm, TimeResolution.MINUTES, viewEndTimestamp);
  }

  public SubAlarmStats(SubAlarm subAlarm, TimeResolution timeResolution, long viewEndTimestamp) {
    slotWidth = subAlarm.getExpression().getPeriod();
    this.subAlarm = subAlarm;
    this.stats = new SlidingWindowStats(Statistics.statTypeFor(subAlarm.getExpression()
        .getFunction()), timeResolution, slotWidth, subAlarm.getExpression().getPeriods(),
        FUTURE_SLOTS + 1, viewEndTimestamp);
    emptyWindowObservationThreshold = (subAlarm.getExpression().getPeriod() == 0 ? 1
        : subAlarm.getExpression().getPeriod() * subAlarm.getExpression().getPeriods())
        * UNDETERMINED_COEFFICIENT;
    emptyWindowObservations = emptyWindowObservationThreshold;
  }

  /**
   * Evaluates the {@link #subAlarm} for stats up to and including the {@code evaluationTimestamp},
   * updating the sub-alarm's state if necessary and sliding the window to the
   * {@code slideToTimestamp}.
   * 
   * @return true if the alarm's state changed, else false.
   */
  public boolean evaluateAndSlideWindow(long evaluateTimestamp, long slideToTimestamp) {
    try {
      return evaluate(evaluateTimestamp);
    } catch (Exception e) {
      LOG.error("Failed to evaluate {} for timestamp {}", this, evaluateTimestamp, e);
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
  boolean evaluate(long timestamp) {
    double[] values = null;

    try {
      values = stats.getValuesUpTo(timestamp);
    } catch (IllegalStateException ignore) {
      return false;
    }

    LOG.debug("Evaluating {} at timestamp {} for values {}", this, timestamp, values);
    AlarmState initialState = subAlarm.getState();
    boolean thresholdExceeded = false;
    for (double value : values) {
      if (!Double.isNaN(value)) {
        emptyWindowObservations = 0;

        // Check if value is OK
        if (!subAlarm.getExpression()
            .getOperator()
            .evaluate(value, subAlarm.getExpression().getThreshold())) {
          if (AlarmState.OK.equals(initialState))
            return false;
          subAlarm.setState(AlarmState.OK);
          return true;
        } else
          thresholdExceeded = true;
      }
    }

    if (thresholdExceeded) {
      if (AlarmState.ALARM.equals(initialState))
        return false;
      subAlarm.setState(AlarmState.ALARM);
      return true;
    }

    // Window is empty at this point
    emptyWindowObservations++;

    if (emptyWindowObservations >= emptyWindowObservationThreshold
        && !AlarmState.UNDETERMINED.equals(initialState)) {
      subAlarm.setState(AlarmState.UNDETERMINED);
      return true;
    }

    return false;
  }
}
