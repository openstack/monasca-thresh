/*
 * (C) Copyright 2014-2016 Hewlett Packard Enterprise Development LP
 * Copyright 2016 FUJITSU LIMITED
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package monasca.thresh.domain.model;

import monasca.common.model.alarm.AlarmState;
import monasca.common.model.alarm.AlarmSubExpression;
import monasca.common.util.stats.SlidingWindowStats;
import monasca.common.util.time.TimeResolution;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Aggregates statistics for a specific SubAlarm.
 */
public class SubAlarmStats {
  private static final Logger logger = LoggerFactory.getLogger(SubAlarmStats.class);
  /** Number of slots for future periods that we should collect metrics for. */
  private static final int FUTURE_SLOTS = 2;
  /** Helps determine how many empty window observations before transitioning to UNDETERMINED. */
  private static final int UNDETERMINED_COEFFICIENT = 2;

  private final int slotWidth;
  private SubAlarm subAlarm;
  private SlidingWindowStats stats;
  /** The number of times we can observe an empty window before transitioning to UNDETERMINED state. */
  protected int emptyWindowObservationThreshold;
  private int emptyWindowObservations;

  public SubAlarmStats(SubAlarm subAlarm, long viewEndTimestamp) {
    this(subAlarm, TimeResolution.MINUTES, viewEndTimestamp);
  }

  public SubAlarmStats(SubAlarm subAlarm, TimeResolution timeResolution, long viewEndTimestamp) {
    slotWidth = subAlarm.getExpression().getPeriod();
    this.subAlarm = subAlarm;
    this.subAlarm.setNoState(true);
    initialize(subAlarm, timeResolution, viewEndTimestamp);
  }

  private void initialize(SubAlarm subAlarm, TimeResolution timeResolution, long viewEndTimestamp) {
    this.stats =
        new SlidingWindowStats(subAlarm.getExpression().getFunction().toStatistic(),
            timeResolution, slotWidth, subAlarm.getExpression().getPeriods(), FUTURE_SLOTS,
            viewEndTimestamp);
    int period = subAlarm.getExpression().getPeriod();
    int periodMinutes = period < 60 ? 1 : period / 60; // Assumes the period is in seconds so we
                                                       // convert to minutes
    emptyWindowObservationThreshold =
        periodMinutes * subAlarm.getExpression().getPeriods() * UNDETERMINED_COEFFICIENT;
    emptyWindowObservations = 0;
  }

  /**
   * Evaluates the {@link #subAlarm} for the current stats window, updating the sub-alarm's state if
   * necessary and sliding the window to the {@code slideToTimestamp}.
   *
   * @return true if the alarm's state changed, else false.
   */
  public boolean evaluateAndSlideWindow(long slideToTimestamp, long alarmDelay) {
    try {
      return evaluate(slideToTimestamp, alarmDelay);
    } catch (Exception e) {
      logger.error("Failed to evaluate {}", this, e);
      return false;
    } finally {
      slideWindow(slideToTimestamp, alarmDelay);
    }
  }

  /**
   * Just slide the window. Either slideWindow or evaluateAndSlideWindow should be called for each
   * time period, but never both
   *
   * @param slideToTimestamp
   */
  public void slideWindow(long slideToTimestamp, long alarmDelay) {
    stats.slideViewTo(slideToTimestamp, alarmDelay);
  }

  /**
   * Returns the stats.
   */
  public SlidingWindowStats getStats() {
    return stats;
  }

  public boolean addValue(double value, long timestamp) {
    return this.getStats().addValue(value, timestamp,
                                    this.getSubAlarm().onlyImmediateEvaluation());
  }

  /**
   * Returns the SubAlarm.
   */
  public SubAlarm getSubAlarm() {
    return subAlarm;
  }

  @Override
  public String toString() {
    return String
        .format(
            "SubAlarmStats [subAlarm=%s, stats=%s, emptyWindowObservations=%s, emptyWindowObservationThreshold=%s]",
            subAlarm, stats, emptyWindowObservations, emptyWindowObservationThreshold);
  }

  /**
   * @param now Current time
   * @param alarmDelay How long to give metrics a chance to arrive
   */
  boolean evaluate(final long now, long alarmDelay) {
    final boolean shouldEvaluate = this.stats.shouldEvaluate(now, alarmDelay);
    final AlarmState newState;

    final boolean immediateAlarmTransition;
    if (immediateAlarmEvaluate()) {
      newState = AlarmState.ALARM;
      immediateAlarmTransition = true;
    }
    else if (immediateOkEvaluate()) {
      newState = AlarmState.OK;
      immediateAlarmTransition = true;
    }
    else {
      if (!shouldEvaluate) {
        return false;
      }
      if (this.subAlarm.onlyImmediateEvaluation()) {
        return false;
      }
      newState = this.determineAlarmStateUsingView();
      immediateAlarmTransition = false;
    }
  
    final boolean shouldSendStateChange = this.shouldSendStateChange(newState);
  
    if (shouldSendStateChange && (shouldEvaluate || immediateAlarmTransition)) {
      logger.debug("SubAlarm[deterministic={}] {} transitions from {} to {}",
          this.getSubAlarm().isDeterministic(),
          this.getSubAlarm().getId(),
          this.getSubAlarm().getState(),
          newState
      );
      setSubAlarmState(newState);
      return true;
    }

    return false;
  }

  private AlarmState determineAlarmStateUsingView() {
    boolean thresholdExceeded = false;
    boolean hasEmptyWindows = false;
    subAlarm.clearCurrentValues();
    double[] values = stats.getViewValues();
    for (double value : values) {
      if (Double.isNaN(value)) {
        hasEmptyWindows = true;
      } else {
        subAlarm.addCurrentValue(value);
        emptyWindowObservations = 0;

        // Check if value is OK
        if (!subAlarm.getExpression().getOperator()
            .evaluate(value, subAlarm.getExpression().getThreshold())) {
          return AlarmState.OK;
        } else
          thresholdExceeded = true;
      }
    }

    if (thresholdExceeded && !hasEmptyWindows) {
      return AlarmState.ALARM;
    }

    final boolean isDeterministic = this.subAlarm.isDeterministic();

    // Window is empty at this point
    this.emptyWindowObservations++;
    final boolean useDefaultState;
    if (isDeterministic) {
      useDefaultState = true;
    }
    else {
      useDefaultState = this.emptyWindowObservations >= this.emptyWindowObservationThreshold;
    }

    if (useDefaultState) {
      final AlarmState state = SubAlarm.getDefaultState(isDeterministic);
      if (this.shouldSendStateChange(state)) {
        logger.debug(
            "SubAlarm[deterministic={}] {} exceeded empty window threshold {}, transition to {} from {}",
            isDeterministic,
            this.subAlarm.getId(),
            this.emptyWindowObservationThreshold,
            state,
            this.subAlarm.getState()
        );
        return state;
      }

    }

    // Hasn't transitioned to default state yet, so use the current state
    return null;
  }

  private boolean immediateAlarmEvaluate() {
    if (!this.subAlarm.canEvaluateAlarmImmediately()) {
      return false;
    }
    // Check the future slots as well
    final double[] allValues = stats.getWindowValues();
    subAlarm.clearCurrentValues();
    int alarmRun = 0;
    for (final double value : allValues) {
      if (Double.isNaN(value)) {
        alarmRun = 0;
        subAlarm.clearCurrentValues();
      } else {

        // Check if value is OK
        if (!subAlarm.getExpression().getOperator()
            .evaluate(value, subAlarm.getExpression().getThreshold())) {
          alarmRun = 0;
          subAlarm.clearCurrentValues();
        }
        else {
          subAlarm.addCurrentValue(value);
          alarmRun++;
          if (alarmRun == subAlarm.getExpression().getPeriods()) {
            return true;
          }
        }
      }
    }
    return false;
  }

  private boolean immediateOkEvaluate() {
    if (!this.subAlarm.canEvaluateOkImmediately()) {
      return false;
    }
    // Check the future slots as well
    final double[] allValues = stats.getWindowValues();
    subAlarm.clearCurrentValues();
    for (final double value : allValues) {
      if (Double.isNaN(value)) {
        subAlarm.clearCurrentValues();
      } else {
        // Check if value is ALARM
        if (subAlarm.getExpression().getOperator()
            .evaluate(value, subAlarm.getExpression().getThreshold())) {
          subAlarm.clearCurrentValues();
        }
        else {
          subAlarm.addCurrentValue(value);
          return true;
        }
      }
    }
    return false;
  }

  private boolean shouldSendStateChange(AlarmState newState) {
    return newState != null && (!subAlarm.getState().equals(newState) || subAlarm.isNoState());
  }

  private void setSubAlarmState(AlarmState newState) {
    subAlarm.setState(newState);
    subAlarm.setNoState(false);
  }

  /**
   * If this.subAlarm.isCompatible(newExpression) is not true, all data
   * will be flushed
   */
  public void updateSubAlarm(final AlarmSubExpression newExpression, long viewEndTimestamp) {
    // Save the old state
    this.subAlarm.setNoState(true);  // Doesn't hurt to send too many state changes, just too few
    final boolean compatible = this.subAlarm.isCompatible(newExpression);
    this.subAlarm.setExpression(newExpression);
    if (!compatible) {
      logger.debug("Changing {} to {} and flushing measurements", this.subAlarm, subAlarm);
      this.initialize(subAlarm, TimeResolution.MINUTES, viewEndTimestamp);
    }
  }
}

