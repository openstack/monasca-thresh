package com.hpcloud.maas.domain.model;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.hpcloud.maas.common.model.alarm.AlarmState;
import com.hpcloud.maas.common.model.alarm.AlarmSubExpression;

/**
 * @author Jonathan Halterman
 */
@Test
public class SubAlarmStatsTest {
  private AlarmSubExpression expression;
  private SubAlarm subAlarm;
  private SubAlarmStats subAlarmStats;

  @BeforeMethod
  protected void beforeMethod() {
    expression = AlarmSubExpression.of("avg(compute:cpu:{id=5}, 1) > 3 times 3");
    subAlarm = new SubAlarm("1", "123", expression);
    subAlarmStats = new SubAlarmStats(subAlarm, 3000);
  }

  public void shouldBeOkIfAnySlotsInViewAreBelowThreshold() {
    subAlarmStats.getStats().addValue(5, 1000);
    subAlarmStats.getStats().addValue(1, 2000);
    subAlarmStats.getStats().addValue(5, 3000);

    assertTrue(subAlarmStats.evaluate(3000));
    assertEquals(subAlarmStats.getSubAlarm().getState(), AlarmState.OK);
  }

  public void shouldBeAlarmedIfAllSlotsInViewExceedThreshold() {
    subAlarmStats.getStats().addValue(5, 1000);
    subAlarmStats.getStats().addValue(5, 2000);
    subAlarmStats.getStats().addValue(5, 3000);

    assertTrue(subAlarmStats.evaluate(3000));
    assertEquals(subAlarmStats.getSubAlarm().getState(), AlarmState.ALARM);
  }

  public void shouldBeAlarmedIfAllSlotsExceedThresholdOrAreUninitialized() {
    subAlarmStats.getStats().addValue(5, 1000);

    assertTrue(subAlarmStats.evaluate(3000));
    assertEquals(subAlarmStats.getSubAlarm().getState(), AlarmState.ALARM);
  }

  /**
   * Simulates the way a window will fill up in practice.
   */
  public void shouldEvaluateAndSlideWindow() {
    long initialTime = 11000;
    subAlarmStats = new SubAlarmStats(subAlarm, initialTime - 1000);

    assertEquals(subAlarmStats.getSubAlarm().getState(), AlarmState.UNDETERMINED);
    assertFalse(subAlarmStats.evaluateAndSlideWindow(initialTime, initialTime += 1000));
    assertFalse(subAlarmStats.evaluateAndSlideWindow(initialTime, initialTime += 1000));
    assertFalse(subAlarmStats.evaluateAndSlideWindow(initialTime, initialTime += 1000));

    // Add value and trigger OK
    subAlarmStats.getStats().addValue(1, initialTime);
    assertTrue(subAlarmStats.evaluateAndSlideWindow(initialTime, initialTime += 1000));
    assertEquals(subAlarmStats.getSubAlarm().getState(), AlarmState.OK);

    // Slide in some values that exceed the threshold
    subAlarmStats.getStats().addValue(5, initialTime);
    assertFalse(subAlarmStats.evaluateAndSlideWindow(initialTime, initialTime += 1000));
    subAlarmStats.getStats().addValue(5, initialTime);
    assertFalse(subAlarmStats.evaluateAndSlideWindow(initialTime, initialTime += 1000));
    subAlarmStats.getStats().addValue(5, initialTime);

    // Trigger ALARM
    assertTrue(subAlarmStats.evaluateAndSlideWindow(initialTime, initialTime += 1000));
    assertEquals(subAlarmStats.getSubAlarm().getState(), AlarmState.ALARM);

    // Add value and trigger OK
    subAlarmStats.getStats().addValue(1, initialTime);
    assertTrue(subAlarmStats.evaluateAndSlideWindow(initialTime, initialTime += 1000));
    assertEquals(subAlarmStats.getSubAlarm().getState(), AlarmState.OK);

    // Must slide 9 times total from the last added value to trigger UNDETERMINED. This is
    // equivalent to the behavior in CloudWatch for an alarm with 3 evaluation periods. 2 more
    // slides to move the value outside of the window and 6 more to exceed the observation
    // threshold.
    for (int i = 0; i < 7; i++)
      assertFalse(subAlarmStats.evaluateAndSlideWindow(initialTime, initialTime += 1000));
    assertTrue(subAlarmStats.evaluateAndSlideWindow(initialTime, initialTime += 1000));
    assertEquals(subAlarmStats.getSubAlarm().getState(), AlarmState.UNDETERMINED);
  }
}