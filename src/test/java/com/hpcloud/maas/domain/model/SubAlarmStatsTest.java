package com.hpcloud.maas.domain.model;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import com.hpcloud.maas.common.model.alarm.AlarmState;
import com.hpcloud.maas.common.model.alarm.AlarmSubExpression;

/**
 * @author Jonathan Halterman
 */
@Test
public class SubAlarmStatsTest {
  /**
   * Simulates the way a window will fill up in practice.
   */
  public void shouldEvaluateAndSlideWindow() {
    AlarmSubExpression expression = AlarmSubExpression.of("sum(compute:cpu:{id=5}, 2, 3) >= 3");
    SubAlarm subAlarm = new SubAlarm("1", "123", expression);
    long initialTime = 11000;
    SubAlarmStats subAlarmStats = new SubAlarmStats(subAlarm, initialTime - 1000);

    assertEquals(subAlarmStats.getSubAlarm().getState(), AlarmState.UNDETERMINED);
    assertFalse(subAlarmStats.evaluateAndSlideWindow(initialTime, initialTime += 1000));
    assertFalse(subAlarmStats.evaluateAndSlideWindow(initialTime, initialTime += 1000));
    assertFalse(subAlarmStats.evaluateAndSlideWindow(initialTime, initialTime += 1000));

    // Fill all slots
    subAlarmStats.getStats().addValue(1, initialTime);
    assertFalse(subAlarmStats.evaluateAndSlideWindow(initialTime, initialTime += 2000));
    subAlarmStats.getStats().addValue(1, initialTime);
    assertFalse(subAlarmStats.evaluateAndSlideWindow(initialTime, initialTime += 2000));
    subAlarmStats.getStats().addValue(1, initialTime);

    // Trigger OK
    assertTrue(subAlarmStats.evaluateAndSlideWindow(initialTime, initialTime += 1000));
    assertEquals(subAlarmStats.getSubAlarm().getState(), AlarmState.OK);
    
    // Bump slots over threshold
    subAlarmStats.getStats().addValue(5, initialTime);
    subAlarmStats.getStats().addValue(5, initialTime - 2000);
    subAlarmStats.getStats().addValue(5, initialTime - 4000);

    // Trigger ALARM
    assertTrue(subAlarmStats.evaluateAndSlideWindow(initialTime, initialTime += 2000));
    assertEquals(subAlarmStats.getSubAlarm().getState(), AlarmState.ALARM);

    // Evaluate window to trigger OK
    assertTrue(subAlarmStats.evaluateAndSlideWindow(initialTime, initialTime += 1000));
    assertEquals(subAlarmStats.getSubAlarm().getState(), AlarmState.OK);

    // Slide it a few more times to trigger UNDETERMINED
    assertFalse(subAlarmStats.evaluateAndSlideWindow(initialTime, initialTime += 1000));
    assertFalse(subAlarmStats.evaluateAndSlideWindow(initialTime, initialTime += 1000));
    assertFalse(subAlarmStats.evaluateAndSlideWindow(initialTime, initialTime += 1000));
    assertFalse(subAlarmStats.evaluateAndSlideWindow(initialTime, initialTime += 1000));
    assertTrue(subAlarmStats.evaluateAndSlideWindow(initialTime, initialTime += 1000));
    assertEquals(subAlarmStats.getSubAlarm().getState(), AlarmState.UNDETERMINED);
  }
}
