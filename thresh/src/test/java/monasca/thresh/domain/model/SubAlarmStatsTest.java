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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

import java.util.UUID;

import monasca.common.model.alarm.AlarmState;
import monasca.common.model.alarm.AlarmSubExpression;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test
public class SubAlarmStatsTest {
  private SubExpression avgExpression;
  private SubAlarm avgSubAlarm;
  private SubAlarmStats avgSubAlarmStats;
  private SubExpression lastExpression;
  private SubAlarm lastSubAlarm;
  private SubAlarmStats lastSubAlarmStats;
  private long lastViewStartTime;

  @BeforeMethod
  protected void beforeMethod() {
    avgExpression =
        new SubExpression(UUID.randomUUID().toString(),
            AlarmSubExpression.of("avg(hpcs.compute.cpu{id=5}, 60) > 3 times 3"));
    avgSubAlarm = new SubAlarm("123", "1", avgExpression);
    avgSubAlarm.setNoState(true);
    avgSubAlarmStats = new SubAlarmStats(avgSubAlarm, avgExpression.getAlarmSubExpression().getPeriod());

    lastExpression =
        new SubExpression(UUID.randomUUID().toString(),
            AlarmSubExpression.of("last(hpcs.compute.cpu{id=5}) > 0"));
    lastSubAlarm = new SubAlarm("456", "1", lastExpression, AlarmState.UNDETERMINED);
    lastSubAlarm.setNoState(true);
    lastViewStartTime = 10000;
    lastSubAlarmStats = new SubAlarmStats(lastSubAlarm,
            lastViewStartTime + lastExpression.getAlarmSubExpression().getPeriod());
  }

  public void shouldAcceptLastMetricIfOld() {
    assertTrue(lastSubAlarmStats.addValue(99, 10));
    assertTrue(lastSubAlarmStats.evaluate(lastViewStartTime + 10, 0));
    assertEquals(lastSubAlarmStats.getSubAlarm().getState(), AlarmState.ALARM);
  }

  public void shouldImmediateTransitionToOk() {
    assertTrue(lastSubAlarmStats.addValue(0, lastViewStartTime + 10));
    assertTrue(lastSubAlarmStats.evaluate(lastViewStartTime + 10, 0));
    assertEquals(lastSubAlarmStats.getSubAlarm().getState(), AlarmState.OK);
  }

  public void shouldNotTransitionToAlarmTwice() {
    assertTrue(lastSubAlarmStats.addValue(99, lastViewStartTime + 10));
    assertTrue(lastSubAlarmStats.evaluate(lastViewStartTime + 10, 0));
    assertEquals(lastSubAlarmStats.getSubAlarm().getState(), AlarmState.ALARM);

    assertTrue(lastSubAlarmStats.addValue(98, lastViewStartTime + 20));
    assertFalse(lastSubAlarmStats.evaluate(lastViewStartTime + 20, 0));
    assertEquals(lastSubAlarmStats.getSubAlarm().getState(), AlarmState.ALARM);
  }

  public void shouldNotTransitionToOkTwice() {
    assertTrue(lastSubAlarmStats.addValue(0, lastViewStartTime + 10));
    assertTrue(lastSubAlarmStats.evaluate(lastViewStartTime + 10, 0));
    assertEquals(lastSubAlarmStats.getSubAlarm().getState(), AlarmState.OK);

    assertTrue(lastSubAlarmStats.addValue(0, lastViewStartTime + 20));
    assertFalse(lastSubAlarmStats.evaluate(lastViewStartTime + 20, 0));
    assertEquals(lastSubAlarmStats.getSubAlarm().getState(), AlarmState.OK);
  }

  public void shouldNotTransitionOnOldMeasurement() {
    assertTrue(lastSubAlarmStats.addValue(99, lastViewStartTime + 10));
    assertTrue(lastSubAlarmStats.evaluate(lastViewStartTime + 10, 0));
    assertEquals(lastSubAlarmStats.getSubAlarm().getState(), AlarmState.ALARM);

    assertTrue(lastSubAlarmStats.addValue(0, lastViewStartTime + 5));
    assertFalse(lastSubAlarmStats.evaluate(lastViewStartTime + 5, 0));
    assertEquals(lastSubAlarmStats.getSubAlarm().getState(), AlarmState.ALARM);
  }

  public void shouldImmediatelyTransition() {
    assertTrue(lastSubAlarmStats.addValue(99, lastViewStartTime + 10));
    assertTrue(lastSubAlarmStats.evaluate(lastViewStartTime + 10, 0));
    assertEquals(lastSubAlarmStats.getSubAlarm().getState(), AlarmState.ALARM);

    assertTrue(lastSubAlarmStats.addValue(0, lastViewStartTime + 15));
    assertTrue(lastSubAlarmStats.evaluate(lastViewStartTime + 15, 0));
    assertEquals(lastSubAlarmStats.getSubAlarm().getState(), AlarmState.OK);

    assertTrue(lastSubAlarmStats.addValue(99, lastViewStartTime + 20));
    assertTrue(lastSubAlarmStats.evaluate(lastViewStartTime + 20, 0));
    assertEquals(lastSubAlarmStats.getSubAlarm().getState(), AlarmState.ALARM);
  }

  public void shouldNotTransitionFromAlarmWithNoMetrics() {
    assertTrue(lastSubAlarmStats.addValue(99, lastViewStartTime + 10));
    assertTrue(lastSubAlarmStats.evaluate(lastViewStartTime + 10, 0));
    assertEquals(lastSubAlarmStats.getSubAlarm().getState(), AlarmState.ALARM);
    for (int period = 1; period < 10; period++) {
      long time = lastViewStartTime + period * lastSubAlarm.getExpression().getPeriod();
      assertFalse(lastSubAlarmStats.evaluateAndSlideWindow(time, 30));
      assertEquals(lastSubAlarmStats.getSubAlarm().getState(), AlarmState.ALARM);
    }
  }

  public void shouldNotTransitionFromOkWithNoMetrics() {
    assertTrue(lastSubAlarmStats.addValue(0, lastViewStartTime + 10));
    assertTrue(lastSubAlarmStats.evaluate(lastViewStartTime + 10, 0));
    assertEquals(lastSubAlarmStats.getSubAlarm().getState(), AlarmState.OK);
    for (int period = 1; period < 10; period++) {
      long time = lastViewStartTime + period * lastSubAlarm.getExpression().getPeriod();
      assertFalse(lastSubAlarmStats.evaluateAndSlideWindow(time, 30));
      assertEquals(lastSubAlarmStats.getSubAlarm().getState(), AlarmState.OK);
    }
  }

  public void shouldBeOkIfAnySlotsInViewAreBelowThreshold() {
    sendMetric(5, 1, false);
    assertFalse(avgSubAlarmStats.evaluateAndSlideWindow(62, 1));
    assertEquals(avgSubAlarmStats.getSubAlarm().getState(), AlarmState.UNDETERMINED);

    sendMetric(1, 62, false);
    assertTrue(avgSubAlarmStats.evaluateAndSlideWindow(122, 1));
    // This went to OK because at least one period is under the threshold
    assertEquals(avgSubAlarmStats.getSubAlarm().getState(), AlarmState.OK);

    sendMetric(5, 123, false);
    assertFalse(avgSubAlarmStats.evaluateAndSlideWindow(182, 1));
    // Still one under the threshold
    assertEquals(avgSubAlarmStats.getSubAlarm().getState(), AlarmState.OK);
  }

  public void shouldBeAlarmedIfAllSlotsInViewExceedThreshold() {
    sendMetric(5, 1, false);
    assertFalse(avgSubAlarmStats.evaluateAndSlideWindow(62, 1));
    assertEquals(avgSubAlarmStats.getSubAlarm().getState(), AlarmState.UNDETERMINED);

    sendMetric(5, 62, false);
    assertFalse(avgSubAlarmStats.evaluateAndSlideWindow(122, 1));
    assertEquals(avgSubAlarmStats.getSubAlarm().getState(), AlarmState.UNDETERMINED);

    sendMetric(5, 123, false);
    assertTrue(avgSubAlarmStats.evaluateAndSlideWindow(182, 1));
    assertEquals(avgSubAlarmStats.getSubAlarm().getState(), AlarmState.ALARM);
  }

  /**
   * Simulates the way a window will fill up in practice.
   */
  public void shouldEvaluateAndSlideWindow() {
    long initialTime = 11;

    assertEquals(avgSubAlarmStats.getSubAlarm().getState(), AlarmState.UNDETERMINED);
    assertFalse(avgSubAlarmStats.evaluateAndSlideWindow(initialTime += 60, 10));
    assertEquals(avgSubAlarmStats.getSubAlarm().getState(), AlarmState.UNDETERMINED);
    assertFalse(avgSubAlarmStats.evaluateAndSlideWindow(initialTime += 60, 10));
    assertEquals(avgSubAlarmStats.getSubAlarm().getState(), AlarmState.UNDETERMINED);
    assertFalse(avgSubAlarmStats.evaluateAndSlideWindow(initialTime += 60, 10));
    assertEquals(avgSubAlarmStats.getSubAlarm().getState(), AlarmState.UNDETERMINED);

    // Add value and trigger OK
    sendMetric(1, initialTime - 1, false);
    assertTrue(avgSubAlarmStats.evaluateAndSlideWindow(initialTime += 60, 10));
    assertEquals(avgSubAlarmStats.getSubAlarm().getState(), AlarmState.OK);

    // Slide in some values that exceed the threshold
    sendMetric(5, initialTime - 1, false);
    assertFalse(avgSubAlarmStats.evaluateAndSlideWindow(initialTime += 60, 10));
    assertEquals(avgSubAlarmStats.getSubAlarm().getState(), AlarmState.OK);
    sendMetric(5, initialTime - 1, false);
    assertFalse(avgSubAlarmStats.evaluateAndSlideWindow(initialTime += 60, 10));
    assertEquals(avgSubAlarmStats.getSubAlarm().getState(), AlarmState.OK);
    sendMetric(5, initialTime - 1, false);
    assertEquals(avgSubAlarmStats.getSubAlarm().getState(), AlarmState.OK);

    // Trigger ALARM
    assertTrue(avgSubAlarmStats.evaluateAndSlideWindow(initialTime += 60, 10));
    assertEquals(avgSubAlarmStats.getSubAlarm().getState(), AlarmState.ALARM);

    // Add value and trigger OK
    sendMetric(1, initialTime - 1, false);
    assertTrue(avgSubAlarmStats.evaluateAndSlideWindow(initialTime += 60, 10));
    assertEquals(avgSubAlarmStats.getSubAlarm().getState(), AlarmState.OK);

    // Must slide 8 times total from the last added value to trigger UNDETERMINED. This is
    // equivalent to the behavior in CloudWatch for an alarm with 3 evaluation periods. 2 more
    // slides to move the value outside of the window and 6 more to exceed the observation
    // threshold.
    for (int i = 0; i < 7; i++) {
      assertFalse(avgSubAlarmStats.evaluateAndSlideWindow(initialTime += 60, 10));
    }
    assertTrue(avgSubAlarmStats.evaluateAndSlideWindow(initialTime += 60, 10));
    assertEquals(avgSubAlarmStats.getSubAlarm().getState(), AlarmState.UNDETERMINED);
    sendMetric(5, initialTime - 1, false);
  }

  private void sendMetric(double value, long timestamp, boolean expected) {
    assertTrue(avgSubAlarmStats.addValue(value, timestamp));
    assertEquals(avgSubAlarmStats.evaluateAndSlideWindow(timestamp, timestamp), expected);
  }

  /**
   * Simulates the way a window will fill up in practice.
   */
  public void shouldImmediatelyEvaluate() {
    long initialTime = 11;

    // Need a different expression for this test
    avgExpression =
        new SubExpression(UUID.randomUUID().toString(),
            AlarmSubExpression.of("max(hpcs.compute.cpu{id=5}, 60) > 3 times 3"));
    avgSubAlarm = new SubAlarm("123", "1", avgExpression);
    assertEquals(avgSubAlarm.getState(), AlarmState.UNDETERMINED);
    avgSubAlarm.setNoState(true);
    avgSubAlarmStats = new SubAlarmStats(avgSubAlarm, avgExpression.getAlarmSubExpression().getPeriod());

    assertEquals(avgSubAlarmStats.getSubAlarm().getState(), AlarmState.UNDETERMINED);
    assertFalse(avgSubAlarmStats.evaluateAndSlideWindow(initialTime += 60, 1));
    assertEquals(avgSubAlarmStats.getSubAlarm().getState(), AlarmState.UNDETERMINED);
    assertFalse(avgSubAlarmStats.evaluateAndSlideWindow(initialTime += 60, 1));
    assertEquals(avgSubAlarmStats.getSubAlarm().getState(), AlarmState.UNDETERMINED);
    assertFalse(avgSubAlarmStats.evaluateAndSlideWindow(initialTime += 60, 1));
    assertEquals(avgSubAlarmStats.getSubAlarm().getState(), AlarmState.UNDETERMINED);

    // Add value and trigger OK
    sendMetric(1, initialTime - 1, false);
    assertTrue(avgSubAlarmStats.evaluateAndSlideWindow(initialTime += 60, 1));
    assertEquals(avgSubAlarmStats.getSubAlarm().getState(), AlarmState.OK);

    // Slide in some values that exceed the threshold
    sendMetric(5, initialTime - 1, false);
    assertFalse(avgSubAlarmStats.evaluateAndSlideWindow(initialTime += 60, 1));
    assertEquals(avgSubAlarmStats.getSubAlarm().getState(), AlarmState.OK);
    sendMetric(5, initialTime - 1, false);
    assertFalse(avgSubAlarmStats.evaluateAndSlideWindow(initialTime += 60, 1));

    // Trigger ALARM
    sendMetric(5, initialTime - 1, true);
    assertEquals(avgSubAlarmStats.getSubAlarm().getState(), AlarmState.ALARM);

    // Ensure it is still ALARM on next evaluation
    assertFalse(avgSubAlarmStats.evaluateAndSlideWindow(initialTime += 60, 1));
    assertEquals(avgSubAlarmStats.getSubAlarm().getState(), AlarmState.ALARM);

    // Add value and trigger OK
    sendMetric(1, initialTime - 1, false);
    assertTrue(avgSubAlarmStats.evaluateAndSlideWindow(initialTime += 60, 1));
    assertEquals(avgSubAlarmStats.getSubAlarm().getState(), AlarmState.OK);

    // Must slide 8 times total from the last added value to trigger UNDETERMINED. This is
    // equivalent to the behavior in CloudWatch for an alarm with 3 evaluation periods. 2 more
    // slides to move the value outside of the window and 6 more to exceed the observation
    // threshold.
    for (int i = 0; i < 7; i++) {
      assertFalse(avgSubAlarmStats.evaluateAndSlideWindow(initialTime += 60, 1));
    }
    assertTrue(avgSubAlarmStats.evaluateAndSlideWindow(initialTime += 60, 1));
    assertEquals(avgSubAlarmStats.getSubAlarm().getState(), AlarmState.UNDETERMINED);

    // Now test that future buckets are evaluated
    // Set the current bucket to ALARM
    sendMetric(5, initialTime - 1, false);
    assertEquals(avgSubAlarmStats.getSubAlarm().getState(), AlarmState.UNDETERMINED);
    // Set the future bucket of current + 2 to ALARM
    sendMetric(5, initialTime + 120, false);
    assertEquals(avgSubAlarmStats.getSubAlarm().getState(), AlarmState.UNDETERMINED);
    // Set the future bucket of current + 1 to ALARM. That will trigger the
    // SubAlarm to go to ALARM
    sendMetric(5, initialTime + 60, true);
    assertEquals(avgSubAlarmStats.getSubAlarm().getState(), AlarmState.ALARM);
  }

  public void shouldAlarmIfAllSlotsAlarmed() {
    long initialTime = 11;

    assertEquals(avgSubAlarmStats.getSubAlarm().getState(), AlarmState.UNDETERMINED);

    assertFalse(avgSubAlarmStats.evaluateAndSlideWindow(initialTime += 60, 1));

    sendMetric(5, initialTime - 1, false);
    assertFalse(avgSubAlarmStats.evaluateAndSlideWindow(initialTime += 60, 1));

    sendMetric(5, initialTime - 1, false);
    assertFalse(avgSubAlarmStats.evaluateAndSlideWindow(initialTime += 60, 1));

    sendMetric(5, initialTime - 1, false);
    assertTrue(avgSubAlarmStats.evaluateAndSlideWindow(initialTime += 60, 1));
    assertEquals(avgSubAlarmStats.getSubAlarm().getState(), AlarmState.ALARM);
  }

  public void testEmptyWindowObservationThreshold() {
    avgExpression =
        new SubExpression(UUID.randomUUID().toString(),
            AlarmSubExpression.of("avg(hpcs.compute.cpu{id=5}) > 3 times 3"));
    avgSubAlarm = new SubAlarm("123", "1", avgExpression);
    assertEquals(avgSubAlarm.getState(), AlarmState.UNDETERMINED);
    SubAlarmStats saStats = new SubAlarmStats(avgSubAlarm, (System.currentTimeMillis() / 1000) + 60);
    assertEquals(saStats.emptyWindowObservationThreshold, 6);
  }

  public void checkUpdateSubAlarm() {
    // Can keep data with threshold change
    verifyUpdateSubAlarm(avgExpression.getAlarmSubExpression().getExpression().replace("> 3", "> 6"), 100.0);
    // Can keep data with operator change
    verifyUpdateSubAlarm(avgExpression.getAlarmSubExpression().getExpression().replace("< 3", "< 6"), 100.0);
    // Have to flush data with function change
    verifyUpdateSubAlarm(avgExpression.getAlarmSubExpression().getExpression().replace("avg", "max"), Double.NaN);
    // Have to flush data with periods change
    verifyUpdateSubAlarm(avgExpression.getAlarmSubExpression().getExpression().replace("times 3", "times 2"), Double.NaN);
    // Have to flush data with period change
    verifyUpdateSubAlarm(avgExpression.getAlarmSubExpression().getExpression().replace(", 60", ", 120"), Double.NaN);
  }

  private void verifyUpdateSubAlarm(String newExpressionString, double expectedValue) {
    final AlarmSubExpression newExpression = AlarmSubExpression.of(newExpressionString);
    assertNotEquals(newExpression, avgExpression.getAlarmSubExpression().getExpression());
    int timestamp = avgExpression.getAlarmSubExpression().getPeriod() / 2;
    sendMetric(100.00, timestamp, false);
    assertEquals(avgSubAlarmStats.getStats().getValue(timestamp), 100.0);
    avgSubAlarmStats.updateSubAlarm(newExpression, avgExpression.getAlarmSubExpression().getPeriod());
    assertEquals(avgSubAlarmStats.getStats().getValue(timestamp), expectedValue);
    assertTrue(avgSubAlarm.isNoState());
  }


  public void checkLongPeriod() {
    final SubExpression subExpr = new SubExpression(UUID.randomUUID().toString(),
        AlarmSubExpression.of("sum(hpcs.compute.mem{id=5}, 120) >= 96"));

    final SubAlarm subAlarm = new SubAlarm("42", "4242", subExpr);
    assertEquals(subAlarm.getState(), AlarmState.UNDETERMINED);

    long t1 = 0;
    final SubAlarmStats stats = new SubAlarmStats(subAlarm, t1 + subExpr.getAlarmSubExpression().getPeriod());
    for (int i = 0; i < 360; i++) {
      t1++;
      stats.addValue(1.0, t1);
      if ((t1 % 60) == 2) {
        stats.evaluateAndSlideWindow(t1, 1);
        if (i <= subExpr.getAlarmSubExpression().getPeriod()) {
          // Haven't waited long enough to evaluate
          assertEquals(stats.getSubAlarm().getState(), AlarmState.UNDETERMINED);
        } else {
          assertEquals(stats.getSubAlarm().getState(), AlarmState.ALARM);
        }
      }
    }
  }

  public void shouldNotAllowSubAlarmTransitionFromOkToUndetermined_Deterministic() {
    final String expression = "sum(log.error{path=/var/log/test.log},deterministic,240) >= 100";
    final SubExpression subExpr = new SubExpression(
        UUID.randomUUID().toString(),
        AlarmSubExpression.of(expression)
    );

    final SubAlarm subAlarm = new SubAlarm("42", "4242", subExpr);
    final SubAlarmStats stats = new SubAlarmStats(subAlarm, subExpr.getAlarmSubExpression().getPeriod());

    // initially in OK because deterministic
    assertTrue(stats.getSubAlarm().isDeterministic());
    assertEquals(stats.getSubAlarm().getState(), AlarmState.OK);

    int t1 = 0;
    for (int i = 0; i < 1080; i++) {
      t1++;
      stats.getStats().addValue(1.0, t1, false);
      if ((t1 % 60) == 2) {
        stats.evaluateAndSlideWindow(t1, 1);
        if (i <= subExpr.getAlarmSubExpression().getPeriod()) {
          // Haven't waited long enough to evaluate,
          // but this is deterministic sub alarm, so it should be in ok
          assertEquals(stats.getSubAlarm().getState(), AlarmState.OK);
        } else {
          assertEquals(stats.getSubAlarm().getState(), AlarmState.ALARM);
        }
      }
    }
  }

}
