/*
 * Copyright (c) 2014 Hewlett-Packard Development Company, L.P.
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
  private SubExpression expression;
  private SubAlarm subAlarm;
  private SubAlarmStats subAlarmStats;

  @BeforeMethod
  protected void beforeMethod() {
    expression =
        new SubExpression(UUID.randomUUID().toString(),
            AlarmSubExpression.of("avg(hpcs.compute.cpu{id=5}, 60) > 3 times 3"));
    subAlarm = new SubAlarm("123", "1", expression);
    subAlarm.setNoState(true);
    subAlarmStats = new SubAlarmStats(subAlarm, expression.getAlarmSubExpression().getPeriod());
  }

  public void shouldBeOkIfAnySlotsInViewAreBelowThreshold() {
    sendMetric(5, 1, false);
    assertFalse(subAlarmStats.evaluateAndSlideWindow(62, 1));
    assertEquals(subAlarmStats.getSubAlarm().getState(), AlarmState.UNDETERMINED);

    sendMetric(1, 62, false);
    assertTrue(subAlarmStats.evaluateAndSlideWindow(122, 1));
    // This went to OK because at least one period is under the threshold
    assertEquals(subAlarmStats.getSubAlarm().getState(), AlarmState.OK);

    sendMetric(5, 123, false);
    assertFalse(subAlarmStats.evaluateAndSlideWindow(182, 1));
    // Still one under the threshold
    assertEquals(subAlarmStats.getSubAlarm().getState(), AlarmState.OK);
  }

  public void shouldBeAlarmedIfAllSlotsInViewExceedThreshold() {
    sendMetric(5, 1, false);
    assertFalse(subAlarmStats.evaluateAndSlideWindow(62, 1));
    assertEquals(subAlarmStats.getSubAlarm().getState(), AlarmState.UNDETERMINED);

    sendMetric(5, 62, false);
    assertFalse(subAlarmStats.evaluateAndSlideWindow(122, 1));
    assertEquals(subAlarmStats.getSubAlarm().getState(), AlarmState.UNDETERMINED);

    sendMetric(5, 123, false);
    assertTrue(subAlarmStats.evaluateAndSlideWindow(182, 1));
    assertEquals(subAlarmStats.getSubAlarm().getState(), AlarmState.ALARM);
  }

  /**
   * Simulates the way a window will fill up in practice.
   */
  public void shouldEvaluateAndSlideWindow() {
    long initialTime = 11;

    assertEquals(subAlarmStats.getSubAlarm().getState(), AlarmState.UNDETERMINED);
    assertFalse(subAlarmStats.evaluateAndSlideWindow(initialTime += 60, 10));
    assertEquals(subAlarmStats.getSubAlarm().getState(), AlarmState.UNDETERMINED);
    assertFalse(subAlarmStats.evaluateAndSlideWindow(initialTime += 60, 10));
    assertEquals(subAlarmStats.getSubAlarm().getState(), AlarmState.UNDETERMINED);
    assertFalse(subAlarmStats.evaluateAndSlideWindow(initialTime += 60, 10));
    assertEquals(subAlarmStats.getSubAlarm().getState(), AlarmState.UNDETERMINED);

    // Add value and trigger OK
    sendMetric(1, initialTime - 1, false);
    assertTrue(subAlarmStats.evaluateAndSlideWindow(initialTime += 60, 10));
    assertEquals(subAlarmStats.getSubAlarm().getState(), AlarmState.OK);

    // Slide in some values that exceed the threshold
    sendMetric(5, initialTime - 1, false);
    assertFalse(subAlarmStats.evaluateAndSlideWindow(initialTime += 60, 10));
    assertEquals(subAlarmStats.getSubAlarm().getState(), AlarmState.OK);
    sendMetric(5, initialTime - 1, false);
    assertFalse(subAlarmStats.evaluateAndSlideWindow(initialTime += 60, 10));
    assertEquals(subAlarmStats.getSubAlarm().getState(), AlarmState.OK);
    sendMetric(5, initialTime - 1, false);
    assertEquals(subAlarmStats.getSubAlarm().getState(), AlarmState.OK);

    // Trigger ALARM
    assertTrue(subAlarmStats.evaluateAndSlideWindow(initialTime += 60, 10));
    assertEquals(subAlarmStats.getSubAlarm().getState(), AlarmState.ALARM);

    // Add value and trigger OK
    sendMetric(1, initialTime - 1, false);
    assertTrue(subAlarmStats.evaluateAndSlideWindow(initialTime += 60, 10));
    assertEquals(subAlarmStats.getSubAlarm().getState(), AlarmState.OK);

    // Must slide 8 times total from the last added value to trigger UNDETERMINED. This is
    // equivalent to the behavior in CloudWatch for an alarm with 3 evaluation periods. 2 more
    // slides to move the value outside of the window and 6 more to exceed the observation
    // threshold.
    for (int i = 0; i < 7; i++) {
      assertFalse(subAlarmStats.evaluateAndSlideWindow(initialTime += 60, 10));
    }
    assertTrue(subAlarmStats.evaluateAndSlideWindow(initialTime += 60, 10));
    assertEquals(subAlarmStats.getSubAlarm().getState(), AlarmState.UNDETERMINED);
    sendMetric(5, initialTime - 1, false);
  }

  private void sendMetric(double value, long timestamp, boolean expected) {
    subAlarmStats.getStats().addValue(value, timestamp);
    assertEquals(subAlarmStats.evaluateAndSlideWindow(timestamp, timestamp), expected);
  }

  /**
   * Simulates the way a window will fill up in practice.
   */
  public void shouldImmediatelyEvaluate() {
    long initialTime = 11;

    // Need a different expression for this test
    expression =
        new SubExpression(UUID.randomUUID().toString(),
            AlarmSubExpression.of("max(hpcs.compute.cpu{id=5}, 60) > 3 times 3"));
    subAlarm = new SubAlarm("123", "1", expression);
    assertEquals(subAlarm.getState(), AlarmState.UNDETERMINED);
    subAlarm.setNoState(true);
    subAlarmStats = new SubAlarmStats(subAlarm, expression.getAlarmSubExpression().getPeriod());

    assertEquals(subAlarmStats.getSubAlarm().getState(), AlarmState.UNDETERMINED);
    assertFalse(subAlarmStats.evaluateAndSlideWindow(initialTime += 60, 1));
    assertEquals(subAlarmStats.getSubAlarm().getState(), AlarmState.UNDETERMINED);
    assertFalse(subAlarmStats.evaluateAndSlideWindow(initialTime += 60, 1));
    assertEquals(subAlarmStats.getSubAlarm().getState(), AlarmState.UNDETERMINED);
    assertFalse(subAlarmStats.evaluateAndSlideWindow(initialTime += 60, 1));
    assertEquals(subAlarmStats.getSubAlarm().getState(), AlarmState.UNDETERMINED);

    // Add value and trigger OK
    sendMetric(1, initialTime - 1, false);
    assertTrue(subAlarmStats.evaluateAndSlideWindow(initialTime += 60, 1));
    assertEquals(subAlarmStats.getSubAlarm().getState(), AlarmState.OK);

    // Slide in some values that exceed the threshold
    sendMetric(5, initialTime - 1, false);
    assertFalse(subAlarmStats.evaluateAndSlideWindow(initialTime += 60, 1));
    assertEquals(subAlarmStats.getSubAlarm().getState(), AlarmState.OK);
    sendMetric(5, initialTime - 1, false);
    assertFalse(subAlarmStats.evaluateAndSlideWindow(initialTime += 60, 1));

    // Trigger ALARM
    sendMetric(5, initialTime - 1, true);
    assertEquals(subAlarmStats.getSubAlarm().getState(), AlarmState.ALARM);

    // Ensure it is still ALARM on next evaluation
    assertFalse(subAlarmStats.evaluateAndSlideWindow(initialTime += 60, 1));
    assertEquals(subAlarmStats.getSubAlarm().getState(), AlarmState.ALARM);

    // Add value and trigger OK
    sendMetric(1, initialTime - 1, false);
    assertTrue(subAlarmStats.evaluateAndSlideWindow(initialTime += 60, 1));
    assertEquals(subAlarmStats.getSubAlarm().getState(), AlarmState.OK);

    // Must slide 8 times total from the last added value to trigger UNDETERMINED. This is
    // equivalent to the behavior in CloudWatch for an alarm with 3 evaluation periods. 2 more
    // slides to move the value outside of the window and 6 more to exceed the observation
    // threshold.
    for (int i = 0; i < 7; i++) {
      assertFalse(subAlarmStats.evaluateAndSlideWindow(initialTime += 60, 1));
    }
    assertTrue(subAlarmStats.evaluateAndSlideWindow(initialTime += 60, 1));
    assertEquals(subAlarmStats.getSubAlarm().getState(), AlarmState.UNDETERMINED);

    // Now test that future buckets are evaluated
    // Set the current bucket to ALARM
    sendMetric(5, initialTime - 1, false);
    assertEquals(subAlarmStats.getSubAlarm().getState(), AlarmState.UNDETERMINED);
    // Set the future bucket of current + 2 to ALARM
    sendMetric(5, initialTime + 120, false);
    assertEquals(subAlarmStats.getSubAlarm().getState(), AlarmState.UNDETERMINED);
    // Set the future bucket of current + 1 to ALARM. That will trigger the
    // SubAlarm to go to ALARM
    sendMetric(5, initialTime + 60, true);
    assertEquals(subAlarmStats.getSubAlarm().getState(), AlarmState.ALARM);
  }

  public void shouldAlarmIfAllSlotsAlarmed() {
    long initialTime = 11;

    assertEquals(subAlarmStats.getSubAlarm().getState(), AlarmState.UNDETERMINED);

    assertFalse(subAlarmStats.evaluateAndSlideWindow(initialTime += 60, 1));

    sendMetric(5, initialTime - 1, false);
    assertFalse(subAlarmStats.evaluateAndSlideWindow(initialTime += 60, 1));

    sendMetric(5, initialTime - 1, false);
    assertFalse(subAlarmStats.evaluateAndSlideWindow(initialTime += 60, 1));

    sendMetric(5, initialTime - 1, false);
    assertTrue(subAlarmStats.evaluateAndSlideWindow(initialTime += 60, 1));
    assertEquals(subAlarmStats.getSubAlarm().getState(), AlarmState.ALARM);
  }

  public void testEmptyWindowObservationThreshold() {
    expression =
        new SubExpression(UUID.randomUUID().toString(),
            AlarmSubExpression.of("avg(hpcs.compute.cpu{id=5}) > 3 times 3"));
    subAlarm = new SubAlarm("123", "1", expression);
    assertEquals(subAlarm.getState(), AlarmState.UNDETERMINED);
    SubAlarmStats saStats = new SubAlarmStats(subAlarm, (System.currentTimeMillis() / 1000) + 60);
    assertEquals(saStats.emptyWindowObservationThreshold, 6);
  }

  public void checkUpdateSubAlarm() {
    // Can keep data with threshold change
    verifyUpdateSubAlarm(expression.getAlarmSubExpression().getExpression().replace("> 3", "> 6"), 100.0);
    // Can keep data with operator change
    verifyUpdateSubAlarm(expression.getAlarmSubExpression().getExpression().replace("< 3", "< 6"), 100.0);
    // Have to flush data with function change
    verifyUpdateSubAlarm(expression.getAlarmSubExpression().getExpression().replace("avg", "max"), Double.NaN);
    // Have to flush data with periods change
    verifyUpdateSubAlarm(expression.getAlarmSubExpression().getExpression().replace("times 3", "times 2"), Double.NaN);
    // Have to flush data with period change
    verifyUpdateSubAlarm(expression.getAlarmSubExpression().getExpression().replace(", 60", ", 120"), Double.NaN);
  }

  private void verifyUpdateSubAlarm(String newExpressionString, double expectedValue) {
    final AlarmSubExpression newExpression = AlarmSubExpression.of(newExpressionString);
    assertNotEquals(newExpression, expression.getAlarmSubExpression().getExpression());
    int timestamp = expression.getAlarmSubExpression().getPeriod() / 2;
    sendMetric(100.00, timestamp, false);
    assertEquals(subAlarmStats.getStats().getValue(timestamp), 100.0);
    subAlarmStats.updateSubAlarm(newExpression, expression.getAlarmSubExpression().getPeriod());
    assertEquals(subAlarmStats.getStats().getValue(timestamp), expectedValue);
    assertTrue(subAlarm.isNoState());
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
      stats.getStats().addValue(1.0, t1);
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
      stats.getStats().addValue(1.0, t1);
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
