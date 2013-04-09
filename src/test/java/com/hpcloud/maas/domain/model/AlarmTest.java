package com.hpcloud.maas.domain.model;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.testng.annotations.Test;

import com.hpcloud.maas.common.model.alarm.AlarmExpression;
import com.hpcloud.maas.common.model.alarm.AlarmState;

/**
 * @author Jonathan Halterman
 */
@Test
public class AlarmTest {
  public void shouldEvaluateExpressionWithBooleanAnd() {
    AlarmExpression expr = new AlarmExpression(
        "avg(compute:cpu:1:{instance_id=5}, 1) > 5 times 3 AND avg(compute:mem:{flavor_id=3}, 2) < 4 times 3");
    SubAlarm subAlarm1 = new SubAlarm("1", "123", expr.getSubExpressions().get(0));
    SubAlarm subAlarm2 = new SubAlarm("1", "456", expr.getSubExpressions().get(1));

    Alarm alarm = new Alarm("1", "joe", "test alarm", expr, Arrays.asList(subAlarm1, subAlarm2),
        AlarmState.UNDETERMINED);

    assertFalse(alarm.evaluate());
    assertEquals(alarm.getState(), AlarmState.UNDETERMINED);

    subAlarm1.setState(AlarmState.OK);
    assertFalse(alarm.evaluate());

    // UNDETERMINED -> OK
    subAlarm2.setState(AlarmState.OK);
    assertTrue(alarm.evaluate());
    assertEquals(alarm.getState(), AlarmState.OK);

    subAlarm2.setState(AlarmState.ALARM);
    assertFalse(alarm.evaluate());

    // OK -> ALARM
    subAlarm1.setState(AlarmState.ALARM);
    assertTrue(alarm.evaluate());
    assertEquals(alarm.getState(), AlarmState.ALARM);

    // ALARM -> UNDETERMINED
    subAlarm1.setState(AlarmState.UNDETERMINED);
    assertTrue(alarm.evaluate());
    assertEquals(alarm.getState(), AlarmState.UNDETERMINED);
  }

  public void shouldEvaluateExpressionWithBooleanOr() {
    AlarmExpression expr = new AlarmExpression(
        "avg(compute:cpu:1:{instance_id=5}, 1) > 5 times 3 OR avg(compute:mem:{flavor_id=3}, 2) < 4 times 3");
    SubAlarm subAlarm1 = new SubAlarm("1", "123", expr.getSubExpressions().get(0));
    SubAlarm subAlarm2 = new SubAlarm("1", "456", expr.getSubExpressions().get(1));

    Alarm alarm = new Alarm("1", "joe", "test alarm", expr, Arrays.asList(subAlarm1, subAlarm2),
        AlarmState.UNDETERMINED);

    assertFalse(alarm.evaluate());
    assertEquals(alarm.getState(), AlarmState.UNDETERMINED);

    subAlarm1.setState(AlarmState.ALARM);
    assertFalse(alarm.evaluate());

    // UNDETERMINED -> ALARM
    subAlarm2.setState(AlarmState.OK);
    assertTrue(alarm.evaluate());
    assertEquals(alarm.getState(), AlarmState.ALARM);

    // ALARM -> OK
    subAlarm1.setState(AlarmState.OK);
    subAlarm2.setState(AlarmState.OK);
    assertTrue(alarm.evaluate());
    assertEquals(alarm.getState(), AlarmState.OK);

    // OK -> ALARM
    subAlarm2.setState(AlarmState.ALARM);
    assertTrue(alarm.evaluate());
    assertEquals(alarm.getState(), AlarmState.ALARM);

    // ALARM -> UNDETERMINED
    subAlarm2.setState(AlarmState.UNDETERMINED);
    assertTrue(alarm.evaluate());
    assertEquals(alarm.getState(), AlarmState.UNDETERMINED);
  }

  public void shouldBuiltStateChangeReason() {
    AlarmExpression expr = new AlarmExpression(
        "avg(compute:cpu:1:{instance_id=5}, 1) > 5 times 3 OR avg(compute:mem:{flavor_id=3}, 2) < 4 times 3");
    SubAlarm subAlarm1 = new SubAlarm("1", "123", expr.getSubExpressions().get(0));
    SubAlarm subAlarm2 = new SubAlarm("1", "456", expr.getSubExpressions().get(1));
    List<String> expressions = Arrays.asList(subAlarm1.getExpression().toString(),
        subAlarm2.getExpression().toString());

    assertEquals(
        Alarm.buildStateChangeReason(AlarmState.UNDETERMINED, expressions),
        "No data was present for the sub-alarms: [average(compute:cpu:1:{instance_id=5}, 1) >= 5.0 times 3, average(compute:mem:{flavor_id=3}, 2) < 4.0 times 3]");

    assertEquals(
        Alarm.buildStateChangeReason(AlarmState.ALARM, expressions),
        "Thresholds were exceeded for the sub-alarms: [average(compute:cpu:1:{instance_id=5}, 1) >= 5.0 times 3, average(compute:mem:{flavor_id=3}, 2) < 4.0 times 3]");
  }
}
