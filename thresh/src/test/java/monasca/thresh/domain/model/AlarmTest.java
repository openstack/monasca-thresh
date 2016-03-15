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
import static org.testng.Assert.assertTrue;

import monasca.common.model.alarm.AggregateFunction;
import monasca.common.model.alarm.AlarmExpression;
import monasca.common.model.alarm.AlarmOperator;
import monasca.common.model.alarm.AlarmState;
import monasca.common.model.alarm.AlarmSubExpression;
import monasca.common.model.alarm.AlarmTransitionSubAlarm;

import monasca.common.model.metric.MetricDefinition;

import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Test
public class AlarmTest {

  public void shouldBeUndeterminedIfAnySubAlarmIsUndetermined() {
    AlarmExpression expr =
        new AlarmExpression(
            "avg(hpcs.compute{instance_id=5,metric_name=cpu,device=1}, 1) > 5 times 3 AND avg(hpcs.compute{flavor_id=3,metric_name=mem}, 2) < 4 times 3");
    final Alarm alarm = createAlarm(expr);
    final Iterator<SubAlarm> iter = alarm.getSubAlarms().iterator();
    SubAlarm subAlarm1 = iter.next();
    subAlarm1.setState(AlarmState.UNDETERMINED);
    SubAlarm subAlarm2 = iter.next();
    subAlarm2.setState(AlarmState.ALARM);

    assertFalse(alarm.evaluate(expr));
    assertEquals(alarm.getState(), AlarmState.UNDETERMINED);
  }

  private Alarm createAlarm(AlarmExpression expr) {
    final AlarmDefinition alarmDefinition = new AlarmDefinition("42", "Test Def", "", expr, "LOW", true, new ArrayList<String>(0));
    return new Alarm(alarmDefinition);
  }

  public void shouldEvaluateExpressionWithBooleanAnd() {
    AlarmExpression expr =
        new AlarmExpression(
            "avg(hpcs.compute{instance_id=5,metric_name=cpu,device=1}, 1) > 5 times 3 AND avg(hpcs.compute{flavor_id=3,metric_name=mem}, 2) < 4 times 3");
    final Alarm alarm = createAlarm(expr);

    final Iterator<SubAlarm> iter = alarm.getSubAlarms().iterator();
    SubAlarm subAlarm1 = iter.next();
    SubAlarm subAlarm2 = iter.next();

    assertFalse(alarm.evaluate(expr));
    assertEquals(alarm.getState(), AlarmState.UNDETERMINED);

    subAlarm1.setState(AlarmState.OK);
    assertFalse(alarm.evaluate(expr));

    // UNDETERMINED -> OK
    subAlarm2.setState(AlarmState.OK);
    assertTrue(alarm.evaluate(expr));
    assertEquals(alarm.getState(), AlarmState.OK);

    subAlarm2.setState(AlarmState.ALARM);
    assertFalse(alarm.evaluate(expr));

    // OK -> ALARM
    subAlarm1.setState(AlarmState.ALARM);
    assertTrue(alarm.evaluate(expr));
    assertEquals(alarm.getState(), AlarmState.ALARM);

    // ALARM -> UNDETERMINED
    subAlarm1.setState(AlarmState.UNDETERMINED);
    assertTrue(alarm.evaluate(expr));
    assertEquals(alarm.getState(), AlarmState.UNDETERMINED);
  }

  public void shouldEvaluateExpressionWithBooleanOr() {
    AlarmExpression expr =
        new AlarmExpression(
            "avg(hpcs.compute{instance_id=5,metric_name=cpu,device=1}, 1) > 5 times 3 OR avg(hpcs.compute{flavor_id=3,metric_name=mem}, 2) < 4 times 3");
    final Alarm alarm = createAlarm(expr);

    final Iterator<SubAlarm> iter = alarm.getSubAlarms().iterator();
    SubAlarm subAlarm1 = iter.next();
    SubAlarm subAlarm2 = iter.next();

    assertFalse(alarm.evaluate(expr));
    assertEquals(alarm.getState(), AlarmState.UNDETERMINED);

    subAlarm1.setState(AlarmState.ALARM);
    assertFalse(alarm.evaluate(expr));

    // UNDETERMINED -> ALARM
    subAlarm2.setState(AlarmState.OK);
    assertTrue(alarm.evaluate(expr));
    assertEquals(alarm.getState(), AlarmState.ALARM);

    // ALARM -> OK
    subAlarm1.setState(AlarmState.OK);
    subAlarm2.setState(AlarmState.OK);
    assertTrue(alarm.evaluate(expr));
    assertEquals(alarm.getState(), AlarmState.OK);

    // OK -> ALARM
    subAlarm2.setState(AlarmState.ALARM);
    assertTrue(alarm.evaluate(expr));
    assertEquals(alarm.getState(), AlarmState.ALARM);

    // ALARM -> ALARM
    assertFalse(alarm.evaluate(expr));
    assertEquals(alarm.getState(), AlarmState.ALARM);

    // ALARM -> UNDETERMINED
    subAlarm2.setState(AlarmState.UNDETERMINED);
    assertTrue(alarm.evaluate(expr));
    assertEquals(alarm.getState(), AlarmState.UNDETERMINED);
  }

  public void shouldBuiltStateChangeReason() {
    AlarmExpression expr =
        new AlarmExpression(
            "avg(hpcs.compute{instance_id=5,metric_name=cpu,device=1}, 1) > 5 times 3 OR avg(hpcs.compute{flavor_id=3,metric_name=mem}, 2) < 4 times 3");
    Alarm alarm = new Alarm();

    List<AlarmTransitionSubAlarm> transitionSubAlarms = new ArrayList<>();
    transitionSubAlarms.add(new AlarmTransitionSubAlarm(expr.getSubExpressions().get(0), AlarmState.UNDETERMINED, new ArrayList<Double>()));
    transitionSubAlarms.add(new AlarmTransitionSubAlarm(expr.getSubExpressions().get(1), AlarmState.ALARM, new ArrayList<Double>()));
    alarm.setTransitionSubAlarms(transitionSubAlarms);

    assertEquals(
        alarm.buildStateChangeReason(AlarmState.UNDETERMINED),
        "No data was present for the sub-alarms: avg(hpcs.compute{device=1, instance_id=5, metric_name=cpu}, 1) > 5.0 times 3");

    assertEquals(
        alarm.buildStateChangeReason(AlarmState.ALARM),
        "Thresholds were exceeded for the sub-alarms: avg(hpcs.compute{flavor_id=3, metric_name=mem}, 2) < 4.0 times 3 with the values: []");
  }

  /**
   * This test is here because this case happened in the Threshold Engine. The AlarmExpression
   * resulted in a MetricDefinition with null dimensions and SubAlarm had empty dimensions and that
   * didn't match causing an IllegalArgumentException. The AlarmSubExpressionListener has been
   * changed to always generate empty dimensions and not null. This test will verify that logic
   * is still working.
   */
  public void testDimensions() {
    final AlarmExpression expression = AlarmExpression.of("max(cpu_system_perc) > 1");
    final MetricDefinition metricDefinition =
        new MetricDefinition("cpu_system_perc", new HashMap<String, String>());
    final AlarmSubExpression ase =
        new AlarmSubExpression(AggregateFunction.MAX, metricDefinition, AlarmOperator.GT, 1, 60, 1);
    final SubAlarm subAlarm = new SubAlarm("123", "456", new SubExpression(UUID.randomUUID().toString(), ase));
    final Map<AlarmSubExpression, Boolean> subExpressionValues =
        new HashMap<>();
    subExpressionValues.put(subAlarm.getExpression(), true);
    assertEquals(expression.getSubExpressions().get(0).getMetricDefinition().hashCode(),
        metricDefinition.hashCode());

    // Handle ALARM state
    assertTrue(expression.evaluate(subExpressionValues));
  }

  public void testShouldInitiallyProceedToOKIfAllSubAlarmsAreDeterministic() {
    final String expression1 = "count(log.error{path=/var/log/test.log}, deterministic, 1) > 10";
    final String expression2 = "count(log.warning{path=/var/log/test.log}, deterministic, 1) > 5";
    final String expression = String.format("%s or %s", expression1, expression2);

    final AlarmExpression expr = new AlarmExpression(expression);
    final Alarm alarm = this.createAlarm(expr);

    assertFalse(alarm.evaluate(expr));
    assertTrue(alarm.isDeterministic());
    assertEquals(alarm.getState(), AlarmState.OK);
  }

  public void testShouldNotInitiallyProceedToOKIfNotAllSubAlarmsAreDeterministic() {
    final String expression1 = "count(log.error{path=/var/log/test.log}, deterministic, 1) > 10";
    final String expression2 = "count(log.warning{path=/var/log/test.log}, deterministic, 1) > 5";
    final String expression3 = "count(log.debug{path=/var/log/test.log}, 1) > 1";
    final String expression = String.format("(%s or %s) and %s",
        expression1,
        expression2,
        expression3
    );

    final AlarmExpression expr = new AlarmExpression(expression);
    final Alarm alarm = this.createAlarm(expr);

    assertFalse(alarm.evaluate(expr));
    assertFalse(alarm.isDeterministic());
    assertEquals(alarm.getState(), AlarmState.UNDETERMINED);
  }

  public void testShouldStayInAlarmDeterministic() {
    final String expression = "count(log.error{path=/var/log/test.log}, deterministic, 1) > 5";
    final AlarmExpression expr = new AlarmExpression(expression);
    final Alarm alarm = this.createAlarm(expr);
    final Iterator<SubAlarm> iter = alarm.getSubAlarms().iterator();

    alarm.setState(AlarmState.ALARM);

    SubAlarm subAlarm1 = iter.next();
    subAlarm1.setState(AlarmState.ALARM);

    assertFalse(alarm.evaluate(expr));
    assertTrue(alarm.isDeterministic());
    assertEquals(alarm.getState(), AlarmState.ALARM);
  }

  public void testShouldStayInOkDeterministic() {
    final String expression = "count(log.error{path=/var/log/test.log}, deterministic, 1) > 5";
    final AlarmExpression expr = new AlarmExpression(expression);
    final Alarm alarm = this.createAlarm(expr);

    alarm.setState(AlarmState.OK);

    assertFalse(alarm.evaluate(expr));
    assertTrue(alarm.isDeterministic());
    assertEquals(alarm.getState(), AlarmState.OK);
  }

  public void testShouldEnterOkFromAlarmDeterministic() {
    final String expression = "count(log.error{path=/var/log/test.log}, deterministic, 1) > 5";
    final AlarmExpression expr = new AlarmExpression(expression);
    final Alarm alarm = this.createAlarm(expr);
    final Iterator<SubAlarm> iter = alarm.getSubAlarms().iterator();

    alarm.setState(AlarmState.ALARM);

    SubAlarm subAlarm1 = iter.next();
    subAlarm1.setState(AlarmState.OK);

    assertTrue(alarm.evaluate(expr));
    assertTrue(alarm.isDeterministic());
    assertEquals(alarm.getState(), AlarmState.OK);
  }

  public void testShouldInitiallySetOKForDeterministic() {
    assertEquals(
        this.createAlarm(AlarmExpression.of("count(log.error,deterministic) > 2")).getState(),
        AlarmState.OK
    );
  }

  public void testShouldInitiallySetUndeterminedForNonDeterministic() {
    assertEquals(
        this.createAlarm(AlarmExpression.of("count(log.error) > 2")).getState(),
        AlarmState.UNDETERMINED
    );
  }

}
