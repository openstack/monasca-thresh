/*
 * (C) Copyright 2015-2016 Hewlett Packard Enterprise Development LP
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

import java.util.UUID;

import org.testng.annotations.Test;

import monasca.common.model.alarm.AlarmState;
import monasca.common.model.alarm.AlarmSubExpression;

@Test
public class SubAlarmTest {

  public void checkCanEvaluateImmediately() {
    checkExpression("avg(hpcs.compute.cpu{id=5}, 60) > 3 times 3", false);
    checkExpression("avg(hpcs.compute.cpu{id=5}, 60) >= 3 times 3", false);
    checkExpression("avg(hpcs.compute.cpu{id=5}, 60) < 3 times 3", false);
    checkExpression("avg(hpcs.compute.cpu{id=5}, 60) <= 3 times 3", false);

    checkExpression("sum(hpcs.compute.cpu{id=5}, 60) > 3 times 3", false);
    checkExpression("sum(hpcs.compute.cpu{id=5}, 60) >= 3 times 3", false);
    checkExpression("sum(hpcs.compute.cpu{id=5}, 60) < 3 times 3", false);
    checkExpression("sum(hpcs.compute.cpu{id=5}, 60) <= 3 times 3", false);

    checkExpression("count(hpcs.compute.cpu{id=5}, 60) < 3 times 3", false);
    checkExpression("count(hpcs.compute.cpu{id=5}, 60) <= 3 times 3", false);
    checkExpression("count(hpcs.compute.cpu{id=5}, 60) > 3 times 3", true);
    checkExpression("count(hpcs.compute.cpu{id=5}, 60) >= 3 times 3", true);

    checkExpression("max(hpcs.compute.cpu{id=5}, 60) > 3 times 3", true);
    checkExpression("max(hpcs.compute.cpu{id=5}, 60) >= 3 times 3", true);
    checkExpression("max(hpcs.compute.cpu{id=5}, 60) < 3 times 3", false);
    checkExpression("max(hpcs.compute.cpu{id=5}, 60) <= 3 times 3", false);

    checkExpression("min(hpcs.compute.cpu{id=5}, 60) > 3 times 3", false);
    checkExpression("min(hpcs.compute.cpu{id=5}, 60) >= 3 times 3", false);
    checkExpression("min(hpcs.compute.cpu{id=5}, 60) < 3 times 3", true);
    checkExpression("min(hpcs.compute.cpu{id=5}, 60) <= 3 times 3", true);
  }

  public void shouldBeDeterministicIfKeywordFound() {
    final SubAlarm subAlarm = this.getSubAlarm("count(log.error{},deterministic,1)>1");
    assertTrue(subAlarm.isDeterministic());
    assertEquals(subAlarm.getState(), AlarmState.OK);
  }

  public void shouldBeNonDeterministicByDefault() {
    final SubAlarm subAlarm = this.getSubAlarm("min(hpcs.compute.cpu{id=5}, 60) > 3 times 3");
    assertFalse(subAlarm.isDeterministic());
    assertEquals(subAlarm.getState(), AlarmState.UNDETERMINED);
  }

  public void verifyDefaultAlarmState() {
    assertEquals(AlarmState.UNDETERMINED, SubAlarm.getDefaultState(false));
    assertEquals(AlarmState.OK, SubAlarm.getDefaultState(true));
  }

  private void checkExpression(String expressionString, boolean expected) {
    final SubAlarm subAlarm = this.getSubAlarm(expressionString);
    assertEquals(subAlarm.canEvaluateAlarmImmediately(), expected);
    assertEquals(subAlarm.getState(), AlarmState.UNDETERMINED);
    assertFalse(subAlarm.isDeterministic());
  }

  private SubAlarm getSubAlarm(final String expressionString) {
    final SubExpression expression =
        new SubExpression(UUID.randomUUID().toString(),
            AlarmSubExpression.of(expressionString));
    return new SubAlarm(UUID.randomUUID().toString(), "1", expression);
  }
}
