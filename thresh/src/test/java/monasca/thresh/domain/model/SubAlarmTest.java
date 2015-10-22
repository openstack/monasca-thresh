/*
 * Copyright (c) 2015 Hewlett-Packard Development Company, L.P.
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

import monasca.common.model.alarm.AlarmSubExpression;

import org.testng.annotations.Test;

import java.util.UUID;

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

  private void checkExpression(String expressionString, boolean expected) {
    final SubExpression expression =
        new SubExpression(UUID.randomUUID().toString(),
            AlarmSubExpression.of(expressionString));
    final SubAlarm subAlarm = new SubAlarm(UUID.randomUUID().toString(), "1", expression);
    assertEquals(subAlarm.canEvaluateImmediately(), expected);
  }
}
