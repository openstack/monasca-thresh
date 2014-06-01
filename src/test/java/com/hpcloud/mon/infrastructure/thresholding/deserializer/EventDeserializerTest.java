/*
 * Copyright (c) 2014 Hewlett-Packard Development Company, L.P.
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
package com.hpcloud.mon.infrastructure.thresholding.deserializer;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import java.util.Collections;

import org.testng.annotations.Test;

import com.hpcloud.mon.common.event.AlarmCreatedEvent;
import com.hpcloud.mon.common.event.AlarmDeletedEvent;
import com.hpcloud.mon.common.event.AlarmUpdatedEvent;
import com.hpcloud.mon.common.model.alarm.AlarmState;
import com.hpcloud.util.Serialization;

@Test
public class EventDeserializerTest {
  private static final String ALARM_EXPRESSION = "avg(hpcs.compute{instance_id=5,metric_name=cpu,device=1}, 1) > 5 times 3 OR avg(hpcs.compute{flavor_id=3,metric_name=mem}, 2) < 4 times 3";
  private static final String ALARM_NAME = "An Alarm";
  private static final String ALARM_DESCRIPTION = "An Alarm Description";
  private static final String ALARM_ID = "123";
  private static final String TENANT_ID = "abc";
  private EventDeserializer deserializer = new EventDeserializer();

  public void shouldDeserializeAlarmDeletedEvent() {
    roundTrip(new AlarmDeletedEvent(TENANT_ID, ALARM_ID, null));
  }

  public void shouldDeserializeAlarmCreatedEvent() {
    roundTrip(new AlarmCreatedEvent(TENANT_ID, ALARM_ID, ALARM_NAME, ALARM_EXPRESSION, null));
  }

  public void shouldDeserializeAlarmUpdatedEvent() {
    roundTrip(new AlarmUpdatedEvent(TENANT_ID, ALARM_ID, ALARM_NAME, ALARM_DESCRIPTION, ALARM_EXPRESSION,
              AlarmState.OK, AlarmState.OK, false, null, null, null, null));
  }

  private void roundTrip(Object event) {
    String serialized = Serialization.toJson(event);
    Object deserialized = deserializer.deserialize(serialized.getBytes());
    Object expected = Collections.singletonList(Collections.singletonList(event));
    assertEquals(deserialized, expected);
  }
  
  public void shouldReturnNullOnDeserializeUnknownEvent() {
    String unknownEventJson = "{\"alarm-foo-deleted\":{\"tenantId\":\"abc\",\"alarmId\":\"123\"}}";
    assertNull(deserializer.deserialize(unknownEventJson.getBytes()));
  }
}
