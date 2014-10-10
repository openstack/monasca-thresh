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

package monasca.thresh.infrastructure.thresholding.deserializer;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import com.hpcloud.mon.common.event.AlarmDefinitionCreatedEvent;
import com.hpcloud.mon.common.event.AlarmDeletedEvent;
import com.hpcloud.mon.common.event.AlarmUpdatedEvent;
import com.hpcloud.mon.common.model.alarm.AlarmExpression;
import com.hpcloud.mon.common.model.alarm.AlarmState;
import com.hpcloud.mon.common.model.alarm.AlarmSubExpression;
import com.hpcloud.mon.common.model.metric.MetricDefinition;
import com.hpcloud.util.Serialization;

import monasca.thresh.domain.model.Alarm;
import monasca.thresh.domain.model.AlarmDefinition;
import monasca.thresh.domain.model.SubAlarm;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@Test
public class EventDeserializerTest {
  private static final String ALARM_EXPRESSION =
      "avg(cpu{instance_id=5,device=1}, 1) > 5 times 3 OR avg(load{instance_id=5}, 2) < 4 times 3";
  private static final String ALARM_NAME = "An Alarm";
  private static final String ALARM_DESCRIPTION = "An Alarm Description";
  private static final String ALARM_ID = "123";
  private static final String ALARM_DEFINITION_ID = "456";
  private static final String TENANT_ID = "abc";
  private EventDeserializer deserializer = new EventDeserializer();
  private final Map<String, AlarmSubExpression> subAlarms = new HashMap<>();

  @BeforeMethod
  protected void beforeMethod() {
    final AlarmDefinition alarmDef =
        new AlarmDefinition("42", TENANT_ID, ALARM_NAME, ALARM_DESCRIPTION, new AlarmExpression(
            ALARM_EXPRESSION), "LOW", true, Arrays.asList("instance_id"));
    final Alarm alarm = new Alarm(ALARM_ID, alarmDef, AlarmState.OK);
    for (final SubAlarm subAlarm : alarm.getSubAlarms()) {
      subAlarms.put(subAlarm.getId(), subAlarm.getExpression());
    }
  }

  public void shouldDeserializeAlarmDeletedEvent() {
    final Map<String, String> dimensions = new HashMap<String, String>();
    dimensions.put("pet", "dino");
    final MetricDefinition md = new MetricDefinition("load", dimensions);
    roundTrip(new AlarmDeletedEvent(TENANT_ID, ALARM_ID, Arrays.asList(md), ALARM_DEFINITION_ID,
        subAlarms));
  }

  public void shouldDeserializeAlarmDefinitionCreatedEvent() {
    roundTrip(new AlarmDefinitionCreatedEvent(TENANT_ID, ALARM_ID, ALARM_NAME, ALARM_DESCRIPTION,
        ALARM_EXPRESSION, null, Arrays.asList("hostname", "dev")));
  }

  public void shouldDeserializeAlarmUpdatedEvent() {
    final List<MetricDefinition> alarmMetrics = new LinkedList<>();
    final Map<String, String> dimensions = new HashMap<>();
    dimensions.put("monasca", "project");
    final MetricDefinition md = new MetricDefinition("load", dimensions);
    alarmMetrics.add(md);
    roundTrip(new AlarmUpdatedEvent(ALARM_ID, ALARM_DEFINITION_ID, TENANT_ID, alarmMetrics,
        subAlarms, AlarmState.OK, AlarmState.UNDETERMINED));
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
