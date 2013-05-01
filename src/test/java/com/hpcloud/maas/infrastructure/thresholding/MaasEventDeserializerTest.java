package com.hpcloud.maas.infrastructure.thresholding;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import java.util.Collections;

import org.testng.annotations.Test;

import com.hpcloud.maas.common.event.AlarmDeletedEvent;
import com.hpcloud.util.Serialization;

/**
 * @author Jonathan Halterman
 */
@Test
public class MaasEventDeserializerTest {
  private MaasEventDeserializer deserializer = new MaasEventDeserializer();

  public void shouldDeserialize() {
    Object event = new AlarmDeletedEvent("abc", "123", null);
    String serialized = Serialization.toJson(event);
    Object deserialized = deserializer.deserialize(serialized);
    Object expected = Collections.singletonList(Collections.singletonList(event));
    assertEquals(deserialized, expected);
  }
  
  public void shouldReturnNullOnDeserializeUnknownEvent() {
    String unknownEventJson = "{\"alarm-foo-deleted\":{\"tenantId\":\"abc\",\"alarmId\":\"123\"}}";
    assertNull(deserializer.deserialize(unknownEventJson));
  }
}
