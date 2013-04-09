package com.hpcloud.maas.domain.model;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import com.hpcloud.util.Serialization;

@Test
public class AlarmStateTransitionEventTest {
  public void shouldSerialize() {
    assertEquals(
        Serialization.toJson(new AlarmStateTransitionEvent()),
        "{\"alarm-transitioned\":{\"tenantId\":null,\"alarmId\":null,\"alarmName\":null,\"oldState\":null,\"newState\":null,\"stateChangeReason\":null,\"timestamp\":0}}");
  }
}
