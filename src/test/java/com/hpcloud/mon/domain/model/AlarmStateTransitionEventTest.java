package com.hpcloud.mon.domain.model;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import com.hpcloud.mon.domain.model.AlarmStateTransitionEvent;
import com.hpcloud.util.Serialization;

@Test
public class AlarmStateTransitionEventTest {
  public void shouldSerialize() {
    assertEquals(
        Serialization.toJson(new AlarmStateTransitionEvent()),
        "{\"alarm-transitioned\":{\"tenantId\":null,\"alarmId\":null,\"alarmName\":null,\"alarmDescription\":null,\"oldState\":null,\"newState\":null,\"stateChangeReason\":null,\"timestamp\":0}}");
  }
}
