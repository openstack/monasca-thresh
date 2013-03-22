package com.hpcloud.maas.infrastructure.thresholding;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * @author Jonathan Halterman
 */
@Test
public class AlarmThresholdingBoltTest {
  private AlarmThresholdingBolt bolt;

  @BeforeMethod
  protected void beforeMethod() {
    bolt = new AlarmThresholdingBolt();
  }

  public void shouldEvaluateThreshold() {

  }
}
