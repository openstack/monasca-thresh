package com.hpcloud.mon.infrastructure.thresholding.deserializer;

import static org.testng.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.testng.annotations.Test;

import com.hpcloud.mon.common.model.metric.Metric;
import com.hpcloud.mon.common.model.metric.Metrics;

/**
 * @author Jonathan Halterman
 */
@Test
public class MaasMetricDeserializerTest {
  private MaasMetricDeserializer deserializer = new MaasMetricDeserializer();

  public void shouldDeserialize() {
    Metric metric = new Metric("bob", null, 123, 5.0);
    List<List<?>> metrics = deserializer.deserialize(Metrics.toJson(metric).getBytes());
    assertEquals(metrics, Collections.singletonList(Arrays.asList(metric.definition(), metric)));
  }
}
