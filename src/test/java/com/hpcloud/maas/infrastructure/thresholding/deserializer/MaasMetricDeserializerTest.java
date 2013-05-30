package com.hpcloud.maas.infrastructure.thresholding.deserializer;

import static org.testng.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.testng.annotations.Test;

import com.hpcloud.maas.common.model.metric.FlatMetric;
import com.hpcloud.maas.common.model.metric.FlatMetrics;
import com.hpcloud.maas.common.model.metric.Metric;
import com.hpcloud.maas.infrastructure.thresholding.deserializer.MaasMetricDeserializer;

/**
 * @author Jonathan Halterman
 */
@Test
public class MaasMetricDeserializerTest {
  private MaasMetricDeserializer deserializer = new MaasMetricDeserializer();

  public void shouldDeserialize() {
    FlatMetric initial = new FlatMetric("bob", "test", "1", null, 123, 5.0);
    List<List<?>> metrics = deserializer.deserialize(FlatMetrics.toJson(initial).getBytes());
    Metric expected = initial.toMetric();
    assertEquals(metrics, Collections.singletonList(Arrays.asList(expected.definition, expected)));
  }
}
