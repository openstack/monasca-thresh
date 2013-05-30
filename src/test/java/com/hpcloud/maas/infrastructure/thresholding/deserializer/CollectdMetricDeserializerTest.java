package com.hpcloud.maas.infrastructure.thresholding.deserializer;

import static org.testng.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.hpcloud.maas.common.model.metric.Metric;
import com.hpcloud.maas.common.model.metric.MetricDefinition;
import com.hpcloud.maas.infrastructure.thresholding.deserializer.CollectdMetricDeserializer;

/**
 * @author Jonathan Halterman
 */
@Test
public class CollectdMetricDeserializerTest {
  private CollectdMetricDeserializer deserializer = new CollectdMetricDeserializer();

  @SuppressWarnings("unchecked")
  public void shouldDeserialize() {
    String metric = "{\"putval\":{\"values\":[36184,51182963],\"dstypes\":[\"derive\",\"derive\"],\"dsnames\":[\"read\",\"write\"],\"time\":1365802618.809,\"interval\":60.000,\"host\":\"instance-000d65f3\",\"plugin\":\"libvirt\",\"plugin_instance\":\"\",\"type\":\"disk_ops\",\"type_instance\":\"vda\"}}";
    List<List<?>> metrics = deserializer.deserialize(metric.getBytes());

    Metric expected1 = new Metric(new MetricDefinition("hpcs.compute", "disk_read_ops", "vda",
        ImmutableMap.<String, String>builder().put("instance_id", "878067").build()), 1365802618,
        36184);
    Metric expected2 = new Metric(new MetricDefinition("hpcs.compute", "disk_write_ops", "vda",
        ImmutableMap.<String, String>builder().put("instance_id", "878067").build()), 1365802618,
        51182963);
    assertEquals(
        metrics,
        Arrays.asList(Arrays.asList(expected1.definition, expected1),
            Arrays.asList(expected2.definition, expected2)));
  }
}
