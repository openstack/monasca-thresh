package com.hpcloud.mon.infrastructure.thresholding.deserializer;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import backtype.storm.tuple.Fields;

import com.hpcloud.maas.common.model.metric.FlatMetric;
import com.hpcloud.maas.common.model.metric.FlatMetrics;
import com.hpcloud.maas.common.model.metric.Metric;
import com.hpcloud.streaming.storm.TupleDeserializer;

/**
 * Deserializes MaaS metrics.
 * 
 * <ul>
 * <li>Output: Metric metric
 * </ul>
 * 
 * @author Jonathan Halterman
 */
public class MaasMetricDeserializer implements TupleDeserializer, Serializable {
  private static final long serialVersionUID = 4021288586913323048L;
  private static final Fields FIELDS = new Fields("metricDefinition", "metric");

  @Override
  public List<List<?>> deserialize(byte[] tuple) {
    FlatMetric flatMetric = FlatMetrics.fromJson(tuple);
    Metric metric = flatMetric.toMetric();
    return Collections.<List<?>>singletonList(Arrays.asList(metric.definition, metric));
  }

  @Override
  public Fields getOutputFields() {
    return FIELDS;
  }
}
