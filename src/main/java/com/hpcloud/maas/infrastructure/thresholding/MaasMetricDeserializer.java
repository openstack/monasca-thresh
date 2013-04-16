package com.hpcloud.maas.infrastructure.thresholding;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

import backtype.storm.tuple.Fields;

import com.hpcloud.maas.common.model.metric.FlatMetric;
import com.hpcloud.maas.infrastructure.storm.TupleDeserializer;
import com.hpcloud.util.Serialization;

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
    FlatMetric flatMetric = Serialization.fromJson(new String(tuple), FlatMetric.class);
    return Collections.<List<?>>singletonList(Collections.singletonList(flatMetric.toMetric()));
  }

  @Override
  public Fields getOutputFields() {
    return FIELDS;
  }
}
