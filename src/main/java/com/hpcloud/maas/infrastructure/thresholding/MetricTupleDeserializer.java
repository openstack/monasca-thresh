package com.hpcloud.maas.infrastructure.thresholding;

import java.util.List;

import backtype.storm.tuple.Fields;

import com.hpcloud.maas.common.metric.InternalMetrics;
import com.hpcloud.maas.infrastructure.storm.TupleDeserializer;

/**
 * Deserializes tuples to metrics.
 * 
 * <p>
 * Outputs: Object metric
 * 
 * @author Jonathan Halterman
 */
public class MetricTupleDeserializer implements TupleDeserializer {
  private static final Fields FIELDS = new Fields("metricDefinition", "metric");

  @Override
  public List<?> deserialize(byte[] tuple) {
    return InternalMetrics.metricsFor(tuple);
  }

  @Override
  public Fields getOutputFields() {
    return FIELDS;
  }
}
