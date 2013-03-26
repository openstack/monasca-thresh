package com.hpcloud.maas.infrastructure.thresholding;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import backtype.storm.tuple.Fields;

import com.hpcloud.maas.common.metric.InternalMetrics;
import com.hpcloud.maas.common.model.metric.Metric;
import com.hpcloud.maas.infrastructure.storm.TupleDeserializer;

/**
 * Deserializes tuples to metrics.
 * 
 * <ul>
 * <li>Output: Metric metric
 * </ul>
 * 
 * @author Jonathan Halterman
 */
public class MetricTupleDeserializer implements TupleDeserializer, Serializable {
  private static final long serialVersionUID = -2340475568691974086L;
  private static final Fields FIELDS = new Fields("metricDefinition", "metric");

  @Override
  public List<List<?>> deserialize(byte[] tuple) {
    List<Metric> metrics = InternalMetrics.metricsFor(tuple);
    List<List<?>> results = new ArrayList<List<?>>(metrics.size());
    for (Metric metric : metrics)
      results.add(Arrays.asList(metric.definition, metric));
    return results;
  }

  @Override
  public Fields getOutputFields() {
    return FIELDS;
  }
}
