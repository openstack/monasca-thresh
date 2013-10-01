package com.hpcloud.maas.infrastructure.thresholding.deserializer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import backtype.storm.tuple.Fields;

import com.hpcloud.maas.common.model.metric.CollectdMetrics;
import com.hpcloud.maas.common.model.metric.Metric;
import com.hpcloud.streaming.storm.TupleDeserializer;

/**
 * Deserializes collectd metrics.
 * 
 * <ul>
 * <li>Output: Metric metric
 * </ul>
 * 
 * @author Jonathan Halterman
 */
public class CollectdMetricDeserializer implements TupleDeserializer, Serializable {
  private static final long serialVersionUID = -2340475568691974086L;
  private static final Fields FIELDS = new Fields("metricDefinition", "metric");

  @Override
  public List<List<?>> deserialize(byte[] tuple) {
    List<Metric> metrics = CollectdMetrics.toMetrics(tuple);
    if (metrics == null)
      return null;

    List<List<?>> results = new ArrayList<List<?>>(metrics.size());
    for (Metric metric : metrics) {
      // TODO remove in the future
//      CollectdMetrics.removeUnsupportedDimensions(metric.definition);
      results.add(Arrays.asList(metric.definition, metric));
    }
    return results;
  }

  @Override
  public Fields getOutputFields() {
    return FIELDS;
  }
}
