package com.hpcloud.maas.infrastructure.thresholding;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.tuple.Fields;

import com.hpcloud.maas.common.model.metric.FlatMetric;
import com.hpcloud.maas.common.model.metric.FlatMetrics;
import com.hpcloud.maas.common.model.metric.Metric;
import com.hpcloud.maas.infrastructure.storm.TupleDeserializer;

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
  private static final Logger LOG = LoggerFactory.getLogger(MaasMetricDeserializer.class);
  private static final long serialVersionUID = 4021288586913323048L;
  private static final Fields FIELDS = new Fields("metricDefinition", "metric");

  @Override
  public List<List<?>> deserialize(byte[] tuple) {
    FlatMetric flatMetric = FlatMetrics.fromJson(tuple);
    Metric metric = flatMetric.toMetric();
    LOG.trace("Deserialized MaaS metric: {}", metric);
    return Collections.<List<?>>singletonList(Arrays.asList(metric.definition, metric));
  }

  @Override
  public Fields getOutputFields() {
    return FIELDS;
  }
}
