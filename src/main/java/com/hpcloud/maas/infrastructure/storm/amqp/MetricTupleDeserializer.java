package com.hpcloud.maas.infrastructure.storm.amqp;

import java.util.List;

import com.hpcloud.maas.common.metric.InternalMetrics;
import com.hpcloud.maas.infrastructure.storm.TupleDeserializer;

/**
 * Deserializes metrics. Outputs a single "output" field.
 * 
 * @author Jonathan Halterman
 */
public class MetricTupleDeserializer implements TupleDeserializer {
  @Override
  public List<?> deserialize(byte[] tuple) {
    return InternalMetrics.metricsFor(tuple);
  }
}
