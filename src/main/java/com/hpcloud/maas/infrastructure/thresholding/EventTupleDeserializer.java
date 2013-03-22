package com.hpcloud.maas.infrastructure.thresholding;

import java.util.List;

import backtype.storm.tuple.Fields;

import com.hpcloud.maas.infrastructure.storm.TupleDeserializer;
import com.hpcloud.util.Serialization;

/**
 * Deserializes api events using registered serialization types.
 * 
 * <p>
 * Outputs: Object event
 * 
 * @author Jonathan Halterman
 */
public class EventTupleDeserializer implements TupleDeserializer {
  private static final Fields FIELDS = new Fields("event");

  @Override
  public List<?> deserialize(byte[] tuple) {
    return Serialization.fromJson(new String(tuple));
  }

  @Override
  public Fields getOutputFields() {
    return FIELDS;
  }
}
