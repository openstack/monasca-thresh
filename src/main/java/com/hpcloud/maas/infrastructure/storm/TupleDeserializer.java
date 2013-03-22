package com.hpcloud.maas.infrastructure.storm;

import java.util.List;

import backtype.storm.tuple.Fields;

/**
 * Deserializes tuples. Similar to a Scheme, but allows for multiple records per
 * {@link #deserialize(byte[])} call.
 * 
 * @author Jonathan Halterman
 */
public interface TupleDeserializer {
  /**
   * Returns a list of deserialized tuples for the {@code tuple}.
   */
  List<?> deserialize(byte[] tuple);

  /**
   * Returns the output fields.
   */
  Fields getOutputFields();
}
