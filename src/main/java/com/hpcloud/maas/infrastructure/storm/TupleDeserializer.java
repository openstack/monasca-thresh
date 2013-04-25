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
   * Returns a list of deserialized tuples, consisting of a list of tuples each with a list of
   * fields, for the {@code tuple}.
   */
  List<List<?>> deserialize(String tuple);

  /**
   * Returns the output fields.
   */
  Fields getOutputFields();
}
