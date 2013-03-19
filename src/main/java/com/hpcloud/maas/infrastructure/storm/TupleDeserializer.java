package com.hpcloud.maas.infrastructure.storm;

import java.util.List;

/**
 * Deserializes tuples.
 * 
 * @author Jonathan Halterman
 */
public interface TupleDeserializer {
  /**
   * Returns a list of deserialized tuples for the {@code tuple}.
   */
  List<?> deserialize(byte[] tuple);
}
