/*
 * (C) Copyright 2016 Hewlett Packard Enterprise Development Company LP.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package monasca.thresh.utils;

import org.apache.storm.tuple.Fields;

import java.util.List;

/**
 * Deserializes tuples. Similar to a Scheme, but allows for multiple records per
 * {@link #deserialize(byte[])} call.
 */
public interface TupleDeserializer {
  /**
   * Returns a list of deserialized tuples, consisting of a list of tuples each with a list of
   * fields, for the {@code tuple}, else null if the {@code tuple} cannot be deserialized.
   */
  List<List<?>> deserialize(byte[] tuple);

  /**
   * Returns the output fields.
   */
  Fields getOutputFields();
}
