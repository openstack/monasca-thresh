/*
 * Copyright (c) 2014 Hewlett-Packard Development Company, L.P.
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

package monasca.thresh.infrastructure.thresholding.deserializer;

import monasca.common.model.event.AlarmDefinitionCreatedEvent;
import monasca.common.model.event.AlarmDefinitionDeletedEvent;
import monasca.common.model.event.AlarmDefinitionUpdatedEvent;
import monasca.common.model.event.AlarmCreatedEvent;
import monasca.common.model.event.AlarmDeletedEvent;
import monasca.common.model.event.AlarmUpdatedEvent;
import monasca.common.streaming.storm.TupleDeserializer;
import monasca.common.util.Serialization;

import backtype.storm.tuple.Fields;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/**
 * Deserializes MaaS events using registered serialization types.
 *
 * <ul>
 * <li>Output: Object event
 * </ul>
 */
public class EventDeserializer implements TupleDeserializer, Serializable {
  private static final long serialVersionUID = -1306620481933667305L;
  private static final Fields FIELDS = new Fields("event");

  static {
    // Register event types
    Serialization.registerTarget(AlarmDefinitionUpdatedEvent.class);
    Serialization.registerTarget(AlarmDefinitionDeletedEvent.class);
    Serialization.registerTarget(AlarmDefinitionCreatedEvent.class);
    Serialization.registerTarget(AlarmUpdatedEvent.class);
    Serialization.registerTarget(AlarmDeletedEvent.class);
  }

  @Override
  public List<List<?>> deserialize(byte[] tuple) {
    try {
      return Collections.<List<?>>singletonList(Collections.singletonList(Serialization
          .fromJson(tuple)));
    } catch (Exception ignore) {
      return null;
    }
  }

  @Override
  public Fields getOutputFields() {
    return FIELDS;
  }
}

