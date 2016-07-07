/*
 * (C) Copyright 2014,2016 Hewlett Packard Enterprise Development Company LP.
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

package monasca.thresh.infrastructure.thresholding;

import monasca.thresh.EventSpoutConfig;
import monasca.thresh.infrastructure.thresholding.deserializer.EventDeserializer;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;

public class EventSpout extends KafkaSpout {
  private static final Logger logger = LoggerFactory.getLogger(EventSpout.class);

  private static final long serialVersionUID = 8457340455857276878L;

  private final EventDeserializer deserializer;

  public EventSpout(EventSpoutConfig configuration, EventDeserializer deserializer) {
    super(configuration);
    this.deserializer = deserializer;
    logger.info("EventSpout created");
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(deserializer.getOutputFields());
  }

  @Override
  protected void processMessage(byte[] message, SpoutOutputCollector collector) {
    List<List<?>> events = deserializer.deserialize(message);
    if (events != null) {
      for (final List<?> event : events) {
        final Object eventToSend = event.get(0);
        if (!(eventToSend instanceof Serializable)) {
          logger.error("Class {} is not Serializable: {}", eventToSend.getClass(), eventToSend);
          continue;
        }
        collector.emit(new Values(eventToSend));
      }
    }
  }
}
