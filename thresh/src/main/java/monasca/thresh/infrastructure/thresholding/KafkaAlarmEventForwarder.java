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

package monasca.thresh.infrastructure.thresholding;

import monasca.common.configuration.KafkaProducerConfiguration;
import monasca.common.configuration.KafkaProducerProperties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaAlarmEventForwarder implements AlarmEventForwarder {

  private static final Logger logger = LoggerFactory.getLogger(KafkaAlarmEventForwarder.class);

  private final Producer<String, String> producer;

  private final String topic;

  private long messageCount = 0;

  public KafkaAlarmEventForwarder(KafkaProducerConfiguration kafkaConfig) {
    this.topic = kafkaConfig.getTopic();
    Properties kafkaProperties = KafkaProducerProperties.createKafkaProperties(kafkaConfig);
    ProducerConfig consumerConfig = new ProducerConfig(kafkaProperties);
    producer = new Producer<String, String>(consumerConfig);
  }

  @Override
  public void send(String json) {
    logger.debug("sending topic: {}, json: {}", topic, json);
    final String routingKey = String.valueOf(messageCount++);
    final KeyedMessage<String, String> message =
        new KeyedMessage<String, String>(topic, routingKey, json);
    producer.send(message);
  }

  @Override
  public void close() {
    producer.close();
  }
}
