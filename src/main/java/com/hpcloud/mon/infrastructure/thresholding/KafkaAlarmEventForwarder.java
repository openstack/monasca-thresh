package com.hpcloud.mon.infrastructure.thresholding;

import com.hpcloud.configuration.KafkaProducerConfiguration;
import com.hpcloud.configuration.KafkaProducerProperties;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaAlarmEventForwarder implements AlarmEventForwarder {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaAlarmEventForwarder.class);

    private final Producer<String, String> producer;

    private final String topic;

    public KafkaAlarmEventForwarder(KafkaProducerConfiguration kafkaConfig) {
        this.topic = kafkaConfig.getTopic();
        Properties kafkaProperties = KafkaProducerProperties.createKafkaProperties(kafkaConfig);
        ProducerConfig consumerConfig = new ProducerConfig(kafkaProperties);
        producer = new Producer<String, String>(consumerConfig);
    }

    @Override
    public void send(String alertExchange, String alertRoutingKey, String json) {
        LOG.debug("sending alertExchange: {}, alertRoutingKey: {}, json: {}", alertExchange,
                alertRoutingKey, json);
        final KeyedMessage<String, String> message = new KeyedMessage<String, String>(topic, alertRoutingKey, json);
        producer.send(message);
    }

    @Override
    public void close() {
        producer.close();
    }


}
