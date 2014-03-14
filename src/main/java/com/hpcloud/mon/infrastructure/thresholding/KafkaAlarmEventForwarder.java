package com.hpcloud.mon.infrastructure.thresholding;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import com.google.inject.AbstractModule;
import com.hpcloud.configuration.KafkaProducerConfiguration;
import com.hpcloud.configuration.KafkaProducerProperties;

public class KafkaAlarmEventForwarder extends AbstractModule implements AlarmEventForwarder {

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
        final KeyedMessage<String, String> message = new KeyedMessage<String, String>(topic, alertRoutingKey, json);
        producer.send(message);
    }

    @Override
    public void close() {
        producer.close();
    }

    @Override
    protected void configure() {
    }
}
