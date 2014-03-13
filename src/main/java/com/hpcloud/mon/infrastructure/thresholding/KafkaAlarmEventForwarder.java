package com.hpcloud.mon.infrastructure.thresholding;

import com.google.inject.AbstractModule;
import com.hpcloud.mon.KafkaProducerConfiguration;

public class KafkaAlarmEventForwarder extends AbstractModule implements AlarmEventForwarder {

    public KafkaAlarmEventForwarder(KafkaProducerConfiguration kafkaConfig) {
    }

    @Override
    public void send(String alertExchange, String alertRoutingKey, String json) {
        
    }

    @Override
    protected void configure() {
    }

}
