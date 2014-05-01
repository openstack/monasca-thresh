package com.hpcloud.mon.infrastructure.thresholding;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.hpcloud.configuration.KafkaProducerConfiguration;

public class ProducerModule extends AbstractModule {
    private KafkaProducerConfiguration config;
    private AlarmEventForwarder alarmEventForwarder;

    @Override
    protected void configure() {
    }

    public ProducerModule(KafkaProducerConfiguration config) {
        this.config = config;
    }

    public ProducerModule(AlarmEventForwarder alarmEventForwarder) {
        this.alarmEventForwarder = alarmEventForwarder;
    }

    @Provides
    AlarmEventForwarder alarmEventForwarder() {
      return alarmEventForwarder == null ? new KafkaAlarmEventForwarder(config) : alarmEventForwarder;
    }
}
