package com.hpcloud.mon;

import java.io.Serializable;

import com.hpcloud.configuration.KafkaConsumerConfiguration;

public class EventSpoutConfig implements Serializable {

    private static final long serialVersionUID = -8129774848323598123L;

    public KafkaConsumerConfiguration kafkaConsumerConfiguration;

}
