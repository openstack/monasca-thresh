package com.hpcloud.mon;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.hpcloud.configuration.KafkaConsumerConfiguration;

public class KafkaSpoutConfig implements Serializable {

    private static final long serialVersionUID = -6477042435089264571L;

    @JsonProperty
    public Integer maxWaitTime = 100;

    public KafkaConsumerConfiguration kafkaConsumerConfiguration;
}
