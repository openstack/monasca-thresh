package com.hpcloud.mon;

import java.io.Serializable;

import com.hpcloud.configuration.*;

public class MetricSpoutConfig implements Serializable {

    private static final long serialVersionUID = -4285448019855024921L;

    public KafkaConsumerConfiguration kafkaConsumerConfiguration;
}
