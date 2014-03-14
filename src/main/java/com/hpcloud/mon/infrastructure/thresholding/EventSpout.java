package com.hpcloud.mon.infrastructure.thresholding;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Values;

import com.hpcloud.mon.EventSpoutConfig;
import com.hpcloud.mon.infrastructure.thresholding.deserializer.EventDeserializer;

public class EventSpout extends KafkaSpout {
    private static final Logger LOG = LoggerFactory.getLogger(EventSpout.class);

    private static final long serialVersionUID = 8457340455857276878L;

    private final EventDeserializer deserializer;

    public EventSpout(EventSpoutConfig configuration, EventDeserializer deserializer) {
        super(configuration.kafkaConsumerConfiguration);
        this.deserializer = deserializer;
        LOG.info("EventSpout created");
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
                collector.emit(new Values(event.get(0)));
            }
        }
    }
}
