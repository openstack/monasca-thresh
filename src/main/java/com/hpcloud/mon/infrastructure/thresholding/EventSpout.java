package com.hpcloud.mon.infrastructure.thresholding;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.hpcloud.configuration.KafkaConsumerProperties;
import com.hpcloud.mon.EventSpoutConfig;
import com.hpcloud.mon.infrastructure.thresholding.deserializer.EventDeserializer;

public class EventSpout extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(EventSpout.class);

    private static final long serialVersionUID = 8457340455857276878L;

    private final EventSpoutConfig configuration;

    private final EventDeserializer deserializer;

    private SpoutOutputCollector collector;

    private String topic;

    private int numThreads;

    private KafkaStream<byte[], byte[]> stream;

    private ConsumerConnector consumerConnector;

    public EventSpout(EventSpoutConfig configuration, EventDeserializer deserializer) {
        this.configuration = configuration;
        this.deserializer = deserializer;
        LOG.info("EventSpout created");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(deserializer.getOutputFields());
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        LOG.info("open called");
        this.topic = configuration.kafkaConsumerConfiguration.getTopic();
        LOG.info(" topic = " + topic);

        this.numThreads = configuration.kafkaConsumerConfiguration.getNumThreads();
        LOG.info(" numThreads = " + numThreads);

        if (numThreads != 1) {
            LOG.warn("Reseting numThreads to 1 from {}", numThreads);
            numThreads = 1;
        }
        Properties kafkaProperties = KafkaConsumerProperties.createKafkaProperties(configuration.kafkaConsumerConfiguration);
        ConsumerConfig consumerConfig = new ConsumerConfig(kafkaProperties);
        this.consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);

        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topic, new Integer(numThreads));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        // TODO - decide about numThreads
        this.stream = streams.get(0);
    }

    @Override
    public void nextTuple() {
        ConsumerIterator<byte[], byte[]> it = this.stream.iterator();
        while (it.hasNext()) {
            List<List<?>> events = deserializer.deserialize(it.next().message());
            if (events != null) {
                for (final List<?> event : events) {
                    collector.emit(new Values(event.get(0)));
                }
            }
        }
    }
}
