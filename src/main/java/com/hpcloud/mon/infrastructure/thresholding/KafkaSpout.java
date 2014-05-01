package com.hpcloud.mon.infrastructure.thresholding;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichSpout;
import com.hpcloud.configuration.KafkaConsumerConfiguration;
import com.hpcloud.configuration.KafkaConsumerProperties;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public abstract class KafkaSpout extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSpout.class);

    private static final long serialVersionUID = 744004533863562119L;

    private final KafkaConsumerConfiguration kafkaConsumerConfig;

    private transient ConsumerConnector consumerConnector;

    private transient List<KafkaStream<byte[], byte[]>> streams = null;

    private SpoutOutputCollector collector;

    protected KafkaSpout(KafkaConsumerConfiguration kafkaConsumerConfig) {
        this.kafkaConsumerConfig = kafkaConsumerConfig;
    }

    @Override
    public void activate() {
        LOG.info("Activated");
        if (streams == null) {
            Map<String, Integer> topicCountMap = new HashMap<>();
            topicCountMap.put(kafkaConsumerConfig.getTopic(), new Integer(1));
            Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);
            streams = consumerMap.get(kafkaConsumerConfig.getTopic());
        }
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        LOG.info("Opened");
        this.collector = collector;
        LOG.info(" topic = " + kafkaConsumerConfig.getTopic());

        Properties kafkaProperties = KafkaConsumerProperties.createKafkaProperties(kafkaConsumerConfig);
        // Have to use a different consumer.id for each spout so use the storm taskId. Otherwise,
        // zookeeper complains about a conflicted ephemeral node when there is more than one spout
        // reading from a topic
        kafkaProperties.setProperty("consumer.id", String.valueOf(context.getThisTaskId()));
        ConsumerConfig consumerConfig = new ConsumerConfig(kafkaProperties);
        this.consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
    }

    @Override
    public void nextTuple() {
        LOG.debug("nextTuple called");
        ConsumerIterator<byte[], byte[]> it = streams.get(0).iterator();
        if (it.hasNext()) {
            LOG.debug("streams iterator has next");
            byte[] message = it.next().message();
            processMessage(message, collector);
        }
    }

    protected abstract void processMessage(byte[] message, SpoutOutputCollector collector2);
}
