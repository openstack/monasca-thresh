package com.hpcloud.mon.infrastructure.thresholding;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.hpcloud.configuration.KafkaConsumerProperties;
import com.hpcloud.mon.MetricSpoutConfig;
import com.hpcloud.mon.common.model.metric.MetricEnvelope;
import com.hpcloud.mon.common.model.metric.MetricEnvelopes;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricSpout extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(MetricSpout.class);

    private static final long serialVersionUID = 744004533863562119L;

    private final MetricSpoutConfig metricSpoutConfig;

    private transient ConsumerConnector consumerConnector;

    private transient List<KafkaStream<byte[], byte[]>> streams = null;

    private SpoutOutputCollector collector;

    public MetricSpout(MetricSpoutConfig metricSpoutConfig) {
        this.metricSpoutConfig = metricSpoutConfig;
        LOG.info("Created");
    }

    @Override
    public void activate() {
        LOG.info("Activated");
        if (streams == null) {
            Map<String, Integer> topicCountMap = new HashMap<>();
            topicCountMap.put(metricSpoutConfig.kafkaConsumerConfiguration.getTopic(), new Integer(1));
            Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);
            streams = consumerMap.get(metricSpoutConfig.kafkaConsumerConfiguration.getTopic());
        }
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        LOG.info("Opened");
        this.collector = collector;

        Properties kafkaProperties = KafkaConsumerProperties.createKafkaProperties(metricSpoutConfig.kafkaConsumerConfiguration);
        ConsumerConfig consumerConfig = new ConsumerConfig(kafkaProperties);
        this.consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);

    }

    @Override
    public void nextTuple() {

        ConsumerIterator<byte[], byte[]> it = streams.get(0).iterator();
        if (it.hasNext()) {
            final MetricEnvelope metricEnvelope;
            try {
                metricEnvelope = MetricEnvelopes.fromJson(it.next().message());
            }
            catch (RuntimeException re) {
                LOG.warn("Error parsing MetricEnvelope", re);
                return;
            }
            collector.emit(new Values(metricEnvelope.metric.definition(), metricEnvelope.metric));
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("metricDefinition", "metric"));
    }

}
