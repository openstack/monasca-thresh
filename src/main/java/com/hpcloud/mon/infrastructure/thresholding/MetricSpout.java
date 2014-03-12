package com.hpcloud.mon.infrastructure.thresholding;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.hpcloud.configuration.KafkaConsumerProperties;
import com.hpcloud.mon.MetricSpoutConfig;
import com.hpcloud.mon.common.model.metric.Metric;
import com.hpcloud.mon.common.model.metric.MetricDefinition;
import com.hpcloud.mon.common.model.metric.MetricEnvelope;
import com.hpcloud.mon.common.model.metric.MetricEnvelopes;
import com.hpcloud.mon.infrastructure.thresholding.deserializer.MetricDeserializer;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.*;

public class MetricSpout extends BaseRichSpout {

    private static final long serialVersionUID = 744004533863562119L;

    private final MetricSpoutConfig metricSpoutConfig;
    private final MetricDeserializer metricDeserializer;

    private ConsumerConnector consumerConnector;

    private List<KafkaStream<byte[], byte[]>> streams = null;

    private SpoutOutputCollector collector;

    public MetricSpout(MetricSpoutConfig metricSpoutConfig, MetricDeserializer metricDeserializer) {
        this.metricSpoutConfig = metricSpoutConfig;
        this.metricDeserializer = metricDeserializer;
    }

    @Override
    public void activate() {
        if (streams == null) {
            Map<String, Integer> topicCountMap = new HashMap<>();
            topicCountMap.put(metricSpoutConfig.kafkaConsumerConfiguration.getTopic(), new Integer(1));
            Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);
            streams = consumerMap.get(metricSpoutConfig.kafkaConsumerConfiguration.getTopic());
        }
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;

        Properties kafkaProperties = KafkaConsumerProperties.createKafkaProperties(metricSpoutConfig.kafkaConsumerConfiguration);
        ConsumerConfig consumerConfig = new ConsumerConfig(kafkaProperties);
        this.consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);

    }

    @Override
    public void nextTuple() {

        ConsumerIterator<byte[], byte[]> it = streams.get(0).iterator();
        if (it.hasNext()) {
            MetricEnvelope metricEnvelope = MetricEnvelopes.fromJson(it.next().message());
            collector.emit(new Values(metricEnvelope.metric.definition(), metricEnvelope.metric));
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("metricDefinition", "metric"));
    }

}
