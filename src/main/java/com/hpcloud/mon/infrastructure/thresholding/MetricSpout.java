package com.hpcloud.mon.infrastructure.thresholding;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.hpcloud.mon.MetricSpoutConfig;
import com.hpcloud.mon.common.model.metric.MetricEnvelope;
import com.hpcloud.mon.common.model.metric.MetricEnvelopes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricSpout extends KafkaSpout {
    private static final Logger LOG = LoggerFactory.getLogger(MetricSpout.class);

    private static final long serialVersionUID = 744004533863562119L;

    public MetricSpout(MetricSpoutConfig metricSpoutConfig) {
        super(metricSpoutConfig.kafkaConsumerConfiguration);
        LOG.info("Created");
    }

    @Override
    protected void processMessage(byte[] message, SpoutOutputCollector collector) {
        final MetricEnvelope metricEnvelope;
        try {
            metricEnvelope = MetricEnvelopes.fromJson(message);
        }
        catch (RuntimeException re) {
            LOG.warn("Error parsing MetricEnvelope", re);
            return;
        }
        collector.emit(new Values(metricEnvelope.metric.definition(), metricEnvelope.metric));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("metricDefinition", "metric"));
    }
}
