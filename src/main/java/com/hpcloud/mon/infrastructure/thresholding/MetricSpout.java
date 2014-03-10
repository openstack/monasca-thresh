package com.hpcloud.mon.infrastructure.thresholding;

import java.util.Map;

import com.hpcloud.mon.KafkaConsumerConfiguration;
import com.hpcloud.mon.infrastructure.thresholding.deserializer.MaasMetricDeserializer;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

public class MetricSpout extends BaseRichSpout {

    private static final long serialVersionUID = 744004533863562119L;

    public MetricSpout(KafkaConsumerConfiguration maasMetricSpout, MaasMetricDeserializer maasMetricDeserializer) {
        // TODO Auto-generated constructor stub
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        
    }

    @Override
    public void nextTuple() {
        
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("metricDefinition", "metric"));
    }

}
