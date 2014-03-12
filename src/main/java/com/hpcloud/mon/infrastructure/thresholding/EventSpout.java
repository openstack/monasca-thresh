package com.hpcloud.mon.infrastructure.thresholding;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;

import com.hpcloud.configuration.KafkaConsumerConfiguration;
import com.hpcloud.mon.EventSpoutConfig;
import com.hpcloud.mon.infrastructure.thresholding.deserializer.MaasEventDeserializer;

public class EventSpout extends BaseRichSpout {

    private static final long serialVersionUID = 8457340455857276878L;

    public EventSpout(EventSpoutConfig eventSpout, MaasEventDeserializer maasEventDeserializer) {
        // TODO Auto-generated constructor stub
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void nextTuple() {
        // TODO Auto-generated method stub
        
    }

}
