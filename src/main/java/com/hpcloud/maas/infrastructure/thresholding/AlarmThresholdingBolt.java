package com.hpcloud.maas.infrastructure.thresholding;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import com.hpcloud.maas.domain.model.Alarm;

/**
 * Determines whether an alarm threshold has been exceeded.
 * 
 * @author Jonathan Halterman
 */
public class AlarmThresholdingBolt extends BaseBasicBolt {
  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    String compositeAlarmId = tuple.getString(0);
    Alarm alarm = (Alarm) tuple.getValue(1);
    
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
  }
}
