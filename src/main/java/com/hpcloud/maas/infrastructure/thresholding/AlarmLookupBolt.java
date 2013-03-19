package com.hpcloud.maas.infrastructure.thresholding;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.hpcloud.maas.domain.model.Alarm;

/**
 * Locates alarms for incoming metrics.
 * 
 * <ul>
 * <li>Input: "object" : Metric
 * <li>Output: "metric" : Metric, "alarm" : Alarm
 * </ul>
 * 
 * @author Jonathan Halterman
 */
public class AlarmLookupBolt extends BaseBasicBolt {
  private static final long serialVersionUID = 397873545987747100L;

  @Override
  public void execute(Tuple input, BasicOutputCollector collector) {
    String metric = input.getString(0);
    Alarm alarm = Alarms.alarmFor(metric);
    collector.emit(new Values(metric, alarm));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("metric", "alarm"));
  }
}
