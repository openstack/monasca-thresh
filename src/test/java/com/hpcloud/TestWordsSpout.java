package com.hpcloud;

import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class TestWordsSpout extends BaseRichSpout {
  public static Logger LOG = Logger.getLogger(TestWordsSpout.class);
  SpoutOutputCollector _collector;

  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    _collector = collector;
  }

  static final String AB = "ABCDEFG";
  static Random rnd = new Random();

  String randomString(int len) {
    StringBuilder sb = new StringBuilder(len);
    for (int i = 0; i < len; i++)
      sb.append(AB.charAt(rnd.nextInt(AB.length())));
    return sb.toString();
  }

  public void nextTuple() {
    Utils.sleep(100);
    _collector.emit(new Values(randomString(1)));
  }

  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("word"));
  }
}
