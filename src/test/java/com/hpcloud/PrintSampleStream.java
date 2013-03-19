package com.hpcloud;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class PrintSampleStream {
  public static void main(String[] args) {

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("spout", new TestWordsSpout(), 5);
    builder.setBolt("print", new PrinterBolt(), 4).fieldsGrouping("spout", new Fields("word"));

    Config conf = new Config();
    conf.setDebug(true);

    LocalCluster cluster = new LocalCluster();

    cluster.submitTopology("test", conf, builder.createTopology());

    Utils.sleep(10000);
    cluster.shutdown();
  }
}
