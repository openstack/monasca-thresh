package com.hpcloud.infrastructure.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;

public class Topologies {
  public static void runLocally(StormTopology topology, String topologyName, Config conf,
      int runtimeInSeconds) {
    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology(topologyName, conf, topology);

    try {
      Thread.sleep((long) runtimeInSeconds * 1000);
    } catch (Exception ignore) {
    }

    cluster.killTopology(topologyName);
    cluster.shutdown();
  }
}
