package com.hpcloud.maas;

public class TestThresholdingEngine extends ThresholdingEngine {

  public TestThresholdingEngine(String topologyName, ThresholdingConfiguration config) {
    super(topologyName, config);
  }
  // @Override
  // protected Config buildConfig() {
  // Config conf = new Config();
  // conf.setDebug(true);
  // return conf;
  // }

  // @Override
  // public void run(MaasRouterConfiguration config, Environment environment) throws Exception {
  //
  // }
  //
  // @Override
  // protected StormTopology buildTopology() {
  //
  // }
  //
  // @Override
  // protected void submitTopology() throws Exception {
  // Topologies.runLocally(buildTopology(), "storm-test-top", buildConfig(), runtimeInSeconds)
  // }
}
