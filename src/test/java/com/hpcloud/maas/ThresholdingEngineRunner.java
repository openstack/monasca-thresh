package com.hpcloud.maas;

public class ThresholdingEngineRunner {
  public static void main(String... args) throws Exception {
    ThresholdingEngine.main("config-pi-test.yml", "test-topo", "1");
  }
}
