package com.hpcloud.maas;

public class ThresholdingEngineRunner {
  public static void main(String... args) throws Exception {
    ThresholdingEngine.main("test-config.yml", "test-topo", "1");
  }
}
