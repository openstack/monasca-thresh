package com.hpcloud.maas;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;

import com.hpcloud.maas.common.event.AlarmCreatedEvent;
import com.hpcloud.maas.common.event.AlarmDeletedEvent;
import com.hpcloud.maas.common.event.EndpointDeletedEvent;
import com.hpcloud.maas.util.config.ConfigurationFactory;
import com.hpcloud.util.Injector;
import com.hpcloud.util.Serialization;

/**
 * Alarm thresholding engine.
 * 
 * @author Jonathan Halterman
 */
public class ThresholdingEngine {
  private static final Logger LOG = LoggerFactory.getLogger(ThresholdingEngine.class);

  protected final ThresholdingConfiguration threshConfig;
  private final boolean local;

  public ThresholdingEngine(ThresholdingConfiguration threshConfig, boolean local) {
    this.threshConfig = threshConfig;
    this.local = local;
  }

  public static final ThresholdingConfiguration configFor(String configFileName) throws Exception {
    return ConfigurationFactory.<ThresholdingConfiguration>forClass(ThresholdingConfiguration.class)
        .build(new File(configFileName));
  }

  public static void main(String... args) throws Exception {
    if (args.length < 1) {
      LOG.error("Expected a configuration file name argument");
      System.exit(1);
    }

    ThresholdingEngine engine = new ThresholdingEngine(configFor(args[0]), args.length > 1 ? true
        : false);
    engine.configure();
    engine.run();
  }

  protected void configure() {
    Injector.registerModules(new TopologyModule(threshConfig));

    // Register event types
    Serialization.registerTarget(AlarmCreatedEvent.class);
    Serialization.registerTarget(AlarmDeletedEvent.class);
    Serialization.registerTarget(EndpointDeletedEvent.class);
  }

  protected void run() throws Exception {
    Config config = Injector.getInstance(Config.class);
    StormTopology topology = Injector.getInstance(StormTopology.class);

    if (local)
      new LocalCluster().submitTopology("maas-alarming", config, topology);
    else
      StormSubmitter.submitTopology("maas-alarming", config, topology);
  }
}
