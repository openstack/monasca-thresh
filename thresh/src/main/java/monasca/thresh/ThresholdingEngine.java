/*
 * Copyright (c) 2014 Hewlett-Packard Development Company, L.P.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package monasca.thresh;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.core.util.StatusPrinter;

import monasca.common.util.Injector;
import monasca.common.util.config.ConfigurationFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Alarm thresholding engine.
 */
public class ThresholdingEngine {
  private static final Logger logger = LoggerFactory.getLogger(ThresholdingEngine.class);

  private final ThresholdingConfiguration threshConfig;
  private final String topologyName;
  private final boolean local;

  public ThresholdingEngine(ThresholdingConfiguration threshConfig, String topologyName,
      boolean local) {
    this.threshConfig = threshConfig;
    this.topologyName = topologyName;
    this.local = local;
    logger.info("local set to {}", local);
  }

  public static final ThresholdingConfiguration configFor(String configFileName) throws Exception {
    return ConfigurationFactory
        .<ThresholdingConfiguration>forClass(ThresholdingConfiguration.class).build(
            new File(configFileName));
  }

  public static void main(String... args) throws Exception {

    // Let's show the logging status.
    StatusPrinter.print((LoggerContext) LoggerFactory.getILoggerFactory());

    if (args.length < 2) {
      logger.error("Expected configuration file name and topology name arguments");
      System.exit(1);
    }

    logger.info("Instantiating ThresholdingEngine with config file: {}, topology: {}", args[0],
        args[1]);

    ThresholdingEngine engine =
        new ThresholdingEngine(configFor(args[0]), args[1], args.length > 2 ? true : false);
    engine.configure();
    engine.run();
  }

  protected void configure() {
    Injector.registerModules(new TopologyModule(threshConfig));
  }

  protected void run() throws Exception {
    Config config = Injector.getInstance(Config.class);
    StormTopology topology = Injector.getInstance(StormTopology.class);
    config.registerSerialization(monasca.thresh.domain.model.SubAlarm.class);

    if (local) {
      logger.info("submitting topology {} to local storm cluster", topologyName);
      new LocalCluster().submitTopology(topologyName, config, topology);
    } else {
      logger.info("submitting topology {} to non-local storm cluster", topologyName);
      StormSubmitter.submitTopology(topologyName, config, topology);
    }
  }
}
