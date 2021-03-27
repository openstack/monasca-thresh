/*
 * (C) Copyright 2014,2016 Hewlett Packard Enterprise Development Company LP.
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

import monasca.common.util.Injector;
import monasca.common.util.config.ConfigurationFactory;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
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

    /*
     * This should allow command line options to show the current version java
     * -jar monasca-thresh.jar --version java -jar monasca-thresh.jar -version
     * java -jar monasca-thresh.jar version Really anything with the word
     * version in it will show the version as long as there is only one argument
     */
    if (args.length == 1 && args[0].toLowerCase().contains("version")) {
      showVersion();
      System.exit(0);
    }

    showVersion();

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

  private static void showVersion() {
    Package pkg;
    pkg = Package.getPackage("monasca.thresh");

    logger.info("-------- Version Information --------");
    logger.info("{}", pkg.getImplementationVersion());
  }

  protected void configure() {
    Injector.registerModules(new TopologyModule(threshConfig));
  }

  protected void run() throws Exception {
    Config config = Injector.getInstance(Config.class);
    StormTopology topology = Injector.getInstance(StormTopology.class);
    config.registerSerialization(monasca.thresh.domain.model.SubAlarm.class);
    config.registerSerialization(monasca.thresh.domain.model.SubExpression.class);

    if (local) {
      logger.info("submitting topology {} to local storm cluster", topologyName);
      new LocalCluster(
          System.getenv("ZOOKEEPER_SERVERS"),
          new Long(System.getenv("ZOOKEEPER_PORT"))
      ).submitTopology(topologyName, config, topology);
    } else {
      logger.info("submitting topology {} to non-local storm cluster", topologyName);
      StormSubmitter.submitTopology(topologyName, config, topology);
    }
  }
}
