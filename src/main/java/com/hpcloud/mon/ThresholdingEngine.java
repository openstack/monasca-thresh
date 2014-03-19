package com.hpcloud.mon;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.core.util.StatusPrinter;
import com.hpcloud.util.Injector;
import com.hpcloud.util.config.ConfigurationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Alarm thresholding engine.
 *
 * @author Jonathan Halterman
 */
public class ThresholdingEngine {
    private static final Logger LOG = LoggerFactory.getLogger(ThresholdingEngine.class);

    private final ThresholdingConfiguration threshConfig;
    private final String topologyName;
    private final boolean local;

    public ThresholdingEngine(ThresholdingConfiguration threshConfig, String topologyName,
                              boolean local) {
        this.threshConfig = threshConfig;
        this.topologyName = topologyName;
        this.local = local;
    }

    public static final ThresholdingConfiguration configFor(String configFileName) throws Exception {
        return ConfigurationFactory.<ThresholdingConfiguration>forClass(ThresholdingConfiguration.class)
                .build(new File(configFileName));
    }

    public static void main(String... args) throws Exception {

        // Let's show the logging status.
        StatusPrinter.print((LoggerContext) LoggerFactory.getILoggerFactory());

        if (args.length < 2) {
            LOG.error("Expected configuration file name and topology name arguments");
            System.exit(1);
        }

        ThresholdingEngine engine = new ThresholdingEngine(configFor(args[0]), args[1],
                args.length > 2 ? true : false);
        engine.configure();
        engine.run();
    }

    protected void configure() {
        Injector.registerModules(new TopologyModule(threshConfig));
    }

    protected void run() throws Exception {
        Config config = Injector.getInstance(Config.class);
        StormTopology topology = Injector.getInstance(StormTopology.class);

        if (local)
            new LocalCluster().submitTopology(topologyName, config, topology);
        else
            StormSubmitter.submitTopology(topologyName, config, topology);
    }
}
