package com.hpcloud.maas;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.hpcloud.maas.infrastructure.storm.amqp.AMQPSpout;
import com.hpcloud.maas.infrastructure.storm.amqp.MetricTupleDeserializer;
import com.hpcloud.maas.infrastructure.thresholding.AlarmLookupBolt;
import com.hpcloud.maas.infrastructure.thresholding.AlarmThresholdingBolt;
import com.hpcloud.maas.infrastructure.thresholding.MetricAggregationBolt;
import com.yammer.dropwizard.config.ConfigurationFactory;
import com.yammer.dropwizard.validation.Validator;

/**
 * Alarm thresholding engine.
 * 
 * @author Jonathan Halterman
 */
public class ThresholdingEngine {
  private static final Logger LOG = LoggerFactory.getLogger(ThresholdingEngine.class);

  private final String topologyName;
  private final ThresholdingConfiguration config;

  public ThresholdingEngine(String topologyName, ThresholdingConfiguration config) {
    this.topologyName = topologyName;
    this.config = config;
  }

  public static final ThresholdingConfiguration configFor(String configFileName) throws Exception {
    return ConfigurationFactory.forClass(ThresholdingConfiguration.class, new Validator()).build();
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      LOG.error("Expected a configuration file name argument");
      System.exit(1);
    }

    new ThresholdingEngine("maas-alarming", configFor(args[0])).run();
  }

  public void run() throws Exception {
    StormSubmitter.submitTopology(topologyName, buildConfig(), buildTopology());
  }

  private Config buildConfig() {
    return new Config();
  }

  private StormTopology buildTopology() {
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("amqp", new AMQPSpout(config.amqpSpout, new MetricTupleDeserializer()), 3);

    // AMQP -> Location
    builder.setBolt("location", new AlarmLookupBolt(), config.locationParallelism).shuffleGrouping(
        "amqp");

    // Location -> Aggregation
    builder.setBolt("aggregation", new MetricAggregationBolt(), config.aggregationParallelism)
        .fieldsGrouping("location", new Fields("alarm"));

    // Aggregation -> Thresholding
    builder.setBolt("thresholding", new AlarmThresholdingBolt(), config.thresholdingParallelism)
        .fieldsGrouping("aggregation", new Fields("compositeAlarmId"));

    return builder.createTopology();
  }
}
