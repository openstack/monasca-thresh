package com.hpcloud.maas;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.hpcloud.maas.common.event.AlarmCreatedEvent;
import com.hpcloud.maas.common.event.AlarmDeletedEvent;
import com.hpcloud.maas.common.event.EndpointDeletedEvent;
import com.hpcloud.maas.infrastructure.storm.amqp.AMQPSpout;
import com.hpcloud.maas.infrastructure.thresholding.AlarmThresholdingBolt;
import com.hpcloud.maas.infrastructure.thresholding.EventProcessingBolt;
import com.hpcloud.maas.infrastructure.thresholding.EventTupleDeserializer;
import com.hpcloud.maas.infrastructure.thresholding.MetricAggregationBolt;
import com.hpcloud.maas.infrastructure.thresholding.MetricTupleDeserializer;
import com.hpcloud.util.Serialization;
import com.yammer.dropwizard.config.ConfigurationFactory;
import com.yammer.dropwizard.validation.Validator;

/**
 * Alarm thresholding engine. Implemented as a distributed Storm topology.
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

  private void run() throws Exception {
    // Register event types
    Serialization.registerTarget(AlarmCreatedEvent.class);
    Serialization.registerTarget(AlarmDeletedEvent.class);
    Serialization.registerTarget(EndpointDeletedEvent.class);

    StormSubmitter.submitTopology(topologyName, buildConfig(), buildTopology());
  }

  private Config buildConfig() {
    return new Config();
  }

  private StormTopology buildTopology() {
    TopologyBuilder builder = new TopologyBuilder();

    // Receives Metrics
    builder.setSpout("metrics", new AMQPSpout(config.metricSpout, new MetricTupleDeserializer()),
        config.metricSpoutParallelism);

    // Receives MaaS events
    builder.setSpout("event-spout", new AMQPSpout(config.apiSpout, new EventTupleDeserializer()),
        config.eventSpoutParallelism);

    // Event -> Event Demultiplexing
    builder.setBolt("event-bolt", new EventProcessingBolt(), config.eventBoltParallelism)
        .shuffleGrouping("event-spout");

    // Metrics / Event -> Aggregation
    builder.setBolt("aggregation", new MetricAggregationBolt(), config.aggregationParallelism)
        .fieldsGrouping("metrics", new Fields("metricDefinition"))
        .fieldsGrouping("event-bolt", EventProcessingBolt.ALARM_EVENT_STREAM_ID,
            new Fields("metricDefinition"));

    // Aggregation / Event -> Thresholding
    builder.setBolt("thresholding", new AlarmThresholdingBolt(), config.thresholdingParallelism)
        .fieldsGrouping("aggregation", new Fields("compositeAlarmId"))
        .fieldsGrouping("event-bolt", EventProcessingBolt.COMPOSITE_ALARM_EVENT_STREAM_ID,
            new Fields("compositeAlarmId"));

    return builder.createTopology();
  }
}
