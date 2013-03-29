package com.hpcloud.maas;

import javax.inject.Named;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.hpcloud.maas.infrastructure.storm.amqp.AMQPSpout;
import com.hpcloud.maas.infrastructure.thresholding.AlarmThresholdingBolt;
import com.hpcloud.maas.infrastructure.thresholding.EventProcessingBolt;
import com.hpcloud.maas.infrastructure.thresholding.EventTupleDeserializer;
import com.hpcloud.maas.infrastructure.thresholding.MetricAggregationBolt;
import com.hpcloud.maas.infrastructure.thresholding.MetricTupleDeserializer;
import com.hpcloud.util.Injector;

/**
 * Configures types for the thresholding topology.
 * 
 * @author Jonathan Halterman
 */
public class TopologyModule extends AbstractModule {
  private final ThresholdingConfiguration config;
  private Config stormConfig;
  private IRichSpout metricSpout;
  private IRichSpout eventSpout;

  public TopologyModule(ThresholdingConfiguration config) {
    this.config = config;
  }

  public TopologyModule(ThresholdingConfiguration threshConfig, Config stormConfig,
      IRichSpout metricSpout, IRichSpout eventSpout) {
    this(threshConfig);
    this.stormConfig = stormConfig;
    this.metricSpout = metricSpout;
    this.eventSpout = eventSpout;
  }

  @Override
  protected void configure() {
  }

  @Provides
  Config stormConfig() {
    if (stormConfig != null)
      return stormConfig;
    return new Config();
  }

  @Provides
  @Named("event-spout")
  IRichSpout eventSpout() {
    return eventSpout == null ? new AMQPSpout(config.eventSpout, new EventTupleDeserializer())
        : eventSpout;
  }

  @Provides
  @Named("metrics")
  IRichSpout metricsSpout() {
    return metricSpout == null ? new AMQPSpout(config.metricSpout, new MetricTupleDeserializer())
        : metricSpout;
  }

  @Provides
  StormTopology topology() {
    TopologyBuilder builder = new TopologyBuilder();

    // Receives Metrics
    builder.setSpout("metrics", Injector.getInstance(IRichSpout.class, "metrics"),
        config.metricSpoutParallelism);

    // Receives MaaS events
    builder.setSpout("event-spout", Injector.getInstance(IRichSpout.class, "event-spout"),
        config.eventSpoutParallelism);

    // Event -> Event Demultiplexing
    builder.setBolt("event-bolt", new EventProcessingBolt(), config.eventBoltParallelism)
        .shuffleGrouping("event-spout");

    // Metrics / Event -> Aggregation
    builder.setBolt("aggregation", new MetricAggregationBolt(), config.aggregationParallelism)
        .fieldsGrouping("metrics", new Fields("metricDefinition"))
        .fieldsGrouping("event-bolt", EventProcessingBolt.METRIC_SUB_ALARM_EVENT_STREAM_ID,
            new Fields("metricDefinition"))
        .fieldsGrouping("event-bolt", EventProcessingBolt.METRIC_ALARM_EVENT_STREAM_ID,
            new Fields("metricDefinition"));

    // Aggregation / Event -> Thresholding
    builder.setBolt("thresholding", new AlarmThresholdingBolt(), config.thresholdingParallelism)
        .fieldsGrouping("aggregation", new Fields("alarmId"))
        .fieldsGrouping("event-bolt", EventProcessingBolt.ALARM_EVENT_STREAM_ID,
            new Fields("alarmId"));

    return builder.createTopology();
  }
}
