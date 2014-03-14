package com.hpcloud.mon;

import javax.inject.Named;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.hpcloud.mon.infrastructure.thresholding.AlarmThresholdingBolt;
import com.hpcloud.mon.infrastructure.thresholding.EventProcessingBolt;
import com.hpcloud.mon.infrastructure.thresholding.EventSpout;
import com.hpcloud.mon.infrastructure.thresholding.MetricAggregationBolt;
import com.hpcloud.mon.infrastructure.thresholding.MetricFilteringBolt;
import com.hpcloud.mon.infrastructure.thresholding.MetricSpout;
import com.hpcloud.mon.infrastructure.thresholding.deserializer.EventDeserializer;
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
    if (stormConfig == null) {
      stormConfig = new Config();
      stormConfig.setNumWorkers(config.numWorkerProcesses);
      stormConfig.setNumAckers(config.numAckerThreads);
      stormConfig.put(ThresholdingConfiguration.ALERTS_EXCHANGE, config.alertsExchange);
      stormConfig.put(ThresholdingConfiguration.ALERTS_ROUTING_KEY, config.alertsRoutingKey);
    }

    return stormConfig;
  }

  @Provides
  @Named("metrics")
  IRichSpout metricSpout() {
    return metricSpout == null ? new MetricSpout(config.metricSpoutConfig) : metricSpout;
  }

  @Provides
  @Named("event")
  IRichSpout eventSpout() {
    return eventSpout == null ? new EventSpout(config.eventSpoutConfig, new EventDeserializer())
        : eventSpout;
  }

  @Provides
  StormTopology topology() {
    TopologyBuilder builder = new TopologyBuilder();

    // Receives metrics
    builder.setSpout("metrics-spout", Injector.getInstance(IRichSpout.class, "metrics"),
        config.metricSpoutThreads).setNumTasks(config.metricSpoutTasks);

    // Receives events
    builder.setSpout("event-spout", Injector.getInstance(IRichSpout.class, "event"),
        config.eventSpoutThreads).setNumTasks(config.eventSpoutTasks);

    // MaaS Event -> Events
    builder.setBolt("event-bolt", new EventProcessingBolt(), config.eventBoltThreads)
        .shuffleGrouping("event-spout")
        .setNumTasks(config.eventBoltTasks);

    // Metrics / Event -> Filtering
    builder.setBolt("filtering-bolt", new MetricFilteringBolt(config.database),
        config.filteringBoltThreads)
        .shuffleGrouping("metrics-spout")
        .allGrouping("event-bolt", EventProcessingBolt.METRIC_SUB_ALARM_EVENT_STREAM_ID)
        .allGrouping("event-bolt", EventProcessingBolt.METRIC_ALARM_EVENT_STREAM_ID)
        .setNumTasks(config.filteringBoltTasks);

    // Filtering / Event -> Aggregation
    builder.setBolt("aggregation-bolt",
        new MetricAggregationBolt(config.database, config.sporadicMetricNamespaces),
        config.aggregationBoltThreads)
        .fieldsGrouping("filtering-bolt", new Fields("metricDefinition"))
        .fieldsGrouping("event-bolt", EventProcessingBolt.METRIC_SUB_ALARM_EVENT_STREAM_ID,
            new Fields("metricDefinition"))
        .fieldsGrouping("event-bolt", EventProcessingBolt.METRIC_ALARM_EVENT_STREAM_ID,
            new Fields("metricDefinition"))
        .setNumTasks(config.aggregationBoltTasks);

    // Aggregation / Event -> Thresholding
    builder.setBolt("thresholding-bolt",
        new AlarmThresholdingBolt(config.database, config.kafkaProducerConfig),
        config.thresholdingBoltThreads)
        .fieldsGrouping("aggregation-bolt", new Fields("alarmId"))
        .fieldsGrouping("event-bolt", EventProcessingBolt.ALARM_EVENT_STREAM_ID,
            new Fields("alarmId"))
        .setNumTasks(config.thresholdingBoltTasks);

    return builder.createTopology();
  }
}
