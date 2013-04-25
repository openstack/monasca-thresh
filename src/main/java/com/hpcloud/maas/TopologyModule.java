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
import com.hpcloud.maas.infrastructure.thresholding.CollectdMetricDeserializer;
import com.hpcloud.maas.infrastructure.thresholding.EventProcessingBolt;
import com.hpcloud.maas.infrastructure.thresholding.MaasEventDeserializer;
import com.hpcloud.maas.infrastructure.thresholding.MaasMetricDeserializer;
import com.hpcloud.maas.infrastructure.thresholding.MetricAggregationBolt;
import com.hpcloud.util.Injector;

/**
 * Configures types for the thresholding topology.
 * 
 * @author Jonathan Halterman
 */
public class TopologyModule extends AbstractModule {
  private final ThresholdingConfiguration config;
  private Config stormConfig;
  private IRichSpout collectdMetricSpout;
  private IRichSpout eventSpout;

  public TopologyModule(ThresholdingConfiguration config) {
    this.config = config;
  }

  public TopologyModule(ThresholdingConfiguration threshConfig, Config stormConfig,
      IRichSpout collectdMetricSpout, IRichSpout eventSpout) {
    this(threshConfig);
    this.stormConfig = stormConfig;
    this.collectdMetricSpout = collectdMetricSpout;
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
  @Named("event")
  IRichSpout eventSpout() {
    return eventSpout == null ? new AMQPSpout(config.eventSpout, new MaasEventDeserializer())
        : eventSpout;
  }

  @Provides
  @Named("collectd-metrics")
  IRichSpout collectdMetricsSpout() {
    return collectdMetricSpout == null ? new AMQPSpout(config.collectdMetricSpout,
        new CollectdMetricDeserializer()) : collectdMetricSpout;
  }

  @Provides
  @Named("maas-metrics")
  IRichSpout maasMetricsSpout() {
    return new AMQPSpout(config.maasMetricSpout, new MaasMetricDeserializer());
  }

  @Provides
  StormTopology topology() {
    TopologyBuilder builder = new TopologyBuilder();

    // Receives CollectD Metrics
    builder.setSpout("collectd-metrics-spout",
        Injector.getInstance(IRichSpout.class, "collectd-metrics"),
        config.collectdMetricSpoutThreads).setNumTasks(config.collectdMetricSpoutTasks);

    // Receives MaaS Metrics
    builder.setSpout("maas-metrics-spout", Injector.getInstance(IRichSpout.class, "maas-metrics"),
        config.maasMetricSpoutThreads).setNumTasks(config.maasMetricSpoutTasks);

    // Receives MaaS events
    builder.setSpout("event-spout", Injector.getInstance(IRichSpout.class, "event"),
        config.eventSpoutThreads).setNumTasks(config.eventSpoutTasks);

    // MaaS Event -> Events
    builder.setBolt("event-bolt", new EventProcessingBolt(), config.eventBoltThreads)
        .shuffleGrouping("event-spout")
        .setNumTasks(config.eventBoltTasks);

    // Metrics / Event -> Aggregation
    builder.setBolt("aggregation-bolt", new MetricAggregationBolt(config.database),
        config.aggregationBoltThreads)
        .fieldsGrouping("collectd-metrics-spout", new Fields("metricDefinition"))
        .fieldsGrouping("maas-metrics-spout", new Fields("metricDefinition"))
        .fieldsGrouping("event-bolt", EventProcessingBolt.METRIC_SUB_ALARM_EVENT_STREAM_ID,
            new Fields("metricDefinition"))
        .fieldsGrouping("event-bolt", EventProcessingBolt.METRIC_ALARM_EVENT_STREAM_ID,
            new Fields("metricDefinition"))
        .setNumTasks(config.aggregationBoltTasks);

    // Aggregation / Event -> Thresholding
    builder.setBolt("thresholding-bolt",
        new AlarmThresholdingBolt(config.database, config.externalRabbit),
        config.thresholdingBoltThreads)
        .fieldsGrouping("aggregation-bolt", new Fields("alarmId"))
        .fieldsGrouping("event-bolt", EventProcessingBolt.ALARM_EVENT_STREAM_ID,
            new Fields("alarmId"))
        .setNumTasks(config.thresholdingBoltTasks);

    return builder.createTopology();
  }
}
