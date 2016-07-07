/*
 * (C) Copyright 2015-2016 Hewlett Packard Enterprise Development Company LP.
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

import monasca.thresh.infrastructure.thresholding.AlarmCreationBolt;
import monasca.thresh.infrastructure.thresholding.AlarmThresholdingBolt;
import monasca.thresh.infrastructure.thresholding.EventProcessingBolt;
import monasca.thresh.infrastructure.thresholding.EventSpout;
import monasca.thresh.infrastructure.thresholding.MetricAggregationBolt;
import monasca.thresh.infrastructure.thresholding.MetricFilteringBolt;
import monasca.thresh.infrastructure.thresholding.MetricSpout;
import monasca.thresh.infrastructure.thresholding.deserializer.EventDeserializer;
import monasca.thresh.utils.StatsdMetricConsumer;

import monasca.common.util.Injector;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import javax.inject.Named;

/**
 * Configures types for the thresholding topology.
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
  protected void configure() {}

  @Provides
  Config stormConfig() {
    if (stormConfig == null) {
      stormConfig = new Config();
      stormConfig.setNumWorkers(config.numWorkerProcesses);
      stormConfig.setNumAckers(config.numAckerThreads);

      /* Configure the StatsdMetricConsumer */
      java.util.Map<Object, Object> statsdConfig = new java.util.HashMap<>();

      /*
       * Catch the case where the config file was not updated
       * in /etc/monasca/thresh-config.yml
       * note that you get default values if these are absent
       */
      if (config.statsdConfig.getHost() != null)
          statsdConfig.put(StatsdMetricConsumer.STATSD_HOST,
                  config.statsdConfig.getHost());
      if (config.statsdConfig.getPort() != null)
          statsdConfig.put(StatsdMetricConsumer.STATSD_PORT,
                  config.statsdConfig.getPort());
      if (config.statsdConfig.getWhitelist() != null)
          statsdConfig.put(StatsdMetricConsumer.STATSD_WHITELIST,
                  config.statsdConfig.getWhitelist());
      if (config.statsdConfig.getMetricmap() != null)
        statsdConfig.put(StatsdMetricConsumer.STATSD_METRICMAP,
            config.statsdConfig.getMetricmap());
      if (config.statsdConfig.getDimensions() != null)
          statsdConfig.put(StatsdMetricConsumer.STATSD_DIMENSIONS,
                  config.statsdConfig.getDimensions());
      if (config.statsdConfig.getDebugmetrics() != null)
        statsdConfig.put(StatsdMetricConsumer.STATSD_DEBUGMETRICS,
            config.statsdConfig.getDebugmetrics());

      stormConfig.registerMetricsConsumer(StatsdMetricConsumer.class,
              statsdConfig, 2);
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

    // Event -> Events
    builder
        .setBolt("event-bolt", new EventProcessingBolt(config.database), config.eventBoltThreads)
        .shuffleGrouping("event-spout").setNumTasks(config.eventBoltTasks);

    // Metrics / Event -> Filtering
    builder
        .setBolt("filtering-bolt", new MetricFilteringBolt(config.database),
            config.filteringBoltThreads)
        .fieldsGrouping("metrics-spout", new Fields(MetricSpout.FIELDS[0]))
        .allGrouping("event-bolt", EventProcessingBolt.METRIC_ALARM_EVENT_STREAM_ID)
        .allGrouping("event-bolt", EventProcessingBolt.ALARM_DEFINITION_EVENT_STREAM_ID)
        .setNumTasks(config.filteringBoltTasks);

    // Filtering /Event -> Alarm Creation
    builder
        .setBolt("alarm-creation-bolt", new AlarmCreationBolt(config.database),
            config.alarmCreationBoltThreads)
        .fieldsGrouping("filtering-bolt",
            MetricFilteringBolt.NEW_METRIC_FOR_ALARM_DEFINITION_STREAM,
            new Fields(AlarmCreationBolt.ALARM_CREATION_FIELDS[3]))
        .allGrouping("event-bolt", EventProcessingBolt.METRIC_SUB_ALARM_EVENT_STREAM_ID)
        .allGrouping("event-bolt", EventProcessingBolt.ALARM_EVENT_STREAM_ID)
        .allGrouping("event-bolt", EventProcessingBolt.ALARM_DEFINITION_EVENT_STREAM_ID)
        .setNumTasks(config.alarmCreationBoltTasks);

    // Filtering / Event / Alarm Creation -> Aggregation
    builder
        .setBolt("aggregation-bolt",
            new MetricAggregationBolt(config), config.aggregationBoltThreads)
        .fieldsGrouping("filtering-bolt", new Fields(MetricFilteringBolt.FIELDS[0]))
        .allGrouping("filtering-bolt", MetricAggregationBolt.METRIC_AGGREGATION_CONTROL_STREAM)
        .fieldsGrouping("filtering-bolt", AlarmCreationBolt.ALARM_CREATION_STREAM,
            new Fields(AlarmCreationBolt.ALARM_CREATION_FIELDS[1]))
        .allGrouping("event-bolt", EventProcessingBolt.METRIC_SUB_ALARM_EVENT_STREAM_ID)
        .fieldsGrouping("event-bolt", EventProcessingBolt.METRIC_ALARM_EVENT_STREAM_ID,
            new Fields(EventProcessingBolt.METRIC_ALARM_EVENT_STREAM_FIELDS[1]))
        .fieldsGrouping("alarm-creation-bolt", AlarmCreationBolt.ALARM_CREATION_STREAM,
            new Fields(AlarmCreationBolt.ALARM_CREATION_FIELDS[1]))
        .setNumTasks(config.aggregationBoltTasks);

    // Alarm Creation / Event
    // Aggregation / Event -> Thresholding
    builder
        .setBolt("thresholding-bolt",
            new AlarmThresholdingBolt(config.database, config.kafkaProducerConfig),
            config.thresholdingBoltThreads)
        .fieldsGrouping("aggregation-bolt", new Fields(MetricAggregationBolt.FIELDS[0]))
        .fieldsGrouping("event-bolt", EventProcessingBolt.ALARM_EVENT_STREAM_ID,
            new Fields(EventProcessingBolt.ALARM_EVENT_STREAM_FIELDS[1]))
        .allGrouping("event-bolt", EventProcessingBolt.ALARM_DEFINITION_EVENT_STREAM_ID)
        .allGrouping("event-bolt", EventProcessingBolt.METRIC_SUB_ALARM_EVENT_STREAM_ID)
        .setNumTasks(config.thresholdingBoltTasks);

    return builder.createTopology();
  }
}
