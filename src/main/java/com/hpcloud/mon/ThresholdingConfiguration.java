package com.hpcloud.mon;

import com.hpcloud.configuration.KafkaConsumerConfiguration;

import java.util.Set;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import org.hibernate.validator.constraints.NotEmpty;

import com.hpcloud.configuration.KafkaConsumerConfiguration;
import com.hpcloud.mon.infrastructure.thresholding.DataSourceFactory;

/**
 * Thresholding configuration.
 * 
 * @author Jonathan Halterman
 */
public class ThresholdingConfiguration {
  public static final String ALERTS_EXCHANGE = "thresh.external.alerts";
  public static final String ALERTS_ROUTING_KEY = "thresh.external.alert";

  /** Total number of workers processes across the cluster. */
  @NotNull public Integer numWorkerProcesses = 12;
  /** Total number of acker threads across the cluster. */
  @NotNull public Integer numAckerThreads = 12;

  @NotNull public Integer metricSpoutThreads = 6;
  @NotNull public Integer metricSpoutTasks = 6;

  @NotNull public Integer eventSpoutThreads = 3;
  @NotNull public Integer eventSpoutTasks = 3;

  @NotNull public Integer eventBoltThreads = 3;
  @NotNull public Integer eventBoltTasks = 3;

  @NotNull public Integer filteringBoltThreads = 6;
  @NotNull public Integer filteringBoltTasks = 15;

  @NotNull public Integer aggregationBoltThreads = 12;
  @NotNull public Integer aggregationBoltTasks = 30;

  @NotNull public Integer thresholdingBoltThreads = 6;
  @NotNull public Integer thresholdingBoltTasks = 15;

  /** Namespaces for which metrics are received sporadically. */
  @NotNull public Set<String> sporadicMetricNamespaces;

  /** Configuration for the spout that receives metrics from the external exchange. */
  @Valid @NotNull public MetricSpoutConfig metricSpoutConfig;
  /** Configuration for the spout that receives MaaS events from the external exchange. */
  @Valid @NotNull public EventSpoutConfig eventSpoutConfig;

  /** Configuration for publishing to the alerts exchange on the external server. */
  @NotEmpty public String alertsExchange = "alerts";
  @NotEmpty public String alertsRoutingKey = "alert";
  @Valid @NotNull public KafkaProducerConfiguration externalRabbit = new KafkaProducerConfiguration();

  /** MaaS API database configuration. */
  @Valid @NotNull public DataSourceFactory database = new DataSourceFactory();
}
