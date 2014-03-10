package com.hpcloud.mon;

import io.dropwizard.db.DataSourceFactory;

import java.util.Set;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import org.hibernate.validator.constraints.NotEmpty;

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

  @NotNull public Integer maasMetricSpoutThreads = 6;
  @NotNull public Integer maasMetricSpoutTasks = 6;

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

  /** Configuration for the spout that receives MaaS metrics from the external exchange. */
  @Valid @NotNull public AMQPSpoutConfiguration maasMetricSpout;
  /** Configuration for the spout that receives MaaS events from the external exchange. */
  @Valid @NotNull public AMQPSpoutConfiguration eventSpout;

  /** Configuration for publishing to the alerts exchange on the external server. */
  @NotEmpty public String alertsExchange = "alerts";
  @NotEmpty public String alertsRoutingKey = "alert";
  @Valid @NotNull public RabbitMQConfiguration externalRabbit = new RabbitMQConfiguration();

  /** MaaS API database configuration. */
  @Valid @NotNull public DataSourceFactory database = new DataSourceFactory();
}
