package com.hpcloud.maas;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import com.hpcloud.maas.infrastructure.storm.amqp.AMQPSpoutConfiguration;
import com.hpcloud.messaging.rabbitmq.RabbitMQConfiguration;
import com.yammer.dropwizard.db.DatabaseConfiguration;

/**
 * Thresholding configuration.
 * 
 * @author Jonathan Halterman
 */
public class ThresholdingConfiguration {
  @NotNull public Integer metricSpoutParallelism = 3;
  @NotNull public Integer eventSpoutParallelism = 1;
  @NotNull public Integer eventBoltParallelism = 2;
  @NotNull public Integer aggregationParallelism = 10;
  @NotNull public Integer thresholdingParallelism = 3;

  /** Configuration for the spout that receives metrics from the internal exchange. */
  @Valid @NotNull public AMQPSpoutConfiguration metricSpout;
  /** Configuration for the spout that receives messages from the API servers. */
  @Valid @NotNull public AMQPSpoutConfiguration eventSpout;
  /** Configuration for publishing to the alerts exchange. */
  @Valid @NotNull public RabbitMQConfiguration externalRabbit = new RabbitMQConfiguration();
  /** MaaS API database configuration. */
  @Valid @NotNull public DatabaseConfiguration database = new DatabaseConfiguration();
}
