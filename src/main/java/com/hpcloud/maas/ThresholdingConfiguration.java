package com.hpcloud.maas;

import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import org.hibernate.validator.constraints.NotEmpty;

import com.hpcloud.maas.infrastructure.storm.amqp.AMQPSpoutConfiguration;
import com.hpcloud.messaging.amqp.AMQPChannelConfiguration;
import com.hpcloud.messaging.rabbitmq.RabbitMQConfiguration;
import com.yammer.dropwizard.db.DatabaseConfiguration;

public class ThresholdingConfiguration {
  @NotNull public Integer metricSpoutParallelism = 3;
  @NotNull public Integer eventSpoutParallelism = 1;
  @NotNull public Integer eventBoltParallelism = 2;
  @NotNull public Integer aggregationParallelism = 10;
  @NotNull public Integer thresholdingParallelism = 3;

  /** Configuration for the spout that receives metrics from the internal exchange. */
  @Valid @NotNull public AMQPSpoutConfiguration metricSpout;
  /** Configuration for the spout that receives messages from the API servers. */
  @Valid @NotNull public AMQPSpoutConfiguration apiSpout;

  @Valid @NotNull public RabbitMQConfiguration internalRabbit = new RabbitMQConfiguration();
  /** Threshold for scaling internal connections up */
  @NotNull @Min(1) public Integer internalConnectionScalingThreshold;
  /** Max number of total internal connections */
  @NotNull public Integer maxInternalConnections;

  @Valid @NotNull public RabbitMQConfiguration externalRabbit = new RabbitMQConfiguration();

  @Valid @NotNull public AMQPChannelConfiguration controlChannel = new AMQPChannelConfiguration();
  @NotEmpty public String internalExchange;
  @NotEmpty public String externalExchange;
  @NotEmpty public String controlExchange;
  @NotEmpty public String controlRoutingKey;

  @Valid @NotNull public DatabaseConfiguration database = new DatabaseConfiguration();
}
