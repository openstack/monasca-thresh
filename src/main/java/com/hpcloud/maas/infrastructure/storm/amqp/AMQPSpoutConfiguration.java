package com.hpcloud.maas.infrastructure.storm.amqp;

import java.io.Serializable;

import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import org.hibernate.validator.constraints.NotEmpty;

import backtype.storm.spout.Scheme;

import com.hpcloud.messaging.rabbitmq.RabbitMQConfiguration;
import com.hpcloud.util.Duration;

/**
 * Encapsulates AMQP related configuration and constraints.
 * 
 * @author Jonathan Halterman
 */
public class AMQPSpoutConfiguration implements Serializable {
  private static final long serialVersionUID = 6386682494713707182L;

  @Valid @NotNull public RabbitMQConfiguration rabbit;

  /** Exchange to consume messages from. */
  @NotEmpty public String exchange;
  /** Queue name to consume messages from. If null a random queue will be created. */
  @Nullable public String queueName;
  /** Routing keys to bind to queue. */
  @NotEmpty public String[] routingKeys;

  /**
   * Indicates whether rejected messages should be re-queued. Note: This can lead to infinite loops
   * if message failure is not transient.
   * 
   * Default: false;
   */
  @NotNull public Boolean requeueOnFail = false;

  /**
   * Time in milliseconds to wait to read the next message from the queue after all messages have
   * been read.
   * 
   * Default: 1 millis
   */
  @NotNull public Duration waitForNextMessage = Duration.millis(1);

  /**
   * Defaults to 100.
   * 
   * <p>
   * This caps the number of messages outstanding (i.e. unacked) at a time that will be sent to each
   * spout worker. Increasing this will improve throughput if the network roundtrip time to the AMQP
   * broker is significant compared to the time for the topology to process each message; this will
   * also increase the RAM requirements as the internal message buffer grows.
   * </p>
   * 
   * <p>
   * AMQP allows a prefetch-count of zero, indicating unlimited delivery, but that is not allowed
   * here to avoid unbounded buffer growth.
   * </p>
   * 
   * Default: 100.
   */
  @Min(1) public int prefetchCount = 100;

  /**
   * Name of the stream where malformed deserialized messages are sent for special handling.
   * Generally used when a {@link Scheme} implementation returns null or a zero-length tuple.
   * 
   * Default: error-stream.
   */
  @NotEmpty public String errorStream = "error-stream";
}
