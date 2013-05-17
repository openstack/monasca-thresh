package com.hpcloud.maas.infrastructure.storm.amqp;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.Scheme;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.google.inject.assistedinject.Assisted;
import com.hpcloud.maas.infrastructure.storm.TupleDeserializer;
import com.hpcloud.messaging.rabbitmq.RabbitMQConnection;
import com.hpcloud.messaging.rabbitmq.RabbitMQConnection.RabbitMQConnectionOptions;
import com.hpcloud.messaging.rabbitmq.RabbitMQConnection.RabbitMQConnectionProvider;
import com.hpcloud.messaging.rabbitmq.RabbitMQModule;
import com.hpcloud.supervision.SupervisionModule;
import com.hpcloud.util.Exceptions;
import com.hpcloud.util.Injector;
import com.rabbitmq.client.AMQP.Queue.DeclareOk;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;

/**
 * Spout to feed messages into Storm from an AMQP queue. Each message routed to the queue will be
 * emitted as a Storm tuple. The message will be acked or rejected once the topology has
 * respectively fully processed or failed the corresponding tuple.
 * 
 * <p>
 * <strong>N.B.</strong> if you need to guarantee all messages are reliably processed, you should
 * have AMQPSpout consume from a queue that is <em>not</em> set as 'exclusive' or 'auto-delete':
 * otherwise if the spout task crashes or is restarted, the queue will be deleted and any messages
 * in it lost, as will any messages published while the task remains down. See
 * {@link com.hpcloud.maas.infrastructure.storm.amqp.SharedQueueWithBinding} to declare a shared
 * queue that allows for guaranteed processing. (For prototyping, an
 * {@link com.hpcloud.maas.infrastructure.storm.amqp.ExclusiveQueueWithBinding} may be simpler to
 * manage.)
 * </p>
 * 
 * <p>
 * <strong>N.B.</strong> this does not currently handle malformed messages (which cannot be
 * deserialised by the provided {@link Scheme}) very well: the spout worker will crash if it fails
 * to serialise a message.
 * </p>
 * 
 * <p>
 * This consumes messages from AMQP asynchronously, so it may receive messages before Storm requests
 * them as tuples; therefore it buffers messages in an internal queue. To avoid this buffer growing
 * large and consuming too much RAM, set the config prefetchCount.
 * </p>
 * 
 * <p>
 * This spout can be distributed among multiple workers, depending on the queue declaration: see
 * {@link QueueDeclarator#isParallelConsumable}.
 * </p>
 * 
 * @see QueueDeclarator
 * @see com.hpcloud.maas.infrastructure.storm.amqp.SharedQueueWithBinding
 * @see com.hpcloud.maas.infrastructure.storm.amqp.ExclusiveQueueWithBinding
 * 
 * @author Sam Stokes (sam@rapportive.com)
 * @author Jonathan Halterman (jonathan@jodah.org)
 */
public class AMQPSpout implements IRichSpout {
  private static final long serialVersionUID = 11258942292629264L;
  private static final Logger LOG = LoggerFactory.getLogger(AMQPSpout.class);

  private final AMQPSpoutConfiguration config;
  private final TupleDeserializer deserializer;
  private final long waitForNextMessageMillis;
  private final QueueDeclarator queueDeclarator;

  private transient boolean spoutActive = true;
  private transient RabbitMQConnection connection;
  private transient Channel channel;
  private transient QueueingConsumer consumer;
  private transient String consumerTag;

  private TopologyContext context;
  private SpoutOutputCollector collector;

  public AMQPSpout(@Assisted AMQPSpoutConfiguration config, @Assisted TupleDeserializer deserializer) {
    this.config = config;
    this.deserializer = deserializer;
    this.waitForNextMessageMillis = config.waitForNextMessage.toMillis();
    this.queueDeclarator = config.queueName == null ? new ExclusiveQueueWithBinding(
        config.exchange, config.routingKeys) : new SharedQueueWithBinding(config.queueName,
        config.exchange, config.routingKeys);
  }

  /**
   * Acks the message with the AMQP broker using the message's delivery tack.
   */
  @Override
  public void ack(Object msgId) {
    LOG.trace("{} Acked message {}", context.getThisTaskId(), msgId);

    if (msgId instanceof Long) {
      final long deliveryTag = (Long) msgId;
      if (channel != null) {
        try {
          channel.basicAck(deliveryTag, false /* not multiple */);
        } catch (IOException e) {
          LOG.warn("Failed to ack delivery-tag {}", deliveryTag, e);
        }
      }
    } else {
      LOG.warn("Don't know how to ack({}: {})", msgId.getClass().getName(), msgId);
    }
  }

  /**
   * Resumes a paused spout.
   */
  public void activate() {
    LOG.info("{} Activating spout {}", context.getThisTaskId(), context.getThisComponentId());

    try {
      connection.open();
      channel = connection.channelFor("spout");
      if (config.prefetchCount > 0)
        channel.basicQos(config.prefetchCount);
      DeclareOk declareOk = queueDeclarator.declare(channel);
      consumer = new QueueingConsumer(channel);
      LOG.info("Consuming from queue {}", declareOk.getQueue());
      consumerTag = channel.basicConsume(declareOk.getQueue(), config.autoAck, consumer);
      spoutActive = true;
    } catch (Exception e) {
      LOG.error("Error while opening connection", e);
      throw Exceptions.uncheck(e, "{} Failed to open AMQP connection", context.getThisTaskId());
    }
  }

  /**
   * Cancels the queue subscription, and disconnects from the AMQP broker.
   */
  @Override
  public void close() {
    LOG.info("{} Closing AMQP spout {}", context.getThisTaskId(), context.getThisComponentId());
    if (consumerTag != null) {
      try {
        channel.basicCancel(consumerTag);
      } catch (IOException e) {
        LOG.warn("Error cancelling AMQP consumer", e);
      }
    }

    if (connection != null)
      connection.close();
  }

  /**
   * Pauses the spout
   */
  public void deactivate() {
    LOG.info("{} Deactivating AMQP spout {}", context.getThisTaskId(), context.getThisComponentId());
    if (connection != null)
      connection.close();
    spoutActive = false;
  }

  /**
   * Declares the output fields of this spout according to the provided
   * {@link backtype.storm.spout.Scheme}.
   * 
   * Additionally declares an error stream (see {@link #ERROR_STREAM_NAME} for handling malformed or
   * empty messages to avoid infinite retry loops
   */
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(deserializer.getOutputFields());
    declarer.declareStream(config.errorStream, new Fields("deliveryTag", "bytes"));
  }

  /**
   * Tells the AMQP broker to reject the message, requeueing the message if configured to do so.
   * <p>
   * <strong>Note:</strong> There's a potential for infinite re-delivery in the event of
   * non-transient failures (e.g. malformed messages).
   */
  @Override
  public void fail(Object msgId) {
    LOG.trace("{} Failed message {}", context.getThisTaskId(), msgId);

    if (msgId instanceof Long) {
      final long deliveryTag = (Long) msgId;
      if (channel != null) {
        try {
          channel.basicReject(deliveryTag, config.requeueOnFail);
        } catch (IOException e) {
          LOG.warn("Failed to reject delivery-tag " + deliveryTag, e);
        }
      }
    } else {
      LOG.warn(String.format("Cannot reject unknown delivery tag (%s: %s)", msgId.getClass()
          .getName(), msgId));
    }
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }

  /**
   * Emits the next message from the queue as a tuple.
   * 
   * Serialization schemes returning null will immediately ack and then emit unanchored on the
   * {@link #ERROR_STREAM_NAME} stream for further handling by the consumer.
   * 
   * <p>
   * If no message is ready to emit, this will wait a short time ({@link #WAIT_FOR_NEXT_MESSAGE})
   * for one to arrive on the queue, to avoid a tight loop in the spout worker.
   * </p>
   */
  @Override
  @SuppressWarnings("unchecked")
  public void nextTuple() {
    if (spoutActive) {
      QueueingConsumer.Delivery delivery = null;

      try {
        delivery = consumer.nextDelivery(waitForNextMessageMillis);
        if (delivery == null)
          return;

        final long deliveryTag = delivery.getEnvelope().getDeliveryTag();
        final byte[] message = delivery.getBody();

        try {
          List<List<?>> tuples = deserializer.deserialize(message);
          if (tuples == null) {
            if (!config.autoAck)
              ack(deliveryTag);
            return;
          }

          for (int i = 0; i < tuples.size(); i++) {
            List<Object> tuple = (List<Object>) tuples.get(i);
            // LOG.trace("{} {} Emitting {} for {}", deliveryTag, context.getThisTaskId(), tuple,
            // message);

            if (i > 0 || config.autoAck)
              collector.emit(tuple);
            else
              collector.emit(tuple, deliveryTag);
          }
        } catch (Exception e) {
          LOG.error("Error while deserializing and emitting message", e);
          handleUnprocessableDelivery(deliveryTag, message);
        }
      } catch (ShutdownSignalException e) {
        LOG.error("{} AMQP connection dropped.", context.getThisTaskId(), e);
        if (!e.isInitiatedByApplication())
          connection.reopen();
      } catch (InterruptedException e) {
        // interrupted while waiting for message, big deal
      }
    }
  }

  /**
   * Connects to the AMQP broker, declares the queue and subscribes to incoming messages.
   */
  @Override
  public void open(@SuppressWarnings("rawtypes") Map stormConfig, TopologyContext context,
      SpoutOutputCollector collector) {
    LOG.info("{} Opening AMQP Spout {}", context.getThisTaskId(), context.getThisComponentId());
    this.context = context;
    this.collector = collector;

    Injector.registerModules(new SupervisionModule());
    Injector.registerIfNotBound(RabbitMQConnectionProvider.class, new RabbitMQModule());
    connection = Injector.getInstance(RabbitMQConnectionProvider.class)
        .get(
            new RabbitMQConnectionOptions("amqp-spout-" + context.getThisTaskId()).withPrefetchCount(config.prefetchCount),
            config.rabbit, null);
  }

  /**
   * Acks the bad message to avoid retry loops. Also emits the bad message unreliably on the
   * {@link #ERROR_STREAM_NAME} stream for consumer handling.
   * 
   * @param deliveryTag AMQP delivery tag
   * @param message bytes of the bad message
   */
  private void handleUnprocessableDelivery(long deliveryTag, byte[] message) {
    ack(deliveryTag);
    collector.emit(config.errorStream, new Values(deliveryTag, message));
  }
}
