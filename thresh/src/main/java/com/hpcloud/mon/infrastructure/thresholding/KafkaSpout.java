/*
 * Copyright (c) 2014 Hewlett-Packard Development Company, L.P.
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

package com.hpcloud.mon.infrastructure.thresholding;

import com.hpcloud.configuration.KafkaConsumerProperties;
import com.hpcloud.mon.KafkaSpoutConfig;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichSpout;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public abstract class KafkaSpout extends BaseRichSpout implements Runnable {
  private static final Logger logger = LoggerFactory.getLogger(KafkaSpout.class);

  private static final long serialVersionUID = 744004533863562119L;

  private final KafkaSpoutConfig kafkaSpoutConfig;

  private transient ConsumerConnector consumerConnector;

  private transient List<KafkaStream<byte[], byte[]>> streams = null;

  private SpoutOutputCollector collector;

  private volatile boolean shouldContinue;

  private byte[] message;

  private Thread readerThread;

  private String spoutName;

  private boolean waiting = false;

  protected KafkaSpout(KafkaSpoutConfig kafkaSpoutConfig) {
    this.kafkaSpoutConfig = kafkaSpoutConfig;
  }

  @Override
  public void activate() {
    logger.info("Activated");
    if (streams == null) {
      Map<String, Integer> topicCountMap = new HashMap<>();
      topicCountMap.put(kafkaSpoutConfig.kafkaConsumerConfiguration.getTopic(), new Integer(1));
      Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap =
          consumerConnector.createMessageStreams(topicCountMap);
      streams = consumerMap.get(kafkaSpoutConfig.kafkaConsumerConfiguration.getTopic());
    }
  }

  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    logger.info("Opened");
    this.collector = collector;
    logger.info(" topic = " + kafkaSpoutConfig.kafkaConsumerConfiguration.getTopic());
    this.spoutName = String.format("%s-%d", context.getThisComponentId(), context.getThisTaskId());

    Properties kafkaProperties =
        KafkaConsumerProperties.createKafkaProperties(kafkaSpoutConfig.kafkaConsumerConfiguration);
    // Have to use a different consumer.id for each spout so use the storm taskId. Otherwise,
    // zookeeper complains about a conflicted ephemeral node when there is more than one spout
    // reading from a topic
    kafkaProperties.setProperty("consumer.id", String.valueOf(context.getThisTaskId()));
    ConsumerConfig consumerConfig = new ConsumerConfig(kafkaProperties);
    this.consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
  }

  @Override
  public synchronized void deactivate() {
    logger.info("deactivated");
    this.consumerConnector.shutdown();
    this.shouldContinue = false;
    // Wake up the reader thread if it is waiting
    notify();
  }

  @Override
  public void run() {
    while (this.shouldContinue) {
      final ConsumerIterator<byte[], byte[]> it = streams.get(0).iterator();
      if (it.hasNext()) {
        final byte[] message = it.next().message();
        synchronized (this) {
          this.message = message;
          // Wake up getMessage() if it is waiting
          if (this.waiting) {
            notify();
          }
          while (this.message != null && this.shouldContinue)
            try {
              wait();
            } catch (InterruptedException e) {
              logger.info("Wait interrupted", e);
            }
        }
      }
    }
    logger.info("readerThread {} exited", this.readerThread.getName());
    this.readerThread = null;
  }

  @Override
  public void nextTuple() {
    logger.debug("nextTuple called");
    checkReaderRunning();
    final byte[] message = getMessage();
    if (message != null) {
      logger.debug("streams iterator has next");
      processMessage(message, collector);
    }
  }

  private void checkReaderRunning() {
    this.shouldContinue = true;
    if (this.readerThread == null) {
      final String threadName = String.format("%s reader", this.spoutName);
      this.readerThread = new Thread(this, threadName);
      this.readerThread.start();
      logger.info("Started Reader Thread {}", this.readerThread.getName());
    }
  }

  /**
   * Must only be called from a synchronized method
   *
   * @return
   */
  private byte[] tryToGetMessage() {
    final byte[] result = this.message;
    if (result != null) {
      this.message = null;
      notify();
    }
    return result;
  }

  private synchronized byte[] getMessage() {
    final byte[] result = tryToGetMessage();
    if (result != null) {
      return result;
    }
    // Storm docs recommend a short sleep but make the sleep time
    // configurable so we can lessen the load on dev systems
    this.waiting = true;
    try {
      wait(kafkaSpoutConfig.maxWaitTime);
    } catch (InterruptedException e) {
      logger.info("Sleep interrupted", e);
    }
    this.waiting = false;
    return tryToGetMessage(); // We might have been woken up because there was a message
  }

  protected abstract void processMessage(byte[] message, SpoutOutputCollector collector2);
}
