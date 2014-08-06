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

package monasca.thresh.infrastructure.thresholding;

import monasca.thresh.MetricSpoutConfig;
import com.hpcloud.mon.common.model.metric.MetricEnvelope;
import com.hpcloud.mon.common.model.metric.MetricEnvelopes;
import monasca.thresh.domain.model.MetricDefinitionAndTenantId;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricSpout extends KafkaSpout {
  private static final Logger logger = LoggerFactory.getLogger(MetricSpout.class);

  private static final long serialVersionUID = 744004533863562119L;

  public static final String[] FIELDS = new String[] {"metricDefinitionAndTenantId",
      "apiTimeStamp", "metric"};
  public static final String DEFAULT_TENANT_ID = "TENANT_ID_NOT_SET";

  public MetricSpout(MetricSpoutConfig metricSpoutConfig) {
    super(metricSpoutConfig);
    logger.info("Created");
  }

  @Override
  protected void processMessage(byte[] message, SpoutOutputCollector collector) {
    final MetricEnvelope metricEnvelope;
    try {
      metricEnvelope = MetricEnvelopes.fromJson(message);
      logger.debug("metric envelope: {}", metricEnvelope);
    } catch (RuntimeException re) {
      logger.warn("Error parsing MetricEnvelope", re);
      return;
    }
    String tenantId = (String) metricEnvelope.meta.get("tenantId");
    if (tenantId == null) {
      logger.error("No tenantId so using default tenantId {} for Metric {}", DEFAULT_TENANT_ID,
          metricEnvelope.metric);
      tenantId = DEFAULT_TENANT_ID;
    }
    collector.emit(new Values(new MetricDefinitionAndTenantId(metricEnvelope.metric.definition(),
        tenantId), metricEnvelope.creationTime, metricEnvelope.metric));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields(FIELDS));
  }
}
