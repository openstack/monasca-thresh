/*
 * (C) Copyright 2014,2016 Hewlett Packard Enterprise Development Company LP.
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

import monasca.common.model.metric.Metric;
import monasca.common.model.metric.MetricEnvelope;
import monasca.common.model.metric.MetricEnvelopes;
import monasca.thresh.MetricSpoutConfig;
import monasca.thresh.domain.model.TenantIdAndMetricName;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;

public class MetricSpout extends KafkaSpout {
  @SuppressWarnings("unchecked")
  private static final Map<String, String> EMPTY_DIMENSIONS = (Map<String, String>) Collections.EMPTY_MAP;

  private static final Logger logger = LoggerFactory.getLogger(MetricSpout.class);

  private static final long serialVersionUID = 744004533863562119L;

  public static final String[] FIELDS = new String[] {"tenantIdAndMetricName",
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
    if (null == metricEnvelope.meta) {
      logger.warn("No tenant metadata error");
      return;
    }
    String tenantId = (String) metricEnvelope.meta.get("tenantId");
    if (tenantId == null) {
      logger.error("No tenantId so using default tenantId {} for Metric {}", DEFAULT_TENANT_ID,
          metricEnvelope.metric);
      tenantId = DEFAULT_TENANT_ID;
    }
    final Metric metric = metricEnvelope.metric;
    if (metric.dimensions == null) {
      metric.dimensions = EMPTY_DIMENSIONS;
    }
    collector.emit(new Values(new TenantIdAndMetricName(tenantId, metricEnvelope.metric
        .definition().name), metricEnvelope.creationTime, metric));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields(FIELDS));
  }
}
