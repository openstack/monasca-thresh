package com.hpcloud.mon.infrastructure.thresholding;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.hpcloud.mon.MetricSpoutConfig;
import com.hpcloud.mon.common.model.metric.MetricEnvelope;
import com.hpcloud.mon.common.model.metric.MetricEnvelopes;
import com.hpcloud.mon.domain.model.MetricDefinitionAndTenantId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricSpout extends KafkaSpout {
    private static final Logger LOG = LoggerFactory.getLogger(MetricSpout.class);

    private static final long serialVersionUID = 744004533863562119L;

    public static final String[] FIELDS = new String[] { "metricDefinitionAndTenantId", "metric" };
    public static final String DEFAULT_TENANT_ID = "TENANT_ID_NOT_SET";

    public MetricSpout(MetricSpoutConfig metricSpoutConfig) {
        super(metricSpoutConfig.kafkaConsumerConfiguration);
        LOG.info("Created");
    }

    @Override
    protected void processMessage(byte[] message, SpoutOutputCollector collector) {
        final MetricEnvelope metricEnvelope;
        try {
            metricEnvelope = MetricEnvelopes.fromJson(message);
            LOG.debug("metric envelope: {}", metricEnvelope);
        }
        catch (RuntimeException re) {
            LOG.warn("Error parsing MetricEnvelope", re);
            return;
        }
        String tenantId = (String)metricEnvelope.meta.get("tenantId");
        if (tenantId == null) {
            LOG.error("No tenantId so using default tenantId {} for Metric {}", DEFAULT_TENANT_ID, metricEnvelope.metric);
            tenantId = DEFAULT_TENANT_ID;
        }
        collector.emit(new Values(new MetricDefinitionAndTenantId(metricEnvelope.metric.definition(), tenantId), metricEnvelope.metric));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(FIELDS));
    }
}
