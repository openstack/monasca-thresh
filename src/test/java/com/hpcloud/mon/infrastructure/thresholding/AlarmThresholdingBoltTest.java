package com.hpcloud.mon.infrastructure.thresholding;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.times;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import backtype.storm.Testing;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.MkTupleParam;
import backtype.storm.tuple.Tuple;

import com.hpcloud.mon.ThresholdingConfiguration;
import com.hpcloud.mon.common.model.alarm.AlarmExpression;
import com.hpcloud.mon.common.model.alarm.AlarmState;
import com.hpcloud.mon.common.model.alarm.AlarmSubExpression;
import com.hpcloud.mon.domain.model.Alarm;
import com.hpcloud.mon.domain.model.SubAlarm;
import com.hpcloud.mon.domain.service.AlarmDAO;
import com.hpcloud.streaming.storm.Streams;

@Test
public class AlarmThresholdingBoltTest {

    private static final String ALERT_ROUTING_KEY = "Alert Routing Key";
	private static final String ALERTS_EXCHANGE = "Alerts";

    private AlarmExpression alarmExpression;
    private Alarm alarm;
    private List<SubAlarm> subAlarms;

    private AlarmEventForwarder alarmEventForwarder;
    private AlarmDAO alarmDAO;
    private AlarmThresholdingBolt bolt;
    private OutputCollector collector;

    @BeforeClass
    protected void beforeClass() {
        final String alarmId = "111111112222222222233333333334";
        final String tenantId = "AAAAABBBBBBCCCCC";
        final String expression = "avg(hpcs.compute.cpu{instance_id=123,device=42}, 1) > 5";
        alarm = new Alarm();
        alarm.setName("Test CPU Alarm");
        alarm.setDescription("Description of Alarm");
        alarm.setTenantId(tenantId);
        alarm.setId(alarmId);
        alarm.setExpression(expression);
        alarm.setState(AlarmState.OK);
        alarmExpression = new AlarmExpression(expression);
        final List<AlarmSubExpression> subExpressions = alarmExpression.getSubExpressions();
        subAlarms = new ArrayList<SubAlarm>(subExpressions.size());
        int subAlarmId = 4242;
        for (int i = 0; i < subExpressions.size(); i++) {
            final SubAlarm subAlarm = new SubAlarm(String.valueOf(subAlarmId++), alarmId, subExpressions.get(i));
            subAlarms.add(subAlarm);
        }
        alarm.setSubAlarms(subAlarms);
    }

    @BeforeMethod
    protected void beforeMethod() {
    	alarmEventForwarder = mock(AlarmEventForwarder.class);
    	alarmDAO = mock(AlarmDAO.class);
    	bolt = new MockAlarmThreshholdBolt(alarmDAO, alarmEventForwarder);
    	collector = mock(OutputCollector.class);
		final Map<String, String> config = new HashMap<>();
		config.put(ThresholdingConfiguration.ALERTS_EXCHANGE, ALERTS_EXCHANGE);
		config.put(ThresholdingConfiguration.ALERTS_ROUTING_KEY, ALERT_ROUTING_KEY);
		final TopologyContext context = mock(TopologyContext.class);
		bolt.prepare(config, context, collector);
    }

    /**
     * Create a simple Alarm with one sub expression.
     * Send a SubAlarm with state set to ALARM.
     * Ensure that the Alarm was triggered and sent
     */
    public void simpleAlarmCreation() {
		final SubAlarm subAlarm = subAlarms.get(0);
		subAlarm.setState(AlarmState.ALARM);
		when(alarmDAO.findById(alarm.getId())).thenReturn(alarm);
        MkTupleParam tupleParam = new MkTupleParam();
        tupleParam.setFields("alarmId", "subAlarm");
        tupleParam.setStream(Streams.DEFAULT_STREAM_ID);
        final Tuple tuple = Testing.testTuple(Arrays.asList(alarm.getId(), subAlarm), tupleParam);
		bolt.execute(tuple);
		bolt.execute(tuple);
		verify(collector, times(2)).ack(tuple);
		final String alarmJson = "{\"alarm-transitioned\":{\"tenantId\":\"AAAAABBBBBBCCCCC\"," +
				"\"alarmId\":\"111111112222222222233333333334\",\"alarmName\":\"Test CPU Alarm\"," +
				"\"alarmDescription\":\"Description of Alarm\",\"oldState\":\"OK\",\"newState\":\"ALARM\"," +
				"\"stateChangeReason\":\"Thresholds were exceeded for the sub-alarms: [avg(hpcs.compute.cpu{device=42, instance_id=123}, 1) > 5.0]\"," +
				"\"timestamp\":1395587091}}";

		verify(alarmEventForwarder, times(1)).send(ALERTS_EXCHANGE, ALERT_ROUTING_KEY, alarmJson);
		verify(alarmDAO, times(1)).updateState(alarm.getId(), AlarmState.ALARM);

		// Now clear the alarm and ensure another notification gets sent out
		subAlarm.setState(AlarmState.OK);
        final Tuple clearTuple = Testing.testTuple(Arrays.asList(alarm.getId(), subAlarm), tupleParam);
		bolt.execute(clearTuple);
		verify(collector, times(1)).ack(clearTuple);
		final String okJson = "{\"alarm-transitioned\":{\"tenantId\":\"AAAAABBBBBBCCCCC\",\"alarmId\":\"111111112222222222233333333334\",\"alarmName\":\"Test CPU Alarm\",\"alarmDescription\":\"Description of Alarm\",\"oldState\":\"ALARM\",\"newState\":\"OK\",\"stateChangeReason\":\"The alarm threshold(s) have not been exceeded\",\"timestamp\":1395587091}}";
		verify(alarmEventForwarder, times(1)).send(ALERTS_EXCHANGE, ALERT_ROUTING_KEY, okJson);
		verify(alarmDAO, times(1)).updateState(alarm.getId(), AlarmState.OK);
	}

    private class MockAlarmThreshholdBolt extends AlarmThresholdingBolt {

		private static final long serialVersionUID = 1L;

		public MockAlarmThreshholdBolt(AlarmDAO alarmDAO,
				AlarmEventForwarder alarmEventForwarder) {
			super(alarmDAO, alarmEventForwarder);
		}

		@Override
		protected long getTimestamp() {
			// Have to keep the time stamp constant so JSON comparison works
			return 1395587091;
		}
    }
}
