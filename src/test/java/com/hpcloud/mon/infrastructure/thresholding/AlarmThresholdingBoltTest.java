package com.hpcloud.mon.infrastructure.thresholding;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.testng.annotations.Test;

import com.hpcloud.mon.common.event.AlarmCreatedEvent;
import com.hpcloud.mon.common.model.alarm.AlarmExpression;
import com.hpcloud.mon.common.model.alarm.AlarmState;
import com.hpcloud.mon.common.model.alarm.AlarmSubExpression;
import com.hpcloud.mon.common.model.metric.Metric;
import com.hpcloud.mon.common.model.metric.MetricEnvelope;
import com.hpcloud.mon.common.model.metric.MetricEnvelopes;
import com.hpcloud.mon.common.model.metric.Metrics;
import com.hpcloud.mon.domain.model.Alarm;
import com.hpcloud.mon.domain.model.AlarmStateTransitionEvent;
import com.hpcloud.mon.domain.model.SubAlarm;
import com.hpcloud.util.Serialization;

/**
 * @author Jonathan Halterman
 */
@Test
public class AlarmThresholdingBoltTest {

    public static void main(String[] args) {
        AlarmStateTransitionEvent event = new AlarmStateTransitionEvent("I am a TenantId",
                "I am a Alarm Id", "I am a Alarm Name", AlarmState.OK, AlarmState.ALARM,
                "I am a Alarm Change Reason", System.currentTimeMillis() / 1000);
        final String s = Serialization.toJson(event);

        final String alarmId = "111111112222222222233333333334";
        final String tenantId = "AAAAABBBBBBCCCCC";
        final String expression = "avg(hpcs.compute.cpu{instance_id=123,device=42}, 1) > 5";
        final Alarm alarm = new Alarm();
        alarm.setName("Test CPU Alarm");
        alarm.setTenantId(tenantId);
        alarm.setId(alarmId);
        alarm.setExpression(expression);
        alarm.setState(AlarmState.OK);
        final AlarmExpression alarmExpression = new AlarmExpression(expression);
        final List<AlarmSubExpression> subExpressions = alarmExpression.getSubExpressions();
        final List<SubAlarm> subAlarms = new ArrayList<SubAlarm>(subExpressions.size());
        int subAlarmId = 4242;
        for (int i = 0; i < subExpressions.size(); i++) {
            final SubAlarm subAlarm = new SubAlarm(String.valueOf(subAlarmId++), alarmId, subExpressions.get(i));
            subAlarms.add(subAlarm);
        }
        alarm.setSubAlarms(subAlarms);

        final Map<String, String> dimensions = new HashMap<String, String>();
        dimensions.put("instance_id", "123");
        dimensions.put("device", "42");
        final Metric metric = new Metric("hpcs.compute.cpu", dimensions, System.currentTimeMillis(), 90.0);
        final MetricEnvelope metricEnvelope = new MetricEnvelope(metric);
        System.out.println(MetricEnvelopes.toJson(metricEnvelope));

        final AlarmCreatedEvent alarmCreatedEvent = new AlarmCreatedEvent();
        alarmCreatedEvent.tenantId = tenantId;
        alarmCreatedEvent.alarmId = alarmId;
        alarmCreatedEvent.alarmExpression = expression;
        final Map<String, AlarmSubExpression> subExpressionMap = new HashMap<String, AlarmSubExpression>();
        for (final SubAlarm subAlarm : subAlarms) {
            subExpressionMap.put(subAlarm.getId(), subAlarm.getExpression());
        }
        alarmCreatedEvent.alarmSubExpressions = subExpressionMap;
        
        System.out.println(Serialization.toJson(alarmCreatedEvent));
    }
}
