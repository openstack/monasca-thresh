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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import monasca.common.model.event.AlarmDefinitionUpdatedEvent;
import monasca.common.model.event.AlarmUpdatedEvent;
import monasca.common.model.alarm.AggregateFunction;
import monasca.common.model.alarm.AlarmExpression;
import monasca.common.model.alarm.AlarmState;
import monasca.common.model.alarm.AlarmSubExpression;
import monasca.common.streaming.storm.Streams;
import monasca.common.util.Serialization;
import monasca.thresh.domain.model.Alarm;
import monasca.thresh.domain.model.AlarmDefinition;
import monasca.thresh.domain.model.SubAlarm;
import monasca.thresh.domain.model.SubExpression;
import monasca.thresh.domain.service.AlarmDAO;
import monasca.thresh.domain.service.AlarmDefinitionDAO;

import backtype.storm.Testing;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.MkTupleParam;
import backtype.storm.tuple.Tuple;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Test
public class AlarmThresholdingBoltTest {

  private static final String tenantId = "AAAAABBBBBBCCCCC";

  private AlarmExpression alarmExpression;
  private AlarmDefinition alarmDefinition;
  private Alarm alarm;
  private List<SubAlarm> subAlarms;
  private AlarmEventForwarder alarmEventForwarder;
  private AlarmDAO alarmDAO;
  private AlarmDefinitionDAO alarmDefinitionDAO;
  private AlarmThresholdingBolt bolt;
  private OutputCollector collector;
  private final String[] subExpressions = {"avg(cpu{instance_id=123,device=42}, 1) > 5",
      "max(load{instance_id=123,device=42}, 1) > 8",
      "sum(diskio{instance_id=123,device=42}, 1) > 5000"};

  @BeforeMethod
  protected void beforeMethod() {
    final StringBuilder builder = new StringBuilder();
    for (final String subExpression : subExpressions) {
      if (builder.length() > 0) {
        builder.append(" or ");
      }
      builder.append(subExpression);
    }
    final String expression = builder.toString();
    alarmExpression = new AlarmExpression(expression);
    alarmDefinition =
        new AlarmDefinition(tenantId, "Test CPU Alarm", "Description of Alarm",
            alarmExpression, "LOW", true, new ArrayList<String>());
    alarm = new Alarm(alarmDefinition, AlarmState.OK);
    subAlarms = new ArrayList<SubAlarm>(alarm.getSubAlarms());

    alarmEventForwarder = mock(AlarmEventForwarder.class);
    alarmDAO = mock(AlarmDAO.class);
    alarmDefinitionDAO = mock(AlarmDefinitionDAO.class);
    bolt = new MockAlarmThreshholdBolt(alarmDAO, alarmDefinitionDAO, alarmEventForwarder);
    collector = mock(OutputCollector.class);
    final Map<String, String> config = new HashMap<>();
    final TopologyContext context = mock(TopologyContext.class);
    bolt.prepare(config, context, collector);
  }

  /**
   * Create a simple Alarm with one sub expression. Send a SubAlarm with state set to ALARM. Ensure
   * that the Alarm was triggered and sent
   */
  public void simpleAlarmTrigger() {
    final SubAlarm subAlarm = subAlarms.get(0);
    final String alarmId = alarm.getId();
    when(alarmDAO.findById(alarmId)).thenReturn(alarm);
    when(alarmDefinitionDAO.findById(alarmDefinition.getId())).thenReturn(alarmDefinition);
    emitSubAlarmStateChange(alarmId, subAlarm, AlarmState.ALARM);
    for (int i = 1; i < subAlarms.size(); i++) {
      emitSubAlarmStateChange(alarmId, subAlarms.get(i), AlarmState.OK);
    }
    final String alarmJson =
        "{\"alarm-transitioned\":{\"tenantId\":\""
            + tenantId
            + "\","
            + "\"alarmId\":\"" + alarmId + "\","
            + "\"alarmDefinitionId\":\"" + alarmDefinition.getId() + "\",\"metrics\":[],"
            + "\"alarmName\":\"Test CPU Alarm\","
            + "\"alarmDescription\":\"Description of Alarm\",\"oldState\":\"OK\",\"newState\":\"ALARM\","
            + "\"actionsEnabled\":true,"
            + "\"stateChangeReason\":\"Thresholds were exceeded for the sub-alarms: "
            + subAlarm.getExpression().getExpression() + " with the values: []\"," + "\"severity\":\"LOW\","
            + "\"subAlarms\":[" + buildSubAlarmJson(alarm.getSubAlarms()) + "],"
            + "\"timestamp\":1395587091003}}";

    verify(alarmEventForwarder, times(1)).send(alarmJson);
    verify(alarmDAO, times(1)).updateState(alarmId, AlarmState.ALARM);

    // Now clear the alarm and ensure another notification gets sent out
    subAlarm.setState(AlarmState.OK);
    final Tuple clearTuple = createSubAlarmStateChangeTuple(alarmId, subAlarm);
    bolt.execute(clearTuple);
    verify(collector, times(1)).ack(clearTuple);
    final String okJson =
        "{\"alarm-transitioned\":{\"tenantId\":\""
            + tenantId
            + "\","
            + "\"alarmId\":\"" + alarmId + "\","
            + "\"alarmDefinitionId\":\"" + alarmDefinition.getId() + "\",\"metrics\":[],"
            + "\"alarmName\":\"Test CPU Alarm\","
            + "\"alarmDescription\":\"Description of Alarm\",\"oldState\":\"ALARM\",\"newState\":\"OK\","
            + "\"actionsEnabled\":true,"
            + "\"stateChangeReason\":\"The alarm threshold(s) have not been exceeded for the sub-alarms: "
            + subAlarm.getExpression().getExpression() + " with the values: [], "
            + subAlarms.get(1).getExpression().getExpression() + " with the values: [], "
            + subAlarms.get(2).getExpression().getExpression() + " with the values: []"
            + "\",\"severity\":\"LOW\","
            + "\"subAlarms\":[" + buildSubAlarmJson(alarm.getSubAlarms()) + "],"
            + "\"timestamp\":1395587091003}}";
    verify(alarmEventForwarder, times(1)).send(okJson);
    verify(alarmDAO, times(1)).updateState(alarmId, AlarmState.OK);
  }

  public void simpleAlarmUpdate() {
    // Now send an AlarmUpdatedEvent
    final AlarmState newState = AlarmState.OK;
    final AlarmUpdatedEvent event =
        EventProcessingBoltTest.createAlarmUpdatedEvent(alarmDefinition, alarm, newState);
    final Tuple updateTuple = createAlarmUpdateTuple(event);
    bolt.execute(updateTuple);
    verify(collector, times(1)).ack(updateTuple);
    assertEquals(alarm.getState(), newState);
  }

  public void simpleAlarmDefinitionUpdate() {
    String alarmDefId = alarmDefinition.getId();

    // Ensure the Alarm Definition gets loaded
    final SubAlarm subAlarm = subAlarms.get(0);
    final String alarmId = alarm.getId();
    when(alarmDAO.findById(alarmId)).thenReturn(alarm);
    when(alarmDefinitionDAO.findById(alarmDefinition.getId())).thenReturn(alarmDefinition);
    emitSubAlarmStateChange(alarmId, subAlarm, AlarmState.ALARM);

    // Now send an AlarmDefinitionUpdatedEvent
    final Map<String, AlarmSubExpression> empty = new HashMap<>();
    final String newName = "New Name";
    final String newDescription = "New Description";
    boolean newEnabled = false;
    final String newSeverity  = "HIGH";
    assertNotEquals(newSeverity, alarmDefinition.getSeverity());
    final AlarmDefinitionUpdatedEvent event =
        new AlarmDefinitionUpdatedEvent(tenantId, alarmDefId, newName, newDescription,
            alarmDefinition.getAlarmExpression().getExpression(), alarmDefinition.getMatchBy(),
            newEnabled, newSeverity, empty, empty, empty, empty);
    final Tuple updateTuple = createAlarmDefinitionUpdateTuple(event);
    bolt.execute(updateTuple);
    verify(collector, times(1)).ack(updateTuple);
    assertEquals(alarmDefinition.getName(), newName);
    assertEquals(alarmDefinition.getDescription(), newDescription);
    assertEquals(alarmDefinition.getSeverity(), newSeverity);
    assertEquals(alarmDefinition.isActionsEnabled(), newEnabled);
  }

  public void complexAlarmUpdate() {
    String alarmId = setUpInitialAlarm();
    when(alarmDAO.findById(alarmId)).thenReturn(alarm);
    when(alarmDefinitionDAO.findById(alarmDefinition.getId())).thenReturn(alarmDefinition);

    // Make sure the Alarm gets loaded
    for (final SubAlarm subAlarm : alarm.getSubAlarms()) {
      emitSubAlarmStateChange(alarmId, subAlarm, AlarmState.OK);
    }

    assertNotNull(bolt.alarms.get(alarmId));
    // Now send an AlarmUpdatedEvent
    final Map<String, AlarmSubExpression> newSubExpressions = new HashMap<>();
    final Map<String, AlarmSubExpression> oldSubExpressions = new HashMap<>();
    final Map<String, AlarmSubExpression> changedSubExpressions = new HashMap<>();
    final Map<String, AlarmSubExpression> unchangedSubExpressions = new HashMap<>();
    final String newExpression =
        alarmExpression.getExpression().replaceAll(" or ", " and ").replace("max", "avg");

    final SubExpression changedSubExpression = alarmDefinition.getSubExpressions().get(2);
    changedSubExpression.getAlarmSubExpression().setFunction(AggregateFunction.AVG);
    changedSubExpressions.put(changedSubExpression.getId(), changedSubExpression.getAlarmSubExpression());
    for (int i = 0; i < 2; i++) {
      final SubExpression unChangedSubExpr = alarmDefinition.getSubExpressions().get(i);
      unchangedSubExpressions.put(unChangedSubExpr.getId(), unChangedSubExpr.getAlarmSubExpression());
    }

    final AlarmDefinitionUpdatedEvent event =
        new AlarmDefinitionUpdatedEvent(tenantId, alarmDefinition.getId(), alarmDefinition.getName(), alarmDefinition.getDescription(),
            newExpression, alarmDefinition.getMatchBy(), alarmDefinition.isActionsEnabled(), alarmDefinition.getSeverity(),
            oldSubExpressions, changedSubExpressions, unchangedSubExpressions, newSubExpressions);
    final Tuple updateTuple = createAlarmDefinitionUpdateTuple(event);
    bolt.execute(updateTuple);
    verify(collector, times(1)).ack(updateTuple);
    assertEquals(alarmDefinition.getAlarmExpression().getExpression(), newExpression);

    final Tuple updateSubExprTuple = createSubExpressionUpdated(changedSubExpression, alarmDefinition.getId());
    bolt.execute(updateSubExprTuple);
    verify(collector, times(1)).ack(updateSubExprTuple);

    final Alarm changedAlarm = bolt.alarms.get(alarmId);
    for (final SubAlarm subAlarm : changedAlarm.getSubAlarms()) {
      final AlarmSubExpression check;
      if (subAlarm.getAlarmSubExpressionId().equals(changedSubExpression.getId())) {
        check = changedSubExpression.getAlarmSubExpression();
      } else {
        check = unchangedSubExpressions.get(subAlarm.getAlarmSubExpressionId());
      }
      assertEquals(subAlarm.getExpression(), check);
    }
  }

  private Tuple createSubExpressionUpdated(final SubExpression newExpr,
                                           final String alarmDefinitionId) {
    final MkTupleParam tupleParam = new MkTupleParam();
    tupleParam.setFields(EventProcessingBolt.METRIC_SUB_ALARM_EVENT_STREAM_FIELDS);
    tupleParam.setStream(EventProcessingBolt.METRIC_SUB_ALARM_EVENT_STREAM_ID);
    return Testing.testTuple(
        Arrays.asList(EventProcessingBolt.UPDATED, newExpr, alarmDefinitionId), tupleParam);
  }

  private String setUpInitialAlarm() {
    final String alarmId = alarm.getId();
    when(alarmDAO.findById(alarmId)).thenReturn(alarm);
    // Load up the original Alarm
    emitSubAlarmStateChange(alarmId, subAlarms.get(0), AlarmState.ALARM);
    return alarmId;
  }

  private String buildSubAlarmJson(Collection<SubAlarm> subAlarms){
    StringBuilder stringBuilder = new StringBuilder();
    for(SubAlarm subAlarm: subAlarms){
      if (stringBuilder.length() != 0) {
        stringBuilder.append(",");
      }
      stringBuilder.append(Serialization.toJson(subAlarm.getExpression())).setCharAt(stringBuilder.length()-1, ',');
      stringBuilder.append("\"subAlarmState\":\"").append(subAlarm.getState()).append("\",");
      stringBuilder.append("\"currentValues\":").append(subAlarm.getCurrentValues()).append("}");
    }
  return stringBuilder.toString().replace("AlarmSubExpression","subAlarmExpression");
  }

  private void emitSubAlarmStateChange(String alarmId, final SubAlarm subAlarm, AlarmState state) {
    // Create a copy so changing the state doesn't directly update the ones in the bolt
    final SubAlarm toEmit =
        new SubAlarm(subAlarm.getId(), subAlarm.getAlarmId(), new SubExpression(
            subAlarm.getAlarmSubExpressionId(), subAlarm.getExpression()));
    toEmit.setState(state);
    final Tuple tuple = createSubAlarmStateChangeTuple(alarmId, toEmit);
    bolt.execute(tuple);
    verify(collector, times(1)).ack(tuple);
  }

  private Tuple createAlarmUpdateTuple(AlarmUpdatedEvent event) {
    final MkTupleParam tupleParam = new MkTupleParam();
    tupleParam.setFields(EventProcessingBolt.ALARM_EVENT_STREAM_FIELDS);
    tupleParam.setStream(EventProcessingBolt.ALARM_EVENT_STREAM_ID);
    final Tuple tuple =
        Testing.testTuple(Arrays.asList(EventProcessingBolt.UPDATED, event.alarmId, event),
            tupleParam);
    return tuple;
  }

  private Tuple createAlarmDefinitionUpdateTuple(AlarmDefinitionUpdatedEvent event) {
    final MkTupleParam tupleParam = new MkTupleParam();
    tupleParam.setFields(EventProcessingBolt.ALARM_DEFINITION_EVENT_FIELDS);
    tupleParam.setStream(EventProcessingBolt.ALARM_DEFINITION_EVENT_STREAM_ID);
    final Tuple tuple =
        Testing.testTuple(Arrays.asList(EventProcessingBolt.UPDATED, event), tupleParam);
    return tuple;
  }

  private Tuple createSubAlarmStateChangeTuple(String alarmId, final SubAlarm subAlarm) {
    final MkTupleParam tupleParam = new MkTupleParam();
    tupleParam.setFields("alarmId", "subAlarm");
    tupleParam.setStream(Streams.DEFAULT_STREAM_ID);
    final Tuple tuple = Testing.testTuple(Arrays.asList(alarmId, subAlarm), tupleParam);
    return tuple;
  }

  private class MockAlarmThreshholdBolt extends AlarmThresholdingBolt {

    private static final long serialVersionUID = 1L;

    public MockAlarmThreshholdBolt(AlarmDAO alarmDAO, AlarmDefinitionDAO alarmDefinitionDAO, AlarmEventForwarder alarmEventForwarder) {
      super(alarmDAO, alarmDefinitionDAO, alarmEventForwarder);
    }

    @Override
    protected long getTimestamp() {
      // Have to keep the time stamp constant so JSON comparison works
      return 1395587091003l;
    }
  }
}
