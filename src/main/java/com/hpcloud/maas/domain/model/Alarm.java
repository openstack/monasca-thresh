package com.hpcloud.maas.domain.model;

import com.hpcloud.maas.common.event.AlarmCreatedEvent.NewAlarm;
import com.hpcloud.maas.common.model.alarm.AggregateFunction;
import com.hpcloud.maas.common.model.alarm.AlarmOperator;
import com.hpcloud.maas.common.model.alarm.AlarmState;
import com.hpcloud.maas.common.model.metric.MetricDefinition;
import com.hpcloud.maas.domain.common.AbstractEntity;

public class Alarm extends AbstractEntity {
  private String compositeId;
  private AggregateFunction function;
  private MetricDefinition metricDefinition;
  private AlarmOperator operator;
  private double threshold;
  private int periodSeconds;
  private int periods;
  private AlarmState state;

  public Alarm(String compositeId, NewAlarm alarm) {
    this.compositeId = compositeId;
    this.function = alarm.function;
    this.metricDefinition = alarm.metricDefinition;
    this.operator = alarm.operator;
    this.threshold = alarm.threshold;
    this.periodSeconds = alarm.periodSeconds;
    this.periods = alarm.periods;
    this.state = AlarmState.UNDETERMINED;
  }

  public Alarm(String compositeId, String id, AggregateFunction function,
      MetricDefinition metricDefinition, AlarmOperator operator, double threshold,
      int periodSeconds, int periods, AlarmState state) {
    this.compositeId = compositeId;
    this.id = id;
    this.function = function;
    this.metricDefinition = metricDefinition;
    this.operator = operator;
    this.threshold = threshold;
    this.periodSeconds = periodSeconds;
    this.periods = periods;
    this.state = state;
  }

  public String getCompositeId() {
    return compositeId;
  }

  public AggregateFunction getFunction() {
    return function;
  }

  public String getId() {
    return id;
  }

  public MetricDefinition getMetricDefinition() {
    return metricDefinition;
  }

  public AlarmOperator getOperator() {
    return operator;
  }

  public int getPeriods() {
    return periods;
  }

  public int getPeriodSeconds() {
    return periodSeconds;
  }

  public AlarmState getState() {
    return state;
  }

  public double getThreshold() {
    return threshold;
  }

  public void setCompositeId(String compositeId) {
    this.compositeId = compositeId;
  }

  public void setFunction(AggregateFunction function) {
    this.function = function;
  }

  public void setId(String id) {
    this.id = id;
  }

  public void setMetricDefinition(MetricDefinition metricDefinition) {
    this.metricDefinition = metricDefinition;
  }

  public void setOperator(AlarmOperator operator) {
    this.operator = operator;
  }

  public void setPeriods(int periods) {
    this.periods = periods;
  }

  public void setPeriodSeconds(int periodSeconds) {
    this.periodSeconds = periodSeconds;
  }

  public void setState(AlarmState state) {
    this.state = state;
  }

  public void setThreshold(double threshold) {
    this.threshold = threshold;
  }

  @Override
  public String toString() {
    return String.format(
        "Alarm [compositeId=%s, function=%s, metricDefinition=%s, operator=%s, threshold=%s, periodSeconds=%s, periods=%s, state=%s]",
        compositeId, function, metricDefinition, operator, threshold, periodSeconds, periods, state);
  }
}
