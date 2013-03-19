package com.hpcloud.maas.domain.model;

import java.util.Map;

import javax.annotation.Nullable;

import com.hpcloud.maas.common.model.AbstractEntity;
import com.hpcloud.maas.common.model.AggregateFunction;
import com.hpcloud.maas.common.model.AlarmOperator;
import com.hpcloud.maas.common.model.AlarmState;

public class Alarm extends AbstractEntity {
  private String compositeId;
  private String id;
  private String name;
  private String namespace;
  private String metricType;
  private String metricSubject;
  private Map<String, String> dimensions;
  private int periodSeconds;
  private int periods;
  private AggregateFunction function;
  private AlarmOperator operator;
  private double threshold;
  private AlarmState state;

  public Alarm() {
  }

  public Alarm(String compositeId, String id, String name, String namespace, String metricType,
      @Nullable String metricSubject, Map<String, String> dimensions, int periodSeconds,
      int periods, AggregateFunction function, AlarmOperator operator, long threshold,
      AlarmState state) {
    this.compositeId = compositeId;
    this.id = id;
    this.name = name;
    this.namespace = namespace;
    this.metricType = metricType;
    this.metricSubject = metricSubject;
    this.dimensions = dimensions;
    this.periodSeconds = periodSeconds;
    this.periods = periods;
    this.function = function;
    this.operator = operator;
    this.threshold = threshold;
    this.state = state;
  }

  public Map<String, String> getDimensions() {
    return dimensions;
  }

  public AggregateFunction getFunction() {
    return function;
  }

  public String getMetricSubject() {
    return metricSubject;
  }

  public String getMetricType() {
    return metricType;
  }

  public String getName() {
    return name;
  }

  public String getNamespace() {
    return namespace;
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

  public boolean isThresholdExceededBy(double value) {
    return operator.evaluate(this.threshold, value);
  }

  public void setDimensions(Map<String, String> dimensions) {
    this.dimensions = dimensions;
  }

  public void setFunction(AggregateFunction function) {
    this.function = function;
  }

  public void setMetricSubject(String metricSubject) {
    this.metricSubject = metricSubject;
  }

  public void setMetricType(String metricType) {
    this.metricType = metricType;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setNamespace(String namespace) {
    this.namespace = namespace;
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

  public void setThreshold(long threshold) {
    this.threshold = threshold;
  }

  public String getCompositeId() {
    return compositeId;
  }

  public void setCompositeId(String compositeId) {
    this.compositeId = compositeId;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }
}
