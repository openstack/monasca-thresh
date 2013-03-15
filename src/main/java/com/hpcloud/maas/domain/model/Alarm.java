package com.hpcloud.maas.domain.model;

import java.util.Map;

import javax.annotation.Nullable;

import com.hpcloud.maas.common.model.AbstractEntity;

public class Alarm extends AbstractEntity {
  private String name;
  private String namespace;
  private String metricType;
  private String metricSubject;
  private Map<String, String> dimensions;
  private String operator;
  private long threshold;

  public Alarm() {
  }

  public Alarm(String id, String name, String namespace, String metricType,
      @Nullable String metricSubject, Map<String, String> dimensions, String operator,
      long threshold) {
    this.id = id;
    this.name = name;
    this.namespace = namespace;
    this.metricType = metricType;
    this.metricSubject = metricSubject;
    this.dimensions = dimensions;
    this.operator = operator;
    this.threshold = threshold;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (!super.equals(obj))
      return false;
    if (getClass() != obj.getClass())
      return false;
    Alarm other = (Alarm) obj;
    if (dimensions == null) {
      if (other.dimensions != null)
        return false;
    } else if (!dimensions.equals(other.dimensions))
      return false;
    if (metricSubject == null) {
      if (other.metricSubject != null)
        return false;
    } else if (!metricSubject.equals(other.metricSubject))
      return false;
    if (metricType == null) {
      if (other.metricType != null)
        return false;
    } else if (!metricType.equals(other.metricType))
      return false;
    if (name == null) {
      if (other.name != null)
        return false;
    } else if (!name.equals(other.name))
      return false;
    if (namespace == null) {
      if (other.namespace != null)
        return false;
    } else if (!namespace.equals(other.namespace))
      return false;
    if (operator == null) {
      if (other.operator != null)
        return false;
    } else if (!operator.equals(other.operator))
      return false;
    if (threshold != other.threshold)
      return false;
    return true;
  }

  public Map<String, String> getDimensions() {
    return dimensions;
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

  public String getOperator() {
    return operator;
  }

  public long getThreshold() {
    return threshold;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((dimensions == null) ? 0 : dimensions.hashCode());
    result = prime * result + ((metricSubject == null) ? 0 : metricSubject.hashCode());
    result = prime * result + ((metricType == null) ? 0 : metricType.hashCode());
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    result = prime * result + ((namespace == null) ? 0 : namespace.hashCode());
    result = prime * result + ((operator == null) ? 0 : operator.hashCode());
    result = prime * result + (int) (threshold ^ (threshold >>> 32));
    return result;
  }

  public void setDimensions(Map<String, String> dimensions) {
    this.dimensions = dimensions;
  }

  public void setId(String id) {
    this.id = id;
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

  public void setOperator(String operator) {
    this.operator = operator;
  }

  public void setThreshold(long threshold) {
    this.threshold = threshold;
  }
}
