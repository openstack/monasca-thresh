package com.hpcloud.maas.domain.model;

/**
 * Statistic.
 * 
 * @author Jonathan Halterman
 */
public interface Statistic {
  /** Returns the value of the statistic. */
  double value();

  /** Adds the {@code value} to the statistic. */
  void addValue(double value);
}
