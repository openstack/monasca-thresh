package com.hpcloud.maas.domain.model;

import com.hpcloud.maas.common.model.AggregateFunction;

/**
 * Statistic implementations.
 * 
 * @author Jonathan Halterman
 */
public final class Statistics {
  public static abstract class AbstractStatistic implements Statistic {
    @Override
    public String toString() {
      return Double.valueOf(value()).toString();
    }
  }

  public static class Average extends Sum {
    protected double count;

    @Override
    public void addValue(double value) {
      super.addValue(value);
      this.count++;
    }

    @Override
    public double value() {
      return sum / count;
    }
  }

  public static class Count extends AbstractStatistic {
    protected double count;

    @Override
    public void addValue(double value) {
      this.count++;
    }

    @Override
    public double value() {
      return count;
    }
  }

  public static class Max extends AbstractStatistic {
    protected double max;
    boolean initialized;

    @Override
    public void addValue(double value) {
      if (!initialized) {
        max = value;
        initialized = true;
      }

      if (value > max)
        max = value;
    }

    @Override
    public double value() {
      return max;
    }
  }

  public static class Min extends AbstractStatistic {
    protected double min;
    boolean initialized;

    @Override
    public void addValue(double value) {
      if (!initialized) {
        min = value;
        initialized = true;
      }

      if (value < min)
        min = value;
    }

    @Override
    public double value() {
      return min;
    }
  }

  public static class Sum extends AbstractStatistic {
    protected double sum;

    @Override
    public void addValue(double value) {
      this.sum += value;
    }

    @Override
    public double value() {
      return sum;
    }
  }

  private Statistics() {
  }

  public static Class<? extends Statistic> statTypeFor(AggregateFunction aggregateFunction) {
    if (AggregateFunction.AVERAGE.equals(aggregateFunction))
      return Average.class;
    if (AggregateFunction.COUNT.equals(aggregateFunction))
      return Count.class;
    if (AggregateFunction.SUM.equals(aggregateFunction))
      return Sum.class;
    if (AggregateFunction.MIN.equals(aggregateFunction))
      return Min.class;
    if (AggregateFunction.MAX.equals(aggregateFunction))
      return Max.class;
    return null;
  }
}
