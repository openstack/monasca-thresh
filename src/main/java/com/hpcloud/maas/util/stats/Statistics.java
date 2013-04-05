package com.hpcloud.maas.util.stats;

import com.hpcloud.maas.common.model.alarm.AggregateFunction;

/**
 * Statistic implementations.
 * 
 * @author Jonathan Halterman
 */
public final class Statistics {
  public static abstract class AbstractStatistic implements Statistic {
    protected boolean initialized;
    protected double value;

    @Override
    public boolean isInitialized() {
      return initialized;
    }

    @Override
    public void reset() {
      initialized = false;
      value = 0;
    }

    @Override
    public String toString() {
      return Double.valueOf(value()).toString();
    }

    @Override
    public double value() {
      return !initialized ? Double.NaN : value;
    }
  }

  public static class Average extends Sum {
    protected int count;

    @Override
    public void addValue(double value) {
      super.addValue(value);
      this.count++;
    }

    @Override
    public void reset() {
      super.reset();
      count = 0;
    }

    @Override
    public double value() {
      return !initialized ? Double.NaN : count == 0 ? 0 : value / count;
    }
  }

  public static class Count extends AbstractStatistic {
    @Override
    public void addValue(double value) {
      initialized = true;
      this.value++;
    }
  }

  public static class Max extends AbstractStatistic {
    @Override
    public void addValue(double value) {
      if (!initialized) {
        initialized = true;
        this.value = value;
      } else if (value > this.value)
        this.value = value;
    }
  }

  public static class Min extends AbstractStatistic {
    @Override
    public void addValue(double value) {
      if (!initialized) {
        initialized = true;
        this.value = value;
      } else if (value < this.value)
        this.value = value;
    }
  }

  public static class Sum extends AbstractStatistic {
    @Override
    public void addValue(double value) {
      initialized = true;
      this.value += value;
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
