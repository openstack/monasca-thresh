package com.hpcloud.maas.util.time;

/**
 * Timescale.
 * 
 * @author Jonathan Halterman
 */
public interface Timescale {
  long scale(long timestamp);

  public static final Timescale RELATIVE = new Timescale() {
    @Override
    public long scale(long timestamp) {
      return timestamp;
    }
  };

  public static final Timescale MILLISECONDS = new Timescale() {
    @Override
    public long scale(long timestamp) {
      return Times.roundDownToNearestSecond(timestamp);
    }
  };
}