package com.hpcloud.maas.util.time;

/**
 * Scales time values.
 * 
 * @author Jonathan Halterman
 */
public interface Timescale {
  long scale(long timestamp);

  public static final Timescale ABSOLUTE = new Timescale() {
    @Override
    public long scale(long timestamp) {
      return timestamp;
    }
  };

  public static final Timescale SECONDS_SINCE_EPOCH = new Timescale() {
    @Override
    public long scale(long timestamp) {
      return Times.roundDownToNearestSecond(timestamp);
    }
  };
}