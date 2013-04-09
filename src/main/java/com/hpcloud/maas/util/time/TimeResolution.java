package com.hpcloud.maas.util.time;

/**
 * Time resolution.
 * 
 * @author Jonathan Halterman
 */
public enum TimeResolution {
  ABSOLUTE {
    @Override
    public long adjust(long timestamp) {
      return timestamp;
    }
  },
  MILLISECONDS {
    @Override
    public long adjust(long timestamp) {
      return Times.roundDownToNearestSecond(timestamp);
    }
  };

  /**
   * Returns the {@code timestamp} adjusted for the resolution.
   */
  public abstract long adjust(long timestamp);
}