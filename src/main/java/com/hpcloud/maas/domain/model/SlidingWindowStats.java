package com.hpcloud.maas.domain.model;

import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A time based sliding window containing statistics for a fixed number of slots of a fixed length.
 * The sliding window is advanced by calling {@link #advanceWindowTo(long)}. Attempts to call
 * {@link #addValue(int, long)} for timestamps that are outside of the window, such as for old
 * values or future values, will be ignored.
 * 
 * @author Jonathan Halterman
 */
@NotThreadSafe
public class SlidingWindowStats {
  private static final Logger LOG = LoggerFactory.getLogger(SlidingWindowStats.class);

  private final int slotWidthInSeconds;
  private final int windowLengthInSeconds;
  private final Class<? extends Statistic> statType;
  private long headTimestamp;
  private int headIndex;
  private final Slot[] slots;

  private static class Slot {
    private final long timestamp;
    private final Statistic stat;

    private Slot(long timestamp, Statistic stat) {
      this.timestamp = timestamp;
      this.stat = stat;
    }

    @Override
    public String toString() {
      return timestamp + "=" + stat;
    }
  }

  /**
   * Creates a RollingWindow containing {@code numSlots} slots of size {@code slotWidthSeconds}
   * starting at the {@code initialPeriod}.
   * 
   * @param slotWidthSeconds the width of a slot in seconds
   * @param numSlots the number of slots in the window
   * @param statType to calculate values for
   * @param initialTimestamp to start window at
   */
  public SlidingWindowStats(int slotWidthSeconds, int numSlots,
      Class<? extends Statistic> statType, long initialTimestamp) {
    this.slotWidthInSeconds = slotWidthSeconds;
    this.windowLengthInSeconds = slotWidthSeconds * numSlots;
    this.statType = statType;
    headTimestamp = initialTimestamp;
    slots = new Slot[numSlots];
  }

  /** Returns a new slot for the {@code timestamp} and {@code statType}. */
  private static Slot createSlot(long timestamp, Class<? extends Statistic> statType) {
    try {
      return new Slot(timestamp, statType.newInstance());
    } catch (Exception e) {
      LOG.error("Failed to initialize slot", e);
      return null;
    }
  }

  /**
   * Adds the {@code value} to the statistics for the slot associated with the {@code timestamp},
   * else <b>does nothing</b> if the {@code timestamp} is outside of the window.
   */
  public void addValue(double value, long timestamp) {
    int index = slotIndexFor(timestamp);
    if (index != -1) {
      if (slots[index] == null)
        slots[index] = createSlot(timestamp, statType);
      slots[index].stat.addValue(value);
    }
  }

  /**
   * Advances the sliding window to the slot for the {@code timestamp}, erasing values for any slots
   * along the way.
   */
  public void advanceWindowTo(long timestamp) {
    if (timestamp <= headTimestamp)
      return;
    int slotsToAdvance = (int) (timestamp - headTimestamp) / slotWidthInSeconds;
    for (int i = slotsToAdvance; i > 0; i--)
      slots[headIndex = indexAfter(headIndex)] = null;
    headTimestamp = timestamp;
  }

  /**
   * Returns the timestamps represented by the current position of the sliding window decreasing from
   * newest to oldest.
   */
  public long[] getTimestamps() {
    long[] timestamps = new long[slots.length];
    long timestamp = headTimestamp;
    for (int i = 0; i < slots.length; i++, timestamp -= slotWidthInSeconds)
      timestamps[i] = timestamp;
    return timestamps;
  }

  /**
   * Returns the values of the sliding window decreasing in time from newest to oldest.
   */
  public double[] getValues() {
    double[] values = new double[slots.length];
    for (int i = 0, index = headIndex; i < slots.length; i++, index = indexBefore(index))
      if (slots[index] != null)
        values[i] = slots[index].stat.value();
    return values;
  }

  /**
   * Returns a logical view of the sliding window with decreasing timestamps from left to right.
   */
  @Override
  public String toString() {
    StringBuilder b = new StringBuilder();
    long timestamp = headTimestamp;
    for (int i = 0, index = headIndex; i < slots.length; i++, index = indexBefore(index), timestamp -= slotWidthInSeconds) {
      if (i != 0)
        b.append(", ");
      b.append(timestamp).append('=').append(slots[index] == null ? "na" : slots[index]);
    }
    return b.toString();
  }

  /**
   * Returns index of the slot associated with the {@code timestamp}, else -1 if the
   * {@code timestamp} is outside of the window. Slots decrease in time from right to left,
   * wrapping.
   */
  int slotIndexFor(long timestamp) {
    if (timestamp == headTimestamp)
      return headIndex;
    if (timestamp < headTimestamp) {
      int timeDiff = (int) (headTimestamp - timestamp);
      if (timeDiff < windowLengthInSeconds) {
        int slotDiff = timeDiff / slotWidthInSeconds;
        int offset = headIndex - slotDiff;
        if (offset < 0)
          offset += slots.length;
        return offset;
      }
    }

    return -1;
  }

  /** Returns the index for the slot in time after the {@index}. */
  private int indexAfter(int index) {
    return ++index == slots.length ? 0 : index;
  }

  /** Returns the index for the slot in time before the {@index}. */
  private int indexBefore(int index) {
    return --index == -1 ? slots.length - 1 : index;
  }
}
