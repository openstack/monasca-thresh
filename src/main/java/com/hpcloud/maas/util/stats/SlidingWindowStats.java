package com.hpcloud.maas.util.stats;

import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hpcloud.maas.util.time.Timescale;

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

  private final Timescale timescale;
  private final int slotWidthInMilliseconds;
  private final int slotWidthInMinutes;
  private final int windowLengthInMilliseconds;
  private final Slot[] slots;
  /** Timestamp for the head of the window in seconds. */
  private long windowHeadTimestamp;
  /** Timestamp for the head slot in seconds. */
  private long slotHeadTimestamp;
  private int headIndex;
  private int emptySlots;

  private static class Slot {
    private long timestamp;
    private Statistic stat;

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
   * @param timescale to scale timestamps with
   * @param slotWidthSeconds the width of a slot in seconds
   * @param numSlots the number of slots in the window
   * @param statType to calculate values for
   * @param initialTimestamp to start window at as milliseconds since epoch
   */
  public SlidingWindowStats(Timescale timescale, int slotWidthSeconds, int numSlots,
      Class<? extends Statistic> statType, long initialTimestamp) {
    this.timescale = timescale;
    initialTimestamp = timescale.scale(initialTimestamp);
    this.slotWidthInMilliseconds = slotWidthSeconds * 1000;
    this.slotWidthInMinutes = slotWidthSeconds / 60;
    this.windowLengthInMilliseconds = slotWidthSeconds * 1000 * numSlots;
    windowHeadTimestamp = initialTimestamp;
    slotHeadTimestamp = initialTimestamp;
    slots = new Slot[numSlots];

    long timestamp = initialTimestamp;
    for (int i = numSlots - 1; i > -1; i--, timestamp -= slotWidthInMilliseconds)
      slots[i] = createSlot(timestamp, statType);
    headIndex = numSlots - 1;
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
   * 
   * @param value to add
   * @param timestamp milliseconds since epoch
   */
  public void addValue(double value, long timestamp) {
    timestamp = timescale.scale(timestamp);
    int index = slotIndexFor(timestamp);
    if (index == -1)
      LOG.trace("Add value attempt for {} outside of window {}", timestamp, toString());
    else {
      if (!slots[index].stat.isInitialized())
        emptySlots--;
      LOG.trace("Adding value for {}. Current window{}", timestamp, toString());
      slots[index].stat.addValue(value);
    }
  }

  /**
   * Advances the sliding window to the slot for the {@code timestamp}, erasing values for any slots
   * along the way.
   * 
   * @param timestamp milliseconds since epoch
   */
  public void advanceWindowTo(long timestamp) {
    timestamp = timescale.scale(timestamp);
    if (timestamp <= windowHeadTimestamp)
      return;
    long timeDiff = timestamp - slotHeadTimestamp;
    int slotsToAdvance = (int) timeDiff / slotWidthInMilliseconds;
    slotsToAdvance += timeDiff % slotWidthInMilliseconds == 0 ? 0 : 1;
    for (int i = slotsToAdvance; i > 0; i--) {
      Slot slot = slots[headIndex = indexAfter(headIndex)];
      slot.timestamp = slotHeadTimestamp += slotWidthInMilliseconds;
      slot.stat.reset();
      emptySlots++;
    }

    windowHeadTimestamp = timestamp;
  }

  /** Returns the number of slots in the window. */
  public int getSlotCount() {
    return slots.length;
  }

  /** Returns the width of the window's slots in minutes. */
  public int getSlotWidthInMinutes() {
    return slotWidthInMinutes;
  }

  /**
   * Returns the timestamps represented by the current position of the sliding window decreasing
   * from newest to oldest.
   */
  public long[] getTimestamps() {
    long[] timestamps = new long[slots.length];
    long timestamp = windowHeadTimestamp;
    for (int i = 0; i < slots.length; i++, timestamp -= slotWidthInMilliseconds)
      timestamps[i] = timestamp;
    return timestamps;
  }

  public double getValue(int index) {
    int offset = headIndex - index;
    if (offset < 0)
      offset += slots.length;
    return slots[offset].stat.value();
  }

  /**
   * Returns the value for the window slot associated with {@code timestamp}.
   * 
   * @param timestamp milliseconds since epoch
   * @throws IllegalStateException if no value is within the window for the {@code timestamp}
   */
  public double getValue(long timestamp) {
    int index = slotIndexFor(timescale.scale(timestamp));
    if (index == -1)
      throw new IllegalStateException();
    return slots[index].stat.value();
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

  /** Returns true if the window has slots that have no values, else false. */
  public boolean hasEmptySlots() {
    return emptySlots > 0;
  }

  /**
   * Returns a logical view of the sliding window with decreasing timestamps from left to right.
   */
  @Override
  public String toString() {
    StringBuilder b = new StringBuilder();
    b.append("SlidingWindowStats [");
    for (int i = 0, index = headIndex; i < slots.length; i++, index = indexBefore(index)) {
      if (i != 0)
        b.append(", ");
      b.append(slots[index]);
    }

    return b.append(']').toString();
  }

  /**
   * Returns index of the slot associated with the {@code timestamp}, else -1 if the
   * {@code timestamp} is outside of the window. Slots decrease in time from right to left,
   * wrapping.
   */
  int slotIndexFor(long timestamp) {
    if (timestamp <= windowHeadTimestamp) {
      int timeDiff = (int) (windowHeadTimestamp - timestamp);
      if (timeDiff < windowLengthInMilliseconds) {
        int slotDiff = timeDiff / slotWidthInMilliseconds;
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
