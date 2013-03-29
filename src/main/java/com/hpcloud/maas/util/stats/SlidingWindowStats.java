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
  private final int slotWidth;
  private final int windowLength;
  private final Slot[] slots;
  private long windowHeadTimestamp;
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
   * Creates a SlidingWindow containing {@code numSlots} slots of size {@code slotWidth} starting at
   * the {@code initialTimestamp}.
   * 
   * @param timescale to scale timestamps with
   * @param slotWidth time-based width of the slot
   * @param numSlots the number of slots in the window
   * @param statType to calculate values for
   * @param initialTimestamp to start window at
   */
  public SlidingWindowStats(Timescale timescale, int slotWidth, int numSlots,
      Class<? extends Statistic> statType, long initialTimestamp) {
    this.timescale = timescale;
    initialTimestamp = timescale.scale(initialTimestamp);
    this.slotWidth = slotWidth;
    this.windowLength = slotWidth * numSlots;
    windowHeadTimestamp = initialTimestamp;
    slotHeadTimestamp = initialTimestamp;
    slots = new Slot[numSlots];

    long timestamp = initialTimestamp;
    for (int i = numSlots - 1; i > -1; i--, timestamp -= slotWidth)
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
   * @param timestamp to add value for
   */
  public void addValue(double value, long timestamp) {
    timestamp = timescale.scale(timestamp);
    int index = slotIndexFor(timestamp);
    if (index == -1)
      LOG.warn("Timestamp {} is outside of window {}", timestamp, toString());
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
   * @param timestamp advance window to
   */
  public void advanceWindowTo(long timestamp) {
    timestamp = timescale.scale(timestamp);
    if (timestamp <= windowHeadTimestamp)
      return;
    long timeDiff = timestamp - slotHeadTimestamp;
    int slotsToAdvance = (int) timeDiff / slotWidth;
    slotsToAdvance += timeDiff % slotWidth == 0 ? 0 : 1;
    for (int i = slotsToAdvance; i > 0; i--) {
      Slot slot = slots[headIndex = indexAfter(headIndex)];
      slot.timestamp = slotHeadTimestamp += slotWidth;
      slot.stat.reset();
      emptySlots++;
    }

    windowHeadTimestamp = timestamp;
  }

  /** Returns the number of slots in the window. */
  public int getSlotCount() {
    return slots.length;
  }

  /** Returns the window's slot width. */
  public int getSlotWidth() {
    return slotWidth;
  }

  /**
   * Returns the timestamps represented by the current position of the sliding window decreasing
   * from newest to oldest.
   */
  public long[] getTimestamps() {
    long[] timestamps = new long[slots.length];
    long timestamp = windowHeadTimestamp;
    for (int i = 0; i < slots.length; i++, timestamp -= slotWidth)
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
   * @param timestamp to get value for
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
      int timeDiff = (int) (slotHeadTimestamp - timestamp);
      if (timeDiff < windowLength) {
        int slotDiff = timeDiff / slotWidth;
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
