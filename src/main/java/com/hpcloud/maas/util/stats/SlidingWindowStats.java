package com.hpcloud.maas.util.stats;

import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hpcloud.maas.util.time.Timescale;

/**
 * A time based sliding window containing statistics for a fixed number of slots of a fixed length.
 * The window provides a fixed size view over the total number of slots in the window.
 * 
 * @author Jonathan Halterman
 */
@NotThreadSafe
public class SlidingWindowStats {
  private static final Logger LOG = LoggerFactory.getLogger(SlidingWindowStats.class);

  private final Timescale timescale;
  private final int slotWidth;
  private final int numViewSlots;
  private final int windowLength;
  private final Slot[] slots;

  private long viewHeadTimestamp;
  private long slotHeadTimestamp;
  private long windowHeadTimestamp;
  private int viewHeadIndex;
  private int windowHeadIndex;
  private int emptySlotsInView;

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
   * Creates a time based SlidingWindowStats containing a fixed {@code numSlots} representing a view
   * up to the {@code initialViewTimestamp}, and an additional {@code numFutureSlots} for timestamps
   * beyond the current window view.
   * 
   * @param statType to calculate values for
   * @param timescale to scale timestamps with
   * @param slotWidth time-based width of the slot
   * @param numViewSlots the number of viewable slots
   * @param numFutureSlots the number of future slots to allow values for
   * @param initialViewTimestamp to start window view at
   */
  public SlidingWindowStats(Class<? extends Statistic> statType, Timescale timescale,
      int slotWidth, int numViewSlots, int numFutureSlots, long initialViewTimestamp) {
    this.timescale = timescale;
    this.slotWidth = slotWidth;
    this.numViewSlots = numViewSlots;
    this.windowLength = (numViewSlots + numFutureSlots) * slotWidth;

    initialViewTimestamp = timescale.scale(initialViewTimestamp);
    viewHeadTimestamp = initialViewTimestamp;
    slotHeadTimestamp = viewHeadTimestamp;
    windowHeadTimestamp = initialViewTimestamp + (numFutureSlots * slotWidth);
    viewHeadIndex = numViewSlots - 1;
    windowHeadIndex = numViewSlots + numFutureSlots - 1;
    emptySlotsInView = numViewSlots;

    slots = new Slot[numViewSlots + numFutureSlots];
    long timestamp = windowHeadTimestamp;
    for (int i = numViewSlots + numFutureSlots - 1; i > -1; i--, timestamp -= slotWidth)
      slots[i] = createSlot(timestamp, statType);
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
    int index = indexOfTime(timestamp);
    if (index == -1)
      LOG.warn("Timestamp {} is outside of window {}", timestamp, toString());
    else {
      if (!slots[index].stat.isInitialized() && isIndexInView(index))
        emptySlotsInView--;
      LOG.trace("Adding value for {}. Current window{}", timestamp, toString());
      slots[index].stat.addValue(value);
    }
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
   * Returns the timestamps represented by the current position of the sliding window increasing
   * from oldest to newest.
   */
  public long[] getTimestamps() {
    long[] timestamps = new long[numViewSlots];
    long timestamp = windowHeadTimestamp - ((slots.length - 1) * slotWidth);
    for (int i = 0; i < numViewSlots; i++, timestamp += slotWidth)
      timestamps[i] = timestamp;
    return timestamps;
  }

  /**
   * Returns the value for the window slot associated with {@code timestamp}.
   * 
   * @param timestamp to get value for
   * @throws IllegalStateException if no value is within the window for the {@code timestamp}
   */
  public double getValue(long timestamp) {
    int index = indexOfTime(timescale.scale(timestamp));
    if (index == -1)
      throw new IllegalStateException(timestamp + " is outside of the window");
    return slots[index].stat.value();
  }

  /**
   * Returns the values for the window up to and including the {@code timestamp}.
   * 
   * @param timestamp to get value for
   * @throws IllegalStateException if no value is within the window for the {@code timestamp}
   */
  public double[] getValuesUpTo(long timestamp) {
    int endIndex = indexOfTime(timescale.scale(timestamp));
    if (endIndex == -1)
      throw new IllegalStateException(timestamp + " is outside of the window");
    double[] values = new double[lengthToIndex(endIndex)];
    for (int i = 0, index = indexOf(0); i < values.length; i++, index = indexAfter(index))
      if (slots[index] != null)
        values[i] = slots[index].stat.value();
    return values;
  }

  /**
   * Returns the values of the sliding view increasing from oldest to newest.
   */
  public double[] getViewValues() {
    double[] values = new double[numViewSlots];
    for (int i = 0, index = indexOf(0); i < numViewSlots; i++, index = indexAfter(index))
      if (slots[index] != null)
        values[i] = slots[index].stat.value();
    return values;
  }

  /**
   * Returns the values of the sliding window increasing from oldest to newest.
   */
  public double[] getWindowValues() {
    double[] values = new double[slots.length];
    for (int i = 0, index = indexOf(0); i < slots.length; i++, index = indexAfter(index))
      if (slots[index] != null)
        values[i] = slots[index].stat.value();
    return values;
  }

  /** Returns true if the window has slots in the view which have no values, else false. */
  public boolean hasEmptySlotsInView() {
    return emptySlotsInView > 0;
  }

  /**
   * Slides window's view to the slot for the {@code timestamp}, erasing values for any slots along
   * the way.
   * 
   * @param timestamp slide view to
   */
  public void slideViewTo(long timestamp) {
    timestamp = timescale.scale(timestamp);
    if (timestamp <= viewHeadTimestamp)
      return;
    long timeDiff = timestamp - slotHeadTimestamp;
    int slotsToAdvance = (int) timeDiff / slotWidth;
    slotsToAdvance += timeDiff % slotWidth == 0 ? 0 : 1;

    for (int i = 0; i < slotsToAdvance; i++) {
      // Adjust emptySlotsInView for the old slot moving out of the window
      if (!slots[indexOf(0)].stat.isInitialized())
        emptySlotsInView--;

      slotHeadTimestamp += slotWidth;
      windowHeadTimestamp += slotWidth;
      viewHeadIndex = indexAfter(viewHeadIndex);
      windowHeadIndex = indexAfter(windowHeadIndex);
      Slot slot = slots[windowHeadIndex];
      slot.timestamp = windowHeadTimestamp;
      slot.stat.reset();

      // Adjust emptySlotsInView for the new slot that moved into the window
      if (!slots[viewHeadIndex].stat.isInitialized())
        emptySlotsInView++;
    }

    viewHeadTimestamp = timestamp;
  }

  /**
   * Returns a logical view of the sliding window with increasing timestamps from left to right.
   */
  @Override
  public String toString() {
    StringBuilder b = new StringBuilder();
    b.append("SlidingWindowStats [[");
    for (int i = 0, index = indexOf(0); i < slots.length; i++, index = indexAfter(index)) {
      if (i == numViewSlots)
        b.append("], ");
      else if (i != 0)
        b.append(", ");
      b.append(slots[index]);
    }

    return b.append(']').toString();
  }

  /** Returns the physical index of the logical {@code slotIndex}. */
  int indexOf(int slotIndex) {
    int offset = windowHeadIndex + 1 + slotIndex;
    if (offset >= slots.length)
      offset -= slots.length;
    return offset;
  }

  /**
   * Returns physical index of the slot associated with the {@code timestamp}, else -1 if the
   * {@code timestamp} is outside of the window. Slots increase in time from left to right,
   * wrapping.
   */
  int indexOfTime(long timestamp) {
    if (timestamp <= windowHeadTimestamp) {
      int timeDiff = (int) (windowHeadTimestamp - timestamp);
      if (timeDiff < windowLength) {
        int slotDiff = timeDiff / slotWidth;
        int offset = windowHeadIndex - slotDiff;
        if (offset < 0)
          offset += slots.length;
        return offset;
      }
    }

    return -1;
  }

  /** Returns true if the physical {@code slotIndex} is inside the view, else false. */
  boolean isIndexInView(int slotIndex) {
    return lengthToIndex(slotIndex) <= numViewSlots;
  }

  /** Returns the length of the window up to and including the physical {@code slotIndex}. */
  int lengthToIndex(int slotIndex) {
    if (windowHeadIndex >= slotIndex)
      return slotIndex + slots.length - windowHeadIndex;
    else
      return slotIndex - windowHeadIndex;
  }

  /** Returns the physical index for the slot logically positioned after the {@code index}. */
  private int indexAfter(int index) {
    return ++index == slots.length ? 0 : index;
  }
}
