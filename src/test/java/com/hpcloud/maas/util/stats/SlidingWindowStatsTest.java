package com.hpcloud.maas.util.stats;

import static org.testng.Assert.*;

import org.testng.annotations.Test;

import com.hpcloud.maas.util.stats.SlidingWindowStats;
import com.hpcloud.maas.util.stats.Statistics;
import com.hpcloud.maas.util.time.Timescale;

/**
 * @author Jonathan Halterman
 */
@Test
public class SlidingWindowStatsTest {
  public void shouldAdvanceWindow() {
    SlidingWindowStats window = new SlidingWindowStats(Timescale.ABSOLUTE, 3, 3,
        Statistics.Average.class, 1);
    window.advanceWindowTo(2);
    window.advanceWindowTo(3);
    window.advanceWindowTo(5);
    assertEquals(window.getTimestamps(), new long[] { 5, 2, -1 });

    window.advanceWindowTo(9);
    assertEquals(window.getTimestamps(), new long[] { 9, 6, 3 });

    window.advanceWindowTo(14);
    assertEquals(window.getTimestamps(), new long[] { 14, 11, 8 });

    // Attempt to advance backwards - Noop
    window.advanceWindowTo(5);
    assertEquals(window.getTimestamps(), new long[] { 14, 11, 8 });
  }

  public void testSlotIndexFor() {
    SlidingWindowStats window = new SlidingWindowStats(Timescale.ABSOLUTE, 3, 5,
        Statistics.Average.class, 15);

    // Slots look like 3 6 9 12 15
    assertEquals(window.slotIndexFor(0), -1);
    assertEquals(window.slotIndexFor(1), 0);
    assertEquals(window.slotIndexFor(4), 1);
    assertEquals(window.slotIndexFor(5), 1);
    assertEquals(window.slotIndexFor(9), 2);
    assertEquals(window.slotIndexFor(10), 3);
    assertEquals(window.slotIndexFor(12), 3);
    assertEquals(window.slotIndexFor(13), 4);
    assertEquals(window.slotIndexFor(15), 4);
    assertEquals(window.slotIndexFor(17), -1);

    window.advanceWindowTo(19);

    // Slots like 18 21 9 12 15
    assertEquals(window.slotIndexFor(20), -1);
    assertEquals(window.slotIndexFor(19), 2);
    assertEquals(window.slotIndexFor(18), 1);
    assertEquals(window.slotIndexFor(16), 1);
    assertEquals(window.slotIndexFor(14), 0);
    assertEquals(window.slotIndexFor(11), 4);
    assertEquals(window.slotIndexFor(8), 3);

    window.advanceWindowTo(15);

    // Window looks like 15 11 12 13 14
    assertEquals(window.slotIndexFor(17), -1);
    assertEquals(window.slotIndexFor(15), 0);
    assertEquals(window.slotIndexFor(14), 4);
    assertEquals(window.slotIndexFor(11), 1);
    assertEquals(window.slotIndexFor(10), -1);

    window.advanceWindowTo(18);

    // Window looks like 15 16 17 18 14
    assertEquals(window.slotIndexFor(20), -1);
    assertEquals(window.slotIndexFor(18), 3);
    assertEquals(window.slotIndexFor(11), -1);
    assertEquals(window.slotIndexFor(14), 4);
    assertEquals(window.slotIndexFor(16), 1);
  }

  public void shouldGetValues() {
    SlidingWindowStats window = new SlidingWindowStats(Timescale.ABSOLUTE, 3, 3,
        Statistics.Sum.class, 10);
    window.addValue(1, 10);
    window.addValue(1, 8);
    window.addValue(2, 7);
    window.addValue(2, 6);
    window.addValue(3, 3);
    window.addValue(3, 2);

    assertEquals(window.getValues(), new double[] { 2, 4, 6 });

    // Outside of the window - Noop
    window.addValue(3, 12);
    window.addValue(3, 0);

    assertEquals(window.getValues(), new double[] { 2, 4, 6 });

    window.advanceWindowTo(11);
    assertEquals(window.getValues(), new double[] { 2, 4, 6 });

    window.advanceWindowTo(13);
    assertEquals(window.getValues(), new double[] { 0, 2, 4 });

    window.advanceWindowTo(12);
    assertEquals(window.getValues(), new double[] { 0, 0, 2 });

    window.advanceWindowTo(20);
    assertEquals(window.getValues(), new double[] { 0, 0, 0 });
  }

  public void shouldGetTimestamps() {
    SlidingWindowStats window = new SlidingWindowStats(Timescale.ABSOLUTE, 1, 5,
        Statistics.Sum.class, 10);

    assertEquals(window.getTimestamps(), new long[] { 10, 9, 8, 7, 6 });
    window.advanceWindowTo(14);
    assertEquals(window.getTimestamps(), new long[] { 14, 13, 12, 11, 10 });
  }

  public void shouldGetValuesAndAdvanceWindow() {
    SlidingWindowStats window = new SlidingWindowStats(Timescale.ABSOLUTE, 5, 3,
        Statistics.Sum.class, 15);
    window.addValue(2, 0); // outside window
    window.addValue(1, 1);
    window.addValue(3, 1);
    window.addValue(3, 3);
    window.addValue(3, 5);
    window.addValue(2, 6);
    window.addValue(1, 8);
    window.addValue(1, 10);
    window.addValue(4, 13);
    window.addValue(3, 14);
    window.addValue(3, 16); // outside window

    assertEquals(window.getValues(), new double[] { 7, 4, 10 });

    window.advanceWindowTo(16);
    window.addValue(4, 16);
    window.addValue(2, 17); // outside window

    assertEquals(window.getValues(), new double[] { 7, 4, 10 });
  }

  public void testHasEmptySlots() {
    SlidingWindowStats window = new SlidingWindowStats(Timescale.ABSOLUTE, 5, 3,
        Statistics.Sum.class, 15);
    assertFalse(window.hasEmptySlots());

    window.advanceWindowTo(20);
    assertTrue(window.hasEmptySlots());
    
    window.addValue(2, 20);
    assertFalse(window.hasEmptySlots());
  }
}
