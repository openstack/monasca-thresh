package com.hpcloud.maas.util.stats;

import static com.hpcloud.maas.Assert.assertArraysEqual;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import com.hpcloud.maas.util.time.Timescale;

/**
 * @author Jonathan Halterman
 */
@Test
public class SlidingWindowStatsTest {
  public void shouldAdvanceWindow() {
    SlidingWindowStats window = new SlidingWindowStats(Timescale.RELATIVE, 3, 3,
        Statistics.Average.class, 1);
    window.advanceWindowTo(2);
    window.advanceWindowTo(3);
    window.advanceWindowTo(5);
    assertEquals(window.getTimestamps(), new long[] { 5, 2, -1 });

    window.advanceWindowTo(9);
    assertArraysEqual(window.getTimestamps(), new long[] { 9, 6, 3 });

    window.advanceWindowTo(14);
    assertArraysEqual(window.getTimestamps(), new long[] { 14, 11, 8 });

    // Attempt to advance backwards - Noop
    window.advanceWindowTo(5);
    assertArraysEqual(window.getTimestamps(), new long[] { 14, 11, 8 });
  }

  public void testSlotIndexFor() {
    SlidingWindowStats window = new SlidingWindowStats(Timescale.RELATIVE, 3, 5,
        Statistics.Average.class, 15);

    // Slots look like 3 6 9 12 15
    assertEquals(window.slotIndexFor(0), -1);
    assertEquals(window.slotIndexFor(1), 0);
    assertEquals(window.slotIndexFor(3), 0);
    assertEquals(window.slotIndexFor(4), 1);
    assertEquals(window.slotIndexFor(6), 1);
    assertEquals(window.slotIndexFor(9), 2);
    assertEquals(window.slotIndexFor(10), 3);
    assertEquals(window.slotIndexFor(12), 3);
    assertEquals(window.slotIndexFor(13), 4);
    assertEquals(window.slotIndexFor(15), 4);
    assertEquals(window.slotIndexFor(17), -1);

    window.advanceWindowTo(19);

    // Slots like 18 21 9 12 15
    assertEquals(window.slotIndexFor(20), -1);
    assertEquals(window.slotIndexFor(19), 1);
    assertEquals(window.slotIndexFor(18), 0);
    assertEquals(window.slotIndexFor(16), 0);
    assertEquals(window.slotIndexFor(14), 4);
    assertEquals(window.slotIndexFor(11), 3);
    assertEquals(window.slotIndexFor(8), 2);
    assertEquals(window.slotIndexFor(5), -1);

    window.advanceWindowTo(22);

    // Window looks like 18 21 24 12 15
    assertEquals(window.slotIndexFor(23), -1);
    assertEquals(window.slotIndexFor(22), 2);
    assertEquals(window.slotIndexFor(20), 1);
    assertEquals(window.slotIndexFor(17), 0);
    assertEquals(window.slotIndexFor(14), 4);
    assertEquals(window.slotIndexFor(11), 3);
    assertEquals(window.slotIndexFor(10), 3);
    assertEquals(window.slotIndexFor(9), -1);

    window.advanceWindowTo(28);

    // Window looks like 18 21 24 27 30
    assertEquals(window.slotIndexFor(30), -1);
    assertEquals(window.slotIndexFor(26), 3);
    assertEquals(window.slotIndexFor(23), 2);
    assertEquals(window.slotIndexFor(22), 2);
    assertEquals(window.slotIndexFor(20), 1);
    assertEquals(window.slotIndexFor(17), 0);
    assertEquals(window.slotIndexFor(14), -1);
  }

  public void shouldGetValues() {
    SlidingWindowStats window = new SlidingWindowStats(Timescale.RELATIVE, 3, 3,
        Statistics.Sum.class, 10);
    window.addValue(1, 10);
    window.addValue(1, 8);
    window.addValue(2, 7);
    window.addValue(2, 6);
    window.addValue(3, 3);
    window.addValue(3, 2);

    // Window is 4, 7, 10
    assertArraysEqual(window.getValues(), new double[] { 2, 4, 6 });

    // Outside of the window - Noop
    window.addValue(3, 12);
    window.addValue(3, 0);

    assertArraysEqual(window.getValues(), new double[] { 2, 4, 6 });

    window.advanceWindowTo(11);

    // Window is 13 7 10
    assertArraysEqual(window.getValues(), new double[] { 0, 2, 4 });

    window.advanceWindowTo(13);

    // Window is 13 7 10
    assertArraysEqual(window.getValues(), new double[] { 0, 2, 4 });

    window.advanceWindowTo(17);

    // Window is 13, 16, 19
    assertArraysEqual(window.getValues(), new double[] { 0, 0, 0 });
  }

  public void shouldGetTimestamps() {
    SlidingWindowStats window = new SlidingWindowStats(Timescale.RELATIVE, 1, 5,
        Statistics.Sum.class, 10);

    assertArraysEqual(window.getTimestamps(), new long[] { 10, 9, 8, 7, 6 });
    window.advanceWindowTo(14);
    assertArraysEqual(window.getTimestamps(), new long[] { 14, 13, 12, 11, 10 });
  }

  public void shouldGetValuesAndAdvanceWindow() {
    SlidingWindowStats window = new SlidingWindowStats(Timescale.RELATIVE, 5, 3,
        Statistics.Sum.class, 15);
    // Window is 5 10 15
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

    // Window is 20 10 15
    window.addValue(4, 16);
    window.addValue(2, 17); // outside window

    assertArraysEqual(window.getValues(), new double[] { 4, 7, 4 });
  }

  public void testHasEmptySlots() {
    SlidingWindowStats window = new SlidingWindowStats(Timescale.RELATIVE, 5, 3,
        Statistics.Sum.class, 15);
    assertFalse(window.hasEmptySlots());

    window.advanceWindowTo(20);
    assertTrue(window.hasEmptySlots());

    window.addValue(2, 20);
    assertFalse(window.hasEmptySlots());
  }
}
