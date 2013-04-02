package com.hpcloud.maas.util.stats;

import static com.hpcloud.maas.Assert.*;
import static org.testng.Assert.*;
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
    SlidingWindowStats window = new SlidingWindowStats(Statistics.Average.class,
        Timescale.RELATIVE, 3, 3, 1);
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
    SlidingWindowStats window = new SlidingWindowStats(Statistics.Average.class,
        Timescale.RELATIVE, 3, 5, 15);

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
    SlidingWindowStats window = new SlidingWindowStats(Statistics.Sum.class, Timescale.RELATIVE, 3,
        3, 10);
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
    SlidingWindowStats window = new SlidingWindowStats(Statistics.Sum.class, Timescale.RELATIVE, 1,
        5, 10);

    assertArraysEqual(window.getTimestamps(), new long[] { 10, 9, 8, 7, 6 });
    window.advanceWindowTo(14);
    assertArraysEqual(window.getTimestamps(), new long[] { 14, 13, 12, 11, 10 });
  }

  public void shouldGetValuesAndAdvanceWindow() {
    SlidingWindowStats window = new SlidingWindowStats(Statistics.Sum.class, Timescale.RELATIVE, 5,
        3, 15);
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

  public void shouldGetValuesUpTo() {
    SlidingWindowStats window = new SlidingWindowStats(Statistics.Sum.class, Timescale.RELATIVE, 5,
        3, 15);
    // Window is 5 10 15
    window.addValue(4, 5);
    window.addValue(3, 10);
    window.addValue(2, 15);

    assertEquals(window.getValuesUpTo(13), new double[] { 2, 3, 4 });
    assertEquals(window.getValuesUpTo(7), new double[] { 3, 4 });
    assertEquals(window.getValuesUpTo(4), new double[] { 4 });

    // WIndow 20 10 15
    window.advanceWindowTo(17);
    window.addValue(5, 17);
    try {
      assertEquals(window.getValuesUpTo(18), new double[] { 2, 3, 4 });
      fail();
    } catch (Exception expected) {
    }
    assertEquals(window.getValuesUpTo(17), new double[] { 5, 2, 3 });
    assertEquals(window.getValuesUpTo(12), new double[] { 2, 3 });
    assertEquals(window.getValuesUpTo(7), new double[] { 3 });

    // WIndow 20 25 15
    window.advanceWindowTo(22);
    window.addValue(6, 21);
    try {
      assertEquals(window.getValuesUpTo(24), new double[] { 2, 3, 4 });
      fail();
    } catch (Exception expected) {
    }
    assertEquals(window.getValuesUpTo(22), new double[] { 6, 5, 2 });
    assertEquals(window.getValuesUpTo(20), new double[] { 5, 2 });
    assertEquals(window.getValuesUpTo(14), new double[] { 2 });
  }

  public void shouldGetValueForSlotIndex() {
    SlidingWindowStats window = new SlidingWindowStats(Statistics.Sum.class, Timescale.RELATIVE, 5,
        3, 15);
    // Logical window is 15 10 5
    window.addValue(4, 5);
    window.addValue(3, 10);
    window.addValue(2, 15);

    assertEquals(window.getValue(0), 2.0);
    assertEquals(window.getValue(1), 3.0);
    assertEquals(window.getValue(2), 4.0);

    // Advance logical window to 20 15 10
    window.advanceWindowTo(16);
    window.addValue(5, 16);

    assertEquals(window.getValue(0), 5.0);
    assertEquals(window.getValue(1), 2.0);
    assertEquals(window.getValue(2), 3.0);
  }

  public void shouldGetValueForTimestamp() {
    SlidingWindowStats window = new SlidingWindowStats(Statistics.Sum.class, Timescale.RELATIVE, 5,
        3, 15);
    // Logical window is 15 10 5
    window.addValue(4, 5);
    window.addValue(3, 10);
    window.addValue(2, 15);

    assertEquals(window.getValue(15l), 2.0);
    assertEquals(window.getValue(10l), 3.0);
    assertEquals(window.getValue(5l), 4.0);

    // Advance logical window to 20 15 10
    window.advanceWindowTo(16);
    window.addValue(5, 16);

    assertEquals(window.getValue(16l), 5.0);
    assertEquals(window.getValue(15l), 2.0);
    assertEquals(window.getValue(10l), 3.0);
  }

  public void testHasEmptySlots() {
    SlidingWindowStats window = new SlidingWindowStats(Statistics.Sum.class, Timescale.RELATIVE, 3,
        2, 15);
    window.addValue(123, 14);
    window.addValue(123, 12);
    assertFalse(window.hasEmptySlots());

    window.advanceWindowTo(20);
    assertTrue(window.hasEmptySlots());

    window.addValue(2, 20);
    window.addValue(2, 18);
    assertFalse(window.hasEmptySlots());
  }

  public void testLengthToIndex() {
    SlidingWindowStats window = new SlidingWindowStats(Statistics.Sum.class, Timescale.RELATIVE, 1,
        5, 5);
    // Window 1, 2, 3, 4, 5
    assertEquals(window.lengthToIndex(4), 5);
    assertEquals(window.lengthToIndex(2), 3);
    assertEquals(window.lengthToIndex(1), 2);
    assertEquals(window.lengthToIndex(0), 1);

    // Window 6, 2, 3, 4, 5
    window.advanceWindowTo(6);
    assertEquals(window.lengthToIndex(4), 4);
    assertEquals(window.lengthToIndex(2), 2);
    assertEquals(window.lengthToIndex(1), 1);
    assertEquals(window.lengthToIndex(0), 5);

    // Window 6, 7, 8, 4, 5
    window.advanceWindowTo(8);
    assertEquals(window.lengthToIndex(4), 2);
    assertEquals(window.lengthToIndex(2), 5);
    assertEquals(window.lengthToIndex(1), 4);
    assertEquals(window.lengthToIndex(0), 3);

    // Window 11, 7, 8, 9, 10
    window.advanceWindowTo(11);
    assertEquals(window.lengthToIndex(4), 4);
    assertEquals(window.lengthToIndex(2), 2);
    assertEquals(window.lengthToIndex(1), 1);
    assertEquals(window.lengthToIndex(0), 5);
  }
}
