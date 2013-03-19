package com.hpcloud.maas.domain.model;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

/**
 * @author Jonathan Halterman
 */
@Test
public class SlidingWindowStatsTest {
  public void shouldGetSlotForTime() {

  }

  public void shouldNotGetSlotForOutOfWindowTime() {

  }

  public void shouldNotAddSlotOutsideOfWindow() {

  }

  public void shouldAdvanceWindowToTimestamp() {

  }

  public void shouldAdvanceWindow() {
    SlidingWindowStats window = new SlidingWindowStats(1, 3, Statistics.Average.class, 1);
    window.advanceWindowTo(2);
    window.advanceWindowTo(3);
    window.advanceWindowTo(5);
    assertEquals(window.getTimestamps(), new long[] { 5, 4, 3 });

    window.advanceWindowTo(9);
    assertEquals(window.getTimestamps(), new long[] { 9, 8, 7 });

    window.advanceWindowTo(14);
    assertEquals(window.getTimestamps(), new long[] { 14, 13, 12 });

    // Attempt to advance backwards - Noop
    window.advanceWindowTo(5);
    assertEquals(window.getTimestamps(), new long[] { 14, 13, 12 });
  }

  public void testSlotIndexFor() {
    SlidingWindowStats window = new SlidingWindowStats(1, 5, Statistics.Average.class, 10);

    // Window looks like 10 6 7 8 9
    assertEquals(window.slotIndexFor(12), -1);
    assertEquals(window.slotIndexFor(10), 0);
    assertEquals(window.slotIndexFor(9), 4);
    assertEquals(window.slotIndexFor(6), 1);
    assertEquals(window.slotIndexFor(2), -1);

    window.advanceWindowTo(12);

    // Window looks like 10 11 12 8 9
    assertEquals(window.slotIndexFor(12), 2);
    assertEquals(window.slotIndexFor(10), 0);
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
    SlidingWindowStats window = new SlidingWindowStats(1, 3, Statistics.Sum.class, 10);
    window.addValue(1, 10);
    window.addValue(1, 10);
    window.addValue(2, 9);
    window.addValue(2, 9);
    window.addValue(3, 8);
    window.addValue(3, 8);

    assertEquals(window.getValues(), new double[] { 2, 4, 6 });

    // Outside of the window - Noop
    window.addValue(3, 12);
    window.addValue(3, 7);

    assertEquals(window.getValues(), new double[] { 2, 4, 6 });

    window.advanceWindowTo(11);
    assertEquals(window.getValues(), new double[] { 0, 2, 4 });

    window.advanceWindowTo(12);
    assertEquals(window.getValues(), new double[] { 0, 0, 2 });

    window.advanceWindowTo(20);
    assertEquals(window.getValues(), new double[] { 0, 0, 0 });
  }

  public void shouldGetTimestamps() {
    SlidingWindowStats window = new SlidingWindowStats(1, 5, Statistics.Sum.class, 10);

    assertEquals(window.getTimestamps(), new long[] { 10, 9, 8, 7, 6 });
    window.advanceWindowTo(14);
    assertEquals(window.getTimestamps(), new long[] { 14, 13, 12, 11, 10 });
  }
}
