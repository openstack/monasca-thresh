package com.hpcloud.maas.util.stats;

import static com.hpcloud.maas.Assert.assertArraysEqual;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.testng.annotations.Test;

import com.hpcloud.maas.util.time.Timescale;

/**
 * @author Jonathan Halterman
 */
@Test
public class SlidingWindowStatsTest {
  public void testIndexOf() {
    SlidingWindowStats window = new SlidingWindowStats(Statistics.Sum.class, Timescale.RELATIVE, 1,
        5, 2, 5);

    // Window 1, 2, 3, 4, 5, 6, 7
    assertEquals(window.indexOf(0), 0);
    assertEquals(window.indexOf(1), 1);
    assertEquals(window.indexOf(2), 2);
    assertEquals(window.indexOf(4), 4);
    assertEquals(window.indexOf(6), 6);

    // Window 8, 9, 10, 4, 5, 6, 7
    window.slideViewTo(8);
    assertEquals(window.indexOf(0), 3);
    assertEquals(window.indexOf(1), 4);
    assertEquals(window.indexOf(2), 5);
    assertEquals(window.indexOf(4), 0);
    assertEquals(window.indexOf(6), 2);

    // Window 8, 9, 10, 11, 12, 6, 7
    window.slideViewTo(10);
    assertEquals(window.indexOf(0), 5);
    assertEquals(window.indexOf(1), 6);
    assertEquals(window.indexOf(2), 0);
    assertEquals(window.indexOf(4), 2);
    assertEquals(window.indexOf(6), 4);

    // Window 15, 9, 10, 11, 12, 13, 14
    window.slideViewTo(13);
    assertEquals(window.indexOf(0), 1);
    assertEquals(window.indexOf(1), 2);
    assertEquals(window.indexOf(2), 3);
    assertEquals(window.indexOf(4), 5);
    assertEquals(window.indexOf(6), 0);
  }

  public void shouldGetTimestamps() {
    SlidingWindowStats window = new SlidingWindowStats(Statistics.Sum.class, Timescale.RELATIVE, 1,
        5, 2, 10);

    assertArraysEqual(window.getTimestamps(), new long[] { 6, 7, 8, 9, 10 });
    window.slideViewTo(14);
    assertArraysEqual(window.getTimestamps(), new long[] { 10, 11, 12, 13, 14 });

    window = new SlidingWindowStats(Statistics.Average.class, Timescale.RELATIVE, 3, 3, 2, 6);

    assertArraysEqual(window.getTimestamps(), new long[] { 0, 3, 6 });
    window.slideViewTo(14);
    assertArraysEqual(window.getTimestamps(), new long[] { 9, 12, 15 });
  }

  public void shouldSlideViewTo() {
    SlidingWindowStats window = new SlidingWindowStats(Statistics.Average.class,
        Timescale.RELATIVE, 3, 3, 2, 6);

    window.slideViewTo(2);
    window.slideViewTo(7);
    assertEquals(window.getTimestamps(), new long[] { 3, 6, 9 });

    window.slideViewTo(9);
    assertArraysEqual(window.getTimestamps(), new long[] { 3, 6, 9 });
    window.slideViewTo(12);
    assertArraysEqual(window.getTimestamps(), new long[] { 6, 9, 12 });

    window.slideViewTo(14);
    assertArraysEqual(window.getTimestamps(), new long[] { 9, 12, 15 });

    window.slideViewTo(18);
    assertArraysEqual(window.getTimestamps(), new long[] { 12, 15, 18 });

    // Attempt to slide backwards - Noop
    window.slideViewTo(10);
    assertArraysEqual(window.getTimestamps(), new long[] { 12, 15, 18 });
  }

  public void shouldAddValueAndGetWindowValues() {
    SlidingWindowStats window = new SlidingWindowStats(Statistics.Average.class,
        Timescale.RELATIVE, 3, 3, 2, 6);
    for (int i = 0; i < 5; i++)
      window.addValue(999, i * 3);

    assertEquals(window.getWindowValues(), new double[] { 999, 999, 999, 999, 999 });

    window.slideViewTo(12);
    assertEquals(window.getWindowValues(), new double[] { 999, 999, 999, 0, 0 });

    window.addValue(777, 14);
    window.addValue(888, 17);
    assertEquals(window.getWindowValues(), new double[] { 999, 999, 999, 777, 888 });
  }

  public void shouldAddValueAndGetViewValues() {
    SlidingWindowStats window = new SlidingWindowStats(Statistics.Average.class,
        Timescale.RELATIVE, 3, 3, 2, 6);
    for (int i = 0; i < 5; i++)
      window.addValue(999, i * 3);

    assertEquals(window.getViewValues(), new double[] { 999, 999, 999 });

    window.slideViewTo(12);
    assertEquals(window.getViewValues(), new double[] { 999, 999, 999 });

    window.addValue(777, 14);
    window.addValue(888, 17);
    assertEquals(window.getViewValues(), new double[] { 999, 999, 999 });
    window.slideViewTo(18);
    assertEquals(window.getViewValues(), new double[] { 999, 777, 888 });
  }

  public void testIndexOfTime() {
    SlidingWindowStats window = new SlidingWindowStats(Statistics.Average.class,
        Timescale.RELATIVE, 3, 3, 2, 15);

    // Slots look like 9 12 15 18 21
    assertEquals(window.indexOfTime(5), -1);
    assertEquals(window.indexOfTime(9), 0);
    assertEquals(window.indexOfTime(10), 1);
    assertEquals(window.indexOfTime(12), 1);
    assertEquals(window.indexOfTime(13), 2);
    assertEquals(window.indexOfTime(15), 2);
    assertEquals(window.indexOfTime(17), 3);
    assertEquals(window.indexOfTime(20), 4);

    window.slideViewTo(19);

    // Slots like 24 27 15 18 21
    assertEquals(window.indexOfTime(12), -1);
    assertEquals(window.indexOfTime(13), 2);
    assertEquals(window.indexOfTime(15), 2);
    assertEquals(window.indexOfTime(17), 3);
    assertEquals(window.indexOfTime(20), 4);
    assertEquals(window.indexOfTime(22), 0);
    assertEquals(window.indexOfTime(26), 1);
    assertEquals(window.indexOfTime(28), -1);

    window.slideViewTo(22);

    // Slots like 24 27 30 18 21
    assertEquals(window.indexOfTime(15), -1);
    assertEquals(window.indexOfTime(17), 3);
    assertEquals(window.indexOfTime(20), 4);
    assertEquals(window.indexOfTime(22), 0);
    assertEquals(window.indexOfTime(26), 1);
    assertEquals(window.indexOfTime(30), 2);
    assertEquals(window.indexOfTime(31), -1);
  }

  public void shouldGetValue() {
    SlidingWindowStats window = new SlidingWindowStats(Statistics.Sum.class, Timescale.RELATIVE, 5,
        3, 2, 15);
    // Logical window is 15 10 5
    window.addValue(4, 5);
    window.addValue(3, 10);
    window.addValue(2, 15);

    assertEquals(window.getValue(15), 2.0);
    assertEquals(window.getValue(10), 3.0);
    assertEquals(window.getValue(5), 4.0);

    // Slide logical window to 20 15 10
    window.slideViewTo(16);
    window.addValue(5, 16);

    assertEquals(window.getValue(16), 5.0);
    assertEquals(window.getValue(15), 2.0);
    assertEquals(window.getValue(10), 3.0);
  }

  public void testLengthToIndex() {
    SlidingWindowStats window = new SlidingWindowStats(Statistics.Sum.class, Timescale.RELATIVE, 1,
        5, 2, 5);
    // Window 1, 2, 3, 4, 5, 6, 7
    assertEquals(window.lengthToIndex(6), 7);
    assertEquals(window.lengthToIndex(4), 5);
    assertEquals(window.lengthToIndex(2), 3);
    assertEquals(window.lengthToIndex(1), 2);
    assertEquals(window.lengthToIndex(0), 1);

    // Window 8, 2, 3, 4, 5, 6, 7
    window.slideViewTo(6);
    assertEquals(window.lengthToIndex(6), 6);
    assertEquals(window.lengthToIndex(4), 4);
    assertEquals(window.lengthToIndex(2), 2);
    assertEquals(window.lengthToIndex(1), 1);
    assertEquals(window.lengthToIndex(0), 7);

    // Window 8, 9, 10, 4, 5, 6, 7
    window.slideViewTo(8);
    assertEquals(window.lengthToIndex(6), 4);
    assertEquals(window.lengthToIndex(4), 2);
    assertEquals(window.lengthToIndex(2), 7);
    assertEquals(window.lengthToIndex(1), 6);
    assertEquals(window.lengthToIndex(0), 5);

    // Window 8, 9, 10, 11, 12, 13, 7
    window.slideViewTo(11);
    assertEquals(window.lengthToIndex(6), 1);
    assertEquals(window.lengthToIndex(4), 6);
    assertEquals(window.lengthToIndex(2), 4);
    assertEquals(window.lengthToIndex(1), 3);
    assertEquals(window.lengthToIndex(0), 2);
  }

  public void testIsIndexInView() {
    SlidingWindowStats window = new SlidingWindowStats(Statistics.Sum.class, Timescale.RELATIVE, 1,
        5, 2, 5);
    // Window 1, 2, 3, 4, 5, 6, 7
    assertEquals(window.isIndexInView(6), false);
    assertEquals(window.isIndexInView(4), true);
    assertEquals(window.isIndexInView(2), true);

    // Window 8, 2, 3, 4, 5, 6, 7
    window.slideViewTo(6);
    assertEquals(window.isIndexInView(6), false);
    assertEquals(window.isIndexInView(4), true);
    assertEquals(window.isIndexInView(2), true);
    assertEquals(window.isIndexInView(1), true);
    assertEquals(window.isIndexInView(0), false);

    // Window 8, 9, 10, 4, 5, 6, 7
    window.slideViewTo(8);
    assertEquals(window.isIndexInView(6), true);
    assertEquals(window.isIndexInView(4), true);
    assertEquals(window.isIndexInView(2), false);
    assertEquals(window.isIndexInView(1), false);
    assertEquals(window.isIndexInView(0), true);

    // Window 8, 9, 10, 11, 12, 13, 7
    window.slideViewTo(11);
    assertEquals(window.isIndexInView(6), true);
    assertEquals(window.isIndexInView(4), false);
    assertEquals(window.isIndexInView(2), true);
    assertEquals(window.isIndexInView(1), true);
    assertEquals(window.isIndexInView(0), true);
  }

  public void testHasEmptySlotsInView() {
    SlidingWindowStats window = new SlidingWindowStats(Statistics.Sum.class, Timescale.RELATIVE, 3,
        4, 2, 15);
    // Window 6, 9, 12, 15, 18, 21
    window.addValue(222, 14);
    window.addValue(222, 12);
    assertTrue(window.hasEmptySlotsInView());

    window.addValue(222, 18);
    window.addValue(222, 21);
    window.slideViewTo(20);
    assertFalse(window.hasEmptySlotsInView());

    // Window 24, 27, 30, 15, 18, 21
    window.slideViewTo(24);
    assertTrue(window.hasEmptySlotsInView());
    window.addValue(333, 24);
    assertFalse(window.hasEmptySlotsInView());

    window.addValue(444, 27);
    // Window 24, 27, 30, 33, 18, 21
    window.slideViewTo(27);
    assertFalse(window.hasEmptySlotsInView());
    window.slideViewTo(30);
    assertTrue(window.hasEmptySlotsInView());
  }

  public void shouldGetValuesUpTo() {
    SlidingWindowStats window = new SlidingWindowStats(Statistics.Sum.class, Timescale.RELATIVE, 5,
        3, 2, 15);
    // Window is 5 10 15 20 25
    window.addValue(2, 5);
    window.addValue(3, 10);
    window.addValue(4, 15);

    assertEquals(window.getValuesUpTo(13), new double[] { 2, 3, 4 });
    assertEquals(window.getValuesUpTo(7), new double[] { 2, 3 });
    assertEquals(window.getValuesUpTo(4), new double[] { 2 });

    // Window is 30 10 15 20 25
    window.slideViewTo(17);
    window.addValue(5, 17);
    assertEquals(window.getValuesUpTo(17), new double[] { 3, 4, 5 });
    assertEquals(window.getValuesUpTo(12), new double[] { 3, 4 });
    assertEquals(window.getValuesUpTo(7), new double[] { 3 });

    // Window is 30 35 15 20 25
    window.slideViewTo(22);
    window.addValue(6, 21);
    assertEquals(window.getValuesUpTo(22), new double[] { 4, 5, 6 });
    assertEquals(window.getValuesUpTo(20), new double[] { 4, 5 });
    assertEquals(window.getValuesUpTo(14), new double[] { 4 });

    // Assert out of bounds
    try {
      assertEquals(window.getValuesUpTo(9), new double[] {});
      fail();
    } catch (Exception expected) {
    }

    // Assert out of bounds
    try {
      assertEquals(window.getValuesUpTo(36), new double[] {});
      fail();
    } catch (Exception expected) {
    }
  }
}
