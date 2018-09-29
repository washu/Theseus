package ai.eloquent.util;

import org.junit.Test;

import java.util.Iterator;

import static org.junit.Assert.*;


/**
 * Test the behavior of the {@link Span} class.
 */
public class SpanTest {

  @Test
  public void testIteratorOneElement() {
    Iterator<Long> iter = new Span(1, 2).iterator();
    assertTrue(iter.hasNext());
    assertEquals(1, iter.next().intValue());
    assertFalse(iter.hasNext());
  }

  @Test
  public void testIteratorTwoElements() {
    Iterator<Long> iter = new Span(1, 3).iterator();
    assertTrue(iter.hasNext());
    assertEquals(1, iter.next().intValue());
    assertTrue(iter.hasNext());
    assertEquals(2, iter.next().intValue());
    assertFalse(iter.hasNext());
  }

  @Test
  public void testIteratorNoElements() {
    Iterator<Long> iter = new Span(1, 1).iterator();
    assertFalse(iter.hasNext());
  }

  @Test
  public void testIteratorMisspecified() {
    Iterator<Long> iter = new Span(1, 0).iterator();
    assertFalse(iter.hasNext());
  }

  @Test
  public void testOverlaps() {
    assertTrue(new Span(2, 5).overlaps(new Span(3, 6)));
    assertTrue(new Span(2, 5).overlaps(new Span(2, 5)));
    assertTrue(new Span(2, 5).overlaps(new Span(3, 4)));
    assertTrue(new Span(2, 5).overlaps(new Span(1, 3)));
    assertTrue(new Span(2, 5).overlaps(new Span(2, 3)));
    assertTrue(new Span(2, 5).overlaps(new Span(4, 5)));
  }

  @Test
  public void testNotOverlaps() {
    assertFalse(new Span(2, 5).overlaps(new Span(1, 2)));
    assertFalse(new Span(2, 5).overlaps(new Span(5, 6)));
    assertFalse(new Span(2, 5).overlaps(new Span(5, 5)));
    assertFalse(new Span(2, 5).overlaps(new Span(2, 2)));
    assertFalse(new Span(2, 5).overlaps(new Span(8, 10)));
    assertFalse(new Span(2, 5).overlaps(new Span(0, 1)));
  }

  @Test
  public void testOverlapCount() {
    assertEquals(1, new Span(2, 5).overlap(new Span(2, 3)));
    assertEquals(1, new Span(2, 5).overlap(new Span(1, 3)));
    assertEquals(1, new Span(2, 5).overlap(new Span(4, 5)));
    assertEquals(1, new Span(2, 5).overlap(new Span(4, 6)));
    assertEquals(1, new Span(2, 5).overlap(new Span(3, 4)));
    assertEquals(3, new Span(2, 5).overlap(new Span(0, 100)));
  }

  @Test
  public void testOverlapCountNoOverlap() {
    assertEquals(0, new Span(2, 5).overlap(new Span(1, 2)));
    assertEquals(0, new Span(2, 5).overlap(new Span(5, 6)));
    assertEquals(0, new Span(2, 5).overlap(new Span(5, 5)));
    assertEquals(0, new Span(2, 5).overlap(new Span(8, 10)));
    assertEquals(0, new Span(2, 5).overlap(new Span(0, 1)));
  }

  @Test
  public void testToString() {
    assertEquals("[1, 5)", new Span(1, 5).toString());
  }

  @Test
  public void testFromValues() {
    assertEquals(new Span(3,4), Span.fromValues(3,4));
    assertEquals(new Span(3,4), Span.fromValues(4,3));
  }

  @Test
  public void testLength() {
    assertEquals(1, new Span(3,4).length());
    assertEquals(0, new Span(4,4).length());
    assertEquals(5, new Span(-4,1).length());
  }

  @Test
  public void testContains() {
    assertTrue(new Span(1, 4).contains(new Span(3, 4)));
    assertTrue(new Span(3, 4).contains(new Span(3, 4)));
    assertTrue(new Span(1, 4).contains(new Span(1, 2)));
  }

  @Test
  public void testContainsInteger() {
    assertFalse(new Span(1,4).contains(0));
    assertTrue(new Span(1,4).contains(1));
    assertTrue(new Span(1,4).contains(2));
    assertTrue(new Span(1,4).contains(3));
    assertFalse(new Span(1,4).contains(4));
  }

  @Test
  public void testIntersect() {
    assertEquals(new Span(1,2), Span.intersect(new Span(1, 4),new Span(0,2)));
    assertEquals(new Span(1,2), Span.intersect(new Span(1, 4),new Span(1,2)));
    assertEquals(new Span(1,2), Span.intersect(new Span(0, 4),new Span(1,2)));
    assertEquals(new Span(1,2), Span.intersect(new Span(0, 2),new Span(1,5)));
  }

  @Test
  public void testIntersectEdgeCases() {
    assertEquals(new Span(4,4), Span.intersect(new Span(1, 4),new Span(4,5)));
    assertEquals(new Span(0,0), Span.intersect(new Span(1, 2),new Span(4,5)));
  }
}

