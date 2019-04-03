package ai.eloquent.util;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * A span, inclusive of {@link Span#begin} and exclusive of {@link Span#end}.
 *
 * @author <a href="mailto:gabor@eloquent.ai">Gabor Angeli</a>
 */
public class Span implements Iterable<Long> {

  /** The beginning of this span, inclusive. */
  public final long begin;

  /** The end of this span, exclusive. */
  public final long end;


  /**
   * Create a new span from two values.
   *
   * @param begin The beginning of the span, inclusive.
   * @param end The end of the span, exclusive.
   */
  public Span(long begin, long end) {
    this.begin = begin;
    this.end = end;
  }


  /**
   * Returns true if this span contains the argument span, inclusive of the start and end points
   *
   * @param other The other span.
   * @return True if the argument span is contained in this span.
   */
  public boolean contains(Span other) {
    return this.begin <= other.begin && this.end >= other.end;
  }


  /**
   * The length of this span
   * Also the distance from {@link Span#begin} to {@link Span#end}.
   */
  public long length() {
    return end - begin;
  }


  /**
   * Returns true if there's any overlap between this span and the argument span.
   *
   * @param other The other span to compare this span to.
   * @return True if this and the other span have any overlap.
   */
  public boolean overlaps(Span other) {
    return intersect(this, other).length() > 0;
  }


  /**
   * Returns the amount of overlap between this span and the argument span.
   *
   * @param other The argument span.
   * @return The number of tokens that overlap between the two spans.
   */
  public long overlap(Span other) {
    return intersect(this, other).length();
  }


  /**
   * Returns the intersection between two spans.
   *
   * @param a The first span.
   * @param b The second span.
   *
   * @return A span denoting the intersection between the two spans.
   */
  public static Span intersect(Span a, Span b) {
    long newStart = Math.max(a.begin, b.begin);
    long newEnd = Math.min(a.end, b.end);
    if (newStart < newEnd) {
      return new Span(newStart, newEnd);
    } else {
      return new Span(0, 0); // There is no intersect
    }
  }


  /**
   * Create a span from two values, if we don't know which one should be the begin / end.
   *
   * @param a One of the values (not necessarily the smaller).
   * @param b The other value (not necessarily bigger).
   *
   * @return The span of a and b.
   */
  public static Span fromValues(long a, long b) {
    if (a < b) {
      return new Span(a, b);
    } else {
      return new Span(b, a);
    }
  }


  /** {@inheritDoc} */
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Span span = (Span) o;
    return begin == span.begin && end == span.end;
  }


  /** {@inheritDoc} */
  @Override
  public String toString() {
    return "[" + begin + ", " + end + ")";
  }


  /**
   * If true, the value is contained within the span.
   * (includes begin, excludes end)
   *
   * @param i The value we are checking.
   *
   * @return True    if begin <= i < end.
   *         False   otherwise
   */
  public boolean contains(int i) {
    return i >= this.begin && i < this.end;
  }


  /**
   * The middle value of the span
   * ie. (end + begin) / 2
   */
  public long middle() {
    return (end + begin) / 2;
  }


  /**
   * @return An iterator that iterates each value within the span
   */
  @Override
  public Iterator<Long> iterator() {
    return new Iterator<Long>() {
      private long i = begin;
      @Override
      public boolean hasNext() {
        return i < end;
      }
      @Override
      public Long next() {
        if (!hasNext()) { throw new NoSuchElementException(); }
        i += 1;
        return i - 1;
      }
    };
  }
}
