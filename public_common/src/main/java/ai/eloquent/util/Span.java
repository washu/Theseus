package ai.eloquent.util;

import java.io.Serializable;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * A span, inclusive of {@link Span#begin} and exclusive of {@link Span#end}.
 *
 * @author gabor
 */
public class Span implements Iterable<Long>, Comparable<Span>, Serializable {
  /** The signature for serializing this version of this class. */
  private static final long serialVersionUID = 1L;

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
   * Returns true if this span contains the argument span.
   * This includes the case when the two spans are the same.
   *
   * @param other The other span.
   * @return True if the argument span is contained in this span.
   */
  public boolean contains(Span other) {
    return this.begin <= other.begin && this.end >= other.end;
  }


  /**
   * The length of this span -- i.e., the distance from {@link Span#begin} to {@link Span#end}.
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
    //noinspection SimplifiableIfStatement
    if (this.begin == this.end || other.begin == other.end) {
      return false;  // length 0 spans can't overlap
    }
    return this.contains(other) ||
        other.contains(this) ||
        (this.end > other.end && this.begin < other.end) ||
        (other.end > this.end && other.begin < this.end) ||
        this.equals(other);
  }


  /**
   * Returns the amount of overlap between this span and the argument span.
   *
   * @param other The argument span.
   * @return The number of tokens that overlap between the two spans.
   */
  public long overlap(Span other) {
    if (this.contains(other) || other.contains(this)) {
      return Math.min(this.end - this.begin, other.end - other.begin);
    } else if ((this.end > other.end && this.begin < other.end) ||
        (other.end > this.end && other.begin < this.end)) {
      return Math.min(this.end, other.end) - Math.max(this.begin, other.begin);
    } else {
      return 0;
    }
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
    } else if (a.begin == b.end) {
      return new Span(a.begin, a.begin);
    } else if (a.end == b.begin) {
      return new Span(a.end, a.end);
    } else {
      return new Span(0, 0);
    }
  }


  /**
   * Create a span from two values.
   *
   * @param a One of the values (not necessarily the smaller).
   * @param b The other value (not necessarily bigger).
   *
   * @return The span between a and b.
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
  public int hashCode() {
    return ((int) begin) ^ ((int) end);
  }


  /** {@inheritDoc} */
  @Override
  public String toString() {
    return "[" + begin + ", " + end + ")";
  }


  /** {@inheritDoc} */
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
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        i += 1;
        return i - 1;
      }
    };
  }


  /** {@inheritDoc} */
  @Override
  public int compareTo(Span o) {
    int pass1 = Long.compare(this.begin, o.begin);
    if (pass1 != 0) {
      return pass1;
    }
    return Long.compare(this.end, o.end);
  }


  /**
   * If true, the value is contined in the span.
   * This is defined with an <b>inclusive</b> begin index and an
   * <b>exclusive</b> end index.
   *
   * @param i The value we are checking.
   *
   * @return True if begin &lt;= i &lt; end.
   */
  public boolean contains(int i) {
    return i >= this.begin && i < this.end;
  }


  /**
   * The middle (average?) of the span. That is, (end + begin / 2).
   */
  public long middle() {
    return (end + begin) / 2;
  }
}
