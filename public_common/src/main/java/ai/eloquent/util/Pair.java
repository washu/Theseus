package ai.eloquent.util;

import java.io.DataOutputStream;
import java.util.List;

/**
 * Pair is a Class for holding mutable pairs of objects.
 */

public class Pair <T1,T2> implements Comparable<Pair<T1,T2>> {

  /**
   * Direct access is deprecated.  Use first().
   *
   * @serial
   */
  public T1 first;

  /**
   * Direct access is deprecated.  Use second().
   *
   * @serial
   */
  public T2 second;

  public Pair() {
    // first = null; second = null; -- default initialization
  }

  public Pair(T1 first, T2 second) {
    this.first = first;
    this.second = second;
  }

  public T1 first() {
    return first;
  }

  public T2 second() {
    return second;
  }

  public void setFirst(T1 o) {
    first = o;
  }

  public void setSecond(T2 o) {
    second = o;
  }

  @Override
  public String toString() {
    return "(" + first + "," + second + ")";
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof Pair) {
      @SuppressWarnings("rawtypes")
      Pair p = (Pair) o;
      return (first == null ? p.first() == null : first.equals(p.first())) && (second == null ? p.second() == null : second.equals(p.second()));
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    int firstHash  = (first == null ? 0 : first.hashCode());
    int secondHash = (second == null ? 0 : second.hashCode());

    return firstHash*31 + secondHash;
  }

  /**
   * Returns a Pair constructed from X and Y.  Convenience method; the
   * compiler will disambiguate the classes used for you so that you
   * don't have to write out potentially long class names.
   */
  public static <X, Y> Pair<X, Y> makePair(X x, Y y) {
    return new Pair<>(x, y);
  }

  /**
   * Write a string representation of a Pair to a DataStream.
   * The <code>toString()</code> method is called on each of the pair
   * of objects and a <code>String</code> representation is written.
   * This might not allow one to recover the pair of objects unless they
   * are of type <code>String</code>.
   */
  public void save(DataOutputStream out) {
    try {
      out.writeUTF(first.toString());
      out.writeUTF(second.toString());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Compares this <code>Pair</code> to another object.
   * If the object is a <code>Pair</code>, this function will work providing
   * the elements of the <code>Pair</code> are themselves comparable.
   * It will then return a value based on the pair of objects, where
   * <code>p &gt; q iff p.first() &gt; q.first() ||
   * (p.first().equals(q.first()) &amp;&amp; p.second() &gt; q.second())</code>.
   * If the other object is not a <code>Pair</code>, it throws a
   * <code>ClassCastException</code>.
   *
   * @param another the <code>Object</code> to be compared.
   * @return the value <code>0</code> if the argument is a
   *         <code>Pair</code> equal to this <code>Pair</code>; a value less than
   *         <code>0</code> if the argument is a <code>Pair</code>
   *         greater than this <code>Pair</code>; and a value
   *         greater than <code>0</code> if the argument is a
   *         <code>Pair</code> less than this <code>Pair</code>.
   * @throws ClassCastException if the argument is not a
   *                            <code>Pair</code>.
   * @see java.lang.Comparable
   */
  @SuppressWarnings("unchecked")
  public int compareTo(Pair<T1,T2> another) {
    if (first() instanceof Comparable) {
      int comp = ((Comparable<T1>) first()).compareTo(another.first());
      if (comp != 0) {
        return comp;
      }
    }

    if (second() instanceof Comparable) {
      return ((Comparable<T2>) second()).compareTo(another.second());
    }

    if ((!(first() instanceof Comparable)) && (!(second() instanceof Comparable))) {
      throw new AssertionError("Neither element of pair comparable");
    }

    return 0;
  }

}