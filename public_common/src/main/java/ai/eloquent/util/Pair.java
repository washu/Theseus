package ai.eloquent.util;

import java.util.Objects;

/**
 * Eloquent's implementation of the Pair class for holding mutable pairs of objects.
 *
 * @author <a href="mailto:zames@eloquent.ai">Zames Chua</a>
 */

public class Pair<T1,T2> {

  private T1 first;

  private T2 second;

  /**
   * Initialize an empty pair with first = null and second = null
   */
  public Pair() {
  }

  /**
   * The default constructor to make a pair out of two objects
   * @param first The first object
   * @param second The second object
   */
  public Pair(T1 first, T2 second) {
    this.first = first;
    this.second = second;
  }

  /**
   * @return the first object in the pair
   */
  public T1 first() {
    return first;
  }

  /**
   * @return the second object in the pair
   */
  public T2 second() {
    return second;
  }

  /**
   * Set the first item in the pair
   * @param object the object to set as the first item in the pair
   */
  public void setFirst(T1 object) {
    first = object;
  }

  /**
   * Set the second item in the pair
   * @param object the object to set as the second item in the pair
   */
  public void setSecond(T2 object) {
    second = object;
  }

  @Override
  public String toString() {
    return "(" + first + ", " + second + ")";
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof Pair) {
      @SuppressWarnings("rawtypes")
      Pair otherPair = (Pair) other;
      return first.equals(otherPair.first()) && second.equals(otherPair.second());
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(first, second);
  }

  // todo[zames] inline this method and use the default constructor instead
  public static <X, Y> Pair<X, Y> makePair(X x, Y y) {
    return new Pair<>(x, y);
  }

}
