package ai.eloquent.util;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

public class StringUtils {

  public static final String[] EMPTY_STRING_ARRAY = new String[0];

  /**
   * Joins each elem in the {@link Iterable} with the given glue.
   * For example, given a list of {@code Integers}, you can create
   * a comma-separated list by calling {@code join(numbers, ", ")}.
   */
  public static <X> String join(Iterable<X> l, String glue) {
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (X o : l) {
      if (!first) {
        sb.append(glue);
      } else {
        first = false;
      }
      sb.append(o);
    }
    return sb.toString();
  }


  /**
   * Joins each elem in the array with the given glue. For example, given a
   * list of ints, you can create a comma-separated list by calling
   * {@code join(numbers, ", ")}.
   */
  public static String join(Object[] elements, String glue) {
    return (join(Arrays.asList(elements), glue));
  }

  /**
   * Joins an array of elements in a given span.
   * @param elements The elements to join.
   * @param start The start index to join from.
   * @param end The end (non-inclusive) to join until.
   * @param glue The glue to hold together the elements.
   * @return The string form of the sub-array, joined on the given glue.
   */
  public static String join(Object[] elements, int start, int end, String glue) {
    StringBuilder b = new StringBuilder(127);
    boolean isFirst = true;
    for (int i = start; i < end; ++i) {
      if (isFirst) {
        b.append(elements[i]);
        isFirst = false;
      } else {
        b.append(glue).append(elements[i]);
      }
    }
    return b.toString();
  }


  /**
   * Joins each elem in the given String[] array with the given glue.
   * For example, given a list of {@code Integers}, you can create
   * a comma-separated list by calling {@code join(numbers, ", ")}.
   */
  public static String join(String[] items, String glue) {
    return join(Arrays.asList(items), glue);
  }


  public static <E> String join(List<? extends E> l, String glue, Function<E,String> toStringFunc, int start, int end) {
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    start = Math.max(start, 0);
    end = Math.min(end, l.size());
    for (int i = start; i < end; i++) {
      if ( ! first) {
        sb.append(glue);
      } else {
        first = false;
      }
      sb.append(toStringFunc.apply(l.get(i)));
    }
    return sb.toString();
  }

  /**
   * Joins each elem in the {@link Stream} with the given glue.
   * For example, given a list of {@code Integers}, you can create
   * a comma-separated list by calling {@code join(numbers, ", ")}.
   *
   * @see StringUtils#join(Iterable, String)
   */
  public static <X> String join(Stream<X> l, String glue) {
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    Iterator<X> iter = l.iterator();
    while (iter.hasNext()) {
      if ( ! first) {
        sb.append(glue);
      } else {
        first = false;
      }
      sb.append(iter.next());
    }
    return sb.toString();
  }

}
