package ai.eloquent.util;

import java.util.Arrays;
import java.util.stream.Stream;

/**
 * A collection of static methods for string manipulation
 *
 * @author <a href="mailto:zames@eloquent.ai">Zames Chua</a>
 */
public class StringUtils {

  /**
   * Joins each elem in the {@link Iterable} with the given glue.
   */
  public static <X> String join(Iterable<X> objects, String glue) {
    StringBuilder sb = new StringBuilder();
    boolean isFirst = true;
    for (X object : objects) {
      if (!isFirst) {
        sb.append(glue);
      } else {
        isFirst = false;
      }
      sb.append(object);
    }
    return sb.toString();
  }


  /**
   * Joins each elem in the array with the given glue. For example, given a
   */
  public static String join(Object[] elements, String glue) {
    return join(Arrays.asList(elements), glue);
  }

  /**
   * Joins each elem in the given String[] array with the given glue.
   */
  public static String join(String[] items, String glue) {
    return join(Arrays.asList(items), glue);
  }


  /**
   * Joins each elem in the {@link Stream} with the given glue.
   *
   * @see StringUtils#join(Iterable, String)
   */
  public static <X> String join(Stream<X> l, String glue) {
    return join(() -> l.iterator(), glue);
  }

}
