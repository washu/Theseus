package ai.eloquent.stats;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * A simple integer Counter implementation that is backed by a HashMap
 * @author <a href="mailto:zames@eloquent.ai">Zames Chua</a>
 */
public class IntCounter<E> {

  /** Start the count of each object to this value */
  private static int DEFAULT_VALUE = 0;

  /** The map that holds each key and their counts **/
  private Map<E, Integer> map;

  /**
   * Constructs a new (empty) Counter.
   */
  public IntCounter() {
    map = new HashMap<>();
  }

  /**
   * Adds 1 to the count for the given key.
   * If the key hasn't been seen before, it is assumed to have count 0,
   * and thus this method will set its count to 1.
   *
   * @return the current count of specified key, after incrementing it
   */
  public double incrementCount(E key) {
    if (map.get(key) == null) {
      map.put(key, DEFAULT_VALUE);
      return 1;
    } else {
      int count = map.get(key) + 1;
      map.put(key, count);
      return count;
    }
  }

  /**
   * Set the count for a certain key to the given value
   **/
  public void setCount(E key, Integer value) {
    map.put(key, value);
  }

  /**
   * @return the sum of all counts being stored by this counter
   */
  public int totalIntCount() {
    return map.values()
        .stream()
        .mapToInt(Integer::intValue)
        .sum();
  }

  /**
   * @return an <code>entrySet</code> from the map that backs this counter
   */
  public Set<Map.Entry<E, Integer>> entrySet() {
    return map.entrySet();
  }
}
