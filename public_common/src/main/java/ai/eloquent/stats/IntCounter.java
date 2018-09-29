package ai.eloquent.stats;


import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class IntCounter<E>  {
  @SuppressWarnings({"NonSerializableFieldInSerializableClass"})
  private Map<E, Integer> map;
  private int defaultValue = 0;

  // CONSTRUCTOR
  /**
   * Constructs a new (empty) Counter.
   */
  public IntCounter() {
    map = new HashMap<>();
  }

  /**
   * Adds 1 to the count for the given key. If the key hasn't been seen
   * before, it is assumed to have count 0, and thus this method will set
   * its count to 1.
   */
  public double incrementCount(E key) {
    if (map.get(key) == null) {
      map.put(key, defaultValue);
      return 1;
    } else {
      int count = map.get(key) + 1;
      map.put(key, count);
      return count;
    }
  }

  public void setCount(E key, Integer value) {
    map.put(key, value);
  }

  public int totalIntCount() {
    return map.values()
        .stream()
        .mapToInt(Integer::intValue)
        .sum();
  }

  public Set<Map.Entry<E, Integer>> entrySet() {
    return map.entrySet();
  }
}