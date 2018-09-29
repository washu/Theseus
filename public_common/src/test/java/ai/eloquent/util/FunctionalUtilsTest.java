package ai.eloquent.util;

import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

/**
 * Test {@link FunctionalUtils}
 *
 * @author <a href="mailto:gabor@eloquent.ai">Gabor Angeli</a>
 */
public class FunctionalUtilsTest {
  @Test
  public void ofThrowable() {
    assertTrue(FunctionalUtils.ofThrowable(() -> 42).isPresent());
    assertFalse(FunctionalUtils.ofThrowable(() -> {
      throw new IllegalStateException("hi");
    }).isPresent());
    assertFalse(FunctionalUtils.ofThrowable(() -> {
      throw new IOException();
    }).isPresent());
  }

  @Test
  public void immutableList() {
    // Check for type preservation
    assertFalse(FunctionalUtils.immutable(new ArrayList<>()) instanceof ArrayList);
    assertFalse(FunctionalUtils.immutable(new LinkedList<>()) instanceof LinkedList);
    assertFalse(FunctionalUtils.immutable(new Stack<>()) instanceof Stack);
    // Check for immutability
    assertFalse(FunctionalUtils.ofThrowable(() -> FunctionalUtils.immutable(new Stack<>()).add("foo")).isPresent());
    // Check for stackoverflows
    List<String> lst = new ArrayList<>();
    lst.add("foo");
    for (int i = 0; i < 1000000; ++i) {
      lst = FunctionalUtils.immutable(lst);
    }
    assertEquals("foo", lst.get(0));
  }

  @Test
  public void immutableMap() {
    // Check for type preservation
    assertFalse(FunctionalUtils.immutable(new HashMap<>()) instanceof HashMap);
    assertFalse(FunctionalUtils.immutable(new TreeMap<>()) instanceof TreeMap);
    assertFalse(FunctionalUtils.immutable(new IdentityHashMap<>()) instanceof IdentityHashMap);
    // Check for immutability
    assertFalse(FunctionalUtils.ofThrowable(() -> FunctionalUtils.immutable(new HashMap<>()).put("foo", "bar")).isPresent());
    // Check for stackoverflows
    Map<String, String> map = new HashMap<>();
    map.put("foo", "bar");
    for (int i = 0; i < 1000000; ++i) {
      map = FunctionalUtils.immutable(map);
    }
    assertEquals("bar", map.get("foo"));
  }

  @Test
  public void immutableSet() {
    // Check for type preservation
    assertFalse(FunctionalUtils.immutable(new HashSet<>()) instanceof HashSet);
    assertFalse(FunctionalUtils.immutable(new TreeSet<>()) instanceof TreeSet);
    assertFalse(FunctionalUtils.immutable(new IdentityHashSet<>()) instanceof IdentityHashSet);
    // Check for immutability
    assertFalse(FunctionalUtils.ofThrowable(() -> FunctionalUtils.immutable(new HashSet<>()).add("foo")).isPresent());
    // Check for stackoverflows
    Set<String> set = new HashSet<>();
    set.add("foo");
    for (int i = 0; i < 1000000; ++i) {
      set = FunctionalUtils.immutable(set);
    }
    assertEquals(1, set.size());
  }

}