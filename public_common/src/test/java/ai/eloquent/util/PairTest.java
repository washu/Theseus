package ai.eloquent.util;

import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Unit tests for {@link Pair}
 *
 * @author <a href="mailto:zames@eloquent.ai">Zames Chua</a>
 */
public class PairTest {

  @Test
  public void testGetAndSet() {
    Pair<Integer, Integer> pair = new Pair<>(1, 2);
    assertEquals((int) pair.first(), 1);
    assertEquals((int) pair.second(), 2);
    pair.setFirst(3);
    pair.setSecond(4);
    assertEquals((int) pair.first(), 3);
    assertEquals((int) pair.second(), 4);

    Pair<String, String> pair2 = new Pair<>("one", "two");
    assertEquals(pair2.first(), "one");
    assertEquals(pair2.second(), "two");
    pair2.setFirst("three");
    pair2.setSecond("four");
    assertEquals(pair2.first(), "three");
    assertEquals(pair2.second(), "four");
  }

  @Test
  public void testEquals() {
    Pair<Integer, Integer> pair = new Pair<>(1, 2);
    Pair<Integer, Integer> pair2 = new Pair<>(1, 2);
    assertTrue(pair.equals(pair2));
    assertTrue(pair2.equals(pair));

    pair2.setSecond(3);
    assertFalse(pair.equals(pair2));
    assertFalse(pair2.equals(pair));
  }
}
