package ai.eloquent.util;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link IdentityHashSet}
 *
 * @author <a href="mailto:zames@eloquent.ai">Zames Chua</a>
 */
public class IdentityHashSetTest {

  @Test
  public void testAdd() {
    IdentityHashSet<Integer> identityHashSet = new IdentityHashSet<>();
    Integer integerObject = new Integer(1);
    assertTrue(identityHashSet.add(integerObject));
    assertTrue(identityHashSet.add(new Integer(1)));
    assertFalse(identityHashSet.add(integerObject));
    assertEquals(identityHashSet.size(), 2);
  }

  @Test
  public void testContains() {
    IdentityHashSet<Integer> identityHashSet = new IdentityHashSet<>();
    Integer integerObject = new Integer(1);
    identityHashSet.add(integerObject);
    assertTrue(identityHashSet.contains(integerObject));
    assertFalse(identityHashSet.contains(new Integer(1)));
  }

}
