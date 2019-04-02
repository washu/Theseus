package ai.eloquent.util;

import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.*;

/**
 * Test {@link Lazy}.
 *
 * @author <a href="mailto:gabor@eloquent.ai">Gabor Angeli</a>
 */
public class LazyTest {

  @Test
  public void testOfValue() {
    Lazy<String> x = Lazy.ofValue("foo");
    assertEquals(Optional.of("foo"), x.getIfPresent());
    assertEquals("foo", x.get());
  }


  @Test
  public void testOfSupplier() {
    Lazy<String> x = Lazy.ofSupplier(() -> "foo");
    assertEquals(Optional.empty(), x.getIfPresent());
    assertEquals("foo", x.get());
  }

  @Test
  public void testDontDoubleCompute() {
    int[] v = new int[]{7};
    Lazy<Integer> x = Lazy.ofSupplier(() -> ++v[0]);
    assertEquals(8, x.get().intValue());
    assertEquals("We should not double compute the value", 8, x.get().intValue());
  }

  @Test
  public void testMap() {
    // Create an int lazy
    int[] v = new int[]{7};
    Lazy<Integer> x = Lazy.ofSupplier(() -> ++v[0]);
    assertEquals(8, x.get().intValue());
    assertEquals("We should not double compute the value", 8, x.get().intValue());
    // Map it to a String
    Lazy<String> y = x.map(String::valueOf);
    assertEquals("8", y.get());
    assertEquals("8", y.get());
  }
}