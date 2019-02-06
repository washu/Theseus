package ai.eloquent.util;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.*;

/**
 * Test {@link DeferredLazy}
 *
 * @author <a href="mailto:gabor@eloquent.ai">Gabor Angeli</a>
 */
public class DeferredLazyTest {

  /**
   * A little mock lazy for the unit tests
   */
  private static class LoggingLazy extends DeferredLazy<Integer> {
    private int value = 1;

    private boolean valid = true;

    private List<Integer> discarded = new ArrayList<>();

    private void invalidate() {
      valid = false;
    }

    /** {@inheritDoc} */
    @Override
    protected void onDiscard(Integer value) {
      discarded.add(value);
    }

    /** {@inheritDoc} */
    @Override
    protected boolean isValid(Integer value, long lastCompute, long now) {
      return valid;
    }

    /** {@inheritDoc} */
    @Override
    protected Integer compute() {
      // Don't run for a while
      for (int i = 0; i < 5; ++i) {
        Thread.yield();
      }
      Uninterruptably.sleep(5);  // just in case
      for (int i = 0; i < 5; ++i) {
        Thread.yield();
      }
      // OK, run
      valid = true;
      return ++value;
    }
  }


  /**
   * Test that we actually go ahead and compute a value
   */
  @Test
  public void happyPath() {
    LoggingLazy lazy = new LoggingLazy();
    assertEquals(Optional.empty(), lazy.get());
    int i = 0;
    while (lazy.value < 2 && (++i < 1000)) {
      Uninterruptably.sleep(1);
    }
    assertEquals(Optional.of(2), lazy.get());
  }


  /**
   * Test that we only compute once if there's an outstanding computation already
   */
  @Test
  public void avoidDuplicateComputation() {
    LoggingLazy lazy = new LoggingLazy();
    // Try to get the value multiple times
    assertEquals(Optional.empty(), lazy.get());
    assertEquals(Optional.empty(), lazy.get());
    assertEquals(Optional.empty(), lazy.get());
    // Wait for the getter to run
    int i = 0;
    while (lazy.value < 2 && (++i < 1000)) {
      Uninterruptably.sleep(1);
    }
    // Ensure that we only called compute() once
    assertEquals(Optional.of(2), lazy.get());
    Uninterruptably.sleep(50);
    assertEquals(Optional.of(2), lazy.get());
  }


  /**
   * Test that we cannot force a recomputation if we're already
   * computing the value
   */
  @Test
  public void cannotForceIfAlreadyComputing() {
    LoggingLazy lazy = new LoggingLazy();
    // Try to get the value multiple times
    assertEquals(Optional.empty(), lazy.get());
    assertEquals(Optional.empty(), lazy.get(true));  // force recomputation
    assertEquals(Optional.empty(), lazy.get());
    // Wait for the getter to run
    int i = 0;
    while (lazy.value < 3 && (++i < 1000)) {
      Uninterruptably.sleep(1);
    }
    // Ensure that we only called compute() once
    assertEquals("We should still only have computed once", Optional.of(2), lazy.get());
    Uninterruptably.sleep(10);
    assertEquals("We should still only have computed once", Optional.of(2), lazy.get());
  }


  /**
   * Force a recomputation of our value.
   */
  @Test
  public void forceRecomputation() {
    LoggingLazy lazy = new LoggingLazy();
    assertEquals(Optional.empty(), lazy.get());
    int i = 0;
    while (lazy.value < 2 && (++i < 1000)) {
      Uninterruptably.sleep(1);
    }
    // force recompute
    assertEquals("Even though we forced a recomputation, we should still try to return the old value", Optional.of(2), lazy.get(true));
    i = 0;
    while (lazy.value < 3 && (++i < 1000)) {
      Uninterruptably.sleep(1);
    }
    assertEquals("We should have recomputed our value", Optional.of(3), lazy.get());
  }


  /**
   * We should recompute our value if it's invalid.
   */
  @Test
  public void recomputeOnInvalid() {
    LoggingLazy lazy = new LoggingLazy();
    assertEquals(Optional.empty(), lazy.get());
    int i = 0;
    while (lazy.value < 2 && (++i < 1000)) {
      Uninterruptably.sleep(1);
    }
    assertEquals("We should have computed our value", Optional.of(2), lazy.get());
    // invalidate
    lazy.invalidate();
    // Recompute
    assertEquals("After invalidating the value, we should be invalid", Optional.empty(), lazy.get());
    i = 0;
    while (lazy.value < 3 && (++i < 1000)) {
      Uninterruptably.sleep(1);
    }
    assertEquals("We should have recomputed our value", Optional.of(3), lazy.get());
  }


  /**
   * We should not return an invalid value, even if forced to recompute
   */
  @Test
  public void dontReturnInvalidValueOnForce() {
    LoggingLazy lazy = new LoggingLazy();
    assertEquals(Optional.empty(), lazy.get());
    int i = 0;
    while (lazy.value < 2 && (++i < 1000)) {
      Uninterruptably.sleep(1);
    }
    assertEquals("We should have computed our value", Optional.of(2), lazy.get());
    // invalidate
    lazy.invalidate();
    // Recompute
    assertEquals("Even if we force the recompute, we should not return an invalid value", Optional.empty(), lazy.get(true));
    i = 0;
    while (lazy.value < 3 && (++i < 1000)) {
      Uninterruptably.sleep(1);
    }
    assertEquals("We should have recomputed our value", Optional.of(3), lazy.get());
    Uninterruptably.sleep(50);
    assertEquals("We should have recomputed our value only once", Optional.of(3), lazy.get());
  }


  /**
   * Test that we can get a value synchronously
   */
  @Test
  public void getSync() {
    LoggingLazy lazy = new LoggingLazy();
    assertEquals(Integer.valueOf(2), lazy.getSync());
  }


  /**
   * Test that when we get a value synchronously, forcing recomputes it.
   */
  @Test
  public void getSyncForce() {
    LoggingLazy lazy = new LoggingLazy();
    assertEquals(Integer.valueOf(2), lazy.getSync());
    assertEquals(Integer.valueOf(3), lazy.getSync(true));
  }


  /**
   * Test that force recomputation in synchronous mode doesn't overwrite the currently
   * running computation.
   */
  @Test
  public void getSyncForceInFlight() {
    LoggingLazy lazy = new LoggingLazy();
    assertEquals(Optional.empty(), lazy.get());
    assertEquals("Even though we forced a recompute, we're already recomputing the value",
        Integer.valueOf(2), lazy.getSync(true));
  }


  /**
   * Test that onDiscard() is called.
   */
  @Test
  public void onDiscardCalled() {
    LoggingLazy lazy = new LoggingLazy();
    assertEquals(Optional.empty(), lazy.get());
    int i = 0;
    while (lazy.value < 2 && (++i < 1000)) {
      Uninterruptably.sleep(1);
    }
    // force recompute
    assertEquals("Even though we forced a recomputation, we should still try to return the old value", Optional.of(2), lazy.get(true));
    i = 0;
    while (lazy.value < 3 && (++i < 1000)) {
      Uninterruptably.sleep(1);
    }
    assertEquals("We should have recomputed our value", Optional.of(3), lazy.get());
    // Check that we discarded the old value
    assertEquals("We should have discarded our old value",
        Collections.singletonList(2), lazy.discarded);
  }

}