package ai.eloquent.util;

import static org.junit.Assert.*;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the utilities in {@link ConcurrencyUtils}.
 *
 * @author <a href="mailto:gabor@eloquent.ai">Gabor Angeli</a>
 */
public class ConcurrencyUtilsTest {


  /**
   * Test that we pick up when we are holding a lock.
   */
  @Test
  public void ensureNoLocksHeld() {
    assertTrue(ConcurrencyUtils.ensureNoLocksHeld());
    synchronized (this) {
      try {
        ConcurrencyUtils.ensureNoLocksHeld();
      } catch (AssertionError e) {
        e.printStackTrace(System.out);
        return;
      }
      fail("We should register that we're holding a lock");
    }
  }


  /**
   * Test that we pick up when we are holding a lock defined on a method.
   */
  @Test
  public synchronized void ensureNoLocksHeldSynchronizedMethod() {  /* test is that this is synchronized */
    try {
      ConcurrencyUtils.ensureNoLocksHeld();
    } catch (AssertionError e) {
      e.printStackTrace(System.out);
      return;
    }
    fail("We should register that we're holding a lock");
  }

}