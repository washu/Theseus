package ai.eloquent.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.LockInfo;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Static utilities for dealing with concurrency -- primarily, with
 * ensuring correctness.
 *
 * @author <a href="mailto:gabor@eloquent.ai">Gabor Angeli</a>
 */
public class ConcurrencyUtils {
  /**
   * An SLF4J Logger for this class.
   */
  @SuppressWarnings("unused")
  private static final Logger log = LoggerFactory.getLogger(ConcurrencyUtils.class);

  /**
   * Thread info bean.
   */
  private static ThreadMXBean threadmx = ManagementFactory.getThreadMXBean();

  /** Static utility class */
  private ConcurrencyUtils() {}


  /**
   * A helper for determining if we can take the given lock according to the lock order.
   *
   * @param lockOrder The lock order we are checking against.
   * @param lockBeingTaken The lock we are trying to take. This can either be a {@link ReentrantLock} or
   *                       an object we're synchronizing on.
   *
   * @return Throws an assert error if something goes wrong, but also returns false just in case.
   */
  @SuppressWarnings({"Duplicates", "ConstantConditions"})
  public static boolean ensureLockOrder(List<Object> lockOrder, Object lockBeingTaken) {
    // Check that there's a lock order defined
    if (lockOrder.isEmpty()) {
      return true;  // we're safe
    }

    // Get the locks before the taken lock
    int lockIndex = lockOrder.indexOf(lockBeingTaken);
    assert lockIndex >= 0 && lockIndex < lockOrder.size() : "Could not find lock in lock order: " + lockBeingTaken;
    if (lockIndex < 0 || lockIndex >= lockOrder.size()) {
      return false;
    }

    // If we already hold the lock, and it's Reentrant, then this is fine
    if (lockBeingTaken instanceof ReentrantLock) {
      if (((ReentrantLock)lockBeingTaken).isHeldByCurrentThread()) {
        return true;
      }
    }

    // Check that the subsequent locks aren't taken
    for (int i = lockIndex + 1; i < lockOrder.size(); ++i) {
      Object downstreamLock = lockOrder.get(i);
      if (downstreamLock instanceof ReentrantLock) {
        assert !((ReentrantLock) downstreamLock).isHeldByCurrentThread() : "Trying to take lock " + lockBeingTaken + " (index " + lockIndex + ") but already hold later lock " + downstreamLock + " (index " + i + ")";
        if (((ReentrantLock) downstreamLock).isHeldByCurrentThread()) {
          return false;
        }
        // (just in case we're synchronizing on a ReentrantLock)
        assert !Thread.holdsLock(downstreamLock) : "Trying to take lock " + lockBeingTaken + " (index " + lockIndex + " ) but already hold later lock " + downstreamLock + " (index " + i + ")";
        if (Thread.holdsLock(downstreamLock)) {
          return false;
        }
      } else {
        assert !Thread.holdsLock(downstreamLock) : "Trying to take lock " + lockBeingTaken + " (index " + lockIndex + ") but already hold later lock " + downstreamLock + " (index " + i + ")";
        if (Thread.holdsLock(downstreamLock)) {
          return false;
        }
      }
    }

    // OK, we can take the lock
    return true;
  }


  /**
   * Ensure that none of the given locks are being held by the current thread.
   * This is useful for, e.g., checking that we are not holding any locks on network calls.
   *
   * @param lockOrder The lock order. The order here doesn't matter, but is meant to
   *                  mirror {@link #ensureLockOrder(List, Object)}.
   *
   * @return Throws an assert error if something goes wrong, but also returns false just in case.
   */
  public static boolean ensureNoLocksHeld(Collection<Object> lockOrder) {
    for (Object downstreamLock : lockOrder) {
      if (downstreamLock instanceof ReentrantLock) {
        assert !((ReentrantLock) downstreamLock).isHeldByCurrentThread() : "Lock " + downstreamLock + " is held by the current thread ( " + Thread.currentThread() + ")";
        if (((ReentrantLock) downstreamLock).isHeldByCurrentThread()) {
          return false;
        }
        // (just in case we're synchronizing on a ReentrantLock)
        assert !Thread.holdsLock(downstreamLock) : "Lock " + downstreamLock + " is held by the current thread ( " + Thread.currentThread() + ")";
        if (Thread.holdsLock(downstreamLock)) {
          return false;
        }
      } else {
        assert !Thread.holdsLock(downstreamLock) : "Lock " + downstreamLock + " is held by the current thread ( " + Thread.currentThread() + ")";
        if (Thread.holdsLock(downstreamLock)) {
          return false;
        }
      }
    }
    return true;
  }


  /**
   * Ensure that the current thread does not hold any synchronized block locks.
   * Note that this method <b>cannot</b> check {@linkplain java.util.concurrent.locks.Lock explicit locks}.
   *
   * @return Throws an assert error if something goes wrong, but also returns false just in case.
   */
  public static boolean ensureNoLocksHeld() {
    ThreadInfo threadInfo = threadmx.getThreadInfo(new long[]{Thread.currentThread().getId()}, true, false)[0];
    LockInfo[] monitorsHeld = threadInfo.getLockedMonitors();
    assert monitorsHeld.length == 0 : "Thread holds " + monitorsHeld.length + " monitor locks (first is " + monitorsHeld[0].getClassName() + ")";
    LockInfo[] objThreadsHeld = threadInfo.getLockedSynchronizers(); // this is always empty unless lockedSynchronizers is set to true in the getThreadInfo() call
    assert objThreadsHeld.length == 0 : "Thread holds " + objThreadsHeld.length + " synchronizers (first is " + objThreadsHeld[0].getClassName() + ")";
    // noinspection ConstantConditions  // note[gabor]: Not always true if we don't have asserts on.
    return objThreadsHeld.length == 0 && monitorsHeld.length == 0;
  }


  /**
   * Ensure that <b>all</b> of the given locks are being held by the current thread.
   * This is useful for, e.g., checking that we hold a lock on a future
   *
   * @param lockOrder The lock order. The order here doesn't matter, but is meant to
   *                  mirror {@link #ensureLockOrder(List, Object)}.
   *
   * @return Throws an assert error if something goes wrong, but also returns false just in case.
   */
  public static boolean ensureLocksHeld(Collection<Object> lockOrder) {
    if (lockOrder.isEmpty()) {
      return true;
    }
    for (Object downstreamLock : lockOrder) {
      if (downstreamLock instanceof ReentrantLock) {
        assert ((ReentrantLock) downstreamLock).isHeldByCurrentThread() : "Lock " + downstreamLock + " is not held by the current thread ( " + Thread.currentThread() + ")";
        if (!((ReentrantLock) downstreamLock).isHeldByCurrentThread()) {
          return false;
        }
        // (just in case we're synchronizing on a ReentrantLock)
        assert Thread.holdsLock(downstreamLock) : "Lock " + downstreamLock + " is not held by the current thread ( " + Thread.currentThread() + ")";
        if (!Thread.holdsLock(downstreamLock)) {
          return false;
        }
      } else {
        assert Thread.holdsLock(downstreamLock) : "Lock " + downstreamLock + " is not held by the current thread ( " + Thread.currentThread() + ")";
        if (!Thread.holdsLock(downstreamLock)) {
          return false;
        }
      }
    }
    return true;
  }
}
