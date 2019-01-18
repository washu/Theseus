package ai.eloquent.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 *   An implementation of a Leaky Bucket rate limiter.
 *   For standard usage, call {@link #submit()} and use the return value
 *   to determine if the request was accepted.
 * </p>
 *
 * <p>
 *   The class also allows for:
 * </p>
 *
 * <ol>
 *   <li>Forcing elements into the bucket, </li>
 * </ol>
 *
 * @author <a href="mailto:gabor@eloquent.ai">Gabor Angeli</a>
 */
public class LeakyBucket {
  /**
   * An SLF4J Logger for this class.
   */
  @SuppressWarnings("unused")
  private static final Logger log = LoggerFactory.getLogger(LeakyBucket.class);

  /**
   * The maximum number of requests that can be queued in the bucket.
   */
  public final long bucketCapacity;

  /**
   * The period between requests being serviced on average.
   * This is the rate of the "leaK", defined as the period between
   * requests leaking.
   */
  public final long nanosBetweenLeaks;

  /**
   * The current number of drops queued in the bucket.
   */
  private long dropsInBucket = 0L;

  /**
   * The last time we computed leaks
   */
  private long lastLeakNanos = 0L;  // start of JVM


  /**
   * The straightforward constructor.
   *
   * @param bucketCapacity See {@link #bucketCapacity}.
   * @param nanosBetweenLeaks See {@link #nanosBetweenLeaks}.
   */
  public LeakyBucket(long bucketCapacity, long nanosBetweenLeaks) {
    assert bucketCapacity > 0 : "It doesn't make sense to create a bucket with capacity <= 0";
    assert nanosBetweenLeaks > 0 : "It doesn't make sense to make the nanos between leaks <= 0";
    this.bucketCapacity = bucketCapacity;
    this.nanosBetweenLeaks = nanosBetweenLeaks;
  }


  /**
   * Refresh the number of drops we have in the bucket.
   *
   * @param nanosNow The current timestamp in nanoseconds.
   */
  private synchronized void refreshDrops(long nanosNow) {
    long nanosElapsed = nanosNow - lastLeakNanos;
    long dropsLost = nanosElapsed / this.nanosBetweenLeaks;
    this.dropsInBucket = Math.max(0, dropsInBucket - dropsLost);
    if (dropsLost > 0 && this.lastLeakNanos == 0) {  // special case for the first drop
      this.lastLeakNanos = nanosNow;
    } else {
      this.lastLeakNanos += (dropsLost * this.nanosBetweenLeaks);  // not nanosElapsed! We need to round to the last drop.
    }
  }


  /**
   * If true, the leaky bucket is currently full and should not accept requests.
   * Prefer using {@link #submit()} in most cases.
   *
   * @param nanosNow The current timestamp in nanoseconds.
   */
  public boolean isFull(long nanosNow) {
    refreshDrops(nanosNow);
    return dropsInBucket >= bucketCapacity;
  }


  /**
   * @see #isFull(long)
   */
  public boolean isFull() {
    return isFull(System.nanoTime());
  }


  /**
   * Submit a run to the bucket.
   *
   * @param nanosNow The current timestamp in nanoseconds.
   *
   * @return True if we can execute our request.
   *         False otherwise.
   *         Note that if this returns false, the request was not actually
   *         queued and is not considered to be in the bucket.
   */
  public synchronized boolean submit(long nanosNow) {
    refreshDrops(nanosNow);
    if (this.dropsInBucket < this.bucketCapacity) {
      this.dropsInBucket += 1;
      return true;
    } else {
      return false;
    }

  }


  /**
   * @see #submit(long)
   */
  public boolean submit() {
    return submit(System.nanoTime());
  }


  /**
   * Force submitting a request, adding a drop to  the bucket.
   */
  public void forceSubmit() {
    this.dropsInBucket += 1;
  }


  /**
   * Submit a run to the bucket, blocking until we actually can.
   *
   * @see #submit(long)
   */
  public void submitBlocking() throws InterruptedException {
    long now = System.nanoTime();
    while (!submit(now)) {
      // Sleep (period - already_elapsed) nanos
      long nanosToSleep = Math.max(1000000, this.nanosBetweenLeaks - (now - this.lastLeakNanos));
      long millisToSleep = nanosToSleep / 1000000 + 1;
      Thread.sleep(millisToSleep);
      now = System.nanoTime();
    }
  }

}
