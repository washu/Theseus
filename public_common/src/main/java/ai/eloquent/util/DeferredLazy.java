package ai.eloquent.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * A supplier that computes a value asynchronously, returning {@link Optional#empty()}
 * until it's available.
 *
 * Optionally, it can also invalidate an object with {@link #isValid(Object, long, long)} and start recomputing it
 * from scratch.
 *
 * @author <a href="mailto:gabor@eloquent.ai">Gabor Angeli</a>
 */
public abstract class DeferredLazy<E> implements Supplier<Optional<E>> {
  /**
   * An SLF4J Logger for this class.
   */
  private static final Logger log = LoggerFactory.getLogger(DeferredLazy.class);


  /**
   * If false, recompute the value of this object -- again, asynchronously.
   *
   * @param value The value we are checking.
   * @param lastComputeTime The time at which we last computed this value.
   *                        This is initialized to {@link Integer#MIN_VALUE}.
   * @param callTime The time at which we requested the value we're computing, as a timestamp
   *                 in milliseconds since the epoch.
   *
   * @return True if this value is still ok.
   */
  protected boolean isValid(E value, long lastComputeTime, long callTime) {
    return true;
  }


  /**
   * A function that's called when we discard a value. This is useful for, e.g.,
   * closing connections that may be associated with the value.
   *
   * @param value The value we're discarding.
   */
  protected void onDiscard(E value) throws Exception {
    /* a noop by default */
  }


  /**
   * Compute the value we should return. Called asynchronously from {@link #get()} when either
   * no value exists, or when {@link #isValid(Object, long, long)} returns false.
   *
   * @return The object we are creating.
   */
  protected abstract E compute() throws Exception;


  /**
   * Our value that we're caching.
   */
  private @Nullable E value = null;


  /**
   * If true, we are in the middle of computing our value.
   */
  private final AtomicBoolean computing = new AtomicBoolean(false);


  /**
   * The last time we computed this value.
   * This is initialized to {@link Integer#MIN_VALUE}
   */
  private long lastComputeTime = Integer.MIN_VALUE;


  /**
   * Get the value of this lazy, if it exists, and recompute it if it doesn't or if it is invalid.
   *
   * @param forceRefresh If true, force a refresh
   * @param now The current time.
   *
   * @return The value we are getting, if it present. Otherwise, {@link Optional#empty()}.
   */
  public Optional<E> get(boolean forceRefresh, long now) {
    boolean isValid = this.isValid(now);
    if ((forceRefresh || !isValid) && !computing.getAndSet(true)) {
      // Case: we need a recompute
      // 1. Start the thread
      // 1.1. Set the last compute time
      lastComputeTime = now;
      Thread getter = new Thread( () -> {
        E oldValue = value;
        try {
          // 1.2. Compute the new value
          value = compute();
        } catch (Exception e) {
          log.warn("Could not compute value for DeferredLazy: ", e);
        } finally {
          // 1.3. Mark that we're done computing
          computing.getAndSet(false);
          // 1.4. Notify any waiting threads
          synchronized (DeferredLazy.this) {
            DeferredLazy.this.notifyAll();
          }
          // 1.5. Discard our old value, if applicable
          try {
            if (oldValue != null) {
              onDiscard(oldValue);
            }
          } catch (Exception e) {
            log.warn("Caught exception while discarding a deferred value", e);
          }
        }
      } );
      getter.setName("deferred-lazy");
      getter.setPriority(Thread.NORM_PRIORITY);
      getter.setDaemon(true);
      getter.setUncaughtExceptionHandler((t, e) -> log.warn("Uncaught exception on {}: ", t, e));
      getter.start();

      // 2. Return the current value
      if (forceRefresh && isValid) {
        return Optional.ofNullable(value);  // still return the old value, but refresh in the background.
      } else {
        return Optional.empty();
      }
    } else {
      // Case: we have our value already
      return Optional.ofNullable(value);
    }
  }


  /** {@inheritDoc} */
  public Optional<E> get(boolean forceRefresh) {
    return get(forceRefresh, System.currentTimeMillis());
  }


  /** {@inheritDoc} */
  @Override
  public Optional<E> get() {
    return get(false);
  }


  /**
   * Get the value of this deferred lazy synchronously.
   *
   * @param forceRefresh If true, force a refresh.
   * @param now The current timestamp
   *
   * @return The value we're computing in the Lazy.
   */
  public E getSync(boolean forceRefresh, long now) {
    get(forceRefresh, now);
    synchronized (this) {
      while (value == null || !isValid(value, lastComputeTime, now) || computing.get()) {
        try {
          this.wait(100);
        } catch (InterruptedException e) {
          throw new RuntimeInterruptedException(e);
        }
      }
    }
    return value;
  }


  /**
   * @see #getSync(boolean, long)
   */
  public E getSync(boolean forceRefresh) {
    return getSync(forceRefresh, System.currentTimeMillis());
  }


  /**
   * @see #getSync(boolean)
   */
  public E getSync() {
    return getSync(false);
  }


  /**
   * If true, this value is currently valid and can be retrieved.
   * Note that this may be time-sensitive, and therefore a call to get() with a later time
   * will not necessarily return a value, even if this returned true.
   *
   * @param now The current time.
   * @return If true, the value is valid at this time.
   */
  public boolean isValid(long now) {
    return (value != null && isValid(value, lastComputeTime, now));
  }
}
