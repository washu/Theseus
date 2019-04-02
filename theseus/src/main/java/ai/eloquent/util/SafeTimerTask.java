package ai.eloquent.util;

import ai.eloquent.raft.RaftLifecycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.TimerTask;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * All TimerTask objects crash their parent thread when they throw an exception, so we wrap timers in SafeTimerTask
 * objects, which don't.
 */

// NOTE[gabor]: If you change this class, also re-run SafeTimerTaskTest. The test is flaky on CI, but runs reliably locally. See #150
public abstract class SafeTimerTask extends TimerTask {
  /**
   * An SLF4J Logger for this class.
   */
  private static final Logger log = LoggerFactory.getLogger(SafeTimerTask.class);

  /**
   * A thread pool for our timer tasks.
   */
  private static final Lazy<ExecutorService> POOL = Lazy.ofSupplier(() -> RaftLifecycle.global.managedThreadPool("safetimer"));

  /**
   * If true, this task has been cancelled.
   */
  public boolean cancelled = false;

  /**
   * A set of functions called when we cancel this timer task.
   */
  private final List<Runnable> onCancelCallbacks = new ArrayList<>();

  /**
   * This is the count of runs of this timertask that are simultaneously processing
   */
  private AtomicInteger runningCount = new AtomicInteger(0);

  /**
   * This is the maximum number of simultaneous runs of this task that we'll allow. Any runs beyond this count will be
   * dropped on the floor, and will issue an error.
   */
  public static final int MAX_SIMULTANEOUS_RUNS = 1;


  /** {@inheritDoc} */
  @Override
  public void run() {
    run(Optional.ofNullable(pool()));
  }

  /**
   * Run this job on an optional pool.
   * @param pool The executor pool to run on.
   */
  public void run(Optional<Executor> pool) {
    if (this.cancelled) {
      return;
    }
    if (!pool.isPresent()) {
      try {
        runUnsafe();
      } catch (Throwable e) {
        // We don't rethrow the exception, because that kills the global timer
        log.warn("SafeTimerTask caught a thrown exception.", e);
      }
    } else {
      Executor executor = pool.get();
      try {
        if (!(executor instanceof ExecutorService) ||
            (!((ExecutorService) executor).isTerminated() && !((ExecutorService) executor).isShutdown())) {
          if (runningCount.incrementAndGet() <= MAX_SIMULTANEOUS_RUNS) {
            try {
              pool.get().execute(() -> {
                try {
                  runUnsafe();
                } catch (Throwable e) {
                  // We don't rethrow the exception, because that kills the global timer
                  log.warn("SafeTimerTask caught a thrown exception.", e);
                } finally {
                  runningCount.decrementAndGet();
                }
              });
            } catch (Throwable t) {
              runningCount.decrementAndGet();
              log.warn("Could not submit timer task to pool: ", t);
            }
          } else {
            int numberRunning = runningCount.decrementAndGet();
            if (numberRunning >= 5) {
              log.warn("SafeTimerTask is not running request, because we already have {} running", numberRunning);
            } else {
              log.debug("SafeTimerTask is not running request, because we already have {} running", numberRunning);
            }
          }
        } else {
          log.warn("Trying to run a task even though our managedThreadPool for running tasks has been shut down.");
        }
      } catch (Throwable t) {
        // We don't rethrow the exception, because that kills the global timer
        log.warn("SafeTimerTask caught an exception when starting a thread to execute an event.", t);
      }
    }
  }


  /**
   * Run this task on the given pool. An alias for {@link #run(Optional)}.
   *
   * @param pool The pool to run on.
   */
  public void run(Executor pool) {
    this.run(Optional.of(pool));
  }


  /**
   * Sometimes we want to be notified when things cancel.
   */
  public void registerCancelCallback(Runnable runnable) {
    this.onCancelCallbacks.add(runnable);
  }


  /** {@inheritDoc} */
  @Override
  public boolean cancel() {
    boolean retVal = super.cancel();
    for (Runnable callbacks: onCancelCallbacks) {
      callbacks.run();
    }
    cancelled = true;  // always cancel
    return retVal;
  }


  /**
   * An overwritable function for getting the pool that we should run this timer's tasks on.
   */
  protected ExecutorService pool() {
    return POOL.get();
  }


  /**
   * The implementation of the task, except that we are allowed to throw arbitrary exceptions.
   *
   * @throws Throwable An exception that this runner is allowed to throw.
   */
  public abstract void runUnsafe() throws Throwable;
}
