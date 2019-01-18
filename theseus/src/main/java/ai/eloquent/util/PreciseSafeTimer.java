package ai.eloquent.util;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

/**
 * A Safe Timer backed by a {@link java.util.concurrent.ScheduledExecutorService},
 * which is more precise than a simple {@link java.util.Timer}.
 *
 * @author <a href="mailto:gabor@eloquent.ai">Gabor Angeli</a>
 */
@SuppressWarnings("Duplicates")
public class PreciseSafeTimer implements SafeTimer {
  /**
   * An SLF4J Logger for this class.
   */
  private static final Logger log = LoggerFactory.getLogger(PreciseSafeTimer.class);

  /**
   * The implementing service
   */
  private final ScheduledExecutorService service;

  /**
   * If true, this timer is still alive and kicking.
   */
  private boolean alive = true;


  /**
   * Create a new precise safe timer
   */
  public PreciseSafeTimer() {
    this.service = new ScheduledThreadPoolExecutor(4, new ThreadFactoryBuilder()
        .setNameFormat("precise-scheduler-%d")
        .setDaemon(true)
        .setUncaughtExceptionHandler((t, e) -> log.warn("Uncaught exception on thread " + t.getName(), e))
        .setPriority(Math.max(Thread.NORM_PRIORITY, Thread.MAX_PRIORITY - 1))
        .build(),
        (r, executor) -> log.warn("Could not execute runnable {} on executor", r));
  }


  /**
   * Run a timer task directly on the given thread, but catch all the resulting exceptions.
   *
   * @param task The task to run.
   *
   * @return An exception-proof wrapped Runnable for the task.
   */
  private static Runnable catchExceptions(SafeTimerTask task) {
    return () -> {
      try {
        // note[gabor]: the use of runUnsafe() is deliberate here -- the scheduler already schedules tasks on threads.
        task.runUnsafe();
      } catch (Throwable e) {
        log.warn("SafeTimerTask caught a thrown exception.", e);
      }
    };
  }


  /** {@inheritDoc} */
  @Override
  public long now() {
    return System.currentTimeMillis();
  }

  /** {@inheritDoc} */
  @Override
  public void schedule(SafeTimerTask task, long delay) {
    if (!this.alive) {
      log.warn("Timer is cancelled; not queuing task", new RuntimeException());
      return;
    }
    this.service.schedule(catchExceptions(task), delay, TimeUnit.MILLISECONDS);
  }

  /** {@inheritDoc} */
  @Override
  public void schedule(SafeTimerTask task, long delay, long period) {
    if (!this.alive) {
      log.warn("Timer is cancelled; not queuing task", new RuntimeException());
      return;
    }
    this.service.scheduleAtFixedRate(catchExceptions(task), delay, period, TimeUnit.MILLISECONDS);
  }

  /** {@inheritDoc} */
  @Override
  public void scheduleAtFixedRate(SafeTimerTask task, long delay, long period) {
    if (!this.alive) {
      log.warn("Timer is cancelled; not queuing task", new RuntimeException());
      return;
    }
    this.service.scheduleAtFixedRate(catchExceptions(task), delay, period, TimeUnit.MILLISECONDS);
  }

  /** {@inheritDoc} */
  @Override
  public void scheduleAtFixedRate(SafeTimerTask task, long period) {
    if (!this.alive) {
      log.warn("Timer is cancelled; not queuing task", new RuntimeException());
      return;
    }
    this.service.scheduleAtFixedRate(catchExceptions(task), 0, period, TimeUnit.MILLISECONDS);
  }

  /** {@inheritDoc} */
  @Override
  public void cancel() {
    if (this.alive) {
      this.alive = false;
      this.service.shutdown();
    } else {
      log.warn("Ignoring duplicate timer cancel");
    }
  }
}
