package ai.eloquent.util;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;


/**
 * A test for {@link SafeTimerTask}.
 *
 * <p>
 *   note[gabor]: this test seems to be flaky on CI (and CI alone) for reasons that boggle
 *   my mind, so I've ignored it. My latest [failed] attempt at fixing it was in #150:
 *   https://jarvis.hybridcrowd.io/eng/eloquent/issues/150
 * </p>
 */
@Ignore
@SuppressWarnings("SynchronizeOnNonFinalField")
public class SafeTimerTaskTest {


  /**
   * A crashing {@link ExecutorService} that simply throws an exception whenever anything happens to it,
   * at least with certain probability
   */
  private static class CrashingPool implements ExecutorService {

    private ExecutorService impl = Executors.newFixedThreadPool(4);
    private final double crashProb;
    private final Random random = new Random(42L);

    private CrashingPool(double crashProb) {
      this.crashProb = crashProb;
    }

    /** {@inheritDoc} */
    @Override
    public void shutdown() {
      impl.shutdown();
    }

    /** {@inheritDoc} */
    @Nonnull
    @Override
    public List<Runnable> shutdownNow() {
      return impl.shutdownNow();
    }

    /** {@inheritDoc} */
    @Override
    public boolean isShutdown() {
      return impl.isShutdown();
    }

    /** {@inheritDoc} */
    @Override
    public boolean isTerminated() {
      return impl.isTerminated();
    }

    /** {@inheritDoc} */
    @Override
    public boolean awaitTermination(long timeout, @Nonnull TimeUnit unit) throws InterruptedException {
      return impl.awaitTermination(timeout, unit);
    }

    /** {@inheritDoc} */
    @Nonnull
    @Override
    public <T> Future<T> submit(@Nonnull Callable<T> task) {
      if (this.random.nextDouble() < this.crashProb) { throw new RuntimeException("Random pool crash"); }
      return impl.submit(task);
    }

    /** {@inheritDoc} */
    @Nonnull
    @Override
    public <T> Future<T> submit(@Nonnull Runnable task, T result) {
      if (this.random.nextDouble() < this.crashProb) { throw new RuntimeException("Random pool crash"); }
      return impl.submit(task, result);
    }

    /** {@inheritDoc} */
    @Nonnull
    @Override
    public Future<?> submit(@Nonnull Runnable task) {
      if (this.random.nextDouble() < this.crashProb) { throw new RuntimeException("Random pool crash"); }
      return impl.submit(task);
    }

    /** {@inheritDoc} */
    @Nonnull
    @Override
    public <T> List<Future<T>> invokeAll(@Nonnull Collection<? extends Callable<T>> tasks) throws InterruptedException {
      if (this.random.nextDouble() < this.crashProb) { throw new RuntimeException("Random pool crash"); }
      return impl.invokeAll(tasks);
    }

    /** {@inheritDoc} */
    @Nonnull
    @Override
    public <T> List<Future<T>> invokeAll(@Nonnull Collection<? extends Callable<T>> tasks, long timeout, @Nonnull TimeUnit unit) throws InterruptedException {
      if (this.random.nextDouble() < this.crashProb) { throw new RuntimeException("Random pool crash"); }
      return impl.invokeAll(tasks);
    }

    /** {@inheritDoc} */
    @Nonnull
    @Override
    public <T> T invokeAny(@Nonnull Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
      if (this.random.nextDouble() < this.crashProb) { throw new RuntimeException("Random pool crash"); }
      return impl.invokeAny(tasks);
    }

    /** {@inheritDoc} */
    @Override
    public <T> T invokeAny(@Nonnull Collection<? extends Callable<T>> tasks, long timeout, @Nonnull TimeUnit unit) throws InterruptedException, ExecutionException {
      if (this.random.nextDouble() < this.crashProb) { throw new RuntimeException("Random pool crash"); }
      return impl.invokeAny(tasks);
    }

    /** {@inheritDoc} */
    @Override
    public void execute(@Nonnull Runnable command) {
      if (this.random.nextDouble() < this.crashProb) { throw new RuntimeException("Random pool crash"); }
      impl.execute(command);
    }
  }

  /**
   * A timer task that waits for some fake-time and then increments a run count.
   */
  private SafeTimerTask task = new SafeTimerTask() {
    @Override
    public void runUnsafe() throws InterruptedException {
      synchronized (barrier) {
        int iters = 0;
        while (!barrier.get() && iters < 10) {
          barrier.wait(5000);
          iters += 1;
        }
      }
      try {
        if (shouldCrashTask) {
          throw new RuntimeException("Crashing task");
        } else {
          numRuns.incrementAndGet();
        }
      } finally {
        synchronized (numRuns) {
          numRuns.notifyAll();
        }
      }
    }
  };


  /**
   * An executor service we can submit to.
   */
  private ExecutorService pool;

  /**
   * An executor service that always crashes when we do something on it.
   */
  private ExecutorService crashingPool;

  /**
   * The number of times our task has run.
   */
  private AtomicInteger numRuns;

  /**
   * A barrier for the timer to wait on. We are allowed to continue if it's set to true.
   */
  private AtomicBoolean barrier;

  /**
   * If true, we should crash our timer tasks.
   */
  private boolean shouldCrashTask;


  /**
   * Reset the state, just in case.
   */
  @Before
  public void resetState() {
    pool = new CrashingPool(0.0);
    crashingPool = new CrashingPool(1.0);
    numRuns = new AtomicInteger(0);
    barrier = new AtomicBoolean(false);
    shouldCrashTask = false;
  }


  /**
   * Stop all of our pools
   */
  @After
  public void stopPools() {
    pool.shutdown();
    crashingPool.shutdown();
  }


  /**
   * Allow the running timer(s) to continue and increment their run count.
   * This will:
   *
   * <ol>
   *   <li>Wake up the timer task and let it complete, and</li>
   *   <li>Wait for the timer task to complete.</li>
   * </ol>
   *
   * @param numExpectedRuns The number of runs we're expecting to have after waking the timer.
   *
   * @throws InterruptedException Thrown if we could not wait on a condition.
   */
  private void wakeTimer(int numExpectedRuns) throws InterruptedException {
    synchronized (barrier) {
      barrier.set(true);
      barrier.notifyAll();
    }
    Thread.yield();
    synchronized (numRuns) {
      int iters = 0;
      while (numRuns.get() < numExpectedRuns && iters < 10) {
        numRuns.wait(5000);
        iters += 1;
      }
    }
  }


  /**
   * This tests running the same SafeTimerTask multiple times concurrently, and checking that the excess runs get
   * dropped on the floor.
   */
  @Test
  public synchronized void testMaxSimultaneousRunsExceeded() throws InterruptedException {
    // 1. Run the task
    assertEquals("We should have no runs in the beginning", 0, numRuns.get());
    task.run(pool);
    // 2. Run the task again immediately (should be dropped)
    assertEquals("We should not have run the first task yet", 0, numRuns.get());
    task.run(pool);
    assertEquals("Still no runs should have run", 0, numRuns.get());
    // 3. Wait for the all task to finish
    wakeTimer(1);
    // 4. Check that only one task has run
    assertEquals("The first, but only the first task should have run", 1, numRuns.get());
    // 5. Run another task
    task.run(pool);
    // 6. Wait for this third task to finish
    wakeTimer(2);
    // 7. Make sure two tasks have run total
    assertEquals("The first and third tasks should have run", 2, numRuns.get());
  }


  /**
   * This tests that we can continue submitting tasks even if the pool submit call exceptions
   */
  @Test
  public synchronized void dontChokeIfPoolFails() throws InterruptedException {
    // 1. Run the task (on a crashing pool)
    assertEquals("We should have no runs in the beginning", 0, numRuns.get());
    task.run(crashingPool);
    wakeTimer(0);
    // 2. Run the task again on a non-crashing pool
    assertEquals("We should have no runs, because the pool crashed", 0, numRuns.get());
    task.run(pool);
    // 3. Wait for the all task to finish
    wakeTimer(1);
    // 4. Check that a task has run
    assertEquals("The second task should have run, despite the pool rejecting the first", 1, numRuns.get());
  }


  /**
   * This tests that we can continue submitting tasks even if a task exceptions
   */
  @Test
  public synchronized void dontChokeIfTaskFails() throws InterruptedException {
    // 1. Run the task (on a crashing task)
    assertEquals("We should have no runs in the beginning", 0, numRuns.get());
    shouldCrashTask = true;
    task.run(pool);
    wakeTimer(0);
    // 2. Run a non-crashing task
    assertEquals("We should have no runs, because the task crashed", 0, numRuns.get());
    shouldCrashTask = false;
    task.run(pool);
    // 3. Wait for the all task to finish
    wakeTimer(1);
    // 4. Check that a task has run
    assertEquals("The second task should have run, despite the first task exceptioning", 1, numRuns.get());
  }


  /**
   * This tests running the same SafeTimerTask multiple times concurrently, and checking that the excess runs get
   * dropped on the floor.
   */
  @Test
  public synchronized void dontChokeIfPoolClosed() throws InterruptedException {
    // 1. Run a task
    assertEquals("We should have no runs in the beginning", 0, numRuns.get());
    task.run(pool);
    wakeTimer(1);
    assertEquals("We should have run a task", 1, numRuns.get());
    // 2. Stop the pool
    pool.shutdown();
    // 3. Run a task
    task.run(pool);
    wakeTimer(1);
    assertEquals("We should not have run the task on the shutdown pool", 1, numRuns.get());
    // 3. Run a task on an alive pool
    ExecutorService service = Executors.newFixedThreadPool(4);
    try {
      task.run(service);
      wakeTimer(2);
      assertEquals("We should still be able to submit tasks", 2, numRuns.get());
    } finally {
      service.shutdown();
    }
  }
}