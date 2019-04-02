package ai.eloquent.raft;

import ai.eloquent.util.*;
import ai.eloquent.web.TrackedExecutorService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class RaftLifecycle {

  /**
   * An SLF4J Logger for this class.
   */
  private static final Logger log = LoggerFactory.getLogger(RaftLifecycle.class);

  /**
   * The global singleton lifecycle to use for an actual running instance.
   */
  public static RaftLifecycle global;

  static {
    // Try to create a lifecycle
    String lifecycleClass = System.getProperty("raft.lifecycle");
    if (lifecycleClass == null) {
      lifecycleClass = System.getenv("RAFT_LIFECYCLE");
    }
    if (lifecycleClass == null) {
      lifecycleClass = "ai.eloquent.web.EloquentLifecycle";
    }
    RaftLifecycle lifecycle;
    try {
      Class<?> clazz = Class.forName(lifecycleClass);
      lifecycle = (RaftLifecycle) clazz.getMethod("register").invoke(null);
      log.info("Using lifecycle @ " + lifecycleClass);
    } catch (Throwable t) {
      log.info("Using default Raft lifecycle");
      lifecycle = RaftLifecycle.newBuilder().build();
    }
    RaftLifecycle.global = lifecycle;

    // Set up the shutdown hook
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      // if we're on a CI server (where shutdown doesn't matter)
      if (System.getenv("CI") != null) {
        return;
      }

      log.info(global.logServerNamePrefix()+"-----------------BEGIN SHUTDOWN " + SystemUtils.HOST + "--------------------");

      // 1-11. Run shutdown
      global.shutdown(false);

      // N+1. Shutdown logging
      log.info(global.logServerNamePrefix()+"-----------------END SHUTDOWN " + SystemUtils.HOST + "--------------------");
      Uninterruptably.sleep(1000);  // sleep a while to give everyone a chance to flush if they haven't already
      log.info(global.logServerNamePrefix()+"Done with shutdown");
      log.info(global.logServerNamePrefix()+"-----------------TERMINATION " + SystemUtils.HOST + "--------------------");
    }));
  }


  /**
   * A global timer that we can use.
   */
  public Lazy<SafeTimer> timer;  // TODO(gabor) make this final at some point

  /** The set of managed thread pools */
  protected final Map<String, ExecutorService> managedThreadPools = new HashMap<>();

  /** The set of managed thread pools, which run core system-level things (e.g., Rabbit or the DB connections). */
  protected final Map<String, ExecutorService> coreThreadPools = new HashMap<>();

  /** This is the Theseus that's tied to this RaftLifecycle - if any. We use this knowledge on shutdown. */
  protected Optional<HasRaftLifecycle> registeredRaft = Optional.empty();

  public RaftLifecycle(Lazy<SafeTimer> timer) {
    this.timer = timer;
  }


  /** A critical section, which is allowed to throw an exception. */
  @FunctionalInterface
  public interface CriticalSection {
    /** The block of code to execute, disallowing server shutdown. */
    void execute() throws Exception;
  }


  /**
   * A builder for a lifecycle.
   */
  public static class Builder {
    private boolean mockTimer = false;

    public Builder mockTime() {
      this.mockTimer = true;
      return this;
    }

    public RaftLifecycle build() {
      Lazy<SafeTimer> timer;
      if (this.mockTimer) {
        timer = Lazy.ofSupplier(SafeTimerMock::new);
      } else {
        timer = Lazy.ofSupplier(PreciseSafeTimer::new);
      }
      return new RaftLifecycle(timer);
    }
  }


  /**
   * Create a new lifecycle builder.
   */
  public static Builder newBuilder() {
    return new Builder();
  }


  /**
   * This registers Raft on this RaftLifecycle, so that the RaftLifecycle can shut it down when it's ready.
   */
  public void registerRaft(Theseus raft) {
    this.registeredRaft = Optional.of(raft);
  }

  /**
   * For tests, we can also register a simple Raft node so we don't have to
   * make a whole {@link Theseus} instance.
   */
  void registerRaft(EloquentRaftNode raft) {
    this.registeredRaft = Optional.of(raft);
  }


  /**
   * This creates a helpful prefix for watching the log messages of RaftLifecycle when we have multiple RaftLifecycle objects in
   * the same test.
   */
  public String logServerNamePrefix() {
    return registeredRaft.map(eloquentRaftNode -> eloquentRaftNode.serverName() + " - ").orElse("");
  }


  /**
   * Create a thread pool that closes itself on program shutdown.
   *
   * @param numThreads The number of threads to allocate in the pool
   * @param threadPrefix When naming threads, use this prefix to identify threads in this pool vs. other pools.
   * @param core If true, this is a core system thread pool. Core thread pools are shut down after Rabbit and the database close,
   *             whereas non-core thread pools close with Rabbit and the database still open.
   * @param priority The priority of the threads scheduled on this pool.
   *
   * @return The thread pool created.
   */
  public ExecutorService managedThreadPool(int numThreads, String threadPrefix, boolean core, int priority) {
    if ((core && !coreThreadPools.containsKey(threadPrefix)) || (!core && !managedThreadPools.containsKey(threadPrefix))) {
      ExecutorService service;
      if (numThreads == 1) {
        service = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
            .setNameFormat(threadPrefix + "-%d")
            .setDaemon(true)
            .setUncaughtExceptionHandler((t, e) -> log.warn("Uncaught exception on thread " + t.getName(), e))
            .setPriority(priority)
            .build());
      } else {
        service = Executors.newFixedThreadPool(numThreads, new ThreadFactoryBuilder().setNameFormat(threadPrefix + "-%d").setDaemon(true).build());
      }
      if (this == RaftLifecycle.global) {
        service = new TrackedExecutorService(threadPrefix, service);
      }
      if (core) {
        coreThreadPools.put(threadPrefix, service);
      } else {
        managedThreadPools.put(threadPrefix, service);
      }
    } else {
      log.warn(logServerNamePrefix() + "Getting a thread pool that already exists for \"" + threadPrefix + "\", but asking for a pool with a " +
          "fixed size = " + numThreads + ". This will likely lead to trouble as multiple people each think they have " +
          "exclusive access to " + numThreads + " threads, when in fact they do not", new IllegalStateException());
    }

    if (core) return coreThreadPools.get(threadPrefix);
    else return managedThreadPools.get(threadPrefix);
  }


  /**
   * @see #managedThreadPool(int, String, boolean, int)
   */
  public ExecutorService managedThreadPool(int numThreads, String threadPrefix, boolean core) {
    return managedThreadPool(numThreads, threadPrefix, core, Thread.NORM_PRIORITY);
  }


  /**
   * @see #managedThreadPool(int, String, boolean, int)
   */
  public ExecutorService managedThreadPool(int numThreads, String threadPrefix, int priority) {
    return managedThreadPool(numThreads, threadPrefix, false, priority);
  }


  /**
   * @see #managedThreadPool(int, String, boolean)
   */
  public ExecutorService managedThreadPool(int numThreads, String threadPrefix) {
    return managedThreadPool(numThreads, threadPrefix, false);
  }


  /**
   * Create a managed cached thread pool.
   *
   * @param threadPrefix The prefix to apply to all the threads in the pool
   * @param core If true, this is a core system thread pool. Core thread pools are shut down after Rabbit and the database close,
   *             whereas non-core thread pools close with Rabbit and the database still open.
   * @param priority The priority of the threads scheduled on this pool.
   *
   * @return The thread pool.
   */
  public ExecutorService managedThreadPool(String threadPrefix, boolean core, int priority) {
    if ((core && !coreThreadPools.containsKey(threadPrefix)) || (!core && !managedThreadPools.containsKey(threadPrefix))) {
      ExecutorService service = Executors.newCachedThreadPool(
          new ThreadFactoryBuilder()
              .setNameFormat(threadPrefix + "-%d")
              .setDaemon(true)
              .setUncaughtExceptionHandler((t, e) -> log.warn("Uncaught exception on thread " + t.getName(), e))
              .setPriority(priority)
              .build());
      if (this == RaftLifecycle.global) {
        service = new TrackedExecutorService(threadPrefix, service);
      }
      if (core) {
        coreThreadPools.put(threadPrefix, service);
      } else {
        managedThreadPools.put(threadPrefix, service);
      }
    }

    if (core) {
      return coreThreadPools.get(threadPrefix);
    } else {
      return managedThreadPools.get(threadPrefix);
    }
  }


  /**
   * @see #managedThreadPool(String, boolean, int)
   */
  public ExecutorService managedThreadPool(String threadPrefix, boolean core) {
    return managedThreadPool(threadPrefix, core, Thread.NORM_PRIORITY);
  }


  /**
   * Create a managed cached thread pool.
   *
   * @param threadPrefix The prefix to apply to all the threads in the pool
   *
   * @return The thread pool.
   */
  public ExecutorService managedThreadPool(String threadPrefix) {
    return managedThreadPool(threadPrefix, false, Thread.NORM_PRIORITY);
  }


  /**
   * Create a managed cached thread pool with a given priority.
   *
   * @param threadPrefix The prefix to apply to all the threads in the pool
   * @param priority The priority of the threads scheduled on this pool.
   *
   * @return The thread pool.
   */
  public ExecutorService managedThreadPool(String threadPrefix, int priority) {
    return managedThreadPool(threadPrefix, false, priority);
  }


  /** Add a shutdown hook to the end of the list. It will be run last. */
  public void addShutdownHook(Runnable hook) {
    synchronized (shutdownHooks) {
      if (IS_SHUTTING_DOWN.get()) {
        log.warn(logServerNamePrefix()+"Got shutdown hook while shutting down. Running immediately");
        hook.run();
      }
      else {
        shutdownHooks.add(hook);
      }
    }
  }


  /**
   * Remove a shutdown hook.
   *
   * @param hook The hook to remove. This must be identical (==) to a hook that was added
   *             with {@link RaftLifecycle#addShutdownHook(Runnable)} or
   *
   * @return True if we found the shutdown hook and removed it.
   */
  public boolean removeShutdownHook(Runnable hook) {
    synchronized (shutdownHooks) {
      return shutdownHooks.remove(hook);
    }
  }


  /**
   * This replaces the global timer with a mock, if useMock is true, or recreates a timer with a SafeTimerReal.
   */
  public void useTimerMock(boolean useMock) {
    if (useMock) {
      timer = Lazy.ofSupplier(SafeTimerMock::new);
    } else {
      timer = Lazy.ofSupplier(PreciseSafeTimer::new);
    }
  }


  /**
   * Register a block of code as a critical section.
   * This prevents this code from being interrupted by the system shutting down.
   * The system will wait for this code to finish when it is shutting down.
   * Of course, this is no protection against SIGKILL, only SIGTERM.
   *
   * @param fn The code to run protected from shutdown.
   */
  public void criticalSection(CriticalSection fn) throws Exception {
    ReentrantLock lock = new ReentrantLock();
    // 1. Lock the lock
    lock.lock();
    boolean haveUnlocked = false;
    try {
      // 2. Register the critical section
      //    This must be after locking the lock, to prevent race conditions
      synchronized (criticalSections) {
        if (allowCriticalSections) {
          criticalSections.add(lock);
        } else {
          // In this case, we are already shutting down, and should not allow the critical section
          // to execute.
          lock.unlock();
          haveUnlocked = true;
          throw new IllegalStateException("We are in the process of shutting down -- not entering critical section");
        }
      }
      // 3. Run the critical section
      try {
        fn.execute();
      } finally {
        // 4. Release the lock, in case anyone is waiting on it.
        //    At this point, it doesn't matter if we remove or unlock first,
        //    we just have to make sure to unlock the lock at some point
        lock.unlock();
        synchronized (criticalSections) {
          haveUnlocked = true;
          criticalSections.remove(lock);
        }
      }
    } finally {
      if (!haveUnlocked) {
        lock.unlock();
      }
    }
  }


  // Stuff to shut down elegantly
  /** The set of shutdown hooks we should run on shutdown, once we are no longer receiving traffic */
  protected final Set<Runnable> shutdownHooks = Collections.newSetFromMap(new IdentityHashMap<>());

  /** The indicator for whether our web server is accepting new requests. */
  public final AtomicBoolean IS_READY = new AtomicBoolean(false);
  /**
   * This indicator gets set when we are shutting down, and should not allow new requests to proceed.
   * This is subtly different from {@link RaftLifecycle#IS_READY}, in that Kubernetes can send new requests
   * between when {@link RaftLifecycle#IS_READY} is marked false, and when its timeout on readiness checks
   * expires.
   * In contrast, this variable is only set once Kubernetes should no longer be sending us new requests.
   * Any new requests that are sent should be considered to be an error.
   */
  public final AtomicBoolean IS_SHUTTING_DOWN = new AtomicBoolean(false);

  /**
   * This indicates when we have started a shutdown.
   * As distinct from {@link #IS_READY} (indicating the Kubernetes readiness state),
   * and {@link #IS_SHUTTING_DOWN} (indicating that readiness is down and we're
   */
  public final AtomicBoolean SHUTDOWN_BEGIN = new AtomicBoolean(false);

  /**
   * This indicates when we have closed the core thread pools, one of the last steps right before shutdown. This is here
   * so that mocks that simulate multiple EloquentLifecycle objects on the same VM can stop performing actions that would have
   * executed on a core thread pool.
   */
  public final AtomicBoolean CORE_THREAD_POOLS_CLOSED = new AtomicBoolean(false);


  /** The interval in which liveness and readiness checks are sent to our server. This must match deployment.yaml. */
  public static final int STATUS_PERIOD_SEC = 2;
  /** The number of readiness checks we have to fail for us to be considered dead by Kuberneters. This must match deployment.yaml. */
  public static final int STATUS_FAILURE_THRESHOLD = 3;
  /** The timeout on the ready endpoint. This should match deployment.yaml. */
  public static final int STATUS_TIMEOUT_SEC = 5;

  /**
   * A lock we can take to prevent the system from shutting down.
   * This can be used to, e.g., create critical sections of code that cannot be
   * interrupted by the system shutting down.
   */
  protected final Set<ReentrantLock> criticalSections = new IdentityHashSet<>();

  /**
   * If false, throw an exception rather than entering the critical section.
   * This is the case when we are already in the process of shutting down and we attempt
   * to enter a new critical section.
   */
  protected boolean allowCriticalSections = true;


  /**
   * This is called from the shutdown hooks, but also can be called from within tests to simulate a shutdown for a
   * single EloquentLifecycle
   *
   * @param allowClusterDeath If true, allow the Raft cluster to lose state and completely shut down.
   *                          Otherwise, we wait for another live node to show up before shutting
   *                          down (the default).
   */
  @SuppressWarnings("Duplicates")
  public void shutdown(boolean allowClusterDeath) {
    if (SHUTDOWN_BEGIN.getAndSet(true)) {
      // If we're already shutting down somewhere else, prevent a duplicate shutdown
      log.warn(logServerNamePrefix()+"Detected an attempt to double-shutdown. Ignoring.");
      return;
    }

    // 1. Run a GC (concurrently)
    Thread gcThread = new Thread( () -> {
      log.info(global.logServerNamePrefix()+"Memory (pre-gc):  free=" + Runtime.getRuntime().freeMemory() / (1024 * 1024) + "MB  total=" + Runtime.getRuntime().totalMemory() / (1024 * 1024) + "MB  max=" + Runtime.getRuntime().maxMemory() / (1024 * 1024) + "MB");
      Runtime.getRuntime().gc();  // Run an explicit GC
      log.info(global.logServerNamePrefix()+"Memory (post-gc): free=" + Runtime.getRuntime().freeMemory() / (1024 * 1024) + "MB  total=" + Runtime.getRuntime().totalMemory() / (1024 * 1024) + "MB  max=" + Runtime.getRuntime().maxMemory() / (1024 * 1024) + "MB");
    });
    gcThread.setDaemon(true);
    gcThread.setName("shutdown-gc");
    gcThread.start();

    // 2. Bring readiness down and wait
    if (IS_READY.getAndSet(false)) {
      long msToWait = ((RaftLifecycle.STATUS_PERIOD_SEC + RaftLifecycle.STATUS_TIMEOUT_SEC) * 1000 * STATUS_FAILURE_THRESHOLD) + 2000;  // +2s for good measure
      if ("true".equals(System.getenv("ELOQUENT_LOCAL"))) {
        msToWait = 0L;
      }
      log.info(logServerNamePrefix()+"Waiting " + TimerUtils.formatTimeDifference(msToWait) + " before shutting down to let Kubernetes detect we're not READY...");
      Uninterruptably.sleep(msToWait);
    }

    // -- After this point, Kubernetes should not be sending us any more requests. We are just shutting down gracefully.
    // -- We also don't necessarily have an accessible IP address anymore; other boxes (and the public) may not see us.
    IS_SHUTTING_DOWN.set(true);
    // 3. Disallow requests + kill Jetty
    // 3.1. Disallow requests
    synchronized (criticalSections) {
      allowCriticalSections = false;
    }

    // 4. Run shutdown hooks
    log.info(logServerNamePrefix()+"Running shutdown hooks (1 minute timeout on each)");
    Set<Runnable> hooks;
    synchronized (shutdownHooks) {
      hooks = new HashSet<>(shutdownHooks);
    }
    hooks.stream().map(task -> {
      log.info(logServerNamePrefix()+"Starting shutdown task {}", task.getClass());
      Thread t = new Thread(task);
      t.setName("shutdown-hook");
      t.start();
      return Pair.makePair(t, task.getClass());
    }).collect(Collectors.toList()).forEach(pair -> {
      try {
        pair.first.join(Duration.ofMinutes(1).toMillis());
        log.info(logServerNamePrefix()+"Joined shutdown task {}", pair.second);
      } catch (InterruptedException e) {
        log.warn(logServerNamePrefix()+"Shutdown hook got interrupted before it could finish!");
      }
    });

    // 5. Await critical sections
    log.info(logServerNamePrefix()+"Waiting on critical sections to finish (max 1 minute)...");
    Set<ReentrantLock> criticalSectionsToAwait;
    synchronized (criticalSections) {
      allowCriticalSections = false;  // redundant with call at top of shutdown hook
      criticalSectionsToAwait = new IdentityHashSet<>(criticalSections);
    }
    criticalSectionsToAwait.forEach(x -> {
      try {
        x.tryLock(Duration.ofMinutes(1).toMillis(), TimeUnit.MILLISECONDS);  // allow 1 minute for each critical section to finish
      } catch (InterruptedException ignored) {}
    });

    // 6. Run a final GC
    log.info(logServerNamePrefix()+"Finalizing as much as we can (connections are still alive)");
    try {
      gcThread.join(30000);
    } catch (InterruptedException e) {
      log.warn("shutdown GC thread interrupted before completion");
    }
    System.runFinalization();

    // 7. Shutdown our RAFT cluster, if we have one, and block until shutdown is complete
    if (this.registeredRaft.isPresent()) {
      HasRaftLifecycle raft = this.registeredRaft.get();
      log.info(logServerNamePrefix() + "Shutting down raft (blocking={})...", !allowClusterDeath);
      raft.close(allowClusterDeath);  // wait for someone else to come online to carry on the state (if we have a raft at all)
      log.info(logServerNamePrefix() + "Raft shut down");
    } else {
      log.warn("No Raft registered -- not shutting down Raft.");
    }

    // 8. Cancel the timers
    log.info(logServerNamePrefix()+"Cancelling timers");
    timer.getIfPresent().ifPresent(SafeTimer::cancel);
    log.info(logServerNamePrefix()+"Timers cancelled");

    // 9. Shutdown all the managed thread pools, and wait a max of 1 min for everything to shut down
    log.info(logServerNamePrefix()+"Stopping non-essential thread pools");
    stopPool(managedThreadPools.values());
    log.info(logServerNamePrefix()+"All non-essential threads should be stopped");

    // 10. Shutdown core thread pools
    log.info(logServerNamePrefix()+"Stopping core thread pools");
    stopPool(coreThreadPools.values());
    log.info(logServerNamePrefix()+"All core thread pools should be stopped");

    CORE_THREAD_POOLS_CLOSED.set(true);

    // 11. Finalize everything again, to clean up connections
    log.info(logServerNamePrefix()+"Finalizing as much as we can (connections are dead)");
    System.runFinalization();
  }


  /**
   * Shut down a thread pool collection.
   * This is a common method for stopping {@link #managedThreadPools} and
   * {@link #coreThreadPools}.
   *
   * @param pool The thread pool collection to stop.
   */
  protected void stopPool(Collection<ExecutorService> pool) {
    pool.stream().map((service) -> {
      service.shutdown();
      Thread awaitTermination = new Thread(() -> {
        try {
          long start = System.currentTimeMillis();
          service.awaitTermination(1, TimeUnit.MINUTES);
          if (System.currentTimeMillis() - start > 1000 * 55) {
            log.warn(logServerNamePrefix()+"Service took >55s to shut down: {}", service);
          }
        } catch (InterruptedException ignored) {}
      });
      awaitTermination.setDaemon(true);
      awaitTermination.setName("waiting for " + service + " to terminate");
      awaitTermination.start();
      return awaitTermination;
    }).forEach(thread -> {
      try {
        thread.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    });
    log.info(logServerNamePrefix()+"pools stopped");
  }


}
