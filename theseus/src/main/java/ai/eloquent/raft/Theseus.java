package ai.eloquent.raft;

import ai.eloquent.error.RaftErrorListener;
import ai.eloquent.monitoring.Prometheus;
import ai.eloquent.util.Lazy;
import ai.eloquent.util.SafeTimerTask;
import ai.eloquent.util.TimerUtils;
import ai.eloquent.web.TrackedExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * This is the high-level user-facing API for RAFT. It's designed to appear simple to end-users. The key reason we need
 * this as opposed to an off-the-shelf implementation is that we want it to be embedded in an application that runs on
 * Kubernetes, and that imposes a key difference in assumptions over the original RAFT algorithm: boxes that fail will
 * be replaced, not rebooted. We want this to be embedded so that our implementation for distributed locks will
 * automatically release the lock of a box that disconnects from the cluster.
 *
 * Our core design goals:
 *
 * - Use EloquentChannel as the transport layer, to avoid flaky networking on Kubernetes
 * - Don't persist state to disk, since boxes that fail will be replaced, not rebooted.
 * - Keep it simple
 *
 */
public class Theseus implements HasRaftLifecycle {
  /**
   * An SLF4J Logger for this class.
   */
  static final Logger log = LoggerFactory.getLogger(Theseus.class);

  /**
   * A unique counter for, e.g., creating unique hashes.
   */
  private static final AtomicLong UNIQUE_COUNTER = new AtomicLong();

  /**
   * The number of listeners bound to this transport.
   */
  private static final Object COUNTER_CALLS =
      Prometheus.counterBuild("theseus_calls", "The calls that are made to Theseus", "method");

  /**
   * The number of times we failed to acquire a lock.
   */
  private static final Object COUNTER_FAILED_LOCKS =
      Prometheus.counterBuild("theseus_failed_locks", "The number of times we failed to acquire a lock");


  /**
   * The default name for this server, assuming only one Raft is running
   * per box (i.e., IP address)
   */
  private static Lazy<String> DEFAULT_SERVER_NAME = Lazy.of( () -> {
    // 1. Get the server name
    // 1.1. Get the host name, so that we're human readable
    String serverNameBuilder;
    try {
      serverNameBuilder = InetAddress.getLocalHost().toString();
    } catch (UnknownHostException e) {
      log.warn("Could not get InetAddress.getLocalHost() in order to determine Theseus' hostname", e);
      Optional<String> hostname = Optional.ofNullable(System.getenv("HOST"));
      serverNameBuilder = hostname.orElseGet(() -> UUID.randomUUID().toString());
    }
    if (serverNameBuilder.contains("/")) {
      serverNameBuilder = serverNameBuilder.substring(serverNameBuilder.indexOf('/') + 1);
    }
    // 1.1. Append a random ID to avoid conflicts
    serverNameBuilder += "_" + System.currentTimeMillis();
    return serverNameBuilder;
  } );


  /**
   * This holds a lock that is expected to live longer than the duration of a single method call. Usually this lock is
   * associated with a resource that the holder is responsible for. Because forgetting/failing to release long lived
   * locks can be catastrophic, this class packages up a number of safegaurds to ensure that the lock does eventually
   * get released. Since locks auto-release when their owner disconnects from the cluster, this can safely be a part of
   * Theseus, since as soon as Theseus closes these locks no longer need cleaning up anyways.
   */
  public interface LongLivedLock extends AutoCloseable {
    /**
     * Returns the name of the lock that is held.
     */
    String lockName();
    /**
     * Returns true if the lock represented is still certainly held.
     * If this is true, {@link #isPerhapsHeld()} is always true as well, but not
     * visa versa.
     */
    boolean isCertainlyHeld();
    /**
     * Returns true if the lock represented has a chance of being held.
     * This can be true even when {@link #isCertainlyHeld()} is false, in cases where we are in the process
     * of releasing the lock.
     * Note that unlike {@link #isCertainlyHeld()}, this may never revert to false in rare cases when we cannot talk
     * to Raft effectively. Therefore, the caller should be wary of waiting on this function.
     */
    boolean isPerhapsHeld();
    /**
     * This releases a lock, and cleans up any resources waiting on it. Calling this more than once is a no-op.
     */
    CompletableFuture<Boolean> release();

    /** {@inheritDoc} */
    @Override
    default void close() throws Exception {
      release().get();
    }
  }


  /**
   * The reason this is broken out from the interface is so that it is possible to mock LongLivedLock objects in the
   * RaftManagerMock.
   */
  private class LongLivedLockImpl implements LongLivedLock {
    /** The name of the lock */
    public final String lockName;
    /** A unique hash for this lock, to disambiguate this instance from other lock request instances of the same name. */
    public final String uniqueHash;
    /** The window after which we should release the lock no matter what, as a last ditch on deadlocks. */
    public final Duration safetyReleaseWindow;

    /** If true, we currently hold this lock. This is an optimistic boolean -- we may no longer hold it technically. */
    private boolean held = true;
    /** If true, we want to hold this lock. If false, we are in the process of releasing it. */
    private boolean wantToHold = true;

    public final SafeTimerTask cleanupTimerTask;

    /**
     * This creates a LongLivedLock object which will automatically clean itself up in the event of catastrophic
     * failure.
     *
     * @param lockName the name of the lock
     * @param uniqueHash the unique hash of the lock, to prevent the same machine from getting the same lock multiple
     *                   times.
     * @param safetyReleaseWindow a duration after which we will automatically release the lock, if it hasn't been
     *                            released by some other safety mechanism.
     */
    protected LongLivedLockImpl(String lockName, String uniqueHash, Duration safetyReleaseWindow) {
      this.lockName = lockName;
      this.uniqueHash = uniqueHash;
      this.safetyReleaseWindow = safetyReleaseWindow;

      cleanupTimerTask = new LockCleanupTimerTask(this);
      node.transport.schedule(cleanupTimerTask, safetyReleaseWindow.toMillis());
    }


    /**
     * Returns the name of the lock that is held.
     */
    @Override
    public String lockName() {
      return this.lockName;
    }


    /** {@inheritDoc} */
    @Override
    public boolean isCertainlyHeld() {
      return this.held && this.wantToHold;
    }


    /** {@inheritDoc} */
    @Override
    public boolean isPerhapsHeld() {
      if (!this.held) {
        return false;
      } else if (this.wantToHold) {
        return true;
      } else {
        // This is the case where we may hold the lock, but don't want to.
        // Let's check the state machine for our lock, though this is a bit slow
        KeyValueStateMachine.QueueLock lock = stateMachine.locks.get(this.lockName);
        if (lock == null || lock.holder == null) {
          // Case: there is no lock anymore
          synchronized (this) {
            this.held = false;
          }
        } else {
          // Case: someone else holds the lock
          KeyValueStateMachine.LockRequest holder = lock.holder;
          synchronized (this) {
            this.held = holder.server.equals(serverName) && holder.uniqueHash.equals(this.uniqueHash);
          }
        }
        return this.held;
      }
    }


    /**
     * This releases a lock, and cleans up any resources waiting on it. Calling this more than once is a no-op.
     */
    @Override
    public synchronized CompletableFuture<Boolean> release() {
      if (!wantToHold) {
        if (held) {
          log.warn("Double-releasing a lock will have no effect. We see that this lock is currently perhaps held; the only recourse is to wait for the failsafe to release the lock.");
        }
        return CompletableFuture.completedFuture(!held);
      }
      this.wantToHold = false;
      cleanupTimerTask.cancel();
      byte[] transition = KeyValueStateMachine.createReleaseLockTransition(lockName, serverName, uniqueHash);
      return node.submitTransition(transition)  // note[gabor]: don't retry; let the cleanup thread take care of it on
                                                // its own time. This is to prevent cascading release lock requests
                                                // queuing up.
          .whenComplete((success, e) -> {  // note[gabor]: this is not exception-proof; `e` may not be null.
            handleReleaseLockResult(success, e, transition);
          });
    }


    /**
     * This is a safety check to ensure that if a lock gets GC'd, it also gets released
     */
    @Override
    protected void finalize() throws Throwable {
      try {
        super.finalize();
      } finally {
        // Optimization: Don't take the synchronized block from within finalize unless we haven't released yet
        if (held) {
          log.warn("{} - LongLivedLock for \"{}\" is being cleaned up from finalize()! This is very bad!", serverName, lockName);
          synchronized (unreleasedLocks) {
            queueFailedLock(KeyValueStateMachine.createReleaseLockTransition(lockName, serverName, uniqueHash));
          }
          synchronized (this) {
            // Check again inside the synchronized block for if we've released yet
            if (held) {
              release();
            }
          }
        }
      }
    }
  }


  /**
   * This is a little hack of a class to let up have a TimerTask that keeps a weak reference to our LongLivedLock, so
   * that we can rely on the GC as a line of defense despite having the TimerTask outstanding.
   */
  private static class LockCleanupTimerTask extends SafeTimerTask {
    /** The weak reference to our lock */
    WeakReference<LongLivedLock> weakLock;

    public LockCleanupTimerTask(LongLivedLock longLivedLock) {
      weakLock = new WeakReference<>(longLivedLock);
    }

    @Override
    public void runUnsafe() {
      final LongLivedLock lock = weakLock.get();
      // This has already been GC'd, so it's no problem
      if (lock == null) {
        return;
      }
      // Optimization: Don't take the synchronized block from within finalize unless we haven't released yet
      if (lock.isCertainlyHeld()) {
        //noinspection SynchronizationOnLocalVariableOrMethodParameter
        synchronized (lock) {
          // Check again inside the synchronized block for if we've released yet
          if (lock.isCertainlyHeld()) {
            log.warn("LongLivedLock for \"{}\" is being cleaned up from a TimerTask! This is very, very bad! It means we didn't release it, and finalize() never fired.", lock.lockName());
            lock.release();
          }
        }
      }
    }
  }


  /**
   * The name of our node.
   * This can also be gotten from {@link #node}.{@link EloquentRaftNode#algorithm}.{@link EloquentRaftAlgorithm#state()}.{@link RaftState#serverName}.
   * But, from reading that path, you can tell why there's a helper here.
   */
  public final String serverName;

  /**
   * The actual Raft node. This encapuslates a Raft algorithm with a transport.
   */
  public final EloquentRaftNode node;

  /**
   * The state machine we're running over Raft.
   */
  final KeyValueStateMachine stateMachine;

  /**
   * The RaftLifecycle that governs this Theseus
   */
  public final RaftLifecycle lifecycle;

  /**
   * A set of release lock transitions that did not complete in their usual loop -- we should continue to
   * try to release these locks so long as we can, in hopes Raft comes back up sometime.
   */
  // note[gabor]: Package protected to allow for fine-grained testing of the release locks thread
  final List<byte[]> unreleasedLocks = new ArrayList<>();


  /**
   * If false, we have stopped this Raft.
   */
  private boolean alive = true;

  /**
   * An executor pool for async tasks.
   */
  private final ExecutorService pool;

  /**
   * The default timeout for our calls. Large enough that we can weather an election timeout, but small
   * enough that we we return a failure in a reasonable amount of time.
   */
  protected final Duration defaultTimeout;


  /**
   * The constructor takes three arguments: a cluster name (for discovery), a server name (for identifying ourselves,
   * must be unique within the cluster), and a reference to the lifecycle object that governs this Theseus (so that
   * tests can pass different RaftLifecycle objects to different Raft instances).
   *
   * @param algo The Raft algorithm to use. Defaults to {@link EloquentRaftAlgorithm}.
   * @param transport The type of transport to use for this Raft cluster.
   * @param lifecycle The governing RaftLifecycle for this Theseus, so that we can pass mock ones in inside tests
   */
  public Theseus(RaftAlgorithm algo, RaftTransport transport, RaftLifecycle lifecycle) {
    lifecycle.registerRaft(this);

    //
    // I. Set variables
    //
    this.serverName = algo.serverName();
    this.node = new EloquentRaftNode(algo, transport, lifecycle);
    this.node.registerShutdownHook(() -> {
      alive = false;
      // Wake up the lock cleanup thread
      synchronized (unreleasedLocks) {
        unreleasedLocks.notifyAll();
      }
    });
    this.defaultTimeout = Duration.ofMillis(node.algorithm.electionTimeoutMillisRange().end * 5);
    this.stateMachine = (KeyValueStateMachine) algo.mutableStateMachine();
    this.lifecycle = lifecycle;
    this.pool = algo.mutableState().log.pool;

    //
    // II. Create lock cleanup thread
    //
    Thread lockCleanupThread = new Thread(() -> {
      long lastTry = 0;  // not Long#MIN_VALUE so we don't accidentally underflow
      long timeout = node.algorithm.electionTimeoutMillisRange().end * 10;
      while (alive) {
        try {
          // 1. Wait on new unreleased locks
          byte[][] unreleasedLocksCopy;
          synchronized (unreleasedLocks) {
            while (alive && (unreleasedLocks.isEmpty() || (System.currentTimeMillis() - lastTry) < timeout)) {  // we're alive, and either we have no locks or we haven't tried to release recently
              try {
                unreleasedLocks.wait(timeout);  // allow any outstanding election to finish
              } catch (InterruptedException ignored) {}
            }
            unreleasedLocksCopy = unreleasedLocks.toArray(new byte[0][]);
          }
          if (unreleasedLocksCopy.length > 0 &&                      // note[gabor]: only run if we have something to run
              (!alive || this.errors().isEmpty()) &&                 // note[gabor]: only run if we're error free (or shutting down). Otherwise this is a foolish attempt
              this.node.algorithm.mutableState().leader.isPresent()  // note[gabor]: if we have no leader (we're in the middle of an election), we're just asking for pain
          ) {
            // 2. Release the locks
            log.info("Trying to release {} unreleased locks", unreleasedLocksCopy.length);
            byte[] bulkTransition = KeyValueStateMachine.createGroupedTransition(unreleasedLocksCopy);
            Boolean success = exceptionProof(node.submitTransition(bulkTransition))
                .get(node.algorithm.electionTimeoutMillisRange().end * 2, TimeUnit.MILLISECONDS);
            if (success != null && success) {
              // 3.A. Success: stop trying locks
              log.info("Successfully released {} unreleased locks", unreleasedLocksCopy.length);
              synchronized (unreleasedLocks) {
                unreleasedLocks.removeIf(lock -> {
                  for (byte[] justReleased : unreleasedLocksCopy) {
                    if (Arrays.equals(justReleased, lock)) {
                      return true;
                    }
                  }
                  return false;
                });
              }
            } else {
              // 3.B. Failure: signal failure
              log.debug("Could not release {} locks; retrying later.", unreleasedLocksCopy.length);
            }
          }
        } catch (Throwable t) {
          if (t instanceof TimeoutException ||
              (t instanceof ExecutionException && t.getCause() != null && t.getCause() instanceof TimeoutException)) {
            log.debug("Caught a timeout exception in the lockCleanupThread in Theseus");
          } else {
            log.warn("Caught an exception in the lockCleanupThread in Theseus", t);
          }
        } finally {
          lastTry = System.currentTimeMillis();  // This counts as a try, even if we don't end up doing anything
        }
      }
      // Just a sanity check
      //noinspection ConstantConditions  // note[gabor]: worry if this stopps being needed.
      if (alive) {
        log.error("Theseus lock cleanup thread stopped while we're still alive!");
      } else {
        log.info("Stopped Theseus lock cleanup thread on shutdown");
      }
    });
    lockCleanupThread.setName("raft-lock-cleanup");
    lockCleanupThread.setDaemon(true);
    lockCleanupThread.setPriority(Thread.MIN_PRIORITY);
    lockCleanupThread.start();
  }


  /**
   * Create a new auto-resizing raft with the default algorithm, using the given transport.
   *
   * @param serverName The name of this server in the cluster.
   * @param transport The transport to use to communicate with the cluster.
   * @param targetClusterSize The target quorum size we try to maintain with auto-resizing
   * @param lifecycle The governing RaftLifecycle for this Theseus, so that we can pass mock ones in inside tests
   */
  public Theseus(String serverName, RaftTransport transport, int targetClusterSize, RaftLifecycle lifecycle) {
    this(
        new SingleThreadedRaftAlgorithm(
            new EloquentRaftAlgorithm(
                serverName,
                new KeyValueStateMachine(serverName),
                transport,
                targetClusterSize,
                lifecycle.managedThreadPool(16, "theseus_" + serverName, true),
                Optional.of(lifecycle))),
        transport, lifecycle);
  }


  /**
   * Create a new fixed-size raft with the default algorithm, using the given transport.
   *
   * @param serverName The name of this server in the cluster.
   * @param initialMembership The initial cluster membership.
   * @param lifecycle The governing EloquentLifecycle for this Theseus, so that we can pass mock ones in inside tests
   */
  public Theseus(String serverName, RaftTransport transport, Collection<String> initialMembership, RaftLifecycle lifecycle) {
    this(
        new SingleThreadedRaftAlgorithm(
            new EloquentRaftAlgorithm(
                serverName,
                new KeyValueStateMachine(serverName),
                transport,
                initialMembership,
                lifecycle.managedThreadPool(16, "theseus_" + serverName, true),
                Optional.of(lifecycle))),
        transport, lifecycle);
  }


  /**
   * Create a Raft cluster with a fixed quorum.
   *
   * @param serverName The server name for this Raft node.
   * @param quorum The fixed quorum for the cluster.
   *               This is a set of server names
   *
   * @throws IOException Thrown if we could not create the underlying transport.
   */
  public Theseus(String serverName, Collection<String> quorum) throws IOException {
    this(
        serverName,
        RaftTransport.create(serverName, RaftTransport.Type.NET),
        quorum,
        RaftLifecycle.global);
  }


  /**
   * Create a new dynamically resizing Raft cluster, with the given number
   * of nodes as the target quorum size. We will shrink the cluster if we have more
   * than this number, and grow it if we have less.
   *
   * @param targetQuorumSize The target number of nodes in the
   *                         quorum.
   *
   * @throws IOException Thrown if we could not create the underlying transport.
   */
  public Theseus(int targetQuorumSize) throws IOException {
    this(
        DEFAULT_SERVER_NAME.get(),
        RaftTransport.create(DEFAULT_SERVER_NAME.get(), RaftTransport.Type.NET),
        targetQuorumSize,
        RaftLifecycle.global);
  }


  /**
   * Create a unique id, keyed on the server name and a unique counter
   * for this server ({@link #UNIQUE_COUNTER}) to ensure near certain global uniqueness.
   */
  private String generateUniqueHash() {
    return this.serverName + "_" + UNIQUE_COUNTER.incrementAndGet();
  }


  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Public interface:
  //
  // These are the methods that are safe to use with Raft, wrapped in an interface to avoid mistakes like forgetting to
  // release locks.
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


  /** {@inheritDoc} */
  @Override
  public String serverName() {
    return serverName;
  }


  /**
   * Stop this raft node.
   *
   * @param allowClusterDeath If true, allow stopping even if we lose
   *                          quorum and lose state.
   *
   * @see EloquentRaftNode#close(boolean)
   */
  @Override
  public void close(boolean allowClusterDeath) {
    node.close(allowClusterDeath);
  }


  /**
   * Start this Raft.
   *
   * @see EloquentRaftNode#start()
   */
  void start() {
    node.start();
  }


  /**
   * Bootstrap this cluster, if there are no leaders.
   *
   * @param force If true, attempt to take leadership by force.
   *              That is, massively increase the term number.
   *
   * @return True if the cluster was successfully bootstrapped
   */
  public boolean bootstrap(boolean force) {
    log.info("Bootstrapping Raft");
    return node.bootstrap(force);
  }


  /**
   * Bootstrap this cluster, if there are no leaders.
   *
   * @return True if the cluster was successfully bootstrapped
   */
  public boolean bootstrap() {
    return bootstrap(false);
  }


  /**
   * Get the current Raft state. Note that this is a copy -- it may not be up to date,
   * but it's safe to change
   */
  public RaftState state() {
    return node.algorithm.state();
  }


  /**
   * Return any errors Raft has encountered.
   */
  public List<String> errors() {
    return node.errors();
  }


  //////////////////////////////////////////////////////////////////
  // With distributed locks
  //////////////////////////////////////////////////////////////////

  /** @see #withDistributedLockAsync(String, Supplier)
   *
   * This wraps a runnable with an instantly complete future.
   */
  public CompletableFuture<Boolean> withDistributedLockAsync(String lockName, Runnable runnable) {
    return withDistributedLockAsync(lockName, () ->
       CompletableFuture.supplyAsync(() -> {
         try {
           runnable.run();
           return true;
         } catch (Throwable t) {
           log.warn("Caught exception on withDistributedLockAsync ", t);
           return false;
         }
       }, pool)
    );
  }


  /**
   * This runs a function while holding a distributed lock. The lock is global across the cluster, so you're safe to run
   * from anywhere without fear of contention. This is similar to taking a SQL lock, but is much faster and is therefore
   * appropriate for much lower latency scenarios.
   *
   * @param lockName the name of the lock -- in a global flat namespace
   * @param runnable the runnable to execute while holding the lock
   */
  public CompletableFuture<Boolean> withDistributedLockAsync(String lockName, Supplier<CompletableFuture<Boolean>> runnable) {
    Prometheus.counterInc(Prometheus.labelCounter(COUNTER_CALLS, "withDistributedLockAsync"));
    // 1. Create and submit the lock request
    final String randomHash = generateUniqueHash();
    byte[] releaseLockTransition = KeyValueStateMachine.createReleaseLockTransition(lockName, serverName, randomHash);

    return retryTransitionAsync(KeyValueStateMachine.createRequestLockTransition(lockName, serverName, randomHash), defaultTimeout).thenCompose((success) -> {
      // 2.a. If we fail to submit the transition to get the lock, it is possible that we failed while waiting for the
      // transition to commit, but the transition is still out there. To be totally correct, we need to release the lock
      // here in the rare event that the request lock transition is still around and eventually gets committed.
      if (!success) {
        return node.submitTransition(releaseLockTransition)  // note[gabor]: let failsafe retry -- same reasoning as above.
            .whenComplete((s, e) -> {  // note[gabor]: this is not exception-proof; `e` may not be null.
              handleReleaseLockResult(s, e, releaseLockTransition);
            });
      } else {
        // 2.b. Otherwise, wait on the lock
        return stateMachine.createLockAcquiredFuture(lockName, serverName, randomHash).thenCompose((gotLock) -> {
          if (gotLock) {
            // 3. Run our runnable, which returns a CompletableFuture
            try {
              return runnable.get();
            } catch (Throwable t) {
              log.warn("Uncaught exception on runnable in withDistributedLockAsync: ", t);
              return CompletableFuture.completedFuture(false);
            }
          } else {
            return CompletableFuture.completedFuture(false);
          }
        }).whenComplete((runSuccess, t) -> {
          // 4. Always release the lock
          node.submitTransition(releaseLockTransition)  // note[gabor]: let failsafe retry -- same reasoning as above.
              .whenComplete((s, e) -> {  // note[gabor]: this is not exception-proof; `e` may not be null.
                handleReleaseLockResult(s, e, releaseLockTransition);
              });
        });
      }
    });
  }


  //////////////////////////////////////////////////////////////////
  // Try locks
  //////////////////////////////////////////////////////////////////

  /**
   * This will try to acquire a lock. It will block for network IO, but will not block waiting for the lock if the lock
   * is not immediately acquired upon request. It returns a LongLivedLock object that is a reference to this lock, and
   * contains a method to release it. The LongLivedLock object will also do its best to recover from situations where
   * users fail/forget to release the lock, though it will yell at you if it has to clean up after you.
   *
   * @param lockName the name of the lock to attempt to acquire
   * @param safetyReleaseWindow this is the duration of time after which we will automatically release the lock if it
   *                            hasn't already been released. This is just a safety check, you should set it to much
   *                            longer than you expect to hold the lock for.
   * @return if successful, a LongLivedLock object that can be kept around as a reference to this lock, with a method to
   *         release it. If we were unable to acquire the lock, returns empty.
   */
  public Optional<LongLivedLock> tryLock(String lockName, Duration safetyReleaseWindow) {
    try {
      return tryLockAsync(lockName, safetyReleaseWindow).get();
    } catch (InterruptedException | ExecutionException e) {
      return Optional.empty();
    }
  }


  /**
   * This will try to acquire a lock. It will block for network IO, but will not block waiting for the lock if the lock
   * is not immediately acquired upon request. It returns a LongLivedLock object that is a reference to this lock, and
   * contains a method to release it. The LongLivedLock object will also do its best to recover from situations where
   * users fail/forget to release the lock, though it will yell at you if it has to clean up after you.
   *
   * @param lockName the name of the lock to attempt to acquire
   * @param safetyReleaseWindow this is the duration of time after which we will automatically release the lock if it
   *                            hasn't already been released. This is just a safety check, you should set it to much
   *                            longer than you expect to hold the lock for.
   * @return if successful, a LongLivedLock object that can be kept around as a reference to this lock, with a method to
   *         release it. If we were unable to acquire the lock, returns empty.
   */
  public CompletableFuture<Optional<LongLivedLock>> tryLockAsync(String lockName, Duration safetyReleaseWindow) {
    Prometheus.counterInc(Prometheus.labelCounter(COUNTER_CALLS, "tryLockAsync"));
    String randomHash = generateUniqueHash();
    CompletableFuture<Optional<LongLivedLock>> future = new CompletableFuture<>();

    // 1. Create and submit the lock request
    retryTransitionAsync(KeyValueStateMachine.createTryLockTransition(lockName, serverName, randomHash), defaultTimeout).thenAccept((success) -> {
      try {
        // 2. If we got the lock, then return an object representing that
        KeyValueStateMachine.QueueLock lock = stateMachine.locks.get(lockName);
        if (lock != null && lock.holder != null && Objects.equals(lock.holder.server, serverName) && Objects.equals(lock.holder.uniqueHash, randomHash)) {
          future.complete(Optional.of(new LongLivedLockImpl(lockName, randomHash, safetyReleaseWindow)));
        } else {
          future.complete(Optional.empty());
        }
      } catch (Throwable t) {
        // If something goes wrong, ensure we try to release the lock
        byte[] releaseLockTransition = KeyValueStateMachine.createReleaseLockTransition(lockName, serverName, randomHash);
        node.submitTransition(releaseLockTransition)  // note[gabor]: let failsafe retry -- same reasoning as above.
            .whenComplete((s, e) -> {  // note[gabor]: this is not exception-proof; `e` may not be null.
              handleReleaseLockResult(s, e, releaseLockTransition);
              future.complete(Optional.empty());  // wait for release to finish (or at least try to finish) to return
            });
      }
    });

    return future;
  }


  /**
   * Release a Raft lock
   *
   * @param lockName The name of the lock we are releasing
   *
   * @return A completable future for whether the lock was released
   */
  public CompletableFuture<Boolean> releaseLock(String lockName) {
    KeyValueStateMachine.QueueLock lock = stateMachine.locks.get(lockName);
    if (lock != null && lock.holder != null) {
      KeyValueStateMachine.LockRequest holder = lock.holder;
      return retryTransitionAsync(KeyValueStateMachine.createReleaseLockTransition(lockName, holder.server, holder.uniqueHash), defaultTimeout);
    } else {
      return CompletableFuture.completedFuture(false);
    }
  }


  //////////////////////////////////////////////////////////////////
  // With element calls
  //////////////////////////////////////////////////////////////////


  /**
   * Queue a failed lock release request.
   * The main point of this function, rather than just calling {@linkplain ArrayList#add(Object) add()}
   * on the underlying map, is to deduplicate lock release requests.
   * If a request is already queued, we don't double-add it.
   *
   * @param releaseLockTransition The request to queue.
   */
  // note[gabor] package protected to allow fine-grained testing
  void queueFailedLock(byte[] releaseLockTransition) {
    synchronized (unreleasedLocks) {
      if (unreleasedLocks.size() < (0x1<<20)) {  // 1M locks
        log.debug("Could not release lock! Queueing for later deletion.");
        boolean isDuplicate = false;
        for (byte[] lock : unreleasedLocks) {
          if (Arrays.equals(lock, releaseLockTransition)) {
            isDuplicate = true;
            break;
          }
        }
        if (!isDuplicate) {
          unreleasedLocks.add(releaseLockTransition);
        }
      } else {
        log.error("Could not release a lock and did not queue it for later deletion (queue full)");
      }
    }
  }


  /**
   * This is a little helper that's responsible for queueing up a release lock transition, if necessary
   */
  private void handleReleaseLockResult(Boolean success, Throwable error, byte[] releaseLockTransition) {
    if (error != null || success == null || !success) {  // note[gabor] pass the boolean as an object to prevent possible null pointer
      if (error != null &&
          // Timeouts are semi-common
          !(error instanceof TimeoutException) &&
          !(error instanceof CompletionException && error.getCause() != null && error.getCause() instanceof TimeoutException) &&
          // Rejected executions are common under load
          !(error instanceof RejectedExecutionException) &&
          !(error instanceof CompletionException && error.getCause() != null && error.getCause() instanceof RejectedExecutionException)
      ) {
        log.warn("Release lock encountered an unexpected error: ", error);
      }
      queueFailedLock(releaseLockTransition);
    }
  }


  /**
   * This is the safest way to do a withElement() update. We take a global lock, and then manipulate the item, and set
   * the result. This means the mutator() can have side effects and not be idempotent, since it will only be called
   * once. This safety comes at a performance cost, since this requires two writes to the raft cluster. This is in
   * contrast to withElementIdempotent(), which uses a single compare-and-swap instead, to make it faster.
   *
   * Since we don't trust the Java serializer, which is used by Atomix for everything, we use a byte[] interface when
   * interacting with the distributed storage. That way we can store elements that have a proto serialization interface,
   * and still use proto.
   *
   * @param elementName the name of the element -- in a global flat namespace
   * @param mutator the function to call, while holding a lock on the element, to mutate the element (doesn't have to
   *                actually change anything, but can).
   * @param createNew a function to supply a new element, if none is present in the map already
   * @param permanent if false, this element will be automatically removed when we disconnect from the cluster, if we're
   *                  the last people to have edited the element.
   *
   * @throws NoSuchElementException if we didn't supply a creator, and the object does not exist in Raft.
   *
   * @return true on success, false if something went wrong
   */
  public CompletableFuture<Boolean> withElementAsync(String elementName, Function<byte[], byte[]> mutator, @Nullable Supplier<byte[]> createNew, boolean permanent) {
    Prometheus.counterInc(Prometheus.labelCounter(COUNTER_CALLS, "withElementAsync"));
    // 1. Create the lock request
    // 1.1. The lock request
    final String randomHash = generateUniqueHash();
    byte[] requestLockTransition = KeyValueStateMachine.createRequestLockTransition(elementName, serverName, randomHash);
    byte[] releaseLockTransition = KeyValueStateMachine.createReleaseLockTransition(elementName, serverName, randomHash);
    // 1.2. The lock release thunk
    BiFunction<String, Throwable, CompletableFuture<Boolean>> releaseLockWithoutChange = (error, exception) -> {
      if (error != null) {
        Prometheus.counterInc(COUNTER_FAILED_LOCKS);
        log.debug("Releasing lock '{}' on error: {}", elementName, error, exception);
      }
      return exceptionProof(node.submitTransition(releaseLockTransition)  // note[gabor]: let failsafe retry -- same reasoning as above.
          .whenComplete((s, e) -> {  // note[gabor]: this is not exception-proof; `e` may not be null.
            handleReleaseLockResult(s, e, releaseLockTransition);
          }));
    };

    // 2. Submit the lock request
    CompletableFuture<Boolean> future = new CompletableFuture<>();
    exceptionProof(retryTransitionAsync(requestLockTransition, defaultTimeout)).whenComplete((acquireSuccess, acquireException) -> {
      if (acquireSuccess != null && acquireException == null && acquireSuccess) {
        // 2.A. Case: we've verifiably submitted the lock request
        // Next, let's actually wait on the lock
        stateMachine.createLockAcquiredFuture(elementName, serverName, randomHash).whenComplete((gotLockSuccess, gotLockException) -> {
          if (gotLockSuccess != null && gotLockException == null && gotLockSuccess) {
            // 2.A.A. Case: we've acquired the lock
            // Next, let's do our mutation
            pool.execute(() -> {
              // (some asserts)
              assert stateMachine.locks.get(elementName) != null : "We do not hold the lock we claim to (no lock)";
              assert stateMachine.locks.get(elementName).holder != null : "We do not hold the lock we claim to (no holder)";
              //noinspection ConstantConditions
              assert Objects.equals(stateMachine.locks.get(elementName).holder.server, serverName)  : "Someone else holds the lock we claim to hold";
              //noinspection ConstantConditions
              assert Objects.equals(stateMachine.locks.get(elementName).holder.uniqueHash, randomHash)  : "Someone else holds the lock we claim to hold (different hash)";

              // i. Create the object, if it isn't present
              Optional<byte[]> optionalObject = stateMachine.get(elementName, node.transport.now());
              boolean newObject = false;
              byte[] object;
              if (optionalObject.isPresent()) {
                object = optionalObject.get();
              } else if (createNew != null) {
                try {
                  object = createNew.get();
                  newObject = true;
                } catch (Throwable t) {
                  releaseLockWithoutChange.apply("withElementAsync() object creator threw an exception: ", t).whenComplete((x, e) -> future.complete(false));
                  return;
                }
              } else {
                releaseLockWithoutChange.apply("withElementAsync() object creator is null and there's nothing in the map. Returning failure", null).whenComplete((x, e) -> future.complete(false));
                return;
              }

              // ii. If we returned a null from creation, that's a signal to stop the withElement call
              if (object == null) {
                releaseLockWithoutChange.apply("Creator returned null -- not mutating", null).whenComplete((x, e) -> future.complete(false));
                return;
              }

              // iii. Mutate the object
              byte[] mutated;
              byte[] input = new byte[object.length];  // note[gabor]: copy so mutator doesn't have to be functional
              System.arraycopy(object, 0, input, 0, object.length);
              try {
                mutated = mutator.apply(input);
              } catch (Throwable t) {
                releaseLockWithoutChange.apply("withElementAsync() object mutator threw an exception: ", t).whenComplete((x, e) -> future.complete(false));
                return;
              }

              // iv. Put the object back into the map AND release the lock in a single transition
              if (newObject || (mutated != null && !Arrays.equals(object, mutated))) {  // only if there was a change, or if it's a new object
                retryTransitionAsync(
                    KeyValueStateMachine.createGroupedTransition(createSetValueTransition(elementName, mutated, permanent), releaseLockTransition),
                    defaultTimeout).whenComplete((transitionSuccess, transitionException) -> {
                      if (transitionSuccess != null && transitionException == null && transitionSuccess) {
                        // 2.A.A.A. Case: we've finished our mutation
                        future.complete(true); // SUCCESSFUL CASE
                      } else {
                        // 2.A.A.B. Case: we could not submit our transition and/or lock
                        releaseLockWithoutChange.apply("Could not apply transition and/or release object lock", transitionException).whenComplete((x, e) -> future.complete(false));
                        //noinspection UnnecessaryReturnStatement
                        return;
                      }
                });
              } else {

                // v. If the mutator chose not to mutate the object, then this is trivially successful
                releaseLockWithoutChange.apply(null, null).whenComplete((x, e) -> future.complete(true));  // note[gabor]: completes as true
                //noinspection UnnecessaryReturnStatement
                return;

              }
            });

          } else {
            // 2.A.B. Case: we were unable to acquire the lock
            releaseLockWithoutChange.apply("Failed to acquire the lock because the future returned false", null).whenComplete((x, e) -> future.complete(false));
          }
        });
      } else {
        // 2.B. Case: something went wrong submitting the lock request
        releaseLockWithoutChange.apply("Failed to apply the request lock transition", null).whenComplete((x, e) -> future.complete(false));
      }
    });

    return future;
  }


  /**
   * Like {@link #withElementAsync(String, Function, Supplier, boolean)}, but without the safety of taking the lock on the element beforehand.
   * This is a bit of a dangerous method, as it can open the caller up to race conditions, and should be used sparingly.
   *
   * @param elementName the name of the element -- in a global flat namespace
   * @param mutator the function to call, while holding a lock on the element, to mutate the element (doesn't have to
   *                actually change anything, but can).
   * @param createNew a function to supply a new element, if none is present in the map already
   * @param permanent if false, this element will be automatically removed when we disconnect from the cluster, if we're
   *                  the last people to have edited the element.
   *
   * @throws NoSuchElementException if we didn't supply a creator, and the object does not exist in Raft.
   *
   * @return true on success, false if something went wrong
   */
  public CompletableFuture<Boolean> withElementUnlockedAsync(String elementName, Function<byte[], byte[]> mutator, @Nullable Supplier<byte[]> createNew, boolean permanent) {
      // 1. Get the object, if it is present
      Optional<byte[]> optionalObject = stateMachine.get(elementName, node.transport.now());

      CompletableFuture<Boolean> future = new CompletableFuture<>();

      pool.execute(() -> {
        // 2. Create the object, if it isn't present
        boolean newObject = false;
        byte[] object;
        if (optionalObject.isPresent()) {
          object = optionalObject.get();
        } else if (createNew != null) {
          try {
            object = createNew.get();
            newObject = true;
          } catch (Throwable e) {
            log.warn("withElementAsync() object creator threw an exception. Returning failure");
            future.complete(false);
            return;
          }
        } else {
          log.warn("withElementAsync() object creator is null and there's nothing in the map. Returning failure");
          future.complete(false);
          return;
        }

        // If we returned a null from creation, that's a signal to stop the withElement call
        if (object == null) {
          future.complete(false);
          return;
        }

        // 3. Mutate the object
        byte[] mutated = mutator.apply(object);

        // 4. Put the object back into the map
        if (newObject || (mutated != null && !Arrays.equals(object, mutated))) {  // only if there was a change, or if it's a new object
          retryTransitionAsync(createSetValueTransition(elementName, mutated, permanent), defaultTimeout).whenComplete((success, t) -> {
            if (t != null) {
              future.completeExceptionally(t);
            } else {
              future.complete(success);
            }
          });
        } else {
          // If the mutator chose not to mutate the object, then this is trivially successful
          future.complete(true);
        }
      });

      return future;
  }


  //////////////////////////////////////////////////////////////////
  // Set element calls
  //////////////////////////////////////////////////////////////////

  /**
   * THIS IS DANGEROUS TO USE! People can clobber each other's writes, and there are tons of race conditions if you use
   * this call in conjunction with getElement() with no outside synchronization mechanism. Much safer, if you haven't
   * thought about it much, is to use {@link #withElementAsync(String, Function, Supplier, boolean)}. Only use setElement() if you're really certain that you are the
   * only one in the cluster writing, or you don't mind being clobbered.
   *
   * @param elementName the name of the element to write
   * @param value the value to set the element to
   * @param permanent if false, the element will be automatically removed when we disconnect from the cluster, if we're
   *                  the last people to have edited the element. If true, the element will stick around forever.
   */
  @SuppressWarnings("unused")
  public CompletableFuture<Boolean> setElementAsync(String elementName, byte[] value, boolean permanent, Duration timeout) {
    return retryTransitionAsync(createSetValueTransition(elementName, value, permanent), timeout);
  }


  /**
   * Creates the right transition proto
   */
  private byte[] createSetValueTransition(String elementName, byte[] value, boolean permanent) {
    if (permanent) {
      return KeyValueStateMachine.createSetValueTransition(elementName, value);
    } else {
      return KeyValueStateMachine.createSetValueTransitionWithOwner(elementName, value, serverName);
    }
  }


  //////////////////////////////////////////////////////////////////
  // Remove element calls
  //////////////////////////////////////////////////////////////////

  /**
   * This removes an element from the Raft key-value store. It's a no-op if the value isn't already in the database.
   *
   * @param elementName the name of the element to remove
   */
  public CompletableFuture<Boolean> removeElementAsync(String elementName, Duration timeout) {
    // Remove the object from the map
    return retryTransitionAsync(KeyValueStateMachine.createRemoveValueTransition(elementName), timeout);
  }


  /**
   * This removes a set of elements from the Raft key-value store. It's a no-op if the value isn't already in the database.
   *
   * @param elementName the name of the element to remove
   */
  public CompletableFuture<Boolean> removeElementsAsync(Set<String> elementName, Duration timeout) {
    // Create a grouped transition to remove all the elements from Raft at once
    return retryTransitionAsync(KeyValueStateMachine.createGroupedTransition(elementName.stream().map(KeyValueStateMachine::createRemoveValueTransition).collect(Collectors.toList()).toArray(new byte[elementName.size()][])), timeout);
  }

  //////////////////////////////////////////////////////////////////
  // Getters
  //////////////////////////////////////////////////////////////////

  /**
   * This grabs the current state of an element, if it's present in the system. This may be out of date, since no lock
   * is held on the item when retrieving it. If a lock is desired then use withElement() instead.
   *
   * This is non-blocking and fast.
   *
   * @param elementName the name of the element -- in a global flat namespace
   */
  public Optional<byte[]> getElement(String elementName) {
    return stateMachine.get(elementName, node.transport.now());
  }


  /**
   * This returns the current understanding of the cluster membership on this node.
   */
  public Set<String> getConfiguration() {
    return node.algorithm.mutableState().log.getQuorumMembers();
  }

  /**
   * This returns a snapshot of the current values in the state machine. This is passed by value, not by reference, so
   * this is safe to hold on to.
   */
  public Map<String, byte[]> getMap() {
    return stateMachine.map();
  }


  /**
   * This returns the current keys in the state machine. It's impossible to hold some sort of lock while fetching these,
   * so these will be an eventually-consistent set, not an immediately consistent set.
   */
  public Collection<String> getKeys() {
    return stateMachine.keys();
  }


  /**
   * Get the set of locks that are held by the state machine, and the server that holds them.
   * The keys are locks, mapped to the server that holds it.
   */
  public Map<String, String> getLocks() {
    Map<String, String> locks = new HashMap<>();
    for (Map.Entry<String, KeyValueStateMachine.QueueLock> entry : stateMachine.locks.entrySet()) {
      locks.put(entry.getKey(), entry.getValue().holder == null ? "<none>" : entry.getValue().holder.server);
    }
    return locks;
  }


  //////////////////////////////////////////////////////////////////
  // Change listeners
  //////////////////////////////////////////////////////////////////

  /**
   * This registers a listener that will be called whenever the key-value store changes.
   *
   * @param changeListener the listener to register
   */
  public synchronized void addChangeListener(KeyValueStateMachine.ChangeListener changeListener) {
    stateMachine.addChangeListener(changeListener);
  }


  /**
   * This removes a listener that will be called whenever the key-value store changes.
   *
   * @param changeListener the listener to deregister
   */
  public synchronized void removeChangeListener(KeyValueStateMachine.ChangeListener changeListener) {
    stateMachine.removeChangeListener(changeListener);
  }


  //////////////////////////////////////////////////////////////////
  // Error listeners
  //////////////////////////////////////////////////////////////////

  /**
   * Raft keeps track of an additional error listener
   * Errors are thrown from {@link KeyValueStateMachine} and {@link EloquentRaftNode} by default but
   * users are free to attach their own error listeners
   *
   *
   * Usage:
   *  RaftErrorHandler errorListener = (debugMessage, stackTrace) -&gt; {
   *    // Do something with the debug message / stackTrace
   *    // Eg. Logging, or alerting via PagerDuty
   *  }
   *  addErrorListener(errorListener);
   *
   *  // Later in the code where there is an error
   *  throwRaftError(incident_key, debug_message);
   *
   * @param errorListener The error listener to add.
   */
  public void addErrorListener(RaftErrorListener errorListener) {
    stateMachine.addErrorListener(errorListener);
    node.addErrorListener(errorListener);
    if (pool instanceof TrackedExecutorService) {
      ((TrackedExecutorService) pool).addErrorListener(errorListener);
    }
  }


  /**
   * Remove an error listener from Raft.
   *
   * @param errorListener The error listener to remove.
   */
  @SuppressWarnings("unused")
  public void removeErrorListener(RaftErrorListener errorListener) {
    stateMachine.removeErrorListener(errorListener);
    node.removeErrorListener(errorListener);
    if (pool instanceof TrackedExecutorService) {
      ((TrackedExecutorService) pool).removeErrorListener(errorListener);
    }
  }


  /**
   * Remove all error listeners from Raft.
   */
  @SuppressWarnings("unused")
  public void clearErrorListeners() {
    stateMachine.clearErrorListeners();
    node.clearErrorListeners();
    if (pool instanceof TrackedExecutorService) {
      ((TrackedExecutorService) pool).clearErrorListeners();
    }
  }


  //////////////////////////////////////////////////////////////////
  // Private implementation details
  //////////////////////////////////////////////////////////////////

  /**
   * This returns a CompletableFuture for retrying a transition up until the timeout is reached.
   *
   * @param transition the transition we're trying to apply
   * @param timeout a length of time in which to retry failed transitions - IMPORTANT: the CompletableFuture returned
   *                doesn't have to finish in this amount of time, we just stop retrying failed transitions after this
   *                window elapses.
   *
   * @return a CompletableFuture for the transition wrapped in retries
   */
  private CompletableFuture<Boolean> retryTransitionAsync(byte[] transition, Duration timeout) {
    int uniqueID = new Random().nextInt();
    long startTime = System.currentTimeMillis();
    log.trace("\n-------------\nSTARTING TRANSITION {}\n-------------\n", uniqueID);
    return retryAsync(() -> node.submitTransition(transition), timeout, node.transport.now()).thenApply((success) -> {
      log.trace("\n-------------\nFINISHED TRANSITION {}: {} ({})\n-------------\n", uniqueID, success, TimerUtils.formatTimeSince(startTime));
      return success;
    });
  }


  /**
   * This is a helper that takes a CompletableFuture<Boolean> and wraps it so that it cannot return an exception,
   * instead it just returns false.
   *
   * @param future the future to wrap
   * @return a CompletableFuture that will never complete exceptionally
   */
  private static CompletableFuture<Boolean> exceptionProof(CompletableFuture<Boolean> future) {
    CompletableFuture<Boolean> wrapper = new CompletableFuture<>();
    future.whenComplete((success, t) -> {
      try {
        // If we got an exception, mark success as false.
        if (t != null || success == null) {
          if (t instanceof TimeoutException ||
              (t instanceof CompletionException && t.getCause() != null && t.getCause() instanceof TimeoutException)) {
            log.debug("Caught a timeout exception");
          } else if (t instanceof RejectedExecutionException ||
                (t instanceof CompletionException && t.getCause() != null && t.getCause() instanceof RejectedExecutionException)) {
            log.debug("Caught a rejected execution exception");
          } else {
            log.warn("Caught an exception in exception proof wrapper", t);
          }
          success = false;
        }
      } finally {
        // Complete our future no matter what
        wrapper.complete(success);
      }
    });
    return wrapper;
  }


  /**
   * This returns a CompletableFuture for retrying a transition up until the timeout is reached.
   *
   * @param action the action to retry, which returns a CompletableFuture indicating when the action is done and what
   *               the status of the action was.
   * @param timeout a length of time in which to retry failed transitions - IMPORTANT: the CompletableFuture returned
   *                doesn't have to finish in this amount of time, we just stop retrying failed transitions after this
   *                window elapses.
   * @param startTime The time we started the call>
   * @return a CompletableFuture for the transition wrapped in retries
   */
  private CompletableFuture<Boolean> retryAsync(Supplier<CompletableFuture<Boolean>> action, Duration timeout, long startTime) {
    // Create a transition future
    CompletableFuture<Boolean> future = exceptionProof(action.get());
    return future.thenCompose((result) -> {
      if (result != null && result) {
        // A. Case: the future was successful
        return CompletableFuture.completedFuture(true);
      } else {
        // B. Case: the future failed
        // B.1. Get the remaining time on the timeout
        long elapsed = node.transport.now() - startTime;
        long remainingTime = timeout.toMillis() - elapsed;
        log.debug("Retrying a failed transition @ {} ({}ms left). This is fine, but should be rare", node.transport.now(), remainingTime);
        // B.2. Check if there's still time on the timeout, and we haven't shut down the node yet
        if (remainingTime < 0 || !node.isAlive()) {
          log.warn("Failed transition @ {} after {}ms (timeout of {}); not retrying further.", node.transport.now(), elapsed, timeout.toMillis());
          return CompletableFuture.completedFuture(false);
        }
        // B.3. Retry if there's still time
        CompletableFuture<Boolean> ret = new CompletableFuture<>();
        node.transport.schedule(new SafeTimerTask() {
                                  @Override
                                  public void runUnsafe() {
                                    retryAsync(action, timeout, startTime).thenApply(ret::complete);
                                  }
                                }, node.algorithm.electionTimeoutMillisRange().begin / 5); // Wait a bit before trying again, to avoid flooding the system with too many requests
        return ret;
      }
    });
  }


  /**
   * If true, this node is the leader of the Raft cluster.
   */
  public boolean isLeader() {
    return node.algorithm.mutableState().isLeader();
  }


  /**
   * Register a new failsafe to run occasionally on this raft node.
   *
   * @param failsafe the one to register
   */
  public void registerFailsafe(RaftFailsafe failsafe) {
    node.registerFailsafe(failsafe);
  }

}
