package ai.eloquent.raft;

import ai.eloquent.error.RaftErrorListener;
import ai.eloquent.util.SafeTimerTask;
import ai.eloquent.util.TimeUtils;
import ai.eloquent.web.TrackedExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.lang.ref.WeakReference;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.function.Supplier;

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
public class EloquentRaft {
  /**
   * An SLF4J Logger for this class.
   */
  private static final Logger log = LoggerFactory.getLogger(EloquentRaft.class);

  /** The Raft cluster name */
  public static final String clusterName =
      (System.getenv("CHECKED_DEPLOY") != null || System.getenv("CI") != null)
      ? "ci_raft_cluster"
      : Optional.ofNullable(System.getenv("ELOQUENT_RAFT_CLUSTER")).orElse("eloquent");


  /**
   * This holds a lock that is expected to live longer than the duration of a single method call. Usually this lock is
   * associated with a resource that the holder is responsible for. Because forgetting/failing to release long lived
   * locks can be catastrophic, this class packages up a number of safegaurds to ensure that the lock does eventually
   * get released. Since locks auto-release when their owner disconnects from the cluster, this can safely be a part of
   * EloquentRaft, since as soon as EloquentRaft closes these locks no longer need cleaning up anyways.
   */
  public interface LongLivedLock {
    /**
     * Returns the name of the lock that is held.
     */
    String lockName();
    /**
     * Returns true if the lock represented is still held.
     */
    boolean isHeld();
    /**
     * This releases a lock, and cleans up any resources waiting on it. Calling this more than once is a no-op.
     */
    CompletableFuture<Boolean> release();
  }

  /**
   * The reason this is broken out from the interface is so that it is possible to mock LongLivedLock objects in the
   * RaftManagerMock.
   */
  private class LongLivedLockImpl implements LongLivedLock {
    public final String lockName;
    public final String uniqueHash;
    public final Duration safetyReleaseWindow;

    public boolean held;

    public final SafeTimerTask cleanupTimerTask;

    /**
     * This creates a LongLivedLock object which will automatically clean itself up in the event of catastrophic
     * failure. By creating this object, you are asserting
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
      this.held = true;

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

    /**
     * Returns true if the lock represented is still held.
     */
    @Override
    public boolean isHeld() {
      return this.held;
    }

    /**
     * This releases a lock, and cleans up any resources waiting on it. Calling this more than once is a no-op.
     */
    @Override
    public synchronized CompletableFuture<Boolean> release() {
      if (!held) return CompletableFuture.completedFuture(true);
      cleanupTimerTask.cancel();
      held = false;
      return retryTransitionAsync(KeyValueStateMachine.createReleaseLockTransition(lockName, serverName, uniqueHash), Duration.ofMinutes(1));
    }

    /**
     * This is a safety check to ensure that if a lock gets GC'd, it also gets released
     */
    @Override
    public void finalize() {
      // Optimization: Don't take the synchronized block from within finalize unless we haven't released yet
      if (held) {
        synchronized (this) {
          // Check again inside the synchronized block for if we've released yet
          if (held) {
            log.warn(serverName+" - LongLivedLock for \""+lockName+"\" is being cleaned up from finalize()! This is very bad!");
            release();
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
    WeakReference<LongLivedLock> weakReference;

    public LockCleanupTimerTask(LongLivedLock longLivedLock) {
      weakReference = new WeakReference<>(longLivedLock);
    }

    @Override
    public void runUnsafe() {
      final LongLivedLock lock = weakReference.get();
      // This has already been GC'd, so it's no problem
      if (lock == null) return;
      // Optimization: Don't take the synchronized block from within finalize unless we haven't released yet
      if (lock.isHeld()) {
        //noinspection SynchronizationOnLocalVariableOrMethodParameter
        synchronized (lock) {
          // Check again inside the synchronized block for if we've released yet
          if (lock.isHeld()) {
            log.warn("LongLivedLock for \"" + lock.lockName() + "\" is being cleaned up from a TimerTask! This is very, very bad! It means we didn't release it, and finalize() never fired.");
            lock.release();
          }
        }
      }
    }
  }


  /**
   * The name of our node.
   * This can also be gotten from {@link #node}.{@link EloquentRaftNode#algorithm}.{@link EloquentRaftAlgorithm#state}.{@link RaftState#serverName}.
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
   * The RaftLifecycle that governs this EloquentRaft
   */
  public final RaftLifecycle lifecycle;

  /**
   * A set of release lock transitions that did not complete in their usual loop -- we should continue to
   * try to release these locks so long as we can, in hopes Raft comes back up sometime.
   */
  private final List<byte[]> unreleasedLocks = new ArrayList<>();

  /**
   * If false, we have stopped this Raft.
   */
  private boolean alive = true;

  /**
   * An executor pool for async tasks.
   */
  private final ExecutorService pool;


  /**
   * The constructor takes three arguments: a cluster name (for discovery), a server name (for identifying ourselves,
   * must be unique within the cluster), and a reference to the lifecycle object that governs this EloquentRaft (so that
   * tests can pass different RaftLifecycle objects to different Raft instances).
   *
   * @param algo The Raft algorithm to use. Defaults to {@link EloquentRaftAlgorithm}.
   * @param transport The type of transport to use for this Raft cluster.
   * @param lifecycle The governing RaftLifecycle for this EloquentRaft, so that we can pass mock ones in inside tests
   */
  public EloquentRaft(RaftAlgorithm algo, RaftTransport transport, RaftLifecycle lifecycle) {
    this.serverName = algo.serverName();
    this.node = new EloquentRaftNode(algo, transport, lifecycle);
    this.node.registerShutdownHook(() -> {
      alive = false;
      // Wake up the lock cleanup thread
      synchronized (unreleasedLocks) {
        unreleasedLocks.notifyAll();
      }
    });
    this.stateMachine = (KeyValueStateMachine) algo.mutableStateMachine();
    this.lifecycle = lifecycle;
    this.pool = lifecycle.managedThreadPool("eloquent-raft-async", true);

    // Create lock cleanup thread
    Thread lockCleanupThread = new Thread(() -> {
      while (alive) {
        try {
          synchronized (unreleasedLocks) {
            while (unreleasedLocks.isEmpty() && alive) {
              try {
                unreleasedLocks.wait(1000);
              } catch (InterruptedException ignored) {
              }
            }
            if (!unreleasedLocks.isEmpty()) {
              log.warn("Trying to release {} unreleased locks", unreleasedLocks.size());
            }
            for (byte[] releaseLockRequest : new HashSet<>(unreleasedLocks)) {
              try {
                retryTransitionAsync(releaseLockRequest, Duration.ofSeconds(5))
                    .get(node.algorithm.electionTimeoutMillisRange().end + 100, TimeUnit.MILLISECONDS);
                unreleasedLocks.remove(releaseLockRequest);
              } catch (InterruptedException | ExecutionException | TimeoutException ignored) {}
            }
          }
        } catch (Throwable t) {
          log.warn("Caught an exception in the lockCleanupThread in EloquentRaft", t);
        }
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
   * @param lifecycle The governing RaftLifecycle for this EloquentRaft, so that we can pass mock ones in inside tests
   */
  public EloquentRaft(String serverName, RaftTransport transport, int targetClusterSize, RaftLifecycle lifecycle) {
    this(
        new SingleThreadedRaftAlgorithm(
            new EloquentRaftAlgorithm(
                serverName,
                new KeyValueStateMachine(serverName),
                transport,
                targetClusterSize,
                lifecycle.managedThreadPool("raft-public", true),
                Optional.of(lifecycle)),
            lifecycle.managedThreadPool("raft-pubic", true)),
        transport, lifecycle);
  }


  /**
   * Create a new fixed-size raft with the default algorithm, using the given transport.
   *
   * @param serverName The name of this server in the cluster.
   * @param initialMembership The initial cluster membership.
   * @param lifecycle The governing EloquentLifecycle for this EloquentRaft, so that we can pass mock ones in inside tests
   */
  public EloquentRaft(String serverName, RaftTransport transport, Set<String> initialMembership, RaftLifecycle lifecycle) {
    this(
        new SingleThreadedRaftAlgorithm(
            new EloquentRaftAlgorithm(
                serverName,
                new KeyValueStateMachine(serverName),
                transport,
                initialMembership,
                lifecycle.managedThreadPool("raft-public", true),
                Optional.of(lifecycle)),
            lifecycle.managedThreadPool("raft-pubic", true)),
        transport, lifecycle);
  }


  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Public interface:
  //
  // These are the methods that are safe to use with Raft, wrapped in an interface to avoid mistakes like forgetting to
  // release locks.
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


  /**
   * Stop this raft node.
   *
   */
  public void close() {
      // Close the cluster
      node.close();
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
   * Bootstrap this cluster, if there are leaders.
   *
   * @return True if the cluster was successfully bootstrapped
   */
  public boolean bootstrap(boolean force) {
    log.info("Bootstrapping Raft");
    return node.bootstrap(force);
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
    // 1. Create and submit the lock request
    final String randomHash = UUID.randomUUID().toString();
    byte[] releaseLockTransition = KeyValueStateMachine.createReleaseLockTransition(lockName, serverName, randomHash);

    return retryTransitionAsync(KeyValueStateMachine.createRequestLockTransition(lockName, serverName, randomHash), Duration.ofSeconds(5)).thenCompose((success) -> {
      // 2.a. If we fail to submit the transition to get the lock, it is possible that we failed while waiting for the
      // transition to commit, but the transition is still out there. To be totally correct, we need to release the lock
      // here in the rare event that the request lock transition is still around and eventually gets committed.
      if (!success) {
        return retryTransitionAsync(releaseLockTransition, Duration.ofMinutes(5))
            .handle((Boolean s,Throwable e) -> {
              handleReleaseLockResult(s,e,releaseLockTransition);
              return false;
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
        }).thenCompose((runSuccess) -> {
          // 4. Always release the lock
          return retryTransitionAsync(releaseLockTransition, Duration.ofMinutes(5))
              .handle((Boolean s,Throwable e) -> {
                handleReleaseLockResult(s,e,releaseLockTransition);
                return runSuccess;
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
    String randomHash = Long.toHexString(new Random().nextLong());
    CompletableFuture<Optional<LongLivedLock>> future = new CompletableFuture<>();

    // 1. Create and submit the lock request
    retryTransitionAsync(KeyValueStateMachine.createTryLockTransition(lockName, serverName, randomHash), Duration.ofSeconds(5)).thenAccept((success) -> {
      try {
        // 2. If we got the lock, then return an object representing that
        if (stateMachine.locks.containsKey(lockName) && stateMachine.locks.get(lockName).holder.map(h -> h.server.equals(serverName) && h.uniqueHash.equals(randomHash)).orElse(false)) {
          future.complete(Optional.of(new LongLivedLockImpl(lockName, randomHash, safetyReleaseWindow)));
        } else {
          future.complete(Optional.empty());
        }
      } catch (Throwable t) {
        // If something goes wrong, ensure we try to release the lock
        future.complete(Optional.empty());
        retryTransitionAsync(KeyValueStateMachine.createReleaseLockTransition(lockName, serverName, randomHash), Duration.ofMinutes(1));  // ensure we release the lock at all costs
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
    if (lock != null && lock.holder.isPresent()) {
      KeyValueStateMachine.LockRequest holder = lock.holder.get();
      return retryTransitionAsync(KeyValueStateMachine.createReleaseLockTransition(lockName, holder.server, holder.uniqueHash), Duration.ofSeconds(5));
    } else {
      return CompletableFuture.completedFuture(false);
    }
  }


  //////////////////////////////////////////////////////////////////
  // With element calls
  //////////////////////////////////////////////////////////////////

  /**
   * This is a little helper that's responsible for queueing up a release lock transition, if necessary
   */
  private void handleReleaseLockResult(Boolean success, Throwable error, byte[] releaseLockTransition) {
    if (error != null || success == null || !success) {  // note[gabor] pass the boolean as an object to prevent possible null pointer
      if (error != null) {
        log.warn("Release lock encountered an error: ", error);
      }
      synchronized (unreleasedLocks) {
        if (unreleasedLocks.size() < 1000) {
          log.warn("Could not release lock! Queueing for later deletion.");
          unreleasedLocks.add(releaseLockTransition);
        } else {
          log.error("Could not release a lock and did not queue it for later deletion (queue full)");
        }
      }
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
    // 1. Create and submit the lock request
    // 1.1. The lock request
    final String randomHash = UUID.randomUUID().toString();
    byte[] requestLockTransition = KeyValueStateMachine.createRequestLockTransition(elementName, serverName, randomHash);
    byte[] releaseLockTransition = KeyValueStateMachine.createReleaseLockTransition(elementName, serverName, randomHash);
    // 1.2. The lock release thunk
    Supplier<CompletableFuture<Boolean>> releaseLockWithoutChange = () -> retryTransitionAsync(releaseLockTransition, Duration.ofMinutes(5))
        .handle((Boolean s,Throwable e) -> {
          handleReleaseLockResult(s,e,releaseLockTransition);
          return false;
        });

    // 2. Get the lock, handling exceptions appropriately
    CompletableFuture<Boolean> exceptionProofFuture = new CompletableFuture<>();
    retryTransitionAsync(requestLockTransition, Duration.ofSeconds(5))
        .whenComplete((success, t) -> {
          if (t == null) {
            exceptionProofFuture.complete(success);
          } else {
            log.warn("Got an exception from transition retry: ", t);
            exceptionProofFuture.complete(false);
          }
        });

    // 3. Handle the withElement
    return exceptionProofFuture.thenCompose((success) -> {

      // 3.a. If we fail to submit the transition to get the lock, it is possible that we failed while waiting for the
      // transition to commit, but the transition is still out there. To be totally correct, we need to release the lock
      // here in the rare event that the request lock transition is still around and eventually gets committed.
      if (!success) {
        return releaseLockWithoutChange.get();
      } else {

        // 3.b. Otherwise, wait on the lock
        return stateMachine.createLockAcquiredFuture(elementName, serverName, randomHash).thenCompose((gotLock) -> {
          if (gotLock) {
            // Run our runnable, which returns a CompletableFuture
            try {
              // i. Get the object, if it is present
              Optional<byte[]> optionalObject = stateMachine.get(elementName, node.transport.now());

              CompletableFuture<Boolean> future = new CompletableFuture<>();
              pool.execute(() -> {
                // ii. Create the object, if it isn't present
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
                    releaseLockWithoutChange.get().thenAccept(future::complete);
                    return;
                  }
                } else {
                  log.warn("withElementAsync() object creator is null and there's nothing in the map. Returning failure");
                  releaseLockWithoutChange.get().thenAccept(future::complete);
                  return;
                }

                // iii. If we returned a null from creation, that's a signal to stop the withElement call
                if (object == null) {
                  releaseLockWithoutChange.get().thenAccept(future::complete);
                  return;
                }

                // iv. Mutate the object
                byte[] mutated;
                try {
                  mutated = mutator.apply(object);
                } catch (Throwable t) {
                  releaseLockWithoutChange.get().thenAccept(future::complete);
                  return;
                }

                // v. Put the object back into the map AND release the lock in a single transition
                if (newObject || (mutated != null && !Arrays.equals(object, mutated))) {  // only if there was a change, or if it's a new object
                  retryTransitionAsync(
                      KeyValueStateMachine.createGroupedTransition(createSetValueTransition(elementName, mutated, permanent), releaseLockTransition),
                      Duration.ofSeconds(30)).whenComplete((result, exception) -> {
                    if (result == null || !result) {
                      log.warn("Could not apply transition and/or release object lock: ", exception);
                      releaseLockWithoutChange.get().thenAccept(future::complete);
                    }
                    else future.complete(true); // SUCCESSFUL CASE
                  });
                } else {
                  // vi. If the mutator chose not to mutate the object, then this is trivially successful
                  releaseLockWithoutChange.get().thenAccept(future::complete);
                }
              });

              return future;
            } catch (Throwable t) {
              log.warn("Uncaught exception when mutating element in withElementAsync: ", t);
              return releaseLockWithoutChange.get();
            }
          } else {
            // Always release the lock just in case
            return releaseLockWithoutChange.get();
          }
        });
      }
    });
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
          retryTransitionAsync(createSetValueTransition(elementName, mutated, permanent), Duration.ofMinutes(5)).thenApply(future::complete);
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
   * thought about it much, is to use withElement(). Only use setElement() if you're really certain that you are the
   * only one in the cluster writing, or you don't mind being clobbered.
   *
   * @param elementName the name of the element to write
   * @param value the value to set the element to
   * @param permanent if false, the element will be automatically removed when we disconnect from the cluster, if we're
   *                  the last people to have edited the element. If true, the element will stick around forever.
   */
  @SuppressWarnings("unused")
  public CompletableFuture<Boolean> setElementRetryAsync(String elementName, byte[] value, boolean permanent, Duration timeout) {
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
  public CompletableFuture<Boolean> removeElementRetryAsync(String elementName, Duration timeout) {
    // Remove the object from the map
    return retryTransitionAsync(KeyValueStateMachine.createRemoveValueTransition(elementName), timeout);
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
      locks.put(entry.getKey(), entry.getValue().holder.map(x -> x.server).orElse("<none>"));
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
   *  RaftErrorHandler errorListener = (debugMessage, stackTrace) -> {
   *    // Do something with the debug message / stackTrace
   *    // Eg. Logging, or alerting via PagerDuty
   *  }
   *  addErrorListener(errorListener);
   *
   *  // Later in the code where there is an error
   *  throwRaftError(incident_key, debug_message);
   *
   * @param errorListener
   */
  public void addErrorListener(RaftErrorListener errorListener) {
    stateMachine.addErrorListener(errorListener);
    node.addErrorListener(errorListener);
    if (pool instanceof TrackedExecutorService) {
      ((TrackedExecutorService) pool).addErrorListener(errorListener);
    }
  }

  public void removeErrorListener(RaftErrorListener errorListener) {
    stateMachine.removeErrorListener(errorListener);
    node.removeErrorListener(errorListener);
    if (pool instanceof TrackedExecutorService) {
      ((TrackedExecutorService) pool).removeErrorListener(errorListener);
    }
  }

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
   * @return a CompletableFuture for the transition wrapped in retries
   */
  private CompletableFuture<Boolean> retryTransitionAsync(byte[] transition, Duration timeout) {
    int uniqueID = new Random().nextInt();
    long startTime = System.currentTimeMillis();
    log.trace("\n-------------\nSTARTING TRANSITION {}\n-------------\n", uniqueID);
    return retryAsync(() -> node.submitTransition(transition), timeout).thenApply((success) -> {
      log.trace("\n-------------\nFINISHED TRANSITION {}: {} ({})\n-------------\n", uniqueID, success, TimeUtils.formatTimeSince(startTime));
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
    future.whenComplete((success, throwable) -> {
      if (throwable != null) {
        log.warn("Caught an exception in an exception-proof future:", throwable);
        wrapper.complete(false);
      }
      else {
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
   * @return a CompletableFuture for the transition wrapped in retries
   */
  private CompletableFuture<Boolean> retryAsync(Supplier<CompletableFuture<Boolean>> action, Duration timeout) {
    long startTime = node.transport.now();

    // Create a transition future
    CompletableFuture<Boolean> future = exceptionProof(action.get());
    return future.thenCompose((result) -> {
      if (result) {
        // A. Case: the future was successful
        return CompletableFuture.completedFuture(true);
      } else {
        log.warn("Retrying a failed transition - this is fine, but should be rare");
        // B. Case: the future failed
        // B.1. Get the remaining time on the timeout
        long elapsed = node.transport.now() - startTime;
        long remainingTime = timeout.toMillis() - elapsed - 500;  // include the 500ms delay time before we try again
        // B.2. Check if there's still time on the timeout, and we haven't shut down the node yet
        if (remainingTime < 0 || !node.isAlive()) {
          return CompletableFuture.completedFuture(false);
        }
        // B.3. Retry if there's still time
        CompletableFuture<Boolean> ret = new CompletableFuture<>();
        node.transport.schedule(new SafeTimerTask() {
                                  @Override
                                  public void runUnsafe() {
                                    retryAsync(action, Duration.ofMillis(remainingTime)).thenApply(ret::complete);
                                  }
                                }, 500); // Wait 500ms before trying again, to avoid flooding the system with too many requests
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
