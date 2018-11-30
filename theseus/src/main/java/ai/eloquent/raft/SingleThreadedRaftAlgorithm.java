package ai.eloquent.raft;

import ai.eloquent.util.IdentityHashSet;
import ai.eloquent.util.RuntimeInterruptedException;
import ai.eloquent.util.SafeTimerTask;
import ai.eloquent.util.StackTrace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.Nonnull;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A {@link RaftAlgorithm} that wraps all of its calls in a queue to ensure that they
 * are executed single-threaded.
 *
 * @author <a href="mailto:gabor@eloquent.ai">Gabor Angeli</a>
 */
public class SingleThreadedRaftAlgorithm implements RaftAlgorithm {
  /**
   * An SLF4J Logger for this class.
   */
  private static final Logger log = LoggerFactory.getLogger(SingleThreadedRaftAlgorithm.class);

  /**
   * An enum for a given task's priority
   */
  private enum TaskPriority {
    CRITICAL,
    HIGH,
    LOW,
    ;
  }


  /**
   * A task that we're running synchronized on Raft. This is a runnable
   * and an exception handler.
   */
  private static class RaftTask {
    /** The runnable for the task */
    public final Runnable fn;
    /** The function to be called if we encounter an error */
    public final Consumer<Throwable> onError;
    /** The priority of this task. */
    public final TaskPriority priority;
    /** A human-readable name for this task. */
    public final String debugString;

    /** The straightforward constructor */
    private RaftTask(String debugString, TaskPriority priority, Runnable fn, Consumer<Throwable> onError) {
      this.fn = fn;
      this.onError = onError;
      this.debugString = debugString;
      this.priority = priority;
    }
  }


  /**
   * A Deque for {@linkplain RaftTask Raft Tasks} that handles different priorities
   * of messages.
   */
  private static class RaftDeque implements Deque<RaftTask> {

    /** Critical priority messages */
    private final ArrayDeque<RaftTask> criticalPriority = new ArrayDeque<>();
    /** High priority messages */
    private final ArrayDeque<RaftTask> highPriority = new ArrayDeque<>();
    /** Low (normal) priority messages */
    private final ArrayDeque<RaftTask> lowPriority = new ArrayDeque<>();

    @Override
    public void addFirst(RaftTask raftTask) {
      switch (raftTask.priority) {
        case CRITICAL:
          this.criticalPriority.addFirst(raftTask);
          break;
        case HIGH:
          this.highPriority.addFirst(raftTask);
          break;
        case LOW:
          this.lowPriority.addFirst(raftTask);
          break;
        default:
          throw new IllegalArgumentException("Unhandled priority " + raftTask.priority + " for task " + raftTask.debugString);
      }
    }

    @Override
    public void addLast(RaftTask raftTask) {
      switch (raftTask.priority) {
        case CRITICAL:
          this.criticalPriority.addLast(raftTask);
          break;
        case HIGH:
          this.highPriority.addLast(raftTask);
          break;
        case LOW:
          this.lowPriority.addLast(raftTask);
          break;
        default:
          throw new IllegalArgumentException("Unhandled priority " + raftTask.priority + " for task " + raftTask.debugString);
      }
    }

    @Override
    public boolean offerFirst(RaftTask raftTask) {
      switch (raftTask.priority) {
        case CRITICAL:
          return this.criticalPriority.offerFirst(raftTask);
        case HIGH:
          return this.highPriority.offerFirst(raftTask);
        case LOW:
          if (this.lowPriority.size() > 10000) {
            return false;
          }
          return this.lowPriority.offerFirst(raftTask);
        default:
          throw new IllegalArgumentException("Unhandled priority " + raftTask.priority + " for task " + raftTask.debugString);
      }
    }

    @Override
    public boolean offerLast(RaftTask raftTask) {
      switch (raftTask.priority) {
        case CRITICAL:
          return this.criticalPriority.offerLast(raftTask);
        case HIGH:
          return this.highPriority.offerLast(raftTask);
        case LOW:
          if (this.lowPriority.size() > 10000) {
            return false;
          }
          return this.lowPriority.offerLast(raftTask);
        default:
          throw new IllegalArgumentException("Unhandled priority " + raftTask.priority + " for task " + raftTask.debugString);
      }
    }

    @Override
    public RaftTask removeFirst() {
      if (criticalPriority.peekFirst() != null) {
        return criticalPriority.removeFirst();
      } else if (highPriority.peekFirst() != null) {
        return highPriority.removeFirst();
      } else {
        return lowPriority.removeFirst();
      }
    }

    @Override
    public RaftTask removeLast() {
      if (criticalPriority.peekLast() != null) {
        return criticalPriority.removeLast();
      } else if (highPriority.peekLast() != null) {
        return highPriority.removeLast();
      } else {
        return lowPriority.removeLast();
      }
    }

    @Nullable
    @Override
    public RaftTask pollFirst() {
      if (criticalPriority.peekFirst() != null) {
        return criticalPriority.pollFirst();
      } else if (highPriority.peekFirst() != null) {
        return highPriority.pollFirst();
      } else {
        return lowPriority.pollFirst();
      }
    }

    @Nullable
    @Override
    public RaftTask pollLast() {
      if (criticalPriority.peekLast() != null) {
        return criticalPriority.pollLast();
      } else if (highPriority.peekLast() != null) {
        return highPriority.pollLast();
      } else {
        return lowPriority.pollLast();
      }
    }

    @Override
    public RaftTask getFirst() {
      if (criticalPriority.peekFirst() != null) {
        return criticalPriority.getFirst();
      } else if (highPriority.peekFirst() != null) {
        return highPriority.getFirst();
      } else {
        return lowPriority.getFirst();
      }
    }

    @Override
    public RaftTask getLast() {
      if (criticalPriority.peekLast() != null) {
        return criticalPriority.getLast();
      } else if (highPriority.peekLast() != null) {
        return highPriority.getLast();
      } else {
        return lowPriority.getLast();
      }
    }

    @Override
    public RaftTask peekFirst() {
      if (criticalPriority.peekFirst() != null) {
        return criticalPriority.peekFirst();
      } else if (highPriority.peekFirst() != null) {
        return highPriority.peekFirst();
      } else {
        return lowPriority.peekFirst();
      }
    }

    @Override
    public RaftTask peekLast() {
      if (criticalPriority.peekLast() != null) {
        return criticalPriority.peekLast();
      } else if (highPriority.peekLast() != null) {
        return highPriority.peekLast();
      } else {
        return lowPriority.peekLast();
      }
    }

    @Override
    public boolean removeFirstOccurrence(Object o) {
      return criticalPriority.removeFirstOccurrence(o) ||
          highPriority.removeFirstOccurrence(o) ||
          lowPriority.removeFirstOccurrence(o);
    }

    @Override
    public boolean removeLastOccurrence(Object o) {
      return lowPriority.removeLastOccurrence(o) ||
          highPriority.removeLastOccurrence(o) ||
          criticalPriority.removeLastOccurrence(o);
    }

    @Override
    public boolean add(RaftTask raftTask) {
      this.addLast(raftTask);
      return true;
    }

    @Override
    public boolean offer(RaftTask raftTask) {
      return this.offerLast(raftTask);
    }

    @Override
    public RaftTask remove() {
      return this.removeFirst();
    }

    @Override
    public RaftTask poll() {
      return this.pollFirst();
    }

    @Override
    public RaftTask element() {
      return this.getFirst();
    }

    @Override
    public RaftTask peek() {
      return this.peekFirst();
    }

    @Override
    public void push(RaftTask raftTask) {
      this.addFirst(raftTask);
    }

    @Override
    public RaftTask pop() {
      return this.removeFirst();
    }

    @Override
    public boolean remove(Object o) {
      return this.removeFirstOccurrence(o);
    }

    @Override
    public boolean containsAll(@Nonnull Collection<?> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(@Nonnull Collection<? extends RaftTask> c) {
      for (RaftTask t : c) {
        this.add(t);
      }
      return true;
    }

    @Override
    public boolean removeAll(@Nonnull Collection<?> c) {
      for (Object t : c) {
        this.remove(t);
      }
      return true;
    }

    @Override
    public boolean retainAll(@Nonnull Collection<?> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
      this.criticalPriority.clear();
      this.highPriority.clear();
      this.lowPriority.clear();

    }

    @Override
    public boolean contains(Object o) {
      return this.criticalPriority.contains(o) ||
          this.highPriority.contains(o) ||
          this.lowPriority.contains(o);
    }

    @Override
    public int size() {
      return this.criticalPriority.size() +
          this.highPriority.size() +
          this.lowPriority.size();
    }

    @Override
    public boolean isEmpty() {
      return this.criticalPriority.isEmpty() &&
          this.highPriority.isEmpty() &&
          this.lowPriority.isEmpty();
    }

    @Nonnull
    @Override
    public Iterator<RaftTask> iterator() {
      Deque<RaftTask> all = new ArrayDeque<>(this.criticalPriority);
      all.addAll(this.highPriority);
      all.addAll(this.lowPriority);
      return all.iterator();
    }

    @Nonnull
    @Override
    public Object[] toArray() {
      throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public <T> T[] toArray(@Nonnull T[] a) {
      throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public Iterator<RaftTask> descendingIterator() {
      throw new UnsupportedOperationException();
    }
  }


  /**
   * The Raft algorithm we're actually running, wrapped in a single-threaded
   * environment.
   */
  public final RaftAlgorithm impl;

  /**
   * The thread that's going to be driving the Raft algorith,
   */
  @SuppressWarnings("FieldCanBeLocal")
  private final Thread raftThread;

  /**
   * The {@link RaftTask#debugString} of the currently running task.
   * This is used primarily in {@link #flush(Runnable)} to make
   * sure we don't mark ourselves as flushed if there's a task
   * currently in progress.
   */
  private Optional<String> taskRunning = Optional.empty();

  /**
   * The marker for whether the raft algorithm is alive.
   */
  private boolean alive = true;

  /**
   * The queue of tasks for Raft to pick up on.
   */
  private final RaftDeque raftTasks = new RaftDeque();

  /**
   * The count of futures that we're still waiting on
   */
  private final Set<CompletableFuture> waitingForFutures = new IdentityHashSet<>();

  /**
   * The pool that'll be used to run any Future that can see into the outside world.
   * This is often the same as the pool in {@link RaftLog}.
   */
  private final ExecutorService boundaryPool;

  /**
   * JUST FOR TESTS: Only used in LocalTransport
   *
   * We need this in order to prevent time from slipping while boundary pool threads are created but have not yet
   * started.
   */
  public static final AtomicInteger boundaryPoolThreadsWaiting = new AtomicInteger(0);


  /** @see RaftTransport#threadsCanBlock() */
  private final boolean threadsCanBlock;


  /**
   * Create a single-thread driven Raft algorithm from an implementing instance.
   *
   * @param impl The implemeting algorithm. See {@link #impl}.
   * @param boundaryPool The boundary pool. See {@link #boundaryPool}.
   */
  public SingleThreadedRaftAlgorithm(RaftAlgorithm impl, ExecutorService boundaryPool) {
    this.impl = impl;
    this.threadsCanBlock = impl.getTransport().threadsCanBlock();
    this.raftThread = new Thread( () -> {
      if (impl instanceof EloquentRaftAlgorithm) {
        ((EloquentRaftAlgorithm) impl).setDrivingThread(r -> {
          synchronized (raftTasks) {
            raftTasks.offer(new RaftTask("EloquentRaftAlgorithm Callback", TaskPriority.CRITICAL, r, e -> log.warn("Error in queued task", e)));
            raftTasks.notifyAll();
          }
        });
      }
      try {
        while (alive) {
          RaftTask task;
          try {
            synchronized (raftTasks) {
              taskRunning = Optional.empty();
              raftTasks.notifyAll();
              while (raftTasks.isEmpty()) {
                raftTasks.wait(1000);
                if (!alive) {
                  return;
                }
              }
              task = raftTasks.poll();
              taskRunning = Optional.of(task.debugString);
            }
            try {
              task.fn.run();
            } catch (Throwable t) {
              task.onError.accept(t);
            }
          } catch (Throwable t) {
            log.warn("Caught exception ", t);
          }
        }
      } finally {
        synchronized (raftTasks) {
          // Clean up any leftovers
          raftTasks.forEach(t -> t.onError.accept(new RuntimeException("SingleThreadedRaftAlgorithm main thread killed from killMainThread(), so this will never complete")));
          raftTasks.clear();
          waitingForFutures.forEach(completableFuture -> completableFuture.completeExceptionally(new RuntimeException("SingleThreadedRaftAlgorithm main thread killed from killMainThread(), so this will never complete")));
          waitingForFutures.clear();
        }
      }
    });
    this.raftThread.setPriority(Math.max(Thread.MIN_PRIORITY, Thread.MAX_PRIORITY - 2));
    this.raftThread.setDaemon(false);
    this.raftThread.setName("raft-control-" + impl.serverName());
    this.raftThread.setUncaughtExceptionHandler((t, e) -> log.warn("Caught exception on {}:", t.getName(), e));
    this.raftThread.start();
    this.boundaryPool = boundaryPool;
  }


  /**
   * Return the number of tasks we have queued to be executed by Raft.
   */
  public int queuedTaskCount() {
    synchronized (this.raftTasks) {
      return this.raftTasks.size();
    }
  }


  /**
   * Run a given function, returning a completable future for when this function is complete.
   * Note that this completable future completes <b>on the raft thread</b>, and therefore
   * should not be exposed to the outside world.
   *
   * @param debugName A debug name for this task.
   * @param priority The priority for this task
   * @param fn The function we are running. Typically, a {@link RaftAlgorithm} method.
   * @param <E> The return type of our function.
   *
   * @return A future for tracking when we actually have finished scheduling and running
   *         this function.
   */
  private <E> CompletableFuture<E> execute(String debugName, TaskPriority priority, Function<RaftAlgorithm, E> fn) {
    log.trace("{} - [{}] Executing as Future {}", this.serverName(), getTransport().now(), debugName);
    if (Thread.currentThread() == raftThread) {  // don't queue if we're on the raft thread
      return CompletableFuture.completedFuture(fn.apply(this.impl));
    }
    if (!alive) {
      throw new IllegalStateException("Node is dead -- failing the future");
    }
    CompletableFuture<E> future = new CompletableFuture<>();
    Runnable task = () -> future.complete(fn.apply(this.impl));
    Consumer<Throwable> onError = future::completeExceptionally;
    synchronized (raftTasks) {
      raftTasks.offer(new RaftTask(debugName, priority, task, onError));
      raftTasks.notifyAll();
    }
    return future;
  }


  /**
   * Run a given function, returning a completable future for when this function is complete.
   * Unlike {@link #execute(String, TaskPriority, Function)}, this returns a <b>safe future to be show to the
   * outside world</b>.
   *
   * @param debugName A debug name for this task.
   * @param priority The priority for this task
   * @param fn The function we are running. Typically, a {@link RaftAlgorithm} method.
   * @param <E> The return type of our function.
   *
   * @return A future for tracking when we actually have finished scheduling and running
   *         this function.
   */
  private <E> CompletableFuture<E> executeFuture(String debugName, TaskPriority priority, Function<RaftAlgorithm, CompletableFuture<E>> fn) {
    // 1. Check if we should execute directly
    log.trace("{} - [{}] Executing as Composite Future {}", this.serverName(), getTransport().now(), debugName);
    if (!alive) {
      throw new IllegalStateException("Node is dead -- failing the future");
    }
    if (Thread.currentThread().getId() == raftThread.getId()) {  // don't queue if we're on the raft thread
      return fn.apply(this.impl);
    }
    CompletableFuture<E> future = new CompletableFuture<>();

    // 2. Define the timeout for the future
    CompletableFuture<CompletableFuture<E>> futureOfFuture = execute(debugName, priority, fn);
    final SafeTimerTask timeoutResult = new SafeTimerTask() {
      @Override
      public void runUnsafe() {
        CompletableFuture<E> result = futureOfFuture.getNow(null);
        if (result == null) {
          // note[gabor]: OK to fail on this thread; they'll defer to boundary pool in the exception handling below
          futureOfFuture.completeExceptionally(new TimeoutException("Timed out executeFuture() (never got future)"));
        } else if (!result.isDone()) {
          // note[gabor]: OK to fail on this thread; they'll defer to boundary pool in the exception handling below
          result.completeExceptionally(new TimeoutException("Timed out executeFuture() (never completed future)"));
        }
      }
    };

    synchronized (raftTasks) {
      waitingForFutures.add(futureOfFuture);
    }
    futureOfFuture.whenComplete((CompletableFuture<E> result, Throwable t) ->
      execute(debugName, priority, raft -> {  // ensure that we're on the controller thread
        // note: this must be running on the Raft control thread
        if (Thread.currentThread().getId() != raftThread.getId()) {
          log.warn("Future of future should be completing on the Raft control thread; running on {} instead", Thread.currentThread());
        }
        // 3. Check our future
        // 3.1. Check that we got our future OK from the Raft main thread
        if (t != null) {
          boundaryPoolThreadsWaiting.incrementAndGet();  // see canonical deadlock below -- we need to handle it here as well
          boundaryPool.submit(() -> {
            try {
              future.completeExceptionally(t);
            } finally {
              boundaryPoolThreadsWaiting.decrementAndGet();
            }
          });
          return;
        }
        // 3.2. Register our future appropriately
        synchronized (raftTasks) {
          waitingForFutures.remove(futureOfFuture);
          waitingForFutures.add(result);
        }


        // 4. Register the completion on the boundary pool
        result.whenComplete((E r, Throwable t2) -> {
          // note: this is likely running on the Raft control thread
          if (t2 == null && Thread.currentThread().getId() != raftThread.getId()) {  // ok to fail on timer thread -- we defer to boundary thread below
            log.warn("Future of future's implementation should be completing on the Raft control thread; running on {} instead", Thread.currentThread().getId());
          }
          // 4.1. Cancel the timeout
          synchronized (timeoutResult) {
            timeoutResult.cancel();
          }
          // JUST FOR TESTS: this helps resolve a deadlock detailed below
          boundaryPoolThreadsWaiting.incrementAndGet();
          // There's a race condition here that's tricky and hard to remove - and only shows up in the tests
          // The time between the above line ^ and the below line v must be 0 for the tests, but of course can't be.
          //
          // EXAMPLE: If we make an RPC call from a follower to the leader, all the network messages can propagate around
          // synchronously, and we'll still end up timing out the RPC call because time can slip before the boundary pool
          // task wakes up.
          //
          // The solution is to have LocalTransport's timekeeper thread spin till SingleThreadedRaftAlgorithm.boundaryPoolThreadsWaiting is 0.
          //
          // 4.2. Define the function to complete the future
          Runnable completeFuture = () -> {  // make sure the future is run from the boundary pool
            try {
              if (r != null) {
                future.complete(r);
              } else if (t2 != null) {
                future.completeExceptionally(t2);
              } else {
                log.warn("whenComplete() called with a null result and a null exception, this should be impossible!");
                future.completeExceptionally(new RuntimeException("This should be impossible!"));
              }
            } finally {
              synchronized (raftTasks) {
                waitingForFutures.remove(result);
              }
              boundaryPoolThreadsWaiting.decrementAndGet();
            }
          };
          // 4.3. Schedule the completion on the pool
          try {
            boundaryPool.submit(completeFuture);
          } catch (Throwable boundaryPoolError) {
            log.error("We got an exception submitting a task to the boundary pool from SingleThreadedRaftAlgorithm. Falling back to a daemon thread.", boundaryPoolError);
            Thread thread = new Thread(completeFuture);
            thread.setDaemon(true);
            thread.setName("boundary-pool-fallback");
            thread.setPriority(Thread.NORM_PRIORITY);
            thread.start();
          }
        });
      })
    );

    // 5. Schedule the timeout
    try {
      synchronized (timeoutResult) {
        if (!timeoutResult.cancelled) {
          getTransport().schedule(timeoutResult, impl.electionTimeoutMillisRange().end + 100);
        }
      }
    } catch (Throwable timeoutError) {
      log.warn("Could not schedule timeout future: ", timeoutError);
    }

    // 6. Return
    return future;
  }


  /**
   * Run a given function, dumping the result into the void.
   * This is useful for void return type methods on a {@link RaftAlgorithm}.
   *
   * @param debugName A debug name for this task.
   * @param priority The priority for this task
   * @param fn The function we are running. Typically, a {@link RaftAlgorithm} method.
   */
  private void execute(String debugName, TaskPriority priority, Consumer<RaftAlgorithm> fn) {
    log.trace("{} - [{}] Executing {}", this.serverName(), getTransport().now(), debugName);
    if (Thread.currentThread().getId() == raftThread.getId()) {  // don't queue if we're on the raft thread
      fn.accept(this.impl);
      return;
    }
    AtomicBoolean done = new AtomicBoolean(false);
    synchronized (raftTasks) {
      if (!alive) {
        log.debug("Node is dead -- ignoring any messages to it");
        return;
      }
      if (!raftTasks.offer(new RaftTask(debugName, priority,
          () -> {
            try {
              fn.accept(this.impl);
            } finally {
              synchronized (done) {
                done.set(true);
                done.notifyAll();
              }
            }
          },
          (e) -> {
            log.warn("Got exception running Raft method {}", debugName, e);
            synchronized (done) {
              done.set(true);
              done.notifyAll();
            }
          }))) {
        log.warn("Dropping task {} due to size constraints (queue size={})", debugName, raftTasks.size());
      }
      raftTasks.notifyAll();
    }

    if (this.threadsCanBlock) {
      synchronized (done) {
        if (!done.get()) {
          try {
            done.wait(1000);
          } catch (InterruptedException e) {
            log.warn("Task seems to be backed up");
          }
        }
      }
    }
  }


  /** {@inheritDoc} */
  @Override
  public RaftState state() {
    try {
      return execute("state", TaskPriority.LOW, RaftAlgorithm::state).get(30, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      log.warn("Could not get RaftState -- returning unlocked version as a failsafe");
      return impl.state();
    }
  }


  /** {@inheritDoc} */
  @Override
  public RaftState mutableState() {
    return impl.mutableState();  // note[gabor] don't run as a future -- this is mutable anyways
  }


  /** {@inheritDoc} */
  @Override
  public RaftStateMachine mutableStateMachine() {
    return impl.mutableStateMachine();  // note[gabor] don't run as a future -- this is mutable anyways
  }


  /** {@inheritDoc} */
  @Override
  public long term() {
    return impl.term();
  }


  /** {@inheritDoc} */
  @Override
  public String serverName() {
    return impl.serverName();  // note[gabor] don't run as a future -- this should never change
  }


  /** {@inheritDoc} */
  @Override
  public void broadcastAppendEntries() {
    execute("broadcastAppendEntries", TaskPriority.HIGH, RaftAlgorithm::broadcastAppendEntries);
  }


  /** {@inheritDoc} */
  @Override
  public void sendAppendEntries(String target, long nextIndex) {
    execute("sendAppendEntries", TaskPriority.HIGH,(Consumer<RaftAlgorithm>) x -> x.sendAppendEntries(target, nextIndex));
  }


  /** {@inheritDoc} */
  @Override
  public void receiveAppendEntriesRPC(EloquentRaftProto.AppendEntriesRequest heartbeat, Consumer<EloquentRaftProto.RaftMessage> replyLeader) {
    execute("receiveAppendEntriesRPC", TaskPriority.HIGH, (Consumer<RaftAlgorithm>) x -> x.receiveAppendEntriesRPC(heartbeat, replyLeader));

  }


  /** {@inheritDoc} */
  @Override
  public void receiveAppendEntriesReply(EloquentRaftProto.AppendEntriesReply reply) {
    execute("receiveAppendEntriesReply", TaskPriority.HIGH, (Consumer<RaftAlgorithm>) x -> x.receiveAppendEntriesReply(reply));
  }


  /** {@inheritDoc} */
  @Override
  public void receiveInstallSnapshotRPC(EloquentRaftProto.InstallSnapshotRequest snapshot, Consumer<EloquentRaftProto.RaftMessage> replyLeader) {
    execute("receiveInstallSnapshotRPC", TaskPriority.HIGH, (Consumer<RaftAlgorithm>) x -> x.receiveInstallSnapshotRPC(snapshot, replyLeader));
  }


  /** {@inheritDoc} */
  @Override
  public void receiveInstallSnapshotReply(EloquentRaftProto.InstallSnapshotReply reply) {
    execute("receiveInstallSnapshotReply", TaskPriority.HIGH, (Consumer<RaftAlgorithm>) x -> x.receiveInstallSnapshotReply(reply));
  }


  /** {@inheritDoc} */
  @Override
  public void triggerElection() {
    execute("triggerElection", TaskPriority.LOW, RaftAlgorithm::triggerElection);
  }


  /** {@inheritDoc} */
  @Override
  public void receiveRequestVoteRPC(EloquentRaftProto.RequestVoteRequest voteRequest, Consumer<EloquentRaftProto.RaftMessage> replyLeader) {
    execute("receiveRequestVoteRPC", TaskPriority.CRITICAL, (Consumer<RaftAlgorithm>) x -> x.receiveRequestVoteRPC(voteRequest, replyLeader));
  }


  /** {@inheritDoc} */
  @Override
  public void receiveRequestVotesReply(EloquentRaftProto.RequestVoteReply reply) {
    execute("receiveRequestVotesReply", TaskPriority.CRITICAL, (Consumer<RaftAlgorithm>) x -> x.receiveRequestVotesReply(reply));
  }


  /** {@inheritDoc} */
  @Override
  public CompletableFuture<EloquentRaftProto.RaftMessage> receiveAddServerRPC(EloquentRaftProto.AddServerRequest addServerRequest) {
    return executeFuture("receiveAddServerRPC", TaskPriority.LOW, x -> x.receiveAddServerRPC(addServerRequest));
  }


  /** {@inheritDoc} */
  @Override
  public CompletableFuture<EloquentRaftProto.RaftMessage> receiveRemoveServerRPC(EloquentRaftProto.RemoveServerRequest removeServerRequest) {
    return executeFuture("receciveRemoveServerRPC", TaskPriority.HIGH, x -> x.receiveRemoveServerRPC(removeServerRequest));
  }


  /** {@inheritDoc} */
  @Override
  public CompletableFuture<EloquentRaftProto.RaftMessage> receiveApplyTransitionRPC(EloquentRaftProto.ApplyTransitionRequest transition) {
    return executeFuture("receiveApplyTransitionRPC", TaskPriority.LOW, x -> x.receiveApplyTransitionRPC(transition));
  }


  /** {@inheritDoc} */
  @Override
  public boolean bootstrap(boolean force) {
    try {
      return execute("bootstrap", TaskPriority.CRITICAL, (Function<RaftAlgorithm, Boolean>) x -> x.bootstrap(force)).get(30, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      log.warn("Could not bootstrap -- returning unlocked version as a failsafe");
      return impl.bootstrap(force);
    }
  }


  /** {@inheritDoc} */
  @Override
  public void stop(boolean kill) {
    // Call the implementing algorithm's stop
    execute("stop", TaskPriority.LOW, (Consumer<RaftAlgorithm>) x -> x.stop(kill));
    flush(() -> {});

    // Stop our thread
    synchronized (raftTasks) {
      this.alive = false;
      this.boundaryPool.shutdown();
      // Wake up the main thread so it can die
      this.raftTasks.notifyAll();
      this.waitingForFutures.forEach(completableFuture -> completableFuture.completeExceptionally(new RuntimeException("killMainThread() killed this future")));
    }
  }


  /** {@inheritDoc} */
  @Override
  public boolean isRunning() {
    try {
      return execute("isRunning", TaskPriority.LOW, RaftAlgorithm::isRunning).get(30, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      log.warn("Could not check if Raft is running -- returning unlocked version as a failsafe");
      return impl.isRunning();
    }
  }


  /** {@inheritDoc} */
  @Override
  public void heartbeat() {
    execute("heartbeat", TaskPriority.HIGH, RaftAlgorithm::heartbeat);
  }


  /** {@inheritDoc} */
  @Override
  public void receiveBadRequest(EloquentRaftProto.RaftMessage message) {
    execute("receiveBadRequest", TaskPriority.LOW, (Consumer<RaftAlgorithm>) x -> x.receiveBadRequest(message));
  }


  /** {@inheritDoc} */
  @Override
  public Optional<RaftLifecycle> lifecycle() {
    return impl.lifecycle();  // note[gabor] don't run as a future -- this should be final
  }

  @Override
  public RaftTransport getTransport() {
    return impl.getTransport();
  }


  /**
   * Flush the task queue. This is useful primarily for unit tests where
   * we're mocking time.
   *
   * @param additionalCriteria A function to run once things are flushed, after
   *                           which we should flush again. That is, make sure both
   *                           the transport is flushed, and this additional criteria
   *                           is also met (i.e., has run) when the algorithm is flushed.
   */
  public void flush(Runnable additionalCriteria) {
    boolean isEmpty;
    // 1. Run the critera and check for emptiness
    additionalCriteria.run();
    synchronized (this.raftTasks) {
      isEmpty = this.raftTasks.isEmpty() && !this.taskRunning.isPresent();
    }

    // 2. Our loop
    while (!isEmpty) {
      // 2.1. Flush
      synchronized (this.raftTasks) {
        while (!this.raftTasks.isEmpty()) {
          try {
            this.raftTasks.wait(100);
          } catch (InterruptedException e) {
            throw new RuntimeInterruptedException(e);
          }
        }
      }
      // 2.2. Rerun criteria
      additionalCriteria.run();
      synchronized (this.raftTasks) {
        isEmpty = this.raftTasks.isEmpty() && !this.taskRunning.isPresent();
      }
    }
  }


  /**
   * Get errors from this Raft algorithm
   */
  public List<String> errors() {
    List<String> errors = new ArrayList<>();

    // 1. Check queued tasks
    int queuedTasks = this.queuedTaskCount();
    if (queuedTasks > 5) {
      errors.add("" + queuedTasks + " tasks queued on Raft control thread (> threshold of 5)." +
          " Running task is '" + this.taskRunning.orElse("<unknown>") + "'" +
          " with a stack trace of:\n" + new StackTrace(this.raftThread.getStackTrace())
      );
    }

    // 2. Get algorithm errors
    if (impl instanceof EloquentRaftAlgorithm) {
      if (Thread.currentThread().getId() == raftThread.getId()) {  // don't queue if we're on the raft thread
        return ((EloquentRaftAlgorithm) impl).errors();
      }
      CompletableFuture<List<String>> future = new CompletableFuture<>();
      Runnable task = () -> future.complete(((EloquentRaftAlgorithm) this.impl).errors());
      Consumer<Throwable> onError = future::completeExceptionally;
      synchronized (raftTasks) {
        raftTasks.offer(new RaftTask("errors", TaskPriority.LOW, task, onError));
        raftTasks.notifyAll();
      }
      try {
        errors.addAll(future.get(10, TimeUnit.SECONDS));
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        errors.add("Could not get errors from implementing algorithm");
      }
    }

    // Return
    return errors;
  }

  /**
   * Kill this Raft on GC
   */
  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    stop(true);
  }
}
