package ai.eloquent.raft;

import ai.eloquent.monitoring.Prometheus;
import ai.eloquent.util.*;
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
   * The amount of time to wait if the buffer is full before trying again.
   */
  private static final long MAX_DELAY = 50;

  /**
   * The time, in seconds, that a task sits on the queue before being run.
   */
  private static final Object HISTOGRAM_QUEUE_TIME =
      Prometheus.histogramBuild("single_threaded_raft_queuetime", "The time, in seconds, that a task sits on the queue before being run.",
          0, 0.1 / 1000.0, 0.5 / 1000.0, 1.0 / 1000.0, 5.0 / 1000.0, 10.0 / 1000.0, 50.0 / 1000.0, 100.0 / 1000.0, 200.0 / 1000.0, 300.0 / 1000.0, 1.0 / 2.0, 1.0, 10.0);

  /**
   * The time, in seconds, that it takes to run a task on the raft algorithm
   */
  private static final Object HISTOGRAM_RUN_TIME =
      Prometheus.histogramBuild("single_threaded_raft_runtime", "The time, in seconds, that it takes to run a task on the raft algorithm",
          0, 0.1 / 1000.0, 0.5 / 1000.0, 1.0 / 1000.0, 5.0 / 1000.0, 10.0 / 1000.0, 50.0 / 1000.0, 100.0 / 1000.0, 200.0 / 1000.0, 300.0 / 1000.0, 1.0 / 2.0, 1.0, 10.0);


  /**
   * The priority for queueing a task.
   */
  private enum OfferPolicy {
    /** Forces this task on the queue, regardless of the length of the queue. */
    FORCE,
    /** Blocks until the queue is sufficiently short that we can add it without overflowing the queue length. */
    BLOCK,
    /** Offers to the queue, discarding the task if the queue is too long. */
    OPTIONAL
  }

  /**
   * An enum for a given task's priority
   */
  enum TaskPriority {
    CRITICAL,
    HIGH,
    LOW,
    ;
  }


  /**
   * A task that we're running synchronized on Raft. This is a runnable
   * and an exception handler.
   */
  static class RaftTask {
    /** The runnable for the task */
    public final Runnable fn;
    /** The function to be called if we encounter an error */
    public final Consumer<Throwable> onError;
    /** The priority of this task. */
    public final TaskPriority priority;
    /** A human-readable name for this task. */
    public final String debugString;
    /** The timestamp, in system nanos, when this task was queued. @see System#nanoTime() */
    public final long queuedTimestamp;
    /** If true, this is a heartbeat task */
    private final boolean isHeartbeat;

    /** The straightforward constructor */
    RaftTask(String debugString, TaskPriority priority, Runnable fn, Consumer<Throwable> onError) {
      this(debugString, priority, fn, onError, false);
    }

    /** The straightforward constructor, including heartbeat marker */
    RaftTask(String debugString, TaskPriority priority, Runnable fn, Consumer<Throwable> onError, boolean isHeartbeat) {
      this.fn = fn;
      this.onError = onError;
      this.debugString = debugString;
      this.priority = priority;
      this.queuedTimestamp = System.nanoTime();
      this.isHeartbeat = isHeartbeat;
      assert !isHeartbeat || "heartbeat".equals(debugString) : "Only the 'heartbeat' task should be a heartbeat";
      assert isHeartbeat || !"heartbeat".equals(debugString) : "The heartbeat task should be called 'heartbeat'";
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
      return debugString;
    }
  }


  /**
   * A Deque for {@linkplain RaftTask Raft Tasks} that handles different priorities
   * of messages.
   */
  static class RaftDeque implements Deque<RaftTask> {
    /** To save our logs from spamming the "spent too long waiting" message */
    long lastWarn = 0L;
    /** The number of warnings we didn't show. */
    int maskedWarnings = 0;
    /** The maximum time we delayed in a masked warning. */
    long maxDelayInMaskedWarnings = 0;

    /**
     * The maximum number of tasks to keep in the queue.
     * More than this, and we start blocking on adding new tasks.
     * This should, roughly, be the number of concurrent transitions we want to support.
     */
    static final int MAX_SIZE = 32;

    /** Critical priority messages */
    private final ArrayDeque<RaftTask> criticalPriority = new ArrayDeque<>();
    /** High priority messages */
    private final ArrayDeque<RaftTask> highPriority = new ArrayDeque<>();
    /** Low (normal) priority messages */
    private final ArrayDeque<RaftTask> lowPriority = new ArrayDeque<>();

    /**
     * Ensure that we actually have the capacity to add to the Deque
     */
    private synchronized void ensureCapacity(TaskPriority priority) {
      // Wait for the queue to free up
      long startTime = System.currentTimeMillis();
      while (! (this.size() < MAX_SIZE || (priority == TaskPriority.CRITICAL && criticalPriority.isEmpty())) ) {
        try {
          this.wait(MAX_DELAY);
        } catch (InterruptedException e) {
          throw new RuntimeInterruptedException(e);
        }
      }
      long endTime = System.currentTimeMillis();

      // 2. A lot of stupid code for logging
      if (endTime - startTime > 100) {
        if (endTime - lastWarn > 10000) {
          log.warn("Spent {} waiting for Raft task to queue (priority {}; squashed {} similar messages w/max delay of {})",
              TimerUtils.formatTimeDifference(endTime - startTime), priority, maskedWarnings,
              TimerUtils.formatTimeDifference(maxDelayInMaskedWarnings));
          maskedWarnings = 0;
          maxDelayInMaskedWarnings = 0;
          lastWarn = endTime;
        } else {
          maskedWarnings += 1;
          maxDelayInMaskedWarnings = Math.max(maxDelayInMaskedWarnings, endTime - startTime);
        }
      }
    }

    /** {@inheritDoc} */
    @Override
    public synchronized void addFirst(RaftTask raftTask) {
      ensureCapacity(raftTask.priority);
      try {
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
      } finally {
        this.notifyAll();
      }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("Duplicates")
    @Override
    public synchronized void addLast(RaftTask raftTask) {
      ensureCapacity(raftTask.priority);
      try {
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
      } finally {
        this.notifyAll();
      }
    }

    /** {@inheritDoc} */
    @Override
    public synchronized boolean offerFirst(RaftTask raftTask) {
      if (! (this.size() < MAX_SIZE || (raftTask.priority == TaskPriority.CRITICAL && criticalPriority.isEmpty())) ) {
        return false;
      }
      try {
        switch (raftTask.priority) {
          case CRITICAL:
            if (raftTask.isHeartbeat && this.criticalPriority.stream().anyMatch(x -> x.isHeartbeat)) {
              return false;
            } else {
              return this.criticalPriority.offerFirst(raftTask);
            }
          case HIGH:
            return this.highPriority.offerFirst(raftTask);
          case LOW:
            return this.lowPriority.offerFirst(raftTask);
          default:
            throw new IllegalArgumentException("Unhandled priority " + raftTask.priority + " for task " + raftTask.debugString);
        }
      } finally {
        this.notifyAll();
      }
    }

    /** {@inheritDoc} */
    @Override
    public synchronized boolean offerLast(RaftTask raftTask) {
      if (! (this.size() < MAX_SIZE || (raftTask.priority == TaskPriority.CRITICAL && criticalPriority.isEmpty())) ) {
        return false;
      }
      try {
        switch (raftTask.priority) {
          case CRITICAL:
            if (raftTask.isHeartbeat && this.criticalPriority.stream().anyMatch(x -> x.isHeartbeat)) {
              return false;
            } else {
              return this.criticalPriority.offerLast(raftTask);
            }
          case HIGH:
            return this.highPriority.offerLast(raftTask);
          case LOW:
            return this.lowPriority.offerLast(raftTask);
          default:
            throw new IllegalArgumentException("Unhandled priority " + raftTask.priority + " for task " + raftTask.debugString);
        }
      } finally {
        this.notifyAll();
      }
    }

    /** {@inheritDoc} */
    @Override
    public synchronized RaftTask removeFirst() {
      try {
        if (criticalPriority.peekFirst() != null) {
          return criticalPriority.removeFirst();
        } else if (highPriority.peekFirst() != null) {
          return highPriority.removeFirst();
        } else {
          return lowPriority.removeFirst();
        }
      } finally {
        this.notifyAll();
      }
    }

    /** {@inheritDoc} */
    @Override
    public synchronized RaftTask removeLast() {
      try {
        if (criticalPriority.peekLast() != null) {
          return criticalPriority.removeLast();
        } else if (highPriority.peekLast() != null) {
          return highPriority.removeLast();
        } else {
          return lowPriority.removeLast();
        }
      } finally {
        this.notifyAll();
      }
    }

    /** {@inheritDoc} */
    @Nullable
    @Override
    public synchronized RaftTask pollFirst() {
      try {
        RaftTask elem;
        if ((elem = criticalPriority.pollFirst()) != null) {
          return elem;
        } else if ((elem = highPriority.pollFirst()) != null) {
          return elem;
        } else {
          return lowPriority.pollFirst();
        }
      } finally {
        this.notifyAll();
      }
    }

    /** {@inheritDoc} */
    @Nullable
    @Override
    public synchronized RaftTask pollLast() {
      try {
        RaftTask elem;
        if ((elem = criticalPriority.pollLast()) != null) {
          return elem;
        } else if ((elem = highPriority.pollLast()) != null) {
          return elem;
        } else {
          return lowPriority.pollLast();
        }
      } finally {
        this.notifyAll();
      }
    }

    /** {@inheritDoc} */
    @Override
    public synchronized RaftTask getFirst() {
      try {
        if (criticalPriority.peekFirst() != null) {
          return criticalPriority.getFirst();
        } else if (highPriority.peekFirst() != null) {
          return highPriority.getFirst();
        } else {
          return lowPriority.getFirst();
        }
      } finally {
        this.notifyAll();
      }
    }

    /** {@inheritDoc} */
    @Override
    public synchronized RaftTask getLast() {
      try {
        if (criticalPriority.peekLast() != null) {
          return criticalPriority.getLast();
        } else if (highPriority.peekLast() != null) {
          return highPriority.getLast();
        } else {
          return lowPriority.getLast();
        }
      } finally {
        this.notifyAll();
      }
    }

    /** {@inheritDoc} */
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

    /** {@inheritDoc} */
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

    /** {@inheritDoc} */
    @Override
    public synchronized boolean removeFirstOccurrence(Object o) {
      try {
        return criticalPriority.removeFirstOccurrence(o) ||
            highPriority.removeFirstOccurrence(o) ||
            lowPriority.removeFirstOccurrence(o);
      } finally {
        this.notifyAll();
      }
    }

    /** {@inheritDoc} */
    @Override
    public synchronized boolean removeLastOccurrence(Object o) {
      try {
        return lowPriority.removeLastOccurrence(o) ||
            highPriority.removeLastOccurrence(o) ||
            criticalPriority.removeLastOccurrence(o);
      } finally {
        this.notifyAll();
      }
    }

    /** {@inheritDoc} */
    @Override
    public boolean add(RaftTask raftTask) {
      this.addLast(raftTask);
      return true;
    }


    /**
     * Add this task to the queue, ignoring the capacity of the queue.
     *
     * @param raftTask The task we're adding.
     *
     * @see #add(RaftTask)
     */
    @SuppressWarnings("Duplicates")
    public synchronized void forceAdd(RaftTask raftTask) {
      try {
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
      } finally {
        this.notifyAll();
      }
    }

    /** {@inheritDoc} */
    @Override
    public boolean offer(RaftTask raftTask) {
      return this.offerLast(raftTask);
    }

    /** {@inheritDoc} */
    @Override
    public RaftTask remove() {
      return this.removeFirst();
    }

    /** {@inheritDoc} */
    @Override
    public RaftTask poll() {
      return this.pollFirst();
    }

    /** {@inheritDoc} */
    @Override
    public RaftTask element() {
      return this.getFirst();
    }

    /**
     * {@inheritDoc}
     *
     * <p>
     *   <b>IMPORTANT: </b> This is from the {@link Stack} interface, not the
     *   {@link Queue} interface, and will behave appropriately.
     * </p>
     */
    @Override
    public RaftTask peek() {
      return this.peekFirst();
    }

    /**
     * {@inheritDoc}
     *
     * <p>
     *   <b>IMPORTANT: </b> This is from the {@link Stack} interface, not the
     *   {@link Queue} interface, and will behave appropriately.
     * </p>
     */
    @Override
    public void push(RaftTask raftTask) {
      this.addFirst(raftTask);
    }

    /**
     * {@inheritDoc}
     *
     * <p>
     *   <b>IMPORTANT: </b> This is from the {@link Stack} interface, not the
     *   {@link Queue} interface, and will behave appropriately.
     * </p>
     */
    @Override
    public RaftTask pop() {
      return this.removeFirst();
    }

    /** {@inheritDoc} */
    @Override
    public boolean remove(Object o) {
      return this.removeFirstOccurrence(o);
    }

    /** {@inheritDoc} */
    @Override
    public boolean containsAll(@Nonnull Collection<?> c) {
      throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public boolean addAll(@Nonnull Collection<? extends RaftTask> c) {
      for (RaftTask t : c) {
        this.add(t);
      }
      return true;
    }

    /** {@inheritDoc} */
    @Override
    public boolean removeAll(@Nonnull Collection<?> c) {
      for (Object t : c) {
        this.remove(t);
      }
      return true;
    }

    /** {@inheritDoc} */
    @Override
    public boolean retainAll(@Nonnull Collection<?> c) {
      throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public synchronized void clear() {
      try {
        this.criticalPriority.clear();
        this.highPriority.clear();
        this.lowPriority.clear();
      } finally {
        this.notifyAll();
      }
    }

    /** {@inheritDoc} */
    @Override
    public boolean contains(Object o) {
      return this.criticalPriority.contains(o) ||
          this.highPriority.contains(o) ||
          this.lowPriority.contains(o);
    }

    /** {@inheritDoc} */
    @Override
    public int size() {
      return this.criticalPriority.size() +
          this.highPriority.size() +
          this.lowPriority.size();
    }

    /** {@inheritDoc} */
    @Override
    public synchronized boolean isEmpty() {
      return this.criticalPriority.isEmpty() &&
          this.highPriority.isEmpty() &&
          this.lowPriority.isEmpty();
    }

    /** {@inheritDoc} */
    @Nonnull
    @Override
    public synchronized Iterator<RaftTask> iterator() {
      Deque<RaftTask> all = new ArrayDeque<>(this.criticalPriority);
      all.addAll(this.highPriority);
      all.addAll(this.lowPriority);
      return all.iterator();
    }

    /** {@inheritDoc} */
    @Nonnull
    @Override
    public synchronized Object[] toArray() {
      RaftTask[] arr = new RaftTask[this.size()];
      int i = 0;
      for (RaftTask task : criticalPriority) {
        arr[i++] = task;
      }
      for (RaftTask task : highPriority) {
        arr[i++] = task;
      }
      for (RaftTask task : lowPriority) {
        arr[i++] = task;
      }
      return arr;
    }

    /** {@inheritDoc} */
    @Nonnull
    @Override
    public <T> T[] toArray(@Nonnull T[] a) {
      //noinspection unchecked
      return (T[]) toArray();
    }

    /** {@inheritDoc} */
    @Nonnull
    @Override
    public Iterator<RaftTask> descendingIterator() {
      Deque<RaftTask> all = new ArrayDeque<>(this.criticalPriority);
      all.addAll(this.highPriority);
      all.addAll(this.lowPriority);
      return all.descendingIterator();
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
  boolean alive = true;

  /**
   * The queue of tasks for Raft to pick up on.
   */
  private final RaftDeque raftTasks = new RaftDeque();

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
   * @param impl The implementing algorithm. See {@link #impl}.
   * @param boundaryPool The boundary pool. See {@link #boundaryPool}.
   */
  public SingleThreadedRaftAlgorithm(RaftAlgorithm impl, ExecutorService boundaryPool) {
    this.impl = impl;
    this.threadsCanBlock = impl.getTransport().threadsCanBlock();
    this.raftThread = new Thread( () -> {
      if (impl instanceof EloquentRaftAlgorithm) {
        ((EloquentRaftAlgorithm) impl).setDrivingThread(r ->
          raftTasks.add(new RaftTask("EloquentRaftAlgorithm Callback", TaskPriority.CRITICAL, r, e -> log.warn("Error in queued task", e)))
        );
      }
      try {
        while (alive) {
          RaftTask task;
          try {
            synchronized (raftTasks) {
              taskRunning = Optional.empty();
              while (raftTasks.isEmpty()) {
                raftTasks.wait(MAX_DELAY);
                if (!alive) {
                  return;
                }
              }
              task = raftTasks.poll();
              assert task.priority == TaskPriority.CRITICAL || raftTasks.criticalPriority.isEmpty();
              taskRunning = Optional.of(task.debugString);
              raftTasks.notifyAll();
            }
            long dequeueTime = System.nanoTime();
            Prometheus.histogramObserve(HISTOGRAM_QUEUE_TIME, ((double) (dequeueTime - task.queuedTimestamp)) / 1000000000.);
            try {
              task.fn.run();  // RUN THE TASK
            } catch (Throwable t) {
              task.onError.accept(t);
            } finally {
              long finishTime = System.nanoTime();
              Prometheus.histogramObserve(HISTOGRAM_RUN_TIME, ((double) (finishTime - dequeueTime)) / 1000000000.);
              if (finishTime - dequeueTime > 100000000) {
                log.warn("Task took >100ms to run: {}; time={}", task.debugString, TimerUtils.formatTimeDifference((finishTime - dequeueTime) / 1000000));
              }
            }
          } catch (Throwable t) {
            log.warn("Caught exception ", t);
          }
        }
      } finally {
        List<RaftTask> tasks;
        synchronized (raftTasks) {
          // Clean up any leftovers
          tasks = new ArrayList<>(this.raftTasks);
        }
        tasks.forEach(t -> t.onError.accept(new RuntimeException("SingleThreadedRaftAlgorithm main thread killed from killMainThread(), so this will never complete")));
        raftTasks.clear();
      }
    });
    this.raftThread.setPriority(Math.max(Thread.NORM_PRIORITY, Thread.MAX_PRIORITY - 2));
    this.raftThread.setDaemon(false);
    this.raftThread.setName("raft-control-" + impl.serverName());
    this.raftThread.setUncaughtExceptionHandler((t, e) -> log.warn("Caught exception on {}:", t.getName(), e));
    this.raftThread.start();
    this.boundaryPool = boundaryPool;
  }


  /**
   * Create a single threaded Raft algorithm, using the implementing algorithm's pool
   *
   * @param impl The implementing algorithm.
   */
  public SingleThreadedRaftAlgorithm(RaftAlgorithm impl) {
    this(impl, impl.mutableState().log.pool);
  }


  /**
   * Return the number of tasks we have queued to be executed by Raft.
   */
  public int queuedTaskCount() {
    return this.raftTasks.size();
  }


  /**
   * Run a given function, returning a completable future for when this function is complete.
   * Note that this completable future completes <b>on the raft thread</b>, and therefore
   * should not be exposed to the outside world.
   *
   * @param debugName A debug name for this task.
   * @param priority The priority for this task
   * @param offerPolicy The mode in which to offer this task to the queue.
   * @param fn The function we are running. Typically, a {@link RaftAlgorithm} method.
   * @param <E> The return type of our function.
   *
   * @return A future for tracking when we actually have finished scheduling and running
   *         this function.
   */
  private <E> CompletableFuture<E> execute(
      String debugName,
      TaskPriority priority,
      OfferPolicy offerPolicy,
      Function<RaftAlgorithm, E> fn) {
    log.trace("{} - [{}] Executing {}", this.serverName(), getTransport().now(), debugName);
    if (Thread.currentThread() == raftThread) {  // don't queue if we're on the raft thread
      return CompletableFuture.completedFuture(fn.apply(this.impl));
    }
    CompletableFuture<E> future = new CompletableFuture<>();
    if (!alive) {
      future.completeExceptionally(new RejectedExecutionException("Node is dead -- failing the future"));
      return future;
    }
    Runnable task = () -> {
      try {
        E result = fn.apply(this.impl);
        future.complete(result);
      } catch (Throwable t) {
        future.completeExceptionally(t);
      }
    };
    Consumer<Throwable> onError = future::completeExceptionally;
    switch (offerPolicy) {
      default:
      case FORCE:
        raftTasks.forceAdd(new RaftTask(debugName, priority, task, onError));
        break;
      case BLOCK:
        raftTasks.add(new RaftTask(debugName, priority, task, onError));
        break;
      case OPTIONAL:
        if (!raftTasks.offer(new RaftTask(debugName, priority, task, onError))) {
          onError.accept(new RejectedExecutionException("Rejected " + debugName + " since queue is full"));
        }
        break;
    }
    return future;
  }


  /**
   * Run a given function, returning a completable future for when this function is complete.
   * Unlike {@link #execute(String, TaskPriority, OfferPolicy, Function)}, this returns a <b>safe future to be show to the
   * outside world</b>.
   *
   * @param debugName A debug name for this task.
   * @param priority The priority for this task
   * @param offerPolicy The mode in which to offer this task to the queue.
   * @param fn The function we are running. Typically, a {@link RaftAlgorithm} method.
   * @param <E> The return type of our function.
   *
   * @return A future for tracking when we actually have finished scheduling and running
   *         this function.
   */
  private <E> CompletableFuture<E> executeFuture(
      String debugName,
      TaskPriority priority,
      OfferPolicy offerPolicy,
      Function<RaftAlgorithm, CompletableFuture<E>> fn) {
    // 1. Check if we should execute directly
    log.trace("{} - [{}] Executing as Composite Future {}", this.serverName(), getTransport().now(), debugName);
    if (!alive) {
      throw new RejectedExecutionException("Node is dead -- failing the future for " + debugName);
    }
    if (Thread.currentThread().getId() == raftThread.getId()) {  // don't queue if we're on the raft thread
      return fn.apply(this.impl);
    }
    CompletableFuture<E> future = new CompletableFuture<>();

    // 2. Define the timeout for the future
    CompletableFuture<CompletableFuture<E>> futureOfFuture = execute(debugName, priority, offerPolicy, fn);

    futureOfFuture.whenComplete((CompletableFuture<E> result, Throwable t) -> {
          if (t != null) {
            // 3.A. Case: we encountered an exception -- immediately fail
            boundaryPoolThreadsWaiting.incrementAndGet();  // see canonical deadlock below -- we need to handle it here as well
            boundaryPool.execute(() -> {
              try {
                future.completeExceptionally(t);
              } finally {
                boundaryPoolThreadsWaiting.decrementAndGet();
              }
            });
          } else {
            // 3.B. Case: wait for future to complete
            result.whenComplete((E r, Throwable t2) -> {
              // JUST FOR TESTS: this helps resolve a deadlock detailed below
              boundaryPoolThreadsWaiting.incrementAndGet();
              // note[keenon]: There's a race condition here that's tricky and hard to remove - and only shows up in the tests
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
                } catch (Throwable rt) {
                  log.warn("Exception in completion of executeFuture() of {}", debugName, rt);
                } finally {
                  boundaryPoolThreadsWaiting.decrementAndGet();
                }
              };
              // 4.3. Schedule the completion on the pool
              try {
                boundaryPool.execute(completeFuture);
              } catch (Throwable boundaryPoolError) {
                log.error("We got an exception submitting a task to the boundary pool from SingleThreadedRaftAlgorithm. Falling back to a daemon thread.", boundaryPoolError);
                Thread thread = new Thread(completeFuture);
                thread.setDaemon(true);
                thread.setName("boundary-pool-fallback");
                thread.setPriority(Thread.NORM_PRIORITY);
                thread.start();
              }
            });
          }
        }
    );

    // 6. Return
    return future;
  }


  /**
   * Run a given function, dumping the result into the void.
   * This is useful for void return type methods on a {@link RaftAlgorithm}.
   *
   * @param debugName A debug name for this task.
   * @param priority The priority for this task
   * @param offerPolicy The mode in which to offer this task to the queue.
   * @param fn The function we are running. Typically, a {@link RaftAlgorithm} method.
   * @param onError A function to call if something went wrong queuing the task
   * @param isHeartbeat A marker for whether this is the heartbeat.
   *                    Heartbeats are handled a bit differently from other tasks,
   *                    in that there should only be one queued at any time.
   */
  private void execute(String debugName,
                       TaskPriority priority,
                       OfferPolicy offerPolicy,
                       Consumer<RaftAlgorithm> fn,
                       Consumer<Throwable> onError,
                       boolean isHeartbeat) {
    log.trace("{} - [{}] Executing {}", this.serverName(), getTransport().now(), debugName);

    // 1. Short circuit if we're on the control thread
    if (Thread.currentThread().getId() == raftThread.getId()) {  // don't queue if we're on the raft thread
      fn.accept(this.impl);
      return;
    }

    // 2. Run the task through queue
    AtomicBoolean done = new AtomicBoolean(false);
    // 2.1. Immediately fail if we're dead
    if (!alive) {
      log.debug("Node is dead -- ignoring any messages to it");
      onError.accept(new RejectedExecutionException("Node is dead when executing task " + debugName));
      return;
    }
    // 2.2. Define the Raft task
    RaftTask task = new RaftTask(debugName, priority,
        () -> {
          try {
            fn.accept(this.impl);
          } catch (Throwable t) {
            log.warn("Exception in execute() of {}", debugName, t);
          } finally {
            synchronized (done) {
              done.set(true);
              done.notifyAll();
            }
          }
        },
        (e) -> {
          synchronized (done) {
            done.set(true);
            done.notifyAll();
          }
          onError.accept(e);
        }, isHeartbeat);
    // 2.3. Run the task
    switch (offerPolicy) {
      default:
      case FORCE:
        raftTasks.forceAdd(task);
        break;
      case BLOCK:
        raftTasks.add(task);
        break;
      case OPTIONAL:
        if (!raftTasks.offer(task)) {
          onError.accept(new RejectedExecutionException("Rejected " + debugName + " since queue is full"));
        }
        break;
    }

    // 3. Wait for the task to complete, if applicable
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


  /**
   * @see #execute(String, TaskPriority, OfferPolicy, Consumer, Consumer, boolean)
   */
  private void execute(String debugName,
                       TaskPriority priority,
                       OfferPolicy offerPolicy,
                       Consumer<RaftAlgorithm> fn) {
    this.execute(debugName, priority, offerPolicy, fn, t -> log.warn("Got exception running Raft method {}", debugName, t), false);
  }



  /** {@inheritDoc} */
  @Override
  public RaftState state() {
    try {
      return execute("state", TaskPriority.LOW, OfferPolicy.OPTIONAL, RaftAlgorithm::state).get(30, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      log.warn("Could not get RaftState -- returning unlocked version as a failsafe");
      return impl.mutableState();
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
    execute("broadcastAppendEntries", TaskPriority.HIGH, OfferPolicy.FORCE, RaftAlgorithm::broadcastAppendEntries);
  }


  /** {@inheritDoc} */
  @Override
  public void sendAppendEntries(String target, long nextIndex) {
    execute("sendAppendEntries", TaskPriority.HIGH, OfferPolicy.FORCE, (Consumer<RaftAlgorithm>) x -> x.sendAppendEntries(target, nextIndex));
  }


  /** {@inheritDoc} */
  @Override
  public void receiveAppendEntriesRPC(EloquentRaftProto.AppendEntriesRequest heartbeat, Consumer<EloquentRaftProto.RaftMessage> replyLeader) {
    execute("receiveAppendEntriesRPC", TaskPriority.HIGH, OfferPolicy.FORCE, (Consumer<RaftAlgorithm>) x -> x.receiveAppendEntriesRPC(heartbeat, replyLeader));

  }


  /** {@inheritDoc} */
  @Override
  public void receiveAppendEntriesReply(EloquentRaftProto.AppendEntriesReply reply) {
    execute("receiveAppendEntriesReply", TaskPriority.HIGH, OfferPolicy.FORCE, (Consumer<RaftAlgorithm>) x -> x.receiveAppendEntriesReply(reply));
  }


  /** {@inheritDoc} */
  @Override
  public void receiveInstallSnapshotRPC(EloquentRaftProto.InstallSnapshotRequest snapshot, Consumer<EloquentRaftProto.RaftMessage> replyLeader) {
    execute("receiveInstallSnapshotRPC", TaskPriority.HIGH, OfferPolicy.FORCE, (Consumer<RaftAlgorithm>) x -> x.receiveInstallSnapshotRPC(snapshot, replyLeader));
  }


  /** {@inheritDoc} */
  @Override
  public void receiveInstallSnapshotReply(EloquentRaftProto.InstallSnapshotReply reply) {
    execute("receiveInstallSnapshotReply", TaskPriority.HIGH, OfferPolicy.FORCE, (Consumer<RaftAlgorithm>) x -> x.receiveInstallSnapshotReply(reply));
  }


  /** {@inheritDoc} */
  @Override
  public void triggerElection() {
    execute("triggerElection", TaskPriority.LOW, OfferPolicy.OPTIONAL, RaftAlgorithm::triggerElection);
  }


  /** {@inheritDoc} */
  @Override
  public void receiveRequestVoteRPC(EloquentRaftProto.RequestVoteRequest voteRequest, Consumer<EloquentRaftProto.RaftMessage> replyLeader) {
    execute("receiveRequestVoteRPC", TaskPriority.HIGH, OfferPolicy.FORCE, (Consumer<RaftAlgorithm>) x -> x.receiveRequestVoteRPC(voteRequest, replyLeader));
  }


  /** {@inheritDoc} */
  @Override
  public void receiveRequestVotesReply(EloquentRaftProto.RequestVoteReply reply) {
    execute("receiveRequestVotesReply", TaskPriority.HIGH, OfferPolicy.FORCE, (Consumer<RaftAlgorithm>) x -> x.receiveRequestVotesReply(reply));
  }


  /** {@inheritDoc} */
  @Override
  public CompletableFuture<EloquentRaftProto.RaftMessage> receiveAddServerRPC(EloquentRaftProto.AddServerRequest addServerRequest) {
    // TODO(gabor) there are only some cases where we want to force this. E.g., we should maybe drop user requests for reconfigures
    return executeFuture("receiveAddServerRPC", TaskPriority.LOW, OfferPolicy.FORCE, x -> x.receiveAddServerRPC(addServerRequest));
  }


  /** {@inheritDoc} */
  @Override
  public CompletableFuture<EloquentRaftProto.RaftMessage> receiveRemoveServerRPC(EloquentRaftProto.RemoveServerRequest removeServerRequest) {
    // TODO(gabor) there are only some cases where we want to force this. E.g., we should maybe drop user requests for reconfigures
    return executeFuture("receiveRemoveServerRPC", TaskPriority.HIGH, OfferPolicy.FORCE, x -> x.receiveRemoveServerRPC(removeServerRequest));
  }


  /** {@inheritDoc} */
  @Override
  public CompletableFuture<EloquentRaftProto.RaftMessage> receiveApplyTransitionRPC(EloquentRaftProto.ApplyTransitionRequest transition, boolean forceOntoQueue) {
    return executeFuture(
        "receiveApplyTransitionRPC",
        TaskPriority.LOW,
        forceOntoQueue ? OfferPolicy.FORCE : OfferPolicy.BLOCK,
        x -> x.receiveApplyTransitionRPC(transition, forceOntoQueue)
    );
  }


  /** {@inheritDoc} */
  @Override
  public boolean bootstrap(boolean force) {
    try {
      return execute("bootstrap", TaskPriority.HIGH, OfferPolicy.BLOCK, (Function<RaftAlgorithm, Boolean>) x -> x.bootstrap(force)).get(30, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      log.warn("Could not bootstrap -- returning unlocked version as a failsafe");
      return impl.bootstrap(force);
    }
  }


  /** {@inheritDoc} */
  @Override
  public void stop(boolean kill) {
    // Call the implementing algorithm's stop
    execute("stop", TaskPriority.LOW, OfferPolicy.BLOCK, (Consumer<RaftAlgorithm>) x -> x.stop(kill));
    flush(() -> {});

    // Stop our thread
    synchronized (raftTasks) {
      this.alive = false;
      // Wake up the main thread so it can die
      this.raftTasks.notifyAll();
    }
    // Give the main thread time to actually stop
    Thread.yield();
    // Kill any outstanding tasks
    synchronized (raftTasks) {
      for (RaftTask task : raftTasks) {
        task.onError.accept(new RejectedExecutionException("Raft is shutting down"));
      }
    }
    // Kill our pool
    this.boundaryPool.shutdown();
  }


  /** {@inheritDoc} */
  @Override
  public boolean isRunning() {
    try {
      return execute("isRunning", TaskPriority.LOW, OfferPolicy.BLOCK, RaftAlgorithm::isRunning).get(30, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      log.warn("Could not check if Raft is running -- returning unlocked version as a failsafe");
      return impl.isRunning();
    }
  }


  /** {@inheritDoc} */
  @Override
  public void heartbeat() {
    // note[gabor]: it's initially strange that the heartbeat is optional, but this is a convenient
    // way to ensure that we don't queue up tons of heartbeats. If there's already a heartbeat
    // in the queue, we simply drop this one
    execute("heartbeat", TaskPriority.CRITICAL, OfferPolicy.OPTIONAL, RaftAlgorithm::heartbeat,
        t -> log.debug("Skipping heartbeat since queue is full"),
        true);
  }


  /** {@inheritDoc} */
  @Override
  public void receiveBadRequest(EloquentRaftProto.RaftMessage message) {
    execute("receiveBadRequest", TaskPriority.LOW, OfferPolicy.OPTIONAL, (Consumer<RaftAlgorithm>) x -> x.receiveBadRequest(message));
  }


  /** {@inheritDoc} */
  @Override
  public Optional<RaftLifecycle> lifecycle() {
    return impl.lifecycle();  // note[gabor] don't run as a future -- this should be final
  }


  /** {@inheritDoc} */
  @Override
  public RaftTransport getTransport() {
    return impl.getTransport();
  }


  /** {@inheritDoc} */
  @Override
  public void awaitLeaderKnown() {
    this.raftTasks.ensureCapacity(TaskPriority.LOW);
    this.impl.awaitLeaderKnown();
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
            this.raftTasks.wait(MAX_DELAY);
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
      raftTasks.add(new RaftTask("errors", TaskPriority.LOW, task, onError));
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
    if (this.alive) {
      log.warn("Stopping raft from finalize() -- this is a memory leak that you should have cleaned up!");
      stop(true);
    }
  }
}
