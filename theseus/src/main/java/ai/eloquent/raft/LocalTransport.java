package ai.eloquent.raft;

import ai.eloquent.util.*;
import com.google.protobuf.InvalidProtocolBufferException;
import ai.eloquent.util.IdentityHashSet;
import ai.eloquent.util.RuntimeInterruptedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;


/**
 * A mocked transport, that delivers messages to others listening on the same transport
 * instance.
 *
 * @author <a href="mailto:gabor@eloquent.ai">Gabor Angeli</a>
 */
public class LocalTransport implements RaftTransport {
  /**
   * An SLF4J Logger for this class.
   */
  private static final Logger log = LoggerFactory.getLogger(LocalTransport.class);

  private volatile static StackTraceElement[] singleton = null;

  /** An event that can be queued on the transport. */
  private interface TransportEvent {}


  /**
   * A simple object to represent a message over the wire.
   */
  private class TransportMessage implements TransportEvent {
    /** A unique ID for the message. */
    private final long id;
    /** The id of the inbound message, for resolving RPCs. */
    private final long correlationId;
    /** The node name that sent this message. */
    private final String sender;
    /** The node name that received this message. */
    private final String target;
    /** The contents of the message we're sending, already serialized. */
    private final byte[] contents;

    /** For debugging: the time at which the message was sent */
    private final long sentAt;

    /** A straightforward constructor, with no correlation id. */
    private TransportMessage(long id, String sender, String target, byte[] contents) {
      this(id, -1, sender, target, contents);
    }

    /** A straightforward constructor. */
    private TransportMessage(long id, long correlationId, String sender, String target, byte[] contents) {
      this.id = id;
      this.correlationId = correlationId;
      this.sender = sender;
      this.target = target;
      this.contents = contents;

      this.sentAt = now();
    }

    @Override
    public String toString() {
      try {
        EloquentRaftProto.RaftMessage raftMessage = EloquentRaftProto.RaftMessage.parseFrom(contents);
        String message = raftMessage.getContentsCase().toString()+" ";
        switch (raftMessage.getContentsCase()) {
          case APPENDENTRIES:
            message += raftMessage.getAppendEntries().toString();
            break;
          case APPENDENTRIESREPLY:
            message += raftMessage.getAppendEntriesReply().toString();
            break;
          case APPLYTRANSITION:
            message += raftMessage.getApplyTransition().toString();
            break;
          case APPLYTRANSITIONREPLY:
            message += raftMessage.getApplyTransitionReply().toString();
            break;
        }
        return "@"+this.sentAt+" :"+sender+"->"+target+": "+message.replaceAll("\n", ", ");
      }
      catch (InvalidProtocolBufferException e) {
        return "@"+this.sentAt+" :"+sender+"->"+target+": Unrecognized message";
      }
    }
  }


  /**
   * The specification for code awaiting a callback.
   */
  private static class WaitingCallback {
    /** The message we sent that's waiting for a callback */
    private final long messageId;
    /** The success function, if the callback returns. */
    private final Consumer<EloquentRaftProto.RaftMessage> onSuccess;
    /** The timeout function, if the callback does not return. */
    private final Runnable onTimeout;
    /** The time at which this message times out. */
    private final long timeoutTime;

    /** The straightforward constructor. */
    private WaitingCallback(long messageId, Consumer<EloquentRaftProto.RaftMessage> onSuccess, Runnable onTimeout, long timeoutTime) {
      this.messageId = messageId;
      this.onSuccess = onSuccess;
      this.onTimeout = onTimeout;
      this.timeoutTime = timeoutTime;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() { return "callback(" + messageId + ")@" + timeoutTime; }
  }


  /**
   * A network partition. Any given timestamp can have multiple of these.
   */
  private static class Partition {
    /** The time at which this partition began. */
    private final long startTime;
    /** The time at which this partition should be closed. */
    private final long endTime;
    /** The members of the partition, partitioned off from the rest of the cluster but not from each other. */
    private final Set<String> members;
    /** A straightforward constructor */
    private Partition(long startTime, long endTime, String[] members) {
      this.startTime = startTime;
      this.endTime = endTime;
      this.members = new HashSet<>(Arrays.asList(members));
    }
    /** {@inheritDoc} */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Partition partition = (Partition) o;
      return startTime == partition.startTime &&
          endTime == partition.endTime &&
          Objects.equals(members, partition.members);
    }
    /** {@inheritDoc} */
    @Override
    public int hashCode() {
      return Objects.hash(startTime, endTime, members);
    }
  }


  /** The minimum amount of network delay. */
  public final long delayMin;
  /** The maximum amount of network delay. */
  public final long delayMax;
  /** The probability of dropping a packet on the "network." */
  public final double dropProb;
  /** The probability of getting an IO exception when calling a transport function. */
  public final double ioExceptionProb;
  /** If true, run in real-time (or close to). Otherwise, mock the transport time. */
  public final boolean trueTime;

  /** The number of RPC messages this transport has sent. */
  public long numRPCsSent = 0;

  /** A list of RPCs that are waiting for callbacks. */
  private final Set<WaitingCallback> waitingCallbacks = new IdentityHashSet<>();

  /** A counter for creating unique message ids. */
  private final AtomicLong nextMessageId = new AtomicLong(0);

  /** The implementing algorithm. */
  private List<RaftAlgorithm> nodes = new ArrayList<>();
  private final ReadWriteLock nodesLock = new ReentrantReadWriteLock();

  /** If true, this transport is still running. If false, we will exit from the thread loop. */
  private boolean isAlive = true;

  /** A random number generator to use on the transport. */
  private final Random rand;

  /** If true, this transport is still running. If false, we will exit from the thread loop. */
  private final Thread timekeeper;

  /** This is a Future we can block on when we're waiting for the timekeeper thread to shut down. */
  private final CompletableFuture<Void> timekeeperFinished;

  /**
   * The list of exceptions encountered by the transport. This should usually be empty at the end of a test.
   * You can verify this with {@link #assertNoErrors()}.
   */
  public final List<Throwable> exceptions = new ArrayList<>();

  /** The set of partition definitions we must respect. */
  private final Set<Partition> partitions = new HashSet<>();

  /**
   * This is the timer that we'll use to hook our events into the global mock clock.
   */
  private final SafeTimerMock transportMockTimer = new SafeTimerMock();


  /** Create a mock transport. */
  public LocalTransport(long delayMin, long delayMax, double dropProb, double ioExceptionProb,
                        boolean trueTime, long randomSeed) {
    // 1. Run error checks
    if (delayMin < 1 || delayMax < 0 || delayMax < delayMin || dropProb < 0.0 || dropProb > 1.0 ||
        ioExceptionProb < 0 || ioExceptionProb > 1.0) {
      throw new IllegalArgumentException("Invalid params for mock Raft transport");
    }
    if (singleton != null) {
      log.warn("Created two local transports at once. Old one created from:");
      for (StackTraceElement s : singleton) {
        if (s.toString().startsWith("ai.eloquent")) {
          log.warn("  " + s.toString());
        }
      }
      log.warn("New one is:");
      for (StackTraceElement s : Thread.currentThread().getStackTrace()) {
        if (s.toString().startsWith("ai.eloquent")) {
          log.warn("  " + s.toString());
        }
      }
      throw new RuntimeException("Two LocalTransports are running at the same time");
    }
    singleton = Thread.currentThread().getStackTrace();
    // 2. Set the variables
    this.delayMin = delayMin;
    this.delayMax = delayMax;
    this.dropProb = dropProb;
    this.ioExceptionProb = ioExceptionProb;
    this.trueTime = trueTime;
    this.rand = new Random(randomSeed);

    // 3. Start time
    this.timekeeperFinished = new CompletableFuture<>();
    this.timekeeper = new Thread(() -> {
      while (true) {
        // Check if we've stopped the thread
        if (Thread.interrupted()) {
          throw new RuntimeInterruptedException();
        }
        if (!isAlive) {
          transportMockTimer.cancel();  // cancel tasks before we stop
          break;
        }

        // can't synchronize on queue when we run this
        boolean shouldAdvanceTimer = false;
        synchronized (this) {
          // Actually move time forward by 1ms
          if (this.transportMockTimer.numTasksScheduled() > 0) {  // don't move if nothing's happening
            shouldAdvanceTimer = true;
          }
        }

        // Advance time, but very very carefully
        if (shouldAdvanceTimer) {
          for (RaftAlgorithm algorithm : boundAlgorithms()) {  // note[gabor] assume that all scheduled tasks will complete in 1ms or less; and wait for them to complete before advancing
            if (algorithm instanceof SingleThreadedRaftAlgorithm) {
              ((SingleThreadedRaftAlgorithm) algorithm).flush(() -> {});
            }
          }
          Thread.yield();
          // Never advance time while we're waiting on boundary pool threads to start
          while (SingleThreadedRaftAlgorithm.boundaryPoolThreadsWaiting.get() > 0) {
            for (RaftAlgorithm algorithm : boundAlgorithms()) {
              if (algorithm instanceof SingleThreadedRaftAlgorithm) {
                ((SingleThreadedRaftAlgorithm) algorithm).flush(() -> {});
              }
            }
            Thread.yield();
          }
          // OK, now we can advance time
          SafeTimerMock.advanceTime(1);
        }

        if (this.trueTime) {
          Uninterruptably.sleep(1);  // run at 1ms per ms, give or take
        } else {
          Thread.yield();
        }
      }
      timekeeperFinished.complete(null);
    });
    timekeeper.setName("mock-raft-transport-time");
    timekeeper.setDaemon(true);
    timekeeper.setPriority(Thread.MIN_PRIORITY);  // run this guy at low priority
    timekeeper.setUncaughtExceptionHandler((t, e) -> {
      log.warn("Caught exception on timekeeper: ", e);
      isAlive = false;
    });
  }


  /** A constructor that has presets for whether the transport is stable (i.e., lossless, etc.) or not */
  public LocalTransport(boolean stable) {
    this(5, 100, stable ? 0.0 : 0.3, stable ? 0.0 : 0.3, false, 42L);
  }


  /** A constructor that has presets for whether the transport is stable (i.e., lossless, etc.) or not, and whether we should run in true time */
  public LocalTransport(boolean stable, boolean trueTime) {
    this(5, 100, stable ? 0.0 : 0.3, stable ? 0.0 : 0.3, trueTime, 42L);
  }

  /** Creates a stable transport. */
  @SuppressWarnings("unused")
  public LocalTransport() {
    this(true);
  }


  /**
   * A really stupid little function to ensure that no synchronized function is being run on any of the nodes.
   * This is useful for ensuring that time can proceed.
   */
  private void silenceOn(Iterator<?> toSynchronizeOn, Runnable toRun) {
    if (toSynchronizeOn.hasNext()) {
      final Object o = toSynchronizeOn.next();
      if (o != null) {
        //noinspection SynchronizationOnLocalVariableOrMethodParameter
        synchronized(o) {
          silenceOn(toSynchronizeOn, toRun);
        }
      }
    } else {
      toRun.run();
    }
  }


  /**
   * Run a block of code while synrhronizing on the current timestep.
   * This prevents the clock from slipping between lines of code
   *
   * @param r The runnable to run.
   */
  public void synchronizedRun(Runnable r) {
    r.run();
  }


  /**
   * Wait for the transport to fall silent.
   */
  public void waitForSilence() {
    log.info("[{}] Waiting for transport to flush. queue_size={}", now(), transportMockTimer.numTasksScheduled());
    try {
      transportMockTimer.waitForSilence();
      log.info("[{}] Transport has flushed.", now());
    } catch (Throwable t) {
      // Kill the main thread, just so we don't leak threads if this test failed in a suite of tests
      this.isAlive = false;
      // Wait for the main thread to die
      try {
        this.timekeeperFinished.get(10, TimeUnit.SECONDS);
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        log.warn("Failed to shut down the timekeeper thread for 10 seconds. This is a bug.");
      }

      // Once the main timer thread is dead, then notify the caller by rethrowing the exception
      log.warn("Transport failed to wait for silence: ", t);
      throw t;
    }
  }


  /**
   * Start the transport, if it hasn't already started
   */
  @Override
  public void start() {
    synchronized (timekeeper) {
      if (!timekeeper.isAlive() && this.isAlive) {
        timekeeper.start();
      }
    }
  }


  /**
   * Stop this transport.
   */
  @Override
  public void stop() {
    try {
      // Kill scheduled tasks
      transportMockTimer.cancel();
      // Short-circuit to prevent double closing
      if (!isAlive || !timekeeper.isAlive()) {
        return;
      }
      // Wait for liveness
      waitForSilence();
      // Kill liveness
      this.isAlive = false;
      // Wait for the main thread to die
      try {
        this.timekeeperFinished.get(10, TimeUnit.SECONDS);
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        log.error("Failed to shut down the timekeeper thread for 10 seconds. This is a bug.");
      }
    } finally {
      // Clear the singleton
      singleton = null;
    }
  }


  /** {@inheritDoc} */
  @Override
  public boolean threadsCanBlock() {
    return true;
  }


  /** {@inheritDoc} */
  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    stop();
  }


  /** {@inheritDoc} */
  @Override
  public void bind(RaftAlgorithm listener) {
    // note: don't mock exceptions here. It's just a pain in the butt...
    Lock writeLock = nodesLock.writeLock();
    try {
      writeLock.lock();
      if (!this.nodes.contains(listener)) {
        this.nodes.add(listener);
      }
    }
    finally {
      writeLock.unlock();
    }
  }


  /** {@inheritDoc} */
  @Override
  public Collection<RaftAlgorithm> boundAlgorithms() {
    Lock readLock = nodesLock.readLock();
    try {
      readLock.lock();
      return new ArrayList<>(this.nodes);
    }
    finally {
      readLock.unlock();
    }
  }


  /** {@inheritDoc} */
  @Override
  public Span expectedNetworkDelay() {
    return new Span(delayMin, delayMax);
  }

  private TransportMessage sendMessage(String sender, String destination, EloquentRaftProto.RaftMessage message, Optional<Long> correlationId) {
    assert ConcurrencyUtils.ensureNoLocksHeld();
    TransportMessage m;
    //noinspection OptionalIsPresent
    if (correlationId.isPresent()) {
      m = new TransportMessage(nextMessageId.incrementAndGet(), correlationId.get(), sender, destination, message.toByteArray());
    } else {
      m = new TransportMessage(nextMessageId.incrementAndGet(), sender, destination, message.toByteArray());
    }
    long transportDelay = sampleDelay();

    // We need to hold the transport timer lock while doing this, because otherwise time can slip between computing if
    // we should drop the packet and adding it to the schedule.
    transportMockTimer.withTimerLock(() -> {
      if (shouldDrop(sender, destination, transportDelay)) {
        log.trace("[{}] Dropped RPC {} -> {}; with delay {}", now(), sender, destination, transportDelay);
      } else {
        log.trace("[{}] Sending {} -> {}; with delay {}", now(), sender, destination, transportDelay);
        numRPCsSent += 1;

        SafeTimerTask messageDelivery = new SafeTimerTask() {
          @Override
          public void runUnsafe() {
            receiveMessage(m, now());
          }
        };
        transportMockTimer.schedule(messageDelivery, transportDelay);
      }
    });

    // Ensure the timekeeper thread is alive
    synchronized (timekeeper) {
      if (!timekeeper.isAlive() && this.isAlive) {
        timekeeper.start();
      }
    }

    return m;
  }

  /** {@inheritDoc} */
  @Override
  public void rpcTransport(String sender, String destination, EloquentRaftProto.RaftMessage message,
                           Consumer<EloquentRaftProto.RaftMessage> onResponseReceived, Runnable onTimeout,
                           long timeout) {
    assert ConcurrencyUtils.ensureNoLocksHeld();
    TransportMessage m = sendMessage(sender, destination, message, Optional.empty());
    WaitingCallback waitingCallback = new WaitingCallback(m.id, onResponseReceived, onTimeout, now() + timeout);
    synchronized (waitingCallbacks) {
      waitingCallbacks.add(waitingCallback);
    }

    transportMockTimer.schedule(new SafeTimerTask() {
      @Override
      public void runUnsafe() {
        boolean doTimeout = false;
        synchronized (waitingCallback) {
          if (waitingCallbacks.contains(waitingCallback)) {
            waitingCallbacks.remove(waitingCallback);
            doTimeout = true;
          }
        }
        if (doTimeout) {
          log.debug("Timing out RPC for message {} from {} -> {}", m.id, sender, destination);
          onTimeout.run();
        }
      }
    }, timeout);
  }


  /** {@inheritDoc} */
  @Override
  public void sendTransport(String sender, String destination, EloquentRaftProto.RaftMessage message) {
    assert ConcurrencyUtils.ensureNoLocksHeld();
    sendMessage(sender, destination, message, Optional.empty());
  }


  /** {@inheritDoc} */
  @Override
  public void broadcastTransport(String sender, EloquentRaftProto.RaftMessage message) {
    assert ConcurrencyUtils.ensureNoLocksHeld();
    assert this.boundAlgorithms().stream().allMatch(x -> x.getClass().getName().contains("RecordingRaft")) || this.boundAlgorithms().stream().filter(x -> x.serverName().equals(sender)).findFirst().map(x -> x.mutableState().leadership != RaftState.LeadershipStatus.OTHER).orElse(false)
        : "A follower / shadow should not be broadcasting on the transport!";
    Lock nodesReadLock = this.nodesLock.readLock();
    try {
      nodesReadLock.lock();
      for (RaftAlgorithm target : this.nodes) {
        String destination = target.serverName();
        if (!Objects.equals(target.serverName(), sender)) {
          sendMessage(sender, destination, message, Optional.empty());
        }
      }
    }
    finally {
      nodesReadLock.unlock();
    }
  }


  /**
   * Checks that there are no errors on this transport in the course of operation.
   *
   * @return this object, to allow for chaining methods.
   */
  public LocalTransport assertNoErrors() {
    // (no exceptions on transport)
    assert exceptions.isEmpty() :
        "Got " + exceptions.size() + " exceptions on transport. First one: <" +
            exceptions.get(0).getClass() + ": " + exceptions.get(0).getMessage() + ">";
    // (return ok)
    return this;
  }


  /**
   * This checks several invariants across all the Raft nodes attached to the LocalTransport. If any invariant fails,
   * this will throw an assert and fail.
   *
   * These invariants are from page 14 of Diego's thesis.
   */
  public void assertInvariantsHold() {
    Lock nodesReadLock = this.nodesLock.readLock();
    try {
      nodesReadLock.lock();

      // Election Safety:
      // At most one leader can be elected in a given term.

      Set<Long> leaderTerms = new HashSet<>();
      for (RaftAlgorithm node : nodes) {
        if (node.state().isLeader()) {
          assert(!leaderTerms.contains(node.mutableState().currentTerm)) : "At most one leader can be elected in a given term. More than one leader for term "+node.state().currentTerm;
          leaderTerms.add(node.mutableState().currentTerm);
        }
      }

      // Leader Append-Only:
      // A leader never overwrites or deletes entries in its log; it only appends new entries.

      // TODO: unsure how to verify this from here

      // Log Matching:
      // If two logs contain an entry with the same index and term, then the logs are identical in all entries up through
      // the given index.

      for (int i = 0; i < nodes.size() - 1; i++) {
        RaftLog log1 = nodes.get(i).mutableState().log;
        for (int j = i+1; j < nodes.size(); j++) {
          RaftLog log2 = nodes.get(j).mutableState().log;

          long maxSharedIndex = Math.min(log1.getLastEntryIndex(), log2.getLastEntryIndex());
          boolean haveAgreedOnIndexAndTerm = false;
          for (long index = maxSharedIndex; index >= 0; index --) {
            Optional<Long> log1Term = log1.getPreviousEntryTerm(index);
            Optional<Long> log2Term = log2.getPreviousEntryTerm(index);
            if (log1Term.isPresent() && log2Term.isPresent()) {
              if (log1Term.get().equals(log2Term.get())) {
                haveAgreedOnIndexAndTerm = true;
              }
              else {
                assert(!haveAgreedOnIndexAndTerm) : "If two logs contain an entry with the same index and term, then the logs are identical in all entries up through the given index. Violation at index "+index;
              }
            }
            else break;
          }
        }
      }

      // Leader Completeness:
      // If a log entry is committed in a given term, then that entry will be present in the logs of the leaders for all
      // higher-numbered terms

      // TODO: unsure how to verify this from here

      // State Machine Safety:
      // If any server has applied a log entry at a given index, then no other server will ever apply a different log
      // entry for the same index.

      for (int i = 0; i < nodes.size() - 1; i++) {
        RaftLog log1 = nodes.get(i).mutableState().log;
        for (int j = i+1; j < nodes.size(); j++) {
          RaftLog log2 = nodes.get(j).mutableState().log;

          // Check non-compacted log entries

          long maxSharedIndex = Math.min(log1.getLastEntryIndex(), log2.getLastEntryIndex());
          for (long index = maxSharedIndex; index >= 0; index --) {
            Optional<EloquentRaftProto.LogEntry> log1Entry = log1.getEntryAtIndex(index);
            Optional<EloquentRaftProto.LogEntry> log2Entry = log2.getEntryAtIndex(index);
            if (log1Entry.isPresent() && log2Entry.isPresent()) {
              //noinspection RedundantIfStatement
              if (log1Entry.get().getTerm() == log2Entry.get().getTerm()) {
                assert(log1Entry.get().toByteString().equals(log2Entry.get().toByteString())) : "Two log entries at the same index with the same term should be identical";
              }
            }
            else break;
          }
        }
      }
    }
    finally {
      nodesReadLock.unlock();
    }
  }


  /**
   * The current time on the transport.
   */
  public void schedule(long interval, int count, Consumer<Long> task) {
    SafeTimerTask safeTask = new SafeTimerTask() {
      @Override
      public void runUnsafe() throws Throwable {
        task.accept(now());
      }
    };

    // Ensure time doesn't slip while we schedule all of these
    transportMockTimer.withTimerLock(() -> {
      for (int i = 0; i < count; ++i) {
        long targetDelay = interval * (i + 1);
        transportMockTimer.schedule(safeTask, targetDelay);
      }
    });

    synchronized (timekeeper) {
      if (!timekeeper.isAlive() && this.isAlive) {
        timekeeper.start();
      }
    }
  }


  /**
   * Create a nework partition of the given nodes, separating them from the rest of the cluster
   * (but allowing them to talk to each other just fine).
   *
   * @param fromMillis The time at which to install the network partition.
   * @param toMillis The time at which to lift the network partition.
   * @param nodeNames The names of the nodes forming the new partition. These can talk to each other,
   *                  but cannot talk to anyone else in the cluster (or visa versa).
   */
  public void partitionOff(long fromMillis, long toMillis, String... nodeNames) {
    partitions.add(new Partition(fromMillis, toMillis, nodeNames));
  }


  /**
   * Form a complete partition between all of the specified nodes.
   *
   * @see #partitionOff(long, long, String...)
   */
  public void completePartition(long fromMillis, long toMillis, String... nodeNames) {
    for (String node : nodeNames) {
      partitionOff(fromMillis, toMillis, node);
    }
  }

  /**
   * Lift all of our network partitions.
   */
  public void liftPartitions() {
    partitions.clear();
  }


  /** {@inheritDoc} */
  @Override
  public long now() {
    if (this.trueTime) {
      return System.currentTimeMillis();
    } else {
      return transportMockTimer.now();
    }
  }


  /** {@inheritDoc} */
  @Override
  public long nowNanos() {
    if (this.trueTime) {
      return System.nanoTime();
    } else {
      return transportMockTimer.now() * 1000000;
    }
  }


  /** Sleep for the given number of milliseconds. This is purely for mocking for tests. */
  @Override
  public void sleep(long millis, int nanos) {
    AtomicBoolean slept = new AtomicBoolean(false);
    // Wait for the given time to arrive
    schedule(millis, 1, now -> {
      synchronized (slept) {
        slept.set(true);
        slept.notifyAll();
      }
    });
    // Wake up the thread
    synchronized (slept) {
      while (!slept.get() && isAlive) {
        try {
          slept.wait(100);
        } catch (InterruptedException ignored) {}
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void scheduleHeartbeat(Supplier<Boolean> alive, long period, Runnable heartbeatFn, Logger log) {
    this.scheduleAtFixedRate(new SafeTimerTask() {
      protected ExecutorService pool() {
        return null;
      }
      @Override
      public void runUnsafe() {
        if (alive.get()) {
          heartbeatFn.run();
        } else {
          this.cancel();
        }
      }
    }, period);
  }


  /** Schedule an event on the transport's time. */
  @Override
  public void scheduleAtFixedRate(SafeTimerTask task, long period) {
    task.run(Optional.empty());
    transportMockTimer.schedule(task, 0, period);
  }


  /** Schedule an event on the transport's time. */
  @Override
  public void schedule(SafeTimerTask task, long delay) {
    schedule(delay, 1, now -> task.run(Optional.empty()));
  }


  /** {@inheritDoc} */
  @SuppressWarnings("unchecked")
  @Override
  public <E> E getFuture(CompletableFuture<E> future, Duration timeout) throws InterruptedException, ExecutionException, TimeoutException {
    long millisSlept = 0;
    while (!future.isDone() && !future.isCancelled() && !future.isCompletedExceptionally()) {
      sleep(1);
      if (Thread.interrupted()) {
        throw new InterruptedException();
      }
      millisSlept += 1;
      if (millisSlept > timeout.toMillis()) {
        throw new TimeoutException("Took too long to return future");
      }
    }
    if (future.isCompletedExceptionally()) {
      Throwable exception = (Throwable) future.exceptionally(ex -> (E) ex).get();
      throw new ExecutionException(exception);
    }
    E result = future.getNow(null);
    if (result == null) {
      throw new IllegalStateException("Logic error in future resolution");
    }
    return result;
  }


  /**
   * If true, this packet is destined destination be dropped.
   * This can happen either sender random network failures (see {@link #dropProb}),
   * or if there's a partition in place at the delivery time (see {@link #partitions}).
   *
   * @param sender The source server.
   * @param destination The server we're sending destination.
   * @param delay The time at which this packet is intended destination arrive at its destination.
   *
   * @return True if we should drop the packet.
   */
  private boolean shouldDrop(String sender, String destination, long delay) {
    // 1. Resolve partitions
    Iterator<Partition> partitions = this.partitions.iterator();
    long deliveryTime = now() + delay;

    while (partitions.hasNext()) {
      Partition partition = partitions.next();
      if (partition.endTime < now()) {
        partitions.remove();  // some cleanup
      } else if (partition.startTime <= deliveryTime && partition.endTime > deliveryTime) {
        // Case: this partition is active
        if (( partition.members.contains(sender) && !partition.members.contains(destination)) ||
            (!partition.members.contains(sender) &&  partition.members.contains(destination)) ) {
          // Case: the packet crosses the partition -- drop it
          return true;
        }
      }
    }

    // 2. Resolve random packet drops
    //noinspection RedundantIfStatement
    if (rand.nextDouble() < dropProb) {
      // Case: randomly dropping
      return true;
    }
    // Default: the packet goes through happily
    return false;
  }

  /**
   * Sample a delay from the delay distribution.
   * This is some value between {@link #delayMin} and {@link #delayMax}, with
   * a bias towards {@link #delayMin} plus some epsilon (currently, 10ms).
   */
  private long sampleDelay() {
    if (delayMin == delayMax) {
      // Case: we're running on a deterministic delay
      return delayMin;
    } else if (rand.nextDouble() < 0.1) {
      // Case: we're hit an unusually long delay
      return delayMin + rand.nextInt((int) (delayMax - delayMin));
    } else {
      // Case: we've hit our usual delay range
      return delayMin + rand.nextInt((int) (Math.min(delayMax, delayMin + 10) - delayMin));
    }
  }


  /**
   * Receive a message, and handle the message.
   * This is called by the events scheduled on the main mock timer thread.
   *
   * @param message The received message.
   * @param now The current time. This is mostly for a consistency check -- time should not
   *            move while we are in this function.
   */
  private void receiveMessage(TransportMessage message, long now) {
    List<WaitingCallback> toRun = new ArrayList<>();
    synchronized (waitingCallbacks) {
      Set<WaitingCallback> toDelete = new IdentityHashSet<>();
      try {
        for (WaitingCallback callbackCandidate : waitingCallbacks) {
          if (callbackCandidate.messageId == message.correlationId) {
            // 3.3.1. Case: this is an RPC reply
            log.trace("[{}] {} received RPC reply from {} at time {}; id={} correlation_id={}", now, message.target, message.sender, now, message.id, message.correlationId);
            toRun.add(callbackCandidate);
            toDelete.add(callbackCandidate);
          }
        }
      } finally {
        waitingCallbacks.removeAll(toDelete);  // clean up callbacks
      }
    }

    if (!toRun.isEmpty()) {
      for (WaitingCallback callback : toRun) {
        try {
          callback.onSuccess.accept(EloquentRaftProto.RaftMessage.parseFrom(message.contents));
        } catch (InvalidProtocolBufferException e) {
          log.warn("Transport got a bad protocol buffer; logging in exceptions", e);
          exceptions.add(e);
        }
      }
      return;
    }

    // 3.3.2. Case: this is not an RPC reply
    log.trace("[{}] {} received message from {}; id={} correlation_id={}", now(), message.target, message.sender, message.id, message.correlationId);
    if (message.correlationId >= 0) {
      synchronized (waitingCallbacks) {
        log.trace("[{}] Above message (id={}) has an unmatched correlation_id (corr_id={}). waitingCallbacks={}", now(), message.id, message.correlationId, waitingCallbacks);
      }
    }
    try {
      List<RaftAlgorithm> nodes = new ArrayList<>();
      Lock nodesReadLock = nodesLock.readLock();
      try {
        nodesReadLock.lock();
        nodes.addAll(this.nodes);
      }
      finally {
        nodesReadLock.unlock();
      }

      // 1. Parse the message
      EloquentRaftProto.RaftMessage request;
      try {
        request = EloquentRaftProto.RaftMessage.parseFrom(message.contents);
      } catch (InvalidProtocolBufferException e) {
        log.warn("Could not decode message {}; adding to exception list", message);
        this.exceptions.add(e);
        return;
      }

      // 2. Broadcast the message to the nodes
      // 2.1. Check that the destination is valid
      if (nodes.stream().noneMatch(node -> Objects.equals(message.target, node.serverName()))) {
        log.warn("Server {} was not found in server list {}", message.target, this.nodes.stream().map(RaftAlgorithm::serverName).collect(Collectors.toSet()));
      }
      // 2.2. Deliver the message
      nodes.stream()
          .filter(node -> Objects.equals(message.target, node.serverName()))
          .findAny()
          .ifPresent(node -> {
            Optional<RaftLifecycle> lifecycle = node.lifecycle();
            if (lifecycle.isPresent() && lifecycle.get().CORE_THREAD_POOLS_CLOSED.get()) {
              log.trace("Not delivering messages to "+node.serverName()+" because core thread pools are already closed");
              return;
            }
            if (request.getIsRPC()) {
              // 2.2.1. Case: this is a blocking RPC
              CompletableFuture<EloquentRaftProto.RaftMessage> response = node.receiveRPC(request, true);
              if (response == null) {
                NullPointerException exception = new NullPointerException();
                log.warn("Got null response from RPC: ", exception);
                exceptions.add(exception);
                return;
              }
              assert now() == now : "Time should not be slipping";
              response.whenComplete((reply, e) -> {
                // (this is an RPC response, which is special because it has a correlation id)
                if (e != null) {
                  if (e instanceof TimeoutException || (e instanceof CompletionException && e.getCause() != null && e.getCause() instanceof TimeoutException)) {
                    log.debug("RPC timed out: ", e);
                  } else {
                    log.warn("Got exception from RPC: ", e);
                  }
                }
                sendMessage(message.target, message.sender, reply, Optional.of(message.id));
              });
            } else {
              // 2.2.2. Case: this is a non-blocking message
              if (!(node instanceof SingleThreadedRaftAlgorithm) || ((SingleThreadedRaftAlgorithm) node).alive) {  // don't receive the message if the node is dead. This avoids a lot of otherwise spammy exceptions
                node.receiveMessage(request, (proto) -> sendTransport(message.target, message.sender, proto), true);
              }
            }
          });
    } catch (Throwable t) {
      log.warn("Caught exception on receiving message: ", t);
      exceptions.add(t);
    }
  }
}
