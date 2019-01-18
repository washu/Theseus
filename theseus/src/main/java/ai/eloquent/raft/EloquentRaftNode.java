package ai.eloquent.raft;

import ai.eloquent.error.RaftErrorListener;
import ai.eloquent.monitoring.Prometheus;
import ai.eloquent.util.*;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;


/**
 * A node in the Raft cluster.
 * This is as lightweight a class as possible, marrying
 * a {@link RaftAlgorithm} (purely functional with no actual transport logic)
 * with an {@link RaftTransport} (pure transport logic)
 * to produce a functioning node in the cluster.
 *
 * @author <a href="mailto:gabor@eloquent.ai">Gabor Angeli</a>
 */
public class EloquentRaftNode implements HasRaftLifecycle {
  /**
   * An SLF4J Logger for this class.
   */
  private static final Logger log = LoggerFactory.getLogger(EloquentRaftNode.class);

  /** Keeps track of existing {@link RaftErrorListener} **/
  private static ArrayList<RaftErrorListener> errorListeners = new ArrayList<>();

  /**
   * A histogram for measuring the empirical interval between heartbeats.
   */
  private final Object histogramHeartbeatTiming = Prometheus.histogramBuild(
      "raft_heartbeat_interval",
      "The bucketed intervals between heartbeats, to ensure that they're run consistently every 50ms",
      25, 40, 45, 48, 49, 50, 51, 52, 55, 60, 75, 100, 150, 500, 1000
  );


  /**
   * Keeps track of an additional {@link RaftErrorListener} in this class
   *
   * @param errorListener The error listener to add to the listener list.
   *                      Errors will be sent to this and all other listeners.
   */
  protected void addErrorListener(RaftErrorListener errorListener) {
    errorListeners.add(errorListener);
  }

  /**
   * Stop listening from a specific {@link RaftErrorListener}
   *
   * @param errorListener The error listener to be removed
   */
  protected void removeErrorListener(RaftErrorListener errorListener) {
    errorListeners.remove(errorListener);
  }

  /**
   * Clears all the {@link RaftErrorListener}s attached to this class.
   */
  protected void clearErrorListeners() {
    errorListeners.clear();
  }

  /**
   * Alert each of the {@link RaftErrorListener}s attached to this class.
   */
  @SuppressWarnings("unused")
  private void throwRaftError(String incidentKey, String debugMessage) {
    errorListeners.forEach(listener -> listener.accept(incidentKey, debugMessage, Thread.currentThread().getStackTrace()));
  }

  /**
   * The Raft algorithm.
   */
  public final RaftAlgorithm algorithm;


  /**
   * The Raft transport logic.
   */
  public final RaftTransport transport;


  /**
   * The RaftLifecycle object that this Raft node is tied to.
   */
  public final RaftLifecycle lifecycle;


  /**
   * If true, this node is still active.
   * This is used in the timer, and can be shut off via {@link #close()}.
   */
  private boolean alive = true;  // Start alive, of course


  /**
   * The last time we issued a heartbeat to the algorithm. This is mostly for debugging.
   */
  private long lastHeartbeat;


  /**
   * These are hooks we can register on this RaftNode to run right before we begin the official shutdown process.
   * Created for Theseus to be able to close itself when EloquentRaftNode is shut down by RaftLifecycle.
   */
  private List<Runnable> shutdownHooks = new ArrayList<>();


  /**
   * This is the set of failsafes that are registered to run every so often to ensure that Raft is in an OK state.
   */
  private final List<RaftFailsafe> failsafes = new ArrayList<>();

  /**
   * This is a reference to the failsafe timer task we have running, so we can clean it up on shutdown.
   */
  private Pointer<SafeTimerTask> failsafeTimerTask = new Pointer<>();


  /** The straightforward constructor. */
  public EloquentRaftNode(RaftAlgorithm algorithm, RaftTransport transport, RaftLifecycle lifecycle) {
    this.algorithm = algorithm;
    this.transport = transport;
    this.lifecycle = lifecycle;
    this.lastHeartbeat = transport.now();
    // Bind to the transport just in case. This should already be bound though
    try {
      transport.bind(algorithm);
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
  }


  /**
   * This adds a hook to run before we shutdown this RaftNode. The hook will run on the thread that calls close() on
   * this EloquentRaftNode (generally the shutdown hook in RaftLifecycle, from RaftLifecycle.shutdown()).
   *
   * @param shutdownHook The hook to run on shutdown
   */
  public void registerShutdownHook(Runnable shutdownHook) {
    this.shutdownHooks.add(shutdownHook);
  }


  /**
   * This begins the process of discovering and joining the Raft cluster as a member.
   */
  public void start() {
    // 1. Heartbeat thread
    // Note[gabor]: we use a thread here to avoid issues with scheduling timers. This is nearly busy-waiting,
    // but with 0.1ms sleeps between loop iterations so we don't consume quite as much CPU.
    this.transport.scheduleHeartbeat(() -> alive, algorithm.heartbeatMillis(), () -> {
      long lastHb = this.lastHeartbeat;
      Prometheus.histogramObserve(histogramHeartbeatTiming, transport.now() - lastHb);
      try {
        algorithm.heartbeat();
      } finally {
        this.lastHeartbeat = transport.now();
      }
    }, log);

    // 2. Failsafe timer
    SafeTimerTask failsafeTask = new SafeTimerTask() {
      /** A thread pool for running Raft heartbeats */
      private final ExecutorService pool = lifecycle.managedThreadPool(
          "raft-failsafe",
          Math.max(Thread.MAX_PRIORITY - 1, Thread.MIN_PRIORITY)  // Raft heartbeats are high priority
      );

      /** {@inheritDoc} */
      @Override
      protected ExecutorService pool() {
        return pool;  // note[gabor] must return a cached pool, or else we recreate the pool on every task
      }

      /** {@inheritDoc} */
      @Override
      public void runUnsafe() {
        try {
          // Run the failsafe
          if (alive) {
            // Run our any registered failsafes
            for (RaftFailsafe failsafe : EloquentRaftNode.this.failsafes) {
              failsafe.heartbeat(EloquentRaftNode.this.algorithm, System.currentTimeMillis());
            }
          } else {
            log.info("{} - Stopping failsafe timer by user request", algorithm.serverName());
            this.cancel();
          }
        } catch (Throwable t) {
          log.warn("Got exception on Raft failsafe timer task: ", t);
        }
      }
    };
    transport.scheduleAtFixedRate(failsafeTask, Duration.ofSeconds(1).toMillis());
    failsafeTimerTask.set(failsafeTask);
  }


  /**
   * Bootstrap this node of the cluster.
   * This will put it into a state where it can become a candidate and start elections.
   *
   * @return True if we could bootstrap the node.
   */
  public boolean bootstrap(boolean force) {
    return algorithm.bootstrap(force);
  }


  /**
   * Submit a transition to Raft.
   *
   * @param transition The transition we are submitting.
   *
   * @return A future for the transition, marking whether it completed successfully.
   */
  public CompletableFuture<Boolean> submitTransition(byte[] transition) {
    return algorithm.receiveRPC(RaftTransport.mkRaftRPC(algorithm.serverName(),
        EloquentRaftProto.ApplyTransitionRequest.newBuilder()
            .setTransition(ByteString.copyFrom(transition))
            .setTerm(algorithm.term())
            .build()),
        false
    ).thenApply(reply -> reply.getApplyTransitionReply().getSuccess());
  }


  /**
   * Return any errors Raft has encountered.
   */
  public List<String> errors() {
    List<String> errors = new ArrayList<>();
    if (algorithm instanceof EloquentRaftAlgorithm) {
      errors.addAll(((EloquentRaftAlgorithm) algorithm).errors());
    } else if (algorithm instanceof SingleThreadedRaftAlgorithm) {
      errors.addAll(((SingleThreadedRaftAlgorithm) algorithm).errors());
    }
    if (!alive) {
      errors.add("Node is not alive");
    }
    if (alive) {
      if (Math.abs(transport.now() - lastHeartbeat) > algorithm.heartbeatMillis() * 2) {
        errors.add("Last heartbeat was " + TimerUtils.formatTimeSince(lastHeartbeat) + " ago");
      }
    }
    return errors;
  }


  /**
   * Returns true if we're currently alive, false if we've closed.
   */
  public boolean isAlive() {
    return this.alive;
  }


  /** {@inheritDoc} */
  @Override
  public String serverName() {
    return algorithm.serverName();
  }


  /**
   * This does an orderly shutdown of this member of the Raft cluster, stopping its heartbeats and removing it
   * from the cluster.
   *
   * If blocking is true, this will wait until the
   * cluster has at least one other member to carry on the state before shutting down.
   *
   * <p>
   *   <b>WARNING:</b> once closed, this cannot be reopened
   * </p>
   *
   * @param allowClusterDeath If true, allow the cluster to lose state and completely shut down.
   *                          Otherwise, we wait for another live node to show up before shutting
   *                          down (the default).
   */
  @Override
  public void close(boolean allowClusterDeath) {
    if (this.alive) {
      log.info(algorithm.serverName() + " - " + "Stopping Raft node {}", this.algorithm.serverName());
      for (Runnable runnable : shutdownHooks) {
        runnable.run();
      }
      RaftAlgorithm.shutdown(this.algorithm, this.transport, allowClusterDeath);
      this.alive = false;
      log.info(algorithm.serverName() + " - " + "Stopped Raft node {}", this.algorithm.serverName());
    } else {
      log.warn("Detected double shutdown of {} -- ignoring", this.algorithm.serverName());
    }
  }


  /**
   * Register a new failsafe to run occasionally on this node.
   *
   * @param failsafe the one to register
   */
  public void registerFailsafe(RaftFailsafe failsafe) {
    this.failsafes.add(failsafe);
  }
}
