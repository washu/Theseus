package ai.eloquent.raft;

import ai.eloquent.util.SafeTimerTask;
import ai.eloquent.util.Span;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;

import ai.eloquent.raft.EloquentRaftProto.*;
import ai.eloquent.util.Uninterruptably;
import org.slf4j.Logger;

/**
 * This encapsulates all the non-functional components in EloquentRaftMember so that the remaining code can be easily
 * unit-tested. That means this handles interfacing with EloquentChannel, as well as handling the timers.
 */
public interface RaftTransport {


  /**
   * The enumeration of different transport types.
   */
  enum Type {
    /** Communicate over UDP and RPC. */
    NET,
    /** A local transport -- only suitable for tests and single-box clusters. */
    LOCAL
  }


  /**
   * Register a listener for RPCs.
   * This must be idempotent!
   *
   * @param listener The listener that should be called on all received RPC requests.
   */
  void bind(RaftAlgorithm listener) throws IOException;


  /**
   * Get the collection of algorithms that have been bound to this transport.
   * This is primarily useful for unit testing.
   */
  Collection<RaftAlgorithm> boundAlgorithms();


  /**
   * Send an RPC request over the transport, expecting a reply.
   *
   * @param sender The name of the node sending the message. In many implementations, this is redundant.
   * @param destination The destination we are sending the message to.
   *                    This is a server name
   *                    on the same cluster as the node this transport is bound to.
   * @param message The message to send, as a {@link ai.eloquent.raft.EloquentRaftProto.RaftMessage}.
   * @param onResponseReceived The callback to run when a response is received from the server
   *                           for the RPC. Either this or onTimeout is always called.
   * @param onTimeout Called when no response is received in the given timeout threshold.
   * @param timeout The number of milliseconds to wait before considering an RPC timed out.
   */
  void rpcTransport(String sender, String destination, EloquentRaftProto.RaftMessage message,
                    Consumer<EloquentRaftProto.RaftMessage> onResponseReceived, Runnable onTimeout, long timeout);


  /**
   * Send an RPC request over the transport, not necessarily expecting a reply.
   *
   * @param sender The name of the node sending the message. In many implementations, this is redundant.
   * @param destination The destination we are sending the message to.
   *                    This is a server name
   *                    on the same cluster as the node this transport is bound to.
   * @param message The message to send, as a {@link ai.eloquent.raft.EloquentRaftProto.RaftMessage}.
   */
  void sendTransport(String sender, String destination, EloquentRaftProto.RaftMessage message);


  /**
   * Broadcast an RPC request over the transport to all members of the cluster.
   *
   * @param sender The name of the node sending the message. In many implementations, this is redundant.
   * @param message The message to send, as a {@link ai.eloquent.raft.EloquentRaftProto.RaftMessage}.
   */
  void broadcastTransport(String sender, EloquentRaftProto.RaftMessage message);


  /**
   * The expected delay, in milliseconds, for a round-trip on this transport.
   * This is used to calibrate the Raft algorithm, and to test the unit tests.
   */
  Span expectedNetworkDelay();


  /** For unit tests primarily -- most transports don't need to be started explicitly */
  default void start() {}


  /** For unit tests primarily -- stop this transport and clean up if necessary. */
  default void stop() {}

  /**
   * If true, we are allowed to block while running on this transport. This is useful
   * for, e.g., unit tests. But, in general, we should try to not block as much as posisble.
   * Defaults to false.
   */
  default boolean threadsCanBlock() {
    return false;
  }


  /**
   * The current (local) time on the transport. This is usually {@link System#currentTimeMillis()} if this isn't
   * a mock.
   */
  default long now() {
    return System.currentTimeMillis();
  }


  /** Sleep for the given number of milliseconds. This is purely for mocking for tests. */
  default void sleep(long millis) {
    sleep(millis, 0);
  }


  /** Sleep for the given number of milliseconds + nanoseconds. This is purely for mocking for tests. */
  default void sleep(long millis, int nanos) {
    try {
      Thread.sleep(millis, nanos);
    } catch (InterruptedException ignored) {}
  }


  /**
   * A special method for scheduling heartbeats. This is so that
   * test transports can mock this method while ensuring that the live raft
   * gets is busy-waiting high-priority thread.
   *
   * @param alive A check for whether this scheduler is still alive.
   * @param period The period on which to schedule heartbeats. The heartbeat interval.
   * @param heartbeatFn The function to call for a heartbeat.
   * @param log A log for printing messages.
   */
  default void scheduleHeartbeat(Supplier<Boolean> alive, long period, Runnable heartbeatFn, Logger log) {
    Thread heartbeat = new Thread() {
      private long lastHeartbeat = 0;
      @Override
      public void run() {
        while (alive.get()) {
          try {
            long nowBefore = now();
            if (nowBefore - lastHeartbeat > 2 * period) {
              log.warn("Heartbeat interval slipped to {}ms", nowBefore - lastHeartbeat);
            }
            if (nowBefore - lastHeartbeat >= period - 1) {
              long nowAfter = nowBefore;  // to be overwritten below
              try {
                // -- do the heartbeat --
                heartbeatFn.run();
                // --                  --
                nowAfter = now();
                long timeTakenOnHeartbeat = nowBefore - nowAfter;
                if (timeTakenOnHeartbeat > period) {
                  log.warn("Heartbeat execution took {}ms", timeTakenOnHeartbeat);
                }
              } finally {
                lastHeartbeat = nowAfter;  // use heartbeat time from after scheduling it
              }
            }
            sleep(0, 100000);  // 0.1ms
          } catch (InterruptedException ignored) {
          } catch (Throwable t) {
            log.warn("Exception on heartbeat thread: ", t);
          }
        }
      }
    };
    heartbeat.setPriority(Thread.MAX_PRIORITY);
    heartbeat.setDaemon(true);
    heartbeat.setName("raft-heartbeat");
    // 1.2. Start the thread
    heartbeat.start();
  }


  /** Schedule an event every |period| seconds. This is an alias for {@link ai.eloquent.util.SafeTimer#scheduleAtFixedRate(SafeTimerTask, long, long)}, but mockable */
  default void scheduleAtFixedRate(SafeTimerTask timerTask, long period) {
    RaftLifecycle.global.timer.get().scheduleAtFixedRate(timerTask, 0L, period);
  }


  /** Schedule an for a time |delay| seconds from now. This is an alias for {@link ai.eloquent.util.SafeTimer#schedule(SafeTimerTask, long)}, but mockable */
  default void schedule(SafeTimerTask timerTask, long delay) {
    RaftLifecycle.global.timer.get().schedule(timerTask, delay);
  }


  /** Get a future. This is just a mockable alias for {@link CompletableFuture#get(long, TimeUnit)}. */
  default <E> E getFuture(CompletableFuture<E> future, Duration timeout) throws InterruptedException, ExecutionException, TimeoutException {
    return future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
  }


  /** @see #sendTransport(String, String, EloquentRaftProto.RaftMessage) */
  default void sendTransport(String sender, String destination, Object messageProto) {
    sendTransport(sender, destination, mkRaftMessage(sender, messageProto));
  }


  /** @see #broadcastTransport(String, EloquentRaftProto.RaftMessage) */
  default void broadcastTransport(String sender, Object messageProto) {
    broadcastTransport(sender, mkRaftMessage(sender, messageProto));
  }


  /** @see #rpcTransport(String, String, EloquentRaftProto.RaftMessage, Consumer, Runnable, long) */
  default void rpcTransport(String sender, String destination, Object message,
                            Consumer<EloquentRaftProto.RaftMessage> onResponseReceived, Runnable onTimeout, long timeout) {
    rpcTransport(sender, destination, mkRaftRPC(sender, message), onResponseReceived, onTimeout, timeout);
  }


  /** @see #rpcTransport(String, String, EloquentRaftProto.RaftMessage, Consumer, Runnable, long) */
  default <E> CompletableFuture<E> rpcTransportAsFuture(String sender,
                                                        String destination,
                                                        Object message,
                                                        BiFunction<RaftMessage, Throwable, E> onResponseReceived,
                                                        Consumer<Runnable> runMethod,
                                                        long timeout) {
    CompletableFuture<E> future = new CompletableFuture<>();
    rpcTransport(
        sender,
        destination,
        mkRaftRPC(sender, message),
        reply -> runMethod.accept(() -> future.complete(onResponseReceived.apply(reply, null))),
        () -> runMethod.accept(() -> future.complete(onResponseReceived.apply(null, new TimeoutException("Timed out RPC from " + sender + " to " + destination)))),
        timeout);
    return future;
  }



  /** @see #rpcTransport(String, String, EloquentRaftProto.RaftMessage, Consumer, Runnable, long) */
  default <E> CompletableFuture<E> rpcTransportAsFuture(String sender, String destination, Object message,
                                                        BiFunction<RaftMessage, Throwable, E> onResponseReceived,
                                                        Consumer<Runnable> runMethod
  ) {
    return rpcTransportAsFuture(sender, destination, message, onResponseReceived, runMethod, 10000L);
  }

  /**
   * Create an RPC request to send over the transport.
   * This is tagged as an RPC, and then the appropriate request is populated in the resulting object.
   *
   * @param requestProto The proto of the request, which we'll wrap in a
   *                     {@linkplain ai.eloquent.raft.EloquentRaftProto.RaftMessage raft request}.
   *
   * @return A Raft RPC request proto we can send to the receiver.
   */
  static EloquentRaftProto.RaftMessage mkRaftMessage(String sender, Object requestProto, boolean isRPC) {
    if (requestProto instanceof EloquentRaftProto.RaftMessage) {
      return ((EloquentRaftProto.RaftMessage) requestProto).toBuilder().setSender(sender).build();
    } else if (requestProto instanceof EloquentRaftProto.RaftMessage.Builder) {
      return ((EloquentRaftProto.RaftMessage.Builder) requestProto).setSender(sender).build();
    }
    EloquentRaftProto.RaftMessage.Builder request = EloquentRaftProto.RaftMessage.newBuilder()
        .setSender(sender)
        .setIsRPC(isRPC);
    if (requestProto instanceof AppendEntriesRequest) {
      // Append Entries
      request.setAppendEntries((AppendEntriesRequest) requestProto);
    } else if (requestProto instanceof InstallSnapshotRequest) {
      // Install Snapshot
      request.setInstallSnapshot((InstallSnapshotRequest) requestProto);
    } else if (requestProto instanceof RequestVoteRequest) {
      // Request Vote
      request.setRequestVotes((EloquentRaftProto.RequestVoteRequest) requestProto);
    } else if (requestProto instanceof AddServerRequest) {
      // Add Server
      request.setAddServer((AddServerRequest) requestProto);
    } else if (requestProto instanceof RemoveServerRequest) {
      // Remove Server
      request.setRemoveServer((RemoveServerRequest) requestProto);
    } else if (requestProto instanceof ApplyTransitionRequest) {
      // Apply Transition
      request.setApplyTransition((ApplyTransitionRequest) requestProto);

    } else if (requestProto instanceof AppendEntriesReply) {
      // RE: Append Entries
      request.setAppendEntriesReply((AppendEntriesReply) requestProto);
    } else if (requestProto instanceof InstallSnapshotReply) {
      // RE: Install Snapshot
      request.setInstallSnapshotReply((InstallSnapshotReply) requestProto);
    } else if (requestProto instanceof RequestVoteReply) {
      // RE: Request Votes
      request.setRequestVotesReply((RequestVoteReply) requestProto);
    } else if (requestProto instanceof AddServerReply) {
      // RE: Add Server
      request.setAddServerReply((AddServerReply) requestProto);
    } else if (requestProto instanceof RemoveServerReply) {
      // RE: Remove Server
      request.setRemoveServerReply((RemoveServerReply) requestProto);
    } else if (requestProto instanceof ApplyTransitionReply) {
      // RE: Apply Transition
      request.setApplyTransitionReply((ApplyTransitionReply) requestProto);

    } else {
      throw new IllegalStateException("Unknown request type for an RPC request: " + requestProto.getClass());
    }
    return request.build();
  }


  /** Create a non-blocking RPC. @see #mkRaftMessage(String, Object, boolean) */
  static EloquentRaftProto.RaftMessage mkRaftMessage(String sender, Object requestProto) {
    return mkRaftMessage(sender, requestProto, false);
  }


  /** Create a blocking RPC. @see #mkRaftMessage(Object, boolean) */
  static EloquentRaftProto.RaftMessage mkRaftRPC(String sender, Object requestProto) {
    return mkRaftMessage(sender, requestProto, true);
  }


  /**
   * Create a new transport of the given type.
   *
   * @param serverName The name of us (our server)
   * @param type The type of transport to create.
   *
   * @return A transport of the given type.
   *
   * @throws IOException Thrown if we could not create the given transport.
   */
  static RaftTransport create(String serverName, RaftTransport.Type type) throws IOException {
    switch (type) {
      case NET:
        return new NetRaftTransport(serverName);
      case LOCAL:
        return new LocalTransport(true, true);
      default:
        throw new IOException("Uncreatable transport type: " + type);
    }
  }


  /**
   * For debugging, get the message type of a given message proto object.
   *
   * @param messageProto A raft message, of presumably unknown type.
   *
   * @return The type of the message
   */
  default String messageType(RaftMessage messageProto) {
    if (messageProto.getAppendEntries() != AppendEntriesRequest.getDefaultInstance()) {
      // Append Entries
      return "append_entries";
    } else if (messageProto.getRequestVotes() != RequestVoteRequest.getDefaultInstance()) {
      // Request Votes
      return "request_votes";
    } else if (messageProto.getInstallSnapshot() != InstallSnapshotRequest.getDefaultInstance()) {
      // Install Snapshot
      return "install_snapshot";
    } else if (messageProto.getAddServer() != AddServerRequest.getDefaultInstance()) {
      // Add Server
      return "add_server";
    } else if (messageProto.getRemoveServer() != RemoveServerRequest.getDefaultInstance()) {
      // Remove Server
      return "remove_server";
    } else if (messageProto.getApplyTransition() != ApplyTransitionRequest.getDefaultInstance()) {
      // Apply Transition
      return "apply_transition";
    } else {
      return "unknown";
    }
  }

}
