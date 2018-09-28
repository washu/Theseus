package ai.eloquent.raft;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import ai.eloquent.monitoring.Prometheus;
import ai.eloquent.raft.EloquentRaftProto.*;
import ai.eloquent.util.Span;
import ai.eloquent.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A simple little interface for receiving Raft RPC messages.
 * This is also, in effect, the implementation of the Raft protocol.
 */
public interface RaftAlgorithm {
  /**
   * An SLF4J Logger for this class.
   */
  Logger log = LoggerFactory.getLogger(RaftAlgorithm.class);

  /**
   * Timing statistics for Raft.
   */
  Object Summary_timing = Prometheus.summaryBuild("raft", "Statistics on the Raft RPC calls", "rpc");

  /**
   * Receive a non-blocking RPC in the form of a {@link ai.eloquent.raft.EloquentRaftProto.RaftMessage} proto.
   *
   * @param messageProto The {@link ai.eloquent.raft.EloquentRaftProto.RaftMessage} we have received.
   * @param replySender A callback for sending a message to the sender on the underlying transport.
   * @param now The timestamp of the current time. In non-mock cases, this is always {@link System#currentTimeMillis()}.
   */
  @SuppressWarnings("Duplicates")
  default void receiveMessage(EloquentRaftProto.RaftMessage messageProto,
                              Consumer<EloquentRaftProto.RaftMessage> replySender,
                              long now) {

    if (!messageProto.getAppendEntries().equals(AppendEntriesRequest.getDefaultInstance())) {
      // Append Entries
      Object Timer_start = Prometheus.labelAndStartTimer(Summary_timing,"append_entries");
      receiveAppendEntriesRPC(messageProto.getAppendEntries(), msg -> {
        Prometheus.observeDuration(Timer_start);
        replySender.accept(msg);
      }, now);
    } else if (!messageProto.getRequestVotes().equals(RequestVoteRequest.getDefaultInstance())) {
      // Request Votes
      Object Timer_start = Prometheus.labelAndStartTimer(Summary_timing,"request_votes");
      receiveRequestVoteRPC(messageProto.getRequestVotes(), msg -> {
        Prometheus.observeDuration(Timer_start);
        replySender.accept(msg);
      }, now);
    } else if (!messageProto.getInstallSnapshot().equals(InstallSnapshotRequest.getDefaultInstance())) {
      // Install Snapshot
      Object Timer_start = Prometheus.labelAndStartTimer(Summary_timing,"install_snapshot");
      receiveInstallSnapshotRPC(messageProto.getInstallSnapshot(), msg -> {
        Prometheus.observeDuration(Timer_start);
        replySender.accept(msg);
      }, now);
    } else if (!messageProto.getAppendEntriesReply().equals(AppendEntriesReply.getDefaultInstance())) {
      // REPLY Append Entries
      Object Timer_start = Prometheus.labelAndStartTimer(Summary_timing,"append_entries_reply");
      try {
        receiveAppendEntriesReply(messageProto.getAppendEntriesReply(), now);
      } finally {
        Prometheus.observeDuration(Timer_start);
      }
    } else if (!messageProto.getRequestVotesReply().equals(RequestVoteReply.getDefaultInstance())) {
      // REPLY Request Votes
      Object Timer_start = Prometheus.labelAndStartTimer(Summary_timing,"request_votes_reply");
      try {
        receiveRequestVotesReply(messageProto.getRequestVotesReply(), now);
      } finally {
        Prometheus.observeDuration(Timer_start);
      }
    } else if (!messageProto.getInstallSnapshotReply().equals(InstallSnapshotReply.getDefaultInstance())) {
      // REPLY Install Snapshot
      Object Timer_start = Prometheus.labelAndStartTimer(Summary_timing,"installl_snapshot_reply");
      try {
        receiveInstallSnapshotReply(messageProto.getInstallSnapshotReply(), now);
      } finally {
        Prometheus.observeDuration(Timer_start);
      }
    } else if (!messageProto.getAddServer().equals(AddServerRequest.getDefaultInstance())) {
      // Add Server
      Object Timer_start = Prometheus.labelAndStartTimer(Summary_timing,"add_server");
      CompletableFuture<RaftMessage> future = receiveAddServerRPC(messageProto.getAddServer(), now);
      future.whenComplete( (reply, exception) -> {
        Prometheus.observeDuration(Timer_start);
        if (exception != null && reply != null) {
          replySender.accept(reply);
        } else {
          replySender.accept(null);
        }
      });
    } else if (!messageProto.getRemoveServer().equals(RemoveServerRequest.getDefaultInstance())) {
      // Remove Server
      Object Timer_start = Prometheus.labelAndStartTimer(Summary_timing,"remove_server");
      CompletableFuture<RaftMessage> future = receiveRemoveServerRPC(messageProto.getRemoveServer(), now);
      future.whenComplete( (reply, exception) -> {
        Prometheus.observeDuration(Timer_start);
        if (exception != null && reply != null) {
          replySender.accept(reply);
        } else {
          replySender.accept(null);
        }
      });
    } else if (!messageProto.getApplyTransition().equals(ApplyTransitionRequest.getDefaultInstance())) {
      // Apply Transition
      Object Timer_start = Prometheus.labelAndStartTimer(Summary_timing,"apply_transition");
      CompletableFuture<RaftMessage> future = receiveApplyTransitionRPC(messageProto.getApplyTransition(), now);
      future.whenComplete( (reply, exception) -> {
        Prometheus.observeDuration(Timer_start);
        if (exception != null && reply != null) {
          replySender.accept(reply);
        } else {
          replySender.accept(null);
        }
      });
    } else {
      receiveBadRequest(messageProto);
    }
  }


  /**
   * Receive a blocking RPC in the form of a {@link ai.eloquent.raft.EloquentRaftProto.RaftMessage} proto.
   * This function will return a {@link CompletableFuture} that is completed when the RPC completes.
   *
   * @param messageProto The {@link ai.eloquent.raft.EloquentRaftProto.RaftMessage} we have received.
   * @param now The timestamp of the current time. In non-mock cases, this is always {@link System#currentTimeMillis()}.
   *
   * @return A {@link CompletableFuture} that completes when the RPC completes.
   */
  default CompletableFuture<EloquentRaftProto.RaftMessage> receiveRPC(EloquentRaftProto.RaftMessage messageProto,
                                                                      long now) {
    Object Timer_start = null;
    CompletableFuture<EloquentRaftProto.RaftMessage> future = new CompletableFuture<>();
    try {
      if (messageProto.getAppendEntries() != AppendEntriesRequest.getDefaultInstance()) {
        // Append Entries
        Timer_start = Prometheus.labelAndStartTimer(Summary_timing, "append_entries_rpc");
        receiveAppendEntriesRPC(messageProto.getAppendEntries(), future::complete, now);
      } else if (messageProto.getRequestVotes() != RequestVoteRequest.getDefaultInstance()) {
        // Request Votes
        Timer_start = Prometheus.labelAndStartTimer(Summary_timing, "request_votes_rpc");
        receiveRequestVoteRPC(messageProto.getRequestVotes(), future::complete, now);
      } else if (messageProto.getInstallSnapshot() != InstallSnapshotRequest.getDefaultInstance()) {
        // Install Snapshot
        Timer_start = Prometheus.labelAndStartTimer(Summary_timing, "install_snapshop_rpc");
        receiveInstallSnapshotRPC(messageProto.getInstallSnapshot(), future::complete, now);
      } else if (messageProto.getAddServer() != AddServerRequest.getDefaultInstance()) {
        // Add Server
        Timer_start = Prometheus.labelAndStartTimer(Summary_timing, "add_server_rpc");
        future = receiveAddServerRPC(messageProto.getAddServer(), now);
      } else if (messageProto.getRemoveServer() != RemoveServerRequest.getDefaultInstance()) {
        // Remove Server
        Timer_start = Prometheus.labelAndStartTimer(Summary_timing, "remove_server_rpc");
        future = receiveRemoveServerRPC(messageProto.getRemoveServer(), now);
      } else if (messageProto.getApplyTransition() != ApplyTransitionRequest.getDefaultInstance()) {
        // Apply Transition
        Timer_start = Prometheus.labelAndStartTimer(Summary_timing, "transition_rpc");
        future = receiveApplyTransitionRPC(messageProto.getApplyTransition(), now);
      } else {
        Timer_start = Prometheus.labelAndStartTimer(Summary_timing, "unknown_rpc");
        future.completeExceptionally(new IllegalStateException("Message type not implemented: " + messageProto));
      }
    } catch (Throwable t) {
      future.completeExceptionally(t);
    }
    Object Timer_startFinal = Timer_start;
    return future.thenApply(x -> {
      if (Timer_startFinal != null) {
        Prometheus.observeDuration(Timer_startFinal);
      }
      return x;
    });
  }


  /**
   * get A COPY OF the current Raft state.
   */
  RaftState state();

  /**
   * DANGEROUS: USE ONLY IF YOU KNOW WHAT YOU ARE DOING
   *
   * get A REFERENCE TO the current Raft state.
   */
  RaftState mutableState();

  /**
   * DANGEROUS: USE ONLY IF YOU KNOW WHAT YOU ARE DOING
   *
   * get A REFERENCE TO the current Raft state machine.
   */
  RaftStateMachine mutableStateMachine();


  /**
   * The term this node sees. This should simply return {@link RaftState#currentTerm}
   */
  long term();

  /**
   * The name of this server. This should simply return {@link RaftState#serverName}
   */
  String serverName();



  //
  // --------------------------------------------------------------------------
  // APPEND ENTRIES RPC
  // --------------------------------------------------------------------------
  //


  /**
   * Send out an append entries RPC (i.e., a heartbeat) to all listeners on the transport.
   *
   * @param now The current time. This is usually {@link System#currentTimeMillis()}, but can
   *            be mocked by unit tests.
   */
  void broadcastAppendEntries(long now);


  /**
   * Send an append entries request to a particular server.
   *
   * @param target The server we're sending the append entries RPC to.
   * @param nextIndex The nextIndex value on the target machine.
   */
  void sendAppendEntries(String target, long nextIndex);


  /**
   * Receive a request to apply a transition or multiple transitions.
   * <b>This also doubles as the heartbeat for the server.</b>
   * There are three cases here:
   *
   * <ol>
   *   <li><b>If the node is an Oligarch:</b> The node should respond to the heartbeat.</li>
   *   <li><b>If the node is a Shadow and the Oligarchy is full:</b> The node should apply the transition
   *       and not reply.</li>
   *   <li><b>If the node is a Shadow and the Oligarchy is not full:</b> The node should apply the transition
   *       and respond to the heartbeat.</li>
   * </ol>
   *
   * @param heartbeat The request body, doubling both as a heartbeat and a request to mutate the
   *                  state machine.
   * @param replyLeader The method for replying to the leader with an ACK of the request.
   * @param now The timestamp of the current time. In non-mock cases, this is always {@link System#currentTimeMillis()}.
   */
  void receiveAppendEntriesRPC(EloquentRaftProto.AppendEntriesRequest heartbeat,
                               Consumer<EloquentRaftProto.RaftMessage> replyLeader,
                               long now);


  /**
   * We received an asynchronous heartbeat from a server.
   * This function is called to handle that heartbeat reply.
   *
   * @param  reply The heartbeat ACK from the follower node.
   * @param now The timestamp of the current time. In non-mock cases, this is always {@link System#currentTimeMillis()}.
   */
  void receiveAppendEntriesReply(EloquentRaftProto.AppendEntriesReply reply, long now);


  //
  // --------------------------------------------------------------------------
  // INSTALL SNAPSHOT RPC
  // --------------------------------------------------------------------------
  //


  /**
   * Receive a snapshot request RPC call.
   * This should install a snapshot, and does not have to issue a response.
   *
   * @param snapshot The snapshot to install.
   * @param replyLeader The method for replying to the leader with an ACK of the request.
   * @param now The timestamp of the current time. In non-mock cases, this is always {@link System#currentTimeMillis()}.
   */
  void receiveInstallSnapshotRPC(InstallSnapshotRequest snapshot,
                                 Consumer<RaftMessage> replyLeader,
                                 long now);


  /**
   * We received an asynchronous snapshot reply from a server.
   *
   * @param reply The snapshot reply from the follower node.
   * @param now The timestamp of the current time. In non-mock cases, this is always {@link System#currentTimeMillis()}.
   */
  void receiveInstallSnapshotReply(EloquentRaftProto.InstallSnapshotReply reply, long now);


  //
  // --------------------------------------------------------------------------
  // ELECTIONS
  // --------------------------------------------------------------------------
  //


  /**
   * Signal to the cluster that we are a candidate for an election, and we are soliciting votes
   */
  void triggerElection(long now);


  /**
   * Clearly an election has been initiated, and some candidate is requesting votes to become the new leader.
   * This <b>must</b> be responded to both by Oligarch and Shadow nodes.
   *
   * @param voteRequest The request for votes for a particular server.
   * @param replyLeader The method for replying to the leader with an ACK of the request.
   * @param now The timestamp of the current time. In non-mock cases, this is always {@link System#currentTimeMillis()}.
   */
  void receiveRequestVoteRPC(EloquentRaftProto.RequestVoteRequest voteRequest, Consumer<EloquentRaftProto.RaftMessage> replyLeader, long now);


  /**
   * We received votes from another server.
   *
   * @param reply The vote from the server, which we should count to see if we have a
   *              majority.
   * @param now The timestamp of the current time. In non-mock cases, this is always {@link System#currentTimeMillis()}.
   */
  void receiveRequestVotesReply(EloquentRaftProto.RequestVoteReply reply, long now);


  //
  // --------------------------------------------------------------------------
  // CLUSTER MEMBERSHIP
  // --------------------------------------------------------------------------
  //


  /**
   * <p>
   *   A request has been received to add a server to the cluster.
   *   This is only well-defined for the leader -- if the request was not received by the leader,
   *   then it should forward to the leader.
   * </p>
   *
   * <p>
   *   The algorithm for this is the following:
   * </p>
   *
   * <ol>
   *   <li> Reply NOT_LEADER if not leader (in practice, we forward it to the leader).</li>
   *   <li> Catch up server for a fixed number of rounds. Reply TIMEOUT if a server does not make progress for an election
   *        timeout or if the last round takes longer than the election timeout.</li>
   *   <li> Wait until previous configuration in log is committed.</li>
   *   <li> Append a new configuration entry to the log, (old configuration plus new server), commit it using a majority of
   *        the new configuration.</li>
   *   <li> Reply OK.</li>
   * </ol>
   *
   * @param addServerRequest The request to add the server.
   * @param now The timestamp of the current time. In non-mock cases, this is always {@link System#currentTimeMillis()}.
   *
   * @return the RPC response, as a future.
   */
  CompletableFuture<RaftMessage> receiveAddServerRPC(AddServerRequest addServerRequest, long now);


  /**
   * A request has been received to remove a server to the cluster.
   * This is only well-defined for the leader -- if the request was not received by the leader,
   * then it should forward to the leader.
   *
   * @param removeServerRequest The snapshot to install.
   * @param now The timestamp of the current time. In non-mock cases, this is always {@link System#currentTimeMillis()}.
   *
   * @return the RPC response, as a future.
   */
  CompletableFuture<RaftMessage> receiveRemoveServerRPC(RemoveServerRequest removeServerRequest, long now);


  //
  // --------------------------------------------------------------------------
  // CONTROL
  // --------------------------------------------------------------------------
  //


  /**
   * Apply a transition to Raft.
   *
   * @param transition The transition to apply
   * @param now The timestamp of the current time. In non-mock cases, this is always {@link System#currentTimeMillis()}.
   */
  CompletableFuture<RaftMessage> receiveApplyTransitionRPC(EloquentRaftProto.ApplyTransitionRequest transition, long now);


  /**
   * Mark this node for bootstrapping. This should just be an alias for {@link RaftState#bootstrap(boolean)}, with
   * some error checks beforehand.
   * If this node cannot bootstrap, we return false.
   *
   * @return True if we could bootstrap this node.
   */
  boolean bootstrap(boolean force);


  /**
   * A method, called primarily from unit tests, to stop this algorithm and clean up the underlying transport,
   * if appropriate.
   * An empty implementation is OK if there's nothing to stop.
   *
   * @param kill If true, kill the server unceremoniously, without waiting for a handoff.
   */
  default void stop(boolean kill) {}


  /**
   * Check if the algorithm has already been stopped via {@link #stop(boolean)}.
   */
  boolean isRunning();


  /**
   * The interval between heartbeats for this particular Raft node / implementation.
   * This should be substantially shorter than {@link #electionTimeoutMillisRange()}
   */
  default long heartbeatMillis() {
    return 50;  // by default, 50 millis / heartbeat
  }


  /** The default timeout range for elections. */
  Span DEFAULT_ELECTION_RANGE = new Span(1000, 2000);


  /**
   * The election timeout.
   * This should be substantially longer than {@link #heartbeatMillis()}
   */
  default Span electionTimeoutMillisRange() {
    return DEFAULT_ELECTION_RANGE;
  }


  /**
   * A method to be called on every heartbeat interval.
   * If we're the leader, this sends out heartbeats.
   * If we're a follower, this ensures that we can trigger the appropriate timeout events.
   */
  void heartbeat(long now);


  /**
   * A bad RPC call was received. Signal the error appropriately.
   * This can happen for two reasons:
   *
   * <ol>
   *   <li>The RPC type is not supported.</li>
   *   <li>The proto of the RPC was poorly formatted.</li>
   * </ol>
   *
   * @param message The raw message received.
   */
  void receiveBadRequest(RaftMessage message);

  /**
   * Get the RaftLifecycle object that this Algorithm is registered for.
   */
  Optional<RaftLifecycle> lifecycle();

  /**
   * This gets the RaftTransport associated with this RaftAlgorithm.
   */
  RaftTransport getTransport();


  /**
   * Shutdown the argument Raft algorithm, using the time given in the argument transport.
   *
   * @param raft The implementing Raft algorithm.
   * @param transport The transport to run timing on, where necessary.
   * @param allowClusterDeath If true, allow the cluster to lose state and completely shut down.
   *                          Otherwise, we wait for another live node to show up before shutting
   *                          down (the default).
   */
  static void shutdown(RaftAlgorithm raft, RaftTransport transport, boolean allowClusterDeath) {
    // 1. Add ourselves to the hospice
    log.info("{} - Shutting down Raft", raft.mutableState().serverName);
    log.info("{} - [{}] Entering the hospice", raft.mutableState().serverName, transport.now());
    boolean inHospice = false;
    int attempts = 0;
    while (!inHospice && (++attempts) < 50) {
      try {
        // 1.1. Try to add ourselves to the hospice
        RaftMessage response = raft.receiveApplyTransitionRPC(ApplyTransitionRequest.newBuilder()
                .setNewHospiceMember(raft.serverName())
                .build(),
            transport.now()).get(raft.electionTimeoutMillisRange().end + 100, TimeUnit.MILLISECONDS);
        inHospice = response.getApplyTransitionReply().getSuccess();
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        log.info("{} - [{}] Could not apply hospice transition: ", raft.mutableState().serverName, transport.now(), e);
      } finally {
        if (!inHospice) {
          // 1.2. If we failed, wait for an election
          transport.sleep(raft.electionTimeoutMillisRange().end + 100);
        }
      }
    }
    if (attempts >= 50) {
      log.warn("{} - [{}] Could not add ourselves to the hospice; continuing anyways and wishing for the best", raft.mutableState().serverName, transport.now());
    } else {
      log.info("{} - [{}] Entered the hospice", raft.mutableState().serverName, transport.now());
    }

    // 2. Wait for someone else to appear
    while (!allowClusterDeath && raft.mutableState().log.committedQuorumMembers.size() < 2) {
      log.warn("{} - [{}] We're the last member of the quorum -- sleeping to wait for someone else to arrive. Errors={}. Heartbeats from={}. Hospice={}.",
          raft.mutableState().serverName, transport.now(),
          raft instanceof EloquentRaftAlgorithm ? StringUtils.join(((EloquentRaftAlgorithm) raft).errors(), ", ") : "<n/a>",
          raft.mutableState().lastMessageTimestamp.orElse(Collections.emptyMap()).keySet(),
          raft.mutableStateMachine().getHospice()
      );
      transport.sleep(1000);
    }

    // 3. Remove ourselves from the cluster
    log.info("{} - [{}] Removing ourselves from the cluster", raft.mutableState().serverName, transport.now());
    boolean inCluster = true;
    attempts = 0;
    while (inCluster && (++attempts) < 50) {
      try {
        // 3.1. Try to remove ourselves from the cluster
        RaftMessage response = raft.receiveRemoveServerRPC(RemoveServerRequest.newBuilder()
            .setOldServer(raft.serverName())
            .build(), transport.now()).get(raft.electionTimeoutMillisRange().end + 100, TimeUnit.MILLISECONDS);
        if (response.getRemoveServerReply().getStatus() == MembershipChangeStatus.OK) {
          inCluster = false;
        }
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        log.info("{} - [{}] Could not apply remove server transition: ", raft.mutableState().serverName, transport.now(), e);
      } finally {
        attempts += 1;
        if (inCluster) {
          // 3.2. If we failed, wait for an election
          transport.sleep(raft.electionTimeoutMillisRange().end + 100);
          transport.sleep(raft.electionTimeoutMillisRange().end + 100);
        }
      }
    }
    if (attempts >= 50) {
      log.warn("{} - [{}] Could not remove ourselves to the cluster; continuing anyways and wishing for the best", raft.mutableState().serverName, transport.now());
    } else {
      log.info("{} - [{}] Removed ourselves from the cluster", raft.mutableState().serverName, transport.now());
    }

    // 4. Stop the algorithm
    log.info("{} - [{}] Stopping the algorithm", raft.mutableState().serverName, transport.now());
    raft.stop(false);
    log.info("{} - [{}] Stopped the algorithm", raft.mutableState().serverName, transport.now());
  }

}
