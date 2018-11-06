package ai.eloquent.raft;

import ai.eloquent.monitoring.Prometheus;
import ai.eloquent.raft.EloquentRaftProto.*;
import ai.eloquent.util.RuntimeInterruptedException;
import ai.eloquent.util.TimerUtils;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.sun.management.GcInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * The implementation of Raft, filling in the expected behavior for all of the callbacks.
 *
 * @author <a href="mailto:gabor@eloquent.ai">Gabor Angeli</a>
 */
@SuppressWarnings("Duplicates")
public class EloquentRaftAlgorithm implements RaftAlgorithm {
  /**
   * An SLF4J Logger for this class.
   */
  private static final Logger log = LoggerFactory.getLogger(EloquentRaftAlgorithm.class);

  /**
   * A <b>very</b> conservative timeout that defines when we consider a machine to be down.
   * This is used in (at least) two places:
   *
   * <ol>
   *   <li>Timing out membership changes, in the case that it takes us longer than this amount to get to the point of
   *       being able to add a configuration entry.</li>
   *   <li>Removing a server from the configuration that has not been responding to heartbeats.</li>
   * </ol>
   */
  public static final long MACHINE_DOWN_TIMEOUT = 30000;

  /**
   * The number of broadcasts that can happen within a heartbeat interval before
   * we start dropping them.
   * For example, setting this to 5 means that 5 broadcasts can happen every 50ms.
   * Setting this to 10 means that 10 broadcasts can happen every 50ms.
   */
  private static int LEAKY_BUCKET_SIZE = 10;


  /**
   * The Raft state
   */
  private final RaftState state;


  /**
   * The underlying transport for this raft implementation
   */
  public final RaftTransport transport;


  /**
   * Only one add server / remove server call can be taking place at a time, so we have a single Future for
   * serializing these calls one after another.
   *
   * This is used exclusively in
   * {@link #receiveAddServerRPC(AddServerRequest)} and
   * {@link #receiveRemoveServerRPC(RemoveServerRequest)},
   * and is not stored in the {@link RaftState} because it's only for control flow (a form of synchronization), not for keeping actual state.
   */
  private CompletableFuture<RaftMessage> clusterMembershipFuture;  // initialized in the constructor


  /**
   * The last time a cluster membership state was committed.
   * This is to compute the grace period for cluster mismatches to be forcefully
   * overwritten, and is not part of the core Raft algorithm.
   */
  private volatile long lastClusterMembershipChange;

  /**
   * The last times we broadcast our heartbeat.
   * Useful for rate-limiting our messages.
   */
  private final long[] lastBroadcastTimes = new long[LEAKY_BUCKET_SIZE + 1];

  /**
   * The pointer for the next index in {@link #lastBroadcastTimes}.
   */
  private volatile AtomicInteger lastBroadcastNextIndex = new AtomicInteger(0);

  /**
   * This is the lifecycle that this algorithm is associated with. This is only for mocks to use.
   */
  private final Optional<RaftLifecycle> lifecycle;

  /** The ID of the thread that's meant to be driving Raft, if we have one. */
  private long drivingThreadId = -1;  // Disabled initially

  /** A means of queuing tasks on the driving thread. */
  private Consumer<Runnable> drivingThreadQueue = Runnable::run;  // Disabled initially


  /**
   * Create a new auto-resizing Raft algorithm.
   * Note that this node is initially silent -- it will not initiate elections, and is not a voting member
   * of its own cluster.
   *
   * @param serverName The name of this server. This <b>MUST</b> be unique within a cluster.
   * @param stateMachine The state machine we are running on this Raft cluster.
   * @param transport The underlying transport. This should match the cluster name argument.
   * @param targetClusterSize The target size for the Raft cluster.
   * @param raftStateThreadPool A pool to pass to RaftState for it to use
   */
  public EloquentRaftAlgorithm(String serverName, RaftStateMachine stateMachine,
                        RaftTransport transport, int targetClusterSize,
                        ExecutorService raftStateThreadPool, Optional<RaftLifecycle> lifecycle) {
    this(new RaftState(serverName, stateMachine, targetClusterSize, raftStateThreadPool), transport, lifecycle);
  }


  /**
   * Create a new fixed-size Raft algorithm.
   * That is, a node that won't automatically resize -- you can still add or remove nodes
   * manually.
   *
   * @param serverName The name of this server. This <b>MUST</b> be unique within a cluster.
   * @param stateMachine The state machine we are running on this Raft cluster.
   * @param transport The underlying transport. This should match the cluster name argument.
   * @param quorum The configuration of the voting members of the cluster. The cluster will not automatically
   *               resize.
   * @param raftStateThreadPool A pool to pass to RaftState for it to use
   */
  public EloquentRaftAlgorithm(String serverName, RaftStateMachine stateMachine,
                               RaftTransport transport, Collection<String> quorum, ExecutorService raftStateThreadPool, Optional<RaftLifecycle> lifecycle) {
    this(new RaftState(serverName, stateMachine, quorum, raftStateThreadPool), transport, lifecycle);
  }


  /**
   * A constructor for unit tests primarily, that explicitly sets the state for Raft.
   */
  public EloquentRaftAlgorithm(RaftState state, RaftTransport transport, Optional<RaftLifecycle> lifecycle) {
    this.state = state;
    this.transport = transport;
    this.lifecycle = lifecycle;
    this.clusterMembershipFuture = CompletableFuture.completedFuture(RaftMessage.newBuilder().setSender(state.serverName).build());
    this.lastClusterMembershipChange = transport.now();
  }


  /**
   * Set the thread that should be driving Raft, for assertions
   */
  void setDrivingThread(Consumer<Runnable> queue) {
    this.drivingThreadId = Thread.currentThread().getId();
    this.drivingThreadQueue = queue;
  }


  /** {@inheritDoc} */
  @Override
  public RaftState state() {
    assert drivingThreadId < 0 || drivingThreadId == Thread.currentThread().getId() : "Eloquent Raft Algorithm should only be run from the driving thread (" + drivingThreadId + ") but is being driven by " + Thread.currentThread();
    return state.copy();
  }

  /** {@inheritDoc} */
  @Override
  public RaftState mutableState() {
    // note[gabor]: Don't do driving thread assert -- this is a mutable state anyways
    return state;
  }

  @Override
  public RaftStateMachine mutableStateMachine() {
    // note[gabor]: Don't do driving thread assert -- this is a mutable state anyways
    return state.log.stateMachine;
  }


  /** {@inheritDoc} */
  @Override
  public long term() {
    return state.currentTerm;
  }


  /** {@inheritDoc} */
  @Override
  public String serverName() {
    // note[gabor]: Don't do driving thread assert -- this is a final variable
    return state.serverName;
  }


  //
  // --------------------------------------------------------------------------
  // APPEND ENTRIES RPC
  // --------------------------------------------------------------------------
  //


  /** {@inheritDoc} */
  @Override
  public void receiveAppendEntriesRPC(AppendEntriesRequest heartbeat,
                                      Consumer<RaftMessage> replyLeader) {
    assert drivingThreadId < 0 || drivingThreadId == Thread.currentThread().getId() : "Eloquent Raft Algorithm should only be run from the driving thread (" + drivingThreadId + ") but is being driven by " + Thread.currentThread();
    String methodName = "AppendEntriesRPC";
    RaftMessage reply;
    if (heartbeat.getEntriesCount() > 0) log.trace("{} - [{}] {}; num_entries={}  prevIndex={}  leader={}", state.serverName, transport.now(), methodName, heartbeat.getEntriesCount(), heartbeat.getPrevLogIndex(), heartbeat.getLeaderName());
    AppendEntriesReply.Builder partialReply = AppendEntriesReply.newBuilder()
        .setFollowerName(state.serverName);  // signal our name

    // Step 1: Reply false if term < currentTerm
    if (!canPerformFollowerAction(methodName, heartbeat.getTerm(), false)) {
      reply = RaftTransport.mkRaftMessage(state.serverName, partialReply
          .setTerm(state.currentTerm)                         // signal the new term
          .setNextIndex(state.log.getLastEntryIndex() + 1)    // signal the next index in the new term
          .setSuccess(false)                                  // there was no update
          .setMissingFromQuorum(!this.state.log.latestQuorumMembers.contains(this.state.serverName))
          .build());
    } else {

      // Valid heartbeat: Reset the election timeout
      Optional<String> leader = state.leader;
      state.resetElectionTimeout(transport.now(), heartbeat.getLeaderName());
      if (!leader.equals(state.leader)) {
        log.info("{} - {}; registered new leader={}  old leader={}  time={}", state.serverName, methodName, state.leader.orElse("<none>"), leader.orElse("<none>"), transport.now());
      }

      // Step 2-4:
      //   2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
      //   3. If an existing entry conflicts with the new one (same index but different terms), delete the existing
      //       entry and all that follow it.
      //   4. Append any new entries not already in the log
      if (state.log.appendEntries(heartbeat.getPrevLogIndex(), heartbeat.getPrevLogTerm(), heartbeat.getEntriesList())) {
        // Case: success
        // (some debugging)
        assert state.log.getLastEntryIndex() >= heartbeat.getPrevLogIndex() + heartbeat.getEntriesCount()
            : "appendEntries succeeded on " + state.serverName + ", but latest log index is not caught up(!?)  prevLogIndex=" + heartbeat.getPrevLogIndex() + "  entryCount=" + heartbeat.getEntriesCount() +"  new_lastIndex=" + state.log.getLastEntryIndex();
        if (!heartbeat.getEntriesList().isEmpty()) {
          try {
            KeyValueStateMachineProto.Transition transition = KeyValueStateMachineProto.Transition.parseFrom(heartbeat.getEntriesList().get(0).getTransition());
            log.trace("{} - {} Appended entry @ time={} to index {} of type {}: {}",
                state.serverName, methodName, transport.now(), heartbeat.getEntriesList().get(0).getIndex(), transition.getType(), transition);
          } catch (InvalidProtocolBufferException ignored) { }
        }
        // (update our commit index)
        if (state.commitIndex() < heartbeat.getLeaderCommit()) {
          assert state.commitIndex() <= heartbeat.getLeaderCommit()
              : "Leader has committed past our current index on " + state.serverName + "!  commitIndex=" + state.commitIndex() + "  heartbeat.commitIndex=" + heartbeat.getLeaderCommit() + "  prevLogIndex=" + heartbeat.getPrevLogIndex() + "  entryCount=" + heartbeat.getEntriesCount() +"  new_lastIndex=" + state.log.getLastEntryIndex();
          state.commitUpTo(heartbeat.getLeaderCommit(), transport.now());
        }
        // (step down from any elections)
        if (state.leadership == RaftState.LeadershipStatus.LEADER) {
          log.warn("{} - {} Raft got an inbound heartbeat as a leader -- this is a possible split-brain. Stepping down from leadership so we can sort it out democratically.", state.serverName, methodName);
          state.stepDownFromElection();
        }
        // (reply)
        if (heartbeat.getEntriesCount() > 0) log.trace("{} - {} replying success;  term={}  nextIndex={}  commitIndex={}", state.serverName, methodName, state.currentTerm, state.log.getLastEntryIndex() + 1, state.commitIndex());
        reply = RaftTransport.mkRaftMessage(state.serverName, partialReply
            .setSuccess(true)                                   // RPC was successful
            .setTerm(state.currentTerm)                         // signal the new term
            .setNextIndex(state.log.getLastEntryIndex() + 1)    // signal the next index in the new term
            .setMissingFromQuorum(!this.state.log.latestQuorumMembers.contains(this.state.serverName))
            .build());
      } else if (heartbeat.getEntriesCount() > 0) {
        // Case: failed to append
        //noinspection StatementWithEmptyBody
        if (heartbeat.getPrevLogIndex() < this.state.log.snapshot.map(snapshot -> snapshot.lastIndex).orElse(0L)) {
          // This is actually ok, because we can receive appends out of order and fail an append that comes before a snapshot
        } else {
          log.warn("{} - {} replying error;  term={}  lastIndex={}  heartbeat.term={}  heartbeat.prevIndex={}",
              state.serverName, methodName, state.currentTerm, state.log.getLastEntryIndex(), heartbeat.getTerm(), heartbeat.getPrevLogIndex());
        }
        long requestIndex = Math.max(
            state.log.snapshot.map(snapshot -> snapshot.lastIndex).orElse(0L),
            Math.min(heartbeat.getPrevLogIndex() - 1, state.log.getLastEntryIndex())
        );
        reply = RaftTransport.mkRaftMessage(state.serverName, partialReply
            .setSuccess(false)                                  // the update failed
            .setTerm(state.currentTerm)                         // signal the new term
            .setNextIndex(requestIndex)    // signal the next index in the new term
            .setMissingFromQuorum(!this.state.log.latestQuorumMembers.contains(this.state.serverName))
            .build());
      } else {
        // Case: this heartbeat has no payload
        if (heartbeat.getEntriesCount() > 0) log.trace("{} - {} heartbeat has no payload;  term={}  nextIndex={}",
            state.serverName, methodName, state.currentTerm, heartbeat.getPrevLogIndex());
        reply = RaftTransport.mkRaftMessage(state.serverName, partialReply
            .setSuccess(false)                                  // everything went ok, but no updates
            .setTerm(state.currentTerm)                         // signal the new term
            .setNextIndex(state.log.getLastEntryIndex() + 1)    // signal the next index in the new term
            .setMissingFromQuorum(!this.state.log.latestQuorumMembers.contains(this.state.serverName))
            .build());
      }
    }

    replyLeader.accept(reply);
  }


  /** {@inheritDoc} */
  @SuppressWarnings({"ConstantConditions", "StatementWithEmptyBody"})
  @Override
  public void receiveAppendEntriesReply(AppendEntriesReply reply) {
    assert drivingThreadId < 0 || drivingThreadId == Thread.currentThread().getId() : "Eloquent Raft Algorithm should only be run from the driving thread (" + drivingThreadId + ") but is being driven by " + Thread.currentThread();
    String methodName = "AppendEntriesReply";
    long begin = System.currentTimeMillis();
    try {
      log.trace("{} - [{}] {} from {}. success={}  term={}  nextIndex={}", state.serverName, transport.now(), methodName, reply.getFollowerName(), reply.getSuccess(), reply.getTerm(), reply.getNextIndex());
      asLeader(reply.getFollowerName(), reply.getTerm(), () -> {
        assert state.isLeader() : "We should still be a leader if we process this reply";
        assert reply.getTerm() == state.currentTerm : "We should be on the correct term when getting a heartbeat reply";
        assert state.nextIndex.isPresent() : "We should have a nextIndex map";
        if (reply.getSuccess()) {
          // Case: the call was successful; update nextIndex + matchIndex
          state.nextIndex.ifPresent(map -> map.put(reply.getFollowerName(), Math.max(map.getOrDefault(reply.getFollowerName(), 0L), reply.getNextIndex())));
          state.matchIndex.ifPresent(map -> map.put(reply.getFollowerName(), Math.max(map.getOrDefault(reply.getFollowerName(), 0L), reply.getNextIndex() - 1)));
          // Eloquent Addition: sanity check the quorum
          if (reply.getMissingFromQuorum() &&
              state.log.committedQuorumMembers.contains(reply.getFollowerName()) &&
              state.log.latestQuorumMembers.contains(reply.getFollowerName()) &&
              clusterMembershipFuture.isDone() &&
              lastClusterMembershipChange + this.electionTimeoutMillisRange().end < transport.now()
          ) {
            log.warn("{} - {} detected quorum mismatch on node {}. Trying to recover, but this is an error state.", this.state.serverName, methodName, reply.getFollowerName());
            receiveAddServerRPC(AddServerRequest.newBuilder()
                .setNewServer(reply.getFollowerName())
                .addAllQuorum(state.log.latestQuorumMembers)
                .build());
          }
        } else {
          // Case: the call was rejected by the client -- resend the append entries call
          log.trace("{} - {} from {} was rejected. resending with term={} and nextIndex={}", state.serverName, methodName, reply.getFollowerName(), reply.getTerm(), reply.getNextIndex());
          long index = Math.max(0, reply.getNextIndex() - 1);
          if (index > 0) {
            Optional<LogEntry> entryAtIndex = state.log.getEntryAtIndex(index);
            if (entryAtIndex.isPresent()) {
              // Case: the entry we'd like to send is in our logs
            } else if (state.log.snapshot.isPresent() && index == state.log.snapshot.get().lastIndex) {
              // Case: the entry we'd like to send is the last entry of our snapshot
            } else {
              // Case: the follower asked for an index that we do not have -- this is a bit of an error case
              long match = state.matchIndex.map(map -> map.get(reply.getFollowerName())).orElse(0L);
              if (match >= index) {
                // Case: this is a message arriving out of order, and we've already snapshotted the old logs, so this is fine
              } else {
                state.matchIndex.ifPresent(map -> map.put(reply.getFollowerName(), 0L));
                state.nextIndex.ifPresent(map -> map.put(reply.getFollowerName(), 0L));
                log.warn("{} - {}  Follower asked for an index we do not have; setting index to 0 (to trigger snapshot)", state.serverName, methodName);
                index = 0; // note[gabor] this used to decrement by 1 and loop on what is now 'if (index > 0)', but that loop will never succeed?
              }
            }
          }

          // Check that we're not sending a set of empty entries, cause that would cause an infinite loop
          Optional<List<LogEntry>> entries = state.log.getEntriesSinceInclusive(index + 1);
          if (!entries.isPresent() || entries.get().size() > 0) {
            // This handles choosing between InstallSnapshot and AppendEntries
            sendAppendEntries(reply.getFollowerName(), index + 1);
          }
        }
      });
    } finally {
      assert checkDuration(methodName, begin, System.currentTimeMillis());
    }
  }


  /**
   * A generalized function for both {@link #sendAppendEntries(String, long)} and {@link #rpcAppendEntries(String, long, long)}.
   */
  private <E> E generalizedAppendEntries(long nextIndex, BiFunction<AppendEntriesRequest, InstallSnapshotRequest, E> fn) {
    assert drivingThreadId < 0 || drivingThreadId == Thread.currentThread().getId() : "Eloquent Raft Algorithm should only be run from the driving thread (" + drivingThreadId + ") but is being driven by " + Thread.currentThread();
    // The request we're sending. This does not need to be sent in a lock
    Optional<AppendEntriesRequest> appendEntriesRequest = Optional.empty();
    Optional<InstallSnapshotRequest> snapshotRequest = Optional.empty();

    // 1. Construct the broadcast
    Optional<List<LogEntry>> entries = state.log.getEntriesSinceInclusive(nextIndex);
    Optional<Long> prevEntryTerm = state.log.getPreviousEntryTerm(nextIndex - 1);
    if (entries.isPresent() && prevEntryTerm.isPresent()) {
      // 1A. We have entries to send -- send them.
      appendEntriesRequest = Optional.of(AppendEntriesRequest
          .newBuilder()
          .setTerm(state.currentTerm)
          .setLeaderName(state.serverName)
          .setPrevLogIndex(nextIndex - 1)
          .setPrevLogTerm(prevEntryTerm.get())
          .addAllEntries(entries.get())
          .setLeaderCommit(state.log.getCommitIndex())
          .build());
      log.trace("{} - sending appendEntriesRequest; logIndex={}  logTerm={}  # entries={}", state.serverName, nextIndex - 1, prevEntryTerm.get(), entries.get().size());
    } else {
      // 1B. We should send a snapshot
      snapshotRequest = state.log.snapshot.map(snapshot ->
          InstallSnapshotRequest
              .newBuilder()
              .setTerm(state.currentTerm)
              .setLeaderName(state.serverName)
              .setLastIndex(snapshot.lastIndex)
              .setLastTerm(snapshot.lastTerm)
              .addAllLastConfig(snapshot.lastClusterMembership)
              .setData(ByteString.copyFrom(snapshot.serializedStateMachine))
              .build()
      );
    }

    // 2. Send the appropriate message
    return fn.apply(appendEntriesRequest.orElse(null), snapshotRequest.orElse(null));
  }


  /**
   * Equivalent to {@link #sendAppendEntries(String, long)}, but as an RPC.
   * This is used when adding servers to the cluster.
   */
  private CompletableFuture<RaftMessage> rpcAppendEntries(String target, long nextIndex, long timeout) {
    assert drivingThreadId < 0 || drivingThreadId == Thread.currentThread().getId() : "Eloquent Raft Algorithm should only be run from the driving thread (" + drivingThreadId + ") but is being driven by " + Thread.currentThread();
    String methodName = "AppendEntriesRPC";
    return generalizedAppendEntries(nextIndex, (heartbeat, snapshot) -> {
      assert drivingThreadId < 0 || drivingThreadId == Thread.currentThread().getId() : "Eloquent Raft Algorithm should only be run from the driving thread (" + drivingThreadId + ") but is being driven by " + Thread.currentThread();
      if (heartbeat != null) {
        // Case: this is a heartbeat request
        return transport.rpcTransportAsFuture(state.serverName, target, heartbeat, (result, exception) -> {
          assert drivingThreadId < 0 || drivingThreadId == Thread.currentThread().getId() : "Eloquent Raft Algorithm should only be run from the driving thread (" + drivingThreadId + ") but is being driven by " + Thread.currentThread();
          if (result != null) {
            return result;
          } else {
            log.warn("{} - {} Failure: <{}>", state.serverName, methodName, exception == null ? "unknown error" : (exception.getClass().getName() + ": " + exception.getMessage()));
            return RaftTransport.mkRaftMessage(state.serverName, RaftMessage.newBuilder().setAppendEntriesReply(AppendEntriesReply.newBuilder()
                .setFollowerName(target)
                .setTerm(state.currentTerm)
                .setSuccess(false)).build());
          }
        }, this.drivingThreadQueue, timeout);
      } else if (snapshot != null) {
        // Case: this is a snapshot request
        return transport.rpcTransportAsFuture(state.serverName, target, snapshot, (result, exception) -> {
          assert drivingThreadId < 0 || drivingThreadId == Thread.currentThread().getId() : "Eloquent Raft Algorithm should only be run from the driving thread (" + drivingThreadId + ") but is being driven by " + Thread.currentThread();
          if (result != null) {
            return result;
          } else {
            log.warn("{} - {} Failure: <{}>", state.serverName, methodName, exception == null ? "unknown error" : (exception.getClass().getName() + ": " + exception.getMessage()));
            return RaftTransport.mkRaftMessage(state.serverName, RaftMessage.newBuilder().setInstallSnapshotReply(InstallSnapshotReply.newBuilder()
                .setTerm(state.currentTerm)
                .build()));
          }
        }, this.drivingThreadQueue, timeout);
      } else {
        log.warn("{} - We have neither log entries or a snapshot to send to {}.", state.serverName, target);
        return CompletableFuture.completedFuture(RaftMessage.getDefaultInstance());
      }
    }).thenApply(message -> {
      assert drivingThreadId < 0 || drivingThreadId == Thread.currentThread().getId() : "Eloquent Raft Algorithm should only be run from the driving thread (" + drivingThreadId + ") but is being driven by " + Thread.currentThread();
      if (message.getAppendEntriesReply() != AppendEntriesReply.getDefaultInstance()) {
        AppendEntriesReply reply = message.getAppendEntriesReply();
        receiveAppendEntriesReply(reply);
      }
      else if (message.getInstallSnapshotReply() != InstallSnapshotReply.getDefaultInstance()) {
        InstallSnapshotReply reply = message.getInstallSnapshotReply();
        receiveInstallSnapshotReply(reply);
      }
      return message;
    });
  }


  /** {@inheritDoc} */
  @Override
  public void sendAppendEntries(String target, long nextIndex) {
    assert drivingThreadId < 0 || drivingThreadId == Thread.currentThread().getId() : "Eloquent Raft Algorithm should only be run from the driving thread (" + drivingThreadId + ") but is being driven by " + Thread.currentThread();
    log.trace("{} - Sending appendEntries to {} with nextIndex {}", state.serverName, target, nextIndex);
    generalizedAppendEntries(nextIndex, (heartbeat, snapshot) -> {
      if (heartbeat != null) {
        transport.sendTransport(state.serverName, target, heartbeat);
      } else if (snapshot != null) {
        transport.sendTransport(state.serverName, target, snapshot);
      } else {
        log.warn("{} - We have neither log entries or a snapshot to send to {}.", state.serverName, target);
      }
      return null;
    });
  }



  /** {@inheritDoc} */
  @Override
  public void broadcastAppendEntries() {
    assert drivingThreadId < 0 || drivingThreadId == Thread.currentThread().getId() : "Eloquent Raft Algorithm should only be run from the driving thread (" + drivingThreadId + ") but is being driven by " + Thread.currentThread();
    broadcastAppendEntries(false, false);
  }


  /**
   * Broadcast entries, optionally forcing at least the last element to be sent.
   *
   * @see #broadcastAppendEntries()
   */
  private void broadcastAppendEntries(boolean forceLastElement, boolean force) {
    assert drivingThreadId < 0 || drivingThreadId == Thread.currentThread().getId() : "Eloquent Raft Algorithm should only be run from the driving thread (" + drivingThreadId + ") but is being driven by " + Thread.currentThread();
    String methodName = "broadcastAppendEntries";
    AppendEntriesRequest appendEntriesRequest;  // The request we're sending. This does not need to be sent in a lock

    // 1. Rate Limit
    if (!force) {
      int broadcastsSinceLastHeartbeat = 0;
      for (long broadcastTime : lastBroadcastTimes) {
        if (broadcastTime > 0 && transport.now() > broadcastTime && (transport.now() - broadcastTime) < this.heartbeatMillis()) {  // note[gabor] broadcastTime > 0 so that unit tests grounded at 0 can pass
          broadcastsSinceLastHeartbeat += 1;
        }
      }
      if (broadcastsSinceLastHeartbeat > LEAKY_BUCKET_SIZE) {
//        log.info("{} - {}; Rate limiting broadcasts: {}; now={}", state.serverName, methodName, Arrays.toString(lastBroadcastTimes), now);
        return;
      }
    }
    lastBroadcastTimes[lastBroadcastNextIndex.getAndIncrement() % lastBroadcastTimes.length] = transport.now();

    // 2. Preconditions
    if (!this.state.isLeader()) {  // in case we lost leadership in between
      log.trace("{} - {}; Not the leader", state.serverName, methodName);
      return;
    }
    assert state.nextIndex.isPresent() : "Running heartbeat as leader, but don't have a nextIndex defined";

    // 3. Find the largest update we have to send.
    long findUpdateStart = System.currentTimeMillis();
    long minLogIndex = Long.MAX_VALUE;
    long minLogTerm = Long.MAX_VALUE;
    List<LogEntry> argminEntries = null;
    if (state.nextIndex.isPresent()) {
      //noinspection ConstantConditions,OptionalGetWithoutIsPresent
      Map<String, Long> nextIndex = state.nextIndex.get();
      //noinspection ConstantConditions
      for (String member : state.log.getQuorumMembers()) {
        if (!member.equals(state.serverName)) {  // don't include the leader
          // 3.1. Make sure the member is in state's nextIndex
          if (!nextIndex.containsKey(member)) {
            long initializeNextIndex = state.log.getLastEntryIndex() + 1;
            nextIndex.put(member, initializeNextIndex);
          }
          // 3.2. Get the relevant term + index of this member
          long nextIndexForMember = nextIndex.get(member);
          long prevLogIndex = Math.max(0, nextIndexForMember - 1);
          long prevLogTerm = state.log.getPreviousEntryTerm(prevLogIndex).orElse(-1L);
          Optional<List<LogEntry>> entries = state.log.getEntriesSinceInclusive(nextIndexForMember);
          // 3.3. Update the min
          if (entries.isPresent()) {  // Ignore nodes that need a snapshot
            if (prevLogTerm < minLogTerm ||
                (prevLogTerm == minLogTerm && prevLogIndex < minLogIndex)
            ) {
              minLogTerm = prevLogTerm;
              minLogIndex = prevLogIndex;
              argminEntries = entries.get();
            }
          }
        }
      }
    }
    checkDuration("finding update index", findUpdateStart, System.currentTimeMillis());

    // 4. Handle the case that we have no updates
    //noinspection ConstantConditions
    if (minLogTerm == Long.MAX_VALUE || minLogIndex == Long.MAX_VALUE || argminEntries == null) {
      minLogIndex = state.log.getLastEntryIndex();
      minLogTerm = state.log.getLastEntryTerm();
      argminEntries = Collections.emptyList();
      if (forceLastElement && minLogIndex > 0) {  // if we're forcing an update
        minLogIndex -= 1;
        minLogTerm = state.log.getPreviousEntryTerm(minLogIndex).orElse(-1L);
        argminEntries = state.log.getEntriesSinceInclusive(minLogIndex + 1).orElse(Collections.emptyList());
      }
    }

    // 5. Update invariants
    if (state.isLeader()) {
      updateLeaderInvariantsPostMessage(Optional.empty());
    }

    // 6. Construct the broadcast
    long constructProtoStart = System.currentTimeMillis();
    appendEntriesRequest = AppendEntriesRequest
        .newBuilder()
        .setTerm(state.currentTerm)
        .setLeaderName(state.serverName)
        .setPrevLogIndex(minLogIndex)
        .setPrevLogTerm(minLogTerm)
        .addAllEntries(argminEntries)
        .setLeaderCommit(state.log.getCommitIndex())
        .build();
    checkDuration("creating heartbeat proto", constructProtoStart, System.currentTimeMillis());

    // 7. Send the broadcast
    // 7.1. (log the broadcast while in a lock)
    log.trace("{} - Broadcasting appendEntriesRequest; logIndex={}  logTerm={}  # entries={}", state.serverName, minLogIndex, minLogTerm, argminEntries.size());
    // 7.2. (the sending does not need to be locked)
    long broadcastStart = System.currentTimeMillis();
    try {
      transport.broadcastTransport(state.serverName, appendEntriesRequest);
    } finally {
      checkDuration("broadcast transport call", broadcastStart, System.currentTimeMillis());
    }
  }


  //
  // --------------------------------------------------------------------------
  // INSTALL SNAPSHOT RPC
  // --------------------------------------------------------------------------
  //


  /** {@inheritDoc} */
  @Override
  public void receiveInstallSnapshotRPC(InstallSnapshotRequest snapshot, Consumer<RaftMessage> replyLeader) {
    assert drivingThreadId < 0 || drivingThreadId == Thread.currentThread().getId() : "Eloquent Raft Algorithm should only be run from the driving thread (" + drivingThreadId + ") but is being driven by " + Thread.currentThread();
    String methodName = "InstallSnapshotRPC";
    RaftMessage reply;

    log.trace("{} - [{}] {}:  term={}  lastIndex={}", state.serverName, transport.now(), methodName, snapshot.getTerm(), snapshot.getLastIndex());
    InstallSnapshotReply.Builder partialReply = InstallSnapshotReply.newBuilder()
        .setFollowerName(state.serverName);

    // Reset the election timeout
    Optional<String> leader = state.leader;
    state.resetElectionTimeout(transport.now(), snapshot.getLeaderName());
    if (!leader.equals(state.leader)) {
      log.info("{} - {}; registered new leader={}  old leader={}  term={}  lastIndex={}  time={}",
          state.serverName, methodName, state.leader.orElse("<none>"), leader.orElse("<none>"),
          snapshot.getTerm(), snapshot.getLastIndex(), transport.now());
    }

    // Step 1: Reply false if term < currentTerm
    if (!canPerformFollowerAction(methodName, snapshot.getTerm(), false)) {
      reply = RaftTransport.mkRaftMessage(state.serverName, partialReply
          .setTerm(state.currentTerm)                       // signal our term
          .setNextIndex(state.log.getLastEntryIndex() + 1)  // signal our last log index
          .build());
    } else {
      assert snapshot.getTerm() == state.currentTerm : "Should not have been allowed to continue if the terms don't match!";

      // Step 2: Create new snapshot file
      RaftLog.Snapshot requestedSnapshot = new RaftLog.Snapshot(
          snapshot.getData().toByteArray(),
          snapshot.getLastIndex(),
          snapshot.getLastTerm(),
          snapshot.getLastConfigList());

      // Step 3 - 8: Write data into snapshot file + save the snapshot file. If exsiting log entry has same index
      //             and term as lastIndex and lastTerm, discard log up through lastIndex (but retain any following
      //             entries) and reply. Discard entire log. Reset state machine using snapshot contents.
      state.log.installSnapshot(requestedSnapshot, transport.now());

      //  Reply
      log.trace("{} - {} success: last index in log is {}", state.serverName, methodName, state.log.getLastEntryIndex());
      reply = RaftTransport.mkRaftMessage(state.serverName, partialReply
          .setTerm(state.currentTerm)                       // signal our term
          .setNextIndex(state.log.getLastEntryIndex() + 1)  // signal our last log index
          .build());
    }

    replyLeader.accept(reply);
  }



  /** {@inheritDoc} */
  @Override
  public void receiveInstallSnapshotReply(InstallSnapshotReply reply) {
    assert drivingThreadId < 0 || drivingThreadId == Thread.currentThread().getId() : "Eloquent Raft Algorithm should only be run from the driving thread (" + drivingThreadId + ") but is being driven by " + Thread.currentThread();
    String methodName = "InstallSnapshotReply";
    log.trace("{} - [{}] {} from {}. term={}  nextIndex={}", state.serverName, transport.now(), methodName, reply.getFollowerName(), reply.getTerm(), reply.getNextIndex());
    asLeader(reply.getFollowerName(), reply.getTerm(), () -> {
      // As an efficiency boost, update nextIndex and matchIndex
      // This is, strictly speaking, optional, as the next heartbeat will handle this automatically
      state.nextIndex.ifPresent(map -> map.compute(reply.getFollowerName(), (k, v) -> reply.getNextIndex()));
      state.matchIndex.ifPresent(map -> map.compute(reply.getFollowerName(), (k, v) -> reply.getNextIndex() - 1));

      // Always send any updates immediately, if the other endpoint is behind us

      // Check that we're not sending a set of empty entries, cause that would cause an infinite loop
      Optional<List<LogEntry>> entries = state.log.getEntriesSinceInclusive(reply.getNextIndex());
      if (!entries.isPresent() || entries.get().size() > 0) {
        // This handles choosing between InstallSnapshot and AppendEntries
        sendAppendEntries(reply.getFollowerName(), reply.getNextIndex());
      }
    });
  }


  //
  // --------------------------------------------------------------------------
  // ELECTIONS
  // --------------------------------------------------------------------------
  //


  /** {@inheritDoc} */
  @Override
  public void receiveRequestVoteRPC(RequestVoteRequest voteRequest,
                                    Consumer<RaftMessage> replyLeader) {
    assert drivingThreadId < 0 || drivingThreadId == Thread.currentThread().getId() : "Eloquent Raft Algorithm should only be run from the driving thread (" + drivingThreadId + ") but is being driven by " + Thread.currentThread();
    //    if (!alive) { return; }  // Always vote -- we may need the vote on handoff
    String methodName = "RequestVoteRPC";
    RaftMessage reply;

    log.trace("{} - [{}] {};  candidate={}  term={}", state.serverName, transport.now(), methodName, voteRequest.getCandidateName(), voteRequest.getTerm());
    log.info("{} - {};  candidate={}  candidate_term={}  my_term={}", state.serverName, methodName, voteRequest.getCandidateName(), voteRequest.getTerm(), state.currentTerm);

    if (canPerformFollowerAction(methodName, voteRequest.getTerm(), false)) {
      // 1. Resolve who to vote for
      boolean voteGranted;
      if (voteRequest.getTerm() < state.currentTerm) {
        voteGranted = false;
      } else {
        voteGranted = (
            (!state.votedFor.isPresent() || state.votedFor.get().equals(voteRequest.getCandidateName())) &&
                voteRequest.getLastLogIndex() >= state.log.getLastEntryIndex()
        );
        if (voteGranted) {
          Optional<String> leader = state.leader;
          state.voteFor(voteRequest.getCandidateName(), transport.now());  // resets election timer
          if (!leader.equals(state.leader)) {
            log.info("{} - {}; registered new leader={}  old leader={}  time={}", state.serverName, methodName, state.leader.orElse("<none>"), leader.orElse("<none>"), transport.now());
          }
        }
      }

      // 2. Respond
      log.trace("{} - {} replying;  candidate={}  vote_granted={}  term={}  voted_for={}", state.serverName, methodName, voteRequest.getCandidateName(), voteGranted, state.currentTerm, state.votedFor.orElse("<nobody>"));
      reply = RaftTransport.mkRaftMessage(state.serverName, RequestVoteReply.newBuilder()
          .setFollowerName(state.serverName)
          .setTerm(voteRequest.getTerm())
          .setFollowerTerm(state.currentTerm)
          .setVoteGranted(voteGranted)
          .build());
    } else {
      log.trace("{} - {} we're not allowed to perform this action; responding with a rejected vote", state.serverName, methodName);
      reply = RaftTransport.mkRaftMessage(state.serverName, RequestVoteReply.newBuilder()
          .setFollowerName(state.serverName)
          .setTerm(voteRequest.getTerm())
          .setFollowerTerm(state.currentTerm)
          .setVoteGranted(false)
          .build());
    }

    replyLeader.accept(reply);
  }


  /** {@inheritDoc} */
  @Override
  public void receiveRequestVotesReply(RequestVoteReply reply) {
    assert drivingThreadId < 0 || drivingThreadId == Thread.currentThread().getId() : "Eloquent Raft Algorithm should only be run from the driving thread (" + drivingThreadId + ") but is being driven by " + Thread.currentThread();
    String methodName = "RequestVoteReply";
    log.trace("{} - [{}] {} from {};  term={}  follower_term={}  vote_granted={}",
        state.serverName, transport.now(), methodName, reply.getFollowerName(), reply.getTerm(), reply.getFollowerTerm(), reply.getVoteGranted());
    if (canPerformFollowerAction(methodName, reply.getTerm(), true)) {
      // 1. Grant the vote
      if (reply.getVoteGranted()) {
        state.receiveVoteFrom(reply.getFollowerName());
        log.trace("{} - {} received vote; have {} votes total ({})", state.serverName, methodName, state.votesReceived.size(), state.votesReceived);
        // 2. Check if we won the election
        if (state.votesReceived.size() > state.log.getQuorumMembers().size() / 2) {
          // 3. If we won the election, elect ourselves
          if (state.leadership != RaftState.LeadershipStatus.LEADER) {
            state.elect(transport.now());
            log.info("{} - Raft won an election for term {} (time={}  members={})", state.serverName, this.state.currentTerm, transport.now(), state.log.getQuorumMembers());
            broadcastAppendEntries(false, true);  // immediately broadcast our leadership status
          } else {
            log.trace("{} - {} already elected to term {}", state.serverName, methodName, this.state.currentTerm);

          }
        }
      }
    }
  }


  /** {@inheritDoc} */
  @Override
  public void triggerElection() {
    assert drivingThreadId < 0 || drivingThreadId == Thread.currentThread().getId() : "Eloquent Raft Algorithm should only be run from the driving thread (" + drivingThreadId + ") but is being driven by " + Thread.currentThread();
    String methodName = "triggerElection";
    // The request we're sending. This does not need to be sent in a lock
    RequestVoteRequest voteRequest;

    // 1. Become a candidate, if not already one
    if (!this.state.isCandidate()) {
      state.becomeCandidate();
    }
    assert this.state.isCandidate() : "Should not be requesting votes if we're not a candidate";
    long originalTerm = state.currentTerm;

    // 2. Initiate Election
    // 2.1. Increment currentTerm
    state.setCurrentTerm(state.currentTerm + 1);
    log.info("{} - Raft triggered an election with term {} -> {} (time={} chkpt={} cluster={} leader={})", state.serverName, originalTerm, state.currentTerm, transport.now(), state.electionTimeoutCheckpoint, state.log.getQuorumMembers(), state.leader.orElse("<unknown>"));
    // 2.2. Vote for self
    state.voteFor(state.serverName, transport.now());  // resets election timer
    // 2.3. Reset election timer
    Optional<String> leader = state.leader;
    state.resetElectionTimeout(transport.now(), state.serverName);  // propose ourselves as the leader
    if (!leader.equals(state.leader)) {
      log.info("{} - {}; registered new leader={}  old leader={}  time={}", state.serverName, methodName, state.leader.orElse("<none>"), leader.orElse("<none>"), transport.now());
    }
    voteRequest = RequestVoteRequest.newBuilder()
        .setTerm(state.currentTerm)
        .setCandidateName(state.serverName)
        .setLastLogIndex(state.log.getLastEntryIndex())
        .setLastLogTerm(state.log.getLastEntryTerm())
        .build();

    // 3. Check if we won the election by default (i.e., we're the only node)
    if (state.votesReceived.size() > state.log.getQuorumMembers().size() / 2) {
      if (state.leadership != RaftState.LeadershipStatus.LEADER) {
        state.elect(transport.now());
        log.info("{} - Raft won an election (by default) for term {} (time={}; cluster={})", state.serverName, this.state.currentTerm, transport.now(), state.log.latestQuorumMembers);
      } else {
        log.trace("{} - already elected to term {}", state.serverName, this.state.currentTerm);

      }
    }

    // 4. Send the broadcast
    // 4.1. (log the broadcast while in a lock)
    log.trace("{} - Broadcasting requestVote", state.serverName);
    // 4.2. (send the broadcast)
    transport.broadcastTransport(this.state.serverName, voteRequest);
  }


  //
  // --------------------------------------------------------------------------
  // ADD SERVER
  // --------------------------------------------------------------------------
  //


  /** {@inheritDoc} */
  @Override
  public CompletableFuture<RaftMessage> receiveAddServerRPC(
      AddServerRequest addServerRequest) {
    assert drivingThreadId < 0 || drivingThreadId == Thread.currentThread().getId() : "Eloquent Raft Algorithm should only be run from the driving thread (" + drivingThreadId + ") but is being driven by " + Thread.currentThread();
    long beginAdd = transport.now();
    String methodName = "AddServerRPC";
    log.info("{} - [{}] {};  is_leader={}  new_server={}", state.serverName, transport.now(), methodName, state.isLeader(), addServerRequest.getNewServer());

    if (state.isLeader()) {  // Step 1: Make sure we're the leader
      // Case: We're on the leader -- add the server
      clusterMembershipFuture = clusterMembershipFuture.thenApply(x -> {
        assert drivingThreadId < 0 || drivingThreadId == Thread.currentThread().getId() : "Eloquent Raft Algorithm should only be run from the driving thread (" + drivingThreadId + ") but is being driven by " + Thread.currentThread();
        return RaftMessage.newBuilder().setSender(state.serverName).build();
      });

      // Step 2: Catch up the new server for a fixed number of rounds.
      state.observeLifeFrom(addServerRequest.getNewServer(), transport.now());  // important so that we initialize matchIndex and nextIndex
      long addServerMatchIndex = state.matchIndex.orElse(new HashMap<>()).getOrDefault(addServerRequest.getNewServer(), 0L);
      if (addServerMatchIndex < state.log.getLastEntryIndex()) {  // if we don't have a log or we already have a full match (this was a shadow node before), we can skip this step
        for (int roundI = 0; roundI < 3; ++roundI) {
          final long roundNum = roundI + 1;
          long timeout = Math.max(5000, Math.min(1000, this.electionTimeoutMillisRange().begin * (3 - roundI) / 2));

          long currentMatchIndex = state.matchIndex.orElse(new HashMap<>()).getOrDefault(addServerRequest.getNewServer(), 0L);
          if (currentMatchIndex == state.log.getLastEntryIndex()) {
            break;
          }

          clusterMembershipFuture = clusterMembershipFuture.thenCompose(message -> {
            assert drivingThreadId < 0 || drivingThreadId == Thread.currentThread().getId() : "Eloquent Raft Algorithm should only be run from the driving thread (" + drivingThreadId + ") but is being driven by " + Thread.currentThread();
            log.trace("{} - {}.2;  round={}  is_leader={}", state.serverName, methodName, roundNum, state.isLeader());
            // Check for errors
            if (message.getAddServerReply() != AddServerReply.getDefaultInstance()) {
              return CompletableFuture.completedFuture(message);
            }
            if (!state.isLeader()) {
              return CompletableFuture.completedFuture(RaftMessage.newBuilder().setSender(state.serverName).setAddServerReply(AddServerReply.newBuilder()
                  .setStatus(MembershipChangeStatus.NOT_LEADER)).build());
            }
            // Send the next catch-up request
            try {
              log.trace("{} - {}.2 sending_append_entries  round={}  term={}  nextIndex={}", state.serverName, methodName, roundNum, addServerRequest.getNewServer(), message.getAppendEntriesReply().getTerm(), message.getAppendEntriesReply().getNextIndex());
              return rpcAppendEntries(addServerRequest.getNewServer(), currentMatchIndex+1, timeout);
            } catch (Exception t) {
              t.printStackTrace();
              log.warn("{} - {}.2 Could not execute Raft update for server {}; is the server up? Exception: <{}>", state.serverName, methodName, addServerRequest.getNewServer(), t.getClass() + ": " + t.getMessage());
              return CompletableFuture.completedFuture(RaftMessage.newBuilder().setSender(state.serverName).setAddServerReply(AddServerReply.newBuilder()
                  .setStatus(MembershipChangeStatus.OTHER_ERROR)).build());
            }
          });
        }
      } else {
        log.trace("{} - {}.2;  no update needed, so skipping updating the new server", state.serverName, methodName);
      }

      // Step 3: Wait until the previous configuration in log is committed
      clusterMembershipFuture = clusterMembershipFuture.thenCompose(message -> {
        assert drivingThreadId < 0 || drivingThreadId == Thread.currentThread().getId() : "Eloquent Raft Algorithm should only be run from the driving thread (" + drivingThreadId + ") but is being driven by " + Thread.currentThread();
        log.trace("{} - {}.3;  is_leader={}", state.serverName, methodName, state.isLeader());
        // 3.1. Propagate errors through
        if (message.getAddServerReply() != AddServerReply.getDefaultInstance()) {
          return CompletableFuture.completedFuture(message);
        }
        if (!state.isLeader()) {
          return CompletableFuture.completedFuture(RaftMessage.newBuilder().setSender(state.serverName).setAddServerReply(AddServerReply.newBuilder()
              .setStatus(MembershipChangeStatus.NOT_LEADER)).build());
        }
        // 3.2. Get last configuration
        Optional<RaftLogEntryLocation> location = state.log.lastConfigurationEntryLocation();
        if (location.isPresent()) {
          // 3.3A. Case: there was a configuration: wait for it to commit
          log.trace("{} - {}.3 waiting for commit", state.serverName, methodName);
          broadcastAppendEntries(false, true);
          return state.log.createCommitFuture(location.get().index, location.get().term, true).thenApply(success -> {
            assert drivingThreadId < 0 || drivingThreadId == Thread.currentThread().getId() : "Eloquent Raft Algorithm should only be run from the driving thread (" + drivingThreadId + ") but is being driven by " + Thread.currentThread();
            log.trace("{} - {}.3 committed;  success={}", state.serverName, methodName, success);
            if (success) {
              broadcastAppendEntries(false, true);
              return RaftMessage.newBuilder().setSender(state.serverName).setAddServerReply(AddServerReply.newBuilder()
                  .setStatus(MembershipChangeStatus.OK)).build();
            } else {
              return RaftMessage.newBuilder().setSender(state.serverName).setAddServerReply(AddServerReply.newBuilder()
                  .setStatus(MembershipChangeStatus.OTHER_ERROR)).build();
            }
          });
        } else {
          // 3.3B. Case: there was no configuration: it's axiomatically committed
          return CompletableFuture.completedFuture(message);
        }
      });

      // Step 4: Append new entry and wait for commit
      clusterMembershipFuture = clusterMembershipFuture.thenCompose(message -> {
        assert drivingThreadId < 0 || drivingThreadId == Thread.currentThread().getId() : "Eloquent Raft Algorithm should only be run from the driving thread (" + drivingThreadId + ") but is being driven by " + Thread.currentThread();
        RaftLogEntryLocation location;
        log.trace("{} - {}.4;  is_leader={}", state.serverName, methodName, state.isLeader());
        // 4.1. Propagate errors through
        if (message.getAddServerReply().getStatus() != MembershipChangeStatus.OK) {
          return CompletableFuture.completedFuture(message);
        }
        if (!state.isLeader()) {
          return CompletableFuture.completedFuture(RaftMessage.newBuilder().setSender(state.serverName).setAddServerReply(AddServerReply.newBuilder()
              .setStatus(MembershipChangeStatus.NOT_LEADER)).build());
        }
        // 4.2. Time out if we've been waiting too long
        //      This is important to prevent the case where something has gone suboptimally in the RPC
        //      (e.g., we didn't have majority for a long time, the server took too long to update, etc.)
        //      and we may not want to blindly add a now-bad server to the configuration.
        if (transport.now() - beginAdd > MACHINE_DOWN_TIMEOUT) {
          return CompletableFuture.completedFuture(RaftMessage.newBuilder().setSender(state.serverName).setAddServerReply(AddServerReply.newBuilder()
              .setStatus(MembershipChangeStatus.TIMEOUT)).build());
        }
        // 4.3. Submit configuration
        Set<String> newMembership = new HashSet<>(state.log.getQuorumMembers());
        boolean force = !newMembership.add(addServerRequest.getNewServer());
        if (!force && !addServerRequest.getQuorumList().isEmpty() && !newMembership.equals(new HashSet<>(addServerRequest.getQuorumList()))) {
          log.warn("{} - {} consistency error (forcing consistency);  adding={}  newMembership={}  presumedMembership={}", state.serverName, methodName, addServerRequest.getNewServer(), newMembership, addServerRequest.getQuorumList());
          newMembership = new HashSet<>(addServerRequest.getQuorumList());
          force = true;
        }
        location = state.reconfigure(newMembership, force, transport.now());
        log.info("{} - {} submitted new configuration to log: {}", state.serverName, methodName, newMembership);
        // This is the point of no return -- the new configuration has been committed to the log
        // 4.4. Wait for the configuration to commit
        log.trace("{} - {}.4 waiting for commit", state.serverName, methodName);
        // 4.4.1. Broadcast a heartbeat to speed things along
        broadcastAppendEntries(false, true);
        // 4.4.2. Set up the commit future
        CompletableFuture<Boolean> commitFuture = state.log.createCommitFuture(location.index, location.term, true);
        // 4.4.3. Actually wait on the commit
        return commitFuture.thenApply(success -> {
          assert drivingThreadId < 0 || drivingThreadId == Thread.currentThread().getId() : "Eloquent Raft Algorithm should only be run from the driving thread (" + drivingThreadId + ") but is being driven by " + Thread.currentThread();
          log.trace("{} - {}.4 committed;  success={}", state.serverName, methodName, success);
          if (success) {
            log.info("{} - {} committed new configuration to log: {}", state.serverName, methodName, state.log.committedQuorumMembers);
            broadcastAppendEntries(false, true);
            return RaftMessage.newBuilder().setSender(state.serverName).setAddServerReply(AddServerReply.newBuilder()
                .setStatus(MembershipChangeStatus.OK)).build();
          } else {
            return RaftMessage.newBuilder().setSender(state.serverName).setAddServerReply(AddServerReply.newBuilder()
                .setStatus(MembershipChangeStatus.OTHER_ERROR)).build();
          }
        });
      });

      // Step 5: Reply
      CompletableFuture<RaftMessage> rtn = new CompletableFuture<>();
      clusterMembershipFuture.handle((result, ex) -> {
        log.trace("{} - {}.5 complete;  result={}  have_exception={}", state.serverName, methodName, result == null ? "<exception>" : result.getAddServerReply().getStatus(), ex != null);
        if (ex != null) {
          log.warn("Exception on addServer", ex);
        }
        lastClusterMembershipChange = transport.now();
        if (result != null) {
          rtn.complete(result);
        } else {
          rtn.complete(RaftMessage.newBuilder().setSender(state.serverName).setAddServerReply(AddServerReply.newBuilder()
              .setStatus(MembershipChangeStatus.TIMEOUT)).build());
        }
        return result;
      });
      return rtn;

    } else {
      if (state.leader.map(leader -> !leader.equals(this.state.serverName)).orElse(false)) {
        // Case: We're not the leader -- forward it to the leader
        if (addServerRequest.getForwardedByList().contains(state.serverName)) {
          // Case: We're being forwarded this message in a loop (we've forwarded this message in the past), so fail it
          log.info("{} - {}; we've been forwarded our own message back to us in a loop. Failing the message", state.serverName, methodName);
          return CompletableFuture.completedFuture(RaftMessage.newBuilder().setSender(state.serverName).setAddServerReply(AddServerReply
              .newBuilder()
              .setStatus(MembershipChangeStatus.NOT_LEADER)).build());
        } else {
          // Case: We're not the leader -- forward it to the leader
          log.trace("{} - got {}; forwarding request to {}", state.serverName, methodName, this.state.leader.orElse("<unknown>"));
          return transport.rpcTransportAsFuture(state.serverName, state.leader.orElse("<unknown>"), addServerRequest.toBuilder().addForwardedBy(state.serverName).build(), (result, exception) -> {
            assert drivingThreadId < 0 || drivingThreadId == Thread.currentThread().getId() : "Eloquent Raft Algorithm should only be run from the driving thread (" + drivingThreadId + ") but is being driven by " + Thread.currentThread();
            if (result != null) {
              return result;
            } else {
              log.warn("{} - {} Failure: <{}>", state.serverName, methodName, exception == null ? "unknown error" : (exception.getClass().getName() + ": " + exception.getMessage()));
              return RaftMessage.newBuilder().setSender(state.serverName).setAddServerReply(AddServerReply.newBuilder()
                  .setStatus(MembershipChangeStatus.NOT_LEADER)).build();
            }
          }, this.drivingThreadQueue, this.electionTimeoutMillisRange().end);
        }
      } else {
        // Case: We're not the leader, and we don't know who is.
        //       We have no choice but to fail the request.
        log.info("{} - got {}; we're not the leader and don't know who the leader is", state.serverName, methodName);
        return CompletableFuture.completedFuture(RaftMessage.newBuilder().setSender(state.serverName).setAddServerReply(AddServerReply
            .newBuilder()
            .setStatus(MembershipChangeStatus.NOT_LEADER)).build());
      }
    }
  }


  //
  // --------------------------------------------------------------------------
  // REMOVE SERVER
  // --------------------------------------------------------------------------
  //


  /** {@inheritDoc} */
  @Override
  public CompletableFuture<RaftMessage> receiveRemoveServerRPC(
      RemoveServerRequest removeServerRequest) {
    assert drivingThreadId < 0 || drivingThreadId == Thread.currentThread().getId() : "Eloquent Raft Algorithm should only be run from the driving thread (" + drivingThreadId + ") but is being driven by " + Thread.currentThread();
    long beginRemove = transport.now();
    String methodName = "RemoveServerRPC";
    log.info("{} - [{}] {};  is_leader={}  to_remove={}", state.serverName, transport.now(), methodName, state.isLeader(), removeServerRequest.getOldServer());

    if (state.isLeader()) {  // Step 1: Make sure we're the leader
      // Case: We're on the leader -- remove the server
      clusterMembershipFuture = clusterMembershipFuture.thenApply(x -> {
        assert drivingThreadId < 0 || drivingThreadId == Thread.currentThread().getId() : "Eloquent Raft Algorithm should only be run from the driving thread (" + drivingThreadId + ") but is being driven by " + Thread.currentThread();
        return RaftMessage.newBuilder().setSender(state.serverName).build();
      });

      // Step 2: Wait until the previous configuration in log is committed
      clusterMembershipFuture = clusterMembershipFuture.thenCompose(message -> {
        assert drivingThreadId < 0 || drivingThreadId == Thread.currentThread().getId() : "Eloquent Raft Algorithm should only be run from the driving thread (" + drivingThreadId + ") but is being driven by " + Thread.currentThread();
        log.trace("{} - {}.2;  is_leader={}", state.serverName, methodName, state.isLeader());
        // 2.1. Propagate errors through
        if (message.getAddServerReply().getStatus() != MembershipChangeStatus.OK) {
          return CompletableFuture.completedFuture(message);
        }
        if (!state.isLeader()) {
          return CompletableFuture.completedFuture(RaftMessage.newBuilder().setSender(state.serverName).setRemoveServerReply(RemoveServerReply.newBuilder()
              .setStatus(MembershipChangeStatus.NOT_LEADER)).build());
        }
        // 2.2. Get last configuration
        Optional<RaftLogEntryLocation> location = state.log.lastConfigurationEntryLocation();
        if (location.isPresent()) {
          // 2.3A. Case: there was a configuration: wait for it to commit
          log.trace("{} - {}.2 waiting for commit", state.serverName, methodName);
          broadcastAppendEntries(false, true);
          return state.log.createCommitFuture(location.get().index, location.get().term, true).thenApply(success -> {
            assert drivingThreadId < 0 || drivingThreadId == Thread.currentThread().getId() : "Eloquent Raft Algorithm should only be run from the driving thread (" + drivingThreadId + ") but is being driven by " + Thread.currentThread();
            log.trace("{} - {}.2 committed;  success={}", state.serverName, methodName, success);
            if (success) {
              broadcastAppendEntries(false, true);
              return RaftMessage.newBuilder().setSender(state.serverName).setRemoveServerReply(RemoveServerReply.newBuilder()
                  .setStatus(MembershipChangeStatus.OK)).build();
            } else {
              return RaftMessage.newBuilder().setSender(state.serverName).setRemoveServerReply(RemoveServerReply.newBuilder()
                  .setStatus(MembershipChangeStatus.OTHER_ERROR)).build();
            }
          });
        } else {
          // 2.3B. Case: there was no configuration: it's axiomatically committed
          return CompletableFuture.completedFuture(message);
        }
      });

      // Step 3: Append new entry and wait for commit
      clusterMembershipFuture = clusterMembershipFuture.thenCompose(message -> {
        assert drivingThreadId < 0 || drivingThreadId == Thread.currentThread().getId() : "Eloquent Raft Algorithm should only be run from the driving thread (" + drivingThreadId + ") but is being driven by " + Thread.currentThread();
        RaftLogEntryLocation location;
        log.trace("{} - {}.3;  is_leader={}", state.serverName, methodName, state.isLeader());
        // 3.1. Propagate errors through
        if (message.getAddServerReply().getStatus() != MembershipChangeStatus.OK) {
          return CompletableFuture.completedFuture(message);
        }
        if (!state.isLeader()) {
          return CompletableFuture.completedFuture(RaftMessage.newBuilder().setSender(state.serverName).setRemoveServerReply(RemoveServerReply.newBuilder()
              .setStatus(MembershipChangeStatus.NOT_LEADER)).build());
        }
        // 3.2. Time out if we've been waiting too long
        //      This is important to prevent the case where something has gone suboptimally in the RPC
        //      (e.g., we didn't have majority for a long time, the server took too long to update, etc.)
        //      and we may not want to blindly add a now-bad server to the configuration.
        if (transport.now() - beginRemove > MACHINE_DOWN_TIMEOUT) {
          return CompletableFuture.completedFuture(RaftMessage.newBuilder().setSender(state.serverName).setRemoveServerReply(RemoveServerReply.newBuilder()
              .setStatus(MembershipChangeStatus.TIMEOUT)).build());
        }
        // 3.3. Submit configuration
        Set<String> newMembership = new HashSet<>(state.log.getQuorumMembers());
        boolean force = !newMembership.remove(removeServerRequest.getOldServer());
        if (!force && !removeServerRequest.getQuorumList().isEmpty() && !newMembership.equals(new HashSet<>(removeServerRequest.getQuorumList()))) {
          log.warn("{} - {} consistency error (forcing consistency);  adding={}  newMembership={}  presumedMembership={}", state.serverName, methodName, removeServerRequest.getOldServer(), newMembership, removeServerRequest.getQuorumList());
          newMembership = new HashSet<>(removeServerRequest.getQuorumList());
          force = true;
        }
        location = state.reconfigure(newMembership, force, transport.now());
        log.info("{} - {} submitted new configuration to log: {}", state.serverName, methodName, newMembership);
        // This is the point of no return -- the membership has been submitted
        // 3.4. Wait for the configuration to commit
        log.trace("{} - {}.3 waiting for commit", state.serverName, methodName);
        // 3.4.1. Broadcast a heartbeat to speed things along
        broadcastAppendEntries(false, true);
        // 4.4.2. Set up the commit future
        CompletableFuture<Boolean> commitFuture = state.log.createCommitFuture(location.index, location.term, true);
        // 4.4.3. Actually wait on the commit future
        return commitFuture.thenApply(success -> {
          assert drivingThreadId < 0 || drivingThreadId == Thread.currentThread().getId() : "Eloquent Raft Algorithm should only be run from the driving thread (" + drivingThreadId + ") but is being driven by " + Thread.currentThread();
          log.trace("{} - {}.3 committed;  success={}", state.serverName, methodName, success);
          if (success) {
            log.info("{} - {} committed new configuration to log: {}", state.serverName, methodName, state.log.committedQuorumMembers);
            // (clear the removed node's lastMessageTimestamp)
            state.lastMessageTimestamp.ifPresent(map -> map.remove(removeServerRequest.getOldServer()));
            // (broadcast the commit)
            broadcastAppendEntries(true, true);  // corner case: force the append to be broadcast
            // (return)
            return RaftMessage.newBuilder().setSender(state.serverName).setRemoveServerReply(RemoveServerReply.newBuilder()
                .setStatus(MembershipChangeStatus.OK)).build();
          } else {
            return RaftMessage.newBuilder().setSender(state.serverName).setRemoveServerReply(RemoveServerReply.newBuilder()
                .setStatus(MembershipChangeStatus.OTHER_ERROR)).build();
          }
        });
      });

      // Step 4: Reply
      CompletableFuture<RaftMessage> rtn = new CompletableFuture<>();
      clusterMembershipFuture.handle((result, ex) -> {
        log.trace("{} - {}.5 complete;  result={}  have_exception={}", state.serverName, methodName, result == null ? "<exception>" : result.getAddServerReply().getStatus(), ex != null);
        if (ex != null) {
          log.warn("Exception on removeServer", ex);
        }
        lastClusterMembershipChange = transport.now();
        if (result != null) {
          // If we've removed ourselves, step down from leadership.
          if (removeServerRequest.getOldServer().equals(state.serverName) && result.getRemoveServerReply().getStatus() == MembershipChangeStatus.OK) {
            state.stepDownFromElection();
          }
          // Then, return
          rtn.complete(result);
        } else {
          rtn.complete(RaftMessage.newBuilder().setSender(state.serverName).setRemoveServerReply(RemoveServerReply.newBuilder()
              .setStatus(MembershipChangeStatus.TIMEOUT)).build());
        }
        return result;
      });
      return rtn;

    } else {
      if (state.leader.map(leader -> !leader.equals(this.state.serverName)).orElse(false)) {
        // Case: We're not the leader -- forward it to the leader
        if (removeServerRequest.getForwardedByList().contains(state.serverName)) {
          // Case: We're being forwarded this message in a loop (we've forwarded this message in the past), so fail it
          log.info("{} - {}; we've been forwarded our own message back to us in a loop. Failing the message", state.serverName, methodName);
          return CompletableFuture.completedFuture(RaftMessage.newBuilder().setSender(state.serverName).setRemoveServerReply(RemoveServerReply
              .newBuilder()
              .setStatus(MembershipChangeStatus.NOT_LEADER)).build());
        } else {
          log.trace("{} - got {}; forwarding request to {}", state.serverName, methodName, this.state.leader.orElse("<unknown>"));
          return transport.rpcTransportAsFuture(state.serverName, state.leader.orElse("<unknown>"), removeServerRequest.toBuilder().addForwardedBy(state.serverName).build(), (result, exception) -> {
            assert drivingThreadId < 0 || drivingThreadId == Thread.currentThread().getId() : "Eloquent Raft Algorithm should only be run from the driving thread (" + drivingThreadId + ") but is being driven by " + Thread.currentThread();
            if (result != null) {
              return result;
            } else {
              log.warn("{} - {} Failure: <{}>", state.serverName, methodName, exception == null ? "unknown error" : (exception.getClass().getName() + ": " + exception.getMessage()));
              return RaftMessage.newBuilder().setSender(state.serverName).setRemoveServerReply(RemoveServerReply.newBuilder()
                  .setStatus(MembershipChangeStatus.NOT_LEADER)).build();
            }
          }, this.drivingThreadQueue, this.electionTimeoutMillisRange().end + 100);
        }
      } else {
        // Case: We're not the leader, and we don't know who is.
        //       We have no choice but to fail the request.
        log.info("{} - got {}; we're not the leader and don't know who the leader is", state.serverName, methodName);
        return CompletableFuture.completedFuture(RaftMessage.newBuilder().setSender(state.serverName).setRemoveServerReply(RemoveServerReply
            .newBuilder()
            .setStatus(MembershipChangeStatus.NOT_LEADER)).build());
      }
    }
  }


  //
  // --------------------------------------------------------------------------
  // CONTROL
  // --------------------------------------------------------------------------
  //


  /** {@inheritDoc} */
  @Override
  public CompletableFuture<RaftMessage> receiveApplyTransitionRPC(ApplyTransitionRequest transition) {
    assert drivingThreadId < 0 || drivingThreadId == Thread.currentThread().getId() : "Eloquent Raft Algorithm should only be run from the driving thread (" + drivingThreadId + ") but is being driven by " + Thread.currentThread();
    String methodName = "ReceiveApplyTransition";
    log.trace("{} - [{}] {}; is_leader={}", state.serverName, transport.now(), methodName, this.state.isLeader());
    CompletableFuture<RaftMessage> rtn;

    if (state.isLeader()) {

      // Case: We're on the leader -- apply the transition
      RaftLogEntryLocation location = state.transition(
          transition.getTransition().isEmpty() ? Optional.empty() : Optional.of(transition.getTransition().toByteArray()),
          "".equals(transition.getNewHospiceMember()) ? Optional.empty() : Optional.of(transition.getNewHospiceMember())
      );
      rtn = state.log.createCommitFuture(location.index, location.term, true).handle((success, exception) -> {
        // The transition was either committed or overwritten
        assert drivingThreadId < 0 || drivingThreadId == Thread.currentThread().getId() : "Eloquent Raft Algorithm should only be run from the driving thread (" + drivingThreadId + ") but is being driven by " + Thread.currentThread();
        ApplyTransitionReply.Builder reply = ApplyTransitionReply
            .newBuilder()
            .setTerm(state.currentTerm)
            .setNewEntryIndex(-1)  // just so it's not picked up as the default proto. overwritten below
            .setSuccess(success != null ? success : false);
        if (success != null && success) {
          reply
              .setNewEntryIndex(location.index)
              .setNewEntryTerm(location.term);
        } else {
          log.info("{} - {}; failed transition (on leader; could not commit) @ time={}", state.serverName, methodName, transport.now(), exception);
        }
        // Reply
        return RaftMessage.newBuilder().setSender(state.serverName).setApplyTransitionReply(reply).build();
      });
      // Early broadcast message
      broadcastAppendEntries(this.state.log.latestQuorumMembers.size() <= 1, false);
      // Update our leader invariants
      this.updateLeaderInvariantsPostMessage(Optional.empty());

    } else if (state.leader.map(leader -> !leader.equals(this.state.serverName)).orElse(false)) {
      if (transition.getForwardedByList().contains(state.serverName)) {
        // Case: We're being forwarded this message in a loop (we've forwarded this message in the past), so fail it
        log.info("{} - {}; we've been forwarded our own message back to us in a loop. Failing the message", state.serverName, methodName);
        rtn = CompletableFuture.completedFuture(RaftMessage.newBuilder().setSender(state.serverName).setApplyTransitionReply(ApplyTransitionReply
            .newBuilder()
            .setTerm(state.currentTerm)
            .setNewEntryIndex(-2)
            .setSuccess(false)).build());
      } else {
        // Case: We're not the leader -- forward it to the leader
        assert !state.leader.map(x -> x.equals(this.serverName())).orElse(false) : "We are sending a message to the leader, but we think we're the leader!";
        log.trace("{} - [{}] {}; forwarding request to {}", state.serverName, transport.now(), methodName, this.state.leader.orElse("<unknown>"));
        // Forward the request
        rtn = transport.rpcTransportAsFuture(
            state.serverName,
            state.leader.orElse("<unknown>"),
            transition.toBuilder().addForwardedBy(state.serverName).build(),
            (result, exception) -> {
              assert drivingThreadId < 0 || drivingThreadId == Thread.currentThread().getId() : "Eloquent Raft Algorithm should only be run from the driving thread (" + drivingThreadId + ") but is being driven by " + Thread.currentThread();
              if (result != null) {
                log.trace("{} - {}; received reply to forwarded message", state.serverName, methodName);
                return result;
              } else {
                log.warn("{} - [{}] {} Failure: <{}>;  is_leader={}  leader={}",
                    state.serverName, transport.now(), methodName, exception == null ? "unknown error" : (exception.getClass().getName() + ": " + exception.getMessage()),
                    state.isLeader(), state.leader.orElse("<unknown>"));
                return RaftMessage.newBuilder().setSender(state.serverName).setApplyTransitionReply(ApplyTransitionReply
                    .newBuilder()
                    .setTerm(state.currentTerm)
                    .setNewEntryIndex(-3)
                    .setSuccess(false)).build();
              }
            }, this.drivingThreadQueue, this.electionTimeoutMillisRange().end)
            .thenCompose(leaderResponse -> {
              assert drivingThreadId < 0 || drivingThreadId == Thread.currentThread().getId() : "Eloquent Raft Algorithm should only be run from the driving thread (" + drivingThreadId + ") but is being driven by " + Thread.currentThread();
              // Then wait for it to commit locally
              if (leaderResponse.getApplyTransitionReply().getSuccess()) {
                log.trace("{} - {}; waiting for commit", state.serverName, methodName);
                long index = leaderResponse.getApplyTransitionReply().getNewEntryIndex();
                long term = leaderResponse.getApplyTransitionReply().getNewEntryTerm();
                return state.log.createCommitFuture(index, term, true).thenApply(success -> {
                  assert drivingThreadId < 0 || drivingThreadId == Thread.currentThread().getId() : "Eloquent Raft Algorithm should only be run from the driving thread (" + drivingThreadId + ") but is being driven by " + Thread.currentThread();
                  log.trace("{} - {}; Entry @ {} (term={}) is committed in the local log", state.serverName, methodName, index, term);
                  // The transition was either committed or overwritten
                  ApplyTransitionReply.Builder reply = ApplyTransitionReply
                      .newBuilder()
                      .setTerm(state.currentTerm)
                      .setNewEntryIndex(-4)  // just so this isn't the default proto. Overwritten below
                      .setSuccess(success);
                  if (success) {
                    reply
                        .setNewEntryIndex(index)
                        .setNewEntryTerm(term);
                  } else {
                    log.info("{} - {}; failed transition (on follower) @ time={}", state.serverName, methodName, transport.now());
                  }
                  return RaftMessage.newBuilder().setSender(state.serverName).setApplyTransitionReply(reply).build();
                });
              } else {
                log.info("{} - {}; failed to apply transition (reply proto had failure marked); returning failure", state.serverName, methodName);
                return CompletableFuture.completedFuture(leaderResponse);
              }
            });
      }
    } else {
      // Case: We're not the leader, and we don't know who is.
      //       We have no choice but to fail the request.
      log.info("{} - {}; we're not the leader and don't know who the leader is", state.serverName, methodName);
      rtn = CompletableFuture.completedFuture(RaftMessage.newBuilder().setSender(state.serverName).setApplyTransitionReply(ApplyTransitionReply
          .newBuilder()
          .setTerm(state.currentTerm)
          .setNewEntryIndex(-5)
          .setSuccess(false)).build());
    }

    // Return
    return rtn;
  }


  /** {@inheritDoc} */
  @Override
  public void heartbeat() {
    assert drivingThreadId < 0 || drivingThreadId == Thread.currentThread().getId() : "Eloquent Raft Algorithm should only be run from the driving thread (" + drivingThreadId + ") but is being driven by " + Thread.currentThread();
    long begin = transport.now();
    Object timerPrometheusBegin = Prometheus.startTimer(summaryTiming, "heartbeat");
    long sectionBegin;
    try {
      if (this.state.isLeader()) {
        assert checkDuration("leadership check", begin, transport.now());

        // Case: do a leader heartbeat
        sectionBegin = transport.now();
        log.trace("{} - heartbeat (leader) @ time={}  cluster={}", state.serverName, transport.now(), state.log.getQuorumMembers());
        assert checkDuration("get quorum members", begin, transport.now());
        broadcastAppendEntries(false, true);  // the heartbeat.
        if (Thread.interrupted()) {
          throw new RuntimeInterruptedException();
        }
        assert checkDuration("broadcast", sectionBegin, transport.now());

        // Check if we should scale the cluster
        if (state.targetClusterSize >= 0) {  // only do this if we don't have a fixed cluster
          sectionBegin = transport.now();
          if (clusterMembershipFuture.getNow(null) != null) {  // if we're not still waiting on membership changes
            Optional<String> maybeServer;
            if ((maybeServer = state.serverToRemove(transport.now(), MACHINE_DOWN_TIMEOUT)).isPresent()) {
              // Case: we want to scale down the cluster
              log.info("{} - Detected Raft cluster is too large ({} > {}) or we have a delinquent server; scaling down by removing {} (latency {})  current_time={}",
                  state.serverName, state.log.getQuorumMembers().size(), state.targetClusterSize, maybeServer.get(), transport.now() - maybeServer.flatMap(server -> state.lastMessageTimestamp.map(x -> x.get(server))).orElse(-1L), transport.now());
              this.receiveRPC(RaftTransport.mkRaftRPC(state.serverName,
                  EloquentRaftProto.RemoveServerRequest.newBuilder()
                      .setOldServer(maybeServer.get())
                      .build())
              );
              assert checkDuration("remove server", sectionBegin, transport.now());
            } else if ((maybeServer = state.serverToAdd(transport.now(), electionTimeoutMillisRange().begin)).isPresent()) {  // note: add timeout is different from remove timeout. Much more strict to add than to remove
              // Case: we want to scale up the cluster
              log.info("{} - Detected Raft cluster is too small ({} < {}); scaling up by adding {} (latency {})  current_time={}",
                  state.serverName, state.log.getQuorumMembers().size(), state.targetClusterSize, maybeServer.get(), transport.now() - maybeServer.flatMap(server -> state.lastMessageTimestamp.map(x -> x.get(server))).orElse(-1L), transport.now());
              this.receiveRPC(RaftTransport.mkRaftRPC(state.serverName,
                  EloquentRaftProto.AddServerRequest.newBuilder()
                      .setNewServer(maybeServer.get())
                      .build())
              );
              assert checkDuration("add server", sectionBegin, transport.now());
            }
          }
          if (Thread.interrupted()) {
            throw new RuntimeInterruptedException();
          }
          assert checkDuration("size checks", sectionBegin, transport.now());
        }

        // Check if anyone has gone offline
        sectionBegin = transport.now();
        if (state.log.stateMachine instanceof KeyValueStateMachine) {
          Set<String> toKillSet = state.killNodes(transport.now(), MACHINE_DOWN_TIMEOUT);
          CompletableFuture<RaftMessage> future = CompletableFuture.completedFuture(null);
          for (String deadNode : toKillSet) {
            future = future.thenCompose(lastSuccess -> {
              assert drivingThreadId < 0 || drivingThreadId == Thread.currentThread().getId() : "Eloquent Raft Algorithm should only be run from the driving thread (" + drivingThreadId + ") but is being driven by " + Thread.currentThread();
              log.info("{} - Clearing transient data for node {}", state.serverName, deadNode);
              return receiveApplyTransitionRPC(ApplyTransitionRequest.newBuilder()
                  .setTransition(ByteString.copyFrom(KeyValueStateMachine.createClearTransition(deadNode)))
                  .build())
                  .handle((response, exception) -> {
                    if (exception != null || response == null || !response.getApplyTransitionReply().getSuccess()) {
                      state.revive(deadNode);
                    }
                    return null;
                  });
            });
          }
        }
        assert checkDuration("offline check", sectionBegin, transport.now());

      } else {

        // Case: do a follower heartbeat
        log.trace("{} - heartbeat (follower) @ time={}  cluster={}", state.serverName, transport.now(), state.log.getQuorumMembers());
        if (state.shouldTriggerElection(transport.now(), electionTimeoutMillisRange())) {
          this.triggerElection();
        }
      }
    } finally {
      Prometheus.observeDuration(timerPrometheusBegin);
      assert checkDuration("total", begin, transport.now());
    }
  }


  /**
   * A helper method for checking if an operation took too long.
   *
   * @param description The description of the task that timed out.
   * @param begin The begin time of the operation
   * @param now The current time.
   *
   * @return True if the operation took too long
   */
  @SuppressWarnings("Duplicates")
  private boolean checkDuration(String description, long begin, long now) {
    assert drivingThreadId < 0 || drivingThreadId == Thread.currentThread().getId() : "Eloquent Raft Algorithm should only be run from the driving thread (" + drivingThreadId + ") but is being driven by " + Thread.currentThread();
    long timeElapsed = now - begin;
    if (timeElapsed > 50) {
      long lastGcTime = -1L;
      try {
        for (GarbageCollectorMXBean gcBean : ManagementFactory.getGarbageCollectorMXBeans()) {
          com.sun.management.GarbageCollectorMXBean sunGcBean = (com.sun.management.GarbageCollectorMXBean) gcBean;
          GcInfo lastGcInfo = sunGcBean.getLastGcInfo();
          if (lastGcInfo != null) {
            lastGcTime = lastGcInfo.getStartTime();
          }
        }
      } catch (Throwable t) {
        log.warn("Could not get GC info -- are you running on a non-Sun JVM?");
      }
      boolean interruptedByGC = false;
      if (lastGcTime > begin && lastGcTime < now) {
        interruptedByGC = true;
      }
      OperatingSystemMXBean osBean = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);
      log.warn("{} - Raft took >50ms ({}) on step '{}';  leadership={}  system_load={}  interrupted_by_gc={}",
          state.serverName, TimerUtils.formatTimeDifference(timeElapsed), description, state.leadership,
          osBean.getSystemLoadAverage(), interruptedByGC);
    }
    return true;  // always return true, or else we throw an assert
  }


  /** {@inheritDoc} */
  @Override
  public boolean bootstrap(boolean force) {
    assert drivingThreadId < 0 || drivingThreadId == Thread.currentThread().getId() : "Eloquent Raft Algorithm should only be run from the driving thread (" + drivingThreadId + ") but is being driven by " + Thread.currentThread();
    if (force || (this.state.targetClusterSize >= 0 && this.state.log.getQuorumMembers().isEmpty())) {  // only if we need bootstrapping
      state.bootstrap(force);
      return true;
    } else {
      return false;
    }
  }


  /** {@inheritDoc} */
  @Override
  public void stop(boolean kill) {
  }


  /** {@inheritDoc} */
  @Override
  public boolean isRunning() {
    return true;
  }


  /** {@inheritDoc} */
  @Override
  public void receiveBadRequest(RaftMessage message) {
    assert drivingThreadId < 0 || drivingThreadId == Thread.currentThread().getId() : "Eloquent Raft Algorithm should only be run from the driving thread (" + drivingThreadId + ") but is being driven by " + Thread.currentThread();
    if (message.getApplyTransitionReply() != ApplyTransitionReply.getDefaultInstance()) {
      log.warn("{} - Got an apply transition reply -- likely the dangling reply of a timeout somewhere", state.serverName);
    } else {
      log.warn("{} - Bad Raft message {}", state.serverName, message.toString());
    }
  }

  @Override
  public Optional<RaftLifecycle> lifecycle() {
    // Don't check for driving thread -- this is final anyways
    return lifecycle;
  }

  @Override
  public RaftTransport getTransport() {
    return transport;
  }

  /**
   * Print any errors we can find in the Raft node.
   */
  public List<String> errors() {
    assert drivingThreadId < 0 || drivingThreadId == Thread.currentThread().getId() : "Eloquent Raft Algorithm should only be run from the driving thread (" + drivingThreadId + ") but is being driven by " + Thread.currentThread();
    List<String> errors = new ArrayList<>();
    if (clusterMembershipFuture.getNow(null) == null) {
      errors.add("Membership change in progress (future is not empty). My state: " + state.leadership + "; leader=" + state.leader.orElse("<unknown>"));
    }
    if (state.lastMessageTimestamp.map(Map::size).orElse(0) > 1000) {
      errors.add("Keeping track of >1000 heartbeat (" + state.lastMessageTimestamp.map(Map::size).orElse(0) + ")");
    }
    if (state.electionTimeoutCheckpoint < 0) {
      errors.add("No election timeout present (perhaps means heartbeats are not propagating?): " + Instant.ofEpochMilli(state.electionTimeoutCheckpoint));
    }
    long timeSinceBroadcast = transport.now() - (Arrays.stream(lastBroadcastTimes).max().orElse(0));
    if (state.isLeader() && timeSinceBroadcast > this.heartbeatMillis() * 10) {
      errors.add("Last broadcast was " + TimerUtils.formatTimeDifference(timeSinceBroadcast) + " ago");
    }
    return errors;
  }


  //
  // --------------------------------------------------------------------------
  // INVARIANTS
  // --------------------------------------------------------------------------
  //


  /**
   * This is called on every RPC communication step to ensure that we can perform a follower
   * action.
   *
   * @param rpcName The name of the RPC, for debugging.
   * @param remoteTermNumber The term number from the server.
   * @param isRpcReply True if we're inside an RPC reply
   *
   * @return true if we observed a compatible term number
   */
  private boolean canPerformFollowerAction(String rpcName, long remoteTermNumber, boolean isRpcReply) {
    assert drivingThreadId < 0 || drivingThreadId == Thread.currentThread().getId() : "Eloquent Raft Algorithm should only be run from the driving thread (" + drivingThreadId + ") but is being driven by " + Thread.currentThread();
    if (remoteTermNumber < state.currentTerm && !isRpcReply) {
      // Someone is broadcasting with an older term -- ignore the message
      log.trace("{} - {} cannot perform action: remoteTerm={} < currentTerm={}", state.serverName, rpcName, remoteTermNumber, state.currentTerm);
      return false;
    } else if (remoteTermNumber > state.currentTerm) {
      // Someone is broadcasting with a newer term -- defer to them
      log.trace("{} - {} detected remoteTerm={} > currentTerm={} -- forced into follower state but otherwise continuing", state.serverName, rpcName, remoteTermNumber, state.currentTerm);
      state.stepDownFromElection();
      state.setCurrentTerm(remoteTermNumber);
      assert state.leadership == RaftState.LeadershipStatus.OTHER;
      return true;
    } else {
      // Terms match -- everything is OK
      return true;
    }
  }


  /**
   * A short helper wrapping {@link #canPerformLeaderAction(String, long)} and
   * {@link #updateLeaderInvariantsPostMessage(Optional)}.
   */
  private void asLeader(String followerName, long remoteTerm, Runnable fn) {
    assert drivingThreadId < 0 || drivingThreadId == Thread.currentThread().getId() : "Eloquent Raft Algorithm should only be run from the driving thread (" + drivingThreadId + ") but is being driven by " + Thread.currentThread();
    if (canPerformLeaderAction(followerName, remoteTerm)) {
      try {
        assert state.isLeader() : "We should be a leader if we get this far performing a leader action";
        fn.run();
      } finally {
        updateLeaderInvariantsPostMessage(Optional.of(followerName));
      }
    } else {
      log.trace("{} - Leader prereqs failed.", this.state.serverName);
    }
  }


  /**
   * Runs before a message is processed to make sure that we can perform a leader action.
   *
   * <p>
   *   Consider calling {@link #asLeader(String, long, Runnable)} rather than calling this
   *   function directly.
   * </p>
   *
   * @param followerName The follower we received the message from.
   * @param remoteTerm The term received from the follower
   *
   * @return True if the leader can continue with the request. Otherwise, we should
   *         handle the error somehow.
   */
  private boolean canPerformLeaderAction(String followerName, long remoteTerm) {
    assert drivingThreadId < 0 || drivingThreadId == Thread.currentThread().getId() : "Eloquent Raft Algorithm should only be run from the driving thread (" + drivingThreadId + ") but is being driven by " + Thread.currentThread();
    if (!state.isLeader()) {
      log.trace("{} - presumed leader is not a leader", state.serverName);
      return false;
    }
    if (followerName.isEmpty()) {
      log.warn("{} - Got a message with no follower name. This is an error!", state.serverName);
      return false;
    }
    if (remoteTerm < state.currentTerm) {
      // Someone is broadcasting with an older term -- ignore the message, but throw a warning because this is weird and
      // should be very rare
      log.warn("{} - leader cannot perform action. remoteTerm={} < currentTerm={}", state.serverName, remoteTerm, state.currentTerm);
      return false;
    } else if (remoteTerm > state.currentTerm) {
      // Someone is broadcasting with a newer term -- defer to them
      log.trace("{} - detected remoteTerm={} > currentTerm={} -- forced into follower state", state.serverName, remoteTerm, state.currentTerm);
      state.stepDownFromElection();
      state.setCurrentTerm(remoteTerm);
      assert state.leadership == RaftState.LeadershipStatus.OTHER;
      return false;
    } else {
      // Terms match -- everything is OK
      return true;
    }
  }


  /**
   * Run all of the invariants that should be true on the leader. This should be called after
   * every successful message is received by the leader.
   *
   * <p>
   *   Consider calling {@link #asLeader(String, long, Runnable)} rather than calling this
   *   function directly.
   * </p>
   *
   * @param followerName The follower we received the message from.
   */
  private void updateLeaderInvariantsPostMessage(Optional<String> followerName) {
    assert drivingThreadId < 0 || drivingThreadId == Thread.currentThread().getId() : "Eloquent Raft Algorithm should only be run from the driving thread (" + drivingThreadId + ") but is being driven by " + Thread.currentThread();
    if (state.isLeader()) {
      // 1. Observe that a follower is still alive
      followerName.ifPresent(s -> this.state.observeLifeFrom(s, transport.now()));

      // 2. If there exists an N such that N > commitIndex, a majority of matchIndex[i] >= N,
      //    and log[N].term == currentTerm, then set commitIndex = N.
      this.state.matchIndex.ifPresent(matchIndex -> {
        Set<String> clusterMembers = this.state.log.getQuorumMembers();
        if (!clusterMembers.isEmpty()) {
          // 2.1. Get the set of relevant match indices
          long[] matchIndices = clusterMembers.stream().mapToLong(member -> {
            if (this.state.serverName.equals(member)) {
              return this.state.log.getLastEntryIndex();
            } else {
              assert matchIndex.containsKey(member) : "no match index for member " + member;
              return matchIndex.get(member);
            }
          }).toArray();
          // 2.2. Find the median index
          Arrays.sort(matchIndices);
          long medianIndex;
          if (matchIndices.length % 2 == 0) {
            medianIndex = matchIndices[matchIndices.length / 2 - 1];  // round pessimistically (len 4 -> index 1) -- we need a majority, not just a tie
          } else {
            medianIndex = matchIndices[matchIndices.length / 2];      // no need to round (len 3 -> index 1)
          }
//        log.trace("{} - matchIndices={}  medianIndex={}  commitIndex={}", state.serverName, matchIndices, medianIndex, state.commitIndex());
          // 2.3. Commit up to the median index
          if (medianIndex > state.commitIndex() && state.log.getLastEntryTerm() == state.currentTerm) {
            log.trace("{} - Committing up to index {}; matchIndex={} -> {}", state.serverName, medianIndex, matchIndex, matchIndices);
            state.commitUpTo(medianIndex, transport.now());
            // 2.4. Broadcast the commit immediately
            if (followerName.isPresent()) {  // but only if this is from a particular follower's update
              broadcastAppendEntries(false, false);
            }
          }
        }
      });
    }
  }


  /** {@inheritDoc} */
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    EloquentRaftAlgorithm that = (EloquentRaftAlgorithm) o;
    return Objects.equals(state, that.state);
  }


  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return Objects.hash(state);
  }
}

