package ai.eloquent.raft;

import ai.eloquent.monitoring.Prometheus;
import ai.eloquent.raft.EloquentRaftProto.*;
import ai.eloquent.util.LeakyBucket;
import ai.eloquent.util.RuntimeInterruptedException;
import ai.eloquent.util.TimerUtils;
import ai.eloquent.util.Uninterruptably;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
   * An enumeration of the RPCs implemented for Raft.
   */
  private enum RPCType {
    APPEND_ENTRIES,
    APPEND_ENTRIES_REPLY,
    APPLY_TRANSITION,
    APPLY_TRANSITION_REPLY,
    INSTALL_SNAPSHOT,
    INSTALL_SNAPSHOT_REPLY,
    REQUEST_VOTES,
    REQUEST_VOTES_REPLY,
    ADD_SERVER,
    ADD_SERVER_REPLY,
    REMOVE_SERVER,
    REMOVE_SERVER_REPLY,
    TRIGGER_ELECTIOn,
    HEARTBEAT,
    ;
  }


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
  public static final long MACHINE_DOWN_TIMEOUT = Duration.ofMinutes(5).toMillis();  // 5 minutes


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
   * This is the lifecycle that this algorithm is associated with. This is only for mocks to use.
   */
  private final Optional<RaftLifecycle> lifecycle;

  /** The ID of the thread that's meant to be driving Raft, if we have one. */
  private long drivingThreadId = -1;  // Disabled initially

  /** A means of queuing tasks on the driving thread. */
  private Consumer<Runnable> drivingThreadQueue = Runnable::run;  // Disabled initially

  /** A leaky bucket rate limiter for sending out broadcast messages */
  private final LeakyBucket broadcastRateLimiter = new LeakyBucket(8, 5000000, TimeUnit.NANOSECONDS);


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
  // INVARIANTS
  // --------------------------------------------------------------------------
  //


  /**
   * Trigger a new election, and become a candidate.
   *
   * On conversion to candidate, start election:
   * <ul>
   *   <li>Increment currentTerm</li>
   *   <li>Vote for self</li>
   *   <li>Reset election timer</li>
   *   <li>Send RequestVote RPCs to all other servers</li>
   * </ul>
   */
  public void convertToCandidate() {
    // 1. Error checks
    assert this.state.leadership != RaftState.LeadershipStatus.LEADER : "Cannot become a candidate as a leader";
    assert this.state.log.getQuorumMembers().contains(this.state.serverName) : "Cannot become a candidate if we are not in the quorum";

    // 2. Step down from the election
    switch (this.state.leadership) {
      case LEADER:
        // case: we're no longer the leader, apparently
        this.state.stepDownFromElection(this.state.currentTerm, transport.now());
        // FALL THROUGH

      case OTHER:
        // case: trigger an election
        this.state.leadership = RaftState.LeadershipStatus.CANDIDATE;
        // 2.1. Increment currentTerm
        log.info("{} - Converting to candidate; incrementing term {} -> {}",
            this.state.serverName, this.state.currentTerm, this.state.currentTerm + 1);
        this.state.setCurrentTerm(this.state.currentTerm + 1);
        // 2.2. Vote for self
        assert state.votesReceived.isEmpty() : "Should not have any votes received when we start an election (i.e., in the new term)";
        assert !state.votedFor.isPresent() : "Should not have voted for anyone when we start an election (i.e., in the new term)";
        this.state.voteFor(this.state.serverName);
        // 2.3. Reset election timer
        this.state.electionTimeoutCheckpoint = transport.now();
        // 2.4. Send RequestVote RPCs to all other servers
        RequestVoteRequest voteRequest = RequestVoteRequest.newBuilder()
            .setTerm(state.currentTerm)
            .setCandidateName(state.serverName)
            .setLastLogIndex(state.log.getLastEntryIndex())
            .setLastLogTerm(state.log.getLastEntryTerm())
            .build();
        log.info("{}; Broadcasting requestVote", state.serverName);
        transport.broadcastTransport(this.state.serverName, voteRequest);
        break;

      case CANDIDATE:
        // we are already a candidate
        //noinspection UnnecessaryReturnStatement
        return;
    }
  }


  /**
   * Enforce the "All Server" rules in the Raft implementation.
   *
   * @param inboundTerm The term received from the inbound RPC. This is just called
   *                    'T' in the Raft thesis.
   * @param inboundCommitIndex The commit index from the inbound PRC. This is just called
   *                           'commitIndex' in the Raft thesis.
   */
  void enforceServerRules(long inboundTerm, Optional<Long> inboundCommitIndex) {
    /*
     * If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to the
     * state machine
     */
    long lastApplied = this.state.lastApplied();
    if (inboundCommitIndex.isPresent() && inboundCommitIndex.get() > lastApplied) {
      state.commitUpTo(lastApplied, transport.now());
    }

    /*
     * If RPC request or response contains term T > currentTerm:
     * set currentTerm = T, convert to follower
     */
    if (inboundTerm > state.currentTerm) {
      state.stepDownFromElection(inboundTerm, transport.now());
    }
  }


  /**
   * Enforce the follower rules from the Raft thesis.
   *
   * @param rpc The RPC we received that triggered this rule evaluation.
   * @param inboundTerm The term received from the inbound RPC. This is just called
   *                    'T' in the Raft thesis.
   * @param leader The server we think is the leader based off of the inbound RPC.
   */
  void enforceFollowerRules(RPCType rpc, long inboundTerm, Optional<String> leader) {
    assert state.leadership == RaftState.LeadershipStatus.OTHER : "We're enforcing follower rules but are not a follower";
    /*
     * Respond to RPCs from candidates and leaders.
     */
    // This is implicit -- we will respond to the RPC once we're done enforcing the rules

    /*
     * If election timeout elapses without receiving AppendEntries RPC
     * from current leader or granting vote to candidate:
     * convert to candidate.
     */
    if (this.state.shouldTriggerElection(transport.now(), this.electionTimeoutMillisRange())) {
      this.convertToCandidate();
      assert state.leadership == RaftState.LeadershipStatus.CANDIDATE : "We should be a candidate after triggering an election";
    }

    /*
     * Eloquent Addition: detect split brain.
     * If we're receiving heartbeats from two different leaders in the same term, trigger
     * a snap election.
     */
    if (rpc == RPCType.APPEND_ENTRIES && leader.isPresent() && state.leader.isPresent()) {
      //noinspection OptionalGetWithoutIsPresent
      if (inboundTerm == state.currentTerm && !leader.get().equals(this.state.leader.get())) {
        //noinspection OptionalGetWithoutIsPresent
        log.warn("{} - [{}] Detected split-brain in Raft! {} believes both {} and {} are leaders in the same term {}",
            state.serverName, transport.now(), state.serverName, state.leader.get(), leader.get(), state.currentTerm);
        this.convertToCandidate();
        assert state.leadership == RaftState.LeadershipStatus.CANDIDATE : "We should be a candidate after triggering an election";
      }
    }
  }


  /**
   * Enforce the candidate rules from the Raft thesis.
   *
   * @param rpc The RPC we received that triggered this rule evaluation.
   * @param inboundTerm The term received from the inbound RPC. This is just called
   *                    'T' in the Raft thesis.
   */
  void enforceCandidateRules(RPCType rpc, long inboundTerm) {
    assert state.leadership == RaftState.LeadershipStatus.CANDIDATE : "We're enforcing candidate rules but are not a candidate";
    /*
     * On conversion to candidate, start election:
     *   * Increment currentTerm
     *   * Vote for self
     *   * Reset election timer
     *   * Send RequestVote RPCs to all other servers
     */
    // see {@link #convertToCandidate()}

    /*
     * If votes received from majority of servers: become leader
     */
    if (state.votesReceived.size() > state.log.getQuorumMembers().size() / 2) {
      log.info("{} - [{}] Won the election for term {}", state.serverName, transport.now(), state.currentTerm);
      this.state.elect(transport.now());
      assert state.leadership == RaftState.LeadershipStatus.LEADER : "We didn't become the leader after an election";
      /*
       * From {@link #enforceLeaderRules}:
       * Upon election: send initial empty AppendEntries RPC (heartbeat) to each server;
       * repeat during idle periods to prevent election timeouts.
       */
      this.broadcastAppendEntries();
    }

    /*
     * If AppendEntries RPC received from new leader: convert to follower
     */
    if (rpc == RPCType.APPEND_ENTRIES) {
      this.state.stepDownFromElection(inboundTerm, transport.now());
    }

    /*
     * If election timeout elapses, start new election
     */
    if (this.state.shouldTriggerElection(transport.now(), this.electionTimeoutMillisRange())) {
      this.state.stepDownFromElection(state.currentTerm, transport.now());
      assert state.leadership == RaftState.LeadershipStatus.OTHER : "We should have stepped down from our election to trigger a new election";
      this.convertToCandidate();
      assert state.leadership == RaftState.LeadershipStatus.CANDIDATE : "We should be a candidate after triggering an election";
    }
  }


  /**
   * Enforce the leader rules from the Raft thesis.
   *
   * @param rpc The RPC that triggered enforcing these rules.
   * @param follower The follower that sent the RPC
   *                 that we should observe life from.
   * @param inboundTerm The term received from the inbound RPC. This is just called
   *                    'T' in the Raft thesis.
   */
  void enforceLeaderRules(RPCType rpc, String follower, long inboundTerm) {
    /*
     * Upon election: send initial AppendEntries RPC (heartbeat) to each
     * server; repeat during idle periods to prevent election timeouts.
     */
    // see {@link #enfoceCandidateRules}.

    /*
     * If command received from client: append entry to local log,
     * respond after entry applied to state machine
     */
    // see {@link #receiveApplyTransitionRPC(ApplyTransitionRequest, boolean)}.

    /*
     * If last log index >= nextIndex for a follower: send AppendEntries RPC
     * with log entries starting at nextIndex
     *
     * * If successful: update nextIndex and matchIndex for follower
     * * If AppendEntries fails because of log inconsistency: decrement
     *   nextIndex and retry.
     */
    // see {@link #receiveAppendEntriesReply(AppendEntriesReply)}.
    // see {@link #heartbeat()}.

    /*
     * If there exists an N such that N > commitIndex, a majority of
     * matchIndex[i] >= N, and log[N].term == currentTerm:
     * set commitIndex = N.
     */
    assert this.state.matchIndex.isPresent() : "There should always be a match index map for a leader";
    if (!this.state.log.getQuorumMembers().isEmpty() && this.state.matchIndex.isPresent()) {
      // 1. Get the match indices
      //noinspection OptionalGetWithoutIsPresent
      Map<String, Long> matchIndex = this.state.matchIndex.get();
      Set<String> quorum = this.state.log.getQuorumMembers();
      long[] matchIndices = new long[quorum.size()];
      int i = -1;
      for (String member : quorum) {
        if (Objects.equals(member, this.state.serverName)) {
          matchIndices[++i] = this.state.lastApplied();
        } else {
          assert matchIndex.containsKey(member) : "no match index for member " + member;
          matchIndices[++i] = matchIndex.get(member);
        }
      }
      // 2. Get N such that "a majority of matchIndex[i] >= N"
      //    (i.e., get the median matchIndex)
      Arrays.sort(matchIndices);
      long medianIndex;
      if (matchIndices.length % 2 == 0) {
        medianIndex = matchIndices[matchIndices.length / 2 - 1];  // round pessimistically (len 4 -> index 1) -- we need a majority, not just a tie
      } else {
        medianIndex = matchIndices[matchIndices.length / 2];      // no need to round (len 3 -> index 1)
      }
      // 3. Commit up to the median index
      if (medianIndex > state.commitIndex() &&  // N > commitIndex
          state.log.getEntryAtIndex(medianIndex).map(entry -> entry.getTerm() == state.currentTerm).orElse(false)) {  // log[N].term == currentTerm
        log.trace("{} - Committing up to index {}; matchIndex={} -> {}", state.serverName, medianIndex, matchIndex, matchIndices);
        state.commitUpTo(medianIndex, transport.now());
        // 4. Broadcast the commit immediately (Eloquent addition)
        broadcastAppendEntries(false, false);
      }
    }

    /*
     * Eloquent Addition:
     * observe life from the sender of the RPC so that we don't
     * mark them for removal.
     */
    if (state.isLeader() && !Objects.equals(follower, state.serverName)) {
      state.observeLifeFrom(follower, transport.now());
    }

    /*
     * Eloquent Addition:
     * split brain detection triggers an election.
     */
    if (rpc == RPCType.APPEND_ENTRIES && inboundTerm == this.state.currentTerm) {
      log.warn("{} - Detected split brain (received heartbeat from {} on our term={}). Stepping down from election",
          this.state.serverName, follower, inboundTerm);
      state.stepDownFromElection(inboundTerm, transport.now());
    }
  }


  /**
   * Enforce the rules in the "Rules for Server" section of the Raft thesis.
   *
   * @param rpc The RPC that triggered enforcing these rules.
   * @param sender The sender of the RPC.
   * @param inboundTerm The term received from the inbound RPC. This is just called
   *                    'T' in the Raft thesis.
   * @param inboundCommitIndex The commit index from the inbound PRC. This is just called
   *                           'commitIndex' in the Raft thesis.
   * @param leader The server we think is the leader based off of the inbound RPC.
   */
  void enforceRules(RPCType rpc, String sender, long inboundTerm, Optional<Long> inboundCommitIndex, Optional<String> leader) {
    assert drivingThreadId < 0 || drivingThreadId == Thread.currentThread().getId() : "Eloquent Raft Algorithm should only be run from the driving thread (" + drivingThreadId + ") but is being driven by " + Thread.currentThread();
    // 1. Enforce general rules
    if (inboundTerm < this.state.currentTerm) {
      // This is an old message -- ignore it.
      // It should be harmless to let it through too, but safer to just kill it.
      return;
    }
    enforceServerRules(inboundTerm, inboundCommitIndex);

    // 2. Enforce role-specific rules
    switch (state.leadership) {
      case LEADER:
        // 2.A. Enforce leader invariants
        enforceLeaderRules(rpc, sender, inboundTerm);
        if (state.leadership != RaftState.LeadershipStatus.LEADER) {  // note[gabor]: shouldn't happen
          enforceRules(rpc, sender, inboundTerm, inboundCommitIndex, leader);  // recurse if we now have a new role
          return;  // no need to continue on
        }
        break;
      case CANDIDATE:
        // 2.B. Enforce candidate invariants
        enforceCandidateRules(rpc, inboundTerm);
        if (state.leadership != RaftState.LeadershipStatus.CANDIDATE) {
          enforceRules(rpc, sender, inboundTerm, inboundCommitIndex, leader);  // recurse if we now have a new role
          return;  // no need to continue on
        }
        break;
      case OTHER:
        // 2.C. Enforce follower invariants
        enforceFollowerRules(rpc, inboundTerm, leader);
        if (state.leadership != RaftState.LeadershipStatus.OTHER) {
          enforceRules(rpc, sender, inboundTerm, inboundCommitIndex, leader);  // recurse if we now have a new role
          return;  // no need to continue on
        }
        break;
      default:
        log.error("Unknown leadership state when enforcing server rules: {}", state.leader);
        break;
    }

    // 3. Reset election timeout
    if (state.leadership != RaftState.LeadershipStatus.LEADER) {  // leaders don't have election timeouts
      switch (rpc) {
        case APPEND_ENTRIES:
        case REQUEST_VOTES:
        case INSTALL_SNAPSHOT:
          this.state.resetElectionTimeout(transport.now(), leader);
          break;
      }
    }
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
    // Some setup.
    // This is not part of the official spec, it's just boilerplate and logging.
    assert drivingThreadId < 0 || drivingThreadId == Thread.currentThread().getId() : "Eloquent Raft Algorithm should only be run from the driving thread (" + drivingThreadId + ") but is being driven by " + Thread.currentThread();
    long beginTime = System.currentTimeMillis();
    RPCType rpc = RPCType.APPEND_ENTRIES;
    RaftMessage reply;
    if (heartbeat.getEntryCount() > 0) {
      log.trace("{} - [{}] {}; num_entries={}  prevIndex={}  leader={}", state.serverName, transport.now(), rpc, heartbeat.getEntryCount(), heartbeat.getPrevLogIndex(), heartbeat.getLeaderName());
    }
    AppendEntriesReply.Builder partialReply = AppendEntriesReply.newBuilder()
        .setFollowerName(state.serverName);  // signal our name

    // (0.) Server Rules
    this.enforceRules(rpc, heartbeat.getLeaderName(), heartbeat.getTerm(), Optional.of(heartbeat.getLeaderCommit()), Optional.of(heartbeat.getLeaderName()));
    try {

      /*
       * 1. Reply false if term < currentTerm
       */
      if (heartbeat.getTerm() < this.state.currentTerm) {
        reply = RaftTransport.mkRaftMessage(state.serverName, partialReply
            .setTerm(state.currentTerm)                         // signal the new term
            .setNextIndex(state.log.getLastEntryIndex() + 1)    // signal the next index in the new term
            .setSuccess(false)                                  // there was no update
            .setMissingFromQuorum(!this.state.log.latestQuorumMembers.contains(this.state.serverName))
            .build());
      } else {

        /*
         * 2-4. <delegated to appendEntries>
         */
        if (state.log.appendEntries(heartbeat.getPrevLogIndex(), heartbeat.getPrevLogTerm(), heartbeat.getEntryList())) {

          /*
           * 5. if leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last entry)
           */
          if (heartbeat.getLeaderCommit() > this.state.commitIndex()) {
            this.state.commitUpTo(
                Math.min(heartbeat.getLeaderCommit(), state.log.getLastEntryIndex()),  // min(leaderCommit, index of last entry)
                transport.now());
          }
          reply = RaftTransport.mkRaftMessage(state.serverName, partialReply
              .setSuccess(true)                                   // RPC was successful
              .setTerm(state.currentTerm)                         // signal the new term
              .setNextIndex(state.log.getLastEntryIndex() + 1)    // signal the next index in the new term
              .setMissingFromQuorum(!this.state.log.latestQuorumMembers.contains(this.state.serverName))
              .build());

        } else {
          // Reply false if log doesn't contain an entry at prevLogIndex whose term matches
          // prevLogTerm,
          long requestedNextIndex = Math.max(
              state.log.snapshot.map(snapshot -> snapshot.lastIndex).orElse(0L),
              Math.min(heartbeat.getPrevLogIndex() - 1, state.log.getLastEntryIndex())
          ) + 1;
          reply = RaftTransport.mkRaftMessage(state.serverName, partialReply
              .setTerm(state.currentTerm)                         // signal the new term
              .setNextIndex(requestedNextIndex)    // signal the next index in the new term
              .setSuccess(false)                                  // there was no update
              .setMissingFromQuorum(!this.state.log.latestQuorumMembers.contains(this.state.serverName))
              .build());
        }
      }

      // (6.) Send the reply
      replyLeader.accept(reply);

    } finally {
      // Server Rules
      this.enforceRules(rpc, heartbeat.getLeaderName(), heartbeat.getTerm(), Optional.of(heartbeat.getLeaderCommit()), Optional.of(heartbeat.getLeaderName()));
      long endTime = System.currentTimeMillis();
      if (endTime > beginTime + 50) {
        log.warn("{} - Took {} to apply AppendEntiresRPC on {} entries setting commit to {}",
            state.serverName, TimerUtils.formatTimeDifference(endTime - beginTime), heartbeat.getEntryCount(), state.commitIndex());
      }
    }
  }


  /** {@inheritDoc} */
  @Override
  public void receiveAppendEntriesReply(AppendEntriesReply reply) {
    // Some setup.
    // This is not part of the official spec, it's just boilerplate and logging.
    assert drivingThreadId < 0 || drivingThreadId == Thread.currentThread().getId() : "Eloquent Raft Algorithm should only be run from the driving thread (" + drivingThreadId + ") but is being driven by " + Thread.currentThread();
    RPCType rpc = RPCType.APPEND_ENTRIES_REPLY;

    // (0.) Server Rules
    this.enforceRules(rpc, reply.getFollowerName(), reply.getTerm(), Optional.empty(), Optional.empty());
    if (!state.isLeader()) {
      // Can't process an append entries reply if we're not the leader
      // We also shouldn't be getting one, so let's throw a log message for good measure.
      log.debug("{} - [{}] {}; Got an AppendEntriesReply but we are not the leader. Ignoring request.", state.serverName, transport.now(), rpc);
      return;
    }

    try {
      /*
       * If last log index >= nextIndex for a follower: send AppendEntries RPC
       * with log entries starting at nextIndex
       *
       * * If successful: update nextIndex and matchIndex for follower
       * * If AppendEntries fails because of log inconsistency: decrement
       *   nextIndex and retry. *** <- do this ***
       */
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
          log.warn("{} - {} detected quorum mismatch on node {}. Trying to recover, but this is an error state.", this.state.serverName, rpc, reply.getFollowerName());
          receiveAddServerRPC(AddServerRequest.newBuilder()
              .setNewServer(reply.getFollowerName())
              .addAllQuorum(state.log.latestQuorumMembers)
              .build());
        }
      } else {

        // Case: the call was rejected by the client -- resend the append entries call
        log.trace("{} - {} from {} was rejected. resending with term={} and nextIndex={}", state.serverName, rpc, reply.getFollowerName(), reply.getTerm(), reply.getNextIndex());
        // Get the next index
        long index = Math.max(0, reply.getNextIndex() - 1);
        if (index > 0 && state.log.snapshot.isPresent() && state.log.snapshot.get().lastIndex >= index) {
          index = -1;
        }
        long nextIndex = index + 1;
        assert nextIndex >= 0;
        // Update the next index
        state.nextIndex.ifPresent(x -> x.put(reply.getFollowerName(), nextIndex));
        Optional<List<LogEntry>> entries = state.log.getEntriesSinceInclusive(nextIndex);
        if (!entries.isPresent() || entries.get().size() > 0) {  // Check that we're not sending a set of empty entries, cause that would cause an infinite loop
          // This handles choosing between InstallSnapshot and AppendEntries
          sendAppendEntries(reply.getFollowerName(), nextIndex);
        }
      }
    } finally {
      // Server Rules
      this.enforceRules(rpc, reply.getFollowerName(), reply.getTerm(), Optional.empty(), Optional.empty());
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
      log.trace("{} - sending appendEntriesRequest; logIndex={}  logTerm={}  # entries={}", state.serverName, nextIndex - 1, prevEntryTerm.get(), entries.get().size());
      appendEntriesRequest = Optional.of(AppendEntriesRequest
          .newBuilder()
          .setTerm(state.currentTerm)
          .setLeaderName(state.serverName)
          .setPrevLogIndex(nextIndex - 1)
          .setPrevLogTerm(prevEntryTerm.get())
          .addAllEntry(entries.get())
          .setLeaderCommit(state.log.getCommitIndex())
          .build());
    } else {
      // 1B. We should send a snapshot
      log.trace("{} - sending snapshot; logIndex={}  snapshotLastTerm={}", state.serverName, nextIndex - 1, state.log.snapshot.map(x -> x.lastTerm).orElse(-1L));
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
            if (exception instanceof TimeoutException) {
              log.debug("{} - {} Timed out append entries", state.serverName, methodName);
            } else {
              log.warn("{} - {} Failure: <{}>", state.serverName, methodName, exception == null ? "unknown error" : (exception.getClass().getName() + ": " + exception.getMessage()));
            }
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
            if (exception instanceof TimeoutException) {
              log.debug("{} - {} Timed out append entries", state.serverName, methodName);
            } else {
              log.warn("{} - {} Failure: <{}>", state.serverName, methodName, exception == null ? "unknown error" : (exception.getClass().getName() + ": " + exception.getMessage()));
            }
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
    if (force || !transport.throttle()) {
      this.broadcastRateLimiter.forceSubmit();
    } else if (!this.broadcastRateLimiter.submit(transport.nowNanos())){
      log.trace("{} - {}; rate limiting broadcast to avoid saturating the transport.", state.serverName, methodName);
      return;
    }

    // 2. Preconditions
    if (!this.state.isLeader()) {  // in case we lost leadership in between
      log.trace("{} - {}; Not the leader", state.serverName, methodName);
      return;
    }
    assert state.nextIndex.isPresent() : "Running heartbeat as leader, but don't have a nextIndex defined";

    // 3. Find the largest update we have to send.
    long minLogIndex = Long.MAX_VALUE;
    long minLogTerm = Long.MAX_VALUE;
    List<LogEntry> argminEntries = null;
    if (state.nextIndex.isPresent()) {
      //noinspection OptionalGetWithoutIsPresent
      Map<String, Long> nextIndex = state.nextIndex.get();
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
          // 3.3. Update the min
          if (prevLogTerm < minLogTerm ||
              (prevLogTerm == minLogTerm && prevLogIndex < minLogIndex)
          ) {
            Optional<List<LogEntry>> entries = state.log.getEntriesSinceInclusive(nextIndexForMember);
            if (entries.isPresent()) {  // Ignore nodes that need a snapshot
              minLogTerm = prevLogTerm;
              minLogIndex = prevLogIndex;
              argminEntries = entries.get();
            }
          }
        }
      }
    }

    // 4. Handle the case that we have no updates
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

    // 5. Construct the broadcast
    appendEntriesRequest = AppendEntriesRequest
        .newBuilder()
        .setTerm(state.currentTerm)
        .setLeaderName(state.serverName)
        .setPrevLogIndex(minLogIndex)
        .setPrevLogTerm(minLogTerm)
        .addAllEntry(argminEntries)
        .setLeaderCommit(state.log.getCommitIndex())
        .build();

    // 6. Send the broadcast
    // 6.1. (log the broadcast while in a lock)
    log.trace("{} - Broadcasting appendEntriesRequest; logIndex={}  logTerm={}  # entries={}  forced={}  @t={}",
        state.serverName, minLogIndex, minLogTerm, argminEntries.size(), force, transport.now());
    // 6.2. (the sending does not need to be locked)
    transport.broadcastTransport(state.serverName, appendEntriesRequest);
  }


  //
  // --------------------------------------------------------------------------
  // INSTALL SNAPSHOT RPC
  // --------------------------------------------------------------------------
  //


  /** {@inheritDoc} */
  @Override
  public void receiveInstallSnapshotRPC(InstallSnapshotRequest snapshot, Consumer<RaftMessage> replyLeader) {
    // Some setup.
    // This is not part of the official spec, it's just boilerplate and logging.
    assert drivingThreadId < 0 || drivingThreadId == Thread.currentThread().getId() : "Eloquent Raft Algorithm should only be run from the driving thread (" + drivingThreadId + ") but is being driven by " + Thread.currentThread();
    RPCType rpc = RPCType.INSTALL_SNAPSHOT;
    log.trace("{} - [{}] {}:  term={}  lastIndex={}", state.serverName, transport.now(), rpc, snapshot.getTerm(), snapshot.getLastIndex());

    // (0.) Server Rules
    this.enforceRules(rpc, snapshot.getLeaderName(), snapshot.getTerm(), Optional.empty(), Optional.of(snapshot.getLeaderName()));
    try {

      /*
       * 1. Reply immediately if term < currentTerm
       */
      if (snapshot.getTerm() >= state.currentTerm) {

        /*
         * 2. Create new snapshot file if first chunk (offset is 0).
         * 3. Write data into snapshot file at given offset.
         *
         * note[gabor]: we don't have a snapshot file or chunks
         * TODO(gabor) Implement snapshot chunking.
         *             I suspect this is one of the reasons we elect so much...
         */
        RaftLog.Snapshot requestedSnapshot = new RaftLog.Snapshot(
            snapshot.getData().toByteArray(),
            snapshot.getLastIndex(),
            snapshot.getLastTerm(),
            snapshot.getLastConfigList());

        /*
         * 4. Reply and wait for more data chunks if done is false (not implemented)
         * 5. if lastIndex is larger than latest snapshot's, save snapshot file
         *    and raft state (lastIndex, lastTerm, lastConfig). Discard any
         *    existing or partial snapshot.
         * 6. If existing log entry has same index and term as lastIndex and lastTerm, discard log
         *    up through lastIndex (but retain any following entries) and reply
         * 7. Discard the entire log
         * 8. Reset state machine using snapshot contents (and load lastConfig as cluster configuration
         */
        state.log.installSnapshot(requestedSnapshot, transport.now());
      }

      // (9.) Reply
      RaftMessage reply = RaftTransport.mkRaftMessage(state.serverName, InstallSnapshotReply.newBuilder()
          .setFollowerName(state.serverName)
          .setTerm(state.currentTerm)                       // signal our term. This must come after we enforce our initial rules
          .setNextIndex(state.log.getLastEntryIndex() + 1)  // signal our last log index
          .build());
      replyLeader.accept(reply);

    } finally {
      // Server Rules
      this.enforceRules(rpc, snapshot.getLeaderName(), snapshot.getTerm(), Optional.empty(), Optional.of(snapshot.getLeaderName()));
    }
  }


  /** {@inheritDoc} */
  @Override
  public void receiveInstallSnapshotReply(InstallSnapshotReply reply) {
    // Some setup.
    // This is not part of the official spec, it's just boilerplate and logging.
    assert drivingThreadId < 0 || drivingThreadId == Thread.currentThread().getId() : "Eloquent Raft Algorithm should only be run from the driving thread (" + drivingThreadId + ") but is being driven by " + Thread.currentThread();
    RPCType rpc = RPCType.INSTALL_SNAPSHOT_REPLY;
    log.trace("{} - [{}] {} from {}. term={}  nextIndex={}", state.serverName, transport.now(), rpc, reply.getFollowerName(), reply.getTerm(), reply.getNextIndex());


    // (0.) Server Rules
    this.enforceRules(rpc, reply.getFollowerName(), reply.getTerm(), Optional.empty(), Optional.empty());
    try {

      if (state.isLeader() && reply.getTerm() >= state.currentTerm) {
        // 1. Eloquent addition:
        //    As an efficiency boost, update nextIndex and matchIndex
        //    This is, strictly speaking, optional, as the next heartbeat will handle this automatically
        state.nextIndex.ifPresent(map -> map.compute(reply.getFollowerName(), (k, v) -> reply.getNextIndex()));
        state.matchIndex.ifPresent(map -> map.compute(reply.getFollowerName(), (k, v) -> reply.getNextIndex() - 1));

        // note[gabor]: we used to re-send updated entries here, but that seems actually incorrect.
        // those entries have already been sent somewhere else, and if we're behind in the worst case we'll get them
        // on the next heartbeat. Sending them here too seems like it just complicates the algorithm.
      }

    } finally {
      // Server Rules
      this.enforceRules(rpc, reply.getFollowerName(), reply.getTerm(), Optional.empty(), Optional.empty());
    }
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
    // Some setup.
    // This is not part of the official spec, it's just boilerplate and logging.
    assert drivingThreadId < 0 || drivingThreadId == Thread.currentThread().getId() : "Eloquent Raft Algorithm should only be run from the driving thread (" + drivingThreadId + ") but is being driven by " + Thread.currentThread();
    RPCType rpc = RPCType.REQUEST_VOTES;
    RaftMessage reply;

    // (0.) Server Rules
    this.enforceRules(rpc, voteRequest.getCandidateName(), voteRequest.getTerm(), Optional.empty(), Optional.empty());
    try {

      if (voteRequest.getTerm() < state.currentTerm) {
        /*
         * 1. Reply false if term < currentTerm
         */
        reply = RaftTransport.mkRaftMessage(state.serverName, RequestVoteReply.newBuilder()
            .setVoterName(state.serverName)
            .setTerm(state.currentTerm)  // this is axiomatically greater than the requester term
            .setVoteGranted(false)
            .build());

      } else if (
          (!state.votedFor.isPresent() || state.votedFor.get().equals(voteRequest.getCandidateName()))  && // votedFor is null or candidateId
              voteRequest.getLastLogIndex() >= state.log.getLastEntryIndex()  // candidate's log is at least as up-to-date as receiver's log
          ) {

        /*
         * 2. If votedFor is null or candidateId, and candidate's log is at least as up-to-date
         *    as receiver's log, grant vote.
         */
        state.voteFor(voteRequest.getCandidateName());
        reply = RaftTransport.mkRaftMessage(state.serverName, RequestVoteReply.newBuilder()
            .setVoterName(state.serverName)
            .setTerm(Math.max(voteRequest.getTerm(), state.currentTerm))
            .setVoteGranted(true)
            .build());
      } else {

        // Vote is not granted
        reply = RaftTransport.mkRaftMessage(state.serverName, RequestVoteReply.newBuilder()
            .setVoterName(state.serverName)
            .setTerm(Math.max(voteRequest.getTerm(), state.currentTerm))
            .setVoteGranted(false)
            .build());

      }

      // (3.) Send the reply
      log.info("{} - [{}] {};  candidate={}  candidate_term={}  my_term={}  vote_granted={}",
          state.serverName, transport.now(), rpc, voteRequest.getCandidateName(), voteRequest.getTerm(), state.currentTerm,
          reply.getRequestVotesReply().getVoteGranted());
      replyLeader.accept(reply);

    } finally {
      // Server Rules
      this.enforceRules(rpc, voteRequest.getCandidateName(), voteRequest.getTerm(), Optional.empty(), Optional.empty());
    }
  }


  /** {@inheritDoc} */
  @Override
  public void receiveRequestVotesReply(RequestVoteReply reply) {
    // Some setup.
    // This is not part of the official spec, it's just boilerplate and logging.
    assert drivingThreadId < 0 || drivingThreadId == Thread.currentThread().getId() : "Eloquent Raft Algorithm should only be run from the driving thread (" + drivingThreadId + ") but is being driven by " + Thread.currentThread();
    RPCType rpc = RPCType.REQUEST_VOTES_REPLY;
    log.info("{} - [{}] {} {} {} for me ({});  term={}",
        state.serverName, transport.now(), rpc,
        reply.getVoterName(), reply.getVoteGranted() ? "voted" : "did not vote", state.serverName, reply.getTerm());


    // IMPORTANT: don't enforce rules at the beginning of this function.
    // It's important that we don't update our term from the election, or else
    // state.currentTerm will axiomatically be reply.getRequesterTerm() below.
    try {

      // 1. Receive a vote, if we got one.
      //    We allow the vote if:
      //      (i)  The vote was granted by the voter, and
      //      (ii) We are still on the same term as we were when we requested the vote.
      //
      //    The invariants are handled in {@link #enforceRules(RPCType, long, Optional, Optional)}
      if (this.state.currentTerm == reply.getTerm() && reply.getVoteGranted()) {
        state.receiveVoteFrom(reply.getVoterName());
      }

    } finally {
      // Server Rules
      // note[gabor]: this is where we actually elect ourselves if applicable, if we've received a vote above
      this.enforceRules(rpc, reply.getVoterName(), reply.getTerm(), Optional.empty(), Optional.empty());
    }
  }


  /** {@inheritDoc} */
  @Override
  public void triggerElection() {
    assert drivingThreadId < 0 || drivingThreadId == Thread.currentThread().getId() : "Eloquent Raft Algorithm should only be run from the driving thread (" + drivingThreadId + ") but is being driven by " + Thread.currentThread();
    log.info("{} [{}] - triggering an election", state.serverName, transport.now());
    try {
      this.convertToCandidate();
    } finally {
      // Server Rules
      // note[gabor]: this is where we elect ourselves if we immediately elect.
      this.enforceRules(RPCType.TRIGGER_ELECTIOn, this.serverName(), this.state.currentTerm, Optional.empty(), Optional.empty());
    }
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
          log.warn("{} - {}; we've been forwarded our own message back to us in a loop. Failing the message", state.serverName, methodName);
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
              if (exception instanceof TimeoutException) {
                log.debug("{} - {} Timed out transition", state.serverName, methodName);
              } else {
                log.warn("{} - {} Failure: <{}>", state.serverName, methodName, exception == null ? "unknown error" : (exception.getClass().getName() + ": " + exception.getMessage()));
              }
              return RaftMessage.newBuilder().setSender(state.serverName).setAddServerReply(AddServerReply.newBuilder()
                  .setStatus(MembershipChangeStatus.NOT_LEADER)).build();
            }
          }, this.drivingThreadQueue, this.electionTimeoutMillisRange().end);
        }
      } else {
        // Case: We're not the leader, and we don't know who is.
        //       We have no choice but to fail the request.
        //       This is the common failure case during an election.
        log.debug("{} - got {}; we're not the leader and don't know who the leader is", state.serverName, methodName);
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
            state.stepDownFromElection(this.state.currentTerm, this.transport.now());
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
              if (exception instanceof TimeoutException) {
                log.debug("{} - {} Timed out remove server", state.serverName, methodName);
              } else {
                log.warn("{} - {} Failure: <{}>", state.serverName, methodName, exception == null ? "unknown error" : (exception.getClass().getName() + ": " + exception.getMessage()));
              }
              return RaftMessage.newBuilder().setSender(state.serverName).setRemoveServerReply(RemoveServerReply.newBuilder()
                  .setStatus(MembershipChangeStatus.NOT_LEADER)).build();
            }
          }, this.drivingThreadQueue, this.electionTimeoutMillisRange().end + 100);
        }
      } else {
        // Case: We're not the leader, and we don't know who is.
        //       We have no choice but to fail the request.
        log.debug("{} - got {}; we're not the leader and don't know who the leader is", state.serverName, methodName);
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


  /**
   * {@inheritDoc}
   *
   * <p>
   *   From the Raft spec:
   * </p>
   * <p>
   *   If command received from client: append entry to local log,
   *   respond after entry applied to state machine
   * </p>
   */
  @Override
  public CompletableFuture<RaftMessage> receiveApplyTransitionRPC(ApplyTransitionRequest transition, boolean forceOntoQueue) {
    // Some setup.
    // This is not part of the official spec, it's just boilerplate and logging.
    assert drivingThreadId < 0 || drivingThreadId == Thread.currentThread().getId() : "Eloquent Raft Algorithm should only be run from the driving thread (" + drivingThreadId + ") but is being driven by " + Thread.currentThread();
    RPCType rpc = RPCType.APPLY_TRANSITION;
    log.trace("{} - [{}] {}; is_leader={}", state.serverName, transport.now(), rpc, this.state.isLeader());
    CompletableFuture<RaftMessage> rtn;


    // (0.) Server Rules
    this.enforceRules(rpc, state.serverName, transition.getTerm(), Optional.empty(), Optional.empty());
    try {

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
            log.info("{} - {}; failed transition (on leader; could not commit) @ time={}", state.serverName, rpc, transport.now(), exception);
          }
          // Reply
          return RaftMessage.newBuilder().setSender(state.serverName).setApplyTransitionReply(reply).build();
        });
        // Early broadcast message
        broadcastAppendEntries(this.state.log.latestQuorumMembers.size() <= 1, false);

      } else if (state.leader.map(leader -> !leader.equals(this.state.serverName)).orElse(false)) {
        if (transition.getForwardedByList().contains(state.serverName)) {

          // Case: We're being forwarded this message in a loop (we've forwarded this message in the past), so fail it
          log.info("{} - {}; we've been forwarded our own message back to us in a loop. Failing the message", state.serverName, rpc);
          rtn = CompletableFuture.completedFuture(RaftMessage.newBuilder().setSender(state.serverName).setApplyTransitionReply(ApplyTransitionReply
              .newBuilder()
              .setTerm(state.currentTerm)
              .setNewEntryIndex(-2)
              .setSuccess(false)).build());
        } else {

          // Case: We're not the leader -- forward it to the leader
          assert !state.leader.map(x -> x.equals(this.serverName())).orElse(false) : "We are sending a message to the leader, but we think we're the leader!";
          log.trace("{} - [{}] {}; forwarding request to {}", state.serverName, transport.now(), rpc, this.state.leader.orElse("<unknown>"));
          // Forward the request
          rtn = transport.rpcTransportAsFuture(
              state.serverName,
              state.leader.orElse("<unknown>"),
              transition.toBuilder().addForwardedBy(state.serverName).build(),
              (result, exception) -> {
                assert drivingThreadId < 0 || drivingThreadId == Thread.currentThread().getId() : "Eloquent Raft Algorithm should only be run from the driving thread (" + drivingThreadId + ") but is being driven by " + Thread.currentThread();
                if (result != null) {
                  log.trace("{} - {}; received reply to forwarded message", state.serverName, rpc);
                  return result;
                } else {
                  if (exception instanceof TimeoutException) {
                    log.debug("{} - {} Timed out apply transition", state.serverName, rpc);
                  } else {
                    log.warn("{} - [{}] {} Failure: <{}>;  is_leader={}  leader={}",
                        state.serverName, transport.now(), rpc, exception == null ? "unknown error" : (exception.getClass().getName() + ": " + exception.getMessage()),
                        state.isLeader(), state.leader.orElse("<unknown>"));
                  }
                  return RaftMessage.newBuilder().setSender(state.serverName).setApplyTransitionReply(ApplyTransitionReply
                      .newBuilder()
                      .setTerm(state.currentTerm)
                      .setNewEntryIndex(-3)
                      .setSuccess(false)).build();
                }
              }, this.drivingThreadQueue, 2000)  // 2s timeout
              .thenCompose(leaderResponse -> {
                assert drivingThreadId < 0 || drivingThreadId == Thread.currentThread().getId() : "Eloquent Raft Algorithm should only be run from the driving thread (" + drivingThreadId + ") but is being driven by " + Thread.currentThread();
                // Then wait for it to commit locally
                if (leaderResponse.getApplyTransitionReply().getSuccess()) {
                  log.trace("{} - {}; waiting for commit", state.serverName, rpc);
                  long index = leaderResponse.getApplyTransitionReply().getNewEntryIndex();
                  long term = leaderResponse.getApplyTransitionReply().getNewEntryTerm();
                  return state.log.createCommitFuture(index, term, true).handle((success, t) -> {
                    assert drivingThreadId < 0 || drivingThreadId == Thread.currentThread().getId() : "Eloquent Raft Algorithm should only be run from the driving thread (" + drivingThreadId + ") but is being driven by " + Thread.currentThread();
                    log.trace("{} - {}; Entry @ {} (term={}) is committed in the local log", state.serverName, rpc, index, term);
                    // The transition was either committed or overwritten
                    ApplyTransitionReply.Builder reply = ApplyTransitionReply
                        .newBuilder()
                        .setTerm(state.currentTerm)
                        .setNewEntryIndex(-4)  // just so this isn't the default proto. Overwritten below
                        .setSuccess(success == null ? false : success);
                    if (success != null && success) {
                      reply
                          .setNewEntryIndex(index)
                          .setNewEntryTerm(term);
                    } else {
                      log.info("{} - {}; failed transition (on follower) @ time={}", state.serverName, rpc, transport.now());
                    }
                    return RaftMessage.newBuilder().setSender(state.serverName).setApplyTransitionReply(reply).build();
                  });
                } else {
                  log.debug("{} - {}; failed to apply transition (reply proto had failure marked); returning failure", state.serverName, rpc);
                  return CompletableFuture.completedFuture(leaderResponse);
                }
              });
        }
      } else {

        // Case: We're not the leader, and we don't know who is.
        //       We have no choice but to fail the request.
        log.warn("{} - {}; we're not the leader and don't know who the leader is. This is OK if we're in the middle of an election", state.serverName, rpc);
        rtn = CompletableFuture.completedFuture(RaftMessage.newBuilder().setSender(state.serverName).setApplyTransitionReply(ApplyTransitionReply
            .newBuilder()
            .setTerm(state.currentTerm)
            .setNewEntryIndex(-5)
            .setSuccess(false)).build());
      }

      // Return
      if (rtn.isDone()) {
        return rtn;
      } else {
        return rtn.thenApplyAsync(message -> message, this.state.log.pool);  // defer the future to another thread
      }

    } finally {
      // Server Rules
      this.enforceRules(rpc, state.serverName, transition.getTerm(), Optional.empty(), Optional.empty());
    }
  }


  /** {@inheritDoc} */
  @Override
  public void heartbeat() {
    // Some setup.
    // This is not part of the official spec, it's just boilerplate and logging.
    assert drivingThreadId < 0 || drivingThreadId == Thread.currentThread().getId() : "Eloquent Raft Algorithm should only be run from the driving thread (" + drivingThreadId + ") but is being driven by " + Thread.currentThread();
    RPCType rpc = RPCType.HEARTBEAT;
    log.trace("{} - [{}] {}", state.serverName, transport.now(), rpc);
    Object timerPrometheusBegin = Prometheus.startTimer(summaryTiming, "heartbeat", "false", Boolean.toString(this.state.isLeader()));

    // (0.) Server Rules
    this.enforceRules(rpc, state.serverName, state.currentTerm, Optional.empty(), Optional.empty());
    try {
      if (this.state.isLeader()) {

        // Case: do a leader heartbeat
        log.trace("{} - heartbeat (leader) @ time={}  cluster={}", state.serverName, transport.now(), state.log.getQuorumMembers());
        broadcastAppendEntries(false, true);  // the heartbeat.
        if (Thread.interrupted()) {
          throw new RuntimeInterruptedException();
        }

        // Check if we should scale the cluster
        if (state.targetClusterSize >= 0) {  // only do this if we don't have a fixed cluster
          if (clusterMembershipFuture.getNow(null) != null) {  // if we're not still waiting on membership changes
            Optional<String> maybeServer;
            if ((maybeServer = state.serverToRemove(transport.now(), MACHINE_DOWN_TIMEOUT)).isPresent()) {
              // Case: we want to scale down the cluster
              log.info("{} - Detected Raft cluster is too large ({} > {}) or we have a delinquent server; scaling down by removing {} (latency {})  current_time={}",
                  state.serverName, state.log.getQuorumMembers().size(), state.targetClusterSize, maybeServer.get(), transport.now() - maybeServer.flatMap(server -> state.lastMessageTimestamp.map(x -> x.get(server))).orElse(-1L), transport.now());
              this.receiveRPC(RaftTransport.mkRaftRPC(state.serverName,
                  EloquentRaftProto.RemoveServerRequest.newBuilder()
                      .setOldServer(maybeServer.get())
                      .build()),
                  true  // we don't want to block on this call
              );
            } else if ((maybeServer = state.serverToAdd(transport.now(), electionTimeoutMillisRange().begin)).isPresent()) {  // note: add timeout is different from remove timeout. Much more strict to add than to remove
              // Case: we want to scale up the cluster
              log.info("{} - Detected Raft cluster is too small ({} < {}); scaling up by adding {} (latency {})  current_time={}",
                  state.serverName, state.log.getQuorumMembers().size(), state.targetClusterSize, maybeServer.get(), transport.now() - maybeServer.flatMap(server -> state.lastMessageTimestamp.map(x -> x.get(server))).orElse(-1L), transport.now());
              this.receiveRPC(RaftTransport.mkRaftRPC(state.serverName,
                  EloquentRaftProto.AddServerRequest.newBuilder()
                      .setNewServer(maybeServer.get())
                      .build()),
                  true  // we don't want to block on this call
              );
            }
          }
          if (Thread.interrupted()) {
            throw new RuntimeInterruptedException();
          }
        }

        // Check if anyone has gone offline
        if (state.log.stateMachine instanceof KeyValueStateMachine) {
          Set<String> toKillSet = state.killNodes(transport.now(), MACHINE_DOWN_TIMEOUT);
          for (String deadNode : toKillSet) {
            assert drivingThreadId < 0 || drivingThreadId == Thread.currentThread().getId() : "Eloquent Raft Algorithm should only be run from the driving thread (" + drivingThreadId + ") but is being driven by " + Thread.currentThread();
            log.info("{} - Clearing transient data for node {}", state.serverName, deadNode);
            receiveApplyTransitionRPC(ApplyTransitionRequest.newBuilder()
                    .setTransition(ByteString.copyFrom(KeyValueStateMachine.createClearTransition(deadNode)))
                    .build(),
                true)  // don't block on this call!
                .whenComplete((response, exception) -> {
                  assert drivingThreadId < 0 || drivingThreadId == Thread.currentThread().getId() : "Eloquent Raft Algorithm should only be run from the driving thread (" + drivingThreadId + ") but is being driven by " + Thread.currentThread();
                  if (exception != null || response == null || !response.getApplyTransitionReply().getSuccess()) {
                    state.revive(deadNode);
                  }
                });
          }
        }

      } else {

        // Case: Do a follower heartbeat.
        //       This is basically a NOOP, handled by {@link #enforceRules(RPCType, long, Optional, Optional)}.
        log.trace("{} - heartbeat (follower) @ time={}  cluster={}", state.serverName, transport.now(), state.log.getQuorumMembers());
      }

    } finally {
      this.enforceRules(rpc, state.serverName, state.currentTerm, Optional.empty(), Optional.empty());
      Prometheus.observeDuration(timerPrometheusBegin);
    }
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
  public void awaitLeaderKnown() {
    assert drivingThreadId < 0 || drivingThreadId != Thread.currentThread().getId() : "It's extremely dangerous to awaitCapacity on the driving thread!";
    int iters = 0;
    while ((++iters < 1000) && (!state.leader.isPresent())) {
      Uninterruptably.sleep(1);  // sleep for just a bit
    }
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
    return errors;
  }


  //
  // --------------------------------------------------------------------------
  // OBJECT OVERRIDES
  // --------------------------------------------------------------------------
  //

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return this.serverName();
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

