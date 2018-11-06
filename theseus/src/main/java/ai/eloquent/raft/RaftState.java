package ai.eloquent.raft;

import ai.eloquent.util.Span;
import com.google.protobuf.ByteString;

import java.util.*;
import java.util.concurrent.ExecutorService;

/**
 * The state of a Raft node.
 *
 * @author <a href="mailto:gabor@eloquent.ai">Gabor Angeli</a>
 */
public class RaftState {

  /**
   * The leadership status of the given Raft node.
   * The other class encompasses citizens and shadows both.
   */
  public enum LeadershipStatus {
    LEADER,
    CANDIDATE,
    OTHER
  }


  /* --------------------------------------------------------------------------
     PERSISTENT STATE
     -------------------------------------------------------------------------- */

  /**
   * The name (i.e., id) of this server.
   */
  public final String serverName;

  /**
   * The Raft log.
   */
  public final RaftLog log;

  /**
   * Latest term server has seen (initialized to 0 on first boot, increases monotonically.
   */
  public long currentTerm = 0;

  /**
   * candidateId that received vote in the current term (or null if none)
   */
  public Optional<String> votedFor = Optional.empty();

  /**
   * ADDITION: the target size of the cluster, for auto-scaling. A value of -1
   * disables auto-scaling altogether.
   */
  public final int targetClusterSize;


  /* --------------------------------------------------------------------------
     VOLATILE STATE (ALL)
     -------------------------------------------------------------------------- */

  /**
   * The index of the highest log entry applied to state machine (initialized to 0,
   * increases monotonically).
   * This is a computed value, but appears here with the variables because it is part of
   * the core Raft spec as a variable.
   */
  @SuppressWarnings("unused")
  public long lastApplied() {
    return this.log.getLastEntryIndex();
  }

  /**
   * The leadership status of this node (leader / candidate / other)
   */
  public volatile LeadershipStatus leadership = LeadershipStatus.OTHER;

  /* --------------------------------------------------------------------------
     VOLATILE STATE (FOLLOWERS)
     -------------------------------------------------------------------------- */

  /**
   * The timestamp of the last message received by this node from the leader.
   * This is used to compute the election timeout.
   */
  public volatile long electionTimeoutCheckpoint = -1;  // also used by candidates

  /**
   * The identity of the leader, if we know it.
   */
  public volatile Optional<String> leader = Optional.empty();


  /* --------------------------------------------------------------------------
     VOLATILE STATE (CANDIDATES)
     -------------------------------------------------------------------------- */

  /**
   * The timestamp of the last message received by this node from the leader.
   * This is used to compute the election timeout.
   */
  public volatile Set<String> votesReceived = new HashSet<>();

  /* --------------------------------------------------------------------------
     VOLATILE STATE (LEADER)
     -------------------------------------------------------------------------- */

  /**
   * For each server, the index of the next log entry to send to that server
   * (initialized to leader last log index + 1).
   */
  public volatile Optional<Map<String, Long>> nextIndex = Optional.empty();


  /**
   * For each server, the index of the highest log entry known to be replicated
   * on server.
   * (initialized to 0, increases monotonically).
   */
  public volatile Optional<Map<String, Long>> matchIndex = Optional.empty();


  /**
   * <p>
   *   The timestamp of the last message received from each of the servers.
   *   This value should never be absent for any server -- we initialize it optimistically.
   * </p>
   */
  public volatile Optional<Map<String, Long>> lastMessageTimestamp = Optional.empty();

  /**
   * <p>
   *   The set of servers that have timed out, but we have already removed their transient
   *   entries. This prevents us spamming the |ClearTransient| transition.
   * </p>
   */
  public volatile Optional<Set<String>> alreadyKilled = Optional.empty();

  /**
   * A cache on {@link RaftStateMachine#owners()}, since the call can potentially take
   * a long time.
   */
  private volatile Set<String> cachedOwners = new HashSet<>();

  /**
   * The timestamp at which we last refreshed {@link #cachedOwners}.
   */
  private volatile long cachedOwnersTimestamp = Long.MIN_VALUE;


  /* --------------------------------------------------------------------------
     METHODS
     -------------------------------------------------------------------------- */

  /**
   * Create a new Raft state with only one node -- the given argument server name.
   */
  public RaftState(String serverName, RaftStateMachine stateMachine, ExecutorService pool) {
    this(serverName, new RaftLog(stateMachine, Collections.singletonList(serverName), pool), -1);
  }

  /**
   * Create a new Raft state with the given target cluster size. If this size is -1, this becomes a single-node
   * Raft cluster with just this node. If this size is non-negative, it'll become a resizing cluster with the given
   * number of target nodes.
   */
  public RaftState(String serverName, RaftStateMachine stateMachine, int targetClusterSize, ExecutorService pool) {
    this(serverName, new RaftLog(stateMachine, targetClusterSize >= 0 ? Collections.emptyList() : Collections.singletonList(serverName), pool), targetClusterSize);
  }


  /** Create a new Raft state with the given cluster members. This cluster is non-resizing. */
  public RaftState(String serverName, RaftStateMachine stateMachine, Collection<String> clusterMembers, ExecutorService pool) {
    this(serverName, new RaftLog(stateMachine, clusterMembers, pool), -1);
  }


  /** The straightforward constructor */
  public RaftState(String serverName, RaftLog log, int targetClusterSize) {
    this.serverName = serverName;
    this.log = log;
    if (!log.logEntries.isEmpty()) {
      this.currentTerm = log.getLastEntryTerm();
    }
    this.targetClusterSize = targetClusterSize;
  }

  public RaftState(String serverName, RaftLog log) {
    this(serverName, log, 3);
  }


  /**
   * Create a copy of the Raft state at this moment in time.
   */
  public RaftState copy() {
    log.assertConsistency();
    RaftState copy = new RaftState(serverName, log.copy(), this.targetClusterSize);
    copy.currentTerm = this.currentTerm;
    copy.votedFor = this.votedFor;
    copy.leadership = this.leadership;
    copy.leader = this.leader;
    copy.nextIndex = this.nextIndex.map(HashMap::new);
    copy.matchIndex = this.matchIndex.map(HashMap::new);
    copy.lastMessageTimestamp = this.lastMessageTimestamp.map(HashMap::new);
    copy.alreadyKilled = this.alreadyKilled.map(HashSet::new);
    copy.electionTimeoutCheckpoint = this.electionTimeoutCheckpoint;
    copy.votesReceived = new HashSet<>(this.votesReceived);
    return copy;
  }



  /**
   * Index of the highest log entry know to be committed (initialized to 0, increases
   * monotonically).
   */
  public long commitIndex() {
    return log.commitIndex;
  }


  /**
   * Bootstrap this cluster by adding this node to our own list of known nodes.
   * <b>This is inherently an unsafe operation!</b>
   * You should only call this function on one node, or else we may have split-brain
   *
   */
  public void bootstrap(boolean force) {
    log.assertConsistency();
    try {
      if (force || (this.targetClusterSize >= 0 && this.log.getQuorumMembers().isEmpty())) {  // only if we need bootstrapping
        this.setCurrentTerm(this.currentTerm + (force ? 100000 : 1)); // Massively increase the term, which should force other boxes to acknowledge us as leader
        this.log.unsafeBootstrapQuorum(Collections.singleton(serverName));
      }
    } finally {
      log.assertConsistency();
    }
  }


  /** @see #bootstrap(boolean) */
  public void bootstrap() {
    bootstrap(false);
  }


  /**
   * Set the current term for the state.
   *
   * @param term The new term.
   *
   * @see #currentTerm
   */
  public void setCurrentTerm(long term) {
    log.assertConsistency();
    assert term >= this.currentTerm: "The term number can never go backwards";
    try {
      if (term > this.currentTerm) {
        // Clear the election state -- someone succeeded
        this.votesReceived.clear();
        this.votedFor = Optional.empty();
      }
      this.currentTerm = term;
    } finally {
      log.assertConsistency();
    }
  }


  /**
   * Signal to the state that we have signs of life from a particular follower.
   * This updates {@link #lastMessageTimestamp}.
   *
   * @param followerName The name of the follower we observed life from.
   * @param now The current time. This is useful to be explicit about for mocks.
   */
  public void observeLifeFrom(String followerName, long now) {
    log.assertConsistency();
    try {
      assert isLeader() : "Cannot observe signs of life from a follower if we're not the leader!";
      assert this.lastMessageTimestamp.isPresent() : "We think we're the leader, but have no lastMessageTimestamp";
      lastMessageTimestamp.ifPresent(lastMessage -> lastMessage.compute(followerName, (key, currentValue) -> now));
      alreadyKilled.ifPresent(killed -> killed.remove(followerName));
      nextIndex.ifPresent(nextIndex -> nextIndex.computeIfAbsent(followerName, key -> log.getLastEntryIndex()));
      matchIndex.ifPresent(nextIndex -> nextIndex.computeIfAbsent(followerName, key -> 0L));
    } finally {
      log.assertConsistency();
    }
  }


  /** If true, we [think we] are the leader. */
  public boolean isLeader() {
    log.assertConsistency();
    return leadership == LeadershipStatus.LEADER;
  }


  /** If true, we are a candidate to become leader in an election. */
  public boolean isCandidate() {
    log.assertConsistency();
    return leadership == LeadershipStatus.CANDIDATE;
  }


  /**
   * Mark this state as a leader, setting the appropriate variables.
   *
   * @param now The current time, so we can initialize {@link #lastMessageTimestamp}.
   *            This is mostly useful for mocks -- otherwise it should be {@link System#currentTimeMillis()}.
   */
  public void elect(long now) {
    log.assertConsistency();
    try {
      assert this.leadership != LeadershipStatus.LEADER : "Should not be able to elect if already a leader";
      assert log.getQuorumMembers().contains(this.serverName) : "Cannot be elected leader if we're not in quorum";
      if (this.leadership != LeadershipStatus.LEADER) {  // don't double-elect
        this.leadership = LeadershipStatus.LEADER;
        HashMap<String, Long> nextIndex = new HashMap<>(5);
        HashMap<String, Long> matchIndex = new HashMap<>(5);
        HashMap<String, Long> lastMessageTimestamp = new HashMap<>(5);
        for (String node : log.getQuorumMembers()) {
          if (!node.equals(serverName)) {
            nextIndex.put(node, log.getLastEntryIndex() + 1);  // Assume we're up to date
            matchIndex.put(node, 0L);  // Assume no one has committed anything
            lastMessageTimestamp.put(node, now);  // Assume everyone is online
          }
        }
        Set<String> owners = this.log.stateMachine.owners();
        for (String stateOwner : owners) {
          if (!stateOwner.equals(this.serverName)) {
            lastMessageTimestamp.put(stateOwner, now);  // Assume everyone who owns anything on the state is online
          }
        }
        this.nextIndex = Optional.of(nextIndex);
        this.matchIndex = Optional.of(matchIndex);
        this.lastMessageTimestamp = Optional.of(lastMessageTimestamp);
        this.alreadyKilled = Optional.of(new HashSet<>());
        this.leader = Optional.of(serverName);
      }
    } finally {
      log.assertConsistency();
    }
  }


  /** Step down from an election -- become a simple follower. */
  public void stepDownFromElection() {
    log.assertConsistency();
    try {
      this.leadership = LeadershipStatus.OTHER;
      if (this.leader.map(x -> x.equals(serverName)).orElse(false)) {
        this.leader = Optional.empty();
      }
      this.nextIndex = Optional.empty();
      this.lastMessageTimestamp = Optional.empty();
      this.alreadyKilled = Optional.empty();
      this.matchIndex = Optional.empty();
      // Reset follower state
      this.electionTimeoutCheckpoint = -1;
      this.votesReceived.clear();
    } finally {
      log.assertConsistency();
    }
  }


  /**
   * Trigger a new election, and become a candidate.
   */
  public void becomeCandidate() {
    log.assertConsistency();
    try {
      // Error checks
      assert this.leadership == LeadershipStatus.OTHER : "Can only become a candidate from a non-candidate, non-leader state";
      assert this.log.getQuorumMembers().contains(this.serverName) : "Cannot become a candidate if we are not in the quorum";
      if (this.leadership != LeadershipStatus.OTHER) {
        this.stepDownFromElection();
      } // just in case
      // Become a candidate
      if (this.leadership == LeadershipStatus.OTHER) {
        this.leadership = LeadershipStatus.CANDIDATE;
        assert votesReceived.isEmpty() : "Should not have any votes received when we start an election";
        this.votesReceived.clear();
      }
    } finally {
      log.assertConsistency();
    }
  }


  /**
   * Reset the election timeout. That is, register that we've either just started
   * an election, or we've received a message from the leader that suggests that we
   * don't need to start an election.
   *
   * @param now The current timestamp. Should be {@link System#currentTimeMillis()} unless we're in a mock
   */
  public void resetElectionTimeout(long now, Optional<String> leader) {
    log.assertConsistency();
    try {
      // This is possible, just because the clock takes a while to process functions
//    assert this.electionTimeoutCheckpoint < 0 || this.electionTimeoutCheckpoint <= now : "Should not be able to move election timeout checkpoint backwards! now=" + now +";  checkpoint=" + this.electionTimeoutCheckpoint;
      if (this.electionTimeoutCheckpoint < 0 || this.electionTimeoutCheckpoint <= now) {
        // 1. Reset the election timeout
        this.electionTimeoutCheckpoint = now;
        // 2. Set the leader pointer
//        if (!leader.isPresent() || log.getQuorumMembers().contains(leader.get())) {  // note[gabor]: this is ok in case of handoffs
          assert !leader.isPresent() || !leader.get().equals(this.serverName) || this.isLeader() || this.isCandidate() : "Can only set the leader to ourselves if we're a leader or candidate";
          if (!this.leader.equals(leader)) {
            this.leader = leader;
          }
//        }
      }
    } finally {
      log.assertConsistency();
    }
  }


  /**
   * @see #resetElectionTimeout(long, Optional)
   */
  public void resetElectionTimeout(long now, String leader) {
    log.assertConsistency();
    try {
      resetElectionTimeout(now, Optional.ofNullable(leader));
    } finally {
      log.assertConsistency();
    }
  }


  /**
   * If true, we should trigger an election.
   * If we have no timeout checkpoint, this starts the clock on the election timeout.
   *
   * @param now The current time, so that we can start the timer.
   * @param electionTimeoutMillisRange The timeout before we should trigger an election, as a range.
   *                                   The chosen value in this range will be fixed for a given server + term.
   *
   * @return True if we should trigger an election.
   */
  public boolean shouldTriggerElection(long now, Span electionTimeoutMillisRange) {
    log.assertConsistency();
    try {
      if (this.electionTimeoutCheckpoint < 0) {
        // Case: this is our first check -- set the starting checkpoint, and wait for the timeout from here
        this.electionTimeoutCheckpoint = now;
      }

      if (!log.getQuorumMembers().contains(this.serverName)) {
        // Case: we're a shadow node -- never elect ourselves
        return false;
      } else if (isLeader()) {
        return false;  // never trigger an election if we're the leader
      } else {
        // Case: check if we should trigger a timeout
        long seed = this.serverName.hashCode() ^ (new Random(this.currentTerm).nextLong());
        long electionTimeoutMillis = new SplittableRandom(seed).nextLong(electionTimeoutMillisRange.begin, electionTimeoutMillisRange.end);
        return now - this.electionTimeoutCheckpoint > electionTimeoutMillis;
      }
    } finally {
      log.assertConsistency();
    }
  }


  /**
   * Vote for a particular server as the leader.
   *
   * @param vote The server we voted for.
   * @param now The current time.
   */
  public void voteFor(String vote, long now) {
    log.assertConsistency();
    try {
      assert !votedFor.isPresent() || votedFor.get().equals(vote);
      if (!votedFor.isPresent() || votedFor.get().equals(vote)) {
        // Vote for the node
        this.votedFor = Optional.ofNullable(vote);
        // If the node is us, receive a vote from us :)
        if (this.serverName.equals(vote)) {
          receiveVoteFrom(this.serverName);
        }
        if (vote != null && !vote.equals(this.serverName)) {
          // We're sure as hell not keeping our old leader -- may as well guess this candidate is the new leader
          this.leader = Optional.of(vote);
        }
        // Reset the election timer
        // From p.129: "...it’s likely that all servers have reset their timers, _since servers do this when they grant a vote_"
        this.electionTimeoutCheckpoint = now;
      }
    } finally {
      log.assertConsistency();
    }
  }


  /**
   * Receive a vote from a particular server. If we recieve a majority of the votes,
   * then we are considered elected.
   *
   * @param voter The server that voted for us.
   */
  public void receiveVoteFrom(String voter) {
    log.assertConsistency();
    try {
      if (log.getQuorumMembers().contains(voter)) {
        this.votesReceived.add(voter);
      }
    } finally {
      log.assertConsistency();
    }
  }


  /**
   * This creates a new transition log entry, and appends it. Must be the current LEADER, or will throw an assert.
   *
   * @param transition the transition we'd like to add to the log.
   * @param newHospiceMember an optional new hospice member to add to the state machine.
   *
   * @return (term, index) of the new entry
   */
  public RaftLogEntryLocation transition(Optional<byte[]> transition, Optional<String> newHospiceMember) {
    log.assertConsistency();
    try {
      assert isLeader() : "Should only be able to transition as leader.";
      long newEntryIndex = log.getLastEntryIndex() + 1;
      EloquentRaftProto.LogEntry.Builder newLogEntry = EloquentRaftProto.LogEntry
          .newBuilder()
          .setIndex(newEntryIndex)
          .setTerm(currentTerm)
          .setType(EloquentRaftProto.LogEntryType.TRANSITION);
      transition.ifPresent(bytes -> newLogEntry.setTransition(ByteString.copyFrom(bytes)));
      newHospiceMember.ifPresent(newLogEntry::setNewHospiceMember);
      boolean success = log.appendEntries(log.getLastEntryIndex(), log.getLastEntryTerm(), Collections.singletonList(newLogEntry.build()));
      assert success : "Should always be able to add log entries on the leader";
      this.matchIndex.ifPresent(map -> map.put(this.serverName, log.getLastEntryIndex()));  // we're always matched up to date
      return new RaftLogEntryLocation(newEntryIndex, currentTerm);
    } finally {
      log.assertConsistency();
    }
  }


  /**
   * Transition with only a regular transition, but no hospice member.
   *
   * @param transition The transition to implement.
   *
   * @return (term, index) of the new entry
   */
  public RaftLogEntryLocation transition(byte[] transition) {
    return transition(Optional.of(transition), Optional.empty());
  }



  /**
   * Commit up until a given index (inclusive).
   * Note that we can never commit backwards
   *
   * @param commitIndex The index to commit up to.
   * @param now The current time.
   */
  public void commitUpTo(long commitIndex, long now) {
    log.assertConsistency();
    try {
      assert commitIndex >= this.commitIndex() : "Cannot commit backwards!";
      assert commitIndex <= this.log.getLastEntryIndex() : "Cannot commit beyond current log; commitIndex=" + commitIndex + " but last log entry=" + this.log.getLastEntryIndex();
      if (commitIndex > this.commitIndex() && commitIndex <= this.log.getLastEntryIndex()) {
        log.setCommitIndex(commitIndex, now);
      }
    } finally {
      log.assertConsistency();
    }
  }


  /**
   * This creates a new configuration log entry, and appends it. Must be the current LEADER, or will throw an assert.
   *
   * @param quorum the new quorum of nodes we'd like to have in the cluster.
   * @param force force the new configuration, even through it's an error.
   * @param now The current timestamp.
   *
   * @return (index, term) of the new entry
   */
  public RaftLogEntryLocation reconfigure(Collection<String> quorum, boolean force, long now) {
    // Error checks
    log.assertConsistency();
    try {
      assert isLeader() : "Can only reconfigure as leader";
      boolean isSingletonCluster = log.latestQuorumMembers.size() == 1 && log.latestQuorumMembers.iterator().next().equals(this.serverName);
      Set<String> intersection = new HashSet<>(quorum);
      intersection.retainAll(log.getQuorumMembers());
      assert force || ((intersection.equals(new HashSet<>(quorum)) || intersection.equals(log.getQuorumMembers())) && Math.abs(quorum.size() - log.getQuorumMembers().size()) == 1)
          : "We can only add or remove a single server at a time. proposed_config=" + quorum + "  current_config=" + log.getQuorumMembers() + "  intersection=" + intersection;

      // Enter the config
      long newEntryIndex = log.getLastEntryIndex() + 1;
      EloquentRaftProto.LogEntry newLogEntry = EloquentRaftProto.LogEntry
          .newBuilder()
          .setIndex(newEntryIndex)
          .setTerm(currentTerm)
          .setType(EloquentRaftProto.LogEntryType.CONFIGURATION)
          .addAllConfiguration(quorum)
          .build();
      boolean success = log.appendEntries(log.getLastEntryIndex(), log.getLastEntryTerm(), Collections.singletonList(newLogEntry));
      assert success;
      this.matchIndex.ifPresent(map -> map.put(this.serverName, log.getLastEntryIndex()));  // we're always matched up to date

      // Special case: If we're entering a configuration with just ourselves, then immediately commit this entry
      // Addendum[gabor]: we can always remove ourselves from the quorum
      if ((isSingletonCluster && quorum.size() == 0) ||
          (quorum.size() == 1 && quorum.iterator().next().equals(this.serverName))) {
        log.setCommitIndex(newEntryIndex, now);
      }

      // Ensure that everyone in the configuration has the associated metadata
      for (String voter : quorum) {
        if (!voter.equals(serverName)) {
          nextIndex.ifPresent(map -> map.computeIfAbsent(voter, key -> log.getLastEntryIndex() + 1));
          matchIndex.ifPresent(map -> map.computeIfAbsent(voter, key -> 0L));
          lastMessageTimestamp.ifPresent(map -> map.computeIfAbsent(voter, key -> now));
        }
      }

      return new RaftLogEntryLocation(newEntryIndex, currentTerm);
    } finally {
      log.assertConsistency();
    }
  }


  /**
   * @see #reconfigure(Collection, boolean, long)
   */
  public RaftLogEntryLocation reconfigure(Collection<String> quorum, long now) {
    return reconfigure(quorum, false, now);
  }


  /**
   * Get the server to add to the cluster, if one should be added.
   * If no server should (or is available to) be added, we return {@link Optional#empty()}.
   * A server should be added if all of:
   *
   * <ol>
   *   <li>{@link #targetClusterSize} is nonnegative, and</li>
   *   <li>the current cluster size is less than {@link #targetClusterSize}.</li>
   *   <li>the new server is not delinquent (i.e., is not timed out)</li>
   * </ol>
   *
   * @param now The current time in millis. This should be a value relative to {@link #lastMessageTimestamp} -- usually
   *            the time on the transport (itself usually machine-local time).
   * @param maxLatency The maximum amount of time that we tolerate, in milliseconds, of the last heartbeat we received
   *                   from the node we're adding.
   *
   *
   * @return The server to add, or {@link Optional#empty()} if none should / can be added.
   */
  public Optional<String> serverToAdd(long now, long maxLatency) {
    log.assertConsistency();
    try {
      assert isLeader() : "Only the leader should be adding servers";
      assert lastMessageTimestamp.isPresent() : "Leader should have a lastMessageTimestamp";
      Set<String> hospice = log.stateMachine.getHospice();
      if (targetClusterSize >= 0 && log.getQuorumMembers().size() < targetClusterSize) {
        // The cluster is the wrong size
        return lastMessageTimestamp.flatMap(heartbeats -> {
          // Find the most recently responding node that's not in the quorum
          long latestResponse = 0L;
          String argmaxName = null;
          for (Map.Entry<String, Long> entry : heartbeats.entrySet()) {
            if (!log.getQuorumMembers().contains(entry.getKey()) &&
                !hospice.contains(entry.getKey()) &&
                entry.getValue() > latestResponse) {
              latestResponse = entry.getValue();
              argmaxName = entry.getKey();
            } else if (hospice.contains(entry.getKey())) {
              Theseus.log.debug("[RaftState] node is in the hospice: " + entry.getKey());
            }
          }
          // Make sure we wouldn't immediately want to remove the server
          if (argmaxName != null && !argmaxName.equals(serverName) && (now - latestResponse) < maxLatency) {
            // Return OK
            return Optional.of(argmaxName);
          } else {
            return Optional.empty();
          }
        });
      } else {
        // We don't want more servers
        return Optional.empty();
      }
    } finally {
      log.assertConsistency();
    }
  }


  /**
   * Get the server to remove from the cluster, if one should be removed.
   * If no server should be removed, we return {@link Optional#empty()}.
   * A server should be removed if both:
   *
   * <ol>
   *   <li>{@link #targetClusterSize} is nonnegative, and</li>
   *   <li>One of:
   *     <ol>
   *       <li>The current cluster is larger than {@link #targetClusterSize}, or</li>
   *       <li>There exists a server that has been down for longer than the given timeout</li>
   *     </ol>
   *   </li>
   * </ol>
   *
   * @param now The current time in millis. This should be a value relative to {@link #lastMessageTimestamp} -- usually
   *            the time on the transport (itself usually machine-local time).
   * @param timeout The maximum amount of time, in milliseconds, to keep a node in the configuration
   *                before removing it from the cluster.
   *
   * @return The server to remove, or {@link Optional#empty()} if none should be removed.
   */
  public Optional<String> serverToRemove(long now, long timeout) {
    log.assertConsistency();
    try {
      assert isLeader() : "Only the leader should be removing servers";
      assert lastMessageTimestamp.isPresent() : "Leader should have a lastMessageTimestamp";
      if (targetClusterSize >= 0 && log.getQuorumMembers().size() > targetClusterSize) {
        // The cluster is the wrong size
        return lastMessageTimestamp.flatMap(heartbeats -> {
          // Find the least recently responding non-leader node that's in the quorum
          long mostDelinquentResponse = Long.MAX_VALUE;
          String argminName = null;
          for (Map.Entry<String, Long> entry : heartbeats.entrySet()) {
            if (log.getQuorumMembers().contains(entry.getKey()) && !entry.getKey().equals(serverName) && entry.getValue() < mostDelinquentResponse) {
              mostDelinquentResponse = entry.getValue();
              argminName = entry.getKey();
            }
          }
          return Optional.ofNullable(argminName);
        });
      } else if (targetClusterSize >= 0) {
        // Check if there are any nodes that have exceeded the timeout
        return lastMessageTimestamp.flatMap(heartbeats -> {
          for (Map.Entry<String, Long> entry : heartbeats.entrySet()) {
            if (log.getQuorumMembers().contains(entry.getKey()) && !entry.getKey().equals(serverName) && (now - entry.getValue()) > timeout) {
              return Optional.of(entry.getKey());
            }
          }
          return Optional.empty();
        });
      } else {
        // We explicitly don't want to allow resizing
        return Optional.empty();
      }
    } finally {
      log.assertConsistency();
    }
  }


  /**
   * Get the set of dead nodes that we should clear transient entries for.
   * This is only relevant for a {@link KeyValueStateMachine} where the notion of ownership
   * exists.
   *
   * @param now The current time in millis. This should be a value relative to {@link #lastMessageTimestamp} -- usually
   *            the time on the transport (itself usually machine-local time).
   * @param timeout The maximum amount of time, in milliseconds, to keep a node "alive"
   *                before clearing it's transient state from the Raft state.
   *
   * @return The set of nodes to remove. After returned, they are marked as already killed.
   */
  public Set<String> killNodes(long now, long timeout) {
    // Error checks
    log.assertConsistency();
    assert isLeader() : "Can only call deadNodes as leader";
    assert lastMessageTimestamp.isPresent() : "Leader should have a lastMessageTimestamp";
    assert alreadyKilled.isPresent() : "Leader should have alreadyKilled variable";

    // Get nodes to kill
    if (log.stateMachine instanceof KeyValueStateMachine) {
      return lastMessageTimestamp.flatMap(lastMessageTimestamp ->
          alreadyKilled.map(alreadyKilled -> {
            Set<String> rtn = new HashSet<>();
            if (now > cachedOwnersTimestamp + 1000) {
              cachedOwners = log.stateMachine.owners();
              cachedOwnersTimestamp = now;
            }
            for (String owner : cachedOwners) {
              if (!owner.equals(this.serverName) &&
                  (now - lastMessageTimestamp.computeIfAbsent(owner, s -> now - timeout / 2)) > timeout &&
                  !alreadyKilled.contains(owner)) {
                rtn.add(owner);
                alreadyKilled.add(owner);
              }
            }
            return rtn;
          })
      ).orElse(Collections.emptySet());
    } else {
      return Collections.emptySet();
    }
  }


  /**
   * If we could not kill the transient state on a node, we need to revive it with this function.
   *
   * @param node The node we have revived.
   *
   * @see #killNodes(long, long)
   */
  public void revive(String node) {
    this.alreadyKilled.ifPresent( set -> set.remove(node) );
  }


  /** {@inheritDoc} */
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    RaftState raftState = (RaftState) o;
    return currentTerm == raftState.currentTerm &&
        Objects.equals(serverName, raftState.serverName) &&
        Objects.equals(log, raftState.log) &&
        Objects.equals(votedFor, raftState.votedFor) &&
        leadership == raftState.leadership &&
        Objects.equals(leader, raftState.leader) &&
        Objects.equals(nextIndex, raftState.nextIndex) &&
        Objects.equals(matchIndex, raftState.matchIndex) &&
        Objects.equals(electionTimeoutCheckpoint, raftState.electionTimeoutCheckpoint) &&
        Objects.equals(votesReceived, raftState.votesReceived) &&
        Objects.equals(alreadyKilled, raftState.alreadyKilled) &&
        Objects.equals(lastMessageTimestamp, raftState.lastMessageTimestamp);
  }


  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return Objects.hash(serverName, log, currentTerm, votedFor, leadership, nextIndex, matchIndex, lastMessageTimestamp, electionTimeoutCheckpoint, votesReceived);
  }


}
