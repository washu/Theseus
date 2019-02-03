package ai.eloquent.raft;

import ai.eloquent.util.*;
import com.google.protobuf.ByteString;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static ai.eloquent.raft.RaftLogTest.makeEntry;
import static org.junit.Assert.*;

/**
 * This is where we can put in individual test cases for EloquentRaftAlgorithm and assert certain behavior in isolation.
 */
@SuppressWarnings("OptionalGetWithoutIsPresent")
public class EloquentRaftAlgorithmTest {


  //
  // --------------------------------------------------------------------------
  // ASSERTABLE TRANSPORT
  // --------------------------------------------------------------------------
  //


  /**
   * This is here so that we can assert things that have happened on the transport
   */
  public static class AssertableTransport implements RaftTransport {
    List<BroadcastCall> broadcastCalls = new ArrayList<>();
    public static class BroadcastCall {
      String sender;
      EloquentRaftProto.RaftMessage message;

      public BroadcastCall(String sender, EloquentRaftProto.RaftMessage message) {
        this.sender = sender;
        this.message = message;
      }
    }

    List<SendCall> sendCalls = new ArrayList<>();
    public static class SendCall {
      String sender;
      String destination;
      EloquentRaftProto.RaftMessage message;

      public SendCall(String sender, String destination, EloquentRaftProto.RaftMessage message) {
        this.sender = sender;
        this.destination = destination;
        this.message = message;
      }
    }

    List<RpcCall> rpcCalls = new ArrayList<>();
    public static class RpcCall {
      String sender;
      String destination;
      EloquentRaftProto.RaftMessage message;
      Consumer<EloquentRaftProto.RaftMessage> onResponseReceived;
      boolean hasReplied = false;

      public RpcCall(String sender, String destination, EloquentRaftProto.RaftMessage message, Consumer<EloquentRaftProto.RaftMessage> onResponseReceived) {
        this.sender = sender;
        this.destination = destination;
        this.message = message;
        this.onResponseReceived = onResponseReceived;
      }
    }

    SafeTimerMock transportMockTimer = new SafeTimerMock();

    public AssertableTransport() {
      SafeTimerMock.resetMockTime();
    }

    @Override
    public void bind(RaftAlgorithm listener) {
      // Do nothing
    }

    /** {@inheritDoc} */
    @Override
    public boolean threadsCanBlock() {
      return true;
    }

    @Override
    public Collection<RaftAlgorithm> boundAlgorithms() {
      return Collections.emptySet();
    }

    @Override
    public void rpcTransport(String sender, String destination, EloquentRaftProto.RaftMessage message, Consumer<EloquentRaftProto.RaftMessage> onResponseReceived, Runnable onTimeout, long timeout) {
      this.rpcCalls.add(new RpcCall(sender, destination, message, onResponseReceived));
    }

    @Override
    public void sendTransport(String sender, String destination, EloquentRaftProto.RaftMessage message) {
      this.sendCalls.add(new SendCall(sender, destination, message));
    }

    @Override
    public void broadcastTransport(String sender, EloquentRaftProto.RaftMessage message) {
      this.broadcastCalls.add(new BroadcastCall(sender, message));
    }

    @Override
    public Span expectedNetworkDelay() {
      return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void scheduleHeartbeat(Supplier<Boolean> alive, long period, Runnable heartbeatFn, Logger log) {
      this.scheduleAtFixedRate(new SafeTimerTask() {
        @SuppressWarnings("Duplicates")
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
      transportMockTimer.schedule(task, delay);
    }

    @Override
    public long now() {
      return transportMockTimer.now();
    }

    @Override
    public long nowNanos() {
      return transportMockTimer.now() * 1000000;
    }

    @Override
    public void sleep(long millis, int nanos) {
      SafeTimerMock.advanceTime(millis);

    }

    @Override
    public void stop() {
      transportMockTimer.cancel();
    }
  }


  //
  // --------------------------------------------------------------------------
  // HELPERS
  // --------------------------------------------------------------------------
  //


  /**
   * Set up a default raft algorithm instance.
   *
   * <p>
   *   This will have a quorum of 5 nodes: L, A, B, C, and D.
   *   We will be returning L, which is the leader.
   * </p>
   *
   * <p>
   *   We will also have some values populated in the log.
   *   The log will have length 5 (i.e., the last index is 5).
   *   The log will have term 2.
   *   The state machine will have a most recent value of 3
   * </p>
   */
  private EloquentRaftAlgorithm mkAlgorithm() {
    // Set up the algorithm
    SingleByteStateMachine stateMachine = new SingleByteStateMachine();
    RaftLog raftLog = new RaftLog(stateMachine, Arrays.asList("L", "A", "B", "C", "D"), Executors.newSingleThreadExecutor());
    RaftState raftState = new RaftState("L", raftLog);
    AssertableTransport transport = new AssertableTransport();
    transport.sendCalls.clear();

    // Set up the log
    List<EloquentRaftProto.LogEntry> appendEntries = Arrays.asList(
        makeEntry(1, 1, 3),
        makeEntry(2, 1, 4),
        makeEntry(3, 1, 5),
        makeEntry(4, 2, 1),
        makeEntry(5, 2, 3)
    );
    boolean success = raftLog.appendEntries(0, 0, appendEntries);
    assertTrue(success);

    // Elect
    raftState.setCurrentTerm(2);
    raftState.voteFor("L");
    raftState.receiveVoteFrom("L");
    raftState.receiveVoteFrom("A");
    raftState.receiveVoteFrom("B");
    raftState.elect(0L);

    // Sanity check the algorithm
    assertTrue("We should be the leader", raftState.isLeader());
    assertEquals(5, raftLog.getLastEntryIndex());
    assertEquals(2, raftLog.getLastEntryTerm());
    assertEquals("Nothing should be committed to the state machine yet",
        -1, stateMachine.value);

    // Return
    return new EloquentRaftAlgorithm(raftState, transport, Optional.empty());
  }


  //
  // --------------------------------------------------------------------------
  // APPEND ENTRIES
  // --------------------------------------------------------------------------
  //


  /**
   * A short little helper for calling the append entries RPC from unit tests.
   * Note that the "leader" is always 'A' here.
   */
  private EloquentRaftProto.AppendEntriesReply appendEntries(EloquentRaftAlgorithm algorithm, Consumer<EloquentRaftProto.AppendEntriesRequest.Builder> message) {
    Pointer<EloquentRaftProto.AppendEntriesReply> replyPointer = new Pointer<>();
    EloquentRaftProto.AppendEntriesRequest.Builder request = EloquentRaftProto.AppendEntriesRequest.newBuilder()
        .setLeaderName("A")  // always from 'A' as the leader
        ;
    message.accept(request);
    algorithm.receiveAppendEntriesRPC(request.build(), x -> replyPointer.set(x.getAppendEntriesReply()));
    return replyPointer.dereference().orElseThrow(() -> new AssertionError("Should have received a reply from requestVote"));
  }


  /**
   * Test a simple heartbeat happy path
   */
  @Test
  public void appendEntriesHeartbeat() {
    // Set up our algorithm
    EloquentRaftAlgorithm algorithm = mkAlgorithm();
    algorithm.mutableState().stepDownFromElection(2, 0L);  // appendEntries only makes sense as a follower

    // Submit a heartbeat (no entries)
    EloquentRaftProto.AppendEntriesReply reply = appendEntries(algorithm, request -> request
        .setTerm(2)             // our term
        .setPrevLogIndex(5)     // our actual last index
        .setPrevLogTerm(2)      // the term of the last index
        .setLeaderCommit(0)     // we have not committed anything yet
    );

    // Assertions
    assertTrue("The reply should be successful", reply.getSuccess());
    assertEquals("The reply should have term 2", 2, reply.getTerm());
    assertEquals("The reply should be requesting our next index", 6, reply.getNextIndex());
    assertEquals("The reply should be from us", "L", reply.getFollowerName());
    assertEquals("We should not have committed anything", 0, algorithm.mutableState().commitIndex());
  }


  /**
   * Test a simple one entry append
   */
  @Test
  public void appendEntriesSingleEntryHappyPath() {
    // Set up our algorithm
    EloquentRaftAlgorithm algorithm = mkAlgorithm();
    algorithm.mutableState().stepDownFromElection(2, 0L);  // appendEntries only makes sense as a follower

    // Submit a heartbeat (one entry)
    EloquentRaftProto.AppendEntriesReply reply = appendEntries(algorithm, request -> request
        .setTerm(2)             // our term
        .setPrevLogIndex(5)     // our actual last index
        .setPrevLogTerm(2)      // the term of the last index
        .setLeaderCommit(0)     // we have not committed anything yet
        .addEntry(RaftLogTest.makeEntry(6, 2, 42))
    );

    // Assertions
    assertTrue("The reply should be successful", reply.getSuccess());
    assertEquals("The reply should have term 2", 2, reply.getTerm());
    assertEquals("The reply should be requesting our next index", 7, reply.getNextIndex());
    assertEquals("The reply should be from us", "L", reply.getFollowerName());
    assertEquals("We should not have committed anything", 0, algorithm.mutableState().commitIndex());
  }


  /**
   * Test a simple multiple entry append
   */
  @Test
  public void appendEntriesMultiEntryHappyPath() {
    // Set up our algorithm
    EloquentRaftAlgorithm algorithm = mkAlgorithm();
    algorithm.mutableState().stepDownFromElection(2, 0L);  // appendEntries only makes sense as a follower

    // Submit a heartbeat (multiple entries)
    EloquentRaftProto.AppendEntriesReply reply = appendEntries(algorithm, request -> request
        .setTerm(2)             // our term
        .setPrevLogIndex(5)     // our actual last index
        .setPrevLogTerm(2)      // the term of the last index
        .setLeaderCommit(0)     // we have not committed anything yet
        .addEntry(RaftLogTest.makeEntry(6, 2, 42))
        .addEntry(RaftLogTest.makeEntry(7, 2, 43))
        .addEntry(RaftLogTest.makeEntry(8, 2, 44))
    );

    // Assertions
    assertTrue("The reply should be successful", reply.getSuccess());
    assertEquals("The reply should have term 2", 2, reply.getTerm());
    assertEquals("The reply should be requesting our next index", 9, reply.getNextIndex());
    assertEquals("The reply should be from us", "L", reply.getFollowerName());
    assertEquals("We should not have committed anything", 0, algorithm.mutableState().commitIndex());
  }


  /**
   * Send a heartbeat that signals a commit
   */
  @Test
  public void appendEntriesHeartbeatCommit() {
    // Set up our algorithm
    EloquentRaftAlgorithm algorithm = mkAlgorithm();
    algorithm.mutableState().stepDownFromElection(2, 0L);  // appendEntries only makes sense as a follower

    // Submit a heartbeat (no entries)
    EloquentRaftProto.AppendEntriesReply reply = appendEntries(algorithm, request -> request
        .setTerm(2)             // our term
        .setPrevLogIndex(5)     // our actual last index
        .setPrevLogTerm(2)      // the term of the last index
        .setLeaderCommit(2)     // commit through index 2
    );

    // Assertions
    assertTrue("The reply should be successful", reply.getSuccess());
    assertEquals("The reply should have term 2", 2, reply.getTerm());
    assertEquals("The reply should be requesting our next index", 6, reply.getNextIndex());
    assertEquals("We should have committed to 2", 2, algorithm.mutableState().commitIndex());
    assertEquals("Our state machine should have the correct value (the one @ index=2)",
        4, ((SingleByteStateMachine) algorithm.mutableState().log.stateMachine).value);

  }


  /**
   * Send a heartbeat that both appends and signals a commit
   */
  @Test
  public void appendEntriesHeartbeatAppendAndCommit() {
    // Set up our algorithm
    EloquentRaftAlgorithm algorithm = mkAlgorithm();
    algorithm.mutableState().stepDownFromElection(2, 0L);  // appendEntries only makes sense as a follower

    // Submit a heartbeat (one entry)
    EloquentRaftProto.AppendEntriesReply reply = appendEntries(algorithm, request -> request
        .setTerm(2)             // our term
        .setPrevLogIndex(5)     // our actual last index
        .setPrevLogTerm(2)      // the term of the last index
        .setLeaderCommit(2)     // commit through index 2
        .addEntry(RaftLogTest.makeEntry(6, 2, 42))
    );

    // Assertions
    assertTrue("The reply should be successful", reply.getSuccess());
    assertEquals("The reply should have term 2", 2, reply.getTerm());
    assertEquals("The reply should be requesting our next index", 7, reply.getNextIndex());
    assertEquals("We should have committed to 2", 2, algorithm.mutableState().commitIndex());
    assertEquals("Our state machine should have the correct value (the one @ index=2)",
        4, ((SingleByteStateMachine) algorithm.mutableState().log.stateMachine).value);
  }


  /**
   * Ensure that we return false to RPCs from servers with an out-of-date term
   */
  @Test
  public void appendEntriesFailIfOldTerm() {
    // Set up our algorithm
    EloquentRaftAlgorithm algorithm = mkAlgorithm();
    algorithm.mutableState().stepDownFromElection(2, 0L);  // appendEntries only makes sense as a follower

    // Submit a heartbeat (with entries)
    EloquentRaftProto.AppendEntriesReply reply = appendEntries(algorithm, request -> request
        .setTerm(1)             // an old term
        .setPrevLogIndex(5)     // our actual last index
        .setPrevLogTerm(2)      // the term of the last index
        .setLeaderCommit(2)     // commit through index 2
        .addEntry(RaftLogTest.makeEntry(6, 2, 42))
    );

    // Assertions
    assertFalse("The reply should NOT be successful", reply.getSuccess());
    assertEquals("The reply should have term 2", 2, reply.getTerm());
    assertEquals("The reply should be requesting the old next index", 6, reply.getNextIndex());
    assertEquals("We should NOT have committed", 0, algorithm.mutableState().commitIndex());
  }


  /**
   * Ensure that we return false if the term on our last entry doesn't match that
   * of the request (is less than).
   */
  @Test
  public void appendEntriesFailIfOldEntryTerm() {
    // Set up our algorithm
    EloquentRaftAlgorithm algorithm = mkAlgorithm();
    algorithm.mutableState().stepDownFromElection(2, 0L);  // appendEntries only makes sense as a follower

    // Submit a heartbeat
    EloquentRaftProto.AppendEntriesReply reply = appendEntries(algorithm, request -> request
        .setTerm(2)             // our term
        .setPrevLogIndex(5)     // our actual last index
        .setPrevLogTerm(1)      // an old term for this entry
    );

    // Assertions
    assertFalse("The reply should NOT be successful", reply.getSuccess());
    assertEquals("The reply should be re-requesting the bad entry", 5, reply.getNextIndex());
  }


  /**
   * Ensure that we return false if the term on our last entry doesn't match that
   * of the request (is greater than).
   */
  @Test
  public void appendEntriesFailIfNewEntryTerm() {
    // Set up our algorithm
    EloquentRaftAlgorithm algorithm = mkAlgorithm();
    algorithm.mutableState().stepDownFromElection(2, 0L);  // appendEntries only makes sense as a follower

    // Submit a heartbeat
    EloquentRaftProto.AppendEntriesReply reply = appendEntries(algorithm, request -> request
        .setTerm(2)             // our term
        .setPrevLogIndex(5)     // our actual last index
        .setPrevLogTerm(3)      // a newer term for this entry
    );

    // Assertions
    assertFalse("The reply should NOT be successful", reply.getSuccess());
    assertEquals("The reply should be re-requesting the bad entry", 5, reply.getNextIndex());
  }


  /**
   * Ensure that we return false if the term on our last entry doesn't match that
   * of the request, and we have entries to append.
   * This is a trivial variant of {@link #appendEntriesFailIfNewEntryTerm()}
   */
  @Test
  public void appendEntriesFailIfWrongEntryTermWithEntries() {
    // Set up our algorithm
    EloquentRaftAlgorithm algorithm = mkAlgorithm();
    algorithm.mutableState().stepDownFromElection(2, 0L);  // appendEntries only makes sense as a follower

    // Submit a heartbeat (with entries)
    EloquentRaftProto.AppendEntriesReply reply = appendEntries(algorithm, request -> request
        .setTerm(2)             // our term
        .setPrevLogIndex(5)     // our actual last index
        .setPrevLogTerm(1)      // an old term for this entry
        .addEntry(RaftLogTest.makeEntry(6, 2, 42))
    );

    // Assertions
    assertFalse("The reply should NOT be successful", reply.getSuccess());
    assertEquals("The reply should be re-requesting the bad entry", 5, reply.getNextIndex());
  }


  /**
   * Ensure that an append entries request overwrites current entries if
   * necessary (step 3 of the RPC spec)
   */
  @Test
  public void appendEntriesOverwriteLog() {
    // Set up our algorithm
    EloquentRaftAlgorithm algorithm = mkAlgorithm();
    algorithm.mutableState().stepDownFromElection(2, 0L);  // appendEntries only makes sense as a follower

    // Submit a heartbeat (with entries)
    EloquentRaftProto.AppendEntriesReply reply = appendEntries(algorithm, request -> request
        .setTerm(2)             // our term
        .setPrevLogIndex(2)     // in the middle of our log
        .setPrevLogTerm(1)      // the term at that index in our log
        .addEntry(RaftLogTest.makeEntry(3, 3, 42))  // a new entry, with greater term
    );

    // Assertions
    assertTrue("The reply should be successful", reply.getSuccess());
    assertEquals("The reply should be requesting from our next entry", 4, reply.getNextIndex());
    assertEquals("The log should be of length 3", 3, algorithm.mutableState().log.logEntries.size());
  }


  /**
   * Ensure that an append entries request overwrites current entries if
   * necessary (step 3 of the RPC spec), even if the entries its overwriting
   * are in the same term
   */
  @Test
  public void appendEntriesOverwriteLogSameTerm() {
    // Set up our algorithm
    EloquentRaftAlgorithm algorithm = mkAlgorithm();
    algorithm.mutableState().stepDownFromElection(2, 0L);  // appendEntries only makes sense as a follower

    // Submit a heartbeat (with entries)
    EloquentRaftProto.AppendEntriesReply reply = appendEntries(algorithm, request -> request
        .setTerm(2)             // our term
        .setPrevLogIndex(2)     // in the middle of our log
        .setPrevLogTerm(1)      // the term at that index in our log
        .addEntry(RaftLogTest.makeEntry(3, 1, 5))  // the same entry as is already there
    );

    // Assertions
    assertTrue("The reply should be successful", reply.getSuccess());
    assertEquals("It's fishy, but nothing has been contradicted yet, so the log remains intact", 6, reply.getNextIndex());
    assertEquals("The log should have its original length", 5, algorithm.mutableState().log.logEntries.size());
  }


  /**
   * Ensure that we commit monotonically.
   */
  @Test
  public void appendEntriesMonotonicCommit() {
    // Set up our algorithm
    EloquentRaftAlgorithm algorithm = mkAlgorithm();
    algorithm.mutableState().stepDownFromElection(2, 0L);  // appendEntries only makes sense as a follower
    algorithm.mutableState().commitUpTo(4, 0L);

    // Submit a heartbeat (with entries)
    EloquentRaftProto.AppendEntriesReply reply = appendEntries(algorithm, request -> request
        .setTerm(2)             // our term
        .setPrevLogIndex(5)     // our actual last index
        .setPrevLogTerm(2)      // the term of the last index
        .setLeaderCommit(2)     // commit through index 2 (this should fail, we've committed through 4)
    );

    // Assertions
    assertTrue("The reply should be successful", reply.getSuccess());
    assertEquals("We should still be committed through 4", 4, algorithm.mutableState().commitIndex());
    assertEquals("Our state machine should have the correct value (the one @ index=4)",
        1, ((SingleByteStateMachine) algorithm.mutableState().log.stateMachine).value);
  }


  /**
   * Ensure that we can reconfigure the cluster by appending entries.
   */
  @Test
  public void appendEntriesReconfigureCluster() {
    // Setup
    EloquentRaftAlgorithm algorithm = mkAlgorithm();
    algorithm.mutableState().stepDownFromElection(2, 0L);  // appendEntries only makes sense as a follower

    // Change the cluster
    EloquentRaftProto.AppendEntriesReply reply = appendEntries(algorithm, request -> request
        .setTerm(2)             // our term
        .setPrevLogIndex(5)     // our actual last index
        .setPrevLogTerm(2)      // the term of the last index
        .setLeaderCommit(0)     // we have not committed anything yet
        .addEntry(RaftLogTest.makeConfigurationEntry(6, 2, Collections.singletonList("L")))
    );

    // Assertions
    assertTrue("The reply should be successful", reply.getSuccess());
    assertEquals("We should see the entry's quorum as the latest quorum",
        Collections.singleton("L"), algorithm.mutableState().log.latestQuorumMembers);
    assertEquals("We should see the existing quorum as the committed quorum",
        new HashSet<>(Arrays.asList("L", "A", "B", "C", "D")), algorithm.mutableState().log.committedQuorumMembers);

  }


  /**
   * We should step down from an election if we get a heartbeat from another
   * server in a higher term.
   */
  @Test
  public void appendEntriesCancelsElectionHigherTerm() {
    // Set up our algorithm
    EloquentRaftAlgorithm algorithm = mkAlgorithm();
    algorithm.mutableState().stepDownFromElection(2, 0L);
    algorithm.convertToCandidate();
    assertEquals("Reminder: we are now on term 3", 3, algorithm.mutableState().currentTerm);

    // Submit a heartbeat (no entries)
    EloquentRaftProto.AppendEntriesReply reply = appendEntries(algorithm, request -> request
        .setTerm(4)             // a higher term.
        .setPrevLogIndex(5)     // our actual last index
        .setPrevLogTerm(2)      // the term of the last index
        .setLeaderCommit(0)     // we have not committed anything yet
    );

    // Assertions
    assertTrue("The reply should be successful", reply.getSuccess());
    assertEquals("We should no longer be a candidate", RaftState.LeadershipStatus.OTHER, algorithm.mutableState().leadership);
  }


  /**
   * We should step down from an election if we get a heartbeat from another
   * server in our same term.
   */
  @Test
  public void appendEntriesCancelsElectionSameTerm() {
    // Set up our algorithm
    EloquentRaftAlgorithm algorithm = mkAlgorithm();
    algorithm.mutableState().stepDownFromElection(2, 0L);
    algorithm.convertToCandidate();
    assertEquals("Reminder: we are now on term 3", 3, algorithm.mutableState().currentTerm);

    // Submit a heartbeat (no entries)
    EloquentRaftProto.AppendEntriesReply reply = appendEntries(algorithm, request -> request
        .setTerm(3)             // our current term.
        .setPrevLogIndex(5)     // our actual last index
        .setPrevLogTerm(2)      // the term of the last index
        .setLeaderCommit(0)     // we have not committed anything yet
    );

    // Assertions
    assertTrue("The reply should be successful", reply.getSuccess());
    assertEquals("We should no longer be a candidate", RaftState.LeadershipStatus.OTHER, algorithm.mutableState().leadership);
  }


  /**
   * We should step down from leadership if we get a heartbeat from another
   * server in a higher term.
   */
  @Test
  public void appendEntriesCancelsLeadershipHigherTerm() {
    // Set up our algorithm
    EloquentRaftAlgorithm algorithm = mkAlgorithm();

    // Submit a heartbeat (no entries)
    EloquentRaftProto.AppendEntriesReply reply = appendEntries(algorithm, request -> request
        .setTerm(3)             // a higher term.
        .setPrevLogIndex(5)     // our actual last index
        .setPrevLogTerm(2)      // the term of the last index
        .setLeaderCommit(0)     // we have not committed anything yet
    );

    // Assertions
    assertTrue("The reply should be successful", reply.getSuccess());
    assertEquals("We should no longer be a leader", RaftState.LeadershipStatus.OTHER, algorithm.mutableState().leadership);
  }


  /**
   * We should step down from leadership if we get a heartbeat from another
   * server in our same term.
   *
   * <p>
   *   Note that this should be impossible!
   *   This test is here for split-brain detection.
   * </p>
   */
  @Test
  public void appendEntriesCancelsLeadershipSameTerm() {
    // Set up our algorithm
    EloquentRaftAlgorithm algorithm = mkAlgorithm();

    // Receive our vote
    EloquentRaftProto.AppendEntriesReply reply = appendEntries(algorithm, request -> request
        .setTerm(2)             // our current term.
        .setPrevLogIndex(5)     // our actual last index
        .setPrevLogTerm(2)      // the term of the last index
        .setLeaderCommit(0)     // we have not committed anything yet
    );

    // Assertions
    assertTrue("The reply should be successful", reply.getSuccess());
    assertEquals("We should no longer be a leader", RaftState.LeadershipStatus.OTHER, algorithm.mutableState().leadership);
  }


  //
  // --------------------------------------------------------------------------
  // APPEND ENTRIES REPLY
  // --------------------------------------------------------------------------
  //


  /**
   * Try a reply from a server missing all logs
   */
  @Test
  public void appendEntriesReplyAllEntriesMissing() {
    // Setup
    EloquentRaftAlgorithm algorithm = mkAlgorithm();
    AssertableTransport transport = (AssertableTransport) algorithm.transport;

    // Receive the reply
    EloquentRaftProto.AppendEntriesReply reply = EloquentRaftProto.AppendEntriesReply.newBuilder()
        .setSuccess(false)
        .setNextIndex(1)  // this is the first index
        .setTerm(2)
        .setFollowerName("A")
        .build();
    algorithm.receiveAppendEntriesReply(reply);

    // Check the assertions
    // 1. We should have sent a follow up message
    assertEquals("The transport should have sent out a single call",
        1, transport.sendCalls.size());
    assertEquals("The transport should be replying to the server that sent the reply",
        "A", transport.sendCalls.get(0).destination);
    assertEquals("The message being sent should be an install snapshot request (TODO why?)",
        EloquentRaftProto.RaftMessage.ContentsCase.APPENDENTRIES, transport.sendCalls.get(0).message.getContentsCase());
    // 2. The message should be correct
    EloquentRaftProto.AppendEntriesRequest appendEntriesRequest = transport.sendCalls.get(0).message.getAppendEntries();
    assertEquals("We should be sending all the new entries",
        5, appendEntriesRequest.getEntryCount());
    assertEquals("We should be starting with entry 1",
        1, appendEntriesRequest.getEntry(0).getIndex());
    assertEquals("We should be ending with entry 5",
        5, appendEntriesRequest.getEntry(4).getIndex());
  }


  /**
   * Try a reply from a server missing some logs
   */
  @Test
  public void appendEntriesReplySomeEntriesMissing() {
    // Setup
    EloquentRaftAlgorithm algorithm = mkAlgorithm();
    AssertableTransport transport = (AssertableTransport) algorithm.transport;

    // Receive the reply
    EloquentRaftProto.AppendEntriesReply reply = EloquentRaftProto.AppendEntriesReply.newBuilder()
        .setSuccess(false)
        .setNextIndex(3)  // start at index 3 (the third element)
        .setTerm(2)
        .setFollowerName("A")
        .build();
    algorithm.receiveAppendEntriesReply(reply);

    // Check the assertions
    // 1. We should have sent a follow up message
    assertEquals("The transport should have sent out a single call",
        1, transport.sendCalls.size());
    assertEquals("The transport should be replying to the server that sent the reply",
        "A", transport.sendCalls.get(0).destination);
    assertEquals("The message being sent should be an install snapshot request (TODO why?)",
        EloquentRaftProto.RaftMessage.ContentsCase.APPENDENTRIES, transport.sendCalls.get(0).message.getContentsCase());
    // 2. The message should be correct
    EloquentRaftProto.AppendEntriesRequest appendEntriesRequest = transport.sendCalls.get(0).message.getAppendEntries();
    assertEquals("We should be sending only the new entries",
        3, appendEntriesRequest.getEntryCount());
    assertEquals("We should be starting with entry 1",
        3, appendEntriesRequest.getEntry(0).getIndex());
    assertEquals("We should be ending with entry 5",
        5, appendEntriesRequest.getEntry(2).getIndex());
  }


  /**
   * Try a reply from a server missing only one entry
   */
  @Test
  public void appendEntriesReplyOneEntryMissing() {
    // Setup
    EloquentRaftAlgorithm algorithm = mkAlgorithm();
    AssertableTransport transport = (AssertableTransport) algorithm.transport;

    // Receive the reply
    EloquentRaftProto.AppendEntriesReply reply = EloquentRaftProto.AppendEntriesReply.newBuilder()
        .setSuccess(false)
        .setNextIndex(5)  // only missing one entry
        .setTerm(2)
        .setFollowerName("A")
        .build();
    algorithm.receiveAppendEntriesReply(reply);

    // Check the assertions
    // 1. We should have sent a follow up message
    assertEquals("The transport should have sent out a single call",
        1, transport.sendCalls.size());
    assertEquals("The transport should be replying to the server that sent the reply",
        "A", transport.sendCalls.get(0).destination);
    assertEquals("The message being sent should be an install snapshot request (TODO why?)",
        EloquentRaftProto.RaftMessage.ContentsCase.APPENDENTRIES, transport.sendCalls.get(0).message.getContentsCase());
    // 2. The message should be correct
    EloquentRaftProto.AppendEntriesRequest appendEntriesRequest = transport.sendCalls.get(0).message.getAppendEntries();
    assertEquals("We should be sending only the one missing entry",
        1, appendEntriesRequest.getEntryCount());
    assertEquals("We should be sending the most recent entry",
        5, appendEntriesRequest.getEntry(0).getIndex());
  }


  /**
   * Try a reply from a server missing no entries
   */
  @Test
  public void appendEntriesReplyNoEntriesMissing() {
    // Setup
    EloquentRaftAlgorithm algorithm = mkAlgorithm();
    AssertableTransport transport = (AssertableTransport) algorithm.transport;

    // Receive the reply
    EloquentRaftProto.AppendEntriesReply reply = EloquentRaftProto.AppendEntriesReply.newBuilder()
        .setSuccess(false)
        .setNextIndex(6)  // no missing entries
        .setTerm(2)
        .setFollowerName("A")
        .build();
    algorithm.receiveAppendEntriesReply(reply);

    // Check the assertions
    assertEquals("The transport should not have sent out any messages",
        0, transport.sendCalls.size());
  }


  /**
   * Try a reply from a server missing all logs.
   * This is similar to {@link #appendEntriesReplyAllEntriesMissing()}, but the leader
   * has already created a snapshot past the requested index
   */
  @Test
  public void appendEntriesReplyNextIndexBehindSnapshot() {
    // Setup
    EloquentRaftAlgorithm algorithm = mkAlgorithm();
    AssertableTransport transport = (AssertableTransport) algorithm.transport;

    // KEY DIFFERENCE: Assume leader has a snapshot at index 2
    RaftLog.Snapshot snapshot = new RaftLog.Snapshot(new SingleByteStateMachine(4).serialize(), 2, 1, new ArrayList<>());
    algorithm.mutableState().log.installSnapshot(snapshot, 0L);

    // Receive the reply
    EloquentRaftProto.AppendEntriesReply reply = EloquentRaftProto.AppendEntriesReply.newBuilder()
        .setSuccess(false)
        .setNextIndex(1)  // this is older than the snapshot index!
        .setTerm(2)
        .setFollowerName("A")
        .build();
    algorithm.receiveAppendEntriesReply(reply);

    // Check the assertions
    // 1. We should have sent a snapshot
    assertEquals("The transport should have sent out a single call",
        1, transport.sendCalls.size());
    assertEquals("The transport should be replying to the server that sent the reply",
        "A", transport.sendCalls.get(0).destination);
    assertEquals("The message being sent should be an install snapshot request (TODO why?)",
        EloquentRaftProto.RaftMessage.ContentsCase.INSTALLSNAPSHOT, transport.sendCalls.get(0).message.getContentsCase());
    // 2. The snapshot should be correct
    EloquentRaftProto.InstallSnapshotRequest installSnapshotRequest = transport.sendCalls.get(0).message.getInstallSnapshot();
    assertEquals("The snapshot should be up to index 2",
        2, installSnapshotRequest.getLastIndex());
    assertEquals("The snapshot should be on term 1",
        1, installSnapshotRequest.getLastTerm());
    assertEquals("The snapshot's term should match the algorithm's term",
        algorithm.term(), installSnapshotRequest.getTerm());
  }


  //
  // --------------------------------------------------------------------------
  // REQUEST VOTE
  // --------------------------------------------------------------------------
  //


  /**
   * A short little helper for calling the request vote RPC from unit tests.
   * Note that we're always voting for server 'A' here.
   */
  private EloquentRaftProto.RequestVoteReply requestVote(EloquentRaftAlgorithm algorithm, Consumer<EloquentRaftProto.RequestVoteRequest.Builder> message) {
    Pointer<EloquentRaftProto.RequestVoteReply> replyPointer = new Pointer<>();
    EloquentRaftProto.RequestVoteRequest.Builder request = EloquentRaftProto.RequestVoteRequest.newBuilder()
        .setCandidateName("A")  // always voting for server A
        ;
    message.accept(request);
    algorithm.receiveRequestVoteRPC(request.build(), x -> replyPointer.set(x.getRequestVotesReply()));
    return replyPointer.dereference().orElseThrow(() -> new AssertionError("Should have received a reply from requestVote"));
  }


  /**
   * Ensure that our initial setup is correct for the future request vote tests.
   */
  @Test
  public void requestVoteLeaderSetupIsCorrect() {
    EloquentRaftAlgorithm algorithm = mkAlgorithm();
    assertEquals("We should remember that we voted for ourselves", Optional.of("L"), algorithm.mutableState().votedFor);
    assertEquals("We should remember who voted for us",
        new HashSet<>(Arrays.asList("L", "A", "B")), algorithm.mutableState().votesReceived);
  }


  /**
   * Test the happy path of a vote being granted.
   */
  @Test
  public void requestVoteHappyPath() {
    EloquentRaftAlgorithm algorithm = mkAlgorithm();
    EloquentRaftProto.RequestVoteReply reply = requestVote(algorithm, request -> request
        .setTerm(3)          // 1 more than our current term
        .setLastLogIndex(5)  // most recent index
        .setLastLogTerm(2)   // most recent term
    );

    assertTrue("The follower should have granted the vote", reply.getVoteGranted());
    assertEquals("The follower should be on the new term", 3, algorithm.mutableState().currentTerm);
    assertEquals("The follower should be a follower (used to be a leader)", RaftState.LeadershipStatus.OTHER, algorithm.mutableState().leadership);
    assertFalse("We should no longer have a matchIndex", algorithm.mutableState().matchIndex.isPresent());
    assertFalse("We should no longer have a nextIndex", algorithm.mutableState().nextIndex.isPresent());
  }


  /**
   * Test that we can get elected with a term far in the future.
   */
  @Test
  public void requestVoteJumpTerms() {
    EloquentRaftAlgorithm algorithm = mkAlgorithm();
    EloquentRaftProto.RequestVoteReply reply = requestVote(algorithm, request -> request
        .setTerm(42)         // many more than our current term
        .setLastLogIndex(5)  // most recent index
        .setLastLogTerm(2)   // most recent term
    );

    assertTrue("The follower should have granted the vote", reply.getVoteGranted());
    assertEquals("The follower should be on the new term", 42, algorithm.mutableState().currentTerm);
    assertEquals("The follower should be a follower (used to be a leader)", RaftState.LeadershipStatus.OTHER, algorithm.mutableState().leadership);
  }


  /**
   * Test that we don't grant a vote if someone requests it with an old term.
   */
  @Test
  public void requestVoteFailOnOldTerm() {
    EloquentRaftAlgorithm algorithm = mkAlgorithm();
    EloquentRaftProto.RequestVoteReply reply = requestVote(algorithm, request -> request
        .setTerm(1)          // A candidate with an older term is requesting a vote
        .setLastLogIndex(5)  // most recent index
        .setLastLogTerm(2)   // most recent term
    );

    assertFalse("The follower should NOT have granted the vote", reply.getVoteGranted());
    assertEquals("We should maintain our term", 2, algorithm.mutableState().currentTerm);
    assertEquals("We should still be the leader", RaftState.LeadershipStatus.LEADER, algorithm.mutableState().leadership);
  }


  /**
   * Test that we don't grant a vote if someone requests it with an old term.
   */
  @Test
  public void requestVoteFailOnSameTerm() {
    EloquentRaftAlgorithm algorithm = mkAlgorithm();
    assertTrue("Reminder: we are the leader (i.e., already elected) at the start of the test", algorithm.mutableState().isLeader());
    EloquentRaftProto.RequestVoteReply reply = requestVote(algorithm, request -> request
        .setTerm(2)          // A candidate with our same term is requesting a vote
        .setLastLogIndex(5)  // most recent index
        .setLastLogTerm(2)   // most recent term
    );

    assertFalse("The follower should NOT have granted the vote", reply.getVoteGranted());
    assertEquals("We should maintain our term", 2, algorithm.mutableState().currentTerm);
    assertEquals("We should still be the leader", RaftState.LeadershipStatus.LEADER, algorithm.mutableState().leadership);
  }


  /**
   * Test that we don't grant a vote if the requester has a log that's not up to date with ours
   * in that they have an old index.
   */
  @Test
  public void requestVoteFailIfOldLogIndex() {
    EloquentRaftAlgorithm algorithm = mkAlgorithm();
    assertTrue("Reminder: we are the leader (i.e., already elected) at the start of the test", algorithm.mutableState().isLeader());
    EloquentRaftProto.RequestVoteReply reply = requestVote(algorithm, request -> request
        .setTerm(2)          // A candidate with our same term is requesting a vote
        .setLastLogIndex(4)  // less than the most recent index (5)
        .setLastLogTerm(2)   // most recent term
    );

    assertFalse("The follower should NOT have granted the vote", reply.getVoteGranted());
    assertEquals("We should maintain our term", 2, algorithm.mutableState().currentTerm);
    assertEquals("We should still be the leader", RaftState.LeadershipStatus.LEADER, algorithm.mutableState().leadership);
  }


  /**
   * Test that we don't grant a vote if the requester has a log that's not up to date with ours
   * in that they have an old term on their most recent index.
   */
  @Test
  public void requestVoteFailIfOldLogTerm() {
    EloquentRaftAlgorithm algorithm = mkAlgorithm();
    assertTrue("Reminder: we are the leader (i.e., already elected) at the start of the test", algorithm.mutableState().isLeader());
    EloquentRaftProto.RequestVoteReply reply = requestVote(algorithm, request -> request
        .setTerm(2)          // A candidate with our same term is requesting a vote
        .setLastLogIndex(5)  // most recent index
        .setLastLogTerm(1)   // old term on that index
    );

    assertFalse("The follower should NOT have granted the vote", reply.getVoteGranted());
    assertEquals("We should maintain our term", 2, algorithm.mutableState().currentTerm);
    assertEquals("We should still be the leader", RaftState.LeadershipStatus.LEADER, algorithm.mutableState().leadership);
  }


  /**
   * Test that we can vote for the same person multiple times on different terms.
   */
  @Test
  public void requestVoteElectSameServerTwice() {
    EloquentRaftAlgorithm algorithm = mkAlgorithm();

    // Vote the first time
    EloquentRaftProto.RequestVoteReply reply = requestVote(algorithm, request -> request
        .setTerm(3)          // A candidate with our same term is requesting a vote
        .setLastLogIndex(5)  // most recent index
        .setLastLogTerm(1)   // old term on that index
    );
    assertTrue("The follower should have granted the vote", reply.getVoteGranted());

    // Vote the second time
    EloquentRaftProto.RequestVoteReply secondReply = requestVote(algorithm, request -> request
        .setTerm(4)          // A candidate with our same term is requesting a vote
        .setLastLogIndex(5)  // most recent index
        .setLastLogTerm(1)   // old term on that index
    );
    assertTrue("The follower should grant the same vote again", secondReply.getVoteGranted());
    assertEquals("The follower should be on the new term", 4, algorithm.mutableState().currentTerm);
  }


  /**
   * Test that we can vote for the same person multiple times on the same term.
   */
  @Test
  public void requestVoteAllowDuplicateVotes() {
    EloquentRaftAlgorithm algorithm = mkAlgorithm();

    // Vote the first time
    EloquentRaftProto.RequestVoteReply reply = requestVote(algorithm, request -> request
        .setTerm(3)          // A candidate with our same term is requesting a vote
        .setLastLogIndex(5)  // most recent index
        .setLastLogTerm(1)   // old term on that index
    );
    assertTrue("The follower should have granted the vote", reply.getVoteGranted());

    // Vote the second time
    EloquentRaftProto.RequestVoteReply secondReply = requestVote(algorithm, request -> request
        .setTerm(3)          // A candidate with our same term is requesting a vote
        .setLastLogIndex(5)  // most recent index
        .setLastLogTerm(1)   // old term on that index
    );
    assertTrue("The follower should grant the same vote again", secondReply.getVoteGranted());
  }


  /**
   * Test that we can vote if we haven't voted for anyone yet (our votedFor variable is null)
   */
  @Test
  public void requestVoteNullVotedFor() {
    EloquentRaftAlgorithm algorithm = mkAlgorithm();
    algorithm.mutableState().setCurrentTerm(3);
    assertFalse("We should not have voted for anyone in our new term 3", algorithm.mutableState().votedFor.isPresent());
    EloquentRaftProto.RequestVoteReply reply = requestVote(algorithm, request -> request
        .setTerm(4)          // 1 more than our current term
        .setLastLogIndex(5)  // most recent index
        .setLastLogTerm(2)   // most recent term
    );

    assertTrue("The follower should have granted the vote", reply.getVoteGranted());
    assertEquals("The follower should be on the new term", 4, algorithm.mutableState().currentTerm);
  }


  /**
   * Test that we can vote for someone even in our current term, so long as we haven't voted for anyone else
   * yet (including ourselves).
   */
  @Test
  public void requestVoteNullVotedForSameTerm() {
    EloquentRaftAlgorithm algorithm = mkAlgorithm();
    algorithm.mutableState().setCurrentTerm(3);
    assertFalse("We should not have voted for anyone in our new term 3", algorithm.mutableState().votedFor.isPresent());
    EloquentRaftProto.RequestVoteReply reply = requestVote(algorithm, request -> request
        .setTerm(3)          // our current term, but we haven't voted for anyone yet in this term
        .setLastLogIndex(5)  // most recent index
        .setLastLogTerm(2)   // most recent term
    );

    assertTrue("The follower should have granted the vote", reply.getVoteGranted());
    assertEquals("The follower should still be on the current term", 3, algorithm.mutableState().currentTerm);
  }


  //
  // --------------------------------------------------------------------------
  // REQUEST VOTE REPLY
  // --------------------------------------------------------------------------
  //


  /**
   * Test the happy path of a vote being granted.
   */
  @Test
  public void requestVoteReplyHappyPath() {
    // Set up our algorithm
    EloquentRaftAlgorithm algorithm = mkAlgorithm();
    algorithm.mutableState().stepDownFromElection(2, 0L);
    algorithm.convertToCandidate();
    assertEquals("Reminder: we are now on term 3", 3, algorithm.mutableState().currentTerm);

    // Receive our vote
    algorithm.receiveRequestVotesReply(EloquentRaftProto.RequestVoteReply.newBuilder()
        .setTerm(3)            // our current term
        .setVoteGranted(true)  // the vote is granted
        .setVoterName("A")     // the server that granted the vote
        .build());

    // Assertions
    assertEquals("We should now have a vote from L and A", new HashSet<>(Arrays.asList("L", "A")), algorithm.mutableState().votesReceived);
    assertEquals("We should still have voted for ourselves", Optional.of("L"), algorithm.mutableState().votedFor);
    assertEquals("We should not yet have elected ourselves", RaftState.LeadershipStatus.CANDIDATE, algorithm.mutableState().leadership);
  }


  /**
   * Test the happy path of being completely elected
   */
  @Test
  public void requestVoteReplyElectionSuccess() {
    // Set up our algorithm
    EloquentRaftAlgorithm algorithm = mkAlgorithm();
    algorithm.mutableState().stepDownFromElection(2, 0L);
    algorithm.convertToCandidate();
    assertEquals("Reminder: we are now on term 3", 3, algorithm.mutableState().currentTerm);

    // Receive our vote
    algorithm.receiveRequestVotesReply(EloquentRaftProto.RequestVoteReply.newBuilder()
        .setTerm(3)            // our current term
        .setVoteGranted(true)  // the vote is granted
        .setVoterName("A")     // the server that granted the vote
        .build());
    algorithm.receiveRequestVotesReply(EloquentRaftProto.RequestVoteReply.newBuilder()
        .setTerm(3)            // our current term
        .setVoteGranted(true)  // the vote is granted
        .setVoterName("B")     // the server that granted the vote
        .build());

    // Assertions
    assertEquals("We should now have a vote from L and A, and B", new HashSet<>(Arrays.asList("L", "A", "B")), algorithm.mutableState().votesReceived);
    assertEquals("We should still have voted for ourselves", Optional.of("L"), algorithm.mutableState().votedFor);
    assertEquals("We should now have elected ourselves by majority vote", RaftState.LeadershipStatus.LEADER, algorithm.mutableState().leadership);
  }


  /**
   * Ensure that we don't register a vote if it's not given
   */
  @Test
  public void requestVoteReplyVoteNotGiven() {
    // Set up our algorithm
    EloquentRaftAlgorithm algorithm = mkAlgorithm();
    algorithm.mutableState().stepDownFromElection(2, 0L);
    algorithm.convertToCandidate();
    assertEquals("Reminder: we are now on term 3", 3, algorithm.mutableState().currentTerm);

    // Receive our vote
    algorithm.receiveRequestVotesReply(EloquentRaftProto.RequestVoteReply.newBuilder()
        .setTerm(3)             // our current term
        .setVoteGranted(false)  // the vote is NOT granted
        .setVoterName("A")      // the server that granted the vote
        .build());

    // Assertions
    assertEquals("We should still only have votes from ourselves", Collections.singleton("L"), algorithm.mutableState().votesReceived);
    assertEquals("We should not yet have elected ourselves", RaftState.LeadershipStatus.CANDIDATE, algorithm.mutableState().leadership);
  }


  /**
   * Ensure that we don't register a vote that's from an older term
   */
  @Test
  public void requestVoteReplyOldTerm() {
    // Set up our algorithm
    EloquentRaftAlgorithm algorithm = mkAlgorithm();
    algorithm.mutableState().stepDownFromElection(2, 0L);
    algorithm.convertToCandidate();
    assertEquals("Reminder: we are now on term 3", 3, algorithm.mutableState().currentTerm);

    // Receive our vote
    algorithm.receiveRequestVotesReply(EloquentRaftProto.RequestVoteReply.newBuilder()
        .setTerm(2)             // an older term
        .setVoteGranted(true)   // the vote is granted
        .setVoterName("A")      // the server that granted the vote
        .build());

    // Assertions
    assertEquals("We should still only have votes from ourselves", Collections.singleton("L"), algorithm.mutableState().votesReceived);
    assertEquals("We should not yet have elected ourselves", RaftState.LeadershipStatus.CANDIDATE, algorithm.mutableState().leadership);
  }


  /**
   * Ensure that we don't register a vote that's from a newer term.
   * This should never happen, but is nonetheless certainly an error.
   */
  @Test
  public void requestVoteReplyNewTerm() {
    // Set up our algorithm
    EloquentRaftAlgorithm algorithm = mkAlgorithm();
    algorithm.mutableState().stepDownFromElection(2, 0L);
    algorithm.convertToCandidate();
    assertEquals("Reminder: we are now on term 3", 3, algorithm.mutableState().currentTerm);

    // Receive our vote
    algorithm.receiveRequestVotesReply(EloquentRaftProto.RequestVoteReply.newBuilder()
        .setTerm(4)             // a newer term
        .setVoteGranted(true)   // the vote is granted
        .setVoterName("A")      // the server that granted the vote
        .build());

    // Assertions
    assertEquals("We should convert to a follower", RaftState.LeadershipStatus.OTHER, algorithm.mutableState().leadership);
    assertTrue("We should no longer have voted for ourselves", algorithm.mutableState().votesReceived.isEmpty());
  }


  //
  // --------------------------------------------------------------------------
  // INSTALL SNAPSHOT
  // --------------------------------------------------------------------------
  //


  /**
   * A short little helper for calling the append entries RPC from unit tests.
   * Note that the "leader" is always 'A' here.
   */
  private EloquentRaftProto.InstallSnapshotReply installSnapshot(EloquentRaftAlgorithm algorithm, Consumer<EloquentRaftProto.InstallSnapshotRequest.Builder> message) {
    Pointer<EloquentRaftProto.InstallSnapshotReply> replyPointer = new Pointer<>();
    EloquentRaftProto.InstallSnapshotRequest.Builder request = EloquentRaftProto.InstallSnapshotRequest.newBuilder()
        .setLeaderName("A")  // always from 'A' as the leader
        .addAllLastConfig(Arrays.asList("L", "A", "B", "C", "D"))  // keep our config by default
        ;
    message.accept(request);
    algorithm.receiveInstallSnapshotRPC(request.build(), x -> replyPointer.set(x.getInstallSnapshotReply()));
    return replyPointer.dereference().orElseThrow(() -> new AssertionError("Should have received a reply from requestVote"));
  }


  /**
   * Test a simple heartbeat happy path
   */
  @Test
  public void installSnapshotHappyPath() {
    // Set up our algorithm
    EloquentRaftAlgorithm algorithm = mkAlgorithm();
    algorithm.mutableState().stepDownFromElection(2, 0L);  // appendEntries only makes sense as a follower

    // Receive a snapshot
    EloquentRaftProto.InstallSnapshotReply reply = installSnapshot(algorithm, request -> request
        .setTerm(2)          // our term
        .setLastIndex(5)     // our actual last index
        .setLastTerm(2)      // the term of the last index
        .setData(ByteString.copyFrom(new SingleByteStateMachine(3).serialize()))   // the state machine
    );

    // Assertions
    assertEquals("The reply should have term 2", 2, reply.getTerm());
    assertEquals("The reply should be requesting our next index", 6, reply.getNextIndex());
    assertEquals("The reply should be from us", "L", reply.getFollowerName());
    assertEquals("We should have committed up to the snapshot", 5, algorithm.mutableState().commitIndex());
    assertEquals("Our state machine should have the correct value (the one @ index=5)",
        3, ((SingleByteStateMachine) algorithm.mutableState().log.stateMachine).value);
  }


  /**
   * Install a snapshot ahead of where the log currently is.
   */
  @Test
  public void installSnapshotAheadOfLog() {
    // Set up our algorithm
    EloquentRaftAlgorithm algorithm = mkAlgorithm();
    algorithm.mutableState().stepDownFromElection(2, 0L);  // appendEntries only makes sense as a follower

    // Receive a snapshot
    EloquentRaftProto.InstallSnapshotReply reply = installSnapshot(algorithm, request -> request
        .setTerm(3)           // our term
        .setLastIndex(10)     // our actual last index
        .setLastTerm(2)       // the term of the last index
        .setData(ByteString.copyFrom(new SingleByteStateMachine(42).serialize()))   // the state machine
    );

    // Assertions
    assertEquals("We should have term 3 (the snapshot's term)", 3, algorithm.mutableState().currentTerm);
    assertEquals("The reply should have term 3", 3, reply.getTerm());
    assertEquals("The reply should be requesting the index after the snapshot", 11, reply.getNextIndex());
    assertEquals("We should have committed up to the snapshot", 10, algorithm.mutableState().commitIndex());
    assertEquals("Our state machine should have the correct value (the one @ index=10; the snapshot)",
        42, ((SingleByteStateMachine) algorithm.mutableState().log.stateMachine).value);
  }


  /**
   * Install a snapshot behind of where the log currently is.
   */
  @Test
  public void installSnapshotBehindLog() {
    // Set up our algorithm
    EloquentRaftAlgorithm algorithm = mkAlgorithm();
    algorithm.mutableState().stepDownFromElection(2, 0L);  // appendEntries only makes sense as a follower

    // Receive a snapshot
    EloquentRaftProto.InstallSnapshotReply reply = installSnapshot(algorithm, request -> request
        .setTerm(2)          // our term
        .setLastIndex(2)     // our actual last index
        .setLastTerm(1)      // the term of the last index
        .setData(ByteString.copyFrom(new SingleByteStateMachine(4).serialize()))   // the state machine
    );

    // Assertions
    assertEquals("The reply should have term 2", 2, reply.getTerm());
    assertEquals("The reply should be requesting the index at the end of the log", 6, reply.getNextIndex());
    assertEquals("We should have committed up to the snapshot", 2, algorithm.mutableState().commitIndex());
    assertEquals("Our state machine should have the correct value (the one @ index=2; the snapshot)",
        4, ((SingleByteStateMachine) algorithm.mutableState().log.stateMachine).value);
  }


  /**
   * This is a case that should never practically occur, but: ensure that the snapshot's state is the
   * one that is accepted into the state machine when it disagrees with the log.
   */
  @Test
  public void installSnapshotClobbersStateMachine() {
    // Set up our algorithm
    EloquentRaftAlgorithm algorithm = mkAlgorithm();
    algorithm.mutableState().stepDownFromElection(2, 0L);  // appendEntries only makes sense as a follower

    // Receive a snapshot
    installSnapshot(algorithm, request -> request
        .setTerm(2)          // our term
        .setLastIndex(5)     // our actual last index
        .setLastTerm(2)      // the term of the last index
        .setData(ByteString.copyFrom(new SingleByteStateMachine(42).serialize()))   // the state machine, WITH AN INCORRECT VALUE
    );

    // Assertions
    assertEquals("Our state machine should have the correct value (the one in the snapshot, rather than the one @ index=5)",
        42, ((SingleByteStateMachine) algorithm.mutableState().log.stateMachine).value);
  }


  /**
   * Ensure that we don't install snapshots that come with an outdated term.
   */
  @Test
  public void installSnapshotOldTerm() {
    // Set up our algorithm
    EloquentRaftAlgorithm algorithm = mkAlgorithm();
    algorithm.mutableState().stepDownFromElection(2, 0L);  // appendEntries only makes sense as a follower

    // Receive a snapshot
    EloquentRaftProto.InstallSnapshotReply reply = installSnapshot(algorithm, request -> request
        .setTerm(1)          // an old term
        .setLastIndex(5)     // our actual last index
        .setLastTerm(2)      // the correct term of the last index
        .setData(ByteString.copyFrom(new SingleByteStateMachine(3).serialize()))   // the state machine
    );

    // Assertions
    assertEquals("The reply should have term 2", 2, reply.getTerm());
    assertEquals("The reply should be requesting the index at the end of the log", 6, reply.getNextIndex());
    assertEquals("We should NOT have committed up to the snapshot", 0, algorithm.mutableState().commitIndex());
    assertEquals("Our state machine should have the correct value (the original value)",
        -1, ((SingleByteStateMachine) algorithm.mutableState().log.stateMachine).value);
  }


  /**
   * Reconfigure the cluster on an install snapshot request
   */
  @Test
  public void installSnapshotReconfigureCluster() {
    // Set up our algorithm
    EloquentRaftAlgorithm algorithm = mkAlgorithm();
    algorithm.mutableState().stepDownFromElection(2, 0L);  // appendEntries only makes sense as a follower

    // Receive a snapshot
    EloquentRaftProto.InstallSnapshotReply reply = installSnapshot(algorithm, request -> request
        .setTerm(2)          // our current term
        .setLastIndex(6)     // install ahead of our current index
        .setLastTerm(2)      // the correct term of the last index
        .setData(ByteString.copyFrom(new SingleByteStateMachine(42).serialize()))   // the state machine
        .clearLastConfig()
        .addAllLastConfig(Collections.singleton("L"))  // a new configuration

    );

    // Assertions
    assertEquals("The reply should have term 2", 2, reply.getTerm());
    assertEquals("The reply should be requesting the index after the snapshot", 7, reply.getNextIndex());
    assertEquals("We should have committed up to the snapshot", 6, algorithm.mutableState().commitIndex());
    assertEquals("Our state machine should have the snapshot value",
        42, ((SingleByteStateMachine) algorithm.mutableState().log.stateMachine).value);
    assertEquals("We should see the snapshot quorum (latest)",
        Collections.singleton("L"), algorithm.mutableState().log.latestQuorumMembers);
    assertEquals("We should see the snapshot quorum (committed)",
        Collections.singleton("L"), algorithm.mutableState().log.committedQuorumMembers);
  }


  /**
   * Reconfigure the cluster on an install snapshot request,
   * ensuring that the latest quorum members remain the ones from the log
   * if the snapshot is earlier than the log
   */
  @Test
  public void installSnapshotReconfigureClusterMaintainLatestMembers() {
    // Set up our algorithm
    EloquentRaftAlgorithm algorithm = mkAlgorithm();
    algorithm.mutableState().stepDownFromElection(2, 0L);  // appendEntries only makes sense as a follower

    // Change the cluster
    appendEntries(algorithm, request -> request
        .setTerm(2)             // our term
        .setPrevLogIndex(5)     // our actual last index
        .setPrevLogTerm(2)      // the term of the last index
        .setLeaderCommit(0)     // we have not committed anything yet
        .addEntry(RaftLogTest.makeConfigurationEntry(6, 2, Collections.singletonList("L")))
    );
    assertEquals("We should have updated our latest quorum (@ index=6)",
        Collections.singleton("L"), algorithm.mutableState().log.latestQuorumMembers);

    // Receive a snapshot
    EloquentRaftProto.InstallSnapshotReply reply = installSnapshot(algorithm, request -> request
        .setTerm(2)          // our current term
        .setLastIndex(3)     // install ahead of our current index
        .setLastTerm(2)      // the correct term of the last index
        .setData(ByteString.copyFrom(new SingleByteStateMachine(5).serialize()))   // the state machine
        .clearLastConfig()
        .addAllLastConfig(Arrays.asList("L", "A", "B"))  // explicitly send the new configuration

    );

    // Assertions
    assertEquals("The reply should have term 2", 2, reply.getTerm());
    assertEquals("The reply should be requesting the index after the last index", 7, reply.getNextIndex());
    assertEquals("We should have committed up to the snapshot", 3, algorithm.mutableState().commitIndex());
    assertEquals("We should see the log's quorum (latest)",
        Collections.singleton("L"), algorithm.mutableState().log.latestQuorumMembers);
    assertEquals("We should see the snapshot quorum (committed)",
        new HashSet<>(Arrays.asList("L", "A", "B")), algorithm.mutableState().log.committedQuorumMembers);
  }


  //
  // --------------------------------------------------------------------------
  // INSTALL SNAPSHOT REPLY
  // --------------------------------------------------------------------------
  //


  /**
   * Resend a snapshot if a server remains behind us on the last snapshot.
   */
  @Test
  public void installSnapshotReplySimpleCase() {
    // Setup
    EloquentRaftAlgorithm algorithm = mkAlgorithm();
    AssertableTransport transport = (AssertableTransport) algorithm.transport;

    // Leader snapshots are index 2
    RaftLog.Snapshot snapshot = new RaftLog.Snapshot(new SingleByteStateMachine(4).serialize(), 2, 1, new ArrayList<>());
    algorithm.mutableState().log.installSnapshot(snapshot, 0L);

    // Receive the reply
    EloquentRaftProto.InstallSnapshotReply reply = EloquentRaftProto.InstallSnapshotReply.newBuilder()
        .setNextIndex(2)  // Want the snapshot at index 2 again
        .setTerm(2)
        .setFollowerName("A")
        .build();
    algorithm.receiveInstallSnapshotReply(reply);

    // Check the assertions
    assertEquals("installSnapshotReply should never send messages on the transport ",
        0, transport.sendCalls.size());
    assertEquals("We should have updated our server's nextIndex",
        2, algorithm.mutableState().nextIndex.get().get("A").longValue());
    assertEquals("We should have updated our server's matchIndex",
        1, algorithm.mutableState().matchIndex.get().get("A").longValue());
  }


  /**
   * Resend a snapshot if a server remains behind us on the last snapshot.
   */
  @Test
  public void installSnapshotReplyNewerTerm() {
    // Setup
    EloquentRaftAlgorithm algorithm = mkAlgorithm();
    AssertableTransport transport = (AssertableTransport) algorithm.transport;

    // Leader snapshots are index 2
    RaftLog.Snapshot snapshot = new RaftLog.Snapshot(new SingleByteStateMachine(4).serialize(), 2, 1, new ArrayList<>());
    algorithm.mutableState().log.installSnapshot(snapshot, 0L);

    // Receive the reply
    EloquentRaftProto.InstallSnapshotReply reply = EloquentRaftProto.InstallSnapshotReply.newBuilder()
        .setNextIndex(2)  // Want the snapshot at index 2 again
        .setTerm(3)
        .setFollowerName("A")
        .build();
    assertTrue("We should begin as leader", algorithm.mutableState().isLeader());
    algorithm.receiveInstallSnapshotReply(reply);

    // Check the assertions
    assertEquals("installSnapshotReply should never send messages on the transport ",
        0, transport.sendCalls.size());
    assertFalse("We should have stepped down as leader", algorithm.mutableState().isLeader());
    assertFalse("We should have no nextIndex", algorithm.mutableState().nextIndex.isPresent());
    assertFalse("We should have no matchIndex", algorithm.mutableState().matchIndex.isPresent());
  }


  //
  // --------------------------------------------------------------------------
  // KEENON TESTS
  // --------------------------------------------------------------------------
  //


  /**
   * Tests that we're sending our broadcasts on the correct index
   */
  @Test
  public void testLeaderBroadcastIndex() {
    SingleByteStateMachine stateMachine = new SingleByteStateMachine();
    RaftLog raftLog = new RaftLog(stateMachine, Arrays.asList("server1", "server2", "server3", "leader"), Executors.newSingleThreadExecutor());

    // Setup the log

    List<EloquentRaftProto.LogEntry> appendEntries = Arrays.asList(
        makeEntry(1, 1, 3),
        makeEntry(2, 1, 4),
        makeEntry(3, 1, 5),
        makeEntry(4, 2, 1),
        makeEntry(5, 2, 3)
    );
    boolean success = raftLog.appendEntries(0, 0, appendEntries);
    assertTrue(success);
    assertEquals("Nothing should be committed yet", -1, stateMachine.value);
    assertFalse("There should not be a snapshot", raftLog.snapshot.isPresent());
    assertEquals("There should be 5 log entries", 5, raftLog.logEntries.size());
    long lastIndex = 2;
    long lastTerm = 1;
    RaftLog.Snapshot snapshot = new RaftLog.Snapshot(new byte[]{4}, lastIndex, lastTerm, new ArrayList<>());
    raftLog.setCommitIndex(5, 0);
    assertTrue(raftLog.installSnapshot(snapshot, 0));
    assertEquals("Commit index should have been preserved", 5, raftLog.commitIndex);

    // Setup the algorithm

    RaftState raftState = new RaftState("leader", raftLog);
    AssertableTransport transport = new AssertableTransport();
    RaftAlgorithm algorithm = new SingleThreadedRaftAlgorithm(new EloquentRaftAlgorithm(raftState, transport, Optional.empty()), Executors.newSingleThreadExecutor());
    try {
      raftState.elect(0);

      assertTrue(raftState.nextIndex.isPresent());
      assertTrue(raftState.matchIndex.isPresent());

      ///////////////////////////////////////////////////////////////////////////////////////
      // Tests start below
      ///////////////////////////////////////////////////////////////////////////////////////

      // 1. Try a call where everybody is getting a snapshot

      raftState.nextIndex.get().put("server1", 1L);
      raftState.nextIndex.get().put("server2", 1L);
      raftState.nextIndex.get().put("server3", 1L);

      transport.broadcastCalls.clear();
      algorithm.broadcastAppendEntries();
      assertEquals(1, transport.broadcastCalls.size());

      List<EloquentRaftProto.LogEntry> expectedBroadcastEntries = new ArrayList<>();
      assertEquals(expectedBroadcastEntries, transport.broadcastCalls.get(0).message.getAppendEntries().getEntryList());
      assertEquals(2, transport.broadcastCalls.get(0).message.getAppendEntries().getPrevLogTerm());
      assertEquals(5, transport.broadcastCalls.get(0).message.getAppendEntries().getPrevLogIndex());

      // 2. Try a call where some people are getting a snapshot, but others aren't

      raftState.nextIndex.get().put("server1", 1L);
      raftState.nextIndex.get().put("server2", 2L);
      raftState.nextIndex.get().put("server3", 3L);

      transport.broadcastCalls.clear();
      algorithm.broadcastAppendEntries();
      assertEquals(1, transport.broadcastCalls.size());

      expectedBroadcastEntries = Arrays.asList(
          makeEntry(3, 1, 5),
          makeEntry(4, 2, 1),
          makeEntry(5, 2, 3)
      );
      assertEquals(expectedBroadcastEntries, transport.broadcastCalls.get(0).message.getAppendEntries().getEntryList());
      assertEquals(1, transport.broadcastCalls.get(0).message.getAppendEntries().getPrevLogTerm());
      assertEquals(2, transport.broadcastCalls.get(0).message.getAppendEntries().getPrevLogIndex());

      // 3. Try a call where nobody is getting a snapshot, but some people are getting more than others

      raftState.nextIndex.get().put("server1", 3L);
      raftState.nextIndex.get().put("server2", 4L);
      raftState.nextIndex.get().put("server3", 5L);

      transport.broadcastCalls.clear();
      algorithm.broadcastAppendEntries();
      assertEquals(1, transport.broadcastCalls.size());

      expectedBroadcastEntries = Arrays.asList(
          makeEntry(3, 1, 5),
          makeEntry(4, 2, 1),
          makeEntry(5, 2, 3)
      );
      assertEquals(expectedBroadcastEntries, transport.broadcastCalls.get(0).message.getAppendEntries().getEntryList());
      assertEquals(1, transport.broadcastCalls.get(0).message.getAppendEntries().getPrevLogTerm());
      assertEquals(2, transport.broadcastCalls.get(0).message.getAppendEntries().getPrevLogIndex());

      // 4. Try a call where some people are getting nothing

      raftState.nextIndex.get().put("server1", 5L);
      raftState.nextIndex.get().put("server2", 5L);
      raftState.nextIndex.get().put("server3", 6L);

      transport.broadcastCalls.clear();
      algorithm.broadcastAppendEntries();
      assertEquals(1, transport.broadcastCalls.size());

      expectedBroadcastEntries = Collections.singletonList(
          makeEntry(5, 2, 3)
      );
      assertEquals(expectedBroadcastEntries, transport.broadcastCalls.get(0).message.getAppendEntries().getEntryList());
      assertEquals(2, transport.broadcastCalls.get(0).message.getAppendEntries().getPrevLogTerm());
      assertEquals(4, transport.broadcastCalls.get(0).message.getAppendEntries().getPrevLogIndex());

      // 5. Try a call where nobody is getting anything

      raftState.nextIndex.get().put("server1", 6L);
      raftState.nextIndex.get().put("server2", 6L);
      raftState.nextIndex.get().put("server3", 6L);

      transport.broadcastCalls.clear();
      algorithm.broadcastAppendEntries();
      assertEquals(1, transport.broadcastCalls.size());

      expectedBroadcastEntries = new ArrayList<>();
      assertEquals(expectedBroadcastEntries, transport.broadcastCalls.get(0).message.getAppendEntries().getEntryList());
      assertEquals(2, transport.broadcastCalls.get(0).message.getAppendEntries().getPrevLogTerm());
      assertEquals(5, transport.broadcastCalls.get(0).message.getAppendEntries().getPrevLogIndex());
    } finally {
      algorithm.stop(true);
      transport.stop();
    }
  }

  /**
   * Tests that we're handling incoming appendEntries requests correctly
   */
  @Test
  public void testAppendEntriesNeedSnapshot() {
    SingleByteStateMachine stateMachine = new SingleByteStateMachine();
    RaftLog raftLog = new RaftLog(stateMachine, Arrays.asList("server1", "server2", "server3", "leader"), Executors.newSingleThreadExecutor());

    // Setup the algorithm

    RaftState raftState = new RaftState("server1", raftLog);
    AssertableTransport transport = new AssertableTransport();
    RaftAlgorithm algorithm = new SingleThreadedRaftAlgorithm(new EloquentRaftAlgorithm(raftState, transport, Optional.empty()), Executors.newSingleThreadExecutor());
    try {

      // Make the append entries request, which should fail as being too far in the future

      EloquentRaftProto.AppendEntriesRequest request = EloquentRaftProto.AppendEntriesRequest.newBuilder()
          .setPrevLogIndex(5)
          .setPrevLogTerm(2)
          .setLeaderName("leader")
          .setLeaderCommit(5)
          .setTerm(2)
          .build();

      final Pointer<EloquentRaftProto.RaftMessage> replyPointer = new Pointer<>();
      algorithm.receiveAppendEntriesRPC(request, replyPointer::set);

      assertEquals(2, algorithm.term());
      EloquentRaftProto.AppendEntriesReply reply = replyPointer.dereference().get().getAppendEntriesReply();
      assertFalse(reply.getSuccess());
      assertEquals(1, reply.getNextIndex());
      assertEquals(2, reply.getTerm());
    } finally {
      algorithm.stop(true);
      transport.stop();
    }
  }


  //
  // --------------------------------------------------------------------------
  // STRESS TESTS
  // --------------------------------------------------------------------------
  //


  /**
   * Submit a transition to the given Raft node.
   *
   * @return The future for whether the transition was successful.
   */
  private CompletableFuture<Boolean> transition(RaftAlgorithm node,
                                                String key, byte[] value) {
    byte[] transition = KeyValueStateMachine.createSetValueTransition(key, value);
    return node.receiveRPC(RaftTransport.mkRaftRPC(node.serverName(),
        EloquentRaftProto.ApplyTransitionRequest.newBuilder()
            .setTransition(ByteString.copyFrom(transition))
            .build()),
        false
    ).thenApply(reply -> reply.getApplyTransitionReply().getSuccess());
  }


  /**
   * A useful entry point for stress testing heartbeat performance.
   *
   * This is usually run with a profiler so that we can get profiling data on Raft.
   */
  @Ignore
  @Test
  public void stressTestHeartbeat() throws IOException, ExecutionException, InterruptedException {
    // 1. Create an algorithm and elect ourselves
    RaftState state = new RaftState("server", new KeyValueStateMachine("server"), Executors.newSingleThreadExecutor());
    RaftAlgorithm algorithm = new SingleThreadedRaftAlgorithm(new EloquentRaftAlgorithm(state, new NetRaftTransport("server"), Optional.empty()), Executors.newSingleThreadExecutor());
    try {
      algorithm.bootstrap(false);
      algorithm.heartbeat();
      for (int i = 0; i < 50; ++i) {
        Uninterruptably.sleep(100);
        algorithm.heartbeat();
      }

      // 2. Set some values
      for (int i = 0; i < 10000; ++i) {
        transition(algorithm, "key_" + i, new byte[]{(byte) i}).get();
        if (i % 125 == 0) {
          algorithm.heartbeat();
        }
      }

      // 3. Heartbeat a bunch
      long start = System.currentTimeMillis();
      long now = start;
      for (int i = 0; i < 20 * 60 * 60 * 24; ++i) {
        algorithm.heartbeat();
        now += 50;
      }
      System.out.println("Took " + TimerUtils.formatTimeSince(start) + " to run heartbeats over a simulated time of " +
          TimerUtils.formatTimeDifference(now - start) + " => " +
          new DecimalFormat("0.000%").format(((double) (System.currentTimeMillis() - start)) / ((double) (now - start))) +
          " of time spent in heartbeats"
      );
    } finally {
      algorithm.stop(true);
    }
  }


  /**
   * A useful entry point for stress testing heartbeat performance.
   *
   * This is usually run with a profiler so that we can get profiling data on Raft.
   */
  @Ignore
  @Test
  public void stressTestAppendEntries() throws ExecutionException, InterruptedException {
    // 1. Create an algorithm and elect ourselves
    AssertableTransport transport = new AssertableTransport();
    RaftState state = new RaftState("follower", new KeyValueStateMachine("follower"), Executors.newSingleThreadExecutor());
    RaftAlgorithm algorithm = new SingleThreadedRaftAlgorithm(new EloquentRaftAlgorithm(state, transport, Optional.empty()), Executors.newSingleThreadExecutor());
    try {

      System.out.println(state.log.latestQuorumMembers);
      state.log.latestQuorumMembers.add("leader");
      state.leadership = RaftState.LeadershipStatus.OTHER; // Be a follower

      // 2. Heartbeat a bunch
      long start = System.currentTimeMillis();
      long count = 10000000;
      for (int i = 0; i < count; ++i) {
        CompletableFuture<Boolean> success = new CompletableFuture<>();
        algorithm.receiveAppendEntriesRPC(EloquentRaftProto.AppendEntriesRequest.newBuilder()
                .setLeaderCommit(i)
                .addEntry(EloquentRaftProto.LogEntry.newBuilder()
                    .setTransition(ByteString.copyFrom(KeyValueStateMachine.createSetValueTransition("key_" + (i % 50), new byte[]{10})))
                    .setIndex(i + 1)
                    .setTerm(0))
                .build(),
            (reply) -> success.complete(true));
        success.get();
      }
      System.out.println("Took " + TimerUtils.formatTimeSince(start) + " to run " + count + " transitions - " + ((double) (System.currentTimeMillis() - start) / count) + " per transition");
    } finally {
      algorithm.stop(true);
      transport.stop();
    }
  }


  /**
   * This tests a situation in which we're both trying to remove a server and trying to add a server. We're interested
   * in checking that we actually handle calling AddServerRPC and RemoveServerRPC correctly.
   *
   * We have 3 servers in the simulation:
   *
   * "broken_leader" - our hero, a leader with a bad quorum member, and a new server trying to join
   * "dead_server" - a bad quorum member, no longer responds to anything
   * "new_server" - a server trying to join the quorum, which fails to do so
   */
  @Ignore // note[gabor] Not failing, just very slow with the longer machine down timeout
  @Test
  public void testAddServerRunaway() {
    SingleByteStateMachine stateMachine = new SingleByteStateMachine();
    RaftLog raftLog = new RaftLog(stateMachine, Arrays.asList("broken_leader", "dead_server"), Executors.newSingleThreadExecutor());
    RaftState state = new RaftState("broken_leader", raftLog);

    AssertableTransport transport = new AssertableTransport();

    RaftAlgorithm algorithm = new SingleThreadedRaftAlgorithm(new EloquentRaftAlgorithm(state, transport, Optional.empty()), Executors.newSingleThreadExecutor());
    try {
      state.elect(0);

      for (long i = 0; i < EloquentRaftAlgorithm.MACHINE_DOWN_TIMEOUT * 2; i++) {
        transport.sleep(1);
        state.observeLifeFrom("new_server", i);
        algorithm.heartbeat();

        // Reply to any RPCs

        while (transport.rpcCalls.size() > 0) {
          List<AssertableTransport.RpcCall> calls = new ArrayList<>(transport.rpcCalls);
          calls.stream().filter(sendCall -> sendCall.sender.equals("broken_leader") && sendCall.destination.equals("new_server")).forEach(rpcCall -> {
            long nextIndex = rpcCall.message.getAppendEntries().getPrevLogIndex() + rpcCall.message.getAppendEntries().getEntryCount() + 1;
            rpcCall.hasReplied = true;
            rpcCall.onResponseReceived.accept(
                EloquentRaftProto.RaftMessage.newBuilder().setAppendEntriesReply(EloquentRaftProto.AppendEntriesReply.newBuilder()
                    .setTerm(rpcCall.message.getAppendEntries().getTerm())
                    .setNextIndex(nextIndex)
                    .setFollowerName("new_server")
                    .setSuccess(true))
                    .build());
          });
          transport.rpcCalls.removeAll(calls);
        }

        // Reply to any append entries calls

        while (transport.broadcastCalls.size() > 0) {
          List<AssertableTransport.BroadcastCall> calls = new ArrayList<>(transport.broadcastCalls);
          calls.stream().filter(broadcastCall -> broadcastCall.sender.equals("broken_leader")).forEach(broadcastCall -> {
            long nextIndex = broadcastCall.message.getAppendEntries().getPrevLogIndex() + broadcastCall.message.getAppendEntries().getEntryCount() + 1;
            algorithm.receiveAppendEntriesReply(EloquentRaftProto.AppendEntriesReply.newBuilder()
                .setTerm(broadcastCall.message.getAppendEntries().getTerm())
                .setNextIndex(nextIndex)
                .setFollowerName("new_server")
                .setSuccess(true)
                .build());
          });
          transport.broadcastCalls.removeAll(calls);
        }
      }

      assertEquals(2, raftLog.logEntries.size());
      assertFalse(raftLog.getQuorumMembers().contains("dead_server"));
      assertTrue(raftLog.getQuorumMembers().contains("new_server"));

    } finally {
      algorithm.stop(true);
      transport.stop();
    }
  }
}