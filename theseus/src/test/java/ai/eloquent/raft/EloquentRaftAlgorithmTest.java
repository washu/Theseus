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
  /**
   * This is here so that we can assert
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
    @SuppressWarnings("ConstantConditions")
    @Override
    public void scheduleAtFixedRate(SafeTimerTask task, long period) {
      task.run(Optional.empty());
      transportMockTimer.schedule(task, 0, period);
    }


    /** Schedule an event on the transport's time. */
    @SuppressWarnings("ConstantConditions")
    @Override
    public void schedule(SafeTimerTask task, long delay) {
      transportMockTimer.schedule(task, delay);
    }

    @Override
    public long now() {
      return transportMockTimer.now();
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
            long nextIndex = rpcCall.message.getAppendEntries().getPrevLogIndex() + rpcCall.message.getAppendEntries().getEntriesCount() + 1;
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
            long nextIndex = broadcastCall.message.getAppendEntries().getPrevLogIndex() + broadcastCall.message.getAppendEntries().getEntriesCount() + 1;
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
      transport.sleep(EloquentRaftAlgorithm.BROADCAST_PERIOD + 1);
      algorithm.broadcastAppendEntries();
      assertEquals(1, transport.broadcastCalls.size());

      List<EloquentRaftProto.LogEntry> expectedBroadcastEntries = new ArrayList<>();
      assertEquals(expectedBroadcastEntries, transport.broadcastCalls.get(0).message.getAppendEntries().getEntriesList());
      assertEquals(2, transport.broadcastCalls.get(0).message.getAppendEntries().getPrevLogTerm());
      assertEquals(5, transport.broadcastCalls.get(0).message.getAppendEntries().getPrevLogIndex());

      // 2. Try a call where some people are getting a snapshot, but others aren't

      raftState.nextIndex.get().put("server1", 1L);
      raftState.nextIndex.get().put("server2", 2L);
      raftState.nextIndex.get().put("server3", 3L);

      transport.broadcastCalls.clear();
      transport.sleep(EloquentRaftAlgorithm.BROADCAST_PERIOD + 1);
      algorithm.broadcastAppendEntries();
      assertEquals(1, transport.broadcastCalls.size());

      expectedBroadcastEntries = Arrays.asList(
          makeEntry(3, 1, 5),
          makeEntry(4, 2, 1),
          makeEntry(5, 2, 3)
      );
      assertEquals(expectedBroadcastEntries, transport.broadcastCalls.get(0).message.getAppendEntries().getEntriesList());
      assertEquals(1, transport.broadcastCalls.get(0).message.getAppendEntries().getPrevLogTerm());
      assertEquals(2, transport.broadcastCalls.get(0).message.getAppendEntries().getPrevLogIndex());

      // 3. Try a call where nobody is getting a snapshot, but some people are getting more than others

      raftState.nextIndex.get().put("server1", 3L);
      raftState.nextIndex.get().put("server2", 4L);
      raftState.nextIndex.get().put("server3", 5L);

      transport.broadcastCalls.clear();
      transport.sleep(EloquentRaftAlgorithm.BROADCAST_PERIOD + 1);
      algorithm.broadcastAppendEntries();
      assertEquals(1, transport.broadcastCalls.size());

      expectedBroadcastEntries = Arrays.asList(
          makeEntry(3, 1, 5),
          makeEntry(4, 2, 1),
          makeEntry(5, 2, 3)
      );
      assertEquals(expectedBroadcastEntries, transport.broadcastCalls.get(0).message.getAppendEntries().getEntriesList());
      assertEquals(1, transport.broadcastCalls.get(0).message.getAppendEntries().getPrevLogTerm());
      assertEquals(2, transport.broadcastCalls.get(0).message.getAppendEntries().getPrevLogIndex());

      // 4. Try a call where some people are getting nothing

      raftState.nextIndex.get().put("server1", 5L);
      raftState.nextIndex.get().put("server2", 5L);
      raftState.nextIndex.get().put("server3", 6L);

      transport.broadcastCalls.clear();
      transport.sleep(EloquentRaftAlgorithm.BROADCAST_PERIOD + 1);
      algorithm.broadcastAppendEntries();
      assertEquals(1, transport.broadcastCalls.size());

      expectedBroadcastEntries = Collections.singletonList(
          makeEntry(5, 2, 3)
      );
      assertEquals(expectedBroadcastEntries, transport.broadcastCalls.get(0).message.getAppendEntries().getEntriesList());
      assertEquals(2, transport.broadcastCalls.get(0).message.getAppendEntries().getPrevLogTerm());
      assertEquals(4, transport.broadcastCalls.get(0).message.getAppendEntries().getPrevLogIndex());

      // 5. Try a call where nobody is getting anything

      raftState.nextIndex.get().put("server1", 6L);
      raftState.nextIndex.get().put("server2", 6L);
      raftState.nextIndex.get().put("server3", 6L);

      transport.broadcastCalls.clear();
      transport.sleep(EloquentRaftAlgorithm.BROADCAST_PERIOD + 1);
      algorithm.broadcastAppendEntries();
      assertEquals(1, transport.broadcastCalls.size());

      expectedBroadcastEntries = new ArrayList<>();
      assertEquals(expectedBroadcastEntries, transport.broadcastCalls.get(0).message.getAppendEntries().getEntriesList());
      assertEquals(2, transport.broadcastCalls.get(0).message.getAppendEntries().getPrevLogTerm());
      assertEquals(5, transport.broadcastCalls.get(0).message.getAppendEntries().getPrevLogIndex());
    } finally {
      transport.sleep(EloquentRaftAlgorithm.BROADCAST_PERIOD + 1);
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

  /**
   * Test that we respond correctly to AppendEntriesReply messages
   */
  @Test
  public void testLeaderFailedAppendEntriesReplyHandling() {
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

      // 1. Try a reply from a server missing all logs

      EloquentRaftProto.AppendEntriesReply reply = EloquentRaftProto.AppendEntriesReply.newBuilder()
          .setSuccess(false)
          .setNextIndex(1)
          .setTerm(2)
          .setFollowerName("server1")
          .build();

      transport.sendCalls.clear();
      algorithm.receiveAppendEntriesReply(reply);
      assertEquals(1, transport.sendCalls.size());

      assertEquals("server1", transport.sendCalls.get(0).destination);
      assertEquals(EloquentRaftProto.RaftMessage.ContentsCase.INSTALLSNAPSHOT, transport.sendCalls.get(0).message.getContentsCase());
      EloquentRaftProto.InstallSnapshotRequest installSnapshotRequest = transport.sendCalls.get(0).message.getInstallSnapshot();
      assertEquals(snapshot.lastTerm, installSnapshotRequest.getLastTerm());
      assertEquals(snapshot.lastIndex, installSnapshotRequest.getLastIndex());
      assertEquals(algorithm.term(), installSnapshotRequest.getTerm());

      // 2. Try a reply from a server missing some logs

      reply = EloquentRaftProto.AppendEntriesReply.newBuilder()
          .setSuccess(false)
          .setNextIndex(2)
          .setTerm(2)
          .setFollowerName("server1")
          .build();

      transport.sendCalls.clear();
      algorithm.receiveAppendEntriesReply(reply);
      assertEquals(1, transport.sendCalls.size());

      assertEquals("server1", transport.sendCalls.get(0).destination);
      assertEquals(EloquentRaftProto.RaftMessage.ContentsCase.INSTALLSNAPSHOT, transport.sendCalls.get(0).message.getContentsCase());
      installSnapshotRequest = transport.sendCalls.get(0).message.getInstallSnapshot();
      assertEquals(snapshot.lastTerm, installSnapshotRequest.getLastTerm());
      assertEquals(snapshot.lastIndex, installSnapshotRequest.getLastIndex());
      assertEquals(algorithm.term(), installSnapshotRequest.getTerm());

      // 3. Try a reply from a server that needs several entries

      reply = EloquentRaftProto.AppendEntriesReply.newBuilder()
          .setSuccess(false)
          .setNextIndex(3)
          .setTerm(2)
          .setFollowerName("server1")
          .build();

      transport.sendCalls.clear();
      algorithm.receiveAppendEntriesReply(reply);
      assertEquals(1, transport.sendCalls.size());

      assertEquals("server1", transport.sendCalls.get(0).destination);
      assertEquals(EloquentRaftProto.RaftMessage.ContentsCase.APPENDENTRIES, transport.sendCalls.get(0).message.getContentsCase());
      List<EloquentRaftProto.LogEntry> expectedEntries = Arrays.asList(
          makeEntry(3, 1, 5),
          makeEntry(4, 2, 1),
          makeEntry(5, 2, 3)
      );
      assertEquals(expectedEntries, transport.sendCalls.get(0).message.getAppendEntries().getEntriesList());
      assertEquals(1, transport.sendCalls.get(0).message.getAppendEntries().getPrevLogTerm());
      assertEquals(2, transport.sendCalls.get(0).message.getAppendEntries().getPrevLogIndex());

      // 4. Try a reply from a server that needs one entry

      reply = EloquentRaftProto.AppendEntriesReply.newBuilder()
          .setSuccess(false)
          .setNextIndex(5)
          .setTerm(2)
          .setFollowerName("server1")
          .build();

      transport.sendCalls.clear();
      algorithm.receiveAppendEntriesReply(reply);
      assertEquals(1, transport.sendCalls.size());

      assertEquals("server1", transport.sendCalls.get(0).destination);
      assertEquals(EloquentRaftProto.RaftMessage.ContentsCase.APPENDENTRIES, transport.sendCalls.get(0).message.getContentsCase());
      expectedEntries = Collections.singletonList(
          makeEntry(5, 2, 3)
      );
      assertEquals(expectedEntries, transport.sendCalls.get(0).message.getAppendEntries().getEntriesList());
      assertEquals(2, transport.sendCalls.get(0).message.getAppendEntries().getPrevLogTerm());
      assertEquals(4, transport.sendCalls.get(0).message.getAppendEntries().getPrevLogIndex());

      // 5. Try a reply from a server that needs no entries

      reply = EloquentRaftProto.AppendEntriesReply.newBuilder()
          .setSuccess(false)
          .setNextIndex(6)
          .setTerm(2)
          .setFollowerName("server1")
          .build();

      transport.sendCalls.clear();
      algorithm.receiveAppendEntriesReply(reply);
      assertEquals(0, transport.sendCalls.size());
    } finally {
      algorithm.stop(true);
      transport.stop();
    }
  }


  /**
   * Test that we respond correctly to AppendEntriesReply messages
   */
  @Test
  public void testLeaderInstallSnapshotReplyHandling() {
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

      // 1. Try a reply from a server who just got an old snapshot, and needs another

      EloquentRaftProto.InstallSnapshotReply reply = EloquentRaftProto.InstallSnapshotReply.newBuilder()
          .setNextIndex(2)
          .setTerm(2)
          .setFollowerName("server1")
          .build();

      transport.sendCalls.clear();
      algorithm.receiveInstallSnapshotReply(reply);
      assertEquals(1, transport.sendCalls.size());

      assertEquals("server1", transport.sendCalls.get(0).destination);
      assertEquals(EloquentRaftProto.RaftMessage.ContentsCase.INSTALLSNAPSHOT, transport.sendCalls.get(0).message.getContentsCase());
      EloquentRaftProto.InstallSnapshotRequest installSnapshotRequest = transport.sendCalls.get(0).message.getInstallSnapshot();
      assertEquals(snapshot.lastTerm, installSnapshotRequest.getLastTerm());
      assertEquals(snapshot.lastIndex, installSnapshotRequest.getLastIndex());
      assertEquals(algorithm.term(), installSnapshotRequest.getTerm());

      // 3. Try a reply from a server that needs several entries

      reply = EloquentRaftProto.InstallSnapshotReply.newBuilder()
          .setNextIndex(3)
          .setTerm(2)
          .setFollowerName("server1")
          .build();

      transport.sendCalls.clear();
      algorithm.receiveInstallSnapshotReply(reply);
      assertEquals(1, transport.sendCalls.size());

      assertEquals("server1", transport.sendCalls.get(0).destination);
      assertEquals(EloquentRaftProto.RaftMessage.ContentsCase.APPENDENTRIES, transport.sendCalls.get(0).message.getContentsCase());
      List<EloquentRaftProto.LogEntry> expectedEntries = Arrays.asList(
          makeEntry(3, 1, 5),
          makeEntry(4, 2, 1),
          makeEntry(5, 2, 3)
      );
      assertEquals(expectedEntries, transport.sendCalls.get(0).message.getAppendEntries().getEntriesList());
      assertEquals(1, transport.sendCalls.get(0).message.getAppendEntries().getPrevLogTerm());
      assertEquals(2, transport.sendCalls.get(0).message.getAppendEntries().getPrevLogIndex());

      // 4. Try a reply from a server that needs one entry

      reply = EloquentRaftProto.InstallSnapshotReply.newBuilder()
          .setNextIndex(5)
          .setTerm(2)
          .setFollowerName("server1")
          .build();

      transport.sendCalls.clear();
      algorithm.receiveInstallSnapshotReply(reply);
      assertEquals(1, transport.sendCalls.size());

      assertEquals("server1", transport.sendCalls.get(0).destination);
      assertEquals(EloquentRaftProto.RaftMessage.ContentsCase.APPENDENTRIES, transport.sendCalls.get(0).message.getContentsCase());
      expectedEntries = Collections.singletonList(
          makeEntry(5, 2, 3)
      );
      assertEquals(expectedEntries, transport.sendCalls.get(0).message.getAppendEntries().getEntriesList());
      assertEquals(2, transport.sendCalls.get(0).message.getAppendEntries().getPrevLogTerm());
      assertEquals(4, transport.sendCalls.get(0).message.getAppendEntries().getPrevLogIndex());

      // 5. Try a reply from a server that needs no entries

      reply = EloquentRaftProto.InstallSnapshotReply.newBuilder()
          .setNextIndex(6)
          .setTerm(2)
          .setFollowerName("server1")
          .build();

      transport.sendCalls.clear();
      algorithm.receiveInstallSnapshotReply(reply);
      assertEquals(0, transport.sendCalls.size());
    } finally {
      algorithm.stop(true);
      transport.stop();
    }
  }



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
                .addEntries(EloquentRaftProto.LogEntry.newBuilder()
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
}