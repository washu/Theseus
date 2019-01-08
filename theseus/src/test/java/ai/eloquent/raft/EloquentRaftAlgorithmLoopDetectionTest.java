package ai.eloquent.raft;

import ai.eloquent.raft.transport.WithLocalTransport;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import org.junit.Test;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.*;

/**
 * Some simple standalone tests for EloquentRaftAlgorithm
 */
public class EloquentRaftAlgorithmLoopDetectionTest extends WithLocalTransport {
  /**
   * This is an EloquentRaftAlgorithm that doesn't do anything with heartbeats, which makes it easier to get the system
   * into a weird state.
   */
  private static class LobotomizedEloquentRaftAlgorithm extends EloquentRaftAlgorithm {
    public LobotomizedEloquentRaftAlgorithm(RaftState state, RaftTransport transport) {
      super(state, transport, Optional.empty());
    }

    @Override
    public synchronized void heartbeat() {
      // Do nothing
    }
  }

  @Test
  public void testLoopDetectionApplyTransition() {
    Set<String> quorum = new HashSet<>();
    quorum.add("server1");
    quorum.add("server2");

    RaftState raftState1 = new RaftState("server1", new SingleByteStateMachine(), quorum, MoreExecutors.newDirectExecutorService());
    EloquentRaftAlgorithm raftAlgorithm1 = new LobotomizedEloquentRaftAlgorithm(raftState1, transport);
    transport.bind(raftAlgorithm1);

    RaftState raftState2 = new RaftState("server2", new SingleByteStateMachine(), quorum, MoreExecutors.newDirectExecutorService());
    EloquentRaftAlgorithm raftAlgorithm2 = new LobotomizedEloquentRaftAlgorithm(raftState2, transport);
    transport.bind(raftAlgorithm2);

    // Set up a loop in the leadership graph

    raftState1.leader = Optional.of("server2");
    raftState2.leader = Optional.of("server1");

    // Attempt to send the message out -- this should not result in an infinite loop

    CompletableFuture<EloquentRaftProto.RaftMessage> messageCompletableFuture = raftAlgorithm1.receiveApplyTransitionRPC(EloquentRaftProto.ApplyTransitionRequest.newBuilder().setTerm(0).setTransition(ByteString.copyFrom(new byte[]{1})).build(), false);

    transport.waitForSilence();
    transport.stop(); // waits for silence

    assertTrue(messageCompletableFuture.isDone());
  }

  @Test
  public void testLoopDetectionAddServer() {
    Set<String> quorum = new HashSet<>();
    quorum.add("server1");
    quorum.add("server2");

    RaftState raftState1 = new RaftState("server1", new SingleByteStateMachine(), quorum, MoreExecutors.newDirectExecutorService());
    EloquentRaftAlgorithm raftAlgorithm1 = new LobotomizedEloquentRaftAlgorithm(raftState1, transport);
    transport.bind(raftAlgorithm1);

    RaftState raftState2 = new RaftState("server2", new SingleByteStateMachine(), quorum, MoreExecutors.newDirectExecutorService());
    EloquentRaftAlgorithm raftAlgorithm2 = new LobotomizedEloquentRaftAlgorithm(raftState2, transport);
    transport.bind(raftAlgorithm2);

    // Set up a loop in the leadership graph

    raftState1.leader = Optional.of("server2");
    raftState2.leader = Optional.of("server1");

    // Attempt to send the message out -- this should not result in an infinite loop

    CompletableFuture<EloquentRaftProto.RaftMessage> messageCompletableFuture = raftAlgorithm1.receiveAddServerRPC(EloquentRaftProto.AddServerRequest.newBuilder().setNewServer("server3").build());

    transport.waitForSilence();
    transport.stop(); // waits for silence

    assertTrue(messageCompletableFuture.isDone());
  }

  @Test
  public void testLoopDetectionRemoveServer() {
    Set<String> quorum = new HashSet<>();
    quorum.add("server1");
    quorum.add("server2");

    RaftState raftState1 = new RaftState("server1", new SingleByteStateMachine(), quorum, MoreExecutors.newDirectExecutorService());
    EloquentRaftAlgorithm raftAlgorithm1 = new LobotomizedEloquentRaftAlgorithm(raftState1, transport);
    transport.bind(raftAlgorithm1);

    RaftState raftState2 = new RaftState("server2", new SingleByteStateMachine(), quorum, MoreExecutors.newDirectExecutorService());
    EloquentRaftAlgorithm raftAlgorithm2 = new LobotomizedEloquentRaftAlgorithm(raftState2, transport);
    transport.bind(raftAlgorithm2);

    // Set up a loop in the leadership graph

    raftState1.leader = Optional.of("server2");
    raftState2.leader = Optional.of("server1");

    // Attempt to send the message out -- this should not result in an infinite loop

    CompletableFuture<EloquentRaftProto.RaftMessage> messageCompletableFuture = raftAlgorithm1.receiveRemoveServerRPC(EloquentRaftProto.RemoveServerRequest.newBuilder().setOldServer("server3").build());

    transport.waitForSilence();
    transport.stop(); // waits for silence

    assertTrue(messageCompletableFuture.isDone());
  }
}