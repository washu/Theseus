package ai.eloquent.raft.algorithm;

import ai.eloquent.raft.EloquentRaftProto.*;
import ai.eloquent.raft.*;
import ai.eloquent.test.SlowTests;
import ai.eloquent.util.Pointer;
import ai.eloquent.util.RunnableThrowsException;
import ai.eloquent.util.SafeTimerMock;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

/**
 * Unit test the core functionalities in a {@link RaftAlgorithm}.
 * This class should be overridden with particular algorithms and/or transports.
 *
 * @author <a href="mailto:gabor@eloquent.ai">Gabor Angeli</a>
 */
@SuppressWarnings({"ConstantConditions", "SameParameterValue"})
public abstract class AbstractRaftAlgorithmTest {
  /**
   * An SLF4J Logger for this class.
   */
  @SuppressWarnings("unused")
  private static final Logger log = LoggerFactory.getLogger(AbstractRaftAlgorithmTest.class);


  //
  // --------------------------------------------------------------------------
  // ABSTRACT METHODS
  // --------------------------------------------------------------------------
  //


  /**
   * Create a raft implementation with the given initial state.
   * Note that all of these nodes should be on the same transport.
   *
   * @param state The initial state for the Raft cluster
   *
   * @return A raft implementation.
   */
  protected abstract RaftAlgorithm create(RaftState state);


  /**
   * Wait for silence on the transport.
   */
  protected abstract void waitForSilence(RaftAlgorithm algorithm);


  /**
   * @see #waitForSilence(RaftAlgorithm)
   */
  private void waitForSilence(RaftAlgorithm[] algorithms) {
    for (RaftAlgorithm node : algorithms) {
      waitForSilence(node);
    }
  }


  /**
   * If true, we are running on a stable transport.
   * Some more fine-grained tests require a stable transport.
   *
   * @return True if we're running on a stable transport
   */
  protected abstract boolean isStableTransport();


  /**
   * Returns the transport we're using.
   */
  protected abstract RaftTransport transport();


  /**
   * Schedule a task on the transport. This will be done |count| times at the given
   * interval.
   *
   * @param interval The interval of the task -- that is, the time between doing it.
   * @param count The number of times to do the task.
   * @param task The task we're performing. This takes as input the current time in millis.
   */
  protected abstract void schedule(long interval, int count, BiConsumer<Integer, Long> task);


  /**
   * Create a nework partition of the given nodes, separating them from the rest of the cluster
   * (but allowing them to talk to each other just fine).
   *
   * @param fromMillis The time at which to install the network partition.
   * @param toMillis The time at which to lift the network partition.
   * @param nodeNames The names of the nodes forming the new partition. These can talk to each other,
   *                  but cannot talk to anyone else in the cluster (or visa versa).
   */
  protected abstract void partitionOff(long fromMillis, long toMillis, String... nodeNames);


  /**
   * The current time in millis. This is often mocked by the transport.
   */
  protected abstract long now();


  //
  // --------------------------------------------------------------------------
  // HELPERS
  // --------------------------------------------------------------------------
  //


  /** Assert that the given runnable should throw the given exception (or something compatible). */
  protected void assertException(RunnableThrowsException r, Class<? extends Throwable> expectedException) {
    @Nullable
    Throwable exception = null;
    try {
      r.run();
    } catch (Throwable t) {
      exception = t;
    }
    assertNotNull("Expected exception of type " + expectedException.getSimpleName() + " but did not get one.",
        exception);
    assertTrue("Expected exception of type " + expectedException.getSimpleName() +
            " but got exception of type " + exception.getClass().getSimpleName() + ". These are incompatible.",
        expectedException.isAssignableFrom(exception.getClass()));
  }


  /**
   * Create a raft implementation with the given server name, using a default cluster and a {@link SingleByteStateMachine}.
   *
   * @param serverName The name of the server.
   *
   * @return A new RaftAlgorithm implementation, defined by {@link #create(RaftState)}
   *
   * @see #create(RaftState)
   */
  protected RaftAlgorithm create(String serverName) {
    return create(new RaftState(serverName, new SingleByteStateMachine(), MoreExecutors.newDirectExecutorService()));

  }


  /**
   * Stop the transport and the nodes on it.
   *
   * @param nodes The nodes to stop
   */
  public void stop(RaftAlgorithm... nodes) {
    // 1. Stop the algorithm
    for (RaftAlgorithm node : nodes) {
      this.waitForSilence(node);
    }

    // 2. Stop the nodes
    for (RaftAlgorithm node : nodes) {
      // 2.1. Stop the node
      if (node instanceof SingleThreadedRaftAlgorithm) {
        ((SingleThreadedRaftAlgorithm) node).flush(() -> {
          RaftTransport transport = node.getTransport();
          if (transport instanceof LocalTransport) {
            ((LocalTransport) transport).waitForSilence();
          }
        });
      }
      node.stop(true);
      // 2.2. If we have a mock transport, assert that there were no errors on it
      if (node instanceof EloquentRaftAlgorithm) {
        if (((EloquentRaftAlgorithm) node).transport instanceof LocalTransport) {
          ((LocalTransport) ((EloquentRaftAlgorithm) node).transport).assertNoErrors();
        }
      } else if (node instanceof SingleThreadedRaftAlgorithm) {
        if (((SingleThreadedRaftAlgorithm) node).impl instanceof EloquentRaftAlgorithm) {
          if (((EloquentRaftAlgorithm) ((SingleThreadedRaftAlgorithm) node).impl).transport instanceof LocalTransport) {
            ((LocalTransport) ((EloquentRaftAlgorithm) ((SingleThreadedRaftAlgorithm) node).impl).transport).assertNoErrors();
          }
        }
      }
    }

    // 3. Stop the transport
    if (nodes.length > 0 && nodes[0] instanceof EloquentRaftAlgorithm) {
      ((EloquentRaftAlgorithm) nodes[0]).transport.stop();
    }
    if (nodes.length > 0 && nodes[0] instanceof SingleThreadedRaftAlgorithm && ((SingleThreadedRaftAlgorithm) nodes[0]).impl instanceof EloquentRaftAlgorithm) {
      ((EloquentRaftAlgorithm) ((SingleThreadedRaftAlgorithm) nodes[0]).impl).transport.stop();
    }
    log.info("Stopped time @ {}", SafeTimerMock.time());
  }


  /**
   * Bootstrap a cluster, where the first argument is set as the leader, and every other member is
   * marked as a follower.
   * This will create an initial leader node with serverName 'L', and |quorumCount| - 1 followers, with names
   * A, B, ...
   * This method will also take care of cleaning up the nodes + transport after the callback is run.
   *
   * @param nodeType The type of {@link RaftAlgorithm} we are creating.
   * @param quorumCount The size of the cluster.
   * @param shadowCount The number of non-voting cluster members.
   * @param electLeader If true, start with a leader.
   * @param callback The callback on the nodes.
   *
   * @return The final node states, once they are stopped and the underlying transports are closed.
   *         This will have length |quorumCount| + |shadowCount|, where the (initial) quorum nodes come first.
   */
  @SuppressWarnings("SameParameterValue")
  public RaftState[] bootstrap(Function<RaftState, RaftAlgorithm> nodeType, int quorumCount,
                               int shadowCount, boolean electLeader,
                               BiConsumer<RaftAlgorithm[], RaftState[]> callback) {
    RaftState[] states = new RaftState[quorumCount + shadowCount];

    // 1. Compute the node names
    assertTrue("The raft cluster should have at least one element: quorumCount=" + quorumCount, quorumCount > 0);
    List<String> nodeNames = new ArrayList<>();
    List<String> quorumNames = new ArrayList<>();
    nodeNames.add("L");
    quorumNames.add("L");
    for (int i = 1; i < quorumCount + shadowCount; ++i) {
      nodeNames.add(Character.toString((char) ('A' + (i - 1))));
      if (i < quorumCount) {
        quorumNames.add(Character.toString((char) ('A' + (i - 1))));
      }
    }
    RaftAlgorithm[] nodes = new RaftAlgorithm[states.length];

    // 2. Create the leader
    // 2.1. Create a state
    RaftState leaderState = new RaftState(
        "L",
        new RaftLog(new SingleByteStateMachine(), quorumNames, MoreExecutors.newDirectExecutorService()));
    assertFalse(leaderState.isLeader());
    // 2.2. Win an election by default
    if (electLeader) {
      leaderState.elect(0L);  // start at time 0
      assertTrue(leaderState.isLeader());
    }
    // 2.3. Save the node
    states[0] = leaderState;
    nodes[0] = nodeType.apply(leaderState);

    // 3. Create the followers
    for (int i = 1; i < quorumCount; ++i) {
      // 3.1. Create a state
      RaftState followerState = new RaftState(
          nodeNames.get(i),
          new RaftLog(new SingleByteStateMachine(), quorumNames, MoreExecutors.newDirectExecutorService()));
      if (electLeader) {
        followerState.leader = Optional.of("L");
      }
      // 3.2. Save the node
      states[i] = followerState;
      nodes[i] = nodeType.apply(followerState);
    }

    // 3. Create the shadows
    for (int i = 0; i < shadowCount; ++i) {
      // 3.1. Create a state
      RaftState followerState = new RaftState(
          nodeNames.get(quorumCount + i),
          new RaftLog(new SingleByteStateMachine(), quorumNames, MoreExecutors.newDirectExecutorService()));
      if (electLeader) {
        followerState.leader = Optional.of("L");
      }
      // 3.2. Save the node
      states[quorumCount + i] = followerState;
      nodes[quorumCount + i] = nodeType.apply(followerState);
    }

    try {
      // 5. Call the callback
      callback.accept(nodes, states);
    } finally {

      // 6. Close the nodes
      stop(nodes);
    }

    // 5. Return
    return states;
  }


  /**
   * Bootstrap a 3 node cluster with the default creator.
   *
   * @see #bootstrap(Function, int, int, boolean, BiConsumer)
   */
  protected RaftState[] bootstrap(BiConsumer<RaftAlgorithm[], RaftState[]> callback) {
    return bootstrap(this::create, 3, 0, true, callback);
  }


  /**
   * Bootstrap a 3 node quorum plus 2 shadow nodes, all with the default creator.
   *
   * @see #bootstrap(Function, int, int, boolean, BiConsumer)
   */
  protected RaftState[] bootstrapWithShadows(BiConsumer<RaftAlgorithm[], RaftState[]> callback) {
    return bootstrap(this::create, 3, 2, true, callback);
  }


  /**
   * Bootstrap a 3 node cluster with the default creator, but do not
   * install a leader.
   * This is useful for, e.g., testing elections without another leader competing for attention.
   *
   * @see #bootstrap(Function, int, int, boolean, BiConsumer)
   */
  protected RaftState[] bootstrapLeaderless(BiConsumer<RaftAlgorithm[], RaftState[]> callback) {
    return bootstrap(this::create, 3, 0, false, callback);
  }


  /**
   * Run some basic checks on the final states of nodes.
   * This is only valid if we're operating in normal operation once the channel is silent.
   *
   * @param closedNodes The final node states.
   * @param leaderIndex The index of the leader. If -1, we allow any single leader
   */
  @SuppressWarnings("ConstantConditions")
  private void basicSuccessTests(RaftState[] closedNodes, int leaderIndex, Optional<Set<String>> expectedMembership, boolean checkConsistentCommit) {
    // 1. Check that we have one and exactly one leader
    assertEquals("There should be exactly one leader", 1, Arrays.stream(closedNodes).filter(RaftState::isLeader).count());
    RaftState leader;
    if (leaderIndex >= 0) {
      leader = closedNodes[leaderIndex];
    } else {
      leader = Arrays.stream(closedNodes).filter(RaftState::isLeader).findFirst().get();
    }
    assertTrue("The node '"+leader.serverName+"' should be leader", leader.isLeader());

    // 2. Check that heartbeat replies were received and logged
    assertTrue("Leader should have received heartbeats", leader.lastMessageTimestamp.isPresent());
    assertEquals("The leader should know about other servers", closedNodes.length - 1, leader.lastMessageTimestamp.get().keySet().size());
    assertTrue("The leader should have heartbeats from other servers: " + leader.lastMessageTimestamp.get(),
        leader.lastMessageTimestamp.get().values().stream().allMatch(x -> x > 0));

    // 3. Check that cluster membership is intact
    expectedMembership.ifPresent(membership -> {
      for (RaftState state : closedNodes) {
        assertEquals("Cluster membership is different on node " + state.serverName,
            membership, new HashSet<>(state.log.getQuorumMembers()));
      }
    });

    // 4. Check that everyone's state machine has the same value
    if (checkConsistentCommit && leader.log.stateMachine instanceof SingleByteStateMachine) {
      assertEquals("Everyone's state machine should hold the same value",
          1, Arrays.stream(closedNodes).map(x -> ((SingleByteStateMachine) x.log.stateMachine).value).collect(Collectors.toSet()).size());
    }
  }


  /** @see #basicSuccessTests(RaftState[], int, Optional, boolean) */
  private void basicSuccessTests(RaftState[] closedNodes, int leaderIndex) {
    Set<String> membership = Arrays.stream(closedNodes).map(x -> x.serverName).collect(Collectors.toSet());
    basicSuccessTests(closedNodes, leaderIndex, Optional.of(membership), true);
  }


  /**
   * Run the basic tests for after we've submitted a number of transitions to the system.
   * This ensures that:
   *
   * <ol>
   *   <li>The RPCs were successful (in a stable transport) or sometimes successful (on a leaky transport)</li>
   *   <li>On a stable transport, all the state machines agree on their final state.</li>
   * </ol>
   *
   * @param closedNodes The final node states
   * @param leaderIndex The index of the leader, or -1 if any leader is OK
   * @param rpcResults The results of the RPCs. The length of this should be |transitionCount|
   * @param transitionCount The number of transitions we've made.
   */
  private void transitionSuccessTests(RaftState[] closedNodes, int leaderIndex, List<CompletableFuture<Boolean>> rpcResults, int transitionCount) {
    // 1. Assert that the commits were successful
    assertEquals("Every result should be logged", transitionCount, rpcResults.size());
    List<Boolean> completedResults = rpcResults.stream().map(x -> {
      try {
        return x.get(1, TimeUnit.SECONDS);
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        return false;
      }
    }).collect(Collectors.toList());
    long successCount = completedResults.stream().filter(x -> x).count();
    if (isStableTransport()) {
      assertEquals("Every RPC should have been successful", transitionCount, successCount);
    } else {
      assertTrue("Some, (perhaps all) RPCs should have been successful on a leaky transport", successCount > 0 && successCount <= transitionCount);
    }

    // 2. Assert that everyone agrees on the value of the state machine
    if (isStableTransport()) {  // if we're unstable, who knows when we come to consensus actually
      if (closedNodes[0].log.stateMachine instanceof SingleByteStateMachine) {
        byte leaderState;
        if (leaderIndex >= 0) {
          assertTrue("Selected leader is not the leader", closedNodes[leaderIndex].isLeader());
          leaderState = ((SingleByteStateMachine) closedNodes[0].log.stateMachine).value;
        } else {
          leaderState = Arrays.stream(closedNodes).filter(RaftState::isLeader).findAny().map(x -> ((SingleByteStateMachine) x.log.stateMachine).value).orElse((byte) -2);
        }
        for (RaftState node : closedNodes) {
          assertEquals("Everyone should agree on the state machine's state", leaderState, ((SingleByteStateMachine) node.log.stateMachine).value);
        }
      }
    }
  }


  /**
   * Run the basic success tests, assuming the default leader (i.e., index 0 -- node 'L')
   *
   * @param closedNodes The final node states.
   */
  private void basicSuccessTests(RaftState[] closedNodes) {
    basicSuccessTests(closedNodes, 0);
  }


  /**
   * Wait until the given time before continuing.
   *
   * @param time the transport time to wait until.
   */
  private void waitMillis(long time) {
    final Pointer<Boolean> done = new Pointer<>(false);
    Runnable r = () -> schedule(time, 1, (i, now) -> {
        synchronized (done) {
          done.set(true);
          done.notifyAll();
        }
      });
    if (this instanceof RaftAlgorithmWithMockTransportTest) {
      ((RaftAlgorithmWithMockTransportTest) this).transport.synchronizedRun(r);
    } else {
      r.run();
    }
    synchronized (done) {
      while (!done.dereference().orElse(false)) {
        try {
          done.wait();
        } catch (InterruptedException ignored) {}
      }
    }
  }


  /**
   * Suppress trace logs for this block of code
   *
   * @param fn The code to run 'quietly'
   */
  private void quietly(Runnable fn) {
    int logLevel = RaftLog.level();
    RaftLog.setLevel("info");
    try {
      fn.run();
    } finally {
      RaftLog.setLevel(logLevel);
    }
  }


  /**
   * Submit a transition to the given Raft node.
   *
   * @param node The Raft node we're submitting a transition to.
   * @param data The data we're submitting (in this case, a single byte transition)
   *
   * @return The future for whether the transition was successful.
   */
  private CompletableFuture<Boolean> transition(RaftAlgorithm node, int data) {
    return node.receiveRPC(RaftTransport.mkRaftRPC(node.serverName(),
        EloquentRaftProto.ApplyTransitionRequest.newBuilder()
            .setTransition(ByteString.copyFrom(new byte[]{(byte) data}))
            .build())
    ).thenApply(reply -> reply.getApplyTransitionReply().getSuccess());
  }


  /**
   * Transition, and then wait for the resulting state to update.
   *
   * @param node The Raft node we're submitting a transition to.
   * @param state The node's state (or, another node's state. We don't care one way or the other).
   *              This is the state we look up the value in.
   * @param data The data we're submitting (in this case, a single byte transition)
   * @param beforeReturn Some code to run between a success and before we lookup the value of the state machine.
   *
   * @return The value of the state machine, if the transition was successful.
   */
  private Optional<Integer> transitionAndWait(RaftAlgorithm node, RaftState state, int data, Runnable beforeReturn) {
    Assume.assumeTrue(state.log.stateMachine instanceof SingleByteStateMachine);
    try {
      Boolean success = transition(node, data).get(1, TimeUnit.SECONDS);
      if (success) {
        beforeReturn.run();
        return Optional.of((int) ((SingleByteStateMachine) state.log.stateMachine).value);
      } else {
        return Optional.empty();
      }
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      log.info("Transition failed: ", e);
      throw new NoSuchElementException();
    }
  }


  /**
   * Submit the AddServer RPC to a given node.
   *
   * @param node The node we are calling the RPC on.
   * @param serverName The name of the server we are adding to the cluster.
   *
   * @return A future for when we've added the server, with the success value of the addition.
   */
  private CompletableFuture<MembershipChangeStatus> addServer(RaftAlgorithm node, String serverName) {
    return node.receiveRPC(RaftTransport.mkRaftRPC(node.serverName(),
        EloquentRaftProto.AddServerRequest.newBuilder()
            .setNewServer(serverName)
            .build())
    ).thenApply(reply -> reply.getAddServerReply().getStatus());
  }


  /**
   * Submit the RemoveServer RPC to a given node.
   *
   * @param node The node we are calling the RPC on.
   * @param serverName The name of the server we are removing to the cluster.
   *
   * @return A future for when we've removed the server, with the success value of the removal.
   */
  private CompletableFuture<MembershipChangeStatus> removeServer(RaftAlgorithm node, String serverName) {
    return node.receiveRPC(RaftTransport.mkRaftRPC(node.serverName(),
        EloquentRaftProto.RemoveServerRequest.newBuilder()
            .setOldServer(serverName)
            .build())
    ).thenApply(reply -> reply.getRemoveServerReply().getStatus());
  }


  //
  // --------------------------------------------------------------------------
  // BASIC TESTS
  // --------------------------------------------------------------------------
  //


  /**
   * Test creating a Raft algorithm, just to make sure we don't get any trivial exceptions.
   */
  @Test
  public void testCreate() {
    create("A");
  }


  @Test
  public void testBootstrapCluster() {
    // Bootstrap
    bootstrap(this::create, 3, 2, true, (nodes, states) -> {
      // Check some basics
      assertEquals("All the nodes should have been passed back from bootstrap()",
          5, nodes.length);
      assertEquals("Nodes and states should have the same length",
          nodes.length, states.length);
      // Check that the leader is the leader
      assertTrue("The first node in the bootstrapped cluster should be the leader after bootstrapping",
          nodes[0].mutableState().isLeader());
      assertTrue("The first node in the bootstrapped cluster should have a nextIndex",
          nodes[0].mutableState().nextIndex.isPresent());
      assertEquals("The first node in the bootstrapped cluster should be the one with name 'L'",
          "L", nodes[0].mutableState().serverName);
      // Check that the followers are followers
      for (int i = 1; i < nodes.length; ++i) {
        assertFalse("The subsequent nodes in the bootstrapped cluster should be followers",
            nodes[i].mutableState().isLeader());
        String expectedNodeName = Character.toString((char) ('A' + (i - 1)));
        assertEquals("The name is incorrect for the followers; bootstrap() apparently reordered things (index=" + i + ")",
            expectedNodeName, nodes[i].mutableState().serverName);
        if (i >= 3) {
          for (int k = 0; k < 5; ++k) {
            assertFalse("Shadow nodes should not be in the quorum anywhere", nodes[k].mutableState().log.getQuorumMembers().contains(expectedNodeName));
          }
        }
      }
    });
  }


  //
  // --------------------------------------------------------------------------
  // APPEND ENTRIES RPC
  // --------------------------------------------------------------------------
  //


  /**
   * This tests sending empty appendEntries to other boxes in the happy path case.
   */
  @Test
  public void testHeartbeatReply() {
    Assume.assumeTrue(isStableTransport());  // requires a stable transport
    RaftState[] closedNodes = bootstrap((nodes, states) -> nodes[0].heartbeat());
    basicSuccessTests(closedNodes);
  }


  /**
   * An error case for when we try to issue a heartbeat, but we're not the leader.
   * Nothing should happen.
   */
  @Test
  public void testHeartbeatNotLeader() {
    RaftState[] closedNodes = bootstrap((nodes, states) -> {
      nodes[1].heartbeat();  // not the leader!
      for (RaftAlgorithm node : nodes) {
        waitForSilence(node);
      }
    });
    assertTrue(closedNodes[0].isLeader());
    assertFalse(closedNodes[1].isLeader());
    assertTrue(closedNodes[0].lastMessageTimestamp.isPresent());
    assertTrue(closedNodes[0].lastMessageTimestamp.get().containsKey("A"));
    assertFalse("Issued a heartbeat but we weren't the leader!", closedNodes[0].lastMessageTimestamp.get().get("A") > 0);
    assertTrue(closedNodes[0].lastMessageTimestamp.get().containsKey("B"));
    assertFalse("Issued a heartbeat but we weren't the leader!", closedNodes[0].lastMessageTimestamp.get().get("B") > 0);
  }


  /**
   * This tests sending an appendEntries with real entries in it
   * This checks that after one heartbeat, only the leader sees the committed entry.
   */
  @Test
  public void testCommitOnLeader() {
    Assume.assumeTrue(isStableTransport());  // requires a stable transport
    RaftState[] closedNodes = bootstrap((nodes, states) -> {
      states[0].transition(new byte[]{42});
      nodes[0].heartbeat();
    });
    basicSuccessTests(closedNodes, 0, Optional.of(new HashSet<>(Arrays.asList("L", "A", "B"))), false);

    // 1. Check that the entries were added
    Assume.assumeTrue(closedNodes[0].log.stateMachine instanceof SingleByteStateMachine);
    assertEquals("Leader should have a committed entry", 1, closedNodes[0].log.getAllUncompressedEntries().size());
    assertEquals("Value of state machine on leader should be 42 (did we fail to commit?)", 42, ((SingleByteStateMachine) closedNodes[0].log.stateMachine).value);
    for (int i = 1; i < closedNodes.length; ++i) {
      RaftState follower = closedNodes[i];
      assertEquals("Follower should have the entry appended", 1, follower.log.getAllUncompressedEntries().size());
      assertEquals("Follower should have the committed entry", 42, ((SingleByteStateMachine) follower.log.stateMachine).value);
    }
  }


  /**
   * This tests sending an appendEntries with real entries in it.
   * This checks that after two heartbeats, both the leader and the followers see the committed entry.
   */
  @Test
  public void testCommitToFollowers() {
    Assume.assumeTrue(isStableTransport());  // requires a stable transport
    RaftState[] closedNodes = bootstrap((nodes, states) -> {
      states[0].transition(new byte[]{42});
      nodes[0].heartbeat();
      waitForSilence(nodes[0]);
      nodes[0].heartbeat();
    });
    basicSuccessTests(closedNodes);

    // 1. Check that the entries were added
    Assume.assumeTrue(closedNodes[0].log.stateMachine instanceof SingleByteStateMachine);
    assertEquals("Leader should have a committed entry", 1, closedNodes[0].log.getAllUncompressedEntries().size());
    assertEquals("Value of state machine on leader should be 42 (did we fail to commit?)", 42, ((SingleByteStateMachine) closedNodes[0].log.stateMachine).value);
    for (int i = 1; i < closedNodes.length; ++i) {
      RaftState follower = closedNodes[i];
      assertEquals("Follower should have a committed entry", 1, follower.log.getAllUncompressedEntries().size());
      assertEquals("Followers should also see committed entry now", 42L, ((SingleByteStateMachine) follower.log.stateMachine).value);  // IMPORTANT LINE IS THIS
    }
  }


  /**
   * This tests our ability to send a heartbeat when we have a snapshot present.
   */
  @Test
  public void testCommitFromInconsistentState() {
    Assume.assumeTrue(isStableTransport());  // requires a stable transport
    RaftState[] closedNodes = bootstrap((nodes, states) -> {
      // Set some transitions. If the more basic commit tests fail, this step of this test will fail as well
      states[0].transition(new byte[]{42});
      states[0].transition(new byte[]{43});
      nodes[0].heartbeat(); waitForSilence(nodes[0]);
      nodes[0].heartbeat(); waitForSilence(nodes[0]);

      // Nuke someone's log -- oops!
      states[1].log.unsafeSetLog(Collections.emptyList());

      // Run some heartbeats to arrive at consensus again
      nodes[0].heartbeat(); waitForSilence(nodes[0]);
    });
    basicSuccessTests(closedNodes);

    // 1. Check that the entries were added
    Assume.assumeTrue(closedNodes[0].log.stateMachine instanceof SingleByteStateMachine);
    for (RaftState node : closedNodes) {
      assertEquals("Node (" + node.serverName + ") should have two committed entries",
          2, node.log.getAllUncompressedEntries().size());
      assertEquals("Node (" + node.serverName + ") should  see committed entry now",
          43L, ((SingleByteStateMachine) node.log.stateMachine).value);  // IMPORTANT LINE IS THIS
    }
  }


  //
  // --------------------------------------------------------------------------
  // INSTALL SNAPSHOT RPC
  // --------------------------------------------------------------------------
  //


  /**
   * This tests our ability to send a heartbeat when we have a snapshot present.
   */
  @Test
  public void testCommitFromSnapshot() {
    Assume.assumeTrue(isStableTransport());  // requires a stable transport
    RaftState[] closedNodes = bootstrap((nodes, states) -> {
      states[0].transition(new byte[]{42});
      states[0].commitUpTo(1, 0);
      states[0].log.forceSnapshot();
      assertEquals("Raft should not have any entries after a snapshot", 0, states[0].log.getAllUncompressedEntries().size());
      nodes[0].heartbeat();
      waitForSilence(nodes[0]);
      nodes[0].heartbeat();
    });
    basicSuccessTests(closedNodes);

    // 1. Check that the entries were added
    Assume.assumeTrue(closedNodes[0].log.stateMachine instanceof SingleByteStateMachine);
    for (RaftState node : closedNodes) {
      assertEquals("Node (" + node.serverName + ") should not have anything in their log",
          0, node.log.getAllUncompressedEntries().size());
      assertEquals("Node (" + node.serverName + ") should have a committed entry",
          1, node.log.getCommitIndex());
      assertEquals("Node (" + node.serverName + ") should  see committed entry now",
          42L, ((SingleByteStateMachine) node.log.stateMachine).value);  // IMPORTANT LINE IS THIS
    }
  }


  /**
   * Identical to {@link #testCommitFromInconsistentState()}, but the leader has a snapshot that it should
   * commit from.
   */
  @Test
  public void testCommitFromInconsistentStateWithSnapshot() {
    Assume.assumeTrue(isStableTransport());  // requires a stable transport
    RaftState[] closedNodes = bootstrap((nodes, states) -> {
      // Set some transitions. If the more basic commit tests fail, this step of this test will fail as well
      states[0].transition(new byte[]{42});
      states[0].transition(new byte[]{43});
      nodes[0].heartbeat(); waitForSilence(nodes[0]);
      nodes[0].heartbeat(); waitForSilence(nodes[0]);
      states[0].log.forceSnapshot();  // IMPORTANT LINE IS THIS

      // Nuke someone's log -- oops!
      states[1].log.unsafeSetLog(Collections.emptyList());

      // Run some heartbeats to arrive at consensus again
      nodes[0].heartbeat(); waitForSilence(nodes[0]);
    });
    basicSuccessTests(closedNodes);

    // 1. Check
    assertEquals("Leader should have no committed entries",
        0, closedNodes[0].log.getAllUncompressedEntries().size());
    assertEquals("Node A should have no committed entries (got snapshot)",
        0, closedNodes[1].log.getAllUncompressedEntries().size());
    assertEquals("Node B should still have 2 committed entries (has not yet gotten snapshot)",
        2, closedNodes[2].log.getAllUncompressedEntries().size());

    // 2. Check that everyone has their states committed
    Assume.assumeTrue(closedNodes[0].log.stateMachine instanceof SingleByteStateMachine);
    for (RaftState node : closedNodes) {
      assertEquals("Node (" + node.serverName + ") should see committed entries",
          43L, ((SingleByteStateMachine) node.log.stateMachine).value);  // IMPORTANT LINE IS THIS
    }
  }


  //
  // --------------------------------------------------------------------------
  // ELECTIONS
  // --------------------------------------------------------------------------
  //


  /**
   * This tests the happy path for an election.
   * That is, a node marks itself as a candidate, and gets elected without trouble.
   */
  @Test
  public void testElectionHappyPath() {
    Assume.assumeTrue(isStableTransport());  // requires a stable transport
    RaftState[] closedNodes = bootstrapLeaderless((nodes, states) -> {
      nodes[0].triggerElection();
      // Wait a while for the election to resolve
      for (RaftAlgorithm node : nodes) {
        schedule(nodes[0].heartbeatMillis(), (int) (nodes[0].electionTimeoutMillisRange().end / nodes[0].heartbeatMillis()), (i, now) -> node.heartbeat());
      }
    });
    basicSuccessTests(closedNodes);

    // 1. Check that everyone voted for the leader
    assertEquals("The candidate should have all three votes", 3, closedNodes[0].votesReceived.size());
  }


  /**
   * This tests that a node can snag leadership away from the main leader.
   */
  @Test
  public void testSnagLeadership() {
    Assume.assumeTrue(isStableTransport());  // requires a stable transport
    RaftState[] closedNodes = bootstrap((nodes, states) -> {
      nodes[1].triggerElection();
      // Wait a while for the election to resolve
      for (RaftAlgorithm node : nodes) {
        schedule(nodes[0].heartbeatMillis(), (int) (nodes[0].electionTimeoutMillisRange().end / nodes[0].heartbeatMillis()), (i, now) -> node.heartbeat());
      }
    });
    basicSuccessTests(closedNodes, 1);

    // 1. Check that everyone voted for the leader
    assertEquals("The candidate should have all three votes", 3, closedNodes[1].votesReceived.size());
  }


  /**
   * This tests that we resolve an election even if two nodes request it at the same time.
   */
  @Test
  public void testConcurrentElectionNoInitialLeader() {
    Assume.assumeTrue(isStableTransport());  // requires a stable transport
    RaftState[] closedNodes = bootstrapLeaderless((nodes, states) -> {
      // Everyone triggers an election
      nodes[0].triggerElection();
      nodes[1].triggerElection();
      nodes[2].triggerElection();
      // Wait a while for the election to resolve
      for (RaftAlgorithm node : nodes) {
        schedule(nodes[0].heartbeatMillis(), (int) (nodes[0].electionTimeoutMillisRange().end / nodes[0].heartbeatMillis()), (i, now) -> node.heartbeat());
      }
    });
    basicSuccessTests(closedNodes, -1);
  }


  /**
   * This tests that we resolve an election even if two nodes request it at the same time.
   */
  @Test
  public void testOrganicElectionNoInitialLeader() {
    Assume.assumeTrue(isStableTransport());  // requires a stable transport
    RaftState[] closedNodes = bootstrapLeaderless((nodes, states) -> {
      // Wait until an election happens
      for (RaftAlgorithm node : nodes) {
        schedule(nodes[0].heartbeatMillis(), (int) (nodes[0].electionTimeoutMillisRange().end / nodes[0].heartbeatMillis()), (i, now) -> node.heartbeat());
      }
    });
    basicSuccessTests(closedNodes, -1);
  }


  /**
   * This tests that we resolve an election even if two nodes request it at the same time.
   */
  @Test
  public void testConcurrentElectionWithInitialLeader() {
    Assume.assumeTrue(isStableTransport());  // requires a stable transport
    RaftState[] closedNodes = bootstrap((nodes, states) -> {
      // Everyone triggers an election
      nodes[1].triggerElection();
      nodes[2].triggerElection();
      waitMillis(nodes[0].electionTimeoutMillisRange().end);
    });
    basicSuccessTests(closedNodes, -1);
  }


  /**
   * Test that we can run for a while and have everything be happy and functional at the end.
   */
  @Test
  public void testLeaderPartitionedOff() {
    Assume.assumeTrue(isStableTransport());  // requires a stable transport
    // 1. Take down the leader at t=0
    partitionOff(0, Long.MAX_VALUE, "L");
    // 2. Run the cluster for an election timeout (+ buffer)
    RaftState[] closedNodes = bootstrap((nodes, states) -> {
      for (RaftAlgorithm node : nodes) {
        schedule(nodes[0].heartbeatMillis(), (int) (nodes[0].electionTimeoutMillisRange().end / nodes[0].heartbeatMillis()) + 1, (i, now) -> node.heartbeat());
      }
    });
    // 3. Make sure we got a new leader
    assertTrue("Original leader should still think he's a leader",
        closedNodes[0].isLeader());
    assertTrue("We should have a new leader from the remaining cluster",
        closedNodes[1].isLeader() || closedNodes[2].isLeader());
  }


  /**
   * Test that leadership changes hands if the leader disappears and then returns
   */
  @Test
  public void testLeaderPartitionedOffThenReturns() {
    Assume.assumeTrue(isStableTransport());  // requires a stable transport
    long electionTimeout = RaftAlgorithm.DEFAULT_ELECTION_RANGE.end + 200;  // come back online after someone else has elected themselves
    // 1. Take down the leader at t=0 until a bit after the election timeout
    partitionOff(0, electionTimeout, "L");
    // 2. Run the cluster for an election timeout (+ buffer)
    RaftState[] closedNodes = bootstrap((nodes, states) -> {
      int numHeartbeats = (int) (RaftAlgorithm.DEFAULT_ELECTION_RANGE.end / nodes[0].heartbeatMillis()) + 8;
      for (RaftAlgorithm node : nodes) {
        schedule(nodes[0].heartbeatMillis(), numHeartbeats, (i, now) -> node.heartbeat());
      }
    });
    // 3. The old leader should have resigned
    assertFalse("Original leader should have resigned", closedNodes[0].isLeader());
    assertTrue("We should have a new leader from the remaining cluster",
        closedNodes[1].isLeader() || closedNodes[2].isLeader());
  }


  /**
   * Test that we can run for a while and have everything be happy and functional at the end.
   */
  @Test
  public void testLeaderStopsHeartbeats() {
    Assume.assumeTrue(isStableTransport());  // requires a stable transport
    // 1. Run the cluster for an election timeout (+ buffer)
    RaftState[] closedNodes = bootstrap((nodes, states) -> {
      for (int i = 0; i < nodes.length; ++i) {
        RaftAlgorithm node = nodes[i];
        if (i == 0) {
          // kill the leader after 10 heartbeats
          schedule(node.heartbeatMillis(), 10, (k, now) -> node.heartbeat());
        } else {
          // kill everyone else after (10 + election_timeout + epsilon) heartbeats
          schedule(node.heartbeatMillis(), (int) (node.electionTimeoutMillisRange().end / node.heartbeatMillis() + 15), (k, now) -> node.heartbeat());
        }
      }
    });
    // 3. Make sure we got a new leader
    assertFalse("Original leader should have stepped down as leader",
        closedNodes[0].isLeader());
    assertTrue("We should have a new leader from the remaining cluster",
        closedNodes[1].isLeader() || closedNodes[2].isLeader());
  }


  /**
   * This tests the happy path for an election.
   * That is, a node marks itself as a candidate, and gets elected without trouble.
   */
  @Test
  public void testElectSingleServer() {
    Assume.assumeTrue(isStableTransport());  // requires a stable transport
    RaftState[] closedNodes = bootstrap(this::create, 1, 0, false, (nodes, states) -> nodes[0].triggerElection());
    assertTrue("Solitary node should have elected itself.", closedNodes[0].isLeader());
  }


  //
  // --------------------------------------------------------------------------
  // ADD SERVER
  // --------------------------------------------------------------------------
  //


  /**
   * A little helper to increase the determinism of the add/remove RPC tests below.
   */
  private void blockOnRpcResults(List<CompletableFuture<MembershipChangeStatus>> rpcResults) {
    rpcResults.forEach(future -> {
      try {
        future.get(1, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        log.error("Got error waiting for future", e);
      } catch (ExecutionException ignored) {
        // This is fine, some of the futures complete exceptionally
      } catch (TimeoutException e) {
        future.completeExceptionally(e);
      }
    });
  }


  /**
   * The happy path for adding a server to a cluster
   */
  @Test
  public void testAddServer() throws ExecutionException, InterruptedException, TimeoutException {
    Assume.assumeTrue(isStableTransport());  // requires a stable transport
    List<CompletableFuture<MembershipChangeStatus>> rpcResults = new ArrayList<>();
    RaftState[] closedNodes = bootstrapWithShadows((nodes, states) -> {
      rpcResults.add(addServer(nodes[0], "C"));
      // Block until the future completes
      blockOnRpcResults(rpcResults);
    });

    // The tests
    assertTrue("Should still have a leader", closedNodes[0].isLeader());
    assertEquals("Should have gotten an RPC result", 1, rpcResults.size());
    assertEquals(MembershipChangeStatus.OK, rpcResults.get(0).get(1, TimeUnit.SECONDS));
    assertEquals("Leader should see the new configuration", new HashSet<>(Arrays.asList("L", "A", "B", "C")), closedNodes[0].log.getQuorumMembers());
    assertEquals("Followers should see the new configuration", new HashSet<>(Arrays.asList("L", "A", "B", "C")), closedNodes[1].log.getQuorumMembers());
    assertEquals("Followers should see the new configuration", new HashSet<>(Arrays.asList("L", "A", "B", "C")), closedNodes[2].log.getQuorumMembers());
  }


  /**
   * The happy path for adding a server to a cluster from a follower nod
   */
  @Test
  public void testAddServerFromFollower() throws ExecutionException, InterruptedException, TimeoutException {
    Assume.assumeTrue(isStableTransport());  // requires a stable transport
    List<CompletableFuture<MembershipChangeStatus>> rpcResults = new ArrayList<>();
    RaftState[] closedNodes = bootstrapWithShadows((nodes, states) -> {
      rpcResults.add(addServer(nodes[1], "C"));
      // Block until the future completes
      blockOnRpcResults(rpcResults);
    });

    // The tests
    assertTrue("Should still have a leader", closedNodes[0].isLeader());
    assertEquals("Should have gotten an RPC result", 1, rpcResults.size());
    assertEquals(MembershipChangeStatus.OK, rpcResults.get(0).get(1, TimeUnit.SECONDS));
    assertEquals("Leader should see the new configuration", new HashSet<>(Arrays.asList("L", "A", "B", "C")), closedNodes[0].log.getQuorumMembers());
    assertEquals("Followers should see the new configuration", new HashSet<>(Arrays.asList("L", "A", "B", "C")), closedNodes[1].log.getQuorumMembers());
    assertEquals("Followers should see the new configuration", new HashSet<>(Arrays.asList("L", "A", "B", "C")), closedNodes[2].log.getQuorumMembers());
  }


  /**
   * Add a server to the cluster, with entries already on the leader
   */
  @Test
  public void testAddServerWithEntries() throws ExecutionException, InterruptedException, TimeoutException {
    Assume.assumeTrue(isStableTransport());  // requires a stable transport
    List<CompletableFuture<MembershipChangeStatus>> rpcResults = new ArrayList<>();
    RaftState[] closedNodes = bootstrapWithShadows((nodes, states) -> {
      // Add some of entries
      for (int i = 0; i < 5; ++i) {
        transition(nodes[0], i % 127);
      }
      // Then add a server
      rpcResults.add(addServer(nodes[0], "C"));
      // Block until the future completes
      blockOnRpcResults(rpcResults);
    });

    // The tests
    assertTrue("Should still have a leader", closedNodes[0].isLeader());
    assertEquals("Should have gotten an RPC result", 1, rpcResults.size());
    assertEquals(MembershipChangeStatus.OK, rpcResults.get(0).get(10, TimeUnit.SECONDS));
    assertEquals("Leader should see the new configuration", new HashSet<>(Arrays.asList("L", "A", "B", "C")), closedNodes[0].log.getQuorumMembers());
    assertEquals("Followers should see the new configuration", new HashSet<>(Arrays.asList("L", "A", "B", "C")), closedNodes[1].log.getQuorumMembers());
    assertEquals("Followers should see the new configuration", new HashSet<>(Arrays.asList("L", "A", "B", "C")), closedNodes[2].log.getQuorumMembers());
  }


  /**
   * Add a server to the cluster, with entries already on the leader and a snapshot present
   */
  @Test
  public void testAddServerWithEntriesAndSnapshot() throws ExecutionException, InterruptedException, TimeoutException {
    Assume.assumeTrue(isStableTransport());  // requires a stable transport
    List<CompletableFuture<MembershipChangeStatus>> rpcResults = new ArrayList<>();
    RaftState[] closedNodes = bootstrapWithShadows((nodes, states) -> {
      // Add some of entries
      for (int i = 0; i < 5; ++i) {
        transition(nodes[0], i % 127);
      }
      waitForSilence(nodes[0]);  // we want to wait for these to actually go through
      // Snapshot
      states[0].log.forceSnapshot();
      // Add some more entries
      for (int i = 0; i < 5; ++i) {
        transition(nodes[0], i % 127);
      }
      waitForSilence(nodes[0]);  // we want to wait for these to actually go through
      // Then add a server
      rpcResults.add(addServer(nodes[0], "C"));

      // Block until the future completes
      blockOnRpcResults(rpcResults);
    });

    // The tests
    assertTrue("Should still have a leader", closedNodes[0].isLeader());
    assertEquals("Should have gotten an RPC result", 1, rpcResults.size());
    assertEquals(MembershipChangeStatus.OK, rpcResults.get(0).get(1, TimeUnit.SECONDS));
    assertEquals("Leader should see the new configuration", new HashSet<>(Arrays.asList("L", "A", "B", "C")), closedNodes[0].log.getQuorumMembers());
    assertEquals("Followers should see the new configuration", new HashSet<>(Arrays.asList("L", "A", "B", "C")), closedNodes[1].log.getQuorumMembers());
    assertEquals("Followers should see the new configuration", new HashSet<>(Arrays.asList("L", "A", "B", "C")), closedNodes[2].log.getQuorumMembers());
  }


  /**
   * Cannot add a server if the leader is unreachable
   */
  @Test
  public void testAddServerFailsWhenLeaderUnreachable() throws ExecutionException, InterruptedException, TimeoutException {
    partitionOff(0, Long.MAX_VALUE, "L", "C");
    List<CompletableFuture<MembershipChangeStatus>> rpcResults = new ArrayList<>();
    RaftState[] closedNodes = bootstrapWithShadows((nodes, states) -> {
      rpcResults.add(addServer(nodes[1], "C"));
      // Block until the future completes
      blockOnRpcResults(rpcResults);
    });  // add from node 1 -> "A"

    // The tests
    assertTrue("Should still have a leader", closedNodes[0].isLeader());
    assertEquals("Should have gotten an RPC result", 1, rpcResults.size());
    assertEquals("Should not have been able to complete the RPC successfully", MembershipChangeStatus.NOT_LEADER, rpcResults.get(0).get(1, TimeUnit.SECONDS));
    assertEquals("Leader should see the old configuration", new HashSet<>(Arrays.asList("L", "A", "B")), closedNodes[0].log.getQuorumMembers());
    assertEquals("Followers should see the old configuration", new HashSet<>(Arrays.asList("L", "A", "B")), closedNodes[1].log.getQuorumMembers());
    assertEquals("Followers should see the old configuration", new HashSet<>(Arrays.asList("L", "A", "B")), closedNodes[2].log.getQuorumMembers());
  }


  /**
   * Cannot add a server if the leader cannot reach consensus on the commit.
   * Though, the leader should still behave as if it had the new configuration.
   */
  @Test
  public void testAddServerFailsWhenNoConsensus() throws ExecutionException, InterruptedException {
    partitionOff(0, Long.MAX_VALUE, "L");
    List<CompletableFuture<MembershipChangeStatus>> rpcResults = new ArrayList<>();
    RaftState[] closedNodes = bootstrapWithShadows((nodes, states) -> {
      rpcResults.add(addServer(nodes[0], "C"));  // add from leader, but leader is partitioned
      // Don't block on the futures, since they don't fail until we shutdown
    });

    blockOnRpcResults(rpcResults);
    assertTrue(rpcResults.stream().allMatch(CompletableFuture::isCompletedExceptionally));

    // The tests
    assertTrue("Should still have a leader", closedNodes[0].isLeader());
    assertEquals("Should have gotten an RPC result", 1, rpcResults.size());
    assertTrue(rpcResults.get(0).isCompletedExceptionally());
    assertEquals("Leader should have the new configuration", new HashSet<>(Arrays.asList("L", "A", "B", "C")), closedNodes[0].log.getQuorumMembers());
    assertEquals("Followers should use the old configuration", new HashSet<>(Arrays.asList("L", "A", "B")), closedNodes[1].log.getQuorumMembers());
    assertEquals("Followers should use the old configuration", new HashSet<>(Arrays.asList("L", "A", "B")), closedNodes[2].log.getQuorumMembers());
  }


  //
  // --------------------------------------------------------------------------
  // REMOVE SERVER
  // --------------------------------------------------------------------------
  //


  /**
   * The happy path for removing a server from a cluster
   */
  @Test
  public void testRemoveServer() throws ExecutionException, InterruptedException, TimeoutException {
    Assume.assumeTrue(isStableTransport());  // requires a stable transport
    List<CompletableFuture<MembershipChangeStatus>> rpcResults = new ArrayList<>();
    RaftState[] closedNodes = bootstrap((nodes, states) -> {
      rpcResults.add(removeServer(nodes[0], "B"));
      // Block until the future completes
      blockOnRpcResults(rpcResults);
    });

    // The tests
    assertTrue("Should still have a leader", closedNodes[0].isLeader());
    assertEquals("Should have gotten an RPC result", 1, rpcResults.size());
    assertEquals("Should have been able to remove server", MembershipChangeStatus.OK, rpcResults.get(0).get(1, TimeUnit.SECONDS));
    assertEquals("Leader should see the new configuration", new HashSet<>(Arrays.asList("L", "A")), closedNodes[0].log.getQuorumMembers());
    assertEquals("Followers should see the new configuration", new HashSet<>(Arrays.asList("L", "A")), closedNodes[1].log.getQuorumMembers());
    assertEquals("Followers should see the new configuration", new HashSet<>(Arrays.asList("L", "A")), closedNodes[2].log.getQuorumMembers());
  }


  /**
   * Remove a server by calling the RPC on a follower node.
   */
  @Test
  public void testRemoveServerFromFollower() throws ExecutionException, InterruptedException, TimeoutException {
    Assume.assumeTrue(isStableTransport());  // requires a stable transport
    List<CompletableFuture<MembershipChangeStatus>> rpcResults = new ArrayList<>();
    RaftState[] closedNodes = bootstrap((nodes, states) -> {
      rpcResults.add(removeServer(nodes[2], "B"));
      // Block until the future completes
      blockOnRpcResults(rpcResults);
    });  // in fact, this is the node that's dying!

    // The tests
    assertTrue("Should still have a leader", closedNodes[0].isLeader());
    assertEquals("Should have gotten an RPC result", 1, rpcResults.size());
    assertEquals("Should have been able to remove server", MembershipChangeStatus.OK, rpcResults.get(0).get(1, TimeUnit.SECONDS));
    assertEquals("Leader should see the new configuration", new HashSet<>(Arrays.asList("L", "A")), closedNodes[0].log.getQuorumMembers());
    assertEquals("Followers should see the new configuration", new HashSet<>(Arrays.asList("L", "A")), closedNodes[1].log.getQuorumMembers());
    assertEquals("Followers should see the new configuration", new HashSet<>(Arrays.asList("L", "A")), closedNodes[2].log.getQuorumMembers());
  }


  /**
   * We cannot remove a server if the leader is unreachable
   */
  @Test
  public void testRemoveServerFailsWhenLeaderUnreachable() throws InterruptedException {
    partitionOff(0, Long.MAX_VALUE, "L");
    List<CompletableFuture<MembershipChangeStatus>> rpcResults = new ArrayList<>();
    RaftState[] closedNodes = bootstrap((nodes, states) -> {
      rpcResults.add(removeServer(nodes[1], "B"));
      // Block until the future completes
      blockOnRpcResults(rpcResults);
      assertTrue(rpcResults.stream().allMatch(CompletableFuture::isCompletedExceptionally));
    });  // call from another node

    // The tests
    assertTrue("Should still have a leader", closedNodes[0].isLeader());
    assertEquals("Should have gotten an RPC result", 1, rpcResults.size());
    try {
      rpcResults.get(0).get(1, TimeUnit.SECONDS);
      fail("Should not have been able to complete the RPC successfully");
    } catch (TimeoutException | ExecutionException ignored) {}
    assertEquals("Leader should see the old configuration", new HashSet<>(Arrays.asList("L", "A", "B")), closedNodes[0].log.getQuorumMembers());
    assertEquals("Followers should see the old configuration", new HashSet<>(Arrays.asList("L", "A", "B")), closedNodes[1].log.getQuorumMembers());
    assertEquals("Followers should see the old configuration", new HashSet<>(Arrays.asList("L", "A", "B")), closedNodes[2].log.getQuorumMembers());
  }


  /**
   * We cannot commit a server removal if the leader cannot reach consensus with the rest of the cluster.
   */
  @Test
  public void testRemoveServerFailsWhenNoConsensus() throws ExecutionException, InterruptedException {
    partitionOff(0, Long.MAX_VALUE, "L");
    List<CompletableFuture<MembershipChangeStatus>> rpcResults = new ArrayList<>();
    RaftState[] closedNodes = bootstrap((nodes, states) -> {
      rpcResults.add(removeServer(nodes[0], "B"));
      // Don't block on the futures, since they don't fail until shutdown
    });  // call from leader

    blockOnRpcResults(rpcResults);
    assertTrue(rpcResults.stream().allMatch(CompletableFuture::isCompletedExceptionally));

    // The tests
    assertTrue("Should still have a leader", closedNodes[0].isLeader());
    assertEquals("Should have gotten an RPC result", 1, rpcResults.size());
    assertEquals("Leader should have the new configuration", new HashSet<>(Arrays.asList("L", "A")), closedNodes[0].log.getQuorumMembers());
    assertEquals("Followers should see the old configuration", new HashSet<>(Arrays.asList("L", "A", "B")), closedNodes[1].log.getQuorumMembers());
    assertEquals("Followers should see the old configuration", new HashSet<>(Arrays.asList("L", "A", "B")), closedNodes[2].log.getQuorumMembers());
  }


  /**
   * The happy path for removing a server from a cluster
   */
  @Test
  public void testRemoveServerLeaderStepsDown() throws ExecutionException, InterruptedException, TimeoutException {
    Assume.assumeTrue(isStableTransport());  // requires a stable transport
    List<CompletableFuture<MembershipChangeStatus>> rpcResults = new ArrayList<>();
    RaftState[] closedNodes = bootstrap((nodes, states) -> {
      rpcResults.add(removeServer(nodes[0], "L"));
      // Block until the future completes
      blockOnRpcResults(rpcResults);
    });

    // The tests
    assertFalse("Should have stepped down as leader", closedNodes[0].isLeader());
    assertEquals("Should have gotten an RPC result", 1, rpcResults.size());
    assertEquals("Should have been able to remove server", MembershipChangeStatus.OK, rpcResults.get(0).get(1, TimeUnit.SECONDS));
    assertEquals("Leader should see the new configuration", new HashSet<>(Arrays.asList("A", "B")), closedNodes[0].log.getQuorumMembers());
    assertEquals("Followers should see the new configuration", new HashSet<>(Arrays.asList("A", "B")), closedNodes[1].log.getQuorumMembers());
    assertEquals("Followers should see the new configuration", new HashSet<>(Arrays.asList("A", "B")), closedNodes[2].log.getQuorumMembers());
  }


  /**
   * Remove leader from a 2-person cluster, where the previous node disappears entirely.
   * This is complicated, since one node cannot commit if it thinks there are two nodes
   * in the cluster, and so if we add a node that doesn't respond we're a bit hosed.
   */
  @Test
  public void testRemoveSelf() {
    Assume.assumeTrue(isStableTransport());  // requires a stable transport
    RaftState[] closedNodes = bootstrap(this::create, 2, 0, true, (nodes, states) -> {
      // Run some heartbeats on follower, but not leader
      schedule(nodes[0].heartbeatMillis(), (int) (nodes[0].electionTimeoutMillisRange().end / nodes[0].heartbeatMillis() + 5), (k, now) -> {
        nodes[0].heartbeat();
        nodes[1].heartbeat();
      });
      // Important: we have to stop the node that we're removing
      RaftAlgorithm.shutdown(nodes[0], transport(), false);
    });

    // The tests
    assertFalse("Should have stepped down as leader", closedNodes[0].isLeader());
    assertEquals("Leader should see the new configuration", Collections.singleton("A"), closedNodes[0].log.getQuorumMembers());
    assertEquals("Follower should see the new configuration", Collections.singleton("A"), closedNodes[1].log.getQuorumMembers());
    assertEquals("Follower should see the committed configuration", Collections.singleton("A"), closedNodes[1].log.committedQuorumMembers);
  }


  /**
   * Remove leader from a 2-person cluster, where the previous node disappears entirely.
   * This is complicated, since one node cannot commit if it thinks there are two nodes
   * in the cluster, and so if we add a node that doesn't respond we're a bit hosed.
   */
  @Test
  public void testRemoveSelfAddsUsBackWithoutStop() throws ExecutionException, InterruptedException, TimeoutException {
    Assume.assumeTrue(isStableTransport());  // requires a stable transport
    List<CompletableFuture<MembershipChangeStatus>> rpcResults = new ArrayList<>();
    RaftState[] closedNodes = bootstrap(this::create, 2, 0, true, (nodes, states) -> {
      // Remove the server
      rpcResults.add(removeServer(nodes[0], "L"));
      // Run some heartbeats (including on server!)
      schedule(nodes[0].heartbeatMillis(), (int) (nodes[0].electionTimeoutMillisRange().end / nodes[0].heartbeatMillis() + 5), (k, now) -> {
        for (RaftAlgorithm node : nodes) { node.heartbeat(); }
      });
      // Unlike testRemoveSelf, we don't stop the node here!
//      nodes[0].stop();  // commented for documentation
      // Block until the future completes
      blockOnRpcResults(rpcResults);
    });

    // The tests
    assertFalse("Should have stepped down as leader", closedNodes[0].isLeader());
    assertEquals("Should have gotten an RPC result", 1, rpcResults.size());
    assertEquals("Should have been able to remove server", MembershipChangeStatus.OK, rpcResults.get(0).get(1, TimeUnit.SECONDS));
    assertEquals("Leader should see the new configuration", new HashSet<>(Arrays.asList("A", "L")), closedNodes[0].log.getQuorumMembers());
    assertEquals("Follower should see the new configuration", new HashSet<>(Arrays.asList("A", "L")), closedNodes[1].log.getQuorumMembers());
    assertEquals("Follower should see the committed configuration", new HashSet<>(Arrays.asList("A", "L")), closedNodes[1].log.committedQuorumMembers);
  }



  //
  // --------------------------------------------------------------------------
  // RECOVERY CASES
  // --------------------------------------------------------------------------
  //


  /**
   * We've started life in an inconsistent state, where we have two leaders sharing
   * a common node in their quorums.
   */
  @Test
  public void testRecoverFromSplitBrain() {
    Assume.assumeTrue(isStableTransport());  // requires a stable transport

    // 1. Create the states
    RaftState sA = new RaftState("A",
        new RaftLog(new SingleByteStateMachine(), Arrays.asList("A", "L"), MoreExecutors.newDirectExecutorService()));
    sA.elect(0);
    RaftState sB = new RaftState("B",
        new RaftLog(new SingleByteStateMachine(), Arrays.asList("B", "L"), MoreExecutors.newDirectExecutorService()));
    sB.elect(0);
    RaftState sL = new RaftState("L",
        new RaftLog(new SingleByteStateMachine(), Arrays.asList("L", "A"), MoreExecutors.newDirectExecutorService()));

    // 2. Create the nodes
    RaftAlgorithm A = create(sA);
    RaftAlgorithm B = create(sB);
    RaftAlgorithm L = create(sL);

    // 3. Run the system for a few cycles
    schedule(A.heartbeatMillis(), (int) (L.electionTimeoutMillisRange().end / L.heartbeatMillis() + 5), (k, now) -> {
      A.heartbeat();
      B.heartbeat();
      L.heartbeat();
    });
    stop(A, B, L);

    // 4. Everything should be splendid
    basicSuccessTests(new RaftState[]{sL, sA, sB}, -1);
  }


  /**
   * A regression where a candidate in a 2 person cluster is unable to become the leader.
   */
  @Test
  public void testRecoverFromBotchedElection() {
    Assume.assumeTrue(isStableTransport());  // requires a stable transport

    // 1. Create the states
    RaftState sL = new RaftState("L",
        new RaftLog(new SingleByteStateMachine(), Arrays.asList("L", "A"), MoreExecutors.newDirectExecutorService()));
    sL.leadership = RaftState.LeadershipStatus.CANDIDATE;
    sL.log.unsafeSetLog(Arrays.asList(
        LogEntry.newBuilder().setIndex(1).setTerm(3).setTransition(ByteString.copyFrom(new byte[]{1})).build(),
        LogEntry.newBuilder().setIndex(2).setTerm(4).setTransition(ByteString.copyFrom(new byte[]{2})).build(),
        LogEntry.newBuilder().setIndex(3).setTerm(4).setTransition(ByteString.copyFrom(new byte[]{3})).build()
    ));
    sL.setCurrentTerm(4);
    sL.voteFor(sL.serverName, now());
    assertEquals("leader log should have some entries", 3, sL.log.getLastEntryIndex());
    assertEquals("leader term should be 4", 4, sL.currentTerm);
    assertEquals("Leader should be a candidate", RaftState.LeadershipStatus.CANDIDATE, sL.leadership);
    assertEquals("Leader should have voted for themselves", Collections.singleton("L"), sL.votesReceived);
    assertEquals("leader commit index should be 0", 0, sL.commitIndex());
    RaftState sA = new RaftState("A",
        new RaftLog(new SingleByteStateMachine(), Collections.emptySet(), MoreExecutors.newDirectExecutorService()));
    sA.setCurrentTerm(4);
    sA.votedFor = Optional.of("L");
    assertEquals("follower should see no one", Collections.emptySet(), sA.log.getQuorumMembers());
    assertEquals("follower term should be 4", 4, sA.currentTerm);
    assertEquals("follower commit index should be 0", 0, sA.commitIndex());
    assertEquals("follower should have already voted for the leader", Optional.of("L"), sA.votedFor);

    // 2. Create the nodes
    RaftAlgorithm L = create(sL);
    RaftAlgorithm A = create(sA);

    // 3. Run the system for a few cycles
    schedule(A.heartbeatMillis(), (int) ((L.electionTimeoutMillisRange().end / L.heartbeatMillis()) + 5), (k, now) -> {
      L.heartbeat();
      A.heartbeat();
    });
    stop(L, A);

    // 4. Everything should be splendid
    basicSuccessTests(new RaftState[]{sL, sA}, -1);
  }


  //
  // --------------------------------------------------------------------------
  // CONTROL
  // --------------------------------------------------------------------------
  //


  /**
   * Submit a transition on the leader, and ensure that it's committed by the leader
   */
  @Test
  public void testSubmitTransitionFromLeader() {
    Assume.assumeTrue(isStableTransport());  // requires a stable transport
    RaftState[] closedNodes = bootstrap((nodes, states) -> assertEquals("Commit should have been successful", Optional.of(42), transitionAndWait(nodes[0], states[0], 42, () -> {})));
    basicSuccessTests(closedNodes);
  }


  /**
   * This tests that we can transition from a singleton leader with no friends
   */
  @Test
  public void testSubmitTransitionFromSingletonLeader() {
    Assume.assumeTrue(isStableTransport());  // requires a stable transport
    bootstrap(this::create, 1, 0, true, (nodes, states) ->
        assertEquals("Commit should have been successful", Optional.of(42), transitionAndWait(nodes[0], states[0], 42, () -> {}))
    );
  }

  /**
   * This tests that we cannot transition with an unelected leader
   */
  @Test
  public void testSubmitTransitionFailsIfNoLeader() {
    bootstrapLeaderless((nodes, states) ->
        assertEquals("Cannot transition if no one is elected", Optional.empty(), transitionAndWait(nodes[0], states[0], 42, () -> {}))
    );
  }


  /**
   * Submit a transition on the leader, and ensure that it's committed by the leader
   */
  @Test
  public void testSubmitTransitionFromFollower() {
    Assume.assumeTrue(isStableTransport());  // requires a stable transport
    RaftState[] closedNodes = bootstrap((nodes, states) ->
        assertEquals("Commit should have been successful", Optional.of(42), transitionAndWait(nodes[1], states[0], 42, () -> {})));
    basicSuccessTests(closedNodes);
  }


  /**
   * Submit a transition on the leader, let it commit, and then check the result
   */
  @Test
  public void testSubmitTransitionCommitToFollowers() {
    Assume.assumeTrue(isStableTransport());  // requires a stable transport
    RaftState[] closedNodes = bootstrap((nodes, states) ->
        assertEquals("Commit should have been successful", Optional.of(42), transitionAndWait(nodes[1], states[1], 42, () -> {
      nodes[0].heartbeat();
      waitForSilence(nodes[0]);
    })));
    basicSuccessTests(closedNodes);
  }


  /**
   * Same as {@link #testSubmitTransitionCommitToFollowers()}, but with a submit to one follower and
   * checking the commit on the other.
   */
  @Test
  public void testSubmitTransitionFromFollowerCommittedToOtherFollower() {
    Assume.assumeTrue(isStableTransport());  // requires a stable transport
    RaftState[] closedNodes = bootstrap((nodes, states) -> assertEquals("Commit should have been successful", Optional.of(42), transitionAndWait(nodes[1], states[2], 42, () -> {
      nodes[0].heartbeat();
      waitForSilence(nodes[0]);
    })));
    basicSuccessTests(closedNodes);
  }


  /**
   * Test that having shadow nodes does not block a commit
   */
  @Test
  public void testVisibleShadowsDontBlockCommit() {
    Assume.assumeTrue(isStableTransport());  // requires a stable transport
    RaftState[] closedNodes = bootstrapWithShadows((nodes, states) ->
        assertEquals("Commit should have been successful", Optional.of(42), transitionAndWait(nodes[0], states[0], 42, () -> {})));
    basicSuccessTests(closedNodes, -1, Optional.of(new HashSet<>(Arrays.asList("L", "A", "B"))), true);
  }


  /**
   * Test that having shadow nodes does not block a commit
   */
  @Test
  public void testPartitionedShadowsDontBlockCommit() {
    Assume.assumeTrue(isStableTransport());  // requires a stable transport
    partitionOff(0, Long.MAX_VALUE, "C", "D");
    RaftState[] closedNodes = bootstrapWithShadows((nodes, states) ->
        assertEquals("Commit should have been successful", Optional.of(42), transitionAndWait(nodes[0], states[0], 42, () -> {})));
    assertTrue("Should still have a leader", closedNodes[0].isLeader());
    assertEquals("Leader should not see shadow nodex", new HashSet<>(Arrays.asList("L", "A", "B")), closedNodes[0].log.getQuorumMembers());
  }


  /**
   * Test that having shadows receive commits from the leader
   */
  @Test
  public void testShadowsReceiveCommit() {
    Assume.assumeTrue(isStableTransport());  // requires a stable transport
    RaftState[] closedNodes = bootstrapWithShadows((nodes, states) ->
        assertEquals("Commit should have been successful", Optional.of(42), transitionAndWait(nodes[0], states[4], 42, () -> {
          nodes[0].heartbeat();
          waitForSilence(nodes[0]);
        })));
    basicSuccessTests(closedNodes, -1, Optional.of(new HashSet<>(Arrays.asList("L", "A", "B"))), true);
  }


  /**
   * Submit a transition with a single node in the cluster
   */
  @Test
  public void testSubmitTransitionWithSingleNode() {
    Assume.assumeTrue(isStableTransport());  // requires a stable transport
    RaftState[] closedNodes = bootstrap(this::create, 1, 0, true, (nodes, states) ->
        assertEquals("Commit should have been successful", Optional.of(42), transitionAndWait(nodes[0], states[0], 42, () -> {})));

    assertEquals("There should be exactly one leader", 1, Arrays.stream(closedNodes).filter(RaftState::isLeader).count());
    assertTrue("The node 'L' should be leader", closedNodes[0].isLeader());
    assertEquals("Cluster membership is different on node " + closedNodes[0].serverName,
        Collections.singleton("L"), new HashSet<>(closedNodes[0].log.getQuorumMembers()));
  }


  /**
   * Submit a transition with a single node in the cluster, and some shadow nodes
   */
  @Test
  public void testSubmitTransitionWithSingleNodeAndShadows() {
    Assume.assumeTrue(isStableTransport());  // requires a stable transport
    RaftState[] closedNodes = bootstrap(this::create, 1, 5, true, (nodes, states) ->
        assertEquals("Commit should have been successful", Optional.of(42), transitionAndWait(nodes[0], states[0], 42, () -> {})));

    assertEquals("There should be exactly one leader", 1, Arrays.stream(closedNodes).filter(RaftState::isLeader).count());
    assertTrue("The node 'L' should be leader", closedNodes[0].isLeader());
    assertEquals("Cluster membership is different on node " + closedNodes[0].serverName,
        Collections.singleton("L"), new HashSet<>(closedNodes[0].log.getQuorumMembers()));
  }


  /**
   * Submit with 4 nodes, rather than the usual 3
   */
  @Test
  public void testSubmitTransitionWithOddNodeCount() {
    Assume.assumeTrue(isStableTransport());  // requires a stable transport
    RaftState[] closedNodes = bootstrap(this::create, 4, 0, true, (nodes, states) -> assertEquals("Commit should have been successful", Optional.of(42), transitionAndWait(nodes[0], states[0], 42, () -> {})));

    assertEquals("There should be exactly one leader", 1, Arrays.stream(closedNodes).filter(RaftState::isLeader).count());
    assertTrue("The node 'L' should be leader", closedNodes[0].isLeader());
    assertEquals("Cluster membership should have all nodes",
        new HashSet<>(Arrays.asList("L", "A", "B", "C")), new HashSet<>(closedNodes[0].log.getQuorumMembers()));
  }


  /**
   * Ensure that we can't commit with just 2 of 4 nodes
   */
  @Test
  public void testSubmitTransitionFailWithTiedVote() {
    partitionOff(0, Long.MAX_VALUE, "L", "A");
    bootstrap(this::create, 4, 0, true, (nodes, states) ->
        assertException(() -> transitionAndWait(nodes[0], states[0], 42, () -> {}), NoSuchElementException.class)  // wraps TimeoutException
    );
  }


  /**
   * Test what happens when we boot up an auto-resizing cluster, ensuring that initially there's silence,
   * and that we can then bootstrap a new cluster.
   */
  @Test
  public void testBootstrapFromSilence() {
    // Create three nodes, none of whom know about each other
    RaftState leaderState = new RaftState("L", new SingleByteStateMachine(), 3, MoreExecutors.newDirectExecutorService());
    RaftAlgorithm leaderNode = create(leaderState);
    RaftState followerA = new RaftState("A", new SingleByteStateMachine(), 3, MoreExecutors.newDirectExecutorService());
    RaftAlgorithm nodeA = create(followerA);
    RaftState followerB = new RaftState("B", new SingleByteStateMachine(), 3, MoreExecutors.newDirectExecutorService());
    RaftAlgorithm nodeB = create(followerB);
    RaftAlgorithm[] nodes = new RaftAlgorithm[] { leaderNode, nodeA, nodeB };
    RaftState[] states = new RaftState[] { leaderState, followerA, followerB };

    // Run heartbeats to ensure that no one triggers an election
    schedule(leaderNode.heartbeatMillis(), (int) (leaderNode.electionTimeoutMillisRange().end / leaderNode.heartbeatMillis()) + 10, (k, now) -> {
      for (RaftAlgorithm node : nodes) { node.heartbeat(); }
    });
    for (RaftAlgorithm node : nodes) { waitForSilence(node); }
    // (everything should be silent)
    for (RaftState state : states) {
      assertFalse("No one should have won an election", state.isLeader());
      assertFalse("No one should even have become a candidate", state.isCandidate());
      assertEquals("In fact, no one should be alive at all", Collections.emptySet(), state.log.getQuorumMembers());
    }

    // Bootstrap one of the machines
    assertTrue("Should be able to bootstrap", leaderNode.bootstrap(false));
    schedule(leaderNode.heartbeatMillis(), (int) (leaderNode.electionTimeoutMillisRange().end / leaderNode.heartbeatMillis()) + 20, (k, now) -> {
      for (RaftAlgorithm node : nodes) { node.heartbeat(); }
    });
    stop(nodes);

    // Someone should have started an election
    assertTrue("Someone should have started an election", Arrays.stream(states).anyMatch(x -> x.isLeader() || x.isCandidate()));
    if (isStableTransport()) {
      for (RaftState state : states) {
        assertEquals("Everyone should think everyone else is alive now", new HashSet<>(Arrays.asList("L", "A", "B")), state.log.getQuorumMembers());
      }
    }
  }


  /**
   * Test what happens when we boot up an auto-resizing cluster, ensuring that initially there's silence,
   * and that we can then bootstrap a new cluster.
   */
  @Test
  public void testBootstrapAutomaticallyOnKnownCluster() {
    // Create three nodes, none of whom know about each other
    RaftState leaderState = new RaftState("L", new SingleByteStateMachine(), Arrays.asList("L", "A", "B"), MoreExecutors.newDirectExecutorService());
    RaftAlgorithm leaderNode = create(leaderState);
    RaftState followerA = new RaftState("A", new SingleByteStateMachine(), Arrays.asList("L", "A", "B"), MoreExecutors.newDirectExecutorService());
    RaftAlgorithm nodeA = create(followerA);
    RaftState followerB = new RaftState("B", new SingleByteStateMachine(), Arrays.asList("L", "A", "B"), MoreExecutors.newDirectExecutorService());
    RaftAlgorithm nodeB = create(followerB);
    RaftAlgorithm[] nodes = new RaftAlgorithm[] { leaderNode, nodeA, nodeB };
    RaftState[] states = new RaftState[] { leaderState, followerA, followerB };

    // Run heartbeats until an election timeout
    schedule(leaderNode.heartbeatMillis(), (int) (leaderNode.electionTimeoutMillisRange().end / leaderNode.heartbeatMillis()) + 10, (k, now) -> {
      for (RaftAlgorithm node : nodes) { node.heartbeat(); }
    });
    assertFalse("Should not be able to bootstrap a node anyways", leaderNode.bootstrap(false));
    stop(nodes);

    // Someone should have started an election
    assertTrue("Someone should have started an election", Arrays.stream(states).anyMatch(x -> x.isLeader() || x.isCandidate()));
    for (RaftState state : states) {
      assertEquals("Everyone should think everyone else is alive still", new HashSet<>(Arrays.asList("L", "A", "B")), state.log.getQuorumMembers());
    }
  }


  //
  // --------------------------------------------------------------------------
  // MISC
  // --------------------------------------------------------------------------
  //


  /**
   * A stupid little test to make sure we don't crash on receiving a bad request.
   */
  @Test
  public void receiveBadRequest() {
    RaftState leaderState = new RaftState("L", new SingleByteStateMachine(), Arrays.asList("L", "A", "B"), MoreExecutors.newDirectExecutorService());
    RaftAlgorithm leaderNode = create(leaderState);
    try {
      leaderNode.receiveBadRequest(RaftMessage.newBuilder().build());
    } catch (AssertionError ignored) {}
  }


  /**
   * A script that commits |numToCommit| elements to the state machine, then boots up a shadow node that will not
   * join the quorum, and then runs the system for enough heartbeats to resolve any elections.
   *
   * @param numToCommit The number of (sequential) entries to commit to the cluster before booting up a shadow node.
   * @param silenceBeforeAdd If true, wait for the commits to succeed before adding the node
   * @param electionBeforeAdd If true, force some elections mingled with the commits
   */
  public void addFollowerScript(int numToCommit, boolean silenceBeforeAdd, boolean electionBeforeAdd) {
    Assume.assumeTrue(isStableTransport());  // requires a stable transport
    Pointer<RaftState> shadowPointer = new Pointer<>();
    Pointer<RaftAlgorithm> shadowNodePointer = new Pointer<>();
    Pointer<Byte> lastTransition = new Pointer<>();
    RaftState[] closedNodes = bootstrap((nodes, states) -> {

      // 1. Commit some things
      log.info("Running commits");
      // 1.1. Start things off with a heartbeat
      if (electionBeforeAdd) {
        for (RaftAlgorithm node : nodes) {
          node.heartbeat();
        }
      }
      CompletableFuture[] futures = new CompletableFuture[numToCommit];
      for (int i = 0; i < numToCommit; ++i) {
        // 1.2. Submit the transition
        byte v = (byte) (i % 127);
        lastTransition.set(v);
        futures[i] = transition(nodes[0], v);
        // 1.3. Force an election, maybe
        if (electionBeforeAdd && (i+1) % 25 == 0) {
          waitMillis(nodes[0].electionTimeoutMillisRange().end + 100);
          log.info("Forcing an election @ t={}", now());
          for (RaftAlgorithm node : nodes) {
            if (!node.mutableState().isLeader()) {
              node.heartbeat();
            }
          }
          waitForSilence(nodes);
          for (RaftAlgorithm node : nodes) {
            node.heartbeat();
          }
          waitForSilence(nodes);
        }
        // 1.4. Run the occasional heartbeat to flush the commit state
        if (electionBeforeAdd && (i+1) % 10 == 0) {
          waitForSilence(nodes);
          for (RaftAlgorithm node : nodes) {
            node.heartbeat();
          }
          waitForSilence(nodes);
        }
      }
      // 1.5. Run a final heartbeat
      for (RaftAlgorithm node : nodes) {
        node.heartbeat();
      }
      // 1.5. Wait for silence
      if (silenceBeforeAdd) {
        waitForSilence(nodes);
        assertTrue("Should have committed all the transitions in order to have silence", CompletableFuture.allOf(futures).isDone());
      }

      // 2. Add the new node
      log.info("Adding follower");
      // 2.1. Add the follower
      RaftState shadowState = new RaftState(
          "F",
          new RaftLog(new SingleByteStateMachine(), Collections.emptySet(), MoreExecutors.newDirectExecutorService()));
      shadowPointer.set(shadowState);
      RaftAlgorithm shadow = create(shadowState);
      shadowNodePointer.set(shadow);
      // 2.2. Run a heartbeat to flush the commit state
      for (RaftAlgorithm node : nodes) {
        node.heartbeat();
      }
      waitForSilence(nodes);

      // 3. Heartbeat for a while
      int numHeartbeats = (int) (nodes[0].electionTimeoutMillisRange().end / nodes[0].heartbeatMillis()) + 5;
      log.info("Running "+numHeartbeats+" Heartbeats");
      schedule(nodes[0].heartbeatMillis(), numHeartbeats, (k, now) -> {  // run for 100 heartbeats
        for (RaftAlgorithm node : nodes) {
          node.heartbeat();
        }
        shadow.heartbeat();
      });
    });
    waitForSilence(shadowNodePointer.dereference().get());
    shadowNodePointer.dereference().get().stop(true);
    RaftState shadow = shadowPointer.dereference().get();
    log.info("Done -- running checks");

    // 4. Check that the state is up to date
    if (numToCommit > RaftLog.COMPACTION_LIMIT) {
      assertTrue("Shadow node should have a snapshot", shadow.log.snapshot.isPresent());
    }
    assertEquals("Shadow node should be committed up to the current index", numToCommit, shadow.log.getCommitIndex());
    if (shadow.log.stateMachine instanceof SingleByteStateMachine) {
      assertEquals("Everyone's state machine should hold the same value",
          1, Arrays.stream(closedNodes).map(x -> ((SingleByteStateMachine) x.log.stateMachine).value).collect(Collectors.toSet()).size());
    }
    // 5. Check the basic success tests
    RaftState leader = Arrays.stream(closedNodes).filter(RaftState::isLeader).findFirst().get();
    assertTrue("Should have a leader", leader.isLeader());
    assertTrue("Leader should have received heartbeats", leader.lastMessageTimestamp.isPresent());
    assertTrue("The leader should know about the follower", leader.lastMessageTimestamp.get().keySet().contains("F"));
    assertTrue("The leader should have heartbeats from other servers", leader.lastMessageTimestamp.get().values().stream().allMatch(x -> x > 0));
  }


  /**
   * Add a quorum, and then add another machine as a shadow and make sure that it has all the most
   * recent information.
   */
  @Ignore  // note[gabor] This is no longer a clear test, with throttling enabled
  @Test
  public void catchUpShadowOnLoad() {
    addFollowerScript(10, true, false);
  }


  /**
   * Like {@link #catchUpShadowOnLoad()}, but there are a lot of commits on the cluster already,
   * and so we have to get both the most recent snapshot and the most recent log entries.
   */
  @Ignore  // note[gabor] This is no longer a clear test, with throttling enabled
  @Category(SlowTests.class)
  @Test
  public void catchUpShadowOnLoadSnapshotAndLogs() {
    addFollowerScript(RaftLog.COMPACTION_LIMIT * 3 + 25, true, false);
  }


  /**
   * Like {@link #catchUpShadowOnLoadSnapshotAndLogs()}, but add the follower immediately
   * rather than waiting for the transitions to commit.
   */
  @Ignore  // note[gabor] This is no longer a clear test, with throttling enabled
  @Category(SlowTests.class)
  @Test
  public void catchUpShadowOnLoadSnapshotAndLogsNoSilence() {
    addFollowerScript(RaftLog.COMPACTION_LIMIT * 3 + 25, false, false);
  }


  /**
   * Like {@link #catchUpShadowOnLoadSnapshotAndLogs()}}, but there were a bunch of elections
   * in between the commits.
   *
   * This also serves as a bit of a fuzz test
   */
  @Ignore  // note[gabor] Unclear that this is meant to work with throttling enabled. We're allowed to lose commits during elections
  @Category(SlowTests.class)
  @Test
  public void catchUpShadowOnLoadSnapshotAndLogsAfterElections() {
    addFollowerScript(RaftLog.COMPACTION_LIMIT * 3 + 25, true, true);
  }


  //
  // --------------------------------------------------------------------------
  // FUZZ TESTS
  // --------------------------------------------------------------------------
  //


  /**
   * Test that we can run for a while and have everything be happy and functional at the end.
   */
  @Test
  public void testHappyOperation() {
    quietly(() -> {
      RaftState[] closedNodes = bootstrap((nodes, states) -> {
        // 1. Schedule the appropriate heartbeats
        schedule(nodes[0].heartbeatMillis(), 1000, (k, now) -> {  // run for 1000 heartbeats
          for (RaftAlgorithm node : nodes) {
            if (isStableTransport()) {
              assertTrue("If we're on a stable transport, we should never lose our leader", states[0].isLeader());
            }
            node.heartbeat();
          }
        });
      });
      basicSuccessTests(closedNodes, isStableTransport() ? 0 : -1);
    });
  }


  /**
   * Test that we can run for a while and have everything be happy and functional at the end.
   */
  @Test
  public void testRandomPartitionsAndDeaths() {
    Assume.assumeTrue(isStableTransport());  // requires a stable transport
    quietly(() -> {
      // 1. Introduce some chaos (and then a quiet period)
      Random rand = new Random(42L);
      RaftState[] closedNodes = bootstrap((nodes, states) -> {
        for (int i = 0; i < nodes.length; ++i) {
          RaftAlgorithm node = nodes[i];
          RaftState state = states[i];
          schedule(node.heartbeatMillis(), (int) (100 + node.electionTimeoutMillisRange().end / node.heartbeatMillis() + 5), (k, now) -> {  // run for 1000 heartbeats
            if (k >= 1000 || rand.nextDouble() < 0.1) {  // only issue 10% of heartbeats
              node.heartbeat();
            }
            if (k < 1000 && rand.nextDouble() < 0.1) {  // 10% of heartbeat pings, introduce a network partition
              partitionOff(now, now + node.electionTimeoutMillisRange().end + 1000, state.serverName, states[rand.nextInt(states.length)].serverName);

            }
          });
        }
      });

      // 2. Ensure that we came to some OK state
      basicSuccessTests(closedNodes, -1, this.isStableTransport() ? Optional.of(new HashSet<>(Arrays.asList("L", "A", "B"))) : Optional.empty(), this.isStableTransport());
    });
  }


  /**
   * Submit a bunch of transitions all at once
   */
  @Test
  public void testTransitionAllAtOnce() {
    Assume.assumeTrue(isStableTransport());  // requires a stable transport
    int count = 100;
    // 1. Run the test
    List<CompletableFuture<Boolean>> rpcResults = new ArrayList<>();
    RaftState[] closedNodes = bootstrap((nodes, states) -> {
      Random rand = new Random(42L);
      for (int i = 0; i < count; ++i) {
//        rpcResults.add(transition(nodes[rand.nextInt(nodes.length)], i % 127));
        rpcResults.add(transition(nodes[1], i % 127));
      }
      for (RaftAlgorithm node : nodes) {
        if (node instanceof SingleThreadedRaftAlgorithm) {
          ((SingleThreadedRaftAlgorithm) node).flush(() -> {});
        }
      }
      log.info("All tasks have been queued");
      waitMillis(500);
      for (int i = 0; i < 10; ++i) {
        waitMillis(nodes[0].heartbeatMillis());
        nodes[0].heartbeat();  // heartbeat a bit, to mitigate throttling
      }
      waitMillis(nodes[0].electionTimeoutMillisRange().end);     // just for good measure
      for (RaftAlgorithm node : nodes) {
        waitForSilence(node);           // wait for heartbeat to complete
      }
    });
    basicSuccessTests(closedNodes, -1);
    transitionSuccessTests(closedNodes, -1, rpcResults, count);
  }


  /**
   * Submit a bunch of transitions spread out over time
   */
  @Test
  public void testTransitionOverTime() {
    int count = 100;
    quietly(() -> {
      // 1. Run the test
      List<CompletableFuture<Boolean>> rpcResults = new ArrayList<>();
      RaftState[] closedNodes = bootstrap((nodes, states) -> {
        Random rand = new Random(42L);
        schedule(100, count, (k, now) -> rpcResults.add(transition(nodes[rand.nextInt(nodes.length)], k % 127)));
        waitForSilence(nodes);
        nodes[0].heartbeat();  // make sure everyone is caught up
        if (!isStableTransport()) {
          for (int i = 0; i < 1000 && Arrays.stream(nodes).map(x -> ((SingleByteStateMachine) x.mutableState().log.stateMachine).value).collect(Collectors.toSet()).size() != 1; ++i) {
            for (RaftAlgorithm node : nodes) {
              waitForSilence(node);           // wait for heartbeat to complete
            }
            nodes[0].heartbeat();  // make sure everyone is caught up
          }
        }
        nodes[0].heartbeat();  // make sure everyone is caught up
      });
      basicSuccessTests(closedNodes, -1);
      transitionSuccessTests(closedNodes, -1, rpcResults, count);
    });
  }

}
