package ai.eloquent.raft;

import ai.eloquent.raft.transport.WithLocalTransport;
import ai.eloquent.util.Pointer;
import ai.eloquent.util.RuntimeInterruptedException;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

/**
 * Test a {@link EloquentRaftNode}.
 *
 * @author <a href="mailto:gabor@eloquent.ai">Gabor Angeli</a>
 */
@SuppressWarnings("ALL")
public class EloquentRaftNodeTest extends WithLocalTransport {
  /**
   * An SLF4J Logger for this class.
   */
  private static final Logger log = LoggerFactory.getLogger(EloquentRaftNodeTest.class);

  /**
   * Make a simple node with some sensible defaults.
   *
   * @param name The server name of the node.
   *
   * @return A node, not yet started or even bootstrapped.
   */
  static EloquentRaftNode mkNode(String name, RaftTransport transport, int clusterSize) {
    RaftLifecycle lifecycle = RaftLifecycle.newBuilder().mockTime().build();
    return new EloquentRaftNode(new SingleThreadedRaftAlgorithm(new EloquentRaftAlgorithm(name, new KeyValueStateMachine(name), transport, clusterSize, MoreExecutors.newDirectExecutorService(), Optional.of(lifecycle)), lifecycle.managedThreadPool(name+"-boundary-pool")), transport, lifecycle);
  }

  /** Create a node in a 3 node cluster */
  static EloquentRaftNode mkNode(String name, RaftTransport transport) {
    return mkNode(name, transport, 3);
  }


  /**
   * Bootstrap a little mini cluster.
   */
  private RaftState[] bootstrap(Consumer<EloquentRaftNode[]> callback, RaftTransport transport) {
    EloquentRaftNode L = mkNode("L", transport);
    EloquentRaftNode A = mkNode("A", transport);
    EloquentRaftNode B = mkNode("B", transport);
    L.start();
    A.start();
    B.start();
    L.bootstrap(false);
    awaitElection(transport, true, L, A, B);
    try {
      assertEquals("L should not have any errors", Collections.emptyList(), L.errors());
      assertTrue("L should have become the leader", L.algorithm.mutableState().isLeader());
      assertEquals("L should know about A and B", new HashSet<>(Arrays.asList("A", "B")), L.algorithm.mutableState().lastMessageTimestamp.get().keySet());
      callback.accept(new EloquentRaftNode[]{L, A, B});
    } finally {
      L.lifecycle.shutdown(false);
    }
    RaftState[] states = new RaftState[]{L.algorithm.mutableState(), A.algorithm.mutableState(), B.algorithm.mutableState()};
    A.lifecycle.shutdown(true);
    B.lifecycle.shutdown(true);
    return states;
  }


  /**
   * Wait for someone to get elected
   */
  private void awaitElection(RaftTransport transport, boolean doChecks, EloquentRaftNode... nodes) {
    int i;
    for (i = 0;
         (i < 1000 &&
               !( Arrays.stream(nodes).anyMatch(x -> x.algorithm.mutableState().isLeader()) &&
                   Arrays.stream(nodes).allMatch(x -> x.algorithm.mutableState().log.getQuorumMembers().containsAll(Arrays.stream(nodes).map(n -> n.algorithm.serverName()).collect(Collectors.toList())))
               ));
         ++i) {
      transport.sleep(100);
    }
    assertTrue("We never got an election -- no one is the leader, and we're at time=" + transport.now() + "; i=" + i + " of 1000; state=" + nodes[0].algorithm.mutableState(),
        Arrays.stream(nodes).anyMatch(x -> x.algorithm.mutableState().isLeader()));  // If we didn't pass an election, this is bad but don't crash the whole test suite
    if (doChecks) {
      assertTrue("Every node should see every other node in the quorum",
          Arrays.stream(nodes).allMatch(x -> x.algorithm.mutableState().log.getQuorumMembers().containsAll(Arrays.stream(nodes).map(n -> n.algorithm.serverName()).collect(Collectors.toList()))));
    }
    log.info("--- Cluster bootstrapped @ t={} ---", transport.now());
  }


  /**
   * Start a single node, close it, and then bring up another node in parallel.
   * The old node should wait to close until the new node is up.
   *
   * @param beforeClose Optional functions to run on the old node before closing it.
   *
   * @return The final raft state of the <b>new</b> node.
   */
  private RaftState handoff(Consumer<EloquentRaftNode> beforeClose) throws InterruptedException {
    // start a single node
    transport.assertNoErrors();
    EloquentRaftNode L = mkNode("L", transport);
    L.start();
    L.bootstrap(false);
    awaitElection(transport, true, L);

    // Variables
    AtomicLong closeStarted = new AtomicLong();
    AtomicLong closeEnded = new AtomicLong();

    // Start closing the single node -- this should block
    beforeClose.accept(L);
    AtomicBoolean startedCloserThread = new AtomicBoolean(false);
    Thread closer = new Thread(() -> {
      synchronized (startedCloserThread) {
        startedCloserThread.set(true);
        startedCloserThread.notifyAll();
      }
      closeStarted.set(transport.now());
      L.lifecycle.shutdown(false);
      closeEnded.set(transport.now());
      log.info("Closed L");
    });
    closer.setName("closer");
    closer.setDaemon(true);
    closer.start();
    synchronized (startedCloserThread) {
      while (!startedCloserThread.get()) {
        startedCloserThread.wait(10);
      }
    }
    Thread.yield();  // let the closer thread actually proceed

    // In parallel, bring up a new node
    Pointer<EloquentRaftNode> rtn = new Pointer<>();
    EloquentRaftNode A = mkNode("A", transport);
    Thread newNode = new Thread(() -> {
      transport.sleep(10);
      A.start();
      transport.sleep(1000);
      log.info("Joined A");
      rtn.set(A);
    });
    newNode.setName("newNode");
    newNode.setDaemon(true);
    newNode.start();

    // Close the threads
    newNode.join(Duration.ofMinutes(1).toMillis());
    closer.join(Duration.ofMinutes(1).toMillis());

    // Assertions
    assertTrue("Should have taken time to close the node", closeStarted.get() < closeEnded.get());
    assertEquals("The new node should see only itself in the cluster", Collections.singleton("A"), A.algorithm.mutableState().log.latestQuorumMembers);
    awaitElection(transport, false, A);
    assertTrue("The new node should eventually win leadership", A.algorithm.mutableState().isLeader());

    assertTrue("The new node should exist", rtn.dereference().isPresent());
    RaftState stateBeforeClose = rtn.dereference().get().algorithm.mutableState();

    A.lifecycle.shutdown(true);

    // Return
    return stateBeforeClose;
  }


  /**
   * Make sure we can create a node.
   */
  @Test
  public void create() {
    EloquentRaftNode node = mkNode("L", transport);
    assertNotNull(node);
    node.close(true);
  }


  /**
   * Make sure we can start and stop a node.
   */
  @Test
  public void lifecycleSimple() {
    EloquentRaftNode L = mkNode("L", transport);
    L.start();
    try {
      assertEquals("L should not have any errors", Collections.emptyList(), L.errors());
    } finally {
      L.lifecycle.shutdown(true);
    }
  }


  /**
   * Test that if we bootstrap, we have ourselves in the cluster membership list
   */
  @Test
  public void bootstrap() {
    EloquentRaftNode L = mkNode("L", transport);
    L.start();
    L.bootstrap(false);
    try {
      assertEquals("L should not have any errors", Collections.emptyList(), L.errors());
      assertEquals("L should see itself in the cluster", Collections.singleton("L"), L.algorithm.mutableState().log.getQuorumMembers());
      assertTrue("L should still be a regular node", !L.algorithm.mutableState().isCandidate() && !L.algorithm.mutableState().isLeader());
    } finally {
      awaitElection(L.transport, false, L);  // await election before stopping
      L.lifecycle.shutdown(true);
    }
  }


  /**
   * Test that we can shutdown even when we are not in a functioning quorum yet.
   */
  @Test
  public void shutdownBeforeElection() {
    EloquentRaftNode L = mkNode("L", transport);
    L.start();
    L.bootstrap(false);
    L.lifecycle.shutdown(true);
  }


  /**
   * Test that if we bootstrap, we will eventually elect someone
   */
  @Test
  public void bootstrapTriggersElection() {
    EloquentRaftNode L = mkNode("L", transport);
    EloquentRaftNode A = mkNode("A", transport);
    L.start();
    A.start();
    L.bootstrap(false);
    awaitElection(transport, true, L, A);
    try {
      assertEquals("L should not have any errors", Collections.emptyList(), L.errors());
      assertTrue("L should have become the leader", L.algorithm.mutableState().isLeader());
      assertEquals("L should know about A", Collections.singleton("A"), L.algorithm.mutableState().lastMessageTimestamp.get().keySet());
    } finally {
      L.lifecycle.shutdown(true);
      A.lifecycle.shutdown(true);
    }
  }


  /**
   * Test that if we bootstrap, we have ourselves in the cluster membership list
   */
  @Test
  public void bootstrapHelper() {
    bootstrap(nodes -> {}, transport);
  }


  /**
   * Test that we can't close until another box comes up
   */
  @Test
  public void gracefulHandoff() throws InterruptedException {
    handoff(node -> {});
  }


  /**
   * Test that we can't close until another box comes up
   */
  @Test
  public void commitPersistOnHandoff() throws InterruptedException {
    RaftState newNode = handoff(node -> {
      try {
        node.submitTransition(KeyValueStateMachine.createSetValueTransition("foo", new byte[]{1})).get();
        node.submitTransition(KeyValueStateMachine.createSetValueTransition("foo", new byte[]{2})).get();
        node.submitTransition(KeyValueStateMachine.createSetValueTransition("foo", new byte[]{3})).get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    });
    Optional<byte[]> value = ((KeyValueStateMachine) newNode.log.stateMachine).get("foo", transport.now());
    assertEquals("Commit should have made it to the new leader", (byte) 3, value.get()[0]);
  }


  private void simultaneousShutdown(int numNodes) {
    // 1. Create the nodes
    EloquentRaftNode[] nodes = new EloquentRaftNode[numNodes];
    nodes[0] = mkNode("leftover", transport, numNodes);
    for (int i = 1; i < numNodes; ++i) {
      nodes[i] = mkNode("a" + i, transport, numNodes);
    }

    // 2. Start the nodes
    for (EloquentRaftNode node : nodes) {
      node.start();
    }

    // 3. Bootstrap
    nodes[1].bootstrap(false);
    awaitElection(transport, true, nodes);

    // 4. Run the threads
    Arrays.stream(nodes)
        .map(node -> {
          if (node != nodes[0]) {
            Thread t = new Thread(() -> node.lifecycle.shutdown(false));
            t.setDaemon(true);
            t.setName("shutdown-" + node.algorithm.serverName());
            return t;
          } else {
            return new Thread( () -> {} );  // don't shutdown the leftover node
          }
        })
        .map(t -> {
          t.start();
          return t;
        })
        .forEach(t -> {
          try {
            t.join();
          } catch (InterruptedException e) {
            throw new RuntimeInterruptedException(e);
          }
        });

    // Assert everything is OK
    EloquentRaftNode leftover = nodes[0];
    awaitElection(transport, false, leftover);
    try {
      assertEquals("The remaining node should see only themsleves in the cluster", Collections.singleton("leftover"), leftover.algorithm.mutableState().log.getQuorumMembers());
      assertEquals("The remaining node should see only themselves committed in the cluster", Collections.singleton("leftover"), leftover.algorithm.mutableState().log.committedQuorumMembers);
    } finally {
      // Clean up
      leftover.lifecycle.shutdown(true);  // the only node we still have to kill
    }

  }


  /**
   * Test that 2 nodes can try to go down at once, and Raft is OK.
   */
  @Test
  public void simultaneousShutdown() throws InterruptedException {
    simultaneousShutdown(3);
  }


  /**
   * Test that 4 nodes can try to go down at once, and Raft is OK.
   */
  @Test
  public void simultaneousShutdownQuorum5() throws InterruptedException {
    simultaneousShutdown(5);
  }


  /**
   * Test that 6 nodes can try to go down at once, and Raft is OK.
   */
  @Test
  public void simultaneousShutdownQuorum7() throws InterruptedException {
    simultaneousShutdown(7);
  }


  /**
   * Start a node and immediately submit a transition. The transition should make it through.
   */
  @Test
  public void submitTransition() throws ExecutionException, InterruptedException {
    EloquentRaftNode L = mkNode("L", transport);
    L.start();
    L.bootstrap(false);
    awaitElection(transport, true, L);
    try {
      assertTrue("Transition should have been successful",
          L.submitTransition(KeyValueStateMachine.createSetValueTransition("foo", new byte[]{1})).get());
    } finally {
      L.lifecycle.shutdown(true);
    }
  }
}