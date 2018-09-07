package ai.eloquent.raft;

import ai.eloquent.raft.transport.WithLocalTransport;
import ai.eloquent.test.SlowTests;
import ai.eloquent.util.RunnableThrowsException;
import ai.eloquent.util.TimeUtils;
import ai.eloquent.util.Uninterruptably;
import ai.eloquent.web.Lifecycle;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

/**
 * This runs fuzz tests on our implementation of Raft
 */
@SuppressWarnings("OptionalGetWithoutIsPresent")
public class EloquentRaftTest extends WithLocalTransport {
  /**
   * An SLF4J Logger for this class.
   */
  private static final Logger log = LoggerFactory.getLogger(EloquentRaftTest.class);


  /**
   * Suppress trace logs for this block of code
   *
   * @param fn The code to run 'quietly'
   */
  private void quietly(RunnableThrowsException fn) throws Exception{
    int logLevel = RaftLog.level();
//    EloquentLogger.setLevel("info");
    try {
      fn.run();
    } finally {
      RaftLog.setLevel(logLevel);
    }
  }


  /**
   * Wait for a cluster to establish itself, starting the nodes and awaiting an election.
   */
   protected void awaitElection(LocalTransport transport, EloquentRaft... nodes) {
    Arrays.stream(nodes).forEach(EloquentRaft::start);
    for (int i = 0;
         i < 1000 &&
             !( Arrays.stream(nodes).anyMatch(x -> x.node.algorithm.state().isLeader()) &&
                 Arrays.stream(nodes).allMatch(x -> x.node.algorithm.state().log.getQuorumMembers().containsAll(Arrays.stream(nodes).map(n -> n.node.algorithm.serverName()).collect(Collectors.toList()))) &&
                 Arrays.stream(nodes).allMatch(x -> x.node.algorithm.state().leader.isPresent())
             );
         ++i) {
      transport.sleep(1000);
    }
    assertTrue("Someone should have gotten elected in 10 [virtual] seconds",
        Arrays.stream(nodes).anyMatch(x -> x.node.algorithm.state().isLeader()));
    assertTrue("Every node should see every other node in the quorum",
        Arrays.stream(nodes).allMatch(x -> x.node.algorithm.state().log.getQuorumMembers().containsAll(Arrays.stream(nodes).map(n -> n.node.algorithm.serverName()).collect(Collectors.toList()))));
     assertTrue("Every node should see a leader",
         Arrays.stream(nodes).allMatch(x -> x.node.algorithm.state().leader.isPresent()));
     assertTrue("No node is a candidate",
         Arrays.stream(nodes).noneMatch(x -> x.node.algorithm.state().isCandidate()));
    log.info("Cluster bootstrapped @ t={}", transport.now());
  }


  @FunctionalInterface
  private interface ThrowableConsumer<E> {
     void accept(E value) throws Exception;
  }


  /**
   * Run a function on a cluster of nodes, starting them, waiting for an election, and then closing them.
   */
  private void withNodes(LocalTransport transport, ThrowableConsumer<EloquentRaft[]> fn, EloquentRaft... nodes) {
    try {
      // quietly(() -> {
        awaitElection(transport, nodes);
        try {
          fn.accept(nodes);
        } finally {
          Arrays.stream(nodes).forEach(node -> {
            if (node.node.algorithm instanceof SingleThreadedRaftAlgorithm) {
              ((SingleThreadedRaftAlgorithm) node.node.algorithm).flush(() -> {});
            }
            node.lifecycle.shutdown(true);
          });
          transport.stop();
        }
      // });
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }


  /**
   * This fuzz-tests the mutual exclusivity of the withDistributedLock() mechanism over our cluster.
   */
  @Test
  public void verifyTryLockSafety() throws Exception {
    quietly(() -> {
      // Create the bootstrapped cluster
      RaftLifecycle lifecycle = RaftLifecycle.newBuilder().mockTime().build();
      EloquentRaft raft = new EloquentRaft("10.0.0.1", transport, Collections.singleton("10.0.0.1"), lifecycle);
      raft.start();
      Optional<EloquentRaft.LongLivedLock> lock = raft.tryLock("test", Duration.ofSeconds(5));
      assertTrue(lock.isPresent());
      Optional<EloquentRaft.LongLivedLock> secondAttempt = raft.tryLock("test", Duration.ofSeconds(5));
      assertFalse(secondAttempt.isPresent());
      lock.get().release().get(10, TimeUnit.SECONDS);
      Optional<EloquentRaft.LongLivedLock> thirdAttempt = raft.tryLock("test", Duration.ofSeconds(5));
      assertTrue(thirdAttempt.isPresent());

      // Release the reference to it, to check if the GC cleanup will work
      //noinspection UnusedAssignment,OptionalAssignedToNull
      thirdAttempt = null;

      // Try to force a GC, which will finalize and release the lock
      for (int i = 0; i < 10; i++) {
        Uninterruptably.sleep(10);
        System.runFinalization();
        System.gc();
      }

      // The lock should have been released
      Optional<EloquentRaft.LongLivedLock> fourthAttempt = raft.tryLock("test", Duration.ofSeconds(5));
      assertTrue(fourthAttempt.isPresent());

      // Wait till well after the timer should fire to auto-release the lock
      transport.sleep(5100);

      // The lock should have been released
      assertFalse(fourthAttempt.get().isHeld());

      Optional<EloquentRaft.LongLivedLock> fifthAttempt = raft.tryLock("test", Duration.ofSeconds(5));
      assertTrue(fifthAttempt.isPresent());

      transport.stop();
      if (raft.node.algorithm instanceof SingleThreadedRaftAlgorithm) {
        ((SingleThreadedRaftAlgorithm) raft.node.algorithm).flush(() -> { if(transport != null) { transport.waitForSilence(); }});
      }
      lifecycle.shutdown(true);
    });
  }


  /**
   * This is just a simple test of the get/set/remove calls.
   */
  @Test
  public void testSetGetRemove() throws Exception {
    quietly(() -> {
      // Create the bootstrapped cluster
      RaftLifecycle lifecycle = RaftLifecycle.newBuilder().mockTime().build();
      EloquentRaft raft = new EloquentRaft("10.0.0.1", transport, Collections.singleton("10.0.0.1"), lifecycle);
      raft.start();
      awaitElection(transport, raft);

      assertFalse(raft.getElement("test").isPresent());
      raft.setElementRetryAsync("test", new byte[]{1}, true, Duration.ofSeconds(2)).get(10, TimeUnit.SECONDS);
      assertTrue(raft.getElement("test").isPresent());
      assertEquals(1, (int) raft.getElement("test").get()[0]);
      raft.removeElementRetryAsync("test", Duration.ofMinutes(1)).get(10, TimeUnit.SECONDS);
      assertFalse("The element 'test' should not in Raft", raft.getElement("test").isPresent());

      transport.stop();
    });
  }


  /**
   * Test taking a distributed lock
   */
  @Test
  public void withDistributedLock() {
    EloquentRaft L = new EloquentRaft("L", transport, new HashSet<>(Arrays.asList("L", "A")), RaftLifecycle.newBuilder().mockTime().build());
    EloquentRaft A = new EloquentRaft("A", transport, new HashSet<>(Arrays.asList("L", "A")), RaftLifecycle.newBuilder().mockTime().build());

    CompletableFuture<Optional<EloquentRaft.LongLivedLock>> lockFuture = new CompletableFuture<>();

    withNodes(transport, nodes -> {
      // Try to have both nodes take a lock
      L.withDistributedLockAsync("foo", () -> {
        log.info("Got lock on L");
        return A.tryLockAsync("foo", Duration.ofSeconds(5)).thenApply((optionalLock) -> {
          lockFuture.complete(optionalLock);
          return true;
        });
      }).get();

      try {
        assertFalse("A should not have been able to take a lock", lockFuture.get().isPresent());
      } catch (InterruptedException | ExecutionException e) {
        e.printStackTrace();
      }
    }, L, A);

    transport.stop();
  }


  /**
   * Tests that withDistributedLockAsync doesn't block, even if the runnable does
   */
  @Test
  public void withDistributedLockBlockingRunnable() {
    EloquentRaft L = new EloquentRaft("L", transport, new HashSet<>(Arrays.asList("L", "A")), RaftLifecycle.newBuilder().mockTime().build());
    EloquentRaft A = new EloquentRaft("A", transport, new HashSet<>(Arrays.asList("L", "A")), RaftLifecycle.newBuilder().mockTime().build());

    withNodes(transport, nodes -> {
      // Deliberately block inside this call
      long start = System.currentTimeMillis();
      CompletableFuture<Boolean> future = L.withDistributedLockAsync("foo", () -> Uninterruptably.sleep(1000));
      assertFalse(future.isDone());
      // No way this took more than 500ms, or we're blocking
      assertTrue(System.currentTimeMillis() - start < 500);

      future.get();
      // We should've blocked for a while
      assertTrue(System.currentTimeMillis() - start > 900);
    }, L, A);

    transport.stop();
  }


  /**
   * Test taking a distributed lock when there's only one node in the cluster
   */
  @Test
  public void withDistributedLockSingleNode() {
    EloquentRaft L = new EloquentRaft("L", transport, Collections.singleton("L"), RaftLifecycle.newBuilder().mockTime().build());
    withNodes(transport, nodes -> L.withDistributedLockAsync("key", () -> log.info("Got lock on L")).get(), L);
    transport.stop();
  }


  /**
   * Test locking and withElement by keeping a running sum
   */
  @Category(SlowTests.class)
  @Test
  public void withElement() {
    // quietly(() -> {
      // Start the cluster
      int numThreads = 5;
      int numIters = 20;
      Random r = new Random(42L);
      EloquentRaft[] nodes = new EloquentRaft[]{
          new EloquentRaft("L", transport, new HashSet<>(Arrays.asList("L", "A", "B")), RaftLifecycle.newBuilder().mockTime().build()),
          new EloquentRaft("A", transport, new HashSet<>(Arrays.asList("L", "A", "B")), RaftLifecycle.newBuilder().mockTime().build()),
          new EloquentRaft("B", transport, new HashSet<>(Arrays.asList("L", "A", "B")), RaftLifecycle.newBuilder().mockTime().build())
      };
      withNodes(transport, (raftNodes) -> {
        // Run the sum computation
        Set<Integer> numbersSeen = ConcurrentHashMap.newKeySet();
        AtomicInteger callCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);

        long startTime = System.currentTimeMillis();
        List<Thread> threads = IntStream.range(0, numThreads).mapToObj(threadI -> {
          Thread t = new Thread(() -> {
            for (int i = 0; i < numIters; ++i) {
              AtomicInteger fnCallCount = new AtomicInteger(0);
              boolean success = false;
              long start = System.currentTimeMillis();
              try {
                EloquentRaft node = raftNodes[r.nextInt(raftNodes.length)];
                CompletableFuture<Boolean> future = node.withElementAsync("running_sum", (oldBytes) -> {
                  callCount.incrementAndGet();
                  fnCallCount.incrementAndGet();
                  int value = ByteBuffer.wrap(oldBytes).getInt() + 1;
                  assertFalse(numbersSeen.contains(value));
                  numbersSeen.add(value);
                  return ByteBuffer.allocate(4).putInt(value).array();
                }, () -> ByteBuffer.allocate(4).putInt(0).array(), true);
                success = future.get(10, TimeUnit.SECONDS);
              } catch (InterruptedException | ExecutionException | TimeoutException e) {
                e.printStackTrace();
              } finally {
                log.info("{} iteration {} / {} completed in {}", Thread.currentThread(), i + 1, numIters, TimeUtils.formatTimeSince(start));
              }
              if (success) {
                // If the transition succeeded, we should have called the mutator exactly once
                assertEquals("Each mutator should be called exactly once!", 1, fnCallCount.get());
              } else {
                // On failed transitions, it's possible to not call the mutator at all (if we failed to acquire the
                // lock).
                assertTrue("Each mutator should be called at most once!", fnCallCount.get() <= 1);
                failureCount.incrementAndGet();
              }
            }
          });
          t.setName("withElement-" + threadI);
          t.setDaemon(true);
          return t;
        }).collect(Collectors.toList());
        threads.forEach(Thread::start);
        threads.forEach(Uninterruptably::join);
        long duration = System.currentTimeMillis() - startTime;
        log.info("Doing "+(numIters * numThreads)+" atomic increments took "+ TimeUtils.formatTimeDifference(duration));
        log.info("Doing "+(numIters * numThreads)+" atomic increments took "+((double)duration / (numIters * numThreads))+"ms per increment");

        // Check the sum
        //noinspection ConstantConditions
        byte[] value = raftNodes[0].getElement("running_sum").get();
        int intValue = ByteBuffer.wrap(value).getInt();
        assertEquals("Should have made the right number of function calls", numThreads * numIters - failureCount.get(), callCount.get());
        assertEquals("Should have seen the right number of numbers", numThreads * numIters - failureCount.get(), numbersSeen.size());
        assertEquals("Running sum should have been consistent", numThreads * numIters - failureCount.get(), intValue);
      }, nodes);
      transport.stop();
    // });
  }


  /**
   * Test that we can get and set an element
   */
  @Test
  public void getSetElement() {
    EloquentRaft leader = new EloquentRaft("L", transport, new HashSet<>(Arrays.asList("L", "A")), RaftLifecycle.newBuilder().mockTime().build());
    EloquentRaft follower = new EloquentRaft("A", transport, new HashSet<>(Arrays.asList("L", "A")), RaftLifecycle.newBuilder().mockTime().build());
    withNodes(transport, nodes -> {
      nodes[1].setElementRetryAsync("element", new byte[]{42}, true, Duration.ofMinutes(1)).get();
      Optional<byte[]> elem = nodes[0].getElement("element");
      assertTrue("Element committed to a node should be available on another node", elem.isPresent());
      assertArrayEquals("Should have been able to retrieve the set element", new byte[]{42}, elem.get());
    }, leader, follower);
    transport.stop();
  }


  /**
   * Test that we can get and set an element
   */
  @Test
  public void getSetElementSingleNode() {
    EloquentRaft leader = new EloquentRaft("L", transport, Collections.singleton("L"), RaftLifecycle.newBuilder().mockTime().build());
    withNodes(transport, nodes -> {
      nodes[0].setElementRetryAsync("element", new byte[]{42}, true, Duration.ofMinutes(1)).get(1, TimeUnit.SECONDS);
      Optional<byte[]> elem = nodes[0].getElement("element");
      assertTrue("Element committed to a node should be available on the same node", elem.isPresent());
      assertArrayEquals("Should have been able to retrieve the set element", new byte[]{42}, elem.get());
    }, leader);
    transport.stop();
  }


  /**
   * Test that we can remove an element
   */
  @Test
  public void removeElement() {
    EloquentRaft leader = new EloquentRaft("L", transport, new HashSet<>(Arrays.asList("L", "A")), RaftLifecycle.newBuilder().mockTime().build());
    EloquentRaft follower = new EloquentRaft("A", transport, new HashSet<>(Arrays.asList("L", "A")), RaftLifecycle.newBuilder().mockTime().build());
    withNodes(transport, nodes -> {
      // Set an element
      nodes[1].setElementRetryAsync("element", new byte[]{42}, true, Duration.ofMinutes(1)).get();
      Optional<byte[]> elem = nodes[0].getElement("element");
      assertTrue("Element committed to a node should be available on another node", elem.isPresent());
      assertArrayEquals("Should have been able to retrieve the set element", new byte[]{42}, elem.get());

      // Remove the element
      nodes[1].removeElementRetryAsync("element", Duration.ofMinutes(1)).get();

      // Element should not exist (on other server)
      Optional<byte[]> reread = nodes[0].getElement("element");
      assertEquals("Element should no longer be in the state machine", Optional.empty(), reread);
    }, leader, follower);
    transport.stop();
  }


  /**
   * Test getting the entire state machine map from a node.
   */
  @Test
  public void getMap() {
    EloquentRaft leader = new EloquentRaft("L", transport, new HashSet<>(Arrays.asList("L", "A")), RaftLifecycle.newBuilder().mockTime().build());
    EloquentRaft follower = new EloquentRaft("A", transport, new HashSet<>(Arrays.asList("L", "A")), RaftLifecycle.newBuilder().mockTime().build());
    withNodes(transport, nodes -> {
      // Set an element
      nodes[1].setElementRetryAsync("element", new byte[]{42}, true, Duration.ofMinutes(1)).get();
      Map<String, byte[]> map = nodes[0].getMap();
      assertEquals("Leader should see the set element", 1, map.size());
      assertArrayEquals("Leader should see the correct set element", new byte[]{42}, map.get("element"));
      map = nodes[1].getMap();
      assertEquals("Follower should see the set element", 1, map.size());
      assertArrayEquals("Follower should see the correct set element", new byte[]{42}, map.get("element"));
    }, leader, follower);
    transport.stop();
  }


  /**
   * Test change listeners on a key in the map
   */
  @Test
  public void addChangeListener() {
    EloquentRaft leader = new EloquentRaft("L", transport, new HashSet<>(Arrays.asList("L", "A")), RaftLifecycle.newBuilder().mockTime().build());
    EloquentRaft follower = new EloquentRaft("A", transport, new HashSet<>(Arrays.asList("L", "A")), RaftLifecycle.newBuilder().mockTime().build());
    AtomicInteger changesSeen = new AtomicInteger(0);
    withNodes(transport, nodes -> {
      KeyValueStateMachine.ChangeListener changeListener = (changedKey, newValue, state) -> changesSeen.incrementAndGet();
      nodes[0].addChangeListener(changeListener);
      // Set an element
      nodes[1].setElementRetryAsync("element", new byte[]{42}, true, Duration.ofMinutes(1)).get();
      nodes[0].removeChangeListener(changeListener);
      nodes[1].setElementRetryAsync("element", new byte[]{43}, true, Duration.ofMinutes(1)).get();
    }, leader, follower);

    assertEquals("Should have seen one (but only one) change on the cluster", 1, changesSeen.get());
    transport.stop();
  }


  /**
   * @see #permanentSet()
   * @see #permanentSetDynamicCluster()
   * @see #transientSet()
   * @see #transientSetDynamicCluster()
   */
  private void permanence(boolean permanent, boolean dynamic, boolean shadow, Consumer<EloquentRaft> killMethod) {
    EloquentRaft L;
    EloquentRaft A;
    EloquentRaft B;
    if (dynamic) {
      L = new EloquentRaft("L", transport, 3, RaftLifecycle.newBuilder().mockTime().build());
      A = new EloquentRaft("A", transport, 3, RaftLifecycle.newBuilder().mockTime().build());
      B = new EloquentRaft("B", transport, 3, RaftLifecycle.newBuilder().mockTime().build());
      L.bootstrap(false);
    } else {
      L = new EloquentRaft("L", transport, new HashSet<>(Arrays.asList("L", "A", "B")), RaftLifecycle.newBuilder().mockTime().build());
      A = new EloquentRaft("A", transport, new HashSet<>(Arrays.asList("L", "A", "B")), RaftLifecycle.newBuilder().mockTime().build());
      B = new EloquentRaft("B", transport, new HashSet<>(Arrays.asList("L", "A", "B")), RaftLifecycle.newBuilder().mockTime().build());
    }
    withNodes(transport, (nodes) -> {
      EloquentRaft submitter = A;
      if (shadow) {
        submitter = new EloquentRaft("C", transport, 3, RaftLifecycle.newBuilder().mockTime().build());
        transport.sleep(1000);  // wait for the submitter to join
      }
      submitter.setElementRetryAsync("transient", new byte[]{42}, permanent, Duration.ofSeconds(5)).get();
      killMethod.accept(submitter);
      transport.sleep(EloquentRaftAlgorithm.MACHINE_DOWN_TIMEOUT);
      transport.sleep(5000);
      if (permanent) {
        assertArrayEquals(new byte[]{42}, L.getElement("transient").orElse(null));
      } else {
        assertEquals(Optional.empty(), L.getElement("transient").map(Arrays::toString));
      }
    }, L, A, B);
    transport.stop();
    if (L.node.algorithm instanceof SingleThreadedRaftAlgorithm) {
      ((SingleThreadedRaftAlgorithm) L.node.algorithm).flush(() -> { if(transport != null) { transport.waitForSilence(); }});
    }
    if (A.node.algorithm instanceof SingleThreadedRaftAlgorithm) {
      ((SingleThreadedRaftAlgorithm) A.node.algorithm).flush(() -> { if(transport != null) { transport.waitForSilence(); }});
    }
    if (B.node.algorithm instanceof SingleThreadedRaftAlgorithm) {
      ((SingleThreadedRaftAlgorithm) B.node.algorithm).flush(() -> { if(transport != null) { transport.waitForSilence(); }});
    }
  }


  private void permanence(boolean permanent, boolean dynamic, boolean shadow) {
    permanence(permanent, dynamic, shadow, node -> {
      node.lifecycle.shutdown(true);
      node.node.algorithm.stop(true);
    });
  }


  /**
   * Test that a value is permanent even if the setting node leaves the cluster.
   *
   * @see #transientSet()
   */
  @Test
  public void permanentSet() {
    permanence(true, false, false);
  }


  /**
   * Test that a value is transient if the setting node leaves the clsuter and it's not marked permanent
   *
   * @see #permanentSet()
   */
  @Test
  public void transientSet() {
    permanence(false, false, false);
  }


  /**
   * Test that a value is permanent even if the setting node leaves the cluster.
   *
   * @see #transientSetDynamicCluster()
   */
  @Test
  public void permanentSetDynamicCluster() {
    permanence(true, true, false);
  }


  /**
   * Test that a value is transient if the setting node leaves the clsuter and it's not marked permanent
   *
   * @see #permanentSetDynamicCluster()
   */
  @Test
  public void transientSetDynamicCluster() {
    permanence(false, true, false);
  }


  /**
   * Test that a value is removed automatically if submitted from a Shadow node.
   */
  @Test
  public void transientFromFollower() {
    permanence(false, true, true);
  }


  /**
   * Test that we clear all transient values when we close our node.
   */
  @Test
  public void transientsClearOnClose() {
    permanence(false, false, false, node -> node.node.close());
  }


  /**
   * Check that when a new element is made via withElement, we save it to Raft even if it doesn't mutate.
   *
   * Related to merge request !78
   */
  @Test
  public void withElementNoMutationSaves() {
    EloquentRaft raft = new EloquentRaft("L", transport, Collections.singleton("L"), RaftLifecycle.newBuilder().mockTime().build());
    withNodes(transport, (nodes) -> {
      raft.withElementAsync("key", Function.identity(), () -> new byte[]{42}, false).get(1, TimeUnit.SECONDS);
      Optional<byte[]> retrieved = raft.getElement("key");
      assertTrue("Even without a mutation we should persist our created key", retrieved.isPresent());
      assertArrayEquals("The persisted key should have the correct value", new byte[]{42}, retrieved.orElse(null));
    }, raft);
  }


}