package ai.eloquent.raft;

import ai.eloquent.raft.transport.WithLocalTransport;
import ai.eloquent.test.SlowTests;
import ai.eloquent.util.FunctionalUtils;
import ai.eloquent.util.RunnableThrowsException;
import ai.eloquent.util.TimerUtils;
import ai.eloquent.util.Uninterruptably;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

/**
 * This runs fuzz tests on our implementation of Raft
 */
@Category(SlowTests.class)
public class TheseusTest extends WithLocalTransport {
  /**
   * An SLF4J Logger for this class.
   */
  private static final Logger log = LoggerFactory.getLogger(TheseusTest.class);


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
   protected void awaitElection(LocalTransport transport, Theseus... nodes) {
    Arrays.stream(nodes).forEach(Theseus::start);
    for (int i = 0;
         i < 25 * nodes[0].node.algorithm.electionTimeoutMillisRange().end / nodes[0].node.algorithm.heartbeatMillis() &&
             !( Arrays.stream(nodes).anyMatch(x -> x.node.algorithm.state().isLeader()) &&
                 Arrays.stream(nodes).allMatch(x -> x.node.algorithm.state().log.getQuorumMembers().containsAll(Arrays.stream(nodes).map(n -> n.node.algorithm.serverName()).collect(Collectors.toList()))) &&
                 Arrays.stream(nodes).allMatch(x -> x.node.algorithm.state().leader.isPresent())
             );
         ++i) {
      transport.sleep(nodes[0].node.algorithm.heartbeatMillis());
    }
    assertTrue("Someone should have gotten elected in 10 election timeouts",
        Arrays.stream(nodes).anyMatch(x -> x.node.algorithm.state().isLeader()));
    assertTrue("Every node should see every other node in the quorum",
        Arrays.stream(nodes).allMatch(x -> x.node.algorithm.state().log.getQuorumMembers().containsAll(Arrays.stream(nodes).map(n -> n.node.algorithm.serverName()).collect(Collectors.toList()))));
     assertTrue("Every node should see a leader",
         Arrays.stream(nodes).allMatch(x -> x.node.algorithm.state().leader.isPresent()));
     assertTrue("No node should be a candidate, but the following are: " + Arrays.stream(nodes).filter(x -> x.node.algorithm.state().isCandidate()).map(x -> x.serverName).collect(Collectors.toList()),
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
  private void withNodes(LocalTransport transport, ThrowableConsumer<Theseus[]> fn, Theseus... nodes) {
    try {
      // quietly(() -> {
        awaitElection(transport, nodes);
        try {
          fn.accept(nodes);
        } finally {
          transport.liftPartitions();
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
      if (e instanceof RuntimeException) {
        throw (RuntimeException) e;
      } else {
        throw new RuntimeException(e);
      }
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
      Theseus raft = new Theseus("10.0.0.1", transport, Collections.singleton("10.0.0.1"), lifecycle);
      raft.start();
      raft.node.algorithm.triggerElection();
      Optional<Theseus.LongLivedLock> lock = raft.tryLock("test", Duration.ofSeconds(5));
      assertTrue(lock.isPresent());
      Optional<Theseus.LongLivedLock> secondAttempt = raft.tryLock("test", Duration.ofSeconds(5));
      assertFalse(secondAttempt.isPresent());
      lock.get().release().get(10, TimeUnit.SECONDS);
      Optional<Theseus.LongLivedLock> thirdAttempt = raft.tryLock("test", Duration.ofSeconds(5));
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
      Optional<Theseus.LongLivedLock> fourthAttempt = raft.tryLock("test", Duration.ofSeconds(5));
      assertTrue(fourthAttempt.isPresent());

      // Wait till well after the timer should fire to auto-release the lock
      transport.sleep(5100);

      // The lock should have been released
      assertFalse(fourthAttempt.get().isCertainlyHeld());

      Optional<Theseus.LongLivedLock> fifthAttempt = raft.tryLock("test", Duration.ofSeconds(5));
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
      Theseus raft = new Theseus("10.0.0.1", transport, Collections.singleton("10.0.0.1"), lifecycle);
      raft.start();
      awaitElection(transport, raft);

      assertFalse(raft.getElement("test").isPresent());
      raft.setElementAsync("test", new byte[]{1}, true, Duration.ofSeconds(2)).get(10, TimeUnit.SECONDS);
      assertTrue(raft.getElement("test").isPresent());
      assertEquals(1, (int) raft.getElement("test").get()[0]);
      raft.removeElementAsync("test", Duration.ofMinutes(1)).get(10, TimeUnit.SECONDS);
      assertFalse("The element 'test' should not in Raft", raft.getElement("test").isPresent());

      transport.stop();
    });
  }


  /**
   * Test taking a distributed lock
   */
  @Test
  public void withDistributedLock() {
    Theseus L = new Theseus("L", transport, new HashSet<>(Arrays.asList("L", "A")), RaftLifecycle.newBuilder().mockTime().build());
    Theseus A = new Theseus("A", transport, new HashSet<>(Arrays.asList("L", "A")), RaftLifecycle.newBuilder().mockTime().build());

    CompletableFuture<Optional<Theseus.LongLivedLock>> lockFuture = new CompletableFuture<>();

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
   * Test that we don't add duplicate lock release requests.
   */
  @Test
  public void queueFailedLock() {
    // 1. Set up variables
    byte[] lock = KeyValueStateMachine.createReleaseLockTransition("lock", "requester", "hash");
    byte[] differentHash = KeyValueStateMachine.createReleaseLockTransition("lock", "requester", "hash2");
    byte[] differentRequester = KeyValueStateMachine.createReleaseLockTransition("lock", "requester2", "hash");
    byte[] differentLock = KeyValueStateMachine.createReleaseLockTransition("lock2", "requester", "hash");
    // 2. Tests
    Theseus L = new Theseus("L", transport, new HashSet<>(Arrays.asList("L", "A")), RaftLifecycle.newBuilder().mockTime().build());
    L.queueFailedLock(lock);
    assertEquals(1, L.unreleasedLocks.size());
    L.queueFailedLock(lock);
    assertEquals("Should not add duplicate lock", 1, L.unreleasedLocks.size());
    L.queueFailedLock(differentHash);
    assertEquals("Should add lock with different hash", 2, L.unreleasedLocks.size());
    L.queueFailedLock(differentRequester);
    assertEquals("Should add lock with different requester", 3, L.unreleasedLocks.size());
    L.queueFailedLock(differentLock);
    assertEquals("Should add lock with different lock", 4, L.unreleasedLocks.size());
  }


  /**
   * Test that we call the distributed lock failsafe, and that this failsafe doesn't interact
   * poorly with regular Raft operations (e.g., blocks another thread on a lock).
   */
  @Ignore  // note[gabor]: We don't actually timeout release lock requests
  @Test
  public void distributedLockFailsafe() {
    Theseus L = new Theseus("L", transport, new HashSet<>(Arrays.asList("L", "A")), RaftLifecycle.newBuilder().mockTime().build());
    Theseus A = new Theseus("A", transport, new HashSet<>(Arrays.asList("L", "A")), RaftLifecycle.newBuilder().mockTime().build());
    withNodes(transport, nodes -> {
      // 1. Take the lock
      assertEquals(Collections.emptyList(), L.unreleasedLocks);
      Theseus.LongLivedLock lock = L.tryLock("lock", Duration.ofMinutes(1)).orElseThrow(() -> new AssertionError("Could not take lock"));
      assertTrue("We should know for sure our lock is held", lock.isCertainlyHeld());
      assertTrue("If we know for sure our lock is held, it's certainly perhaps held", lock.isPerhapsHeld());
      // 2. Break consensus
      transport.partitionOff(0, Long.MAX_VALUE, "A");
      assertTrue("We should still hold our lock", lock.isCertainlyHeld());
      assertTrue("If we know for sure our lock is held, it's certainly perhaps held", lock.isPerhapsHeld());
      // 3. Release the lock
      lock.release();
      assertFalse("We should have registered that our lock may not be held.", lock.isCertainlyHeld());
      assertTrue("Lock should still be releasing at invocation -- we don't have consensus", lock.isPerhapsHeld());
      // 4. Wait for lock to time out
      transport.sleep(10000);
      assertFalse("The lock should, of course, still be possibly gone", lock.isCertainlyHeld());
      assertTrue("Lock should still be releasing -- we don't have consensus", lock.isPerhapsHeld());
      // 5. Check that we registered the failsafe
      for (int i = 0; i < 1000 && L.unreleasedLocks.size() == 0; ++i) {
        Uninterruptably.sleep(10);
      }
      assertEquals("The lock should have queued up on the failsafe", 1, L.unreleasedLocks.size());
      // 6. Lift the partition
      transport.liftPartitions();
      synchronized (L.unreleasedLocks) {
        L.unreleasedLocks.notifyAll();
      }
      for (int i = 0; i < 1000 && L.unreleasedLocks.size() > 0; ++i) {
        Uninterruptably.sleep(10);
      }
      // 7. Check that the locks unlocked
      assertEquals("We should clear out our unreleased lock", 0, L.unreleasedLocks.size());
      assertFalse(lock.isCertainlyHeld());
      assertFalse("Lock should no longer be held", lock.isPerhapsHeld());
    }, L, A);
    transport.stop();
  }


  /**
   * Test that a distributed lock auto-releases itself after a given period of time, and that the
   * timer cleans itself up when this happens.
   */
  @Ignore  // note[gabor]: We don't actually timeout release lock requests
  @Test
  public void distributedLockReleasesOnReleaseTimeout() {
    Theseus L = new Theseus("L", transport, new HashSet<>(Arrays.asList("L", "A")), RaftLifecycle.newBuilder().mockTime().build());
    Theseus A = new Theseus("A", transport, new HashSet<>(Arrays.asList("L", "A")), RaftLifecycle.newBuilder().mockTime().build());
    withNodes(transport, nodes -> {
      // 1. Take a lock and then partition off before releasing
      assertEquals(Collections.emptyList(), L.unreleasedLocks);
      L.withElementAsync("foo", x -> {
        x[0] += 1;
        return x;
      }, () -> {
        transport.partitionOff(0, Long.MAX_VALUE, "A");  // partition while lock is held
        return new byte[1];
      }, true);

      // 2. Check that we registered the failsafe
      for (int i = 0; i < 1000 && L.unreleasedLocks.size() == 0; ++i) {
        Uninterruptably.sleep(100);
      }
      assertEquals("We should have registered our lock as unreleased", 1, L.unreleasedLocks.size());

      // 3. Check that we can't make progress
      Boolean shouldFail = FunctionalUtils.ofThrowable(() -> L.withElementAsync("foo", x -> {
        x[0] += 1;
        return x;
      }, () -> new byte[1], true).get(5, TimeUnit.SECONDS)).orElse(false);
      assertFalse("Should not be able to make progress without unlocking stuck lock", shouldFail);

      // 4. Lift the partition
      transport.liftPartitions();
      transport.sleep(L.node.algorithm.electionTimeoutMillisRange().end + 100);
      awaitElection(transport, L, A);
      synchronized (L.unreleasedLocks) {
        L.unreleasedLocks.notifyAll();
      }
      for (int i = 0; i < 1000 && L.unreleasedLocks.size() > 0; ++i) {
        Uninterruptably.sleep(100);
      }
      Uninterruptably.sleep(100);

      // 5. Check that the lock unlocked
      // 5.1. ...In theory
      assertEquals("We should clear out our unreleased lock", 0, L.unreleasedLocks.size());
      // 5.2. ...In practice
      Boolean result = L.withElementAsync("foo", x -> {
        x[0] += 1;
        return x;
      }, () -> new byte[1], true).get(5, TimeUnit.SECONDS);
      assertTrue("We should be able to complete withElement on the previously locked key", result);

    }, L, A);
    transport.stop();
  }


  /**
   * Test that if a release fails on a {@link Theseus.LongLivedLock}, we still queue it up for retrying.
   */
  @Test
  public void longLivedLockReleaseFails() {
    Theseus L = new Theseus("L", transport, new HashSet<>(Arrays.asList("L", "A")), RaftLifecycle.newBuilder().mockTime().build());
    Theseus A = new Theseus("A", transport, new HashSet<>(Arrays.asList("L", "A")), RaftLifecycle.newBuilder().mockTime().build());
    withNodes(transport, nodes -> {
      Optional<Theseus.LongLivedLock> lock = L.tryLock("lockName", Duration.ofSeconds(10));
      assertTrue(lock.isPresent());
      transport.sleep(Duration.ofSeconds(20).toMillis());
      assertFalse("Lock should be unlocking", lock.get().isCertainlyHeld());
      assertFalse("Lock should be unlocked", lock.get().isPerhapsHeld());
    }, L, A);
    transport.stop();
  }


  /**
   * Tests that withDistributedLockAsync doesn't block, even if the runnable does
   */
  @Test
  public void withDistributedLockBlockingRunnable() {
    Theseus L = new Theseus("L", transport, new HashSet<>(Arrays.asList("L", "A")), RaftLifecycle.newBuilder().mockTime().build());
    Theseus A = new Theseus("A", transport, new HashSet<>(Arrays.asList("L", "A")), RaftLifecycle.newBuilder().mockTime().build());

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
    Theseus L = new Theseus("L", transport, Collections.singleton("L"), RaftLifecycle.newBuilder().mockTime().build());
    withNodes(transport, nodes -> L.withDistributedLockAsync("key", () -> log.info("Got lock on L")).get(), L);
    transport.stop();
  }


  /**
   * Test locking and withElement by keeping a running sum
   */
  @Test
  public void withElement() {
    // Start the cluster
    int numThreads = 5;
    int numIters = 20;
    Random r = new Random(42L);
    Theseus[] nodes = new Theseus[]{
        new Theseus("L", transport, new HashSet<>(Arrays.asList("L", "A", "B")), RaftLifecycle.newBuilder().mockTime().build()),
        new Theseus("A", transport, new HashSet<>(Arrays.asList("L", "A", "B")), RaftLifecycle.newBuilder().mockTime().build()),
        new Theseus("B", transport, new HashSet<>(Arrays.asList("L", "A", "B")), RaftLifecycle.newBuilder().mockTime().build())
    };
    withNodes(transport, (raftNodes) -> {
      // Run the sum computation
      Set<Integer> numbersSeen = ConcurrentHashMap.newKeySet();
      AtomicInteger callCount = new AtomicInteger(0);
      AtomicInteger failureCount = new AtomicInteger(0);

      long startTime = System.currentTimeMillis();
      AtomicBoolean holdLock = new AtomicBoolean(false);
      List<Thread> threads = IntStream.range(0, numThreads).mapToObj(threadI -> {
        Thread t = new Thread(() -> {
          for (int i = 0; i < numIters; ++i) {
            AtomicInteger fnCallCount = new AtomicInteger(0);
            boolean success = false;
            long start = System.currentTimeMillis();
            try {
              Theseus node = raftNodes[r.nextInt(raftNodes.length)];
              CompletableFuture<Boolean> future = node.withElementAsync("running_sum", (oldBytes) -> {
                try {
                  if (holdLock.getAndSet(true)) {
                    log.warn("Someone else holds the running sum lock! This is going to cause an error");
                  }
                  callCount.incrementAndGet();
                  fnCallCount.incrementAndGet();
                  int value = ByteBuffer.wrap(oldBytes).getInt() + 1;
                  log.info("Adding {} to the seen set", value);
                  assertFalse(numbersSeen.contains(value));
                  numbersSeen.add(value);
                  return ByteBuffer.allocate(4).putInt(value).array();
                } finally {
                  holdLock.set(false);
                }
              }, () -> ByteBuffer.allocate(4).putInt(0).array(), true);
              success = future.get(10, TimeUnit.SECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
              e.printStackTrace();
            } finally {
              log.info("{} iteration {} / {} completed in {}", Thread.currentThread(), i + 1, numIters, TimerUtils.formatTimeSince(start));
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
      log.info("Doing "+(numIters * numThreads)+" atomic increments took "+ TimerUtils.formatTimeDifference(duration));
      log.info("Doing "+(numIters * numThreads)+" atomic increments took "+((double)duration / (numIters * numThreads))+"ms per increment");

      // Check the sum
      AtomicInteger intValue = new AtomicInteger(0);
      raftNodes[0].withElementAsync("running_sum", elem -> {
        intValue.set(ByteBuffer.wrap(elem).getInt());
        return elem;
      }, () -> null, false).get(10, TimeUnit.SECONDS);
      assertEquals("Should have made the right number of function calls", numThreads * numIters - failureCount.get(), callCount.get());
      assertEquals("Should have seen the right number of numbers", numThreads * numIters - failureCount.get(), numbersSeen.size());
      assertEquals("Running sum should have been consistent", numThreads * numIters - failureCount.get(), intValue.get());
    }, nodes);
    transport.stop();
  }


  /**
   * Ensure that {@link Theseus#withElementAsync(String, Function, Supplier, boolean)} always
   * releases its held locks
   *
   * @param partitionTime The amount of time to partition for before running withElements. A value of -1 ensures
   *                      a sufficiently long timeout that all the calls timeout and go into failsafe
   */
  private void withElementAlwaysReleasesLocks(long partitionTime) {
    // Start the cluster
    Theseus[] nodes = new Theseus[]{
        new Theseus("L", transport, new HashSet<>(Arrays.asList("L", "A", "B")), RaftLifecycle.newBuilder().mockTime().build()),
        new Theseus("A", transport, new HashSet<>(Arrays.asList("L", "A", "B")), RaftLifecycle.newBuilder().mockTime().build()),
        new Theseus("B", transport, new HashSet<>(Arrays.asList("L", "A", "B")), RaftLifecycle.newBuilder().mockTime().build())
    };
    withNodes(transport, (raftNodes) -> {
      Theseus L = raftNodes[0];
      // 1. Raise partition
      long adjustedPartitionTime = partitionTime;
      if (adjustedPartitionTime != 0) {
        log.info("Raising partition");
        if (adjustedPartitionTime < 0) {
          adjustedPartitionTime = nodes[0].defaultTimeout.toMillis() * 5;
        }
        transport.completePartition(transport.now(), transport.now() + adjustedPartitionTime, "L", "A", "B");
      }

      // 2. Run our calls
      List<CompletableFuture<Boolean>> futures = IntStream.range(0, 1000).mapToObj(iter ->
          L.withElementAsync("elem", value -> {
            log.debug("[{}] mutating sum", ByteBuffer.wrap(value).getInt());
            return ByteBuffer.allocate(4).putInt(ByteBuffer.wrap(value).getInt() + 1).array();
          }, () -> ByteBuffer.allocate(4).putInt(0).array(), true)
      ).collect(Collectors.toList());

      // 3. Complete futures
      for (CompletableFuture<Boolean> future : futures) {
        future.get(600000, TimeUnit.MILLISECONDS);  // assume we're running at least at 0.5x real time
      }

      // 4. Check the lock
      for (int i = 0; i < 100 && Arrays.stream(raftNodes).anyMatch(node -> node.stateMachine.locks.get("elem") != null); ++i) {
        transport.sleep(100);
      }
      for (Theseus node : raftNodes) {
        assertNull(node.stateMachine.locks.get("elem"));
      }
      log.info("----- TESTS PASS -----");
    }, nodes);
  }


  /**
   * @see #withElementAlwaysReleasesLocks(long)
   */
  @Test
  public void withElementAlwaysReleasesLocksSimple() {
    withElementAlwaysReleasesLocks(0);
  }


  /**
   * @see #withElementAlwaysReleasesLocks(long)
   */
  @Test
  public void withElementAlwaysReleasesLocksLongPartition() {
    withElementAlwaysReleasesLocks(-1);  // default timeout
  }


  /**
   * @see #withElementAlwaysReleasesLocks(long)
   */
  @Test
  public void withElementAlwaysReleasesLocksShortPartition() {
    withElementAlwaysReleasesLocks(EloquentRaftAlgorithm.DEFAULT_ELECTION_RANGE.end * 2);  // force a confused election
  }


  /**
   * Test that we can get and set an element
   */
  @Test
  public void getSetElement() {
    Theseus leader = new Theseus("L", transport, new HashSet<>(Arrays.asList("L", "A")), RaftLifecycle.newBuilder().mockTime().build());
    Theseus follower = new Theseus("A", transport, new HashSet<>(Arrays.asList("L", "A")), RaftLifecycle.newBuilder().mockTime().build());
    withNodes(transport, nodes -> {
      nodes[1].setElementAsync("element", new byte[]{42}, true, Duration.ofMinutes(1)).get();
      Optional<byte[]> elemOn1 = nodes[1].getElement("element");
      // sleep to give the actual commit time to propagate
      // note[gabor]: we reply back with success as soon as the log is *replicated* and we've committed,
      //              which is marginally before the entry is committed on the other node. So, we sleep.
      transport.sleep(nodes[0].node.algorithm.heartbeatMillis() * 2);
      Optional<byte[]> elemOn0 = nodes[0].getElement("element");
      assertTrue("Element committed to a node should be available on the calling node", elemOn1.isPresent());
      assertTrue("Element committed to a node should be available on another node", elemOn0.isPresent());
      assertArrayEquals("Should have been able to retrieve the set element on the calling node", new byte[]{42}, elemOn1.get());
      assertArrayEquals("Should have been able to retrieve the set element on another node", new byte[]{42}, elemOn0.get());
    }, leader, follower);
    transport.stop();
  }


  /**
   * Test that we can get and set an element
   */
  @Test
  public void getSetElementSingleNode() {
    Theseus leader = new Theseus("L", transport, Collections.singleton("L"), RaftLifecycle.newBuilder().mockTime().build());
    withNodes(transport, nodes -> {
      nodes[0].setElementAsync("element", new byte[]{42}, true, Duration.ofMinutes(1)).get(1, TimeUnit.SECONDS);
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
    Theseus leader = new Theseus("L", transport, new HashSet<>(Arrays.asList("L", "A")), RaftLifecycle.newBuilder().mockTime().build());
    Theseus follower = new Theseus("A", transport, new HashSet<>(Arrays.asList("L", "A")), RaftLifecycle.newBuilder().mockTime().build());
    withNodes(transport, nodes -> {
      // Set an element
      nodes[1].setElementAsync("element", new byte[]{42}, true, Duration.ofMinutes(1)).get();
      // sleep to give the actual commit time to propagate
      // note[gabor]: we reply back with success as soon as the log is *replicated* and we've committed,
      //              which is marginally before the entry is committed on the other node. So, we sleep.
      transport.sleep(nodes[0].node.algorithm.heartbeatMillis() * 2);
      Optional<byte[]> elem = nodes[0].getElement("element");
      assertTrue("Element committed to a node should be available on another node", elem.isPresent());
      assertArrayEquals("Should have been able to retrieve the set element", new byte[]{42}, elem.get());

      // Remove the element
      nodes[1].removeElementAsync("element", Duration.ofMinutes(1)).get();
      // sleep again; see above
      transport.sleep(nodes[0].node.algorithm.heartbeatMillis() * 2);

      // Element should not exist (on other server)
      Optional<byte[]> reread = nodes[0].getElement("element");
      assertEquals("Element should no longer be in the state machine", Optional.empty(), reread);
    }, leader, follower);
    transport.stop();
  }


  /**
   * Test that we can remove a set of elements
   */
  @Test
  public void removeElements() {
    Theseus leader = new Theseus("L", transport, new HashSet<>(Arrays.asList("L", "A")), RaftLifecycle.newBuilder().mockTime().build());
    Theseus follower = new Theseus("A", transport, new HashSet<>(Arrays.asList("L", "A")), RaftLifecycle.newBuilder().mockTime().build());
    withNodes(transport, nodes -> {
      // Set an element
      Set<String> keys = new HashSet<>();
      for (int i = 0; i < 10; i++) {
        nodes[1].setElementAsync("element"+i, new byte[]{42}, true, Duration.ofMinutes(1)).get();
        keys.add("element"+i);
      }

      // sleep to give the actual commit time to propagate
      // note[gabor]: we reply back with success as soon as the log is *replicated* and we've committed,
      //              which is marginally before the entry is committed on the other node. So, we sleep.
      transport.sleep(nodes[0].node.algorithm.heartbeatMillis() * 2);
      for (int i = 0; i < 10; i++) {
        Optional<byte[]> elem = nodes[0].getElement("element"+i);
        assertTrue("Element "+i+" committed to a node should be available on another node", elem.isPresent());
        assertArrayEquals("Should have been able to retrieve the set element "+i, new byte[]{42}, elem.get());
      }

      // Remove the element
      nodes[1].removeElementsAsync(keys, Duration.ofMinutes(1)).get();

      // Element should not exist (on other server)
      // sleep to give the actual commit time to propagate
      // note[gabor]: we reply back with success as soon as the log is *replicated* and we've committed,
      //              which is marginally before the entry is committed on the other node. So, we sleep.
      transport.sleep(nodes[0].node.algorithm.heartbeatMillis() * 2);
      for (int i = 0; i < 10; i++) {
        Optional<byte[]> reread = nodes[0].getElement("element"+i);
        assertEquals("Element "+i+" should no longer be in the state machine", Optional.empty(), reread);
      }
    }, leader, follower);
    transport.stop();
  }


  /**
   * Test getting the entire state machine map from a node.
   */
  @Test
  public void getMap() {
    Theseus leader = new Theseus("L", transport, new HashSet<>(Arrays.asList("L", "A")), RaftLifecycle.newBuilder().mockTime().build());
    Theseus follower = new Theseus("A", transport, new HashSet<>(Arrays.asList("L", "A")), RaftLifecycle.newBuilder().mockTime().build());
    withNodes(transport, nodes -> {
      // Set an element
      nodes[1].setElementAsync("element", new byte[]{42}, true, Duration.ofMinutes(1)).get();
      // sleep to give the actual commit time to propagate
      // note[gabor]: we reply back with success as soon as the log is *replicated* and we've committed,
      //              which is marginally before the entry is committed on the other node. So, we sleep.
      transport.sleep(nodes[0].node.algorithm.heartbeatMillis() * 2);
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
    Theseus leader = new Theseus("L", transport, new HashSet<>(Arrays.asList("L", "A")), RaftLifecycle.newBuilder().mockTime().build());
    Theseus follower = new Theseus("A", transport, new HashSet<>(Arrays.asList("L", "A")), RaftLifecycle.newBuilder().mockTime().build());
    AtomicInteger changesSeen = new AtomicInteger(0);
    withNodes(transport, nodes -> {
      KeyValueStateMachine.ChangeListener changeListener = (changedKey, newValue, state, pool) -> changesSeen.incrementAndGet();
      nodes[0].addChangeListener(changeListener);
      // Set an element
      nodes[1].setElementAsync("element", new byte[]{42}, true, Duration.ofMinutes(1)).get();
      // sleep to give the actual commit time to propagate
      // note[gabor]: we reply back with success as soon as the log is *replicated* and we've committed,
      //              which is marginally before the entry is committed on the other node. So, we sleep.
      transport.sleep(nodes[0].node.algorithm.heartbeatMillis() * 2);
      nodes[0].removeChangeListener(changeListener);
      nodes[1].setElementAsync("element", new byte[]{43}, true, Duration.ofMinutes(1)).get();
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
  private void permanence(boolean permanent, boolean dynamic, boolean shadow, Consumer<Theseus> killMethod) {
    Theseus L;
    Theseus A;
    Theseus B;
    if (dynamic) {
      L = new Theseus("L", transport, 3, RaftLifecycle.newBuilder().mockTime().build());
      A = new Theseus("A", transport, 3, RaftLifecycle.newBuilder().mockTime().build());
      B = new Theseus("B", transport, 3, RaftLifecycle.newBuilder().mockTime().build());
      L.bootstrap(false);
    } else {
      L = new Theseus("L", transport, new HashSet<>(Arrays.asList("L", "A", "B")), RaftLifecycle.newBuilder().mockTime().build());
      A = new Theseus("A", transport, new HashSet<>(Arrays.asList("L", "A", "B")), RaftLifecycle.newBuilder().mockTime().build());
      B = new Theseus("B", transport, new HashSet<>(Arrays.asList("L", "A", "B")), RaftLifecycle.newBuilder().mockTime().build());
    }
    withNodes(transport, (nodes) -> {
      Theseus submitter = A;
      if (shadow) {
        submitter = new Theseus("C", transport, 3, RaftLifecycle.newBuilder().mockTime().build());
        transport.sleep(1000);  // wait for the submitter to join
      }
      submitter.setElementAsync("transient", new byte[]{42}, permanent, Duration.ofSeconds(5)).get();
      // sleep to give the actual commit time to propagate
      // note[gabor]: we reply back with success as soon as the log is *replicated* and we've committed,
      //              which is marginally before the entry is committed on the other node. So, we sleep.
      transport.sleep(nodes[0].node.algorithm.heartbeatMillis() * 2);
      assertArrayEquals("Item should be submitted on same node", new byte[]{42}, submitter.getElement("transient").orElse(null));
      assertArrayEquals("Item should be submitted on other node", new byte[]{42}, L.getElement("transient").orElse(null));
      killMethod.accept(submitter);
      log.info("Waiting for election (in case our submitter was the leader)");
      awaitElection(transport, L, B);
      log.info("Waiting for machine to be removed");
      transport.sleep(EloquentRaftAlgorithm.MACHINE_DOWN_TIMEOUT + L.node.algorithm.heartbeatMillis() * 2);
      log.info("Waiting for election (again)");
      awaitElection(transport, L, B);
      log.info("Checking permanence");
      // As above, sleep enough for the removal transitions to propagate
      if (permanent) {
        assertArrayEquals("Permanent item should still be present (on B; leader=" + B.node.algorithm.mutableState().isLeader() + ")", new byte[]{42}, B.getElement("transient").orElse(null));
        assertArrayEquals("Permanent item should still be present (on L; leader=" + L.node.algorithm.mutableState().isLeader() + ")", new byte[]{42}, L.getElement("transient").orElse(null));
      } else {
        assertEquals("Transient item should no longer be present (on B; leader=" + B.node.algorithm.mutableState().isLeader() + ")", Optional.empty(), B.getElement("transient").map(Arrays::toString));
        assertEquals("Transient item should no longer be present (on L; leader=" + L.node.algorithm.mutableState().isLeader() + ")", Optional.empty(), L.getElement("transient").map(Arrays::toString));
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
   * Boots and shuts down three nodes. Runs some code in the middle.
   */
  private Theseus[] threeNodeSimpleTest(ThrowableConsumer<Theseus[]> withNodes) {
    Theseus L = new Theseus("L", transport, new HashSet<>(Arrays.asList("L", "A", "B")), RaftLifecycle.newBuilder().mockTime().build());
    Theseus A = new Theseus("A", transport, new HashSet<>(Arrays.asList("L", "A", "B")), RaftLifecycle.newBuilder().mockTime().build());
    Theseus B = new Theseus("B", transport, new HashSet<>(Arrays.asList("L", "A", "B")), RaftLifecycle.newBuilder().mockTime().build());
    withNodes(transport, withNodes, L, A, B);
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
    return new Theseus[]{L, A, B};
  }

  /**
   * This tests throwing an exception inside a withElementAsync
   */
  @Test
  public void testWithElementThrowsException() {
    final String KEY = "key";

    Theseus[] flushedStates = threeNodeSimpleTest((nodes) ->
      nodes[2].withElementAsync(KEY, (old) -> {
        throw new RuntimeException("Wat");
      }, () -> new byte[]{42}, true).get()
    );

    assertEquals(0, flushedStates[0].getLocks().size()); // We don't hold the lock
    assertEquals(0, flushedStates[1].getLocks().size()); // We don't hold the lock
    assertEquals(0, flushedStates[2].getLocks().size()); // We don't hold the lock
    assertFalse(flushedStates[0].getElement(KEY).isPresent()); // Value wasn't set
    assertFalse(flushedStates[1].getElement(KEY).isPresent()); // Value wasn't set
    assertFalse(flushedStates[2].getElement(KEY).isPresent()); // Value wasn't set
  }

  /**
   * This tests a simulated deadlock inside a withElementAsync mutator
   */
  @Test
  @Ignore
  public void testWithElementMutationDeadlock() {
    threeNodeSimpleTest((nodes) -> {
      String KEY = "key";

      nodes[2].withElementAsync(KEY, (old) -> {
        while (true) {
          try {
            Thread.sleep(1);
          } catch (InterruptedException e) {
            break;
          }
        }
        return null;
      }, () -> new byte[]{42}, true).get(5, TimeUnit.SECONDS);

      assertEquals(0, nodes[0].getLocks().size()); // We don't hold the lock
      assertFalse(nodes[0].getElement(KEY).isPresent()); // Value wasn't set
    });
  }

  /**
   * This tests a simulated deadlock inside a withElementAsync creator
   */
  @Ignore
  @Test
  public void testWithElementCreationDeadlock() {
    threeNodeSimpleTest((nodes) -> {
      String KEY = "key";

      nodes[2].withElementAsync(KEY, (old) -> old, () -> {
        while (true) {
          try {
            Thread.sleep(1);
          } catch (InterruptedException e) {
            break;
          }
        }
        return null;
      }, true).get(5, TimeUnit.SECONDS);

      assertEquals(0, nodes[0].getLocks().size()); // We don't hold the lock
      assertFalse(nodes[0].getElement(KEY).isPresent()); // Value wasn't set
    });
  }

  /**
   * This tests infinite looping inside a withElementAsync
   */
  @Ignore
  @Test
  public void testWithElementThreadInterrupt() {
    threeNodeSimpleTest((nodes) -> {
      String KEY = "key";

      Thread thread = new Thread(() -> {
        try {
          nodes[2].withElementAsync(KEY, (old) -> {
            for (int i = 0; i < 500; i++) {
              Uninterruptably.sleep(1);
            }
            return new byte[]{42};
          }, () -> new byte[]{42}, true).get(5, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException ignored) { }
      });
      thread.setDaemon(true);
      thread.start();
      Uninterruptably.sleep(100);
      thread.interrupt();
      thread.join();

      assertEquals(0, nodes[0].getLocks().size()); // We don't hold the lock
      assertFalse(nodes[0].getElement(KEY).isPresent()); // Value wasn't set
    });
  }


  /**
   * Check that when a new element is made via withElement, we save it to Raft even if it doesn't mutate.
   *
   * Related to merge request !78
   */
  @Test
  public void withElementNoMutationSaves() {
    Theseus raft = new Theseus("L", transport, Collections.singleton("L"), RaftLifecycle.newBuilder().mockTime().build());
    withNodes(transport, (nodes) -> {
      raft.withElementAsync("key", Function.identity(), () -> new byte[]{42}, false).get(1, TimeUnit.SECONDS);
      Optional<byte[]> retrieved = raft.getElement("key");
      assertTrue("Even without a mutation we should persist our created key", retrieved.isPresent());
      assertArrayEquals("The persisted key should have the correct value", new byte[]{42}, retrieved.orElse(null));
    }, raft);
  }


}