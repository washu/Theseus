package ai.eloquent.raft.transport;

import ai.eloquent.raft.EloquentRaftProto;
import ai.eloquent.raft.LocalTransport;
import ai.eloquent.raft.RaftTransport;
import ai.eloquent.raft.RaftAlgorithm;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static org.junit.Assert.*;

/**
 * The suite of tests to run on a Raft transport, given the mechanism for creating the transport.
 *
 * @author <a href="mailto:gabor@eloquent.ai">Gabor Angeli</a>
 */
public abstract class AbstractRaftTransportTest {
  /**
   * An SLF4J Logger for this class.
   */
  private static final Logger log = LoggerFactory.getLogger(AbstractRaftTransportTest.class);


  /** Create a transport */
  protected abstract RaftTransport create(RaftAlgorithm node) throws IOException;


  /** Reset the transport. Useful if we want to run multiple tests from a single test (e.g., a fuzz test) */
  @Before
  public abstract void resetTransport() throws IOException;

  @SuppressWarnings("unused")
  @After
  public abstract void stopTransport() throws IOException;


  /** The number of times to retry a function on the transport. */
  protected abstract int retryCount();


  /** The minimum number of times the function has to pass on the transport. */
  protected abstract int minSuccessCount();


  /** The maximum number of times the function can pass on the transport. */
  protected abstract int maxSuccessCount();


  /** If true, this transport is assumed stable. */
  protected boolean isStable() {
    return retryCount() == 1;
  }


  /** For any real transport, we don't want to run the fuzz tests. Fuzz testing the transport itself is someone else's job. */
  protected boolean shouldRunFuzzTests() {
    return false;
  }


  /**
   * A little mock cluster with three nodes, that implements {@link AutoCloseable}.
   */
  class MockCluster implements AutoCloseable {
    String sendName = "sender";
    String recvName = "receiver";
    String thirdName = "third_wheel";
    RecordingRaftimplementation sendNode = new RecordingRaftimplementation(sendName);
    RecordingRaftimplementation receiveNode = new RecordingRaftimplementation(recvName);
    RecordingRaftimplementation thirdNode = new RecordingRaftimplementation(thirdName);
    RaftTransport sendTransport = create(sendNode);
    RaftTransport recvTransport = create(receiveNode);
    RaftTransport thirdTransport = create(thirdNode);

    private MockCluster() throws IOException { }

    /** Stop all of the transports. */
    @Override
    public void close() {
      if (sendTransport instanceof LocalTransport) {
        ((LocalTransport) sendTransport).waitForSilence();
      }
      sendTransport.stop();
      if (recvTransport instanceof LocalTransport) {
        ((LocalTransport) recvTransport).waitForSilence();
      }
      recvTransport.stop();
      if (thirdTransport instanceof LocalTransport) {
        ((LocalTransport) thirdTransport).waitForSilence();
      }
      thirdTransport.stop();
    }
  }


  /** Assert thet the given runnable should throw the given exception (or something compatible). */
  @SuppressWarnings("SameParameterValue")
  protected void assertException(Runnable r, Class<? extends Throwable> expectedException) {
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


  /** Assert that the number of successes is at least {@link #minSuccessCount()} and at most {@link #maxSuccessCount()}. */
  protected void assertSuccessCount(String message, Number count, int minSuccessCount, int maxSuccessCount) {
    assertTrue("Test did not pass at least " + minSuccessCount + " times (" + count + "): " + message,
        count.longValue() >= minSuccessCount);
    assertTrue("Test passed more than " + maxSuccessCount + " times (" + count + "): " + message,
        count.longValue() <= maxSuccessCount);
    log.info("OK: {} count was {}", message, count);
  }


  /** @see #assertSuccessCount(String, Number, int, int) */
  protected void assertSuccessCount(String message, Number count) {
    assertSuccessCount(message, count, minSuccessCount(), maxSuccessCount());
  }


  /**
   * Create a mock cluster, call the specified function on it
   * {@link #retryCount()} number of times, and then close the cluster.
   * This is not terribly complicated, but it a common enough pattern
   * that I wanted to pull it out as a helper.
   *
   * @param fn The function to perform on the cluster (potentially multiple times)
   * @param retryCount The number of times to retry this. Defaults to {@link #retryCount()}.
   *
   * @return The <b>finished and stopped</b> cluster that we can inspect.
   */
  protected MockCluster doOnCluster(Consumer<MockCluster> fn, int retryCount) {
    try {
      MockCluster cluster = new MockCluster();
      try {
        // 1. Run the function
        for (int i = 0; i < retryCount; ++i) {
          fn.accept(cluster);
        }
      } finally {
        // 2. Close the cluster (and wait for everything to flush)
        cluster.close();
      }
      // 3. Run some sanity checks, if the transport is a mock transport
      if (cluster.sendTransport instanceof LocalTransport) {
        ((LocalTransport) cluster.sendTransport).assertNoErrors();
      }
      if (cluster.recvTransport instanceof LocalTransport) {
        ((LocalTransport) cluster.recvTransport).assertNoErrors();
      }
      if (cluster.thirdTransport instanceof LocalTransport) {
        ((LocalTransport) cluster.thirdTransport).assertNoErrors();
      }
      // 4. Return
      return cluster;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }


  /**
   * @see #doOnCluster(Consumer, int)
   */
  protected MockCluster doOnCluster(Consumer<MockCluster> fn) {
    return doOnCluster(fn, retryCount());
  }


  /**
   * Create a mock transport, and immediately stop it.
   */
  @Test
  public void testCreate() throws IOException {
    create(new RecordingRaftimplementation("server")).stop();
  }


  /**
   * Send a non-blocking message on the transport, and make sure it's arrived.
   */
  @Test
  public void testSend() {
    MockCluster closedCluster = doOnCluster(cluster ->
      cluster.sendTransport.sendTransport(cluster.sendName, cluster.recvName,
          EloquentRaftProto.AppendEntriesRequest.newBuilder().setTerm(42L).build())
    );
    assertSuccessCount("receiveNode.appendEntriesReceived.size()",
        closedCluster.receiveNode.appendEntriesReceived.size());
  }


  /**
   * Just runs {@link #testSend()} a bunch, which starts and stops a bunch of transports with the minimal number
   * of submits on each. Should coax out common race conditions when synchronizing on the queue.
   */
  @Ignore // this is slow!
  @Test
  public void testSubmitMessageRaceConditions() throws IOException {
    Assume.assumeTrue(shouldRunFuzzTests());  // this counts as a fuzz test
    for (int i = 0; i < 100; ++i) {
      log.info("Iteration {}", i);
      testSend();
      resetTransport();  // reset after each iteration
    }
  }


  /**
   * Send an RPC (blocking) message on the transport, and make sure it's arrived.
   */
  @Test
  public void testRpc() {
    AtomicLong responsesReceived = new AtomicLong(0);
    AtomicLong timeoutsReceived = new AtomicLong(0);
    doOnCluster(cluster ->
        cluster.sendTransport.rpcTransport(cluster.sendName, cluster.recvName,
            EloquentRaftProto.AddServerRequest.newBuilder().setNewServer("new_server").build(),
            resp -> responsesReceived.incrementAndGet(),
            timeoutsReceived::incrementAndGet,
            1000L)
    );
    assertSuccessCount("responses received", responsesReceived.get());
    if (!isStable()) {
      assertSuccessCount("timeouts received", timeoutsReceived.get());
    }
    assertEquals("Every message should either receive a reply or a timeout",
        retryCount(), responsesReceived.get() + timeoutsReceived.get());
  }


  /**
   * Broadcast a message to all of the other nodes in the cluster.
   */
  @Test
  public void testBroadcast() {
    MockCluster closedCluster = doOnCluster(cluster ->
        cluster.sendTransport.broadcastTransport(cluster.sendName,
            EloquentRaftProto.AppendEntriesRequest.newBuilder().setTerm(42L).build())
    );
    assertSuccessCount("node 1 responses received", closedCluster.receiveNode.appendEntriesReceived.size());
    assertSuccessCount("node 2 responses received", closedCluster.thirdNode.appendEntriesReceived.size());
    assertEquals("We should not broadcast to the person sending the broadcast out",
        0, closedCluster.sendNode.appendEntriesReceived.size());
  }


  /**
   * Set the timeout for an RPC to be within the range of network delay.
   * Note that this method has its own retry semantics, and ignores {@link #retryCount()} and friends.
   */
  @Ignore  // note[gabor]: Good to run on occasion, but this is inherently nondeterministic
  @Test
  public void testMaybeTimeout() {
    Assume.assumeTrue(shouldRunFuzzTests());  // this counts as a fuzz test
    AtomicLong responsesReceived = new AtomicLong(0);
    AtomicLong timeoutsReceived = new AtomicLong(0);
    int count = 5000;
    doOnCluster(cluster ->
        cluster.sendTransport.rpcTransport(cluster.sendName, cluster.recvName,
            EloquentRaftProto.AddServerRequest.newBuilder().setNewServer("new_server").build(),
            resp -> responsesReceived.incrementAndGet(),
            timeoutsReceived::incrementAndGet,
            cluster.sendTransport.expectedNetworkDelay().middle()),  // timeout set to be inside of network latency
        count  // always run for 100 tries -- even if the transport is stable
    );
    assertSuccessCount("responses received", responsesReceived.get(), 1, count);
    assertSuccessCount("timeouts received", timeoutsReceived.get(), 1, count);  // should timeout even on stable transports
    assertEquals("Every message should either receive a reply or a timeout",
        count, responsesReceived.get() + timeoutsReceived.get());
  }


  /**
   * Ensure that binding an algorithm is idempotent.
   */
  @Test
  public void testIdempotentBind() throws IOException {
    RecordingRaftimplementation algorithm = new RecordingRaftimplementation("server");
    RaftTransport transport = create(algorithm);
    assertEquals("Should only bind node once to algorithm, on initialization", 1, transport.boundAlgorithms().size());
    transport.bind(algorithm);
    assertEquals("Should only bind node once to algorithm, on identical bind", 1, transport.boundAlgorithms().size());
    RecordingRaftimplementation copy = new RecordingRaftimplementation("server");
    transport.bind(copy);
    assertEquals("Should only bind node once to algorithm, on equal bind", 1, transport.boundAlgorithms().size());
  }

}
