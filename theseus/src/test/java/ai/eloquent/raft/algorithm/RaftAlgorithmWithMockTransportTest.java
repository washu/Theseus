package ai.eloquent.raft.algorithm;

import ai.eloquent.raft.*;
import ai.eloquent.util.SafeTimerMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

/**
 * A common class for running Raft algorithm tests with our mock transport.
 *
 * @author <a href="mailto:gabor@eloquent.ai">Gabor Angeli</a>
 */
public abstract class RaftAlgorithmWithMockTransportTest extends AbstractRaftAlgorithmTest {

  /** The transport should be shared between nodes */
  protected LocalTransport transport;

  /** If true, our transport is considered stable. */
  protected final boolean isStable;

  protected RaftAlgorithmWithMockTransportTest(boolean isStable) {
    this.transport = new LocalTransport(isStable);
    this.isStable = isStable;
  }

  protected abstract RaftAlgorithm create(RaftState state, RaftTransport transport);


  /** {@inheritDoc} */
  @Override
  protected RaftAlgorithm create(RaftState state) {
    RaftAlgorithm impl = create(state, transport);
    transport.bind(impl);
    return impl;
  }


  /** {@inheritDoc} */
  @Override
  protected void waitForSilence(RaftAlgorithm algorithm) {
    try {
      if (algorithm instanceof SingleThreadedRaftAlgorithm) {
        ((SingleThreadedRaftAlgorithm) algorithm).flush(transport::waitForSilence);
      } else {
        transport.waitForSilence();
      }
    } catch (IllegalStateException e) {
      e.printStackTrace();
      System.exit(42);  // exit to prevent infinite looping
    }
  }


  /** {@inheritDoc} */
  @Override
  protected boolean isStableTransport() {
    return isStable;
  }


  /** {@inheritDoc} */
  @Override
  protected RaftTransport transport() {
    return transport;
  }


  /** {@inheritDoc} */
  @Override
  protected void schedule(long interval, int count, BiConsumer<Integer, Long> task) {
    AtomicInteger index = new AtomicInteger(0);
    transport.schedule(interval, count, now -> task.accept(index.getAndIncrement(), now));
  }


  /** {@inheritDoc} */
  @Override
  protected void partitionOff(long fromMillis, long toMillis, String... nodeNames) {
    transport.partitionOff(fromMillis, toMillis, nodeNames);
  }

  /** {@inheritDoc} */
  @Override
  protected long now() {
    return transport.now();
  }

  /** {@inheritDoc} */
  @Before
  public void resetTransport() {
    if (transport != null) {
      transport.stop();
    }
    SafeTimerMock.resetMockTime();
    transport = new LocalTransport(true);
  }

  @After
  public void stopTransport() {
    transport.stop();
  }

  Logger log = LoggerFactory.getLogger(RaftAlgorithmWithMockTransportTest.class);

  /**
   * This is a little convenience method to hit failing tests over and over again to bring out any race conditions in
   * the tests.
   */
  @Ignore
  @Test
  public void hammerFailingTests() throws InterruptedException, ExecutionException, TimeoutException {
    for (int i = 0; i < 10000; i++) {
      log.info("\n\n\n\n***********************\ntestSubmitTransitionCommitToFollowers():\n\n");
      this.resetTransport();
      this.testSubmitTransitionCommitToFollowers();
      this.stopTransport();

      log.info("\n\n\n\n***********************\ntestSubmitTransitionFromFollower():\n\n");
      this.resetTransport();
      this.testSubmitTransitionFromFollower();
      this.stopTransport();
    }
  }
}
