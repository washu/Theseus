package ai.eloquent.raft.transport;

import ai.eloquent.raft.LocalTransport;
import ai.eloquent.raft.RaftTransport;
import ai.eloquent.raft.RaftAlgorithm;
import ai.eloquent.test.SlowTests;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Test the mock Raft transport, to make sure it's working as expected.
 * This tests the leaky variant of the transport, with dropped packets and IOExceptions.
 *
 * @author <a href="mailto:gabor@eloquent.ai">Gabor Angeli</a>
 */
@Category(SlowTests.class)
public class LeakyMockRaftTransportTest extends AbstractRaftTransportTest {
  /**
   * An SLF4J Logger for this class.
   */
  @SuppressWarnings("unused")
  private static final Logger log = LoggerFactory.getLogger(LeakyMockRaftTransportTest.class);


  /** The singleton transport implementation */
  private LocalTransport transport;

  /** {@inheritDoc} */
  @Override
  public RaftTransport create(RaftAlgorithm node) {
    transport.bind(node);
    return transport;  // important: same transport for all nodes
  }


  /** {@inheritDoc} */
  @Override
  public void resetTransport() {
    if (transport != null) {
      transport.stop();
    }
    transport = new LocalTransport(false);
  }

  @Override
  public void stopTransport() {
    transport.stop();
  }


  @After
  public void killTransport() {
    if (transport != null) {
      transport.stop();
    }
  }


  /** {@inheritDoc} */
  @Override
  protected int retryCount() {
    return 100;
  }

  /** {@inheritDoc} */
  @Override
  protected int minSuccessCount() {
    return 1;
  }


  /** {@inheritDoc} */
  @Override
  protected int maxSuccessCount() {
    return retryCount() - 1;
  }


  /** {@inheritDoc} */
  @Override
  protected boolean shouldRunFuzzTests() {
    return true;
  }


  /**
   * Not really specific to robust/nonrobust raft, but still good to have somewhere.
   */
  @Test
  public void testArgumentConstructor() {
    if (transport != null) {
      transport.stop();  // stop the default transport
    }
    for (long delayMin : new long[]{-10, 0, 10, 100}) {
      for (long delayMax : new long[]{-10, 0, 10, 100}) {
        for (double dropProb : new double[]{-0.5, 0.0, 0.1, 0.3, 1.5}) {
          for (double ioProb : new double[]{-0.5, 0.0, 0.1, 0.3, 1.5}) {
            for (boolean trueTime : new boolean[]{true, false}) {
              if (delayMax < delayMin) {
                assertException(() -> new LocalTransport(delayMin, delayMax, dropProb, ioProb, trueTime, 42L).assertNoErrors().stop(), IllegalArgumentException.class);
              } else if (delayMin < 1) {
                assertException(() -> new LocalTransport(delayMin, delayMax, dropProb, ioProb, trueTime, 42L).assertNoErrors().stop(), IllegalArgumentException.class);
              } else if (delayMax < 0) {
                assertException(() -> new LocalTransport(delayMin, delayMax, dropProb, ioProb, trueTime, 42L).assertNoErrors().stop(), IllegalArgumentException.class);
              } else if (dropProb < 0.0 || dropProb > 1.0) {
                assertException(() -> new LocalTransport(delayMin, delayMax, dropProb, ioProb, trueTime, 42L).assertNoErrors().stop(), IllegalArgumentException.class);
              } else if (ioProb < 0.0 || ioProb > 1.0) {
                assertException(() -> new LocalTransport(delayMin, delayMax, dropProb, ioProb, trueTime, 42L).assertNoErrors().stop(), IllegalArgumentException.class);
              } else {
                // Create the raft transport
                new LocalTransport(delayMin, delayMax, dropProb, ioProb, trueTime, 42L).assertNoErrors().stop();
              }
            }
          }
        }
      }
    }
  }
}
