package ai.eloquent.raft.transport;

import ai.eloquent.raft.EloquentRaftProto;
import ai.eloquent.raft.LocalTransport;
import ai.eloquent.raft.RaftTransport;
import ai.eloquent.raft.RaftAlgorithm;
import ai.eloquent.test.SlowTests;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;


/**
 * Test the mock Raft transport, to make sure it's working as expected.
 * This tests the stable variant of the transport, without dropped packets or IOExceptions.
 *
 * @author <a href="mailto:gabor@eloquent.ai">Gabor Angeli</a>
 */
@Category(SlowTests.class)
public class StableMockRaftTransportTest extends AbstractRaftTransportTest {
  /**
   * An SLF4J Logger for this class.
   */
  @SuppressWarnings("unused")
  private static final Logger log = LoggerFactory.getLogger(StableMockRaftTransportTest.class);

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
    transport = new LocalTransport(true);
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
    return 1;
  }

  /** {@inheritDoc} */
  @Override
  protected int minSuccessCount() {
    return retryCount();
  }


  /** {@inheritDoc} */
  @Override
  protected int maxSuccessCount() {
    return retryCount();
  }


  /** {@inheritDoc} */
  @Override
  protected boolean shouldRunFuzzTests() {
    return true;
  }


  /**
   * Ensure that we can't send through a single partition
   */
  @Test
  public void testSinglePartition() {
    MockCluster closedCluster = doOnCluster(cluster -> {
      transport.partitionOff(0, Long.MAX_VALUE, cluster.recvName);
      cluster.sendTransport.sendTransport(cluster.sendName, cluster.recvName,
          EloquentRaftProto.AppendEntriesRequest.newBuilder().setTerm(42L).build());
    });
    assertEquals("Should not have received message over partition", 0, closedCluster.receiveNode.appendEntriesReceived.size());
  }


  /**
   * Ensure that we can still send messages within a partition
   */
  @Test
  public void testCanSendInsidePartition() {
    MockCluster closedCluster = doOnCluster(cluster -> {
      transport.partitionOff(0, Long.MAX_VALUE, cluster.sendName, cluster.recvName);
      cluster.sendTransport.sendTransport(cluster.sendName, cluster.recvName,
          EloquentRaftProto.AppendEntriesRequest.newBuilder().setTerm(42L).build());
    });
    assertEquals("Should be able to send message within partition", 1, closedCluster.receiveNode.appendEntriesReceived.size());
  }


  /**
   * Ensure that we can't send through multiple partitions
   */
  @Test
  public void testMultiplePartitions() {
    MockCluster closedCluster = doOnCluster(cluster -> {
      transport.partitionOff(0, Long.MAX_VALUE, cluster.sendName, cluster.recvName);
      transport.partitionOff(0, Long.MAX_VALUE, cluster.recvName, cluster.thirdName);
      cluster.sendTransport.sendTransport(cluster.sendName, cluster.recvName,
          EloquentRaftProto.AppendEntriesRequest.newBuilder().setTerm(42L).build());
    });
    assertEquals("Should not have received message over partition", 0, closedCluster.receiveNode.appendEntriesReceived.size());
  }
}
