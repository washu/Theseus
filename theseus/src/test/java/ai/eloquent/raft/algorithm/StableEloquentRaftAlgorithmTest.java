package ai.eloquent.raft.algorithm;

import ai.eloquent.raft.*;

import java.util.Optional;
import java.util.concurrent.Executors;

/**
 * Test the {@link EloquentRaftAlgorithm} on a stable transport.
 *
 * @author <a href="mailto:gabor@eloquent.ai">Gabor Angeli</a>
 */
public class StableEloquentRaftAlgorithmTest extends RaftAlgorithmWithMockTransportTest {
  public StableEloquentRaftAlgorithmTest() {
    super(true);
  }


  /** {@inheritDoc} */
  @Override
  protected RaftAlgorithm create(RaftState state, RaftTransport transport) {
    return new SingleThreadedRaftAlgorithm(new EloquentRaftAlgorithm(state, transport, Optional.empty()), Executors.newSingleThreadExecutor());
  }


  /**
   * Like {@link #catchUpShadowOnLoad()}, but there are a lot of commits on the cluster already,
   * and so we have to get both the most recent snapshot and the most recent log entries.
   */
  /*
  @Test
  public void catchUpShadowOnLoadSnapshotAndLogsLoop() throws IOException {
    for (int i = 0; i < 10000; i++) {
      resetTransport();
      addFollowerScript(RaftLog.COMPACTION_LIMIT * 20 + 25, true, false);
      killTransport();
    }
  }
  */

}
