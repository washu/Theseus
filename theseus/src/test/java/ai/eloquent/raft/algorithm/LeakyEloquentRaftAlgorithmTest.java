package ai.eloquent.raft.algorithm;

import ai.eloquent.raft.*;

import java.util.Optional;
import java.util.concurrent.Executors;

/**
 * Test the {@link EloquentRaftAlgorithm} on a leaky transport (i.e., one that dropps packets / throws exceptions).
 *
 * @author <a href="mailto:gabor@eloquent.ai">Gabor Angeli</a>
 */
public class LeakyEloquentRaftAlgorithmTest extends RaftAlgorithmWithMockTransportTest {

  public LeakyEloquentRaftAlgorithmTest() {
    super(false);
  }


  /** {@inheritDoc} */
  @Override
  protected RaftAlgorithm create(RaftState state, RaftTransport transport) {
    return new SingleThreadedRaftAlgorithm(new EloquentRaftAlgorithm(state, transport, Optional.empty()), Executors.newSingleThreadExecutor());
  }
}
