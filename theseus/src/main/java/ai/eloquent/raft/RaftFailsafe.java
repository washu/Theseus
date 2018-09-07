package ai.eloquent.raft;

/**
 * Failsafes are custom detectors that can make use of deploy-specific information (for example, spying on Kubernetes
 * config) to cheat on CAP theorem limits about detecting cluster deadlocks.
 */
public interface RaftFailsafe {
  /**
   * This gets called every heartbeat from an EloquentRaftAlgorithm where this failsafe is registered.
   *
   * @param algorithm the calling algorithm
   * @param now the current time
   */
  void heartbeat(RaftAlgorithm algorithm, long now);
}
