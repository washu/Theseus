package ai.eloquent.raft;

import java.io.Closeable;

/**
 * A minimal interface for a Raft-like component that can be registered in
 * a lifecycle. This is primarily useful for unit tests, where we can
 * close either a {@link EloquentRaftNode} or a {@link Theseus} instance depending
 * on the needs of the test.
 *
 * Usually, we should be closing a {@link Theseus} in production.
 *
 * @author <a href="mailto:gabor@eloquent.ai">Gabor Angeli</a>
 */
public interface HasRaftLifecycle extends Closeable, AutoCloseable {

  /**
   * The name of this Raft node.
   */
  String serverName();


  /**
   * Stop this Raft-like object.
   *
   * @param allowClusterDeath If true, allow the cluster to lose state, with this
   *                          being the last member of the cluster leaving. Otherwise,
   *                          we wait for another member to take over before dying.
   */
  void close(boolean allowClusterDeath);


  /**
   * Stop this Raft-like object.
   * In the default implementation, this disallows cluster death.
   *
   * @see #close(boolean)
   */
  default void close() {
    this.close(false);
  }
}
