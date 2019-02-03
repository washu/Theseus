package ai.eloquent.raft;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.Consumer;

/**
 * <p>
 *   An implementation of a {@link RaftTransport} that runs a
 *   separate thread to make the outbound IO calls.
 *   At a high level, this class is a {@link java.util.concurrent.BlockingQueue}
 *   and a thread, mirroring a {@link Executors#newSingleThreadExecutor()}.
 * </p>
 *
 * <p>
 *   This means another thread running (in addition to the UDP and TCP listeners,
 *   at least for {@link NetRaftTransport}, but also that critical low-latency
 *   threads (e.g., the control thread of {@link SingleThreadedRaftAlgorithm} are
 *   not going to be held up on IO.
 * </p>
 *
 * @author <a href="mailto:gabor@eloquent.ai">Gabor Angeli</a>
 */
public abstract class DeferredRaftTransport implements RaftTransport {
  /**
   * An SLF4J Logger for this class.
   */
  private static final Logger log = LoggerFactory.getLogger(DeferredRaftTransport.class);


  /** If true, the transport is alive and accepting connections. */
  private boolean alive = true;

  /**
   * The queue of runnables we should execute on the deferred thread.
   */
  protected final LinkedBlockingDeque<Runnable> workQueue = new LinkedBlockingDeque<>();

  /**
   * A simple constructor that starts our deferred thread
   */
  protected DeferredRaftTransport() {
    Thread runner = new Thread(() -> {
      while (alive) {
        try {
          workQueue.take().run();
        } catch (Throwable t) {
          log.warn("Exception on transport thread: ", t);
        }
      }
    });
    runner.setName("raft-transport-runner");
    runner.setUncaughtExceptionHandler((t, e) -> log.error("Caught exception on thread {}", t, e));
    runner.setDaemon(true);
    runner.setPriority(Thread.NORM_PRIORITY + 1);
    runner.start();
  }


  /**
   * The synchronous implementation of {@link #rpcTransport(String, String, EloquentRaftProto.RaftMessage, Consumer, Runnable, long)}
   * that has the thread wait on IO
   */
  protected abstract void rpcTransportImpl(String sender, String destination, EloquentRaftProto.RaftMessage message, Consumer<EloquentRaftProto.RaftMessage> onResponseReceived, Runnable onTimeout, long timeout);


  /**
   * The synchronous implementation of {@link #sendTransport(String, String, EloquentRaftProto.RaftMessage)}
   * that has the thread wait on IO
   */
  public abstract void sendTransportImpl(String sender, String destination, EloquentRaftProto.RaftMessage message);


  /**
   * The synchronous implementation of {@link #broadcastTransport(String, EloquentRaftProto.RaftMessage)}
   * that has the thread wait on IO
   */
  public abstract void broadcastTransportImpl(String sender, EloquentRaftProto.RaftMessage message);


  /** {@inheritDoc} */
  @Override
  public final void rpcTransport(String sender, String destination, EloquentRaftProto.RaftMessage message, Consumer<EloquentRaftProto.RaftMessage> onResponseReceived, Runnable onTimeout, long timeout) {
    if (!this.workQueue.offer(() -> this.rpcTransportImpl(sender, destination, message, onResponseReceived, onTimeout, timeout))) {
      log.warn("Dropping rpcTransport message from {} -> {}; queue is full", sender, destination);
    }
  }


  /** {@inheritDoc} */
  @Override
  public final void sendTransport(String sender, String destination, EloquentRaftProto.RaftMessage message) {
    if (!this.workQueue.offer(() -> this.sendTransportImpl(sender, destination, message))) {
      log.warn("Dropping sendTransport message from {} -> {}; queue is full", sender, destination);
    }
  }


  /** {@inheritDoc} */
  @Override
  public final void broadcastTransport(String sender, EloquentRaftProto.RaftMessage message) {
    if (!this.workQueue.offer(() -> this.broadcastTransportImpl(sender, message))) {
      log.warn("Dropping sendTransport message from {}; queue is full", sender);
    }
  }


  /** Stop this transport's deferred thread. */
  @Override
  public void stop() {
    this.alive = false;
  }
}
