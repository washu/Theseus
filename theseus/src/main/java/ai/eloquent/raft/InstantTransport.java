package ai.eloquent.raft;

import ai.eloquent.util.SafeTimerTask;
import ai.eloquent.util.Span;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * This is used for benchmarking, and to do JIT burn-ins for latency sensitive code paths.
 */
public class InstantTransport implements RaftTransport {
  /**
   * An SLF4J Logger for this class.
   */
  private static final Logger log = LoggerFactory.getLogger(InstantTransport.class);

  Map<String, RaftAlgorithm> bound = new HashMap<>();

  @Override
  public void bind(RaftAlgorithm listener) {
    bound.put(listener.serverName(), listener);
  }

  @Override
  public Collection<RaftAlgorithm> boundAlgorithms() {
    return bound.values();
  }

  @Override
  public void rpcTransport(String sender, String destination, EloquentRaftProto.RaftMessage message, Consumer<EloquentRaftProto.RaftMessage> onResponseReceived, Runnable onTimeout, long timeout) {
    try {
      EloquentRaftProto.RaftMessage reply = bound.get(destination).receiveRPC(message, true).get();
      onResponseReceived.accept(reply);
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void sendTransport(String sender, String destination, EloquentRaftProto.RaftMessage message) {
    bound.get(destination).receiveMessage(message, (reply) -> sendTransport(destination, sender, reply), true);
  }

  @Override
  public void broadcastTransport(String sender, EloquentRaftProto.RaftMessage message) {
    for (RaftAlgorithm algorithm : bound.values()) {
      if (!algorithm.serverName().equals(sender)) {
        try {
          EloquentRaftProto.RaftMessage reply = algorithm.receiveRPC(message, true).get();
          sendTransport(algorithm.serverName(), sender, reply);
        } catch (InterruptedException | ExecutionException e) {
          e.printStackTrace();
        }
      }
    }
  }

  @Override
  public Span expectedNetworkDelay() {
    return new Span(0,1);
  }

  @Override
  public void scheduleHeartbeat(Supplier<Boolean> alive, long period, Runnable heartbeatFn, Logger log) {
    // Ignore
  }

  /** Schedule an event every |period| seconds. This is an alias for {@link ai.eloquent.util.SafeTimer#scheduleAtFixedRate(SafeTimerTask, long, long)}, but mockable */
  @Override
  public void scheduleAtFixedRate(SafeTimerTask timerTask, long period) {
    // ignore
  }


  /** Schedule an event every |period| seconds. This is an alias for {@link ai.eloquent.util.SafeTimer#schedule(SafeTimerTask, long)}, but mockable */
  @Override
  public void schedule(SafeTimerTask timerTask, long delay) {
    // ignore
  }

  private static class AddOne implements Function<byte[], byte[]> {
    @Override
    public byte[] apply(byte[] oldBytes) {
      int value = ByteBuffer.wrap(oldBytes).getInt() + 1;
      return ByteBuffer.allocate(4).putInt(value).array();
    }
  }

  /**
   * This runs a batch of burn-in requests
   *
   * @param L node to burn in on
   * @param numIterations the number of iterations to run
   */
  private static double burnInRun(Theseus L, long numIterations) {
    long startTime = System.nanoTime();
    for (int i = 0; i < numIterations; i++) {
      try {
        L.withElementAsync("key_"+(i % 50), new AddOne(), () -> ByteBuffer.allocate(4).putInt(0).array(), true).get(5, TimeUnit.SECONDS);
        L.node.algorithm.heartbeat();
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        log.warn("Caught exception running JIT burn in:", e);
      }
      /*
      L.node.algorithm.receiveApplyTransitionRPC(EloquentRaftProto.ApplyTransitionRequest.newBuilder()
          .setTerm(0)
          .setTransition(ByteString.copyFrom(KeyValueStateMachine.createSetValueTransition("key_"+(i % 50), new byte[]{10})))
          .build(), i * 1000);
          */
    }
    return (double)(System.nanoTime() - startTime) / numIterations;
  }

  /**
   * We use this to burn in the important routines in Raft
   */
  public static void burnInJIT() {
    log.info("Ignoring the burn in run on Raft, since the InstantTransport deadlocks...");

    /*
    log.info("Setting up to burn in the JIT on Raft");

    int oldLogLevel = EloquentLogger.level();
    EloquentLogger.setLevel(2); // the normal level for logs, so we don't print trace logs during burn-in

    InstantTransport transport = new InstantTransport();
    RaftLifecycle lifecycle = RaftLifecycle.newBuilder().mockTime().build();
    Theseus L = new Theseus(new SingleThreadedRaftAlgorithm(new EloquentRaftAlgorithm("L", new KeyValueStateMachine("L"), transport, 3, MoreExecutors.newDirectExecutorService(), Optional.of(lifecycle)), MoreExecutors.newDirectExecutorService()), transport, lifecycle);
    Theseus A = new Theseus(new SingleThreadedRaftAlgorithm(new EloquentRaftAlgorithm("A", new KeyValueStateMachine("A"), transport, 3, MoreExecutors.newDirectExecutorService(), Optional.of(lifecycle)), MoreExecutors.newDirectExecutorService()), transport, lifecycle);
    Theseus B = new Theseus(new SingleThreadedRaftAlgorithm(new EloquentRaftAlgorithm("B", new KeyValueStateMachine("B"), transport, 3, MoreExecutors.newDirectExecutorService(), Optional.of(lifecycle)), MoreExecutors.newDirectExecutorService()), transport, lifecycle);
    L.start();
    A.start();
    B.start();
    L.bootstrap(false);
    L.node.algorithm.triggerElection(0);
    L.node.algorithm.heartbeat(100);
    L.node.algorithm.mutableState().log.latestQuorumMembers.add("A");
    L.node.algorithm.mutableState().log.latestQuorumMembers.add("B");
    A.node.algorithm.mutableState().log.latestQuorumMembers.add("L");
    A.node.algorithm.mutableState().log.latestQuorumMembers.add("B");
    B.node.algorithm.mutableState().log.latestQuorumMembers.add("L");
    B.node.algorithm.mutableState().log.latestQuorumMembers.add("A");

    // Do a first pass to calibrate initial speed

    log.info("Burning in JIT (This takes a few seconds) ...");
    double initialSpeed = burnInRun(L, 10L);

    // Do a big run to burn in the JIT

    burnInRun(L, 50000L);

    // Do another pass to figure out improvement in speed

    double jitSpeed = burnInRun(L, 10L);

    log.info("JIT results:\nInitial speed: "+initialSpeed+" nanosec compute / commit\nJIT speed: "+jitSpeed+" nanosec compute / commit\nImprovement: "+(initialSpeed * 100 / jitSpeed)+"%");

    lifecycle.shutdown(true);
    L.close();
    A.close();
    B.close();

    log.info("Done burning in JIT...");
    EloquentLogger.setLevel(oldLogLevel);
    */
  }
}
