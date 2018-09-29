package ai.eloquent.raft;

import ai.eloquent.data.UDPBroadcastProtos;
import ai.eloquent.data.UDPTransport;
import ai.eloquent.util.ConcurrencyUtils;
import ai.eloquent.util.IdentityHashSet;
import ai.eloquent.util.SafeTimerTask;
import ai.eloquent.util.Span;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.*;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * A Raft transport implemented directly over UDP
 *
 * @author <a href="mailto:gabor@eloquent.ai">Gabor Angeli</a>
 */
public class NetRaftTransport implements RaftTransport {
  /**
   * An SLF4J Logger for this class.
   */
  private static final Logger log = LoggerFactory.getLogger(NetRaftTransport.class);

  /**
   * The port we are listening to RPC messages on.
   */
  private static final int DEFAULT_RPC_LISTEN_PORT = 42889;

  /**
   * The name we should assign ourselves on the transport.
   */
  public final String serverName;

  /**
   * If true, run the handling of messages on the transport in a new thread.
   */
  public final boolean thread;

  /**
   * The set of algorithms bound to this transport, so that we don't
   * add the same algorithm twice.
   */
  private final Set<RaftAlgorithm> boundAlgorithms = new IdentityHashSet<>();

  /**
   * The port we are listening to RPC messages on.
   */
  private final int rpcListenPort;

  /**
   * A map from destination address to a gRPC managed channel for that destination.
   */
  private final Map<String, ManagedChannel> channel = new HashMap<>();


  /**
   * Create a UDP transport.
   *
   * @param serverName The name of our server
   * @param rpcListenPort The gRPC port to listen on.
   * @param async If true, run messages on the transport in separate threads.
   *
   * @throws UnknownHostException Thrown if we could not get our own hostname.
   */
  public NetRaftTransport(String serverName, int rpcListenPort, boolean async) throws IOException {
    // Save some trivial variables
    this.serverName = serverName;
    this.rpcListenPort = rpcListenPort;
    this.thread = async;

    // Assertions
    if (!serverName.matches("[12]?[0-9]?[0-9]\\.[12]?[0-9]?[0-9]\\.[12]?[0-9]?[0-9]\\.[12]?[0-9]?[0-9](_.*)?")) {
      throw new IllegalArgumentException("Invalid server name \""+serverName+"\". Server name must start with an IPv4 address, followed by an optional underscore and custom descriptor. For example, \"127.0.0.1_foobar\".");
    }

    // Start the Grpc Server
    ServerBuilder
        .forPort(rpcListenPort)
        .addService(new RaftGrpc.RaftImplBase() {
          @Override
          public void rpc(EloquentRaftProto.RaftMessage request, StreamObserver<EloquentRaftProto.RaftMessage> responseObserver) {
            log.trace("Got an RPC request");
            try {
              UDPTransport.DEFAULT.get().doAction(async, "handle inbound RPC", () -> {
                for (RaftAlgorithm listener : boundAlgorithms) {
                  listener.receiveRPC(request, now()).whenComplete((EloquentRaftProto.RaftMessage response, Throwable exception) -> {
                    if (exception != null || response == null) {
                      responseObserver.onError(exception == null ? new RuntimeException() : exception);
                    } else {
                      responseObserver.onNext(response);
                      responseObserver.onCompleted();
                    }
                  });
                }
              });
            } catch (Throwable t) {
              responseObserver.onError(t);
            }
          }
        })
        .build()
        .start();
  }


  /** @see #NetRaftTransport(String, int, boolean) */
  public NetRaftTransport(String serverName) throws IOException {
    this(serverName, DEFAULT_RPC_LISTEN_PORT, false);
  }


  /** {@inheritDoc} */
  @Override
  public void scheduleAtFixedRate(SafeTimerTask timerTask, long period) {
    RaftLifecycle.global.timer.get().scheduleAtFixedRate(timerTask, 0L, period);
  }


  /** {@inheritDoc} */
  @Override
  public void schedule(SafeTimerTask timerTask, long delay) {
    RaftLifecycle.global.timer.get().schedule(timerTask, delay);
  }


  /** {@inheritDoc} */
  @Override
  public synchronized void bind(RaftAlgorithm listener) {
    if (!boundAlgorithms.contains(listener)) {
      // Bind ourselves to UDP
      UDPTransport.DEFAULT.get().bind(UDPBroadcastProtos.MessageType.RAFT, data -> {
        log.trace("Received a UDP message");
        try {
          EloquentRaftProto.RaftMessage message = EloquentRaftProto.RaftMessage.parseFrom(data);
          if (!message.getSender().equals(this.serverName)) {  // don't send ourselves messages
            assert ConcurrencyUtils.ensureNoLocksHeld();
            listener.receiveMessage(message, reply -> sendTransport(listener.serverName(), message.getSender(), reply), now());
          }
        } catch (InvalidProtocolBufferException e) {
          log.warn("Not a Raft message: ", e);
        }
      });

      // Register that we're bound (this binds to gRPC)
      boundAlgorithms.add(listener);
    }
  }


  /** {@inheritDoc} */
  @Override
  public Collection<RaftAlgorithm> boundAlgorithms() {
    return this.boundAlgorithms;
  }


  /** {@inheritDoc} */
  @Override
  public void rpcTransport(String sender, String destination, EloquentRaftProto.RaftMessage message,
                           Consumer<EloquentRaftProto.RaftMessage> onResponseReceived, Runnable onTimeout,
                           long timeout) {
    assert ConcurrencyUtils.ensureNoLocksHeld();

    // Get the destination IP address
    int underscoreIndex;
    final String destinationIp;
    if ((underscoreIndex = destination.indexOf("_")) > 0) {
      destinationIp = destination.substring(0, underscoreIndex);
    } else {
      destinationIp = destination;
    }

    ManagedChannel channel;
    synchronized (this) {
      // Clear old entries
      for (Map.Entry<String, ManagedChannel> entry : new HashSet<>(this.channel.entrySet())) {
        if (entry.getValue().isTerminated() || entry.getValue().isShutdown()) {
          entry.getValue().shutdown();
          this.channel.remove(entry.getKey());
        }
      }
      // Get our channel
      channel = this.channel.computeIfAbsent(destination, name -> ManagedChannelBuilder.forAddress(destinationIp, rpcListenPort)
          .usePlaintext()
          .build());
    }
    // Call our stub
    AtomicBoolean awaitingResponse = new AtomicBoolean(true);
    log.trace("Sending RPC request to {} @ ip {}", destination, destinationIp);
    RaftGrpc.newStub(channel)
        .withDeadlineAfter(timeout, TimeUnit.MILLISECONDS)
        .rpc(message, new StreamObserver<EloquentRaftProto.RaftMessage>() {
          @Override
          public void onNext(EloquentRaftProto.RaftMessage value) {
            log.trace("Got an RPC response from {}", destinationIp);
            if (awaitingResponse.getAndSet(false)) {
              assert ConcurrencyUtils.ensureNoLocksHeld();
              onResponseReceived.accept(value);
            }
          }
          @Override
          public void onError(Throwable t) {
            if (awaitingResponse.getAndSet(false)) {
              assert ConcurrencyUtils.ensureNoLocksHeld();
              onTimeout.run();
            }
          }
          @Override
          public void onCompleted() {
            if (awaitingResponse.getAndSet(false)) {
              assert ConcurrencyUtils.ensureNoLocksHeld();
              onTimeout.run();
            }
          }
        });
  }


  /** {@inheritDoc} */
  @Override
  public void sendTransport(String sender, String destination, EloquentRaftProto.RaftMessage message) {
    log.trace("Sending a UDP message to {}", destination);
    assert ConcurrencyUtils.ensureNoLocksHeld();

    // Get the destination IP address
    int underscoreIndex;
    final String destinationIp;
    if ((underscoreIndex = destination.indexOf("_")) > 0) {
      destinationIp = destination.substring(0, underscoreIndex);
    } else {
      destinationIp = destination;
    }

    // SEnd the message
    UDPTransport.DEFAULT.get().sendTransport(destinationIp, UDPBroadcastProtos.MessageType.RAFT, message.toByteArray());
  }


  /** {@inheritDoc} */
  @Override
  public void broadcastTransport(String sender, EloquentRaftProto.RaftMessage message) {
    log.trace("Broadcasting a UDP message");
    assert ConcurrencyUtils.ensureNoLocksHeld();
    UDPTransport.DEFAULT.get().broadcastTransport(UDPBroadcastProtos.MessageType.RAFT, message.toByteArray());
  }


  /** {@inheritDoc} */
  @Override
  public Span expectedNetworkDelay() {
    return new Span(10, 100);
  }


  /** {@inheritDoc} */
  @Override
  public String toString() {
    return "Raft:" + UDPTransport.DEFAULT.get().toString();
  }

}
