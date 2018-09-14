package ai.eloquent.data;

import ai.eloquent.raft.RaftLifecycle;

import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

/**
 * An interface for a transport layer used in the Eloquent codebase.
 *
 * @author <a href="mailto:gabor@eloquent.ai">Gabor Angeli</a>
 */
public interface Transport {


  /**
   * Bind a consumer to this transport, to listen on incoming messages.
   *
   * @param channel The type of message we're listing for.
   * @param listener The listener, taking as input a raw byte stream.
   */
  void bind(UDPBroadcastProtos.MessageType channel, Consumer<byte[]> listener);


  /**
   * Send a message on the transport.
   *
   * @param destination The location we're sending the message to. This is transport-dependent, but is often
   *                    the IP address of the destination.
   * @param messageType The type of message we're sending. This should match the channel in {@link #bind(UDPBroadcastProtos.MessageType, Consumer)}.
   * @param message The message, as a raw byte stream.
   *
   * @return True if the message was sent.
   */
  boolean sendTransport(String destination, UDPBroadcastProtos.MessageType messageType, byte[] message);


  /**
   * Broadcast a message to everyone listening on the transport.
   *
   * @param messageType The type of message we're sending. This should match the channel in {@link #bind(UDPBroadcastProtos.MessageType, Consumer)}.
   * @param message The message, as a raw byte stream.
   *
   * @return True if the message was sent to everyone.
   */
  boolean broadcastTransport(UDPBroadcastProtos.MessageType messageType, byte[] message);

  /**
   * A thread pool from which to run tasks.
   */
  ExecutorService POOL = RaftLifecycle.global.managedThreadPool("TransportWorker", true);  // we need this to be core for Raft


  /**
   * Do a given action.
   * This method is called on the main IO reader loop; if we want to offload it to a thread,
   * this is the place to do it.
   *
   * @param async If true, run the action asynchronously on {@link #POOL}.
   * @param description A human-readable description of the action
   * @param action The action to perform.
   */
  @SuppressWarnings("unused")
  default void doAction(boolean async, String description, Runnable action) {
    if (async && !POOL.isShutdown()) {
      POOL.submit(action);
    } else {
      action.run();
    }
  }
}
