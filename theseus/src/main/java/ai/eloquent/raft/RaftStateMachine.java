package ai.eloquent.raft;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;

/**
 * This is the interface that any state machine needs to implement in order to be used with Theseus.
 */
public abstract class RaftStateMachine {
  /**
   * An SLF4J Logger for this class.
   */
  private static final Logger log = LoggerFactory.getLogger(KeyValueStateMachine.class);

  /**
   * The set of nodes that cannot be re-added to the cluster, even if we fall below the
   * minimum server size.
   * This is to prevent the case where we start shutting down boxes, and as one shuts down we
   * add it back to the quorum, which breaks Raft's consensus.
   */
  private String[] hospice = new String[0];


  /**
   * Get the current hospice. This is a copy of the hospice,
   * which means it is safe to mutate.
   */
  public final synchronized Set<String> getHospice() {
    return new HashSet<>(Arrays.asList(hospice));
  }


  /**
   * This serializes the state machine's current state into a proto that can be read from overwriteWithSerialized().
   *
   * @return a proto of this state machine
   */
  protected abstract ByteString serializeImpl();


  /**
   * This serializes the state machine's current state into a proto that can be read from overwriteWithSerialized().
   *
   * @return a proto of this state machine
   */
  public final synchronized byte[] serialize() {
    ByteString payload = serializeImpl();
    return EloquentRaftProto.StateMachine.newBuilder()
        .setPayload(payload)
        .addAllHospice(Arrays.asList(this.hospice))
        .build().toByteArray();
  }


  /**
   * This overwrites the current state of the state machine with a serialized proto. All the current state of the state
   * machine is overwritten, and the new state is substituted in its place.
   *
   * @param serialized the state machine to overwrite this one with, in serialized form.
   *                   This is the payload of the state machine -- that is, already of the correct
   *                   type. See {@link EloquentRaftProto.StateMachine#getPayload()}
   * @param now the current time, in epoch millis.
   * @param pool an executor pool to run lock future commits on.
   */
  protected abstract void overwriteWithSerializedImpl(byte[] serialized, long now, ExecutorService pool);


  /**
   * This overwrites the current state of the state machine with a serialized proto. All the current state of the state
   * machine is overwritten, and the new state is substituted in its place.
   *
   * @param serialized the state machine to overwrite this one with, in serialized form
   * @param now the current time, in epoch millis.
   * @param pool an executor pool to run lock future commits on.
   */
  public final synchronized void overwriteWithSerialized(byte[] serialized, long now, ExecutorService pool) {
    try {
      EloquentRaftProto.StateMachine proto = EloquentRaftProto.StateMachine.parseFrom(serialized);
      this.hospice = proto.getHospiceList().toArray(new String[0]);
      this.overwriteWithSerializedImpl(proto.getPayload().toByteArray(), now, pool);
    } catch (InvalidProtocolBufferException e) {
      log.error("Could not install snapshot!", e);
    }
  }


  /**
   * The custom implementation of applying a transition
   */
  protected abstract void applyTransition(byte[] transition, long now, ExecutorService pool);


  /**
   * This is responsible for applying a transition to the state machine. The transition is assumed to be coming in a
   * proto, and so is serialized as a byte array.
   *
   * @param transition The transition to apply, in serialized form
   * @param newHospiceMember A new hospice member to add to the blacklist.
   * @param now The current time, for mocking time in tests.
   * @param pool A pool that can be used for running the update listeners.
   */
  final void applyTransition(Optional<byte[]> transition, Optional<String> newHospiceMember, long now, ExecutorService pool) {
    if (newHospiceMember.isPresent()) {
      assert !transition.isPresent() : "Got both a custom transition and hospice member in transition";
      if (Arrays.stream(this.hospice).noneMatch(x -> x.equals(newHospiceMember.get()))) {  // don't add a duplicate
        String[] newHospice;
        if (this.hospice.length >= 100) {
          newHospice = new String[this.hospice.length];
          System.arraycopy(this.hospice, 1, newHospice, 0, this.hospice.length - 1);
        } else {
          newHospice = new String[this.hospice.length + 1];
          System.arraycopy(this.hospice, 0, newHospice, 0, this.hospice.length);
        }
        newHospice[newHospice.length - 1] = newHospiceMember.get();
        this.hospice = newHospice;
      }
    } else if (transition.isPresent()) {
      this.applyTransition(transition.get(), now, pool);
    } else {
      log.error("Got neither a hospice member or a custom transition in state machine transition");
    }
  }


  /**
   * This is used for debugging log entries. Mostly ignored, but very useful for debugging fuzz tests that are failing.
   *
   * @param transition the transition to debug
   * @return a rendered string version of the transition
   */
  public String debugTransition(byte[] transition) {
    return "<no debugging information>";
  }


  /**
   * The set of nodes that own anything in the state machine.
   * This is an optional override, only for state machines that have a notion
   * of ownership.
   */
  public Set<String> owners() {
    return Collections.emptySet();
  }
}
