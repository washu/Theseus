package ai.eloquent.raft;


import com.google.protobuf.ByteString;

import java.util.concurrent.ExecutorService;

/**
 * This is a state machine that consists of a single byte value, which can be overwritten by a transition.
 */
public class SingleByteStateMachine extends RaftStateMachine {

  /** The value of the state machine */
  public byte value;

  public SingleByteStateMachine(int value) {
    this.value = (byte) value;
  }

  public SingleByteStateMachine() {
    this(-1);  // value starts at -1
  }

  /** {@inheritDoc} */
  @Override
  public ByteString serializeImpl() {
    return ByteString.copyFrom(new byte[]{value});
  }

  /** {@inheritDoc} */
  public synchronized void overwriteWithSerializedImpl(byte[] serialized, long now, ExecutorService pool) {
    value = serialized[0];
  }

  /** {@inheritDoc} */
  public synchronized void applyTransition(byte[] transition, long now, ExecutorService pool) {
    value = transition[0];
  }

  /** {@inheritDoc} */
  @Override
  public String debugTransition(byte[] transition) {
    return "set "+transition[0];
  }
}
