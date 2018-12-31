package ai.eloquent.raft;

import ai.eloquent.util.TimerUtils;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * A set of benchmarks for the {@link KeyValueStateMachine}.
 *
 * @author <a href="mailto:gabor@eloquent.ai">Gabor Angeli</a>
 */

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgs = {"-Xms2G", "-Xmx2G"})
@Warmup(iterations = 2)
@Measurement(iterations = 5)
public class KeyValueStateMachineBenchmarks {
  /**
   * An SLF4J Logger for this class.
   */
  private static final Logger log = LoggerFactory.getLogger(KeyValueStateMachineBenchmarks.class);


  /**
   * A standard, small state machine. See {@link #setup()}.
   */
  private KeyValueStateMachine defaultStatemachine = new KeyValueStateMachine("small");

  /**
   * A state machine with 1M small (4 byte) values. See {@link #setup()}.
   */
  private KeyValueStateMachine manySmallValues = new KeyValueStateMachine("many_small");

  /**
   * A state machine with relatively few (1k) large (32k) values. See {@link #setup()}.
   */
  private KeyValueStateMachine fewLargeValues = new KeyValueStateMachine("few_small");


  /**
   * A short helper to set a key/value pair in a state machine.
   *
   * @param sm The state machine we're modifying.
   * @param key The key we're setting.
   * @param value The value we're setting fo the key.
   */
  private void set(KeyValueStateMachine sm, String key, String value) {
    sm.applyTransition(KeyValueStateMachineProto.Transition.newBuilder()
        .setType(KeyValueStateMachineProto.TransitionType.SET_VALUE)
        .setSetValue(KeyValueStateMachineProto.SetValue.newBuilder()
            .setKey(key)
            .setValue(ByteString.copyFromUtf8(value))
        ).build().toByteArray(), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());

  }


  /**
   * Set up our state machines.
   */
  @Setup
  public void setup() {
    for (int i = 0; i < 100; ++i) {
      set(defaultStatemachine, "key_" + i, UUID.randomUUID().toString());
    }
    for (int i = 0; i < 1000000; ++i) {
      set(manySmallValues, "key_" + i, Integer.toString(i));
    }
    for (int i = 0; i < 1000; ++i) {
      set(fewLargeValues, "key_" + i, new String(new char[0x1<<15]).replace('\0', ' '));
    }
  }


  /**
   * Benchmark serializing our default small state machine
   */
  @Benchmark
  public void serializeDefault(Blackhole bh) {
    bh.consume(defaultStatemachine.serialize());
  }


  /**
   * Benchmark serializing our large state machine with many small values.
   */
  @Benchmark
  public void serializeManySmallValues(Blackhole bh) {
    bh.consume(manySmallValues.serialize());
  }


  /**
   * Benchmark serializing our large state machine with few large values
   */
  @Benchmark
  public void serializeFewLargeValues(Blackhole bh) {
    bh.consume(fewLargeValues.serialize());
  }

}
