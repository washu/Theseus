package ai.eloquent.raft;

import ai.eloquent.util.TimerUtils;
import com.google.protobuf.ByteString;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static ai.eloquent.raft.EloquentRaftNodeTest.mkNode;
import static org.junit.Assert.*;

@SuppressWarnings("ALL")
public class EloquentRaftBenchmark {
  /**
   * An SLF4J Logger for this class.
   */
  private static final Logger log = LoggerFactory.getLogger(EloquentRaftBenchmark.class);

  @Ignore
  @Test
  public void burnInJIT() {
    InstantTransport.burnInJIT();
  }

  /**
   * This is here to benchmark the speed at which it is possible to run a transition in Raft.
   */
  @Ignore
  @Test
  public void testTransitions() throws Exception {
    InstantTransport transport = new InstantTransport();
    EloquentRaftNode L = mkNode("L", transport);
    EloquentRaftNode A = mkNode("A", transport);
    EloquentRaftNode B = mkNode("B", transport);
    L.start();
    A.start();
    B.start();
    L.bootstrap(false);
    L.algorithm.triggerElection(0);
    L.algorithm.heartbeat(100);

    long startTime = System.currentTimeMillis();
    long numIterations = 100000L;
    for (int i = 0; i < numIterations; i++) {
      L.algorithm.receiveApplyTransitionRPC(EloquentRaftProto.ApplyTransitionRequest.newBuilder()
          .setTerm(0)
          .setTransition(ByteString.copyFrom(KeyValueStateMachine.createSetValueTransition("key_"+(i % 50), new byte[]{10})))
          .build(), i * 1000);
    }

    System.out.println("************* DONE "+ TimerUtils.formatTimeSince(startTime)+" ("+((double)(System.currentTimeMillis() - startTime) / numIterations)+"ms / transition) **************");

    // Do some other meaningful work - evict everything from the CPU cache

    double[] array = new double[8 * 1000000];
    for (int i = 0; i < 200; i++) {
      for (int j = 0; j < array.length; j++) {
        array[j] = i;
      }
    }

    // Try again

    startTime = System.currentTimeMillis();
    numIterations = 100L;
    for (int i = 0; i < numIterations; i++) {
      L.algorithm.receiveApplyTransitionRPC(EloquentRaftProto.ApplyTransitionRequest.newBuilder()
          .setTerm(0)
          .setTransition(ByteString.copyFrom(KeyValueStateMachine.createSetValueTransition("key_"+(i % 50), new byte[]{10})))
          .build(), i * 1000);
    }

    System.out.println("************* DONE "+ TimerUtils.formatTimeSince(startTime)+" ("+((double)(System.currentTimeMillis() - startTime) / numIterations)+"ms / transition) **************");
  }


  /**
   * This is here to benchmark the speed at which it is possible to run a transition in Raft.
   */
  @Ignore
  @Test
  public void testBooting() throws Exception {
    long startTime = System.currentTimeMillis();
    for (int i = 0; i < 10; i++) {
      InstantTransport transport = new InstantTransport();
      EloquentRaftNode L = mkNode("L", transport);
      EloquentRaftNode A = mkNode("A", transport);
      EloquentRaftNode B = mkNode("B", transport);
      L.start();
      A.start();
      B.start();
      L.bootstrap(false);
      L.algorithm.triggerElection(0);
      L.algorithm.heartbeat(100);

      assertTrue("L should have become the leader", L.algorithm.state().isLeader());
      assertEquals("L should know about A and B", new HashSet<>(Arrays.asList("A", "B")), L.algorithm.state().lastMessageTimestamp.get().keySet());
    }

    System.out.println("************* DONE "+ TimerUtils.formatTimeSince(startTime)+" **************");
  }
}
