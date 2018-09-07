package ai.eloquent.raft;

import com.google.common.util.concurrent.MoreExecutors;
import org.junit.Test;

import java.util.Collections;
import java.util.Optional;

import static org.junit.Assert.*;

/**
 * Test the core bits of {@link RaftStateMachine}.
 *
 * @author <a href="mailto:gabor@eloquent.ai">Gabor Angeli</a>
 */
public class RaftStateMachineTest {


  /**
   * Test that we can add to the hospice
   */
  @Test
  public void testAddHospice() {
    RaftStateMachine machine = new SingleByteStateMachine();
    machine.applyTransition(Optional.empty(), Optional.of("a"), 0, MoreExecutors.newDirectExecutorService());
    assertEquals(Collections.singleton("a"), machine.getHospice());
  }


  /**
   * Test that we don't add duplicates to the hospice
   */
  @Test
  public void testAddHospiceDuplicate() {
    RaftStateMachine machine = new SingleByteStateMachine();
    machine.applyTransition(Optional.empty(), Optional.of("a"), 0, MoreExecutors.newDirectExecutorService());
    machine.applyTransition(Optional.empty(), Optional.of("a"), 0, MoreExecutors.newDirectExecutorService());
    assertEquals(Collections.singleton("a"), machine.getHospice());
  }


  /**
   * Test that we cap the hospice's size
   */
  @Test
  public void testAddHospiceMaxLength() {
    RaftStateMachine machine = new SingleByteStateMachine();
    for (int i = 0; i < 100; ++i) {
      machine.applyTransition(Optional.empty(), Optional.of("node" + i), 0, MoreExecutors.newDirectExecutorService());
    }
    assertEquals(100, machine.getHospice().size());
    machine.applyTransition(Optional.empty(), Optional.of("new_node"), 0, MoreExecutors.newDirectExecutorService());
    assertEquals(100, machine.getHospice().size());
    assertTrue(machine.getHospice().contains("new_node"));
  }

}