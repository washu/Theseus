package ai.eloquent.raft;

import com.google.common.util.concurrent.MoreExecutors;
import ai.eloquent.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;


/**
 * Test the {@link UnsatisfiableQuorumFailsafe}.
 */
public class UnsatisfiableQuorumFailsafeTest {

  /**
   * Create a simple algorithm state, where a leader (optionally deadlocked) is joined by two servers
   * which have responded at time 0, but are presumably not going to respond afterwards.
   * This sets us up for seeing them slowly get marked as dead, and have the leader reset its state
   * based off of the Kubernetes state.
   *
   * @param elect If true, elect the leader. Usually true.
   * @param haveFollowers If true, actually have followers that we want to kill.
   *                      This is just to test corner cases
   * @param leaderName The name of the leader
   *
   *
   * @return The Raft algorithm set up in the appropriate way as described in the description.
   */
  private RaftAlgorithm deadlockedAlgorithm(boolean elect, boolean haveFollowers, String leaderName) {
    SingleByteStateMachine stateMachine = new SingleByteStateMachine();
    RaftLog raftLog = new RaftLog(stateMachine,
        haveFollowers ? Arrays.asList(leaderName, "dead_server_1", "dead_server_2") : Collections.singletonList("deadlocked_leader"),
        MoreExecutors.newDirectExecutorService());
    RaftState state = new RaftState(leaderName, raftLog);
    EloquentRaftAlgorithmTest.AssertableTransport transport = new EloquentRaftAlgorithmTest.AssertableTransport();
    if (elect) {
      state.elect(0);
      if (haveFollowers) {
        state.observeLifeFrom("dead_server_1", 0);
        state.observeLifeFrom("dead_server_2", 0);
      }
    }
    RaftAlgorithm algorithm = new EloquentRaftAlgorithm(state, transport, Optional.empty());
    algorithm.heartbeat();
    return algorithm;
  }


  /**
   * Test the happy path where the failsafe triggers
   */
  @Test
  public void failsafeTriggers() {
    RaftAlgorithm algorithm = deadlockedAlgorithm(true, true, "deadlocked_leader");
    UnsatisfiableQuorumFailsafe deadlock = new UnsatisfiableQuorumFailsafe(() -> new String[] {"deadlocked_leader"}, Duration.ofSeconds(45));
    deadlock.heartbeat(algorithm, Duration.ofSeconds(46).toMillis());
    assertTrue("We should be the leader", algorithm.state().isLeader());
    assertEquals("The kubernetes failsafe should trigger", 1, algorithm.state().log.latestQuorumMembers.size());
    algorithm.stop(true);
  }


  /**
   * Test that the failsafe does not trigger if we're a candidate and not the leader.
   */
  @Test
  public void failsafeDoesntTriggersIfCandidate() {
    RaftAlgorithm algorithm = deadlockedAlgorithm(false, true, "deadlocked_leader");
    ((EloquentRaftAlgorithm) algorithm).convertToCandidate();
    UnsatisfiableQuorumFailsafe deadlock = new UnsatisfiableQuorumFailsafe(() -> new String[] {"deadlocked_leader"}, Duration.ofSeconds(45));
    deadlock.heartbeat(algorithm, Duration.ofSeconds(46).toMillis());
    assertTrue("We should be a candidate", algorithm.state().isCandidate());
    assertEquals("The kubernetes failsafe should not trigger", 3, algorithm.state().log.latestQuorumMembers.size());
    algorithm.stop(true);
  }


  /**
   * Test that followers never trigger the failsafe.
   *
   * @see #failsafeTriggers()
   */
  @Test
  public void failsafeDoesNotTriggerOnFollowers() {
    RaftAlgorithm algorithm = deadlockedAlgorithm(false, true, "deadlocked_leader");
    UnsatisfiableQuorumFailsafe deadlock = new UnsatisfiableQuorumFailsafe(() -> new String[] {"deadlocked_leader"}, Duration.ofSeconds(45));
    deadlock.heartbeat(algorithm, Duration.ofSeconds(46).toMillis());
    assertFalse("We should not be the leader", algorithm.state().isLeader());
    assertEquals("The kubernetes failsafe should not trigger if we're not the leader", 3, algorithm.state().log.latestQuorumMembers.size());
    algorithm.stop(true);
  }


  /**
   * Test that the failsafe should not trigger too soon.
   *
   * @see #failsafeTriggers()
   */
  @Test
  public void failsafeDoesNotTriggerTooSoon() {
    RaftAlgorithm algorithm = deadlockedAlgorithm(true, true, "deadlocked_leader");
    UnsatisfiableQuorumFailsafe deadlock = new UnsatisfiableQuorumFailsafe(() -> new String[] {"deadlocked_leader"}, Duration.ofSeconds(45));
    for (int seconds = 0; seconds < 44; ++seconds) {
      deadlock.heartbeat(algorithm, Duration.ofSeconds(seconds).toMillis());
      assertTrue("We should be the leader", algorithm.state().isLeader());
      assertEquals("The kubernetes failsafe should not trigger after " + seconds + " second", 3, algorithm.state().log.latestQuorumMembers.size());
    }
    algorithm.stop(true);
  }


  /**
   * Do not trigger the failsafe if we have no followers.
   */
  @Test
  public void failsafeDoesNotTriggerIfNoFollowers() {
    RaftAlgorithm algorithm = deadlockedAlgorithm(true, false, "deadlocked_leader");
    AtomicBoolean runGetMembers = new AtomicBoolean(false);
    UnsatisfiableQuorumFailsafe deadlock = new UnsatisfiableQuorumFailsafe(() -> {
      runGetMembers.set(true);
      return new String[] {"deadlocked_leader"};
    }, Duration.ofSeconds(45));
    deadlock.heartbeat(algorithm, Duration.ofSeconds(46).toMillis());
    assertTrue("We should be the leader", algorithm.state().isLeader());
    assertEquals("We should continue to have one quorum member", 1, algorithm.state().log.latestQuorumMembers.size());
    assertFalse("We should not have asked for the cluster members", runGetMembers.get());
    algorithm.stop(true);
  }


  /**
   * The failsafe should not trigger if Kubernetes detects *any* of the followers to be alive.
   */
  @Test
  public void failsafeDoesNotTriggerIfSomeNodeAlive() {
    RaftAlgorithm algorithm = deadlockedAlgorithm(true, true, "deadlocked_leader");
    UnsatisfiableQuorumFailsafe deadlock = new UnsatisfiableQuorumFailsafe(() -> new String[] {"deadlocked_leader", "dead_server_1"}, Duration.ofSeconds(45));
    deadlock.heartbeat(algorithm, Duration.ofSeconds(46).toMillis());
    assertTrue("We should be the leader", algorithm.state().isLeader());
    assertEquals("The kubernetes failsafe should not trigger if Kubernetes sees a 'dead' server", 3, algorithm.state().log.latestQuorumMembers.size());
    algorithm.stop(true);
  }


  /**
   * The failsafe should not trigger if the cluster membership getter exceptions
   */
  @Test
  public void failsafeDoesNotTriggerOnException() {
    RaftAlgorithm algorithm = deadlockedAlgorithm(true, true, "deadlocked_leader");
    UnsatisfiableQuorumFailsafe deadlock = new UnsatisfiableQuorumFailsafe(() -> {
      throw new IOException("Expected exception");
    }, Duration.ofSeconds(45));
    deadlock.heartbeat(algorithm, Duration.ofSeconds(46).toMillis());
    assertTrue("We should be the leader", algorithm.state().isLeader());
    assertEquals("The kubernetes failsafe should not trigger if we IO Exception", 3, algorithm.state().log.latestQuorumMembers.size());
    algorithm.stop(true);
  }


  /**
   * Test the happy path where the failsafe triggers
   */
  @Test
  public void failsafeTriggersWithOtherNodesInCluster() {
    RaftAlgorithm algorithm = deadlockedAlgorithm(true, true, "deadlocked_leader");
    UnsatisfiableQuorumFailsafe deadlock = new UnsatisfiableQuorumFailsafe(() -> new String[] {"deadlocked_leader", "new_node", "another_new_node"}, Duration.ofSeconds(45));
    deadlock.heartbeat(algorithm, Duration.ofSeconds(46).toMillis());
    assertTrue("We should be the leader", algorithm.state().isLeader());
    assertEquals("The kubernetes failsafe should trigger", 1, algorithm.state().log.latestQuorumMembers.size());
    algorithm.stop(true);
  }


  // ----------------
  // END TO END TESTS
  // ----------------


  /**
   * Test that we actually read end-to-end from the Kubernetes file
   */
  @Test
  public void endToEndTrigger() throws Exception {
    RaftAlgorithm algorithm = deadlockedAlgorithm(true, true, "10.0.0.1");
    UnsatisfiableQuorumFailsafe deadlock = new UnsatisfiableQuorumFailsafe(Duration.ofSeconds(45));
    File memberPath = File.createTempFile("raft_cluster", ".tab");
    memberPath.deleteOnExit();
    IOUtils.writeToFile("10.0.0.1\t1/1\trunning.", memberPath.getPath(), StandardCharsets.UTF_8);
    try {
      injectEnvironmentVariable("ELOQUENT_RAFT_MEMBERS", memberPath.getPath());
      deadlock.heartbeat(algorithm, Duration.ofSeconds(46).toMillis());
      assertTrue("We should be the leader", algorithm.state().isLeader());
      assertEquals("The kubernetes failsafe should trigger", 1, algorithm.state().log.latestQuorumMembers.size());
    } finally {
      algorithm.stop(true);
      injectEnvironmentVariable("ELOQUENT_RAFT_MEMBERS", "");
    }
  }


  /**
   * Test that we actually read end-to-end from the Kubernetes file
   */
  @Test
  public void endToEndNoPath() {
    RaftAlgorithm algorithm = deadlockedAlgorithm(true, true, "10.0.0.1");
    UnsatisfiableQuorumFailsafe deadlock = new UnsatisfiableQuorumFailsafe(Duration.ofSeconds(45));
    try {
      deadlock.heartbeat(algorithm, Duration.ofSeconds(46).toMillis());
      assertTrue("We should be the leader", algorithm.state().isLeader());
      assertEquals("The kubernetes failsafe should not trigger if we IO Exception (on not finding the member path)",
          3, algorithm.state().log.latestQuorumMembers.size());
    }
    finally {
      algorithm.stop(true);
    }
  }


  /**
   * Test that we actually read end-to-end from the Kubernetes file, even if another node (not in the quorum)
   * exists in the Kubernetes state.
   */
  @Test
  public void endToEndTriggerOtherNodesPresent() throws Exception {
    RaftAlgorithm algorithm = deadlockedAlgorithm(true, true, "10.0.0.1");
    UnsatisfiableQuorumFailsafe deadlock = new UnsatisfiableQuorumFailsafe(Duration.ofSeconds(45));
    File memberPath = File.createTempFile("raft_cluster", ".tab");
    memberPath.deleteOnExit();
    IOUtils.writeToFile("10.0.0.1\t1/1\trunning.\n10.0.0.2\t1/1\trunning.", memberPath.getPath(), StandardCharsets.UTF_8);
    try {
      injectEnvironmentVariable("ELOQUENT_RAFT_MEMBERS", memberPath.getPath());
      deadlock.heartbeat(algorithm, Duration.ofSeconds(46).toMillis());
      assertTrue("We should be the leader", algorithm.state().isLeader());
      assertEquals("The kubernetes failsafe should trigger even in the presence of other nodes in the cluster",
          1, algorithm.state().log.latestQuorumMembers.size());
    } finally {
      algorithm.stop(true);
      injectEnvironmentVariable("ELOQUENT_RAFT_MEMBERS", "");
    }
  }


  /**
   * Very dirty hack to help inject environment variables for testing
   * Use with care!
   *
   * @param key The key of the environment variable
   * @param value The value of of the environment variable
   */
  @SuppressWarnings({"unchecked", "SameParameterValue"})
  private static void injectEnvironmentVariable(String key, String value)
      throws Exception {
    Class<?> processEnvironment = Class.forName("java.lang.ProcessEnvironment");
    Field unmodifiableMapField = getAccessibleField(processEnvironment, "theUnmodifiableEnvironment");
    Object unmodifiableMap = unmodifiableMapField.get(null);
    injectIntoUnmodifiableMap(key, value, unmodifiableMap);
    Field mapField = getAccessibleField(processEnvironment, "theEnvironment");
    Map<String, String> map = (Map<String, String>) mapField.get(null);
    map.put(key, value);
  }


  private static Field getAccessibleField(Class<?> clazz, String fieldName)
      throws NoSuchFieldException {
    Field field = clazz.getDeclaredField(fieldName);
    field.setAccessible(true);
    return field;
  }


  @SuppressWarnings("unchecked")
  private static void injectIntoUnmodifiableMap(String key, String value, Object map)
      throws ReflectiveOperationException {
    Class unmodifiableMap = Class.forName("java.util.Collections$UnmodifiableMap");
    Field field = getAccessibleField(unmodifiableMap, "m");
    Object obj = field.get(map);
    ((Map<String, String>) obj).put(key, value);
  }
}