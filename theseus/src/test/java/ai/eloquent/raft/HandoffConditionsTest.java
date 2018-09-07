package ai.eloquent.raft;

import ai.eloquent.test.SlowTests;
import ai.eloquent.util.Pair;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.Assert.*;


/**
 * Assuming Raft is implemented perfectly, check that the preconditions of the handoff condition are actually
 * sufficient to prevent loss of state.
 *
 * <p><b>
 *   The important method here is: {@link RaftCluster#validate(Action, String)}.
 *   This encodes the preconditions we want to ensure for Raft handoff.
 * </b></p>

 * <p>
 *   The conditions we want to check are in {@link #assertConsistent(RaftCluster, AtomicInteger, Stack)}.
 * </p>
 *
 * @author <a href="mailto:gabor@eloquent.ai">Gabor Angeli</a>
 */
public class HandoffConditionsTest {
  /**
   * An SLF4J Logger for this class.
   */
  private static final Logger log = LoggerFactory.getLogger(HandoffConditionsTest.class);

  /**
   * An action that can be performed by a node in the cluster.
   */
  enum Action {
    GO_OFFLINE,
    ELECT_NEW_LEADER,
    LEAVE_CLUSTER,
    JOIN_CLUSTER,
    ;


    /**
     * The actions that we should try for every node (vs just once)
     */
    public static Action[] forNodes = Arrays.stream(Action.values())
        .filter(x -> x != Action.JOIN_CLUSTER && x != Action.ELECT_NEW_LEADER)
        .toArray(Action[]::new);
  }


  /**
   * A minimal raft state, keeping track of leadership and quorum.
   */
  public static class MinimalRaftState {
    public boolean isLeader;
    public Set<String> quorum;

    public MinimalRaftState(boolean isLeader, Set<String> quorum) {
      this.isLeader = isLeader;
      this.quorum = new HashSet<>(quorum);
    }

    public MinimalRaftState() {
      this.isLeader = false;
      this.quorum = new HashSet<>();
    }
  }


  /**
   * A virtual representation of a minimal Raft cluster
   */
  public static class RaftCluster {
    public final Map<String, MinimalRaftState> raftStates;
    public final LinkedHashSet<String> online;
    public final List<Integer> electionResults;

    private final int targetSize;
    private int electionI = 0;


    public RaftCluster(Map<String, MinimalRaftState> raftStates, Set<String> online, List<Integer> electionResults) {
      this.raftStates = new HashMap<>(raftStates);
      this.online = new LinkedHashSet<>(online);
      this.electionResults = Collections.unmodifiableList(electionResults);
      targetSize = raftStates.keySet().size();
      assertCanCommit();
    }


    private RaftCluster(RaftCluster original) {
      this.raftStates = new HashMap<>();
      for (Map.Entry<String, MinimalRaftState> entry : original.raftStates.entrySet()) {
        raftStates.put(entry.getKey(), new MinimalRaftState(entry.getValue().isLeader, entry.getValue().quorum));
      }
      this.online = new LinkedHashSet<>(original.online);
      this.electionResults = original.electionResults;
      this.targetSize = original.targetSize;
      this.electionI = original.electionI;
    }


    /**
     * The validation code for whether we can perform an action.
     * This should be as simple as possible.
     *
     * @param action The action we want to perform
     * @param node The node we want to perform the action on
     */
    public boolean validate(Action action, String node) {
      switch (action) {
        case GO_OFFLINE:
          if (raftStates.get(node).quorum.contains(node)) {
            // A node can't power off unless it is not in the quorum
            return false;
          }
          if (raftStates.get(node).isLeader) {
            // A node can't power off if it's the leader
            return false;
          }
          break;
        case LEAVE_CLUSTER:
          if (raftStates.get(node).quorum.equals(Collections.singleton(node))) {
            // A node can't be the last node to leave the cluster
            return false;
          }
          break;
      }
      return true;
    }


    /**
     * This is just an efficiency tweak to prevent, e.g., duplicate leave actions
     */
    public boolean structurallyValid(Action action, String node) {
      switch (action) {
        case GO_OFFLINE:
          return this.online.contains(node);
        case ELECT_NEW_LEADER:
          return true;
        case LEAVE_CLUSTER:
          return this.raftStates.get(node).quorum.contains(node);
        case JOIN_CLUSTER:
          return !this.online.contains(node);
        default:
          return true;
      }
    }


    /**
     * Assert that the cluster is in a state where it *should* be able to
     * commit an action.
     */
    private void assertCanCommit() {
      // 1. Check leadership
      List<String> leader = raftStates.entrySet().stream().filter(x -> x.getValue().isLeader).map(Map.Entry::getKey).collect(Collectors.toList());
      if (leader.size() != 1) {
        assertEquals("There should be exactly 1 leader in the cluster", 1, leader.size());
      }
      Set<String> quorum = raftStates.get(leader.get(0)).quorum;

      // 2. Check majority
      int numAlive = 0;
      for (String member : quorum) {
        boolean ok = true;
        //noinspection ConstantConditions
        ok &= online.contains(member);
        ok &= raftStates.get(member).quorum.equals(quorum);
        if (ok) {
          numAlive += 1;
        }
      }
      if (numAlive <= quorum.size() / 2) {
        assertTrue("The majority of the quorum cannot respond",numAlive > quorum.size() / 2 );
      }

      // 3. Check quorum size
      if (quorum.size() > targetSize) {
        assertTrue("The quorum should not have grown past the target size", quorum.size() <= targetSize);
      }
    }


    /**
     * @see #perform(Action, String)
     */
    public RaftCluster performAndClone(Action action, String node) {
      RaftCluster copy = new RaftCluster(this);
      copy.perform(action, node);
      return copy;
    }


    /**
     * Perform an action on the cluster.
     *
     * @param action The action we're performing.
     * @param node The node that's performing the action.
     */
    public void perform(Action action, String node) {
      assertTrue("Actions other than JOIN can only be performed on online nodes: " + action + " on " + node,
          online.contains(node) || action == Action.JOIN_CLUSTER || (action == Action.ELECT_NEW_LEADER && node == null));
      String leader = raftStates.entrySet().stream().filter(x -> x.getValue().isLeader).map(Map.Entry::getKey).findFirst().orElseThrow(() -> new AssertionError("There should always be a leader"));
      switch (action) {

        case GO_OFFLINE:
          online.remove(node);
          break;

        case ELECT_NEW_LEADER:
          List<String> candidates = new ArrayList<>(online).stream()
              .filter(x -> !x.equals(leader))  // should be online
              .filter(x -> raftStates.get(x).quorum.contains(x))  // should think they're not shadow nodes
              .collect(Collectors.toList());
          if (!candidates.isEmpty()) {
            String newLeader = candidates.get(electionResults.get(electionI % electionResults.size()) % candidates.size());
            raftStates.get(leader).isLeader = false;
            raftStates.get(newLeader).isLeader = true;
            electionI += 1;
            assertCanCommit();
          }
          break;

        case LEAVE_CLUSTER:
          assertCanCommit();
          for (MinimalRaftState state : raftStates.values()) {
            state.quorum.remove(node);
          }
          if (raftStates.get(node).isLeader) {  // election happens after removal
            perform(Action.ELECT_NEW_LEADER, null);
          }
          assertCanCommit();
          break;

        case JOIN_CLUSTER:
          assertFalse("Cannot have a node join that's already in the cluster", online.contains(node));
          // (check that we can commit)
          assertCanCommit();
          // (add the node)
          this.online.add(node);
          // (get the leader's quorum)
          //noinspection ConstantConditions
          Set<String> quorum = new HashSet<>(raftStates.get(leader).quorum);
          // (add ourselves to the broadcast)
          this.raftStates.put(node, new MinimalRaftState());
          if (quorum.size() < targetSize) {
            // (update the quorum)
            quorum.add(node);
            // (broadcast the quorum)
            for (MinimalRaftState state : raftStates.values()) {
              state.quorum.clear();
              state.quorum.addAll(quorum);
            }
          }
          assertCanCommit();
      }
    }


    /** A helper for {@link #allNondeterministicOptions(Map, int)}  */
    private static List<List<Integer>> recurse(int numOptions, int size) {
      List<List<Integer>> rtn = new ArrayList<>();
      if (size == 0) {
        return Collections.singletonList(Collections.singletonList(0));
      } else if (size == 1) {
        for (int option = 0; option < numOptions; ++option) {
          rtn.add(Collections.singletonList(option));
        }
      } else {
        List<List<Integer>> recurse = recurse(numOptions, size - 1);
        for (List<Integer> r : recurse) {
          for (int option = 0; option < numOptions; ++option) {
            List<Integer> l = new ArrayList<>(r);
            l.add(option);
            rtn.add(l);
          }
        }
      }
      return rtn;
    }


    /**
     * Get a bunch of identical RaftCluster instances, but with all the different choices of election resolution.
     */
    public static List<RaftCluster> allNondeterministicOptions(Map<String, MinimalRaftState> raftStates, Set<String> online, int numElections) {
      List<RaftCluster> clusters = new ArrayList<>();
      for (List<Integer> electionChoices : recurse(raftStates.keySet().size() * 2, numElections)) {
        clusters.add(new RaftCluster(raftStates, online, electionChoices));
      }
      return clusters;
    }


    /**
     * @see #allNondeterministicOptions(Map, Set, int)
     */
    public static List<RaftCluster> allNondeterministicOptions(Map<String, MinimalRaftState> raftStates, int numElections) {
      return allNondeterministicOptions(raftStates, raftStates.keySet(), numElections);
    }
  }


  /**
   * Create a simple healthy initial cluster
   *
   * @param numNodes The number of nodes to have
   * @param numElections The number of elections to prepopulate.
   *
   * @return The cluster options, accounting for all nondeterministic election choices.
   */
  public List<RaftCluster> init(int numNodes, int numElections) {
    // 1. Create the states
    Map<String, MinimalRaftState> states = new HashMap<>();
    for (int i = 0; i < numNodes; ++i) {
      String name = Character.toString((char) ('A' + ((char) i)));
      states.put(name, new MinimalRaftState());
    }
    states.values().forEach(x -> x.quorum.addAll(states.keySet()));
    states.get("A").isLeader = true;

    // 2. Create the cluster
    return RaftCluster.allNondeterministicOptions(states, numElections);
  }


  /**
   * Create an initial cluster, with deterministic elections.
   *
   * @param numNodes The number of nodes to add to the cluster.
   *
   * @return The cluster
   */
  public RaftCluster init(int numNodes) {
    return init(numNodes, 0).get(0);
  }


  /**
   * Check that Raft is in a consistent state.
   *
   * @param cluster The cluster we're checking.
   * @param passes The number of tests we've passed.
   * @param actionStack The actions we have performed so far.
   */
  @SuppressWarnings("ConstantConditions")
  private void assertConsistent(RaftCluster cluster, AtomicInteger passes, Stack<Pair<Action, String>> actionStack) {
    try {
      // 1. Check that we can commit
      cluster.assertCanCommit();

      // 2. Check leader is in their own quorum
      String leader = cluster.raftStates.entrySet().stream().filter(x -> x.getValue().isLeader).map(Map.Entry::getKey).findFirst().get();
      boolean leaderInQuorum = cluster.raftStates.get(leader).quorum.contains(leader);
      if (!leaderInQuorum) {
        assertTrue("The leader should be in their own quorum: leader=" + leader + " quorum=" + cluster.raftStates.get(leader).quorum, leaderInQuorum);
      }


    } catch (AssertionError e) {
      log.warn("Passed {} runs before error", passes.get());
      log.warn("Actions:");
      for (Pair<Action, String> action : actionStack) {
        log.warn("  - {}  on  {}", action.first, action.second);
      }
      throw e;
    }
  }


  /**
   * Do valid stuff for |length| actions.
   *
   * @param cluster The initial cluster
   * @param length The number of actions to perform
   */
  private void fuzzAllNondeterministicOptions(RaftCluster cluster, int length, AtomicInteger passes, Stack<Pair<Action, String>> actionStack) {
    if (length <= 0) {
      assertConsistent(cluster, passes, actionStack);
      if (passes.incrementAndGet() % 100000 == 0) {
        log.info("Passed {} runs so far...", passes.get());
      }
    } else {
      // (do an action on the current cluster)
      for (Action action : Action.forNodes) {
        for (String node : cluster.online) {
          if (cluster.validate(action, node) && cluster.structurallyValid(action, node)) {  // VALIDATION GOES HERE
            RaftCluster withPerformed = cluster.performAndClone(action, node);
            actionStack.push(Pair.makePair(action, node));
            fuzzAllNondeterministicOptions(withPerformed, length - 1, passes, actionStack);
            actionStack.pop();
          }
        }
      }
      // (elect a new leader
      if (cluster.validate(Action.ELECT_NEW_LEADER, null)) {  // VALIDATION GOES HERE
        RaftCluster withPerformed = cluster.performAndClone(Action.ELECT_NEW_LEADER, null);
        actionStack.push(Pair.makePair(Action.ELECT_NEW_LEADER, null));
        fuzzAllNondeterministicOptions(withPerformed, length - 1, passes, actionStack);
        actionStack.pop();
      }
      // (add a node to the cluster)
      String nodeName = "node_" + cluster.raftStates.size();
      RaftCluster withJoin = cluster.performAndClone(Action.JOIN_CLUSTER, nodeName);
      actionStack.push(Pair.makePair(Action.JOIN_CLUSTER, nodeName));
      fuzzAllNondeterministicOptions(withJoin, length - 1, passes, actionStack);
      actionStack.pop();
    }
  }



  // --------------------------------------------------------------------------
  // The Actual Tests
  // --------------------------------------------------------------------------

  @Test
  public void testFuzzCluster3Length1() {
    AtomicInteger passes = new AtomicInteger();
    fuzzAllNondeterministicOptions(init(3), 1, passes, new Stack<>());
    log.info("Passed " + passes.get() + " runs");
  }

  @Test
  public void testFuzzCluster3Length2() {
    AtomicInteger passes = new AtomicInteger();
    fuzzAllNondeterministicOptions(init(3), 2, passes, new Stack<>());
    log.info("Passed " + passes.get() + " runs");
  }

  @Test
  public void testFuzzCluster3Length3() {
    AtomicInteger passes = new AtomicInteger();
    fuzzAllNondeterministicOptions(init(3), 3, passes, new Stack<>());
    log.info("Passed " + passes.get() + " runs");
  }

  @Test
  public void testFuzzCluster3Length4() {
    AtomicInteger passes = new AtomicInteger();
    fuzzAllNondeterministicOptions(init(3), 4, passes, new Stack<>());
    log.info("Passed " + passes.get() + " runs");
  }

  @Test
  public void testFuzzCluster3Length5() {
    AtomicInteger passes = new AtomicInteger();
    fuzzAllNondeterministicOptions(init(3), 5, passes, new Stack<>());
    log.info("Passed " + passes.get() + " runs");
  }

  @Test
  public void testFuzzCluster4Length5() {
    AtomicInteger passes = new AtomicInteger();
    fuzzAllNondeterministicOptions(init(4), 5, passes, new Stack<>());
    log.info("Passed " + passes.get() + " runs");
  }

  @Test
  public void testFuzzCluster5Length5() {
    AtomicInteger passes = new AtomicInteger();
    fuzzAllNondeterministicOptions(init(5), 5, passes, new Stack<>());
    log.info("Passed " + passes.get() + " runs");
  }

  @Test
  public void testFuzzCluster3Length6() {
    AtomicInteger passes = new AtomicInteger();
    fuzzAllNondeterministicOptions(init(3), 6, passes, new Stack<>());
    log.info("Passed " + passes.get() + " runs");
  }


  @Category(SlowTests.class)
  @Ignore
  @Test
  public void testFuzzCluster3Length9() {
    AtomicInteger passes = new AtomicInteger();
    fuzzAllNondeterministicOptions(init(3), 9, passes, new Stack<>());
    log.info("Passed " + passes.get() + " runs");
  }


  @Ignore // this takes ~30 minutes
  @Test
  public void testFuzzCluster3Length12() {
    AtomicInteger passes = new AtomicInteger();
    fuzzAllNondeterministicOptions(init(3), 12, passes, new Stack<>());
    log.info("Passed " + passes.get() + " runs");
  }

}
