package ai.eloquent.raft;

import ai.eloquent.util.Span;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.*;

import static org.junit.Assert.*;

/**
 * Test a Raft State
 *
 * @author <a href="mailto:gabor@eloquent.ai">Gabor Angeli</a>
 */
public class RaftStateTest {


  /** Assert that the given runnable should throw the given exception (or something compatible). */
  @SuppressWarnings("SameParameterValue")
  protected void assertException(Runnable r, Class<? extends Throwable> expectedException) {
    @Nullable
    Throwable exception = null;
    try {
      r.run();
    } catch (Throwable t) {
      exception = t;
    }
    assertNotNull("Expected exception of type " + expectedException.getSimpleName() + " but did not get one.",
        exception);
    assertTrue("Expected exception of type " + expectedException.getSimpleName() +
            " but got exception of type " + exception.getClass().getSimpleName() + ". These are incompatible.",
        expectedException.isAssignableFrom(exception.getClass()));
  }


  /**
   * Make sure we can create a {@link RaftState}.
   */
  @Test
  public void create() {
    new RaftState("name", new KeyValueStateMachine("name"), MoreExecutors.newDirectExecutorService());
    new RaftState("name", new RaftLog(new KeyValueStateMachine("name"), Collections.singletonList("name"), MoreExecutors.newDirectExecutorService()), 3);
  }

  /**
   * Test that if we create a cluster that's meant to be resizable, we start as a nonvoting member and don't
   * start an election immediately. This is important to ensure continuity of a single leader, though it does introduce
   * the bootstrapping problem -- the cluster has to be explicitly bootstrapped.
   */
  @Test
  public void createNewResizableNodeIsNonvoting() {
    RaftState dynamic = new RaftState("name", new KeyValueStateMachine("name"), 3, MoreExecutors.newDirectExecutorService());
    assertTrue("With a dynamic cluster size, we should not have added ourselves to the quorum", dynamic.log.getQuorumMembers().isEmpty());
    RaftState fixed = new RaftState("name", new KeyValueStateMachine("name"), Arrays.asList("name", "A", "B"), MoreExecutors.newDirectExecutorService());
    assertEquals("With a fixed cluster size, we should by default create a singleton cluster",
        new HashSet<>(Arrays.asList("name", "A", "B")), fixed.log.getQuorumMembers());

  }


  /**
   * Check some conditions that should be true on a newly created state
   */
  @Test
  public void newStateConditions() {
    RaftState state = new RaftState("name", new KeyValueStateMachine("name"), MoreExecutors.newDirectExecutorService());
    assertEquals("A new state should have a term of 0", 0, state.currentTerm);
    assertEquals("A new state should have a last entry index of 0", 0, state.log.getLastEntryIndex());
  }


  /**
   *
   */
  @Test
  public void testConstructWithExistingState() {
    RaftLog log = new RaftLog(new KeyValueStateMachine("name"), Collections.singleton("name"), MoreExecutors.newDirectExecutorService());
    log.logEntries.add(EloquentRaftProto.LogEntry.newBuilder().setIndex(1).setTerm(3).setTransition(ByteString.EMPTY).build());
    RaftState state = new RaftState("name", log, 3);
    assertEquals("The state's log should have the correct term", 3, state.log.getLastEntryTerm());
    assertEquals("The state should inherit the log's term", 3, state.currentTerm);
    assertEquals("The state should inherit the log's index", 1, state.log.getLastEntryIndex());
  }


  /**
   * Test {@link RaftState#setCurrentTerm(long)}
   */
  @Test
  public void setCurrentTerm() {
    RaftState state = new RaftState("name", new KeyValueStateMachine("name"), MoreExecutors.newDirectExecutorService());
    state.setCurrentTerm(10);
    assertEquals(10, state.currentTerm);
  }


  /**
   * Test that {@link RaftState#setCurrentTerm(long)} clears received votes
   */
  @Test
  public void setCurrentTermClearsVotesReceived() {
    RaftState state = new RaftState("name", new KeyValueStateMachine("name"), MoreExecutors.newDirectExecutorService());
    state.setCurrentTerm(10);
    state.receiveVoteFrom("name");
    assertEquals("Votes should go through (another test should be failing if this fails)", Collections.singleton("name"), state.votesReceived);
    state.setCurrentTerm(10);
    assertEquals("Should keep votes if term didn't increase", Collections.singleton("name"), state.votesReceived);
  }


  /**
   * Test {@link RaftState#voteFor(String)}
   */
  @Test
  public void voteFor() {
    RaftState state = new RaftState("name", new KeyValueStateMachine("name"), MoreExecutors.newDirectExecutorService());
    state.voteFor("candidate");
    assertEquals("candidate", state.votedFor.orElse(null));
  }


  /**
   * Test that {@link RaftState#voteFor(String)} can't vote for multiple people on the same term
   */
  @Test
  public void voteForCantDoubleVote() {
    RaftState state = new RaftState("name", new KeyValueStateMachine("name"), MoreExecutors.newDirectExecutorService());
    state.voteFor("candidate");
    assertEquals("candidate", state.votedFor.orElse(null));
    state.electionTimeoutCheckpoint = 0L;
    state.voteFor("candidate");
    assertException(() -> state.voteFor("name"), AssertionError.class);
  }


  /**
   * Test that {@link RaftState#voteFor(String)} can't vote for multiple people on the same term
   */
  @Test
  public void voteForResetsElectionTimer() {
    RaftState state = new RaftState("name", new KeyValueStateMachine("name"), MoreExecutors.newDirectExecutorService());
    state.electionTimeoutCheckpoint = 1000L;
    state.voteFor("candidate");
  }


  /**
   * Test {@link RaftState#isLeader()}
   */
  @Test
  public void isLeader() {
    RaftState state = new RaftState("name", new KeyValueStateMachine("name"), MoreExecutors.newDirectExecutorService());
    assertFalse("A state is not leader by default", state.isLeader());
    state.leadership = RaftState.LeadershipStatus.CANDIDATE;
    assertFalse("A candidate is not a leader", state.isLeader());
    state.leadership = RaftState.LeadershipStatus.LEADER;
    assertTrue("A leader should be a leader", state.isLeader());
  }


  /**
   * Test {@link RaftState#isCandidate()}
   */
  @Test
  public void isCandidate() {
    RaftState state = new RaftState("name", new KeyValueStateMachine("name"), MoreExecutors.newDirectExecutorService());
    assertFalse("A state is not a candidate by default", state.isCandidate());
    state.leadership = RaftState.LeadershipStatus.CANDIDATE;
    assertTrue("A candidate is a candidate", state.isCandidate());
    state.leadership = RaftState.LeadershipStatus.LEADER;
    assertFalse("A leader is not a candidate", state.isCandidate());
  }


  /**
   * Test {@link RaftState#elect(long)}
   */
  @Test
  public void elect() {
    RaftState state = new RaftState("name", new KeyValueStateMachine("name"), MoreExecutors.newDirectExecutorService());
    state.leader = Optional.of("someone");
    // Elect
    state.elect(0L);
    assertEquals("Leader was never marked as a leader", RaftState.LeadershipStatus.LEADER, state.leadership);
    assertTrue("Leader should have a nextIndex map", state.nextIndex.isPresent());
    assertTrue("Leader should have a matchIndex map", state.matchIndex.isPresent());
    assertTrue("Leader should have a lastMessageTimestamp map", state.lastMessageTimestamp.isPresent());
    assertEquals("Leader should see themselves as leader", Optional.of("name"), state.leader);

    // Elect again (should exception)
    assertException(() -> state.elect(0L), AssertionError.class);
  }


  /**
   * Test that {@link RaftState#elect(long)} initializes {@link RaftState#nextIndex}, {@link RaftState#matchIndex},
   * and {@link RaftState#lastMessageTimestamp}.
   */
  @SuppressWarnings("OptionalGetWithoutIsPresent")
  @Test
  public void electShouldInitializeMaps() {
    List<String> members = Arrays.asList("L", "A", "B");
    RaftLog log = new RaftLog(new KeyValueStateMachine("name"), members, MoreExecutors.newDirectExecutorService());
    RaftState state = new RaftState("L", log, 3);

    // Elect
    state.elect(0L);
    Set<String> expectedKeys = new HashSet<>(members);
    expectedKeys.remove("L");
    assertEquals(expectedKeys, state.nextIndex.get().keySet());
    assertEquals(expectedKeys, state.matchIndex.get().keySet());
    assertEquals(expectedKeys, state.lastMessageTimestamp.get().keySet());

    // Step down
    state.stepDownFromElection(state.currentTerm, 0L);
    assertFalse(state.nextIndex.isPresent());
    assertFalse(state.matchIndex.isPresent());
    assertFalse(state.lastMessageTimestamp.isPresent());

    // Re-Elect
    state.elect(0L);
    expectedKeys.remove("L");
    assertEquals(expectedKeys, state.nextIndex.get().keySet());
    assertEquals(expectedKeys, state.matchIndex.get().keySet());
    assertEquals(expectedKeys, state.lastMessageTimestamp.get().keySet());
  }


  /**
   * Test that {@link RaftState#elect(long)} initializes {@link RaftState#nextIndex}, {@link RaftState#matchIndex},
   * and {@link RaftState#lastMessageTimestamp} to the correct values
   */
  @SuppressWarnings("OptionalGetWithoutIsPresent")
  @Test
  public void electShouldInitializeMapsCorrectValues() {
    // Some initialization to get a nontrivial state
    List<String> members = Arrays.asList("L", "A", "B");
    RaftLog log = new RaftLog(new SingleByteStateMachine(), members, MoreExecutors.newDirectExecutorService());
    RaftState state = new RaftState("L", log, 3);
    state.elect(0L);
    state.commitUpTo(state.transition(new byte[]{42}).index, 0L);
    state.commitUpTo(state.transition(new byte[]{43}).index, 0L);
    state.stepDownFromElection(state.currentTerm, 0L);

    // Elect
    state.elect(1L);
    assertEquals(3L, state.nextIndex.get().get("A").longValue());
    assertEquals(0L, state.matchIndex.get().get("A").longValue());
    assertEquals(1L, state.lastMessageTimestamp.get().get("A").longValue());
  }


  /**
   * Test {@link RaftState#stepDownFromElection(long, long)}
   */
  @Test
  public void stepDownFromElection() {
    RaftState state = new RaftState("name", new KeyValueStateMachine("name"), MoreExecutors.newDirectExecutorService());
    // 1. Become a candidate
    state.nextIndex = Optional.of(Collections.emptyMap());
    state.matchIndex = Optional.of(Collections.emptyMap());
    state.leadership = RaftState.LeadershipStatus.CANDIDATE;
    state.receiveVoteFrom("name");
    assertEquals("Should be able to vote for self (if this fails, another test should also be failing)", Collections.singleton("name"), state.votesReceived);
    // 2. Step down
    state.stepDownFromElection(state.currentTerm, 0L);
    // 3. Check the resulting state
    assertEquals(RaftState.LeadershipStatus.OTHER, state.leadership);
    assertEquals(Optional.empty(), state.nextIndex);
    assertEquals(Optional.empty(), state.matchIndex);
    assertEquals(Optional.empty(), state.lastMessageTimestamp);
    assertEquals(Collections.emptySet(), state.votesReceived);
  }


  /**
   * Test the various state transitions between {@link RaftState#leadership} values.
   */
  @Test
  public void stateTransitions() {
    RaftState state = new RaftState("name", new KeyValueStateMachine("name"), MoreExecutors.newDirectExecutorService());
    // x : candidate -> candidate
    state.leadership = RaftState.LeadershipStatus.CANDIDATE;
    // ok: candidate -> leader
    state.elect(0L);
    // x : leader -> leader
    assertException(() -> state.elect(1L), AssertionError.class);  // cannot become a candidate twice
    // ok: leader -> other
    state.stepDownFromElection(state.currentTerm, 0L);  // can step down from election
    // ok: other -> leader
    state.elect(0L);  // can elect immediately
    // ok: candidate -> other
    state.stepDownFromElection(state.currentTerm, 0L);
  }


  /**
   * Test equals and hashcode for the Raft state
   */
  @Test
  public void equalsHashCode() {
    RaftState state = new RaftState("name", new KeyValueStateMachine("name"), MoreExecutors.newDirectExecutorService());
    RaftState copy = new RaftState("name", new KeyValueStateMachine("name"), MoreExecutors.newDirectExecutorService());
    assertEquals(state, state);
    assertEquals(state, copy);
    assertEquals(state.hashCode(), state.hashCode());
    assertEquals(state.hashCode(), copy.hashCode());
  }


  /**
   * Test copying the raft state, making sure it's actually a semantic copy
   */
  @Test
  public void copy() {
    RaftState state = new RaftState("name", new KeyValueStateMachine("name"), MoreExecutors.newDirectExecutorService());
    RaftState copy = state.copy();
    assertEquals(state, state);
    assertEquals(state, copy);
    assertEquals(state.hashCode(), state.hashCode());
    assertEquals(state.hashCode(), copy.hashCode());
  }


  /**
   * Test the {@link RaftState#observeLifeFrom(String, long)} ()} function
   */
  @SuppressWarnings("OptionalGetWithoutIsPresent")
  @Test
  public void observeLifeFrom() {
    RaftState state = new RaftState("name", new KeyValueStateMachine("name"), MoreExecutors.newDirectExecutorService());
    assertException(() -> state.observeLifeFrom("foo", 42L), AssertionError.class);  // should only be able to transition as leader
    state.elect(0L);
    assertTrue(state.isLeader());
    state.observeLifeFrom("foo", 42L);
    assertTrue("lastMessageTimestamp not present on presumed leader", state.lastMessageTimestamp.isPresent());
    assertEquals("Timestamp is incorrect for follower", 42L, state.lastMessageTimestamp.get().get("foo").longValue());
    assertTrue("foo should now have a nextIndex", state.nextIndex.get().containsKey("foo"));
    assertTrue("foo should now have a matchIndex", state.matchIndex.get().containsKey("foo"));
  }


  /**
   * Test the {@link RaftState#transition(byte[])} function
   */
  @Test
  public void submitTransition() {
    SingleByteStateMachine machine = new SingleByteStateMachine();
    RaftState state = new RaftState("name", machine, MoreExecutors.newDirectExecutorService());
    assertException(() -> state.transition(new byte[]{42}), AssertionError.class);  // should only be able to transition as leader
    state.elect(0L);
    assertEquals(-1, machine.value);
    RaftLogEntryLocation transition = state.transition(new byte[]{42});
    assertEquals(0L, transition.term);
    assertEquals(1L, transition.index);
    assertEquals(1, state.log.logEntries.size());
  }


  /**
   * Test {@link RaftState#commitUpTo(long, long)}
   */
  @Test
  public void commitUpTo() {
    RaftState state = new RaftState("name", new SingleByteStateMachine(), MoreExecutors.newDirectExecutorService());
    state.elect(0L);
    state.commitUpTo(0L, 0L);
    assertException(() -> state.commitUpTo(1, 0L), AssertionError.class);  // should not be able to commit beyond current index
    state.transition(new byte[]{42});
    state.commitUpTo(1L, 0L);
    assertException(() -> state.commitUpTo(0, 0L), AssertionError.class);  // should not be able to commit backwards
  }


  /**
   * Test the {@link RaftState#transition(byte[])} {@link RaftState#commitUpTo(long, long)} functions.
   *
   * <b>This test relies on {@link #submitTransition()} working!</b>
   */
  @Test
  public void submitTransitionAndCommit() {
    SingleByteStateMachine machine = new SingleByteStateMachine();
    RaftState state = new RaftState("name", machine, MoreExecutors.newDirectExecutorService());
    state.elect(0L);
    assertEquals(-1, machine.value);
    RaftLogEntryLocation transaction = state.transition(new byte[]{42});
    assertEquals(-1, machine.value);
    state.commitUpTo(transaction.index, 0L);
    assertEquals(42L, machine.value);
  }


  /**
   * Test {@link RaftState#resetElectionTimeout(long, Optional)}
   */
  @Test
  public void resetElectionTimeout() {
    RaftLog log = new RaftLog(new KeyValueStateMachine("name"), Arrays.asList("name", "other", "other2"), MoreExecutors.newDirectExecutorService());
    RaftState state = new RaftState("name", log, 3);
    assertFalse("we shouldn't know the leader at first", state.leader.isPresent());
    // Check initial value
    assertEquals(-1, state.electionTimeoutCheckpoint);
    // Check that we can move forwards
    state.resetElectionTimeout(10L, Optional.of("other"));
    assertEquals("We know the leader after refreshing the election timeout", Optional.of("other"), state.leader);
    assertEquals("The election timeout should have updated", 10L, state.electionTimeoutCheckpoint);
    // Check that we can't move backwards
//    assertException(() -> state.resetElectionTimeout(5L, "other2"), AssertionError.class);  // disabled this check, since we technically can appear to move backwards  TODO(gabor) fix me
    // Check that we can't set a leader that's not in the membership  note[gabor]: this is valid, if we're in a handoff
//    state.resetElectionTimeout(15L, "not_present");
//    assertEquals("The election timeout should have updated", 15L, state.electionTimeoutCheckpoint);
//    assertEquals("The leader should not have changed", Optional.of("other"), state.leader);
  }


  /**
   * Test {@link RaftState#shouldTriggerElection(long, Span)}
   */
  @Test
  public void shouldTriggerElection() {
    RaftState state = new RaftState("name", new SingleByteStateMachine(), MoreExecutors.newDirectExecutorService());
    Span timeout = new Span(100, 101);
    // Should not trigger election on first ping
    assertFalse("Should not start with election", state.shouldTriggerElection(1000, timeout));
    // Should not trigger election too quickly
    assertFalse("Should not immediately trigger election", state.shouldTriggerElection(1001, timeout));
    assertFalse("Must exceed, not just match, election timeout", state.shouldTriggerElection(1100, timeout));
    // Should then trigger
    assertTrue("Should trigger election", state.shouldTriggerElection(1101, timeout));
    // Triggering elections is idempotent
    assertTrue("Triggering elections is idempotent", state.shouldTriggerElection(1101, timeout));
    assertTrue("Triggering elections is idempotent", state.shouldTriggerElection(1102, timeout));
    // Reset timer
    state.resetElectionTimeout(1100, Optional.of("new_leader"));
    assertFalse("Should not longer trigger elections after reset", state.shouldTriggerElection(1102, timeout));
  }


  /**
   * Test {@link RaftState#shouldTriggerElection(long, Span)}
   */
  @Test
  public void shouldTriggerElectionHasStableTimeout() {
    RaftState state = new RaftState("name", new SingleByteStateMachine(), MoreExecutors.newDirectExecutorService());
    Span timeout = new Span(100, 200);
    assertFalse("Should not start with election", state.shouldTriggerElection(1000, timeout));
    for (int now = 1100; now < 1200; ++now) {
      if (state.shouldTriggerElection(now, timeout)) {
        for (int tries = 0; tries < 100; ++tries) {
          assertTrue("We triggered an election on this timeout once, but not again", state.shouldTriggerElection(now, timeout));
        }
      }
    }
  }


  /**
   * Test {@link RaftState#shouldTriggerElection(long, Span)}
   */
  @Test
  public void shouldTriggerElectionFalseIfNotInQuorum() {
    RaftLog log = new RaftLog(new KeyValueStateMachine("name"), Arrays.asList("A", "B"), MoreExecutors.newDirectExecutorService());
    RaftState state = new RaftState("name", log, 3);
    Span timeout = new Span(100, 101);
    assertFalse("Should not trigger an election, even though the timeout is there", state.shouldTriggerElection(1101, timeout));
  }

  /**
   * Test {@link RaftState#shouldTriggerElection(long, Span)}
   */
  @Test
  public void shouldTriggerElectionFalseIfLeader() {
    RaftState state = new RaftState("name", new SingleByteStateMachine(), MoreExecutors.newDirectExecutorService());
    Span timeout = new Span(100, 101);
    // Should not trigger election on first ping
    assertFalse("Should not start with election", state.shouldTriggerElection(1000, timeout));
    // Should not trigger election too quickly
    assertFalse("Should not immediately trigger election", state.shouldTriggerElection(1001, timeout));
    assertFalse("Must exceed, not just match, election timeout", state.shouldTriggerElection(1100, timeout));
    // Should then trigger
    assertTrue("Should trigger election", state.shouldTriggerElection(1101, timeout));
    assertTrue("Should trigger election (idempotency)", state.shouldTriggerElection(1101, timeout));
    // Should not trigger an election as leader
    state.elect(1101L);
    assertFalse("Should trigger election", state.shouldTriggerElection(1101, timeout));

  }


  /**
   * Test {@link RaftState#receiveVoteFrom(String)}
   */
  @Test
  public void receiveVotesFrom() {
    RaftLog log = new RaftLog(new KeyValueStateMachine("name"), Arrays.asList("name", "other", "other2"), MoreExecutors.newDirectExecutorService());
    RaftState state = new RaftState("name", log, 3);
    assertEquals("Should start with no votes", Collections.emptySet(), state.votesReceived);
    state.receiveVoteFrom("name");
    assertEquals("Should be able to vote for self", Collections.singleton("name"), state.votesReceived);
    state.receiveVoteFrom("not_present");
    assertEquals("Should not receive votes from servers not in the cluster", Collections.singleton("name"), state.votesReceived);
    state.receiveVoteFrom("other");
    assertEquals("Should be able to receive votes from others", new HashSet<>(Arrays.asList("name", "other")), state.votesReceived);
  }


  /**
   * Test {@link RaftState#reconfigure(Collection, long)} removing a server
   */
  @Test
  public void reconfigureRemove() {
    RaftLog log = new RaftLog(new KeyValueStateMachine("name"), Arrays.asList("name", "other", "other2"), MoreExecutors.newDirectExecutorService());
    RaftState state = new RaftState("name", log, 3);

    assertException(() -> state.reconfigure(Arrays.asList("name", "other"), 42L), AssertionError.class);  // cannot change state if we're not the leader
    state.elect(0L);
    state.reconfigure(Arrays.asList("name", "other"), 42L);

    assertEquals("New configuration should immediately be registered",
        new HashSet<>(Arrays.asList("name", "other")), state.log.getQuorumMembers());
    assertEquals("New configuration should not have been committed",
        new HashSet<>(Arrays.asList("name", "other", "other2")), state.log.committedQuorumMembers);
  }


  /**
   * Test {@link RaftState#reconfigure(Collection, long)} adding a server
   */
  @SuppressWarnings("OptionalGetWithoutIsPresent")
  @Test
  public void reconfigureAdd() {
    RaftLog log = new RaftLog(new KeyValueStateMachine("name"), Arrays.asList("L", "A"), MoreExecutors.newDirectExecutorService());
    RaftState state = new RaftState("L", log, 3);

    assertException(() -> state.reconfigure(Arrays.asList("L", "A", "B"), 42L), AssertionError.class);  // cannot change state if we're not the leader
    state.elect(0L);
    state.reconfigure(Arrays.asList("L", "A", "B"), 42L);

    assertEquals("New configuration should immediately be registered",
        new HashSet<>(Arrays.asList("L", "A", "B")), state.log.getQuorumMembers());
    assertEquals("New configuration should not have been committed",
        new HashSet<>(Arrays.asList("L", "A")), state.log.committedQuorumMembers);
    assertEquals("We should have a correctly-initialized nextIndex for 'B'",
        2L, state.nextIndex.get().get("B").longValue());
    assertEquals("We should have a correctly-initialized matchIndex for 'B'",
        0L, state.matchIndex.get().get("B").longValue());
    assertEquals("We should have a correctly-initialized lastTimestamp for 'B'",
        42L, state.lastMessageTimestamp.get().get("B").longValue());
  }


  /**
   * Test {@link RaftState#reconfigure(Collection, long)} adding a server
   */
  @Test
  public void reconfigureOneAtATime() {
    RaftLog log = new RaftLog(new KeyValueStateMachine("name"), Arrays.asList("L", "A", "B"), MoreExecutors.newDirectExecutorService());
    RaftState state = new RaftState("L", log, 3);
    state.elect(0L);

    // Cannot add two servers at once
    assertException(() -> state.reconfigure(Arrays.asList("L", "A", "B", "C", "D"), 42L), AssertionError.class);
    // Cannot remove two servers at once
    assertException(() -> state.reconfigure(Collections.singletonList("L"), 42L), AssertionError.class);
    // Cannot change two servers at once
    assertException(() -> state.reconfigure(Arrays.asList("L", "C", "D"), 42L), AssertionError.class);
  }


  /**
   * Test {@link RaftState#reconfigure(Collection, long)} commits a single-node configuration immediately
   */
  @Test
  public void reconfigureImmediatelyCommitSingletonConfig() {
    RaftLog log = new RaftLog(new KeyValueStateMachine("name"), Arrays.asList("L", "A"), MoreExecutors.newDirectExecutorService());
    RaftState state = new RaftState("L", log, 3);
    state.elect(0L);
    state.reconfigure(Collections.singletonList("L"), 42L);

    assertEquals("New configuration should immediately be registered",
        Collections.singleton("L"), state.log.getQuorumMembers());
    assertEquals("New configuration should have been committed",
        Collections.singleton("L"), state.log.committedQuorumMembers);
  }


  /**
   * Test {@link RaftState#bootstrap()}
   */
  @Test
  public void bootstrap() {
    RaftState state = new RaftState("L", new SingleByteStateMachine(), 3, MoreExecutors.newDirectExecutorService());
    assertException(() -> state.elect(42L), AssertionError.class);  // cannot be elected if not in quorum
    state.bootstrap();
    assertEquals(Collections.singleton("L"), state.log.getQuorumMembers());
    state.elect(42L);
  }


  /**
   * Test {@link RaftState#serverToAdd(long, long)} in a happy path
   */
  @Test
  public void serverToAdd() {
    RaftState state = new RaftState("L", new SingleByteStateMachine(), 3, MoreExecutors.newDirectExecutorService());

    assertException(() -> state.serverToAdd(0L, 1000L), AssertionError.class);  // server isn't a leader
    state.bootstrap();
    assertException(() -> state.serverToAdd(0L, 1000L), AssertionError.class);  // server isn't a leader
    state.elect(42L);
    assertFalse("We don't know of any other servers", state.serverToAdd(42L, 1000).isPresent());
    state.observeLifeFrom("A", 50L);
    assertEquals("We should want to add server 'A'", Optional.of("A"), state.serverToAdd(50L, 1000));
    state.log.latestQuorumMembers.add("A");  // hard code adding 'A'

    state.observeLifeFrom("B", 55L);
    assertEquals("We should want to add server 'B'", Optional.of("B"), state.serverToAdd(55L, 1000));
    state.log.latestQuorumMembers.add("B");  // hard code adding 'B'

    state.observeLifeFrom("C", 60L);
    assertEquals("We don't want more than 3 members", Optional.empty(), state.serverToAdd(60L, 1000));
  }


  /**
   * Test {@link RaftState#serverToAdd(long, long)} adds the most recent latency server
   */
  @Test
  public void serverToAddMostRecentLatency() {
    RaftState state = new RaftState("L", new SingleByteStateMachine(), 3, MoreExecutors.newDirectExecutorService());

    assertException(() -> state.serverToAdd(0L, 1000L), AssertionError.class);  // server isn't a leader
    state.bootstrap();
    assertException(() -> state.serverToAdd(0L, 1000L), AssertionError.class);  // server isn't a leader
    state.elect(42L);
    assertFalse("We don't know of any other servers", state.serverToAdd(42L, 1000L).isPresent());
    state.observeLifeFrom("A", 50L);
    state.observeLifeFrom("B", 55L);
    assertEquals("We should want to add server 'B' (it's lower latency)", Optional.of("B"), state.serverToAdd(60L, 1000));
  }


  /**
   * Test {@link RaftState#serverToAdd(long, long)} adds the most recent latency server
   */
  @Test
  public void serverToAddIgnoredIfNoTargetClusterSize() {
    RaftState state = new RaftState("L", new SingleByteStateMachine(), -1, MoreExecutors.newDirectExecutorService());
    state.elect(42L);
    state.observeLifeFrom("A", 50L);
    assertFalse("We don't want to add servers -- target size is -1", state.serverToAdd(42L, 1000).isPresent());
  }


  /**
   * Test {@link RaftState#serverToAdd(long, long)} doesn't add delinquent nodes
   */
  @Test
  public void serverToAddEnsureNotDelinquent() {
    RaftState state = new RaftState("L", new SingleByteStateMachine(), 3, MoreExecutors.newDirectExecutorService());
    state.bootstrap();
    state.elect(42L);
    state.observeLifeFrom("A", 50L);
    assertFalse("We don't want to add delinquent servers", state.serverToAdd(1050L, 1000).isPresent());  // 50ms after observing life
  }


  /**
   * Test {@link RaftState#serverToRemove(long, long)} in a happy path
   */
  @Test
  public void serverToRemove() {
    RaftLog log = new RaftLog(new KeyValueStateMachine("name"), Arrays.asList("L", "A", "B", "C"), MoreExecutors.newDirectExecutorService());
    RaftState state = new RaftState("L", log, 3);

    assertException(() -> state.serverToRemove(0L, 1000L), AssertionError.class);  // server isn't a leader
    state.elect(100L);
    state.observeLifeFrom("A", 110);
    state.observeLifeFrom("B", 120);
    assertEquals("Server 'C' is the most delinquent -- we want to remove it", Optional.of("C"), state.serverToRemove(120, 1000L));
    state.log.latestQuorumMembers.remove("C");  // hard code removing 'C'

    assertFalse("We're happy with the cluster -- nothing has timed out", state.serverToRemove(200, 1000L).isPresent());
    assertFalse("We're happy with the cluster -- nothing has timed out (barely)", state.serverToRemove(1110, 1000L).isPresent());

    assertEquals("Server 'A' is delinquent (and most delinquent) on timeouts -- we want to remove it", Optional.of("A"), state.serverToRemove(1150, 1000L));
  }


  /**
   * Test {@link RaftState#serverToRemove(long, long)} should be ignored if no target cluster size
   */
  @Test
  public void serverToRemoveNoTargetClusterSize() {
    RaftLog log = new RaftLog(new KeyValueStateMachine("name"), Arrays.asList("L", "A", "B", "C"), MoreExecutors.newDirectExecutorService());
    RaftState state = new RaftState("L", log, -1);
    state.elect(100L);

    assertFalse("We should not want to remove any server -- no target cluster size",
        state.serverToRemove(120, 1000L).isPresent());
    assertFalse("We should not want to remove any server even in case of downtime -- no target cluster size",
        state.serverToRemove(1200, 1000L).isPresent());

  }


  /**
   * This is a simple helper to create an entry to be inserted into the Raft log.
   */
  public static EloquentRaftProto.LogEntry makeEntry(long index, long term, String key, byte[] value, String owner) {
    return EloquentRaftProto.LogEntry
        .newBuilder()
        .setType(EloquentRaftProto.LogEntryType.TRANSITION)
        .setIndex(index)
        .setTerm(term)
        .setTransition(KeyValueStateMachineProto.Transition.newBuilder()
            .setType(KeyValueStateMachineProto.TransitionType.SET_VALUE)
            .setSetValue(KeyValueStateMachineProto.SetValue.newBuilder()
                .setKey(key)
                .setValue(ByteString.copyFrom(value))
                .setOwner(owner)
                .build())
        .build().toByteString())
        .build();
  }


  /**
   * Test {@link RaftState#killNodes(long, long)}
   */
  @SuppressWarnings("OptionalGetWithoutIsPresent")
  @Test
  public void killNodes() {
    RaftLog log = new RaftLog(new KeyValueStateMachine("name"), Arrays.asList("L", "A", "B"), MoreExecutors.newDirectExecutorService());
    RaftState state = new RaftState("L", log, -1);
    state.elect(100L);
    assertTrue("Should commit an entry (needed to show up as an owner)",
        state.log.appendEntries(state.log.getLastEntryIndex(), log.getLastEntryTerm(),
            Collections.singletonList(makeEntry(state.log.getLastEntryIndex() + 1, log.getLastEntryTerm(), "foo", new byte[]{42}, "A"))));
    state.commitUpTo(log.getLastEntryIndex(), 0L);

    state.observeLifeFrom("A", 1000);
    state.observeLifeFrom("B", 1100);

    assertEquals("Don't want to kill anyone at first", Collections.emptySet(), state.killNodes(1000, 500));
    assertEquals("Don't want to kill anyone right at the cutoff", Collections.emptySet(), state.killNodes(1500, 500));
    assertEquals("Want to kill 'A' right after their timeout", Collections.singleton("A"), state.killNodes(1501, 500));
    assertEquals("'A' should be in the already killed list", Collections.singleton("A"), state.alreadyKilled.get());
    assertEquals("Should not want to double-kill 'A'", Collections.emptySet(), state.killNodes(1500, 500));
    state.revive("A");
    assertEquals("Should want to kill 'A' after revival", Collections.singleton("A"), state.killNodes(1501, 500));
  }


  /**
   * Test {@link RaftState#killNodes(long, long)}
   * Kill a node even if they don't have any life ever
   */
  @SuppressWarnings("OptionalGetWithoutIsPresent")
  @Test
  public void killNodesEvenIfNoLifeEver() {
    RaftLog log = new RaftLog(new KeyValueStateMachine("name"), Arrays.asList("L", "A", "B"), MoreExecutors.newDirectExecutorService());
    RaftState state = new RaftState("L", log, -1);
    state.elect(100L);
    state.lastMessageTimestamp.get().remove("A");   // A doesn't exist for some reason
    assertTrue("Should commit an entry (needed to show up as an owner)",
        state.log.appendEntries(state.log.getLastEntryIndex(), log.getLastEntryTerm(),
            Collections.singletonList(makeEntry(state.log.getLastEntryIndex() + 1, log.getLastEntryTerm(), "foo", new byte[]{42}, "A"))));
    state.commitUpTo(log.getLastEntryIndex(), 0L);

    assertEquals("Don't want to kill anyone at first", Collections.emptySet(), state.killNodes(1000, 500));
    assertEquals("Don't want to kill anyone at half of the cutoff", Collections.emptySet(), state.killNodes(1250, 500));  // 1250 = 1000 + 500 / 2 := (last fn run + timeout / 2)
    assertEquals("Want to kill 'A' right after their timeout", Collections.singleton("A"), state.killNodes(1251, 500));
  }

}