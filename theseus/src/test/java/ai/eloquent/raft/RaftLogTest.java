package ai.eloquent.raft;

import ai.eloquent.raft.algorithm.AbstractRaftAlgorithmTest;
import ai.eloquent.util.TimerUtils;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.Assert.*;

/**
 * This unit tests a lot of the important core logic around appending to the logs and compacting the logs, in addition
 * to dealing with snapshots.
 */
@SuppressWarnings({"SynchronizationOnLocalVariableOrMethodParameter", "unused", "UnusedAssignment"})
public class RaftLogTest {


  //
  // --------------------------------------------------------------------------
  // HELPER METHODS
  // --------------------------------------------------------------------------
  //


  /** Assert that the given runnable should throw the given exception (or something compatible). */
  @SuppressWarnings("SameParameterValue")
  protected void assertException(Runnable r, Class<? extends AssertionError> expectedException) {
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
   * This is a simple helper to create an entry to be inserted into the Raft log.
   */
  public static EloquentRaftProto.LogEntry makeEntry(long index, long term, int value) {
    return EloquentRaftProto.LogEntry
        .newBuilder()
        .setType(EloquentRaftProto.LogEntryType.TRANSITION)
        .setIndex(index)
        .setTerm(term)
        .setTransition(ByteString.copyFrom(new byte[]{(byte)value}))
        .build();
  }

  /**
   * This is a simple helper to verify that log entries are correct.
   */
  private void verifyEntry(RaftLog log, long index, long term, int value) {
    Optional<EloquentRaftProto.LogEntry> optionalEntry = log.getEntryAtIndex(index);
    assertTrue(optionalEntry.isPresent());
    EloquentRaftProto.LogEntry entry = optionalEntry.get();
    assertEquals(EloquentRaftProto.LogEntryType.TRANSITION, entry.getType());
    assertEquals(index, entry.getIndex());
    assertEquals(term, entry.getTerm());
    assertEquals((byte)value, entry.getTransition().byteAt(0));
  }

  /**
   * This is a simple helper to create an entry to be inserted into the Raft log.
   */
  public static EloquentRaftProto.LogEntry makeConfigurationEntry(long index, long term, Collection<String> clusterMembership) {
    return EloquentRaftProto.LogEntry
        .newBuilder()
        .setType(EloquentRaftProto.LogEntryType.CONFIGURATION)
        .setIndex(index)
        .setTerm(term)
        .addAllConfiguration(clusterMembership)
        .build();
  }

  /**
   * This is a simple helper to verify that log entries are correct.
   */
  @SuppressWarnings("SameParameterValue")
  private void verifyConfigurationEntry(RaftLog log, long index, long term, Collection<String> clusterMembership) {
    Optional<EloquentRaftProto.LogEntry> optionalEntry = log.getEntryAtIndex(index);
    assertTrue(optionalEntry.isPresent());
    EloquentRaftProto.LogEntry entry = optionalEntry.get();
    assertEquals(EloquentRaftProto.LogEntryType.CONFIGURATION, entry.getType());
    assertEquals(index, entry.getIndex());
    assertEquals(term, entry.getTerm());
    assertEquals(new HashSet<>(clusterMembership), new HashSet<>(entry.getConfigurationList()));
  }


  /**
   * A helper for creating a log with a fertile state for submitting snapshots.
   *
   * @see #installSnapshotOnCommit()
   * @see #installSnapshotOverCommit()
   * @see #installSnapshotWrongTerm()
   */
  private RaftLog mkSnapshotSetup() {
    RaftLog log = new RaftLog(new SingleByteStateMachine(), new ArrayList<>(), MoreExecutors.newDirectExecutorService());
    List<EloquentRaftProto.LogEntry> appendEntries = Arrays.asList(
        makeEntry(1, 1, 3),
        makeEntry(2, 1, 4),
        makeEntry(3, 1, 5),
        makeEntry(4, 2, 1),
        makeEntry(5, 2, 3)
    );
    boolean success = log.appendEntries(0, 0, appendEntries);
    assertEquals("The log should have the correct index", 5, log.getLastEntryIndex());
    assertEquals("The log should have the correct term", 2, log.getLastEntryTerm());
    assertEquals("Nothing should be committed to the state machine yet",
        -1, ((SingleByteStateMachine) log.stateMachine).value);
    return log;
  }


  //
  // --------------------------------------------------------------------------
  // GET ENTRY AT INDEX
  // --------------------------------------------------------------------------
  //


  /**
   * A simple test case for {@link RaftLog#getEntryAtIndex(long)}
   */
  @Test
  public void getEntryAtIndexSimple() {
    SingleByteStateMachine stateMachine = new SingleByteStateMachine();
    RaftLog log = new RaftLog(stateMachine, new ArrayList<>(), MoreExecutors.newDirectExecutorService());
    assertFalse("An empty log should have no entry @ index 0", log.getEntryAtIndex(0).isPresent());
    assertFalse("An empty log should have no entry @ index 1", log.getEntryAtIndex(1).isPresent());

    boolean success = log.appendEntries(0, 0, Collections.singletonList(makeEntry(1, 1, 3)));
    assertFalse("An log should never have an entry @ index 0", log.getEntryAtIndex(0).isPresent());
    assertTrue("We should have committed our entry @ index 1", log.getEntryAtIndex(1).isPresent());
    assertFalse("We should still not have an entry @ index 2", log.getEntryAtIndex(2).isPresent());
  }


  /**
   * Test {@link RaftLog#getEntryAtIndex(long)} for when we made a snapshot
   */
  @Test
  public void getEntryAtIndexInSnapshot() {
    RaftLog log = mkSnapshotSetup();
    RaftLog.Snapshot snapshot = new RaftLog.Snapshot(new SingleByteStateMachine(5).serialize(), 3, 1, new ArrayList<>());
    assertTrue("Should be able to install our snapshot", log.installSnapshot(snapshot, 0));

    assertFalse("An log should never have an entry @ index 0", log.getEntryAtIndex(0).isPresent());
    assertFalse("Index 1 should be in our snapshot", log.getEntryAtIndex(1).isPresent());
    assertFalse("Index 2 should be in our snapshot", log.getEntryAtIndex(2).isPresent());
    assertFalse("Index 3 should be in our snapshot", log.getEntryAtIndex(3).isPresent());
    assertTrue("Index 4 is not yet in our snapshot", log.getEntryAtIndex(4).isPresent());
    assertTrue("Index 5 is not yet in our snapshot", log.getEntryAtIndex(5).isPresent());
    assertFalse("Index 6 is not yet entered into the log", log.getEntryAtIndex(6).isPresent());
  }


  /**
   * Test {@link RaftLog#getEntryAtIndex(long)} for a sequence of standard transitions
   */
  @Test
  public void getEntryAtIndexFuzzTest() {
    SingleByteStateMachine stateMachine = new SingleByteStateMachine();
    RaftLog log = new RaftLog(stateMachine, new ArrayList<>(), MoreExecutors.newDirectExecutorService());
    Random r = new Random();

    for (int i = 1; i <= 1000; ++i) {
      if (i > 990 || r.nextInt(100) == 0) {
        assertTrue("We should be able to append our new entry",
            log.appendEntries(log.getLastEntryIndex(), log.getLastEntryTerm(), Collections.singletonList(makeEntry(i, 1, i))));
        assertTrue("Index " + i + " should be in our log. last snapshot=" + log.snapshot, log.getEntryAtIndex(i).isPresent());
      } else {
        RaftLog.Snapshot snapshot = new RaftLog.Snapshot(new SingleByteStateMachine(i).serialize(), i, 1, new ArrayList<>());
        log.installSnapshot(snapshot, 0);
        assertFalse("Index " + i + " should now be snapshotted", log.getEntryAtIndex(i).isPresent());
      }
    }
    assertTrue("Index 1000 should be in our log", log.getEntryAtIndex(1000).isPresent());
  }


  /**
   * A simple test case for {@link RaftLog#getEntryAtIndex(long)}
   */
  @Test
  public void getEntryAtIndexOutOfBounds() {
    RaftLog log = new RaftLog(new SingleByteStateMachine(), new ArrayList<>(), MoreExecutors.newDirectExecutorService());
    log.logEntries.add(makeEntry(1, 1, 0));
    log.logEntries.add(makeEntry(3, 1, 0));
    assertException(() -> log.getEntryAtIndex(2), AssertionError.class);
  }


  //
  // --------------------------------------------------------------------------
  // APPEND ENTRIES
  // --------------------------------------------------------------------------
  //


  @Test
  public void appendSimple() {
    SingleByteStateMachine stateMachine = new SingleByteStateMachine();
    RaftLog log = new RaftLog(stateMachine, new ArrayList<>(), MoreExecutors.newDirectExecutorService());

    List<EloquentRaftProto.LogEntry> appendEntries = new ArrayList<>();
    // Add a transition to the value 3
    appendEntries.add(makeEntry(1, 1, 3));

    // Add the entries as uncommitted
    boolean success = log.appendEntries(0, 0, appendEntries);
    assertTrue(success);
    assertEquals("Nothing should be committed yet", -1, stateMachine.value);

    // Check the entry is present
    verifyEntry(log, 1, 1, 3);

    // test getEntriesSinceInclusive()
    Optional<List<EloquentRaftProto.LogEntry>> entries = log.getEntriesSinceInclusive(1);
    assertTrue(entries.isPresent());
    assertEquals(1, entries.get().get(0).getIndex());
    entries = log.getEntriesSinceInclusive(2);
    assertTrue(entries.isPresent());
    assertEquals(0, entries.get().size());

    // Commit the entries
    log.setCommitIndex(1, 0L);
    assertEquals("We should have applied our entry to our state machine", 3, stateMachine.value);
  }


  /**
   * Ensure that we can't append an entry that skips indices in the log.
   */
  @Test
  public void appendSkipIndex() {
    SingleByteStateMachine stateMachine = new SingleByteStateMachine();
    RaftLog log = new RaftLog(stateMachine, new ArrayList<>(), MoreExecutors.newDirectExecutorService());
    log.appendEntries(0, 0, Collections.singletonList(makeEntry(1, 1, 42)));
    assertException(() -> log.appendEntries(0, 0, Collections.singletonList(makeEntry(3, 1, 42))), AssertionError.class);

  }


  /**
   * Create an append that deletes subsequent log entries
   */
  @Test
  public void appendThatDeletes() {
    RaftLog log = new RaftLog(new SingleByteStateMachine(), new ArrayList<>(), MoreExecutors.newDirectExecutorService());
    assertTrue("Should be able to append first entry", log.appendEntries(0, 1, Collections.singletonList(makeEntry(1, 1, 3))));
    assertEquals("Log should have new entry", 1, log.logEntries.size());
    assertTrue("Should be able to append second entry", log.appendEntries(1, 1, Collections.singletonList(makeEntry(2, 1, 3))));
    assertEquals("Log should have new entry", 2, log.logEntries.size());
    assertTrue("Should be able to overwrite only some entries", log.appendEntries(1, 1, Collections.emptyList()));
    assertEquals("Log should not actually have deleted the entry", 2, log.logEntries.size());
  }


  /**
   * Create an append that deletes subsequent log entries
   */
  @Test
  public void emptyAppendNotTruncate() {
    RaftLog log = new RaftLog(new SingleByteStateMachine(), new ArrayList<>(), MoreExecutors.newDirectExecutorService());
    assertTrue("Should be able to append first entry", log.appendEntries(0, 0, Collections.singletonList(makeEntry(1, 1, 3))));
    assertEquals("Log should have new entry", 1, log.logEntries.size());
    assertTrue("Should not be able to overwrite entries with nothing", log.appendEntries(0, 1, Collections.emptyList()));
    assertEquals("Log should not actually have deleted the entry", 1, log.logEntries.size());
  }


  /**
   * Nothing is in the log, and we're not adding anything
   */
  @Test
  public void appendShortCircuitIfNoEntries() {
    RaftLog log = new RaftLog(new SingleByteStateMachine(), new ArrayList<>(), MoreExecutors.newDirectExecutorService());
    assertTrue("Should return true on a noop", log.appendEntries(0, 0, Collections.emptyList()));
  }


  /**
   * Nothing is in the log, and we're not adding anything
   */
  @Test
  public void appendShortCircuitIfUpToDate() {
    RaftLog log = new RaftLog(new SingleByteStateMachine(), new ArrayList<>(), MoreExecutors.newDirectExecutorService());
    assertTrue("Should be able to append first entry", log.appendEntries(0, 0, Collections.singletonList(makeEntry(1, 1, 3))));
    assertTrue("Should return true if we're up to date", log.appendEntries(1, 1, Collections.emptyList()));
  }


  /**
   * We're adding a bunch of entries, then committing, then checking that a snapshot was made
   */
  @Test
  public void appendTooMuchForcesSnapshot() {
    RaftLog log = new RaftLog(new SingleByteStateMachine(), new ArrayList<>(), MoreExecutors.newDirectExecutorService());
    assertTrue("Should be able to append entry 0", log.appendEntries(0, 0, Collections.singletonList(makeEntry(1, 1, 3))));
    for (int i = 1; i < RaftLog.COMPACTION_LIMIT; ++i) {
      assertTrue("Should be able to append entry " + i, log.appendEntries(i, 1, Collections.singletonList(makeEntry(i + 1, 1, 3))));
    }

    assertFalse("Should not have snapshotted yet", log.snapshot.isPresent());
    assertEquals("Should have "+RaftLog.COMPACTION_LIMIT+" entries", RaftLog.COMPACTION_LIMIT, log.logEntries.size());
    log.setCommitIndex(log.getLastEntryIndex(), 0L);
    assertTrue("Should be able to add the entry that triggers the snapshot",
        log.appendEntries(RaftLog.COMPACTION_LIMIT, 1, Collections.singletonList(makeEntry(RaftLog.COMPACTION_LIMIT + 1, 1, 3))));
    assertTrue("Should now have a snapshot", log.snapshot.isPresent());
    assertSame("Incidental: createSnapshot should be idempotent", log.snapshot.get(), log.forceSnapshot());
    assertEquals("Should have one uncommitted log entry", 1, log.logEntries.size());
  }


  /**
   * We're adding a bunch of entries, but not committing, so a snapshot should not be made
   */
  @Test
  public void appendTooUncommittedDoesNotSnapshot() {
    RaftLog log = new RaftLog(new SingleByteStateMachine(), new ArrayList<>(), MoreExecutors.newDirectExecutorService());
    assertTrue("Should be able to append entry 0", log.appendEntries(0, 0, Collections.singletonList(makeEntry(1, 1, 3))));
    for (int i = 1; i < 100; ++i) {
      assertTrue("Should be able to append entry " + i, log.appendEntries(i, 1, Collections.singletonList(makeEntry(i + 1, 1, 3))));
    }

    assertFalse("Should not have snapshotted yet", log.snapshot.isPresent());
    assertEquals("Should have 100 entries", 100, log.logEntries.size());
    // <-- do not commit here! -->
    assertTrue("Should be able to add the entry that triggers the snapshot",
        log.appendEntries(100, 1, Collections.singletonList(makeEntry(101, 1, 3))));
    assertFalse("Should not have made a snapshot", log.snapshot.isPresent());
    assertEquals("Should have 101 uncommitted log entry", 101, log.logEntries.size());
  }


  /**
   * Ensure that we can harmlessly commit the same entry to the same index of the log.
   */
  @SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
  @Test
  public void appendIdempotent() {
    SingleByteStateMachine stateMachine = new SingleByteStateMachine();
    RaftLog log = new RaftLog(stateMachine, new ArrayList<>(), MoreExecutors.newDirectExecutorService());
    List<EloquentRaftProto.LogEntry> appendEntries = Arrays.asList(
        makeEntry(1, 1, 3)
    );
    // Simple add
    boolean success = log.appendEntries(0, 0, appendEntries);
    assertTrue(success);
    assertEquals("Nothing should be committed yet", -1, stateMachine.value);
    assertEquals("There should be a log entry", 1, log.logEntries.size());
    // Adds are idempotent, but allowed
    success = log.appendEntries(0, 0, appendEntries);
    assertTrue("Should be able to write to the same index, if terms match", success);
    assertEquals("Nothing should be committed yet", -1, stateMachine.value);
    assertEquals("There should be only one log entry", 1, log.logEntries.size());
  }


  /**
   * Ensure that we can append an entry with a newer term to the log.
   */
  @SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
  @Test
  public void appendAllowNewerTerm() {
    SingleByteStateMachine stateMachine = new SingleByteStateMachine();
    RaftLog log = new RaftLog(stateMachine, new ArrayList<>(), MoreExecutors.newDirectExecutorService());
    List<EloquentRaftProto.LogEntry> appendEntries = Arrays.asList(
        makeEntry(1, 1, 3)
    );
    // Simple add
    boolean success = log.appendEntries(0, 0, appendEntries);
    assertTrue(success);
    assertEquals("Nothing should be committed yet", -1, stateMachine.value);
    assertEquals("There should be a log entry", 1, log.logEntries.size());
    // Can add these entries
    appendEntries = Arrays.asList(
        makeEntry(1, 2, 4)
    );
    success = log.appendEntries(0, 0, appendEntries);
    assertTrue("Should not be able to write to an entry with an older log", success);
    assertEquals("Nothing should be committed yet", -1, stateMachine.value);
    assertEquals("There should be only one log entry", 1, log.logEntries.size());
  }


  //
  // ---------
  // This sub-section goes over basically every line of appendEntries to make
  // sure that it's actually doing what it's supposed to be doing.
  // ---------
  //


  /**
   * Ensure that we can append an entry with a newer term to the log.
   */
  @Test
  public void appendEntriesAlwaysAllowFirstEntry() {
    RaftLog log = new RaftLog(new SingleByteStateMachine(), new ArrayList<>(), MoreExecutors.newDirectExecutorService());
    assertTrue("We should always be able to append the first entry",
        log.appendEntries(0, 0, Collections.singletonList(makeEntry(1, 42, 0))));
  }


  /**
   * Ensure that we can append an entry with a newer term to the log.
   */
  @Test
  public void appendEntriesAllowIfPrevEntryExists() {
    RaftLog log = new RaftLog(new SingleByteStateMachine(), new ArrayList<>(), MoreExecutors.newDirectExecutorService());
    assertTrue("We should always be able to append the first entry",
        log.appendEntries(0, 0, Collections.singletonList(makeEntry(1, 2, 0))));
    assertTrue("We should be able to append the next entry, with same term",
        log.appendEntries(0, 0, Collections.singletonList(makeEntry(2, 2, 0))));
    assertTrue("We should be able to append the next entry, with higher term",
        log.appendEntries(0, 0, Collections.singletonList(makeEntry(3, 4, 0))));
  }


  /**
   * Ensure that we can't append entries that go backwards (i.e., have lower term than their predecessor.
   */
  @Test
  public void appendEntriesProhibitBackwardsTerm() {
    RaftLog log = new RaftLog(new SingleByteStateMachine(), new ArrayList<>(), MoreExecutors.newDirectExecutorService());
    assertTrue("We should always be able to append the first entry",
        log.appendEntries(0, 0, Collections.singletonList(makeEntry(1, 2, 0))));

    // Case 1: committing @ index=1, correct prev term
    assertFalse("We should NOT be able to append an entry with a lower term (@index=1, correct prev_term)",
        log.appendEntries(0, 0, Collections.singletonList(makeEntry(1, 1, 0))));
    // Case 2: committing @ index=2, incorrect prev term
    // This is basically a stupid corner case
    assertFalse("We should NOT be able to append an entry with a lower term (@index=2, incorrect prev_term)",
        log.appendEntries(0, 0, Collections.singletonList(makeEntry(2, 1, 0))));

    assertTrue(log.appendEntries(1, 2, Collections.singletonList(makeEntry(2, 2, 0))));
    // Case 3: committing @ index=3, correctly report previous term
    assertFalse("We should NOT be able to append an entry with a lower term (@index=3, correct prev_term)",
        log.appendEntries(1, 2, Collections.singletonList(makeEntry(3, 1, 0))));
    // Case 4: committing @ index=3, incorrectly report previous term
    assertFalse("We should NOT be able to append an entry with a lower term (@index=3, incorrect prev_term)",
        log.appendEntries(1, 1, Collections.singletonList(makeEntry(3, 1, 0))));
  }


  /**
   * Test that we can append entries over a snapshot
   */
  @Test
  public void appendEntriesOverSnapshot() {
    RaftLog log = mkSnapshotSetup();
    RaftLog.Snapshot snapshot = new RaftLog.Snapshot(new SingleByteStateMachine(3).serialize(), 5, 2, new ArrayList<>());
    log.installSnapshot(snapshot, 0L);
    assertTrue("We should be able to commit over the last index of a snapshot",
        log.appendEntries(5, 2, Collections.singletonList(makeEntry(6, 2, 0))));
  }


  /**
   * Test that we can append an entry of a newer term over a snapshot.
   */
  @Test
  public void appendEntriesOverSnapshotNewerTerm() {
    RaftLog log = mkSnapshotSetup();
    RaftLog.Snapshot snapshot = new RaftLog.Snapshot(new SingleByteStateMachine(3).serialize(), 5, 2, new ArrayList<>());
    log.installSnapshot(snapshot, 0L);
    assertTrue("We should be able to commit over the last index of a snapshot (even if term is higher)",
        log.appendEntries(5, 2, Collections.singletonList(makeEntry(6, 4, 0))));
  }


  /**
   * Test that we CANNOT append an entry of a older term over a snapshot.
   */
  @Test
  public void appendEntriesOverSnapshotOlderTerm() {
    RaftLog log = mkSnapshotSetup();
    RaftLog.Snapshot snapshot = new RaftLog.Snapshot(new SingleByteStateMachine(3).serialize(), 5, 2, new ArrayList<>());
    log.installSnapshot(snapshot, 0L);
    assertFalse("We should not commit an entry of lower term than our last snapshot",
        log.appendEntries(5, 2, Collections.singletonList(makeEntry(6, 1, 0))));  // term 1; snapshot term is 2
  }


  /**
   * Test that we can append an entry of a newer term over a snapshot.
   */
  @Test
  public void appendEntriesOverSnapshotCannotSkipIndex() {
    RaftLog log = mkSnapshotSetup();
    RaftLog.Snapshot snapshot = new RaftLog.Snapshot(new SingleByteStateMachine(3).serialize(), 5, 2, new ArrayList<>());
    log.installSnapshot(snapshot, 0L);

    // case 1: we correctly report prevLogIndex
    assertFalse("We should not be able to skip indexes after a snapshot",
        log.appendEntries(5, 2, Collections.singletonList(makeEntry(7, 2, 0))));  // note: index=7, after 5
    // case 2: we incorrectly report prevLogIndex
    assertFalse("We should not be able to skip indexes after a snapshot",
        log.appendEntries(6, 2, Collections.singletonList(makeEntry(7, 2, 0))));  // note: index=7, after 5
  }


  /**
   * Test that we can overwrite a log entry with one of a newer term
   */
  @Test
  public void appendEntriesOverwriteLogEntryWithNewerTerm() {
    RaftLog log = new RaftLog(new SingleByteStateMachine(), new ArrayList<>(), MoreExecutors.newDirectExecutorService());
    assertTrue("We should always be able to append the first entry",
        log.appendEntries(0, 0, Collections.singletonList(makeEntry(1, 2, 0))));
    assertTrue("We should be able to commit an entry with a newer term",
        log.appendEntries(0, 0, Collections.singletonList(makeEntry(1, 4, 0))));
  }


  /**
   * Test that we CANNOT overwrite a log entry with one of an older term
   */
  @Test
  public void appendEntriesOverwriteLogEntryWithOlderTerm() {
    RaftLog log = new RaftLog(new SingleByteStateMachine(), new ArrayList<>(), MoreExecutors.newDirectExecutorService());
    assertTrue("We should always be able to append the first entry",
        log.appendEntries(0, 0, Collections.singletonList(makeEntry(1, 2, 0))));
    assertFalse("We should NOT be able to write an entry with an older term (index=1)",
        log.appendEntries(0, 0, Collections.singletonList(makeEntry(1, 1, 0))));

    assertTrue("We should be able to commit a new entry",
        log.appendEntries(1, 2, Collections.singletonList(makeEntry(2, 2, 0))));
    assertFalse("We should NOT be able to write an entry with a older term (index=2)",
        log.appendEntries(1, 2, Collections.singletonList(makeEntry(2, 1, 0))));
  }


  /**
   * Test that we can overwrite a log entry with one of a newer term
   */
  @Test
  public void appendEntriesOverwriteLogEntryWithNewerTermClearsLog() {
    RaftLog log = new RaftLog(new SingleByteStateMachine(), new ArrayList<>(), MoreExecutors.newDirectExecutorService());
    assertTrue(log.appendEntries(0, 0, Collections.singletonList(makeEntry(1, 2, 1))));
    assertTrue(log.appendEntries(1, 2, Collections.singletonList(makeEntry(2, 2, 2))));
    assertTrue(log.appendEntries(2, 2, Collections.singletonList(makeEntry(3, 2, 3))));

    assertTrue("We should be able to commit an entry with a newer term",
        log.appendEntries(0, 0, Collections.singletonList(makeEntry(1, 4, 42))));
    assertEquals("We should have truncated our log", 1, log.logEntries.size());
    assertEquals("We should have the right term for the new entry", 4L, log.logEntries.getFirst().getTerm());
    assertEquals("We should have the right index for the new entry", 1L, log.logEntries.getFirst().getIndex());  // a bit silly, but may as well
    assertArrayEquals("We should have the right value for the new entry",
        new byte[]{42}, log.logEntries.getFirst().getTransition().toByteArray());
  }


  /**
   * Test that we can overwrite a log entry with one of a newer term, even if we're submitting a bulk
   * transition.
   */
  @Test
  public void appendEntriesOverwriteLogEntryWithNewerTermClearsLogBulk() {
    // 1. Set up the log
    RaftLog log = new RaftLog(new SingleByteStateMachine(), new ArrayList<>(), MoreExecutors.newDirectExecutorService());
    assertTrue(log.appendEntries(0, 0, Collections.singletonList(makeEntry(1, 2, 1))));
    assertTrue(log.appendEntries(1, 2, Collections.singletonList(makeEntry(2, 2, 2))));
    assertTrue(log.appendEntries(2, 2, Collections.singletonList(makeEntry(3, 2, 3))));
    // This entry should be removed when we clear the log below
    assertTrue(log.appendEntries(3, 2, Collections.singletonList(makeEntry(4, 2, 4))));

    // 2. Submit a transition that should truncate the log
    assertTrue("We should be able to commit an entry with a newer term",
        log.appendEntries(0, 0, Arrays.asList(
            makeEntry(1, 4, 40),
            makeEntry(2, 4, 41),
            makeEntry(3, 4, 42)
        )));
    assertEquals("We should have truncated our log", 3, log.logEntries.size());
    assertEquals("We should have the right term for the new [first] entry", 4L, log.logEntries.getFirst().getTerm());
    assertEquals("We should have the right term for the new [last] entry", 4L, log.logEntries.getLast().getTerm());
    assertArrayEquals("We should have the right value for the new [last] entry",
        new byte[]{42}, log.logEntries.getLast().getTransition().toByteArray());
  }


  /**
   * Test that we CANNOT overwrite a log entry with one of an older term
   */
  @Test
  public void appendEntriesDuplicateTransition() {
    RaftLog log = new RaftLog(new SingleByteStateMachine(), new ArrayList<>(), MoreExecutors.newDirectExecutorService());
    assertTrue(log.appendEntries(0, 0, Collections.singletonList(makeEntry(1, 2, 1))));
    assertException(() -> log.appendEntries(0, 0, Collections.singletonList(makeEntry(1, 2, 2))), AssertionError.class);
  }


  /**
   * Test that we CANNOT overwrite a log entry with one of an older term, even on a bulk transition
   */
  @Test
  public void appendEntriesDuplicateTransitionBulk() {
    RaftLog log = new RaftLog(new SingleByteStateMachine(), new ArrayList<>(), MoreExecutors.newDirectExecutorService());
    assertTrue(log.appendEntries(0, 0, Collections.singletonList(makeEntry(1, 2, 1))));
    assertException(() -> log.appendEntries(0, 0, Arrays.asList(
        makeEntry(1, 2, 2),  // <-- this entry conflicts! The others are fine.
        makeEntry(2, 2, 3),
        makeEntry(3, 2, 4)
        )), AssertionError.class);
  }


  /**
   * Create a bulk transition at index 1 (i.e., the first commit) where the terms
   * changed in the stream of things to add.
   * This is a regression in {@link AbstractRaftAlgorithmTest#testRecoverFromBotchedElection()}.
   */
  @Test
  public void appendEntriesOverwriteBulkIndex1() {
    RaftLog log = new RaftLog(new SingleByteStateMachine(), new ArrayList<>(), MoreExecutors.newDirectExecutorService());
    // populate the log
    log.appendEntries(0, 0, Arrays.asList(
        makeEntry(1, 2, 2),
        makeEntry(2, 3, 3),
        makeEntry(3, 3, 4)
    ));
    // append things again
    assertTrue("Should be able to replay the log",
        log.appendEntries(0, 0, Arrays.asList(
        makeEntry(1, 2, 2),
        makeEntry(2, 3, 3),
        makeEntry(3, 3, 4)
    )));
  }


  //
  // --------------------------------------------------------------------------
  // INSTALL SNAPSHOT
  // --------------------------------------------------------------------------
  //


  /**
   * Commit up to index 5, then install a snapshot at index 5.
   */
  @Test
  public void installSnapshotOnCommit() {
    // Setup
    RaftLog log = mkSnapshotSetup();
    log.setCommitIndex(5, 0);
    assertEquals("We should have 5 elements in the log before the snapshot",
        5, log.logEntries.size());

    RaftLog.Snapshot snapshot = new RaftLog.Snapshot(new SingleByteStateMachine(3).serialize(), 5, 2, new ArrayList<>());
    assertTrue("Should be able to install an outdated snapshot",
        log.installSnapshot(snapshot, 0));
    assertEquals("Should not go back in commits",
        5, log.commitIndex);
    assertEquals("Should still have the most recent value in the state machine",
        3, ((SingleByteStateMachine) log.stateMachine).value);
    assertEquals("We should have no elements in the log after the snapshot",
        0, log.logEntries.size());
  }


  /**
   * Commit up to index 5, then install a snapshot at index 2.
   * We should still behave as if we were at index 5.
   */
  @Test
  public void installSnapshotOverCommit() {
    // Setup
    RaftLog log = mkSnapshotSetup();
    log.setCommitIndex(5, 0);
    assertEquals("We should have 5 elements in the log before the snapshot",
        5, log.logEntries.size());

    RaftLog.Snapshot snapshot = new RaftLog.Snapshot(new SingleByteStateMachine(4).serialize(), 2, 1, new ArrayList<>());
    assertTrue("Should be able to install an outdated snapshot",
        log.installSnapshot(snapshot, 0));
    assertEquals("Should not go back in commits",
        5, log.commitIndex);
    assertEquals("Should still have the most recent value in the state machine",
        3, ((SingleByteStateMachine) log.stateMachine).value);
    assertEquals("We should have 3 elements in the log after the snapshot",
        3, log.logEntries.size());
  }


  /**
   * Commit up to index 5, then install a snapshot at index 5 but with the wrong term!
   * This should fail.
   */
  @SuppressWarnings("OptionalGetWithoutIsPresent")
  @Test
  public void installSnapshotWrongTerm() {
    // Setup
    RaftLog log = mkSnapshotSetup();
    log.setCommitIndex(5, 0);
    assertEquals("We should have 5 elements in the log before the snapshot",
        5, log.logEntries.size());

    // Install a bad snapshot
    assertEquals("We should be at term 2 -- we're going to try to install a snapshot on term 1",
        2, log.getEntryAtIndex(5).get().getTerm());
    RaftLog.Snapshot snapshot = new RaftLog.Snapshot(new SingleByteStateMachine(3).serialize(), 5, 1, new ArrayList<>());

    // Check state afterwards
    assertFalse("Should not be able to install a snapshot with the wrong term.",
        log.installSnapshot(snapshot, 0));
    assertEquals("Should not go back in commits",
        5, log.commitIndex);
    assertEquals("Should still have the most recent value in the state machine",
        3, ((SingleByteStateMachine) log.stateMachine).value);
    assertEquals("We should still have 5 elements in our log -- the snapshot failed",
        5, log.logEntries.size());
  }


  /**
   * Commit up to index 5, then install a snapshot at index 7 but with an earlier term.
   * This should fail, similar to {@link #installSnapshotWrongTerm()}.
   */
  @SuppressWarnings("OptionalGetWithoutIsPresent")
  @Test
  public void installSnapshotAheadWrongTerm() {
    // Setup
    RaftLog log = mkSnapshotSetup();
    log.setCommitIndex(5, 0);
    assertEquals("We should have 5 elements in the log before the snapshot",
        5, log.logEntries.size());

    // Install a bad snapshot
    assertEquals("We should be at term 2 -- we're going to try to install a snapshot on term 1",
        2, log.getEntryAtIndex(5).get().getTerm());
    RaftLog.Snapshot snapshot = new RaftLog.Snapshot(new SingleByteStateMachine(42).serialize(), 7, 1, new ArrayList<>());

    // Check state afterwards
    assertFalse("Should not be able to install a snapshot with the wrong term.",
        log.installSnapshot(snapshot, 0));
    assertEquals("Should not go back in commits",
        5, log.commitIndex);
    assertEquals("Should still have the most recent value in the state machine",
        3, ((SingleByteStateMachine) log.stateMachine).value);
    assertEquals("We should still have 5 elements in our log -- the snapshot failed",
        5, log.logEntries.size());
  }


  /**
   * Commit up to index 5, then install a snapshot at index 5 but with the wrong term!
   * This should fail.
   */
  @Test
  public void installSnapshotAheadOfLog() {
    // Setup
    RaftLog log = mkSnapshotSetup();
    log.setCommitIndex(5, 0);
    assertEquals("We should have 5 elements in the log before the snapshot",
        5, log.logEntries.size());

    // Install forward-looking
    RaftLog.Snapshot snapshot = new RaftLog.Snapshot(new SingleByteStateMachine(42).serialize(), 7, 2, new ArrayList<>());

    // Check state afterwards
    assertTrue("Should be able to install a snapshot ahead of the current log",
        log.installSnapshot(snapshot, 0));
    assertEquals("Should commit up to the snapshot",
        7, log.commitIndex);
    assertEquals("Should still have the most recent value in the state machine",
        42, ((SingleByteStateMachine) log.stateMachine).value);
    assertEquals("We should not have a log",
        0, log.logEntries.size());
  }


  /**
   * Should not be able to commit the same snapshot twice
   */
  @Test
  public void installSnapshotFailOnDuplicateSnapshot() {
    RaftLog log = mkSnapshotSetup();
    RaftLog.Snapshot firstSnapshot = new RaftLog.Snapshot(new SingleByteStateMachine(5).serialize(), 3, 1, new ArrayList<>());
    assertTrue(log.installSnapshot(firstSnapshot, 0L));
    RaftLog.Snapshot sameSnapshot = new RaftLog.Snapshot(new SingleByteStateMachine(5).serialize(), 3, 1, new ArrayList<>());
    assertFalse("Should not be able to install a snapshot of the same index + term", log.installSnapshot(sameSnapshot, 0L));
  }


  /**
   * Should be able to commit the same snapshot with a new term.
   * Note that this should never actually happen! This would be rewriting a committed entry with a
   * new term.
   */
  @Test
  public void installSnapshotDuplicateSnapshotNewTerm() {
    RaftLog log = mkSnapshotSetup();
    RaftLog.Snapshot firstSnapshot = new RaftLog.Snapshot(new SingleByteStateMachine(5).serialize(), 3, 1, new ArrayList<>());
    assertTrue(log.installSnapshot(firstSnapshot, 0L));
    RaftLog.Snapshot sameSnapshot = new RaftLog.Snapshot(new SingleByteStateMachine(5).serialize(), 3, 2, new ArrayList<>());
    assertTrue("Should be able to commit a snapshot with a new term", log.installSnapshot(sameSnapshot, 0L));
  }


  /**
   * Test some corner cases stemming from {@link #installSnapshotDuplicateSnapshotNewTerm()}.
   * In particular, make sure that we don't clear our log accidentally.
   */
  @Test
  public void installSnapshotClearLogCornerCase() {
    // Create our snapshot
    RaftLog log = mkSnapshotSetup();
    assertEquals("We should have 5 elements in the log before the snapshot", 5, log.logEntries.size());
    RaftLog.Snapshot firstSnapshot = new RaftLog.Snapshot(new SingleByteStateMachine(5).serialize(), 3, 1, new ArrayList<>());
    assertTrue(log.installSnapshot(firstSnapshot, 0L));
    assertEquals("We should have 2 log entries after our snapshot", 2, log.logEntries.size());
    assertEquals("Our first log entry should have term 2", 2, log.logEntries.getFirst().getTerm());

    // Install a snapshot with the same index but new term
    RaftLog.Snapshot sameSnapshot = new RaftLog.Snapshot(new SingleByteStateMachine(5).serialize(), 3, 2, new ArrayList<>());
    assertTrue("Should be able to commit a snapshot with a new term", log.installSnapshot(sameSnapshot, 0L));

    // Make sure we still have our log
    assertEquals("We should still have 2 log entries after our new term snapshot", 2, log.logEntries.size());
  }


  @SuppressWarnings("OptionalGetWithoutIsPresent")
  @Test
  public void simpleSnapshotTests() {
    SingleByteStateMachine stateMachine = new SingleByteStateMachine();
    RaftLog log = new RaftLog(stateMachine, new ArrayList<>(), MoreExecutors.newDirectExecutorService());
    assertEquals(0, log.getLastEntryIndex());
    assertEquals(0, log.getLastEntryTerm());

    List<EloquentRaftProto.LogEntry> appendEntries = new ArrayList<>();
    // Add a transition to the value 3
    appendEntries.add(makeEntry(1, 1, 3));
    appendEntries.add(makeEntry(2, 1, 2));
    appendEntries.add(makeEntry(3, 1, 1));

    // Add the entries as uncommitted
    boolean success = log.appendEntries(0, 0, appendEntries);
    assertTrue(success);
    assertEquals("Nothing should be committed yet", -1, stateMachine.value);
    assertEquals(3, log.getLastEntryIndex());
    assertEquals(1, log.getLastEntryTerm());

    // Check the entry is present
    verifyEntry(log, 1, 1, 3);
    verifyEntry(log, 2, 1, 2);
    verifyEntry(log, 3, 1, 1);

    SingleByteStateMachine recoveredStateMachine = new SingleByteStateMachine();

    RaftLog.Snapshot snapshot0 = log.forceSnapshot();

    recoveredStateMachine.overwriteWithSerialized(snapshot0.serializedStateMachine, TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());
    assertEquals(-1, recoveredStateMachine.value);

    // test getEntriesSinceIndex(), we shouldn't have compacted anything yet
    Optional<List<EloquentRaftProto.LogEntry>> entries = log.getEntriesSinceInclusive(1);
    assertTrue(entries.isPresent());
    assertEquals(3, entries.get().size());
    assertEquals(1, entries.get().get(0).getIndex());
    assertEquals(2, entries.get().get(1).getIndex());
    assertEquals(3, entries.get().get(2).getIndex());
    entries = log.getEntriesSinceInclusive(4);
    assertTrue(entries.isPresent());
    assertEquals(0, entries.get().size());

    // Commit the entries
    synchronized (log) {
      log.setCommitIndex((long) 2, TimerUtils.mockableNow().toEpochMilli());
    }
    assertEquals(3, log.logEntries.size());

    RaftLog.Snapshot snapshot2 = log.forceSnapshot();

    // test getEntriesSinceIndex(), we should now have compacted entries 1 and 2
    entries = log.getEntriesSinceInclusive(1);
    assertFalse(entries.isPresent());
    entries = log.getEntriesSinceInclusive(2);
    assertFalse(entries.isPresent());
    entries = log.getEntriesSinceInclusive(3);
    assertTrue(entries.isPresent());
    assertEquals(1, entries.get().size());
    assertEquals(3, entries.get().get(0).getIndex());
    entries = log.getEntriesSinceInclusive(4);
    assertTrue(entries.isPresent());
    assertEquals(0, entries.get().size());

    assertEquals(1, log.logEntries.size());
    recoveredStateMachine.overwriteWithSerialized(snapshot2.serializedStateMachine, TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());
    assertEquals(2, recoveredStateMachine.value);

    assertEquals("We should have applied our entries up through 2 to our state machine", 2, stateMachine.value);

    // This attempts to install an old snapshot, it should be a no-op
    assertSame(snapshot2, log.snapshot.get());
    boolean result7;
    synchronized (log) {
      result7 = log.installSnapshot(snapshot0, TimerUtils.mockableNow().toEpochMilli());
    }
    assertFalse(result7); // this should fail
    assertSame(snapshot2, log.snapshot.get());

    // If we reinstall the same snapshot, it should also be a no-op, and it should preserve our logs up to this point
    assertSame(snapshot2, log.snapshot.get());
    boolean result6;
    synchronized (log) {
      result6 = log.installSnapshot(snapshot2, TimerUtils.mockableNow().toEpochMilli());
    }
    assertFalse(result6);
    assertSame(snapshot2, log.snapshot.get());
    assertEquals(1, log.logEntries.size());

    // Then commit all the entries, and we should have updated our state
    synchronized (log) {
      log.setCommitIndex((long) 3, TimerUtils.mockableNow().toEpochMilli());
    }
    assertEquals(1, stateMachine.value);
    assertEquals(1, log.logEntries.size());

    // Take the snapshot of the last log entry
    RaftLog.Snapshot snapshot3 = log.forceSnapshot();
    assertEquals(1, stateMachine.value);
    assertEquals(0, log.logEntries.size());

    // We shouldn't be able to go backwards by installing an old snapshot
    boolean result5;
    synchronized (log) {
      result5 = log.installSnapshot(snapshot2, TimerUtils.mockableNow().toEpochMilli());
    }
    assertEquals(1, stateMachine.value);

    SingleByteStateMachine stateMachine2 = new SingleByteStateMachine();
    RaftLog log2 = new RaftLog(stateMachine2, new ArrayList<>(), MoreExecutors.newDirectExecutorService());

    // If we install a 0 snapshot, that should be fine
    assertEquals(-1, stateMachine2.value);
    boolean result4;
    synchronized (log2) {
      result4 = log2.installSnapshot(snapshot0, TimerUtils.mockableNow().toEpochMilli());
    }
    assertTrue(result4);
    assertEquals(-1, stateMachine2.value);
    assertEquals(0, log2.getLastEntryIndex());
    assertEquals(0, log2.getLastEntryTerm());

    // Installing the snapshot from index 2 should work
    boolean result3;
    synchronized (log2) {
      result3 = log2.installSnapshot(snapshot2, TimerUtils.mockableNow().toEpochMilli());
    }
    assertTrue(result3);
    assertEquals(2, stateMachine2.value);
    assertEquals(2, log2.getLastEntryIndex());
    assertEquals(1, log2.getLastEntryTerm());

    // Going backwards should be a no-op
    boolean result2;
    synchronized (log2) {
      result2 = log2.installSnapshot(snapshot0, TimerUtils.mockableNow().toEpochMilli());
    }
    assertFalse(result2);
    assertEquals(2, stateMachine2.value);

    assertEquals(2, log2.getLastEntryIndex());
    assertEquals(1, log2.getLastEntryTerm());

    // Installing the snapshot from index 2 twice should be a no-op
    boolean result1;
    synchronized (log2) {
      result1 = log2.installSnapshot(snapshot2, TimerUtils.mockableNow().toEpochMilli());
    }
    assertFalse(result1);
    assertEquals(2, stateMachine2.value);
    assertEquals(2, log2.getLastEntryIndex());
    assertEquals(1, log2.getLastEntryTerm());

    // Installing the snapshot from index 3 should work
    boolean result;
    synchronized (log2) {
      result = log2.installSnapshot(snapshot3, TimerUtils.mockableNow().toEpochMilli());
    }
    assertTrue(result);
    assertEquals(1, stateMachine2.value);
    assertEquals(3, log2.getLastEntryIndex());
    assertEquals(1, log2.getLastEntryTerm());
  }


  @Test
  public void snapshotAppend() {
    SingleByteStateMachine stateMachine = new SingleByteStateMachine();
    RaftLog log = new RaftLog(stateMachine, new ArrayList<>(), MoreExecutors.newDirectExecutorService());

    List<EloquentRaftProto.LogEntry> appendEntries1 = new ArrayList<>();
    appendEntries1.add(makeEntry(1, 1, 3));

    // Add the entries as uncommitted
    boolean success = log.appendEntries(0, 0, appendEntries1);
    assertTrue(success);
    assertEquals("Nothing should be committed yet", -1, stateMachine.value);

    // Check the entries are present
    assertEquals(1, log.logEntries.size());
    verifyEntry(log, 1, 1, 3);

    synchronized (log) {
      log.setCommitIndex((long) 1, TimerUtils.mockableNow().toEpochMilli());
    }
    log.forceSnapshot();

    assertEquals(0, log.logEntries.size());

    List<EloquentRaftProto.LogEntry> appendEntries2 = new ArrayList<>();
    appendEntries2.add(makeEntry(2, 1, 2));
    appendEntries2.add(makeEntry(3, 1, 1));

    // Add the entries, which should overwrite previous entries
    success = log.appendEntries(1, 1, appendEntries2);
    assertTrue(success);
    assertEquals("The first entry should be committed", 3, stateMachine.value);

    // Check the non-compacted entries are present
    verifyEntry(log, 2, 1, 2);
    verifyEntry(log, 3, 1, 1);

    assertEquals(2, log.logEntries.size());

    // Commit the entries
    synchronized (log) {
      log.setCommitIndex((long) 3, TimerUtils.mockableNow().toEpochMilli());
    }
    log.forceSnapshot();
    assertEquals("We should have applied our entry to our state machine", 1, stateMachine.value);
    assertEquals(0, log.logEntries.size());
  }


  @Test
  public void snapshotTruncateOverwrite() {
    SingleByteStateMachine stateMachine = new SingleByteStateMachine();
    RaftLog log = new RaftLog(stateMachine, new ArrayList<>(), MoreExecutors.newDirectExecutorService());

    List<EloquentRaftProto.LogEntry> appendEntries1 = new ArrayList<>();
    appendEntries1.add(makeEntry(1, 1, 3));
    appendEntries1.add(makeEntry(2, 1, 2));
    appendEntries1.add(makeEntry(3, 1, 1));

    // Add the entries as uncommitted
    boolean success = log.appendEntries(0, 0, appendEntries1);
    assertTrue(success);
    assertEquals("Nothing should be committed yet", -1, stateMachine.value);

    // Check the entries are present
    verifyEntry(log, 1, 1, 3);
    verifyEntry(log, 2, 1, 2);
    verifyEntry(log, 3, 1, 1);

    synchronized (log) {
      log.setCommitIndex((long) 1, TimerUtils.mockableNow().toEpochMilli());
    }
    assertEquals("The first entry should be committed", 3, stateMachine.value);
    log.forceSnapshot();

    List<EloquentRaftProto.LogEntry> appendEntries2 = new ArrayList<>();
    appendEntries2.add(makeEntry(2, 2, 5));
    appendEntries2.add(makeEntry(3, 2, 6));

    // Add the entries, which should overwrite previous entries
    success = log.appendEntries(1, 1, appendEntries2);
    assertTrue(success);
    assertEquals("The first entry should be committed", 3, stateMachine.value);

    // Check the non-compacted entries are present
    verifyEntry(log, 2, 2, 5);
    verifyEntry(log, 3, 2, 6);

    // Commit the entries
    synchronized (log) {
      log.setCommitIndex((long) 3, TimerUtils.mockableNow().toEpochMilli());
    }
    assertEquals("We should have applied our entry to our state machine", 6, stateMachine.value);
  }


  @Test
  public void loadSnapshotTruncateAllLaterEntries() {
    SingleByteStateMachine stateMachine = new SingleByteStateMachine();
    RaftLog log1 = new RaftLog(stateMachine, new ArrayList<>(), MoreExecutors.newDirectExecutorService());

    List<EloquentRaftProto.LogEntry> appendEntries1 = new ArrayList<>();
    appendEntries1.add(makeEntry(1, 1, 3));
    appendEntries1.add(makeEntry(2, 1, 2));
    appendEntries1.add(makeEntry(3, 1, 1));

    // Add the entries as uncommitted
    boolean success = log1.appendEntries(0, 0, appendEntries1);
    assertTrue(success);
    assertEquals("Nothing should be committed yet", -1, stateMachine.value);

    // Check the entries are present
    verifyEntry(log1, 1, 1, 3);
    verifyEntry(log1, 2, 1, 2);
    verifyEntry(log1, 3, 1, 1);

    synchronized (log1) {
      log1.setCommitIndex((long) 3, TimerUtils.mockableNow().toEpochMilli());
    }
    RaftLog.Snapshot snapshot = log1.forceSnapshot();

    assertEquals(0, log1.logEntries.size());

    SingleByteStateMachine stateMachine2 = new SingleByteStateMachine();
    RaftLog log2 = new RaftLog(stateMachine2, new ArrayList<>(), MoreExecutors.newDirectExecutorService());

    // Add the entries as uncommitted
    success = log2.appendEntries(0, 0, appendEntries1);
    assertTrue(success);

    // Check the entries are present
    verifyEntry(log2, 1, 1, 3);
    verifyEntry(log2, 2, 1, 2);
    verifyEntry(log2, 3, 1, 1);

    assertEquals(3, log2.logEntries.size());
    assertEquals(-1, stateMachine2.value);
    boolean result;
    synchronized (log2) {
      result = log2.installSnapshot(snapshot, TimerUtils.mockableNow().toEpochMilli());
    }
    assertEquals(0, log2.logEntries.size());
    assertEquals(3, log2.commitIndex);
    assertEquals(1, stateMachine2.value);
  }


  @Test
  public void testSnapshotWithClusterMembershipChanges() {
    SingleByteStateMachine stateMachine = new SingleByteStateMachine();

    Set<String> configuration0 = new HashSet<>();
    configuration0.add("test1");

    Set<String> configuration1 = new HashSet<>();
    configuration1.add("test1");

    Set<String> configuration2 = new HashSet<>();
    configuration2.add("test1");
    configuration2.add("test2");

    RaftLog log = new RaftLog(stateMachine, configuration0, MoreExecutors.newDirectExecutorService());

    assertEquals(configuration0, log.committedQuorumMembers);
    assertEquals(configuration0, log.latestQuorumMembers);

    // Add the entries as uncommitted
    boolean success = log.appendEntries(0, 0, Arrays.asList(
        makeEntry(1, 1, 3),
        makeConfigurationEntry(2, 1, configuration1),
        makeConfigurationEntry(3, 1, configuration2)
    ));
    assertTrue(success);
    assertEquals("Nothing should be committed yet", -1, stateMachine.value);

    // Check the entries are present
    verifyEntry(log, 1, 1, 3);
    verifyConfigurationEntry(log, 2, 1, configuration1);
    verifyConfigurationEntry(log, 3, 1, configuration2);

    // Check that we still have our initial configuration
    assertEquals(configuration0, log.committedQuorumMembers);
    assertEquals(configuration2, log.latestQuorumMembers);  // but we should be using latest cluster members

    // Commit config1
    synchronized (log) {
      log.setCommitIndex((long) 2, TimerUtils.mockableNow().toEpochMilli());
    }
    RaftLog.Snapshot snapshot1 = log.forceSnapshot();
    assertEquals(configuration1, snapshot1.lastClusterMembership);  // configuration1 should be the committed config
    assertEquals(configuration1, log.committedQuorumMembers);
    assertEquals(configuration2, log.latestQuorumMembers);         // but we should still use configuration 2

    // Commit config2
    synchronized (log) {
      log.setCommitIndex((long) 3, TimerUtils.mockableNow().toEpochMilli());
    }
    RaftLog.Snapshot snapshot2 = log.forceSnapshot();
    assertEquals(configuration2, snapshot2.lastClusterMembership);        // now everyone should use config2
    assertEquals(configuration2, log.committedQuorumMembers);
    assertEquals(configuration2, log.latestQuorumMembers);               // latest membership is of course also config2

    // Start a new log, to restore from a snapshot
    SingleByteStateMachine stateMachine2 = new SingleByteStateMachine();
    RaftLog log2 = new RaftLog(stateMachine2, configuration0, MoreExecutors.newDirectExecutorService());
    assertEquals(configuration0, log2.committedQuorumMembers);
    assertEquals(configuration0, log2.latestQuorumMembers);
    // Install snapshot1 (the one with config1 committed)
    boolean result1;
    synchronized (log2) {
      result1 = log2.installSnapshot(snapshot1, TimerUtils.mockableNow().toEpochMilli());
    }
    assertEquals(2, log2.getCommitIndex());
    assertEquals(configuration1, log2.committedQuorumMembers);
    assertEquals(configuration1, log2.latestQuorumMembers);
    // Install snapshot2 (the one with config1 committed)
    boolean result;
    synchronized (log2) {
      result = log2.installSnapshot(snapshot2, TimerUtils.mockableNow().toEpochMilli());
    }
    assertEquals(3, log2.getCommitIndex());
    assertEquals(configuration2, log2.committedQuorumMembers);
    assertEquals(configuration2, log2.latestQuorumMembers);
  }


  @Test
  public void snapshotLoadResnapshot() {
    SingleByteStateMachine stateMachine1 = new SingleByteStateMachine();
    RaftLog log1 = new RaftLog(stateMachine1, new ArrayList<>(), MoreExecutors.newDirectExecutorService());

    List<EloquentRaftProto.LogEntry> appendEntries1 = new ArrayList<>();
    appendEntries1.add(makeEntry(1, 1, 3));
    appendEntries1.add(makeEntry(2, 1, 2));
    appendEntries1.add(makeEntry(3, 1, 1));

    // Add the entries as uncommitted
    boolean success = log1.appendEntries(0, 0, appendEntries1);
    assertTrue(success);
    assertEquals("Nothing should be committed yet", -1, stateMachine1.value);

    // Check the entries are present
    verifyEntry(log1, 1, 1, 3);
    verifyEntry(log1, 2, 1, 2);
    verifyEntry(log1, 3, 1, 1);

    synchronized (log1) {
      log1.setCommitIndex((long) 3, TimerUtils.mockableNow().toEpochMilli());
    }
    RaftLog.Snapshot snapshot1 = log1.forceSnapshot();

    SingleByteStateMachine stateMachine2 = new SingleByteStateMachine();
    RaftLog log2 = new RaftLog(stateMachine2, new ArrayList<>(), MoreExecutors.newDirectExecutorService());
    boolean result1;
    synchronized (log2) {
      result1 = log2.installSnapshot(snapshot1, TimerUtils.mockableNow().toEpochMilli());
    }
    success = result1;
    assertTrue(success);
    assertEquals(3, log2.getCommitIndex());

    List<EloquentRaftProto.LogEntry> appendEntries2 = new ArrayList<>();
    appendEntries2.add(makeEntry(4, 2, 5));
    appendEntries2.add(makeEntry(5, 2, 6));

    // Add the entries to our loaded snapshot. First try with an invalid previous value
    success = log2.appendEntries(1, 1, appendEntries2);
    assertFalse(success);
    success = log2.appendEntries(3, 1, appendEntries2);
    assertTrue(success);

    // Check the non-compacted entries are present
    verifyEntry(log2, 4, 2, 5);
    verifyEntry(log2, 5, 2, 6);

    // Commit the entries
    synchronized (log2) {
      log2.setCommitIndex((long) 5, TimerUtils.mockableNow().toEpochMilli());
    }
    assertEquals("We should have applied our entry to our state machine", 6, stateMachine2.value);

    RaftLog.Snapshot snapshot2 = log2.forceSnapshot();
    boolean result;
    synchronized (log1) {
      result = log1.installSnapshot(snapshot2, TimerUtils.mockableNow().toEpochMilli());
    }
    assertEquals("We should have applied our entry to our state machine", 6, stateMachine1.value);
  }


  //
  // --------------------------------------------------------------------------
  // CREATE COMMIT FUTURE
  // --------------------------------------------------------------------------
  //


  /**
   * A Keenon test for commit futures
   *
   * TODO(gabor) split me up into tests that are actually readable
   */
  @Test
  public void testCreateCommitFuture() throws ExecutionException, InterruptedException {
    SingleByteStateMachine stateMachine = new SingleByteStateMachine();
    RaftLog log = new RaftLog(stateMachine, new ArrayList<>(), MoreExecutors.newDirectExecutorService());

    List<EloquentRaftProto.LogEntry> appendEntries1 = new ArrayList<>();
    appendEntries1.add(makeEntry(1, 1, 3));
    appendEntries1.add(makeEntry(2, 1, 2));
    appendEntries1.add(makeEntry(3, 1, 1));

    // Add the entries as uncommitted
    boolean success = log.appendEntries(0, 0, appendEntries1);
    assertTrue(success);
    assertEquals("Nothing should be committed yet", -1, stateMachine.value);

    CompletableFuture<Boolean> commit1BadTerm = log.createCommitFuture(1, 0);
    RaftLogEntryLocation location = new RaftLogEntryLocation(1, 1);
    CompletableFuture<Boolean> commit1 = log.createCommitFuture(location.index, location.term);  // just to cover this variant of the function too
    CompletableFuture<Boolean> commit2BadTerm = log.createCommitFuture(2, 0);
    CompletableFuture<Boolean> commit2 = log.createCommitFuture(2, 1);
    CompletableFuture<Boolean> commit3BadTerm = log.createCommitFuture(3, 0);
    CompletableFuture<Boolean> commit3 = log.createCommitFuture(3, 1);

    // Check the entries are present
    verifyEntry(log, 1, 1, 3);
    verifyEntry(log, 2, 1, 2);
    verifyEntry(log, 3, 1, 1);

    assertFalse(commit1BadTerm.isDone());
    assertFalse(commit1.isDone());
    assertFalse(commit2BadTerm.isDone());
    assertFalse(commit2.isDone());
    assertFalse(commit3BadTerm.isDone());
    assertFalse(commit3.isDone());

    synchronized (log) {
      log.setCommitIndex((long) 1, TimerUtils.mockableNow().toEpochMilli());
    }

    assertTrue(commit1BadTerm.isDone());
    assertEquals(false, commit1BadTerm.get());
    assertTrue(commit1.isDone());
    assertEquals(true, commit1.get());
    assertFalse(commit2BadTerm.isDone());
    assertFalse(commit2.isDone());
    assertFalse(commit3BadTerm.isDone());
    assertFalse(commit3.isDone());

    synchronized (log) {
      log.setCommitIndex((long) 3, TimerUtils.mockableNow().toEpochMilli());
    }

    assertTrue(commit1BadTerm.isDone());
    assertEquals(false, commit1BadTerm.get());
    assertTrue(commit1.isDone());
    assertEquals(true, commit1.get());
    assertTrue(commit2BadTerm.isDone());
    assertEquals(false, commit2BadTerm.get());
    assertTrue(commit2.isDone());
    assertEquals(true, commit2.get());
    assertTrue(commit3BadTerm.isDone());
    assertEquals(false, commit3BadTerm.get());
    assertTrue(commit3.isDone());
    assertEquals(true, commit3.get());

    CompletableFuture<Boolean> commit1BadTermPast = log.createCommitFuture(1, 0);
    CompletableFuture<Boolean> commit1Past = log.createCommitFuture(1, 1);

    assertTrue(commit1BadTermPast.isDone());
    assertEquals(false, commit1BadTermPast.get());
    assertTrue(commit1Past.isDone());
    assertEquals(true, commit1Past.get());
  }


  /**
   * Test that commit futures offload to another thread, rather than completing on the main thread.
   */
  @Test
  public void commitFutureRunsOnSeparateThread() throws ExecutionException, InterruptedException, TimeoutException {
    long threadId = Thread.currentThread().getId();
    ExecutorService executor = Executors.newSingleThreadExecutor();  // important: default should be running on another thread
    try {
      RaftLog log = new RaftLog(new SingleByteStateMachine(), new ArrayList<>(), executor);
      CompletableFuture<Boolean> future = log.createCommitFuture(1, 1, false);  // note: not internal
      CompletableFuture<Boolean> onCorrectThread = future.thenApply(success -> Thread.currentThread().getId() == threadId);
      log.appendEntries(0, 0, Collections.singletonList(makeEntry(1, 1, 42)));
      log.setCommitIndex(1, 0L);

      assertFalse("The future should NOT have executed on the main thread", onCorrectThread.get(1, TimeUnit.SECONDS));
    } finally {
      executor.shutdown();
    }
  }


  /**
   * Test creating an "internal" commit future
   */
  @Test
  public void internalCommitFuture() {
    long threadId = Thread.currentThread().getId();
    ExecutorService executor = Executors.newSingleThreadExecutor();  // important: default should be running on another thread
    try {
      RaftLog log = new RaftLog(new SingleByteStateMachine(), new ArrayList<>(), executor);
      CompletableFuture<Boolean> future = log.createCommitFuture(1, 1, true);
      CompletableFuture<Boolean> onCorrectThread = future.thenApply(success -> Thread.currentThread().getId() == threadId);
      log.appendEntries(0, 0, Collections.singletonList(makeEntry(1, 1, 42)));
      log.setCommitIndex(1, 0L);

      assertTrue("The future should have completed", onCorrectThread.isDone());
      assertTrue("The future should have executed on the main thread", onCorrectThread.getNow(false));
    } finally {
      executor.shutdown();
    }
  }


  /**
   * Test that commit futures don't fire until the commit is registered
   */
  @Test
  public void commitFutureWaitForCommit() {
    RaftLog log = new RaftLog(new SingleByteStateMachine(), new ArrayList<>(), MoreExecutors.newDirectExecutorService());
    CompletableFuture<Boolean> future = log.createCommitFuture(1, 1, true);
    log.appendEntries(0, 0, Collections.singletonList(makeEntry(1, 1, 42)));
    assertFalse("The future should NOT have completed before the commit", future.isDone());
    log.setCommitIndex(1, 0L);
    assertTrue("The future should have completed after the commit", future.isDone());
  }


  /**
   * The same as {@link #commitFutureWaitForCommit()}, but the future is registered after we append the entries,
   * rather than before. Who knows, could be a weird corner case.
   */
  @Test
  public void commitFutureRegisterAfterAppend() {
    RaftLog log = new RaftLog(new SingleByteStateMachine(), new ArrayList<>(), MoreExecutors.newDirectExecutorService());
    log.appendEntries(0, 0, Collections.singletonList(makeEntry(1, 1, 42)));
    CompletableFuture<Boolean> future = log.createCommitFuture(1, 1, true);
    assertFalse("The future should NOT have completed before the commit", future.isDone());
    log.setCommitIndex(1, 0L);
    assertTrue("The future should have completed after the commit", future.isDone());
  }


  //
  // --------------------------------------------------------------------------
  // TRUNCATE LOG AFTER
  // --------------------------------------------------------------------------
  //


  /**
   * Ensure that we can't truncate to negative indices
   */
  @Test
  public void truncateAfterInclusiveNegativeIndex() {
    RaftLog log = new RaftLog(new SingleByteStateMachine(), new ArrayList<>(), MoreExecutors.newDirectExecutorService());
    log.appendEntries(0, 0, Collections.singletonList(makeEntry(1, 1, 42)));
    assertException(() -> log.truncateLogAfterIndexInclusive(-1L), AssertionError.class);  // or, can ignore the error
    assertEquals("We should still have our entry", 1, log.logEntries.size());
  }


  /**
   * Ensure that we can't truncate committed entries
   */
  @Test
  public void truncateAfterInclusiveBeforeCommit() {
    RaftLog log = new RaftLog(new SingleByteStateMachine(), new ArrayList<>(), MoreExecutors.newDirectExecutorService());
    log.appendEntries(0, 0, Collections.singletonList(makeEntry(1, 1, 42)));
    log.setCommitIndex(1, 0);

    assertException(() -> log.truncateLogAfterIndexInclusive(0L), AssertionError.class);  // or, can ignore the error
    assertEquals("We should still have our committed entry", 1, log.logEntries.size());
  }


  /**
   * Ensure that we can't truncate an empty log.
   * This is a bit degenerate as a test, but it's a code path in the function.
   */
  @Test
  public void truncateAfterInclusiveEmptyLog() {
    RaftLog log = new RaftLog(new SingleByteStateMachine(), new ArrayList<>(), MoreExecutors.newDirectExecutorService());
    log.truncateLogAfterIndexInclusive(0L);
    assertEquals("The log should still be empty", 0, log.logEntries.size());
  }


  /**
   * Truncate some entries.
   */
  @Test
  public void truncateAfterInclusiveSimple() {
    RaftLog log = new RaftLog(new SingleByteStateMachine(), new ArrayList<>(), MoreExecutors.newDirectExecutorService());
    log.appendEntries(0, 0, Arrays.asList(
        makeEntry(1, 1, 40),
        makeEntry(2, 1, 41),
        makeEntry(3, 1, 42)
    ));

    log.truncateLogAfterIndexInclusive(2L);
    assertEquals("We should have 1 entries now", 1, log.logEntries.size());
    assertEquals("The entry should be at index 1", 1, log.logEntries.getFirst().getIndex());
  }


  /**
   * Truncate the whole log
   */
  @Test
  public void truncateAfterInclusiveWholeLog() {
    RaftLog log = new RaftLog(new SingleByteStateMachine(), new ArrayList<>(), MoreExecutors.newDirectExecutorService());
    log.appendEntries(0, 0, Arrays.asList(
        makeEntry(1, 1, 40),
        makeEntry(2, 1, 41),
        makeEntry(3, 1, 42)
    ));

    log.truncateLogAfterIndexInclusive(1L);
    assertTrue("We should no longer have any entries", log.logEntries.isEmpty());
  }


  /**
   * Revert our cluster configuration from the log.
   */
  @Test
  public void truncateAfterInclusiveRevertConfigFromLog() {
    RaftLog log = new RaftLog(new SingleByteStateMachine(), new ArrayList<>(), MoreExecutors.newDirectExecutorService());
    log.appendEntries(0, 0, Arrays.asList(
        makeConfigurationEntry(1, 1, Collections.singleton("A")),
        makeConfigurationEntry(2, 1, Collections.singleton("B")),
        makeConfigurationEntry(3, 1, Collections.singleton("C"))
        ));

    log.truncateLogAfterIndexInclusive(2L);
    assertEquals("We should now see configuration 'A'", Collections.singleton("A"), new HashSet<>(log.latestQuorumMembers));
    log.truncateLogAfterIndexInclusive(1L);
    assertTrue("We should now see our initial configuration", log.latestQuorumMembers.isEmpty());
  }


  /**
   * Revert our cluster configuration from a snapshot.
   */
  @Test
  public void truncateAfterInclusiveRevertConfigFromSnapshot() {
    RaftLog log = new RaftLog(new SingleByteStateMachine(), new ArrayList<>(), MoreExecutors.newDirectExecutorService());
    log.appendEntries(0, 0, Collections.singletonList(makeConfigurationEntry(1, 1, Collections.singleton("A"))));
    log.forceSnapshot();
    log.appendEntries(0, 0, Collections.singletonList(makeConfigurationEntry(2, 1, Collections.singleton("B"))));

    log.truncateLogAfterIndexInclusive(2L);
    assertEquals("We should now see configuration 'A' from the snapshot", Collections.singleton("A"), new HashSet<>(log.latestQuorumMembers));
  }


  /**
   * Revert our cluster configuration from the log.
   */
  @Test
  public void truncateAfterInclusiveRevertConfigFromLogOverSnapshot() {
    RaftLog log = new RaftLog(new SingleByteStateMachine(), new ArrayList<>(), MoreExecutors.newDirectExecutorService());
    log.appendEntries(0, 0, Collections.singletonList(makeConfigurationEntry(1, 1, Collections.singleton("A"))));
    log.forceSnapshot();
    log.appendEntries(0, 0, Arrays.asList(
        makeConfigurationEntry(2, 1, Collections.singleton("B")),
        makeConfigurationEntry(3, 1, Collections.singleton("C"))
    ));

    log.truncateLogAfterIndexInclusive(3L);
    assertEquals("We should now see configuration 'B' from the log", Collections.singleton("B"), new HashSet<>(log.latestQuorumMembers));
  }


  /**
   * A vintage Keenon test
   */
  @Test
  public void truncateAfterInclusive() {
    SingleByteStateMachine stateMachine = new SingleByteStateMachine();
    RaftLog log = new RaftLog(stateMachine, new ArrayList<>(), MoreExecutors.newDirectExecutorService());

    List<EloquentRaftProto.LogEntry> appendEntries1 = new ArrayList<>();
    appendEntries1.add(makeEntry(1, 1, 3));
    appendEntries1.add(makeEntry(2, 1, 2));
    appendEntries1.add(makeEntry(3, 1, 1));

    // Add the entries as uncommitted
    boolean success = log.appendEntries(0, 0, appendEntries1);
    assertTrue(success);
    assertEquals("Nothing should be committed yet", -1, stateMachine.value);

    // Check the entries are present
    verifyEntry(log, 1, 1, 3);
    verifyEntry(log, 2, 1, 2);
    verifyEntry(log, 3, 1, 1);

    log.truncateLogAfterIndexInclusive(2);

    assertEquals(1, log.logEntries.size());
    assertEquals(1, log.logEntries.getFirst().getIndex());
    verifyEntry(log, 1, 1, 3);
  }


  //
  // --------------------------------------------------------------------------
  // TRUNCATE LOG BEFORE
  // --------------------------------------------------------------------------
  //


  /**
   * Ensure that we can't truncate to negative indices
   */
  @Test
  public void truncateBeforeInclusiveNegativeIndex() {
    RaftLog log = new RaftLog(new SingleByteStateMachine(), new ArrayList<>(), MoreExecutors.newDirectExecutorService());
    log.appendEntries(0, 0, Collections.singletonList(makeEntry(1, 1, 42)));
    assertException(() -> log.truncateLogBeforeIndexInclusive(-1L), AssertionError.class);  // or, can ignore the error
    assertEquals("We should still have our entry", 1, log.logEntries.size());
  }


  /**
   * Ensure that we can't truncate an empty log.
   * This is a bit degenerate as a test, but it's a code path in the function.
   */
  @Test
  public void truncateBeforeInclusiveEmptyLog() {
    RaftLog log = new RaftLog(new SingleByteStateMachine(), new ArrayList<>(), MoreExecutors.newDirectExecutorService());
    log.truncateLogBeforeIndexInclusive(1L);
    assertEquals("The log should still be empty", 0, log.logEntries.size());
  }


  /**
   * Truncate some entries.
   */
  @Test
  public void truncateBeforeInclusiveSimple() {
    RaftLog log = new RaftLog(new SingleByteStateMachine(), new ArrayList<>(), MoreExecutors.newDirectExecutorService());
    log.appendEntries(0, 0, Arrays.asList(
        makeEntry(1, 1, 40),
        makeEntry(2, 1, 41),
        makeEntry(3, 1, 42)
    ));

    log.truncateLogBeforeIndexInclusive(2L);
    assertEquals("We should have 1 entries now", 1, log.logEntries.size());
    assertEquals("The entry should be at index 3", 3, log.logEntries.getFirst().getIndex());
  }


  /**
   * Truncate the whole log
   */
  @Test
  public void truncateBeforeInclusiveWholeLog() {
    RaftLog log = new RaftLog(new SingleByteStateMachine(), new ArrayList<>(), MoreExecutors.newDirectExecutorService());
    log.appendEntries(0, 0, Arrays.asList(
        makeEntry(1, 1, 40),
        makeEntry(2, 1, 41),
        makeEntry(3, 1, 42)
    ));

    log.truncateLogBeforeIndexInclusive(3L);
    assertTrue("We should no longer have any entries", log.logEntries.isEmpty());
  }


  /**
   * A vintage Keenon test
   */
  @Test
  public void testTruncateBeforeInclusive() {
    SingleByteStateMachine stateMachine = new SingleByteStateMachine();
    RaftLog log = new RaftLog(stateMachine, new ArrayList<>(), MoreExecutors.newDirectExecutorService());

    List<EloquentRaftProto.LogEntry> appendEntries1 = new ArrayList<>();
    appendEntries1.add(makeEntry(1, 1, 3));
    appendEntries1.add(makeEntry(2, 1, 2));
    appendEntries1.add(makeEntry(3, 1, 1));

    // Add the entries as uncommitted
    boolean success = log.appendEntries(0, 0, appendEntries1);
    assertTrue(success);
    assertEquals("Nothing should be committed yet", -1, stateMachine.value);

    // Check the entries are present
    verifyEntry(log, 1, 1, 3);
    verifyEntry(log, 2, 1, 2);
    verifyEntry(log, 3, 1, 1);

    log.truncateLogBeforeIndexInclusive(2);

    assertEquals(1, log.logEntries.size());
    assertEquals(3, log.logEntries.getFirst().getIndex());
    verifyEntry(log, 3, 1, 1);
  }


  //
  // --------------------------------------------------------------------------
  // MISC TODO(gabor) ORGANIZE ME
  // --------------------------------------------------------------------------
  //


  /**
   * Tests {@link RaftLog#getAllUncompressedEntries()}
   */
  @Test
  public void testGetAllUncompressedEntries() {
    RaftLog log = new RaftLog(new SingleByteStateMachine(), new ArrayList<>(), MoreExecutors.newDirectExecutorService());
    assertTrue("Should be able to append first entry", log.appendEntries(0, 1, Collections.singletonList(makeEntry(1, 1, 3))));
    assertEquals(new ArrayList<>(log.logEntries), new ArrayList<>(log.getAllUncompressedEntries()));
  }


  @Test
  public void updateQuorumOnUpdate() {
    RaftLog log = new RaftLog(new SingleByteStateMachine(), Arrays.asList("L", "A", "B"), MoreExecutors.newDirectExecutorService());
    assertTrue("Should be able to append configuration",
        log.appendEntries(0, 1, Collections.singletonList(makeConfigurationEntry(1, 1, Arrays.asList("L", "A")))));
    assertEquals("Log should have new entry", 1, log.logEntries.size());
    assertEquals("Quorum should have been updated", new HashSet<>(Arrays.asList("L", "A")), log.getQuorumMembers());
    assertTrue("Should be able to append new configuration",
        log.appendEntries(0, 1, Collections.singletonList(makeConfigurationEntry(2, 1, Arrays.asList("L", "A", "B")))));
    assertEquals("Quorum should have been updated", new HashSet<>(Arrays.asList("L", "A", "B")), log.getQuorumMembers());
  }


  @Test
  public void doubleWriteRoot() {
    SingleByteStateMachine stateMachine = new SingleByteStateMachine();
    RaftLog log = new RaftLog(stateMachine, new ArrayList<>(), MoreExecutors.newDirectExecutorService());

    List<EloquentRaftProto.LogEntry> appendEntries = new ArrayList<>();
    // Add a transition to the value 3
    appendEntries.add(makeEntry(1, 1, 3));
    appendEntries.add(makeEntry(2, 1, 4));

    // Add the entries as uncommitted
    boolean success = log.appendEntries(0, 0, appendEntries);
    assertTrue(success);
    assertEquals(2, log.logEntries.size());
    verifyEntry(log, 1, 1, 3);
    verifyEntry(log, 2, 1, 4);

    // Double add the entries, which should be a no-op
    success = log.appendEntries(0, 0, appendEntries);
    assertTrue(success);

    // Check the entry is present
    verifyEntry(log, 1, 1, 3);
    verifyEntry(log, 2, 1, 4);
    assertEquals(2, log.logEntries.size());
    assertFalse(log.getEntryAtIndex(3).isPresent());

    // Partially commit the entries
    synchronized (log) {
      log.setCommitIndex((long) 1, TimerUtils.mockableNow().toEpochMilli());
    }
    assertEquals("We should have applied our entry to our state machine", 3, stateMachine.value);

    // Commit the other entry
    synchronized (log) {
      log.setCommitIndex((long) 2, TimerUtils.mockableNow().toEpochMilli());
    }
    assertEquals("We should have applied our entry to our state machine", 4, stateMachine.value);
  }


  @Test
  public void simpleFailure() {
    SingleByteStateMachine stateMachine = new SingleByteStateMachine();
    RaftLog log = new RaftLog(stateMachine, new ArrayList<>(), MoreExecutors.newDirectExecutorService());

    List<EloquentRaftProto.LogEntry> appendEntries = new ArrayList<>();
    appendEntries.add(makeEntry(1, 1, 3));

    // Add the entries as uncommitted
    boolean success = log.appendEntries(0, 0, appendEntries);
    assertTrue(success);
    assertEquals("Nothing should be committed yet", -1, stateMachine.value);

    success = log.appendEntries(1, 2, new ArrayList<>()); // <- WRONG TERM
    assertFalse(success);
    assertEquals("Nothing should be committed yet", -1, stateMachine.value);

    // Check the entry is present
    verifyEntry(log, 1, 1, 3);

    // Commit the entries
    synchronized (log) {
      log.setCommitIndex((long) 1, TimerUtils.mockableNow().toEpochMilli());
    }
    assertEquals("We should have applied our entry to our state machine", 3, stateMachine.value);
  }


  @Test
  public void truncateOverwrite() {
    SingleByteStateMachine stateMachine = new SingleByteStateMachine();
    RaftLog log = new RaftLog(stateMachine, new ArrayList<>(), MoreExecutors.newDirectExecutorService());

    List<EloquentRaftProto.LogEntry> appendEntries1 = new ArrayList<>();
    appendEntries1.add(makeEntry(1, 1, 3));
    appendEntries1.add(makeEntry(2, 1, 2));
    appendEntries1.add(makeEntry(3, 1, 1));

    // Add the entries as uncommitted
    boolean success = log.appendEntries(0, 0, appendEntries1);
    assertTrue(success);
    assertEquals("Nothing should be committed yet", -1, stateMachine.value);

    // Check the entries are present
    verifyEntry(log, 1, 1, 3);
    verifyEntry(log, 2, 1, 2);
    verifyEntry(log, 3, 1, 1);

    List<EloquentRaftProto.LogEntry> appendEntries2 = new ArrayList<>();
    appendEntries2.add(makeEntry(2, 2, 5));
    appendEntries2.add(makeEntry(3, 2, 6));

    // Add the entries, which should overwrite previous entries
    success = log.appendEntries(1, 1, appendEntries2);
    assertTrue(success);
    assertEquals("Nothing should be committed yet", -1, stateMachine.value);

    // Check the entries are present
    verifyEntry(log, 1, 1, 3);
    verifyEntry(log, 2, 2, 5);
    verifyEntry(log, 3, 2, 6);

    // test getEntriesSinceInclusive()
    Optional<List<EloquentRaftProto.LogEntry>> entries = log.getEntriesSinceInclusive(1);
    assertTrue(entries.isPresent());
    assertEquals(3, entries.get().size());
    assertEquals(1, entries.get().get(0).getIndex());
    entries = log.getEntriesSinceInclusive(3);
    assertTrue(entries.isPresent());
    assertEquals(1, entries.get().size());

    // Commit the entries
    synchronized (log) {
      log.setCommitIndex((long) 3, TimerUtils.mockableNow().toEpochMilli());
    }
    assertEquals("We should have applied our entry to our state machine", 6, stateMachine.value);
  }


  @Test
  public void doubleTruncateOverwrite() {
    SingleByteStateMachine stateMachine = new SingleByteStateMachine();
    RaftLog log = new RaftLog(stateMachine, new ArrayList<>(), MoreExecutors.newDirectExecutorService());

    List<EloquentRaftProto.LogEntry> appendEntries1 = new ArrayList<>();
    appendEntries1.add(makeEntry(1, 1, 3));
    appendEntries1.add(makeEntry(2, 1, 2));
    appendEntries1.add(makeEntry(3, 1, 1));

    // Add the entries as uncommitted
    boolean success = log.appendEntries(0, 0, appendEntries1);
    assertTrue(success);
    assertEquals("Nothing should be committed yet", -1, stateMachine.value);

    // Check the entries are present
    verifyEntry(log, 1, 1, 3);
    verifyEntry(log, 2, 1, 2);
    verifyEntry(log, 3, 1, 1);

    List<EloquentRaftProto.LogEntry> appendEntries2 = new ArrayList<>();
    appendEntries2.add(makeEntry(2, 2, 5));
    appendEntries2.add(makeEntry(3, 2, 6));

    // Add the entries, which should overwrite previous entries
    success = log.appendEntries(1, 1, appendEntries2);
    assertTrue(success);
    assertEquals("Nothing should be committed yet", -1, stateMachine.value);

    // Check the entries are present
    verifyEntry(log, 1, 1, 3);
    verifyEntry(log, 2, 2, 5);
    verifyEntry(log, 3, 2, 6);
    assertEquals(3, log.logEntries.size());
    assertFalse(log.getEntryAtIndex(4).isPresent());
    assertFalse(log.getEntryAtIndex(5).isPresent());

    // Add the entries again, which should be a no-op
    success = log.appendEntries(1, 1, appendEntries2);
    assertTrue(success);
    assertEquals("Nothing should be committed yet", -1, stateMachine.value);

    // Check the entries are present
    verifyEntry(log, 1, 1, 3);
    verifyEntry(log, 2, 2, 5);
    verifyEntry(log, 3, 2, 6);
    assertEquals(3, log.logEntries.size());
    assertFalse(log.getEntryAtIndex(4).isPresent());
    assertFalse(log.getEntryAtIndex(5).isPresent());

    // Commit the entries
    synchronized (log) {
      log.setCommitIndex((long) 3, TimerUtils.mockableNow().toEpochMilli());
    }
    assertEquals("We should have applied our entry to our state machine", 6, stateMachine.value);
  }


  @Test
  public void failedOverwrite() {
    SingleByteStateMachine stateMachine = new SingleByteStateMachine();
    RaftLog log = new RaftLog(stateMachine, new ArrayList<>(), MoreExecutors.newDirectExecutorService());

    List<EloquentRaftProto.LogEntry> appendEntries1 = new ArrayList<>();
    appendEntries1.add(makeEntry(1, 1, 3));
    appendEntries1.add(makeEntry(2, 1, 2));
    appendEntries1.add(makeEntry(3, 1, 1));

    // Add the entries as uncommitted
    boolean success = log.appendEntries(0, 0, appendEntries1);
    assertTrue(success);
    assertEquals("Nothing should be committed yet", -1, stateMachine.value);

    // Check the entries are present
    verifyEntry(log, 1, 1, 3);
    verifyEntry(log, 2, 1, 2);
    verifyEntry(log, 3, 1, 1);

    // Add the entries, which should overwrite previous entries
    success = log.appendEntries(1, 2, appendEntries1); // <- AN INCORRECT TERM
    assertFalse(success);
    assertEquals("Nothing should be committed yet", -1, stateMachine.value);

    // Check the entries are present
    verifyEntry(log, 1, 1, 3);
    verifyEntry(log, 2, 1, 2);
    verifyEntry(log, 3, 1, 1);

    // Commit the entries
    synchronized (log) {
      log.setCommitIndex((long) 3, TimerUtils.mockableNow().toEpochMilli());
    }
    assertEquals("We should have applied our entry to our state machine", 1, stateMachine.value);
  }

  @Test
  public void testClusterMembershipChanges() {
    SingleByteStateMachine stateMachine = new SingleByteStateMachine();

    Set<String> configuration0 = new HashSet<>();
    configuration0.add("test1");

    Set<String> configuration1 = new HashSet<>();
    configuration1.add("test1");

    Set<String> configuration2 = new HashSet<>();
    configuration2.add("test1");
    configuration2.add("test2");

    RaftLog log = new RaftLog(stateMachine, configuration0, MoreExecutors.newDirectExecutorService());

    List<EloquentRaftProto.LogEntry> appendEntries1 = new ArrayList<>();
    appendEntries1.add(makeEntry(1, 1, 3));
    appendEntries1.add(makeConfigurationEntry(2, 1, configuration1));
    appendEntries1.add(makeConfigurationEntry(3, 1, configuration2));

    assertEquals(configuration0, log.committedQuorumMembers);
    assertEquals(configuration0, log.latestQuorumMembers);

    // Add the entries as uncommitted
    boolean success = log.appendEntries(0, 0, appendEntries1);
    assertTrue(success);
    assertEquals("Nothing should be committed yet", -1, stateMachine.value);

    // Check the entries are present
    verifyEntry(log, 1, 1, 3);
    verifyConfigurationEntry(log, 2, 1, configuration1);
    verifyConfigurationEntry(log, 3, 1, configuration2);

    assertEquals(configuration0, log.committedQuorumMembers);
    assertEquals(configuration2, log.latestQuorumMembers);

    synchronized (log) {
      log.setCommitIndex((long) 2, TimerUtils.mockableNow().toEpochMilli());
    }

    assertEquals(configuration1, log.committedQuorumMembers);
    assertEquals(configuration2, log.latestQuorumMembers);

    synchronized (log) {
      log.setCommitIndex((long) 3, TimerUtils.mockableNow().toEpochMilli());
    }

    assertEquals(configuration2, log.committedQuorumMembers);
    assertEquals(configuration2, log.latestQuorumMembers);
  }

  @Test
  public void testGetPreviousEntryTerm() {
    SingleByteStateMachine stateMachine = new SingleByteStateMachine();
    RaftLog log = new RaftLog(stateMachine, new ArrayList<>(), MoreExecutors.newDirectExecutorService());

    List<EloquentRaftProto.LogEntry> appendEntries1 = new ArrayList<>();
    appendEntries1.add(makeEntry(1, 1, 3));
    appendEntries1.add(makeEntry(2, 1, 2));
    appendEntries1.add(makeEntry(3, 1, 1));

    Optional<Long> previousEntry = log.getPreviousEntryTerm(0);
    assertTrue(previousEntry.isPresent());
    assertEquals(0, (long)previousEntry.get());

    // Add the entries as uncommitted
    boolean success = log.appendEntries(0, 0, appendEntries1);
    assertTrue(success);
    assertEquals("Nothing should be committed yet", -1, stateMachine.value);

    // Check the entries are present
    verifyEntry(log, 1, 1, 3);
    verifyEntry(log, 2, 1, 2);
    verifyEntry(log, 3, 1, 1);

    previousEntry = log.getPreviousEntryTerm(0);
    assertTrue(previousEntry.isPresent());
    assertEquals(0, (long)previousEntry.get());
    previousEntry = log.getPreviousEntryTerm(1);
    assertTrue(previousEntry.isPresent());
    assertEquals(1, (long)previousEntry.get());
    previousEntry = log.getPreviousEntryTerm(2);
    assertTrue(previousEntry.isPresent());
    assertEquals(1, (long)previousEntry.get());
    previousEntry = log.getPreviousEntryTerm(3);
    assertTrue(previousEntry.isPresent());
    assertEquals(1, (long)previousEntry.get());

    synchronized (log) {
      log.setCommitIndex((long) 2, TimerUtils.mockableNow().toEpochMilli());
    }
    log.forceSnapshot();

    previousEntry = log.getPreviousEntryTerm(0);
    assertTrue(previousEntry.isPresent());
    previousEntry = log.getPreviousEntryTerm(1);
    assertFalse(previousEntry.isPresent());
    previousEntry = log.getPreviousEntryTerm(2);
    assertTrue(previousEntry.isPresent());
    assertEquals(1, (long)previousEntry.get());
    previousEntry = log.getPreviousEntryTerm(3);
    assertTrue(previousEntry.isPresent());
    assertEquals(1, (long)previousEntry.get());
  }


  @Test
  public void testGetSafeToChangeMembership() {
    SingleByteStateMachine stateMachine = new SingleByteStateMachine();

    Set<String> configuration0 = new HashSet<>();
    configuration0.add("test1");

    Set<String> configuration1 = new HashSet<>();
    configuration1.add("test1");

    Set<String> configuration2 = new HashSet<>();
    configuration2.add("test1");
    configuration2.add("test2");

    RaftLog log = new RaftLog(stateMachine, configuration0, MoreExecutors.newDirectExecutorService());

    List<EloquentRaftProto.LogEntry> appendEntries1 = new ArrayList<>();
    appendEntries1.add(makeEntry(1, 1, 3));
    appendEntries1.add(makeConfigurationEntry(2, 1, configuration1));

    List<EloquentRaftProto.LogEntry> appendEntries2 = new ArrayList<>();
    appendEntries2.add(makeConfigurationEntry(3, 2, configuration2));

    assertEquals(configuration0, log.committedQuorumMembers);
    assertEquals(configuration0, log.latestQuorumMembers);

    assertTrue(log.getSafeToChangeMembership());

    // Add the entries as uncommitted
    boolean success = log.appendEntries(0, 0, appendEntries1);
    assertTrue(success);
    assertEquals("Nothing should be committed yet", -1, stateMachine.value);

    // we have uncommitted membership changes
    assertFalse(log.getSafeToChangeMembership());

    // Check the entries are present
    verifyEntry(log, 1, 1, 3);
    verifyConfigurationEntry(log, 2, 1, configuration1);

    assertEquals(configuration0, log.committedQuorumMembers);
    assertEquals(configuration1, log.latestQuorumMembers);

    synchronized (log) {
      log.setCommitIndex((long) 2, TimerUtils.mockableNow().toEpochMilli());
    }

    // we have no uncommitted membership changes
    assertTrue(log.getSafeToChangeMembership());

    assertEquals(configuration1, log.committedQuorumMembers);
    assertEquals(configuration1, log.latestQuorumMembers);

    success = log.appendEntries(2, 1, appendEntries2);
    assertTrue(success);

    // we have uncommitted membership changes
    assertFalse(log.getSafeToChangeMembership());

    synchronized (log) {
      log.setCommitIndex((long) 3, TimerUtils.mockableNow().toEpochMilli());
    }

    // we have no uncommitted membership changes
    assertTrue(log.getSafeToChangeMembership());

    assertEquals(configuration2, log.committedQuorumMembers);
    assertEquals(configuration2, log.latestQuorumMembers);
  }


  @Test
  public void getLastEntryIndex() {
    RaftLog log = new RaftLog(new KeyValueStateMachine("name"), Collections.singletonList("name"), MoreExecutors.newDirectExecutorService());
    assertEquals(0, log.getLastEntryTerm());
    assertEquals(0, log.getLastEntryIndex());
    // Add the entries as uncommitted
    assertTrue("Could not append to log", log.appendEntries(0, 0, Collections.singletonList(
        makeEntry(1, 1, 42)
    )));
    assertEquals(1, log.getLastEntryTerm());
    assertEquals(1, log.getLastEntryIndex());
  }


  @Test
  public void equalsHashCode() {
    RaftLog log = new RaftLog(new KeyValueStateMachine("name"), Collections.singletonList("name"), MoreExecutors.newDirectExecutorService());
    RaftLog copy = new RaftLog(new KeyValueStateMachine("name"), Collections.singletonList("name"), MoreExecutors.newDirectExecutorService());
    assertEquals(log, log);
    assertEquals(log, copy);
    assertEquals(log.hashCode(), log.hashCode());
    assertEquals(log.hashCode(), copy.hashCode());
  }


  @Test
  public void copy() {
    RaftLog log = new RaftLog(new KeyValueStateMachine("name"), Collections.singletonList("name"), MoreExecutors.newDirectExecutorService());
    RaftLog copy = log.copy();
    assertEquals(log, log);
    assertEquals(log, copy);
    assertEquals(log.hashCode(), log.hashCode());
    assertEquals(log.hashCode(), copy.hashCode());
  }


  /**
   * Test {@link RaftLog#lastConfigurationEntryLocation()}
   */
  @Test
  public void lastConfigurationEntryLocation() {
    RaftLog log = new RaftLog(new SingleByteStateMachine(), new ArrayList<>(), MoreExecutors.newDirectExecutorService());

    // Check getting configuration entries
    assertEquals("Should have no last entry location", Optional.empty(), log.lastConfigurationEntryLocation());
    assertTrue("Should be able to append first entry",
        log.appendEntries(0, 1, Collections.singletonList(makeConfigurationEntry(1, 1, Collections.singletonList("L")))));
    assertEquals(Optional.of(new RaftLogEntryLocation(1, 1)), log.lastConfigurationEntryLocation());
    assertTrue("Should be able to append second entry",
        log.appendEntries(1, 1, Collections.singletonList(makeConfigurationEntry(2, 1, Arrays.asList("L", "A")))));
    assertEquals(Optional.of(new RaftLogEntryLocation(2, 1)), log.lastConfigurationEntryLocation());

    // Force a snapshot
    synchronized (log) {
      log.setCommitIndex((long) 2, TimerUtils.mockableNow().toEpochMilli());
    }
    log.forceSnapshot();

    // Get entries after the snapshot
    assertEquals("Should not be searching into a snapshot", Optional.empty(), log.lastConfigurationEntryLocation());
    assertTrue("Should be able to append a third entry",
        log.appendEntries(2, 1, Collections.singletonList(makeConfigurationEntry(3, 1, Arrays.asList("L", "A", "B")))));
    assertEquals(Optional.of(new RaftLogEntryLocation(3, 1)), log.lastConfigurationEntryLocation());
  }


  /**
   * Test {@link RaftLog#unsafeBootstrapQuorum(Collection)}
   */
  @Test
  public void unsafeBootstrapQuorum() {
    RaftLog log = new RaftLog(new SingleByteStateMachine(), new ArrayList<>(), MoreExecutors.newDirectExecutorService());
    log.unsafeBootstrapQuorum(Collections.singletonList("L"));
    assertEquals("Should be able to bootstrap a new node", Collections.singleton("L"), log.getQuorumMembers());
  }


  /**
   * Test {@link RaftLog#unsafeBootstrapQuorum(Collection)}
   */
  @Test
  public void unsafeBootstrapQuorumFailsIfQuorumExists() {
    RaftLog log = new RaftLog(new SingleByteStateMachine(), Arrays.asList("A", "B"), MoreExecutors.newDirectExecutorService());
    assertException(() -> log.unsafeBootstrapQuorum(Collections.singletonList("L")), AssertionError.class);
  }

  ////////////////////////////////////////////////////////////////////////////
  // Keenon Tests as of Jan 18, 2018 (may be redundant with test above)
  ////////////////////////////////////////////////////////////////////////////

  /**
   * Ensure that we can append entries that agree initially, and then disagree with the log.
   */
  @Test
  public void appendTruncateAndOverwriteWithInitialAgreement() {
    SingleByteStateMachine stateMachine = new SingleByteStateMachine();
    RaftLog log = new RaftLog(stateMachine, new ArrayList<>(), MoreExecutors.newDirectExecutorService());
    List<EloquentRaftProto.LogEntry> appendEntries = Arrays.asList(
        makeEntry(1, 1, 3),
        makeEntry(2, 1, 4),
        makeEntry(3, 1, 5),
        makeEntry(4, 2, 1),
        makeEntry(5, 2, 3)
    );
    // Simple add
    boolean success = log.appendEntries(0, 0, appendEntries);
    assertTrue(success);
    assertEquals("Nothing should be committed yet", -1, stateMachine.value);
    assertEquals("There should be 5 log entries", 5, log.logEntries.size());

    // Cannot add from older term
    appendEntries = Arrays.asList(
        makeEntry(2, 1, 4),
        makeEntry(3, 1, 5),
        makeEntry(4, 3, 6),
        makeEntry(5, 3, 7)
    );
    success = log.appendEntries(1, 1, appendEntries);
    assertTrue("Should be able to write to an entry with an older log", success);

    List<EloquentRaftProto.LogEntry> expectedEntries = Arrays.asList(
        makeEntry(1, 1, 3),
        makeEntry(2, 1, 4), // <- append entries started here
        makeEntry(3, 1, 5),
        makeEntry(4, 3, 6), // <- changes start here
        makeEntry(5, 3, 7)
    );
    assertEquals("Entries should have truncated and overwritten properly", expectedEntries, new ArrayList<>(log.logEntries));
  }

  /**
   * Ensure that we can append entries that agree fully, and extend the log forward
   */
  @Test
  public void appendTruncateAndOverwriteWithFullAgreement() {
    SingleByteStateMachine stateMachine = new SingleByteStateMachine();
    RaftLog log = new RaftLog(stateMachine, new ArrayList<>(), MoreExecutors.newDirectExecutorService());
    List<EloquentRaftProto.LogEntry> appendEntries = Arrays.asList(
        makeEntry(1, 1, 3),
        makeEntry(2, 1, 4),
        makeEntry(3, 1, 5),
        makeEntry(4, 2, 1),
        makeEntry(5, 2, 3)
    );
    // Simple add
    boolean success = log.appendEntries(0, 0, appendEntries);
    assertTrue(success);
    assertEquals("Nothing should be committed yet", -1, stateMachine.value);
    assertEquals("There should be 5 log entries", 5, log.logEntries.size());

    // Cannot add from older term
    appendEntries = Arrays.asList(
        makeEntry(2, 1, 4),
        makeEntry(3, 1, 5),
        makeEntry(4, 2, 1),
        makeEntry(5, 2, 3),
        makeEntry(6, 2, 5),
        makeEntry(7, 2, 4)
    );
    success = log.appendEntries(1, 1, appendEntries);
    assertTrue("Should be able to write to an entry with an older log", success);

    List<EloquentRaftProto.LogEntry> expectedEntries = Arrays.asList(
        makeEntry(1, 1, 3),
        makeEntry(2, 1, 4), // <- append entries started here
        makeEntry(3, 1, 5),
        makeEntry(4, 2, 1),
        makeEntry(5, 2, 3),
        makeEntry(6, 2, 5),
        makeEntry(7, 2, 4)
    );
    assertEquals("Entries should have truncated and overwritten properly", expectedEntries, new ArrayList<>(log.logEntries));
  }


  /**
   * Ensure that we can append entries that agree initially, and then disagree with the log.
   */
  @Test
  public void truncateOverwriteOnSnapshot() {
    SingleByteStateMachine stateMachine = new SingleByteStateMachine();
    RaftLog log = new RaftLog(stateMachine, new ArrayList<>(), MoreExecutors.newDirectExecutorService());
    List<EloquentRaftProto.LogEntry> appendEntries = Arrays.asList(
        makeEntry(1, 1, 3),
        makeEntry(2, 1, 4),
        makeEntry(3, 1, 5),
        makeEntry(4, 2, 1),
        makeEntry(5, 2, 3)
    );
    // Simple add
    boolean success = log.appendEntries(0, 0, appendEntries);
    assertTrue(success);
    assertEquals("Nothing should be committed yet", -1, stateMachine.value);
    assertFalse("There should not be a snapshot", log.snapshot.isPresent());
    assertEquals("There should be 5 log entries", 5, log.logEntries.size());

    // Snapshot until the 3rd index
    log.setCommitIndex(3, 0);
    log.forceSnapshot();
    assertTrue("There should be a snapshot", log.snapshot.isPresent());
    assertEquals("There should be 2 uncompacted log entries", 2, log.logEntries.size());

    // Cannot add from older term
    appendEntries = Arrays.asList(
        makeEntry(4, 3, 6),
        makeEntry(5, 3, 7)
    );
    success = log.appendEntries(3, 1, appendEntries);
    assertTrue("Should be able to write to an entry with an older log", success);

    List<EloquentRaftProto.LogEntry> expectedEntries = Arrays.asList(
        makeEntry(4, 3, 6), // <- changes start here
        makeEntry(5, 3, 7)
    );
    assertEquals("Entries should have truncated and overwritten properly", expectedEntries, new ArrayList<>(log.logEntries));
  }


  /**
   * Ensure that we can install a snapshot that totally nukes a list of entries by disagreeing with them
   */
  @Test
  public void installSnapshotOverwrite() {
    SingleByteStateMachine stateMachine = new SingleByteStateMachine();
    RaftLog log = new RaftLog(stateMachine, new ArrayList<>(), MoreExecutors.newDirectExecutorService());
    List<EloquentRaftProto.LogEntry> appendEntries = Arrays.asList(
        makeEntry(1, 1, 3),
        makeEntry(2, 1, 4),
        makeEntry(3, 1, 5),
        makeEntry(4, 2, 1),
        makeEntry(5, 2, 3)
    );
    // Simple add
    boolean success = log.appendEntries(0, 0, appendEntries);
    assertTrue(success);
    assertEquals("Nothing should be committed yet", -1, stateMachine.value);
    assertFalse("There should not be a snapshot", log.snapshot.isPresent());
    assertEquals("There should be 5 log entries", 5, log.logEntries.size());

    RaftLog.Snapshot snapshot = new RaftLog.Snapshot(new SingleByteStateMachine(3).serialize(), 3, 3, new ArrayList<>());

    assertTrue(log.installSnapshot(snapshot, 0));
    assertEquals("We should have committed up until at least the last snapshot index", 3, log.commitIndex);

    // Snapshot until the 3rd index
    assertTrue("There should be a snapshot", log.snapshot.isPresent());
    assertEquals("There should be no un-compacted log entries", 0, log.logEntries.size());
  }


  /**
   * Ensure that we can install a snapshot that subsumes part of our log, even while we've already committed beyond the
   * end of it.
   */
  @Test
  public void installSnapshotTruncateAlreadyCommitted() {
    SingleByteStateMachine stateMachine = new SingleByteStateMachine();
    RaftLog log = new RaftLog(stateMachine, new ArrayList<>(), MoreExecutors.newDirectExecutorService());
    List<EloquentRaftProto.LogEntry> appendEntries = Arrays.asList(
        makeEntry(1, 1, 3),
        makeEntry(2, 1, 4),
        makeEntry(3, 1, 5),
        makeEntry(4, 2, 1),
        makeEntry(5, 2, 3)
    );
    // Simple add
    boolean success = log.appendEntries(0, 0, appendEntries);
    assertTrue(success);
    assertEquals("Nothing should be committed yet", -1, stateMachine.value);
    assertFalse("There should not be a snapshot", log.snapshot.isPresent());
    assertEquals("There should be 5 log entries", 5, log.logEntries.size());

    long lastIndex = 3;
    long lastTerm = 1;
    RaftLog.Snapshot snapshot = new RaftLog.Snapshot(new byte[]{3}, lastIndex, lastTerm, new ArrayList<>());

    log.setCommitIndex(5, 0);

    assertTrue(log.installSnapshot(snapshot, 0));
    assertEquals("Commit index should have been preserved", 5, log.commitIndex);

    // Snapshot until the 3rd index
    assertTrue("There should be a snapshot", log.snapshot.isPresent());
    assertEquals("There should be 2 uncompacted log entries", 2, log.logEntries.size());
    List<EloquentRaftProto.LogEntry> expectedEntries = Arrays.asList(
        makeEntry(4, 2, 1),
        makeEntry(5, 2, 3)
    );
    assertEquals("Entries should have truncated properly", expectedEntries, new ArrayList<>(log.logEntries));
  }
}