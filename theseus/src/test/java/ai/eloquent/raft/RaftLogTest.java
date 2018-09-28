package ai.eloquent.raft;

import ai.eloquent.util.TimerUtils;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

/**
 * This unit tests a lot of the important core logic around appending to the logs and compacting the logs, in addition
 * to dealing with snapshots.
 */
@SuppressWarnings({"ConstantConditions", "SynchronizationOnLocalVariableOrMethodParameter", "unused", "UnusedAssignment"})
public class RaftLogTest {


  /** Assert that the given runnable should throw the given exception (or something compatible). */
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
  private EloquentRaftProto.LogEntry makeConfigurationEntry(long index, long term, Collection<String> clusterMembership) {
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
   * Tests {@link RaftLog#getAllUncompressedEntries()}
   */
  @Test
  public void testGetAllUncompressedEntries() {
    RaftLog log = new RaftLog(new SingleByteStateMachine(), new ArrayList<>(), MoreExecutors.newDirectExecutorService());
    assertTrue("Should be able to append first entry", log.appendEntries(0, 1, Collections.singletonList(makeEntry(1, 1, 3))));
    assertEquals(new ArrayList<>(log.logEntries), new ArrayList<>(log.getAllUncompressedEntries()));
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
    log.commitIndex = RaftLog.COMPACTION_LIMIT;  // commit, so we can snapshot
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


  @Test
  public void testTruncateAfterInclusive() {
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
    CompletableFuture<Boolean> commit1 = log.createCommitFuture(new RaftLogEntryLocation(1, 1));  // just to cover this variant of the function too
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
  @SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
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
  @SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
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
  @SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
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
  @SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
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

    long lastIndex = 3;
    long lastTerm = 2;
    RaftLog.Snapshot snapshot = new RaftLog.Snapshot(new byte[]{3}, lastIndex, lastTerm, new ArrayList<>());

    assertTrue(log.installSnapshot(snapshot, 0));
    assertEquals("We should have committed up until at least the last snapshot index", lastIndex, log.commitIndex);

    // Snapshot until the 3rd index
    assertTrue("There should be a snapshot", log.snapshot.isPresent());
    assertEquals("There should be no uncompacted log entries", 0, log.logEntries.size());
  }


  /**
   * Ensure that we can install a snapshot that subsumes part of our log, even while we've already committed beyond the
   * end of it.
   */
  @SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
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