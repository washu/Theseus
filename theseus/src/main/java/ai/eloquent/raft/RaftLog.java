package ai.eloquent.raft;

import ai.eloquent.monitoring.Prometheus;
import ai.eloquent.util.FunctionalUtils;
import ai.eloquent.util.TimerUtils;
import com.sun.management.GcInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

/**
 * There turns out to be an annoying amount of book-keeping around the logging abstraction in Raft, since it can be
 * compacted, truncated, rewritten, committed, and queried in all sorts of strange ways. That made it an excellent
 * candidate to become a class and get tested in isolation.
 */
@SuppressWarnings("OptionalIsPresent")
public class RaftLog {
  /**
   * An SLF4J Logger for this class.
   */
  @SuppressWarnings("unused")
  private static final Logger log = LoggerFactory.getLogger(RaftLog.class);

  /**
   *   <li>0: trace</li>
   *   <li>1: debug</li>
   *   <li>2: info</li> (Default)
   *   <li>3: warn</li>
   *   <li>4: error</li>
   *   <li>99: off</li>
   */
  private static int minLogLevel = 2;

  /**
   * Explicitly set the log level. These are:
   *
   * <ul>
   *   <li>0: trace</li>
   *   <li>1: debug</li>
   *   <li>2: info</li>
   *   <li>3: warn</li>
   *   <li>4: error</li>
   * </ul>
   */
  public static void setLevel(int level) {
    minLogLevel = level;
  }

  /**
   * Explicitly set the log level. These are:
   *
   * <ul>
   *   <li>trace</li>
   *   <li>debug</li>
   *   <li>info</li>
   *   <li>warn</li>
   *   <li>error</li>
   *   <li>off</li>
   * </ul>
   *
   * On an invalid level, we default to 'info'
   */
  public static void setLevel(String level) {
    switch (level.toLowerCase()) {
      case "trace":
      case "0":
        minLogLevel = 0;
        break;
      case "debug":
      case "1":
        minLogLevel = 1;
        break;
      case "info":
      case "2":
        minLogLevel = 2;
        break;
      case "warn":
      case "3":
        minLogLevel = 3;
        break;
      case "error":
      case "4":
        minLogLevel = 4;
        break;
      case "off":
      case "5":
        minLogLevel = 99;
        break;
      default:
        minLogLevel = 2;
    }
  }

  /**
   * Get the current log level.
   */
  public static int level() {
    return minLogLevel;
  }

  /**
   * The number of log entries to keep in the log before compacting into a snapshot.
   */
  public static final int COMPACTION_LIMIT = 1024;

  /**
   * A pool for completing commit futures.
   */
  private final ExecutorService pool;


  /**
   * This holds a listener for when a commit has reached a certain index.
   */
  private static class CommitFuture {
    public final long index;
    public final long term;
    public final Consumer<Boolean> complete;

    public CommitFuture(long index, long term, Consumer<Boolean> complete) {
      this.index = index;
      this.term = term;
      this.complete = complete;
    }
  }


  /**
   * This is used in truncating the logs when they grow too long.
   */
  public static class Snapshot {
    byte[] serializedStateMachine;

    // the snapshot replaces all entries up through and including this index
    long lastIndex;

    // term of lastIndex
    long lastTerm;

    // latest cluster configuration as of lastIndex
    Set<String> lastClusterMembership;

    public Snapshot(byte[] serializedStateMachine, long lastIndex, long lastTerm, Collection<String> lastClusterMembership) {
      this.serializedStateMachine = serializedStateMachine;
      this.lastIndex = lastIndex;
      this.lastTerm = lastTerm;
      this.lastClusterMembership = FunctionalUtils.immutable(new HashSet<>(lastClusterMembership));
    }
  }


  /**
   * The index (inclusive) up to which the log has been committed.
   */
  long commitIndex;

  /**
   * CORE: This is a deque, so that we can support compaction as the log grows long. The first entry is the oldest
   * entry, the last entry is the newest entry. It is compacted by clipping entries from the beginning. It grows by
   * appending to the end.
   */
  final Deque<EloquentRaftProto.LogEntry> logEntries = new ArrayDeque<>(COMPACTION_LIMIT);

  /**
   * CORE: This is the state machine reference that the logs refer to.
   */
  public final RaftStateMachine stateMachine;

  /**
   * CORE: This is used by leaders to know when a commit has been replicated and it's safe to respond to a caller
   */
  final List<CommitFuture> commitFutures = new ArrayList<>();

  /**
   * LOG COMPACTION: This is used to backstop the log when we compact committed entries.
   */
  public Optional<Snapshot> snapshot = Optional.empty();

  /**
   * MEMBERSHIP CHANGES: This is used to keep track of the current membership of the cluster.
   */
  final Set<String> latestQuorumMembers = new HashSet<>();

  /**
   * LOG COMPACTION + MEMBERSHIP CHANGES: This is used for log compaction, so we can track membership at the time of a
   * snapshot of the state.
   */
  public final Set<String> committedQuorumMembers = new HashSet<>();

  /**
   * An immutable set denoting the initial configuration, in the corner case that we've completely
   * deleted a log and need to revert to this state.
   */
  private Set<String> initialQuorumMembers;

  /**
   * Metrics on Raft timing.
   */
  private static Object summaryTiming = Prometheus.summaryBuild("raft_log", "Timing on the Raft log methods");


  /** Create a log from a state machine and initial configuration. */
  public RaftLog(RaftStateMachine stateMachine, Collection<String> initialConfiguration, ExecutorService pool) {
    this.stateMachine = stateMachine;
    this.commitIndex = 0;
    this.initialQuorumMembers = FunctionalUtils.immutable(new HashSet<>(initialConfiguration));
    this.latestQuorumMembers.addAll(initialConfiguration);
    this.committedQuorumMembers.addAll(initialConfiguration);
    this.pool = pool;
  }


  /**
   * Assert that the given operation was fast enough that we're likely not waiting on a lock somewhere.
   *
   * @param description The action we're performing.
   * @param summaryStartTime The time the action started.
   *
   * @return Always true, so we can be put into asserts
   */
  @SuppressWarnings("Duplicates")
  private boolean fast(String description, Object summaryStartTime) {
    long duration =  (long) (Prometheus.observeDuration(summaryStartTime) * 1000);
    if (duration > 5) {
      long lastGcTime = -1L;
      try {
        for (GarbageCollectorMXBean gcBean : ManagementFactory.getGarbageCollectorMXBeans()) {
          com.sun.management.GarbageCollectorMXBean sunGcBean = (com.sun.management.GarbageCollectorMXBean) gcBean;
          GcInfo lastGcInfo = sunGcBean.getLastGcInfo();
          if (lastGcInfo != null) {
            lastGcTime = lastGcInfo.getStartTime();
          }
        }
      } catch (Throwable t) {
        log.warn("Could not get GC info -- are you running on a non-Sun JVM?");
      }
      boolean interruptedByGC = false;
      if (lastGcTime > System.currentTimeMillis() - duration && lastGcTime < System.currentTimeMillis()) {
        interruptedByGC = true;
      }
      if (duration > 1000) {
        log.warn("{} took {};  interrupted_by_gc={}", description, TimerUtils.formatTimeDifference(duration), interruptedByGC);
      } else {
        log.info("{} took {};  interrupted_by_gc={}", description, TimerUtils.formatTimeDifference(duration), interruptedByGC);
      }
    }
    return true;
  }


  /**
   * Create a medium-deep copy of this log.
   * The state machine is not copied, and the futures are not copied, but the cluster configuration is.
   */
  public RaftLog copy() {
    Object timerStart = Prometheus.startTimer(summaryTiming);
    try {
      assertConsistency();
      RaftLog copy = new RaftLog(this.stateMachine, Collections.emptyList(), this.pool);
      copy.latestQuorumMembers.addAll(this.latestQuorumMembers);
      copy.committedQuorumMembers.addAll(this.committedQuorumMembers);
      copy.logEntries.addAll(this.logEntries);
      copy.commitFutures.addAll(this.commitFutures);
      copy.commitIndex = this.commitIndex;
      copy.snapshot = this.snapshot;
      assert copy.equals(this) : "Copy did not copy the log state entirely";
      return copy;
    } finally {
      assert fast("copy", timerStart);
    }
  }


  /**
   * This is an <b>extremely</b> unsafe method that clobbers the current configuration with a new one, without
   * consulting the log. This is only suitable for bootstrapping.
   *
   * @param initialConfiguration The configuration to force into the system -- this also overwrites the log's
   *                             native {@link #initialQuorumMembers}.
   */
  void unsafeBootstrapQuorum(Collection<String> initialConfiguration) {
    Object timerStart = Prometheus.startTimer(summaryTiming);
    assertConsistency();
    try {
      assert getQuorumMembers().isEmpty() : "Cannot bootstrap from an existing quorum";
      this.initialQuorumMembers = FunctionalUtils.immutable(new HashSet<>(initialConfiguration));
      this.latestQuorumMembers.clear();
      this.latestQuorumMembers.addAll(initialConfiguration);
      this.snapshot = Optional.empty();
      this.logEntries.clear();
      this.commitIndex = 0;
    } finally {
      assert fast("unsafeBootstrapQuorum", timerStart);
      assertConsistency();
    }
  }


  /**
   * This gets called when preparing an AppendEntriesRPC, in order to send other members the
   *
   * This returns the commit index that we've applied up to.
   */
  public long getCommitIndex() {
    assertConsistency();
    return commitIndex;
  }


  /**
   * This gets called when preparing to call an RequestVoteRPC, in order to prevent other members from voting for an
   * out-of-date log.
   *
   * This returns index of the last log entry, regardless of whether it's been committed or not
   */
  public long getLastEntryIndex() {
    // Note: don't run consistency on this method
    if (logEntries.size() > 0) {
      return logEntries.getLast().getIndex();
    } else if (snapshot.isPresent()) {
      // the index of the entry that was actually the last one to be compacted is actually the prevLogIndex + 1 of the
      // snapshot
      return snapshot.get().lastIndex;
    } else {
      // We say the index of the entry before any entries are added is 0
      return 0L;
    }
  }

  /**
   * This gets called when preparing to call an RequestVoteRPC, in order to prevent other members from voting for an
   * out-of-date log.
   *
   * This returns index of the last log entry, regardless of whether it's been committed or not
   */
  public long getLastEntryTerm() {
    assertConsistency();
    try {
      if (logEntries.size() > 0) {
        return logEntries.getLast().getTerm();
      } else if (snapshot.isPresent()) {
        // the index of the entry that was actually the last one to be compacted is actually the prevLogIndex + 1 of the
        // snapshot
        return snapshot.get().lastTerm;
      } else {
        // We say the term of the entry before any entries are added is 0
        return 0L;
      }
    } finally {
      assertConsistency();
    }
  }


  /**
   * This returns the most up-to-date view of the cluster membership that we have.
   */
  public Set<String> getQuorumMembers() {
    assertConsistency();
    return this.latestQuorumMembers;
  }


  /**
   * @return all entries that haven't been snapshotted yet.
   */
  public List<EloquentRaftProto.LogEntry> getAllUncompressedEntries() {
    assertConsistency();
    return new ArrayList<>(this.logEntries);
  }


  /**
   * This gets called when preparing to call an AppendEntriesRPC, in order to send other members of the cluster the
   * current view of the log.
   *
   * @param startIndex the index (inclusive) to get the logs from
   * @return all the log entries starting at a given log index. If we've already compacted startIndex, then we
   * return empty.
   */
  public Optional<List<EloquentRaftProto.LogEntry>> getEntriesSinceInclusive(long startIndex) {
    Object timerStart = Prometheus.startTimer(summaryTiming);
    assertConsistency();
    try {
      // Requesting an update in the future is just requesting a heartbeat
      if (startIndex > getLastEntryIndex()) {
        return Optional.of(Collections.emptyList());
      }

      // This would indicate that we've already compacted this part of the logs
      if (!getEntryAtIndex(startIndex).isPresent()) return Optional.empty();

      // Get the entries
      long lastEntryIndex = getLastEntryIndex();
      if (startIndex == lastEntryIndex) {
        // shortcut: only getting one entry
        Optional<EloquentRaftProto.LogEntry> optionalLogEntry = getEntryAtIndex(startIndex);
        assert (optionalLogEntry.isPresent());
        return Optional.of(Collections.singletonList(optionalLogEntry.get()));
      } else {
        // Getting multiple entries -- iterate over whole list
        List<EloquentRaftProto.LogEntry> entries = new ArrayList<>();
        for (EloquentRaftProto.LogEntry entry : this.logEntries) {
          if (entry.getIndex() >= startIndex && entry.getIndex() <= lastEntryIndex) {
            entries.add(entry);
          }
        }
        return Optional.of(entries);
      }
    } finally {
      assertConsistency();
      assert fast("getEntriesSinceInclusive", timerStart);
    }
  }


  /**
   * This gets called when preparing to call an AppendEntriesRPC, in order to send other members of the cluster the
   * current view of the log.
   *
   * @param index the index to find the term of (if possible)
   * @return the term, if this entry is in our logs
   */
  public Optional<Long> getPreviousEntryTerm(long index) {
    Object timerStart = Prometheus.startTimer(summaryTiming);
    assertConsistency();
    try {
      // Requesting an update in the future
      if (index > getLastEntryIndex()) {
        return Optional.empty();
      }
      // If this is referring to the term of the lastIndex in the snapshot, return that
      if (snapshot.isPresent() && snapshot.get().lastIndex == index) {
        return Optional.of(snapshot.get().lastTerm);
      }
      // If the entry is just in the logs, then return that
      Optional<EloquentRaftProto.LogEntry> entry = getEntryAtIndex(index);
      if (entry.isPresent()) {
        return Optional.of(entry.get().getTerm());
      }
      // We always initialize to the 0 term
      if (index == 0) {
        return Optional.of(0L);
      }
      // Otherwise this is out of bounds
      return Optional.empty();
    } finally {
      assertConsistency();
      assert fast("getPreviousEntryTerm", timerStart);
    }
  }


  /**
   * This should only be called by leaders while appending to their log!
   *
   * Raft only allows one membership change to be uncommitted at a time. If we already have a membership change in the
   * log, and it's uncommitted, this returns false. Otherwise, this returns true.
   *
   * @return if it's safe to add an uncommitted membership change to the log.
   */
  public boolean getSafeToChangeMembership() {
    Object timerStart = Prometheus.startTimer(summaryTiming);
    assertConsistency();
    try {
      // If any uncommitted entry is of type CONFIGURATION, return false
      for (long i = commitIndex + 1; i <= getLastEntryIndex(); i++) {
        Optional<EloquentRaftProto.LogEntry> entry = getEntryAtIndex(i);
        assert (entry.isPresent()); // We can't have compacted entries that are after our commit index
        if (entry.get().getType() == EloquentRaftProto.LogEntryType.CONFIGURATION) return false;
      }
      // Otherwise, return true
      return true;
    } finally {
      assertConsistency();
      assert fast("getSafeToChangeMembership", timerStart);
    }
  }


  /**
   * Get the location of the most recent log entry.
   * Note that this does <b>NOT</b> search into snapshots -- this just takes entries in
   * the actual log.
   *
   * @return The index of the most recent log entry.
   */
  public Optional<RaftLogEntryLocation> lastConfigurationEntryLocation() {
    Object timerStart = Prometheus.startTimer(summaryTiming);
    assertConsistency();
    try {
      Iterator<EloquentRaftProto.LogEntry> iter = this.logEntries.descendingIterator();
      while (iter.hasNext()) {
        EloquentRaftProto.LogEntry entry = iter.next();
        if (!entry.getConfigurationList().isEmpty()) {
          return Optional.of(new RaftLogEntryLocation(entry.getIndex(), entry.getTerm()));
        }
      }
      return Optional.empty();
    } finally {
      assertConsistency();
      assert fast("lastConfigurationEntryLocation", timerStart);
    }
  }


  /**
   * This returns a CompletableFuture of a boolean indicating whether or not this entry was successfully committed to
   * the logs. This is used to allow a leader to have a callback once a given transition has either committed to the
   * logs, or failed to commit to the logs. We know when we've had a failure when a commit has a different term number
   * than expected. That indicates it was overwritten.
   *
   * @param index the index we're interested in hearing about
   * @param term the term we expect the index to be in
   * @param isInternal if true, this is an internal future that should complete on the master Raft thread.
   *                   Otherwise, we complete it on the worker pool.
   *
   * @return a CompletableFuture
   */
  CompletableFuture<Boolean> createCommitFuture(long index, long term, boolean isInternal) {
    Object timerStart = Prometheus.startTimer(summaryTiming);
    assertConsistency();
    try {
      CompletableFuture<Boolean> listener = new CompletableFuture<>();
      // 1. If this is a future for an event that has already happened, then complete it now
      if (index <= getCommitIndex()) {
        // 1.1. Check if the commit is in the snapshot
        boolean success = snapshot.map(snap -> index < snap.lastIndex && term <= snap.lastTerm).orElse(false);
        // 1.2. Otherwise, check the commit in the current log
        if (!success) {
          Optional<Long> pastTerm = getPreviousEntryTerm(index);
          success = (pastTerm.isPresent() && pastTerm.get() == term);
          if (!success) {
            log.trace("Failing commit future (bad term; actual={} != expected={})", pastTerm.isPresent() ? pastTerm.get() : "<unk>", term);
          }
        }
        // 1.3. Complete the future
        listener.complete(success);
      } else {
        // If this hasn't happened yet, then add this to a list
        if (isInternal) {
          commitFutures.add(new CommitFuture(index, term, listener::complete));
        } else {
          commitFutures.add(new CommitFuture(index, term, success -> this.pool.submit(() -> listener.complete(success))));
        }
      }
      return listener;
    } finally {
      assertConsistency();
      assert fast("createCommitFuture", timerStart);
    }
  }


  /**
   * @see #createCommitFuture(long, long, boolean)
   */
  public CompletableFuture<Boolean> createCommitFuture(long index, long term) {
    return createCommitFuture(index, term, false);
  }


  /**
   * @see #createCommitFuture(long, long)
   */
  public CompletableFuture<Boolean> createCommitFuture(RaftLogEntryLocation location) {
    return createCommitFuture(location.index, location.term);
  }


  /**
   * If leaderCommit &gt; commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
   *
   * @param leaderCommit the commit from the leader
   */
  public void setCommitIndex(long leaderCommit, long now) {
    Object timerStart = Prometheus.startTimer(summaryTiming);
    assertConsistency();
    try {
      if (leaderCommit > commitIndex) {
        long lastIndex = snapshot.map(s -> s.lastIndex).orElse(0L);
        if (logEntries.size() > 0) lastIndex = Math.max(logEntries.peekLast().getIndex(), lastIndex);
        long newCommitIndex = Math.min(leaderCommit, lastIndex);
        assert (newCommitIndex > commitIndex);

        // Commit the new entry transitions to the state machine

        for (EloquentRaftProto.LogEntry entry : this.logEntries) {
          if (entry.getIndex() > commitIndex && entry.getIndex() <= newCommitIndex) {  // if it's a new entry
            if (entry.getType() == EloquentRaftProto.LogEntryType.TRANSITION) {
              stateMachine.applyTransition(
                  entry.getTransition().isEmpty() ? Optional.empty() : Optional.of(entry.getTransition().toByteArray()),
                  "".equals(entry.getNewHospiceMember()) ? Optional.empty() : Optional.of(entry.getNewHospiceMember()),
                  now,
                  pool);
            } else if (entry.getType() == EloquentRaftProto.LogEntryType.CONFIGURATION) {
              committedQuorumMembers.clear();
              committedQuorumMembers.addAll(entry.getConfigurationList());
            } else {
              throw new IllegalStateException("Unrecognized entry type. This likely means we're very very out of date, and should now crash.");
            }
          }
        }

        // Update the commit index
        commitIndex = newCommitIndex;
        assertConsistency();

        // Complete any CommitFutures that are waiting for this commit
        for (CommitFuture commitFuture : new ArrayList<>(commitFutures)) {
          // (check for success)
          if (commitIndex >= commitFuture.index) {
            Optional<Long> termAtCommit = getPreviousEntryTerm(commitFuture.index);
            boolean success = termAtCommit.map(term -> term == commitFuture.term).orElse(false);
            if (!success) {
              if (termAtCommit.isPresent()) {
                log.trace("Failing commit future (bad term; actual={} != expected={})", termAtCommit.get(), commitFuture.term);
              } else {
                log.trace("Failing commit future (bad term; already compacted entry in snapshot)");
              }
            }
            // (register the future)
            commitFuture.complete.accept(success);
            commitFutures.remove(commitFuture);
          }
        }
      }
    } finally {
      assertConsistency();
      assert fast("setCommitIndex", timerStart);
    }
  }


  /**
   * This gets called from handleAppendEntriesRPC() in EloquentRaftMember. It handles verifying that an append command
   * is sound, by checking prevLogIndex and prevLogTerm, and then applying it.
   *
   * 1. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
   * 2. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all
   *    that follow it.
   * 3. Append any new entries not already in the log
   *
   * @param prevLogIndex the log index for the entry immediately preceding the entries to be appended
   * @param prevLogTerm the log term for the entry immediately preceding the entries to be appended
   * @param entries the entries to be appended
   *
   * @return true if the append command is in the log.
   */
  @SuppressWarnings("ConstantConditions")
  public boolean appendEntries(long prevLogIndex, long prevLogTerm, List<EloquentRaftProto.LogEntry> entries) {
    Object timerStart = Prometheus.startTimer(summaryTiming);
    assertConsistency();

    try {
      // 1. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
      Optional<EloquentRaftProto.LogEntry> prevEntry = getEntryAtIndex(prevLogIndex);
      boolean allowEntry = false;
      if (prevLogIndex == 0) {
        allowEntry = true; // Always allow the first entry
      } else if (prevEntry.isPresent() && prevEntry.get().getTerm() == prevLogTerm) {
        allowEntry = true; // Allow if there's a matching previous entry
      } else if (snapshot.isPresent() && snapshot.get().lastTerm == prevLogTerm && snapshot.get().lastIndex == prevLogIndex) {
        allowEntry = true; // Allow if the snapshot matches the previous entry
      }
      assert (entries.stream().noneMatch(entry -> this.getEntryAtIndex(entry.getIndex()).map(savedEntry -> savedEntry.getTerm() > entry.getTerm()).orElse(false))) : "We should never overwrite an entry with a lower term";
      if (!allowEntry) {
        log.trace("Rejecting log transition: prevLogIndex={}  prevEntry.getTerm()={}  prevLogTerm={}  commitIndex={}",
            prevLogIndex, prevEntry.map(EloquentRaftProto.LogEntry::getTerm).orElse(-1L), prevLogTerm, commitIndex);
        return false;
      }

      // Short circuit for if this is just a heartbeat
      if (entries.isEmpty()) {
        // (but: make sure we're not deleting anything)
        if (this.logEntries.isEmpty()) {
          // the log is empty and we're not adding anything
          return true;
        }
        if (prevEntry.isPresent() &&
            prevEntry.get().getIndex() == prevLogIndex && prevEntry.get().getTerm() == prevLogTerm &&
            prevEntry.get().getIndex() == getLastEntryIndex()
            ) {
          // The log is up to date
          return true;
        }
      }

      // 2. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all
      //    that follow it.
      if (!entries.isEmpty()) {
        for (EloquentRaftProto.LogEntry entry : entries) {
          Optional<EloquentRaftProto.LogEntry> potentiallyConflictingEntry = getEntryAtIndex(entry.getIndex());
          // If we find a conflicting entry, then tructate after that
          if (potentiallyConflictingEntry.isPresent()) {
            if (potentiallyConflictingEntry.get().getTerm() != entry.getTerm()) {
              truncateLogAfterIndexInclusive(entry.getIndex());
              break;
            }
            // If they're the same term, this is an opportunity to assert that it's the same transition
            else {
              assert(entry.toByteString().equals(potentiallyConflictingEntry.get().toByteString()));
            }
          }
        }
      }

      // 3. Append any new entries not already in the log
      if (!entries.isEmpty()) {
        // 3.1. Calculate how many entries we need to skip before appending the new entries
        long startIndex = entries.get(0).getIndex();
        long lastKnownIndex = getLastEntryIndex();
        // in the default cause, where we skip 0, lastKnownIndex = startIndex - 1
        int skipEntries = (int) (lastKnownIndex - startIndex) + 1;
        assert (skipEntries >= 0);
        // 3.2. Add the entries
        for (int i = skipEntries; i < entries.size(); i++) {
          logEntries.add(entries.get(i));
          // If an entry contains information about a new cluster membership, update our cluster membership immediately
          if (entries.get(i).getType() == EloquentRaftProto.LogEntryType.CONFIGURATION) {
            Optional<String> serverName = Optional.empty();
            if (stateMachine instanceof KeyValueStateMachine) {
              serverName = ((KeyValueStateMachine)stateMachine).serverName;
            }
            log.info("{} - Reconfiguring cluster to {}", serverName.orElse("?"), entries.get(i).getConfigurationList());
            latestQuorumMembers.clear();
            latestQuorumMembers.addAll(entries.get(i).getConfigurationList());
          }
        }
        // 3.3. Force a compaction if our log is longer than 100 entries long - this may be a noop if none of these hundred
        // entities is committed, but that's extremely rare in general.
//        if (snapshot.map(sn -> sn.lastIndex + COMPACTION_LIMIT <= this.commitIndex).orElse(commitIndex >= COMPACTION_LIMIT)) {
        if (this.logEntries.size() >= COMPACTION_LIMIT) {
          forceSnapshot();
        }
      }

      return true;
    } finally {
      assertConsistency();
      assert fast("appendEntries", timerStart);
    }
  }


  /**
   * This gets called from handleInstallSnapshotRPC() in EloquentRaftMember. It handles installing the snapshot and
   * updating our own logs appropriately.
   *
   * 1. If lastIndex is larger than the latest Snapshot's, then save the snapshot file and Raft state (lastIndex,
   *    lastTerm, lastConfig). Discard any existing or partial snapshots
   * 2. If existing log entry has the same index and term as lastIndex, discard log up through lastIndex (but retain
   *    following entries), and return.
   * 3. Otherwise, discard the entire log
   * 4. Reset state machine using snapshot contents, and load cluster config from the snapshot.
   *
   * @param snapshot the snapshot to install
   * @param now the current time, in case we want to mock it
   *
   * @return true if the snapshot got committed
   */
  public boolean installSnapshot(Snapshot snapshot, long now) {
    Object timerStart = Prometheus.startTimer(summaryTiming);
    assertConsistency();
    try {
      // 1. If lastIndex is larger than the latest Snapshot's, then save the snapshot file and Raft state (lastIndex,
      //   lastTerm, lastConfig). Discard any existing or partial snapshots. Otherwise return.
      if (!this.snapshot.isPresent() || snapshot.lastIndex > this.snapshot.get().lastIndex) {
        this.snapshot = Optional.of(snapshot);
      } else return false;

      // Note: committing up until the snapshot's last index here is actually incorrect - we can have entries at the
      // latest index with different term numbers than the snapshot, and therefore those should not be committed.

      // 2. If existing log entry has the same index and term as lastIndex, discard log up through lastIndex (but retain
      // following entries), and return.
      Optional<EloquentRaftProto.LogEntry> entry = getEntryAtIndex(snapshot.lastIndex);
      if (entry.isPresent() && entry.get().getTerm() == snapshot.lastTerm) {
        if (commitIndex < snapshot.lastIndex) {
          setCommitIndex(snapshot.lastIndex, now);
        }
        truncateLogBeforeIndexInclusive(snapshot.lastIndex);
        // We don't overwrite the state machine (step 4), since we have entries that are at least as up to date as this snapshot
        return true;
      }

      // 3. Otherwise, discard the entire log
      this.logEntries.clear();

      // 4. Reset state machine using snapshot contents, and load cluster config from the snapshot.
      stateMachine.overwriteWithSerialized(snapshot.serializedStateMachine, now, this.pool);
      committedQuorumMembers.clear();
      committedQuorumMembers.addAll(snapshot.lastClusterMembership);
      latestQuorumMembers.clear();
      latestQuorumMembers.addAll(snapshot.lastClusterMembership);

      // Update the commit index
      setCommitIndex(snapshot.lastIndex, now);
      assertConsistency();

      return true;
    } finally {
      assertConsistency();
      assert fast("installSnapshot", timerStart);
    }
  }


  /**
   * This forces a log compaction step, where all the committed log entries are compacted away, and we backstop with a
   * single snapshot of the state machine. The resulting snapshot is returned.
   */
  public Snapshot forceSnapshot() {
    Object timerStart = Prometheus.startTimer(summaryTiming);
    assertConsistency();
    try {
      Optional<EloquentRaftProto.LogEntry> optionalCommitEntry = getEntryAtIndex(commitIndex);
      // 1. If there are no entries that we can compact, then return a Snapshot without editing the log
      if (!optionalCommitEntry.isPresent()) {
        log.info("Snapshotting without any entries to snapshot. This means that our commitIndex has gotten more than "+COMPACTION_LIMIT+" behind our latest entry: commitIndex={}, number non-compacted entries={}, latestEntry={}", commitIndex, logEntries.size(), logEntries.size() > 0 ? logEntries.getLast().getIndex() : -1);
        // 1.1. If we've taken any previous snapshot, return that
        if (snapshot.isPresent()) {
          return snapshot.get();
        }
        // 1.2. Otherwise, create a snapshot starting with no entries
        else {
          assert (commitIndex <= 1);
          return new Snapshot(stateMachine.serialize(), 0L, 0L, committedQuorumMembers);
        }
      }
      EloquentRaftProto.LogEntry commitEntry = optionalCommitEntry.get();

      // 2. Install a snapshot of the last committed
      Snapshot snapshot = new Snapshot(stateMachine.serialize(), commitEntry.getIndex(), commitEntry.getTerm(), committedQuorumMembers);
      this.snapshot = Optional.of(snapshot);

      // 3. Compact the logs before this index
      truncateLogBeforeIndexInclusive(commitEntry.getIndex());

      assert logEntries.size() <= 0 || (snapshot.lastIndex < logEntries.getLast().getIndex());

      return snapshot;
    } finally {
      assertConsistency();
      assert fast("forceSnapshot", timerStart);
    }
  }


  /**
   * <b>WARNING: UNSAFE!!!</b>
   *
   * This function violates the correctness principles of Raft, and should never be called in production code.
   * But, it's very useful for unit testing bizarre states Raft may accidentally end up in.
   *
   * @param log The new log to set. The old log is cleared in favor of these entries
   */
  public void unsafeSetLog(List<EloquentRaftProto.LogEntry> log) {
    Object timerStart = Prometheus.startTimer(summaryTiming);
    if ("true".equals(System.getenv("ELOQUENT_PRODUCTION"))) {
      System.err.println("Called unsafeSetLog in production! Ignoring the call");
      return;
    }
    assertConsistency();
    try {
      this.logEntries.clear();
      this.logEntries.addAll(log);
      this.commitIndex = 0;
    } finally {
      assertConsistency();
      assert fast("unsafeSetLog", timerStart);
    }
  }


  /**
   * This retrieves a log entry at a given index. If we're asking for an index that's already been compacted, this
   * returns an empty entry. If we ask for an entry that's beyond the latest entry we know about, also return empty.
   *
   * This is package-private, since we want to be able to call it from tests, but otherwise it's perfectly internal.
   *
   * Also note that this is a linear-time operation with respect to the size of the log.
   *
   * @param index the index we'd like to request
   *
   * @return a log entry, unless it's already been compacted in which case we return empty
   */
  Optional<EloquentRaftProto.LogEntry> getEntryAtIndex(long index) {
    Object timerStart = Prometheus.startTimer(summaryTiming);
    assertConsistency();
    try {
      // Some shortcuts
      if (this.logEntries.isEmpty()) {
        return Optional.empty();
      }
      EloquentRaftProto.LogEntry first = this.logEntries.getFirst();
      EloquentRaftProto.LogEntry last = this.logEntries.getLast();
      if (index == first.getIndex()) {
        return Optional.of(first);
      } else if (index < first.getIndex()) {
        return Optional.empty();
      } else if (index == last.getIndex()) {
        return Optional.of(last);
      } else if (index > last.getIndex()) {
        return Optional.empty();
      }

      // Search over the deque
      Iterator<EloquentRaftProto.LogEntry> entries = index > (first.getIndex() + this.logEntries.size() / 2)
          ? this.logEntries.descendingIterator()
          : this.logEntries.iterator();
      while (entries.hasNext()) {
        EloquentRaftProto.LogEntry entry = entries.next();
        if (entry.getIndex() == index) {
          return Optional.of(entry);
        }
      }
      return Optional.empty();
    } finally {
      assertConsistency();
      assert fast("getEntryAtIndex", timerStart);
    }
  }


  /**
   * This deletes the entry at index, and all that follow it, from the log.
   *
   * @param index the index (inclusive) to delete from
   */
  void truncateLogAfterIndexInclusive(long index) {
    Object timerStart = Prometheus.startTimer(summaryTiming);
    assertConsistency();
    assert(index > this.commitIndex);
    try {
      // 1. Check if this request is out of bounds
      if (index < 0) {
        assert(false) : "Cannot truncate to a negative log index";
        index = 0;
      }

      // 2. If we have no log entries, then the one we're requesting has already been compacted
      if (logEntries.size() == 0) return;

      // 3. Get the index of the earliest entry that hasn't been compacted
      long earliestRecordedEntry = logEntries.getFirst().getIndex();

      // 4. If we're requesting to trucate to an index that's earlier than our earliest log entry, then we need to exception
      if (index < earliestRecordedEntry) {
        assert(false) : "Cannot truncate beyond already compacted log entries";
        index = earliestRecordedEntry;
      }

      // 5. Now we can check the offset into the logEntries, and truncate accordingly
      int logOffset = (int) (index - earliestRecordedEntry);
      while (logEntries.size() > logOffset) {
        logEntries.removeLast();
        long newCommitIndex = Math.min(getLastEntryIndex(), this.commitIndex);
        assert(newCommitIndex >= this.commitIndex);
        this.commitIndex = newCommitIndex;
      }

      // 6. Recompute the cluster configuration
      Collection<String> latestConfiguration = null;
      // 6.1. Try to get the configuration from the log
      Iterator<EloquentRaftProto.LogEntry> iter = logEntries.descendingIterator();
      while (iter.hasNext()) {
        EloquentRaftProto.LogEntry entry = iter.next();
        if (entry.getConfigurationCount() > 0) {
          latestConfiguration = entry.getConfigurationList();
          break;
        }
      }
      // 6.2. Try to get the configuration from the snapshot
      if (latestConfiguration == null && snapshot.isPresent()) {
        latestConfiguration = snapshot.get().lastClusterMembership;
      }
      // 6.3. Revert to the original configuration
      if (latestConfiguration == null) {
        latestConfiguration = initialQuorumMembers;
      }
      // 6.4. Update the quorum
      this.latestQuorumMembers.clear();
      this.latestQuorumMembers.addAll(latestConfiguration);
    } finally {
      assertConsistency();
      assert fast("truncateLogAfterIndexInclusive", timerStart);
    }
  }


  /**
   * This deletes the entry at index, and all that precede it, from the log.
   *
   * @param index the index (inclusive) to delete up to
   */
  void truncateLogBeforeIndexInclusive(long index) {
    Object timerStart = Prometheus.startTimer(summaryTiming);
    assertConsistency();
    try {
      // 1. Check if this request is out of bounds
      if (index < 0) throw new IndexOutOfBoundsException("Cannot truncate before a negative log index");

      // 2. If we have no log entries, then the one we're requesting has already been compacted
      if (logEntries.size() == 0) return;

      // 3. Get the index of the earliest entry that hasn't been compacted
      long earliestRecordedEntry = logEntries.getFirst().getIndex();

      // 4. If we're requesting to truncate up to an index that's earlier than our earliest log entry, then that's a no-op
      if (index < earliestRecordedEntry) {
        return;
      }

      // 5. Now we can check the offset into the logEntries, and truncate accordingly

      int logOffset = (int) (index - earliestRecordedEntry);
      for (int i = 0; i <= logOffset; i++) {
        assert (logEntries.size() > 0);
        logEntries.removeFirst();
      }
      assert logEntries.size() <= 0 || logEntries.getFirst().getIndex() == index + 1;
    } finally {
      assertConsistency();
      assert fast("truncateLogBeforeIndexInclusive", timerStart);
    }
  }


  /**
   * Assert that the log is in a self-consistent state.
   *
   * This should be called by every function to help us fail fast in case of errors.
   */
  void assertConsistency() {
    assert this.commitIndex <= this.getLastEntryIndex() : "We've marked ourselves committed past our last entry: commitIndex=" + this.commitIndex + "  lastEntry=" + this.getLastEntryIndex();
  }


  /** {@inheritDoc} */
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    RaftLog raftLog = (RaftLog) o;
    return commitIndex == raftLog.commitIndex &&
        Objects.equals(new ArrayList<>(logEntries), new ArrayList<>(raftLog.logEntries)) &&
        Objects.equals(stateMachine, raftLog.stateMachine) &&
        Objects.equals(commitFutures, raftLog.commitFutures) &&
        Objects.equals(snapshot, raftLog.snapshot) &&
        Objects.equals(latestQuorumMembers, raftLog.latestQuorumMembers) &&
        Objects.equals(committedQuorumMembers, raftLog.committedQuorumMembers)
        ;
  }


  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return (int) commitIndex;
  }
}
