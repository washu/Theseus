package ai.eloquent.raft;

import ai.eloquent.monitoring.Prometheus;
import ai.eloquent.util.FunctionalUtils;
import ai.eloquent.util.Pair;
import ai.eloquent.util.TimerUtils;
import com.sun.management.GcInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.stream.Collectors;

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
   * The number of log entries to keep in the log before compacting into a snapshot.
   */
  public static final int COMPACTION_LIMIT
      = "true".equals(System.getenv("CI")) ? 32 : (0x1 << 14); // 16k

  /**
   * A pool for completing commit futures.
   */
  final ExecutorService pool;


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
    /** The serialized state machine, as a byte blob */
    byte[] serializedStateMachine;

    /** The snapshot replaces all entries up through and including this index */
    long lastIndex;

    /** term of lastIndex */
    long lastTerm;

    /** latest cluster configuration as of lastIndex */
    Set<String> lastClusterMembership;

    /** The straightforward constructor */
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
  final Collection<CommitFuture> commitFutures = new ConcurrentLinkedQueue<>();

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
        long uptime = ManagementFactory.getRuntimeMXBean().getStartTime();
        for (GarbageCollectorMXBean gcBean : ManagementFactory.getGarbageCollectorMXBeans()) {
          com.sun.management.GarbageCollectorMXBean sunGcBean = (com.sun.management.GarbageCollectorMXBean) gcBean;
          GcInfo lastGcInfo = sunGcBean.getLastGcInfo();
          if (lastGcInfo != null) {
            lastGcTime = lastGcInfo.getStartTime() + uptime;
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
        log.warn("{} took {};  interrupted_by_gc={}; log_length={}", description, TimerUtils.formatTimeDifference(duration), interruptedByGC, logEntries.size());
      } else {
        log.info("{} took {};  interrupted_by_gc={}; log_length={}", description, TimerUtils.formatTimeDifference(duration), interruptedByGC, logEntries.size());
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
    if (!logEntries.isEmpty()) {
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
    return this.latestQuorumMembers;
  }


  /**
   * @return all entries that haven't been snapshotted yet.
   */
  public List<EloquentRaftProto.LogEntry> getAllUncompressedEntries() {
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
   * <p>
   *   This returns a CompletableFuture of a boolean indicating whether or not this entry was successfully committed to
   *   the logs. This is used to allow a leader to have a callback once a given transition has either committed to the
   *   logs, or failed to commit to the logs. We know when we've had a failure when a commit has a different term number
   *   than expected. That indicates it was overwritten.
   * </p>
   *
   * <p>
   *   Note that in rare cases, we can fail a commit future even if the commit went through. This happens when we're
   *   checking for entry in the log but it's already snapshotted, and the snapshot has a more recent latest term than
   *   the term we're looking for.
   *   In this case, we can't discriminate between the commit being successful and an election happening afterwards,
   *   or the commit failing from an election and then a snapshot being taken before we can create the commit future.
   * </p>
   *
   * @param index the index we're interested in hearing about
   * @param term the term we expect the index to be in
   * @param isInternal if true, this is an internal future that should complete on the master Raft thread.
   *                   Otherwise, we complete it on the worker pool.
   *
   * @return a CompletableFuture that will fire once either (1) the commit goes through, or (2) the commit fails.
   *         Note, again, that the commit <i>may</i> have gone through even if the future completes with a false
   *         value, we just can't guarantee it.
   */
  CompletableFuture<Boolean> createCommitFuture(long index, long term, boolean isInternal) {
    Object timerStart = Prometheus.startTimer(summaryTiming);
    try {
      CompletableFuture<Boolean> listener = new CompletableFuture<>();
      // 1. If this is a future for an event that has already happened, then complete it now
      if (index <= getCommitIndex()) {
        // 1.1. Check if the commit is in the snapshot
        boolean success = snapshot.map(snap -> index < snap.lastIndex && term == snap.lastTerm).orElse(false);
        // 1.2. Otherwise, check the commit in the current log
        if (!success) {
          Optional<EloquentRaftProto.LogEntry> entry = getEntryAtIndex(index);
          success = entry.isPresent() && entry.get().getTerm() == term;
        }
        // 1.3. Complete the future
        if (isInternal) {
          listener.complete(success);
        } else {
          final boolean successFinal = success;
          this.pool.submit(() -> listener.complete(successFinal));
        }
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
   * If leaderCommit &gt; commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
   *
   * @param leaderCommit the commit from the leader
   */
  public void setCommitIndex(long leaderCommit, long now) {
    Object timerStart = Prometheus.startTimer(summaryTiming);
    try {
      if (leaderCommit > commitIndex) {
        // 1. Some checks
        long lastIndex = snapshot.map(s -> s.lastIndex).orElse(0L);
        if (logEntries.size() > 0) {
          lastIndex = Math.max(logEntries.peekLast().getIndex(), lastIndex);
        }
        long newCommitIndex = Math.min(leaderCommit, lastIndex);
        assert newCommitIndex > commitIndex : "Cannot commit backwards";

        // 2. Commit the new entry transitions to the state machine
        // Reverse iterator, so we can early stop
        Iterator<EloquentRaftProto.LogEntry> revEntryIter = this.logEntries.descendingIterator();
        EloquentRaftProto.LogEntry lastEntry = null;  // for debugging only
        // 2.1. Get entries to apply (reverse search)
        Stack<EloquentRaftProto.LogEntry> toApply = new Stack<>();
        while (revEntryIter.hasNext()) {
          EloquentRaftProto.LogEntry entry = revEntryIter.next();
          assert lastEntry == null || lastEntry.getIndex() > entry.getIndex() : "Log is not monotonic!";
          lastEntry = entry;
          if (entry.getIndex() > commitIndex && entry.getIndex() <= newCommitIndex) {  // if it's a new entry
            toApply.push(entry);
          } else if (entry.getIndex() <= commitIndex) {
            break;  // entries are added monotonically to the log, so we can early stop
          }
        }
        // 2.2. Apply the relevant entries (in the correct order)
        while (!toApply.isEmpty()) {
          EloquentRaftProto.LogEntry entry = toApply.pop();
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

        // 3. Update the commit index
        commitIndex = newCommitIndex;

        // 4. Complete any CommitFutures that are waiting for this commit
        Map<Long, Long> termForIndex = new HashMap<>();  // A cache to avoid too many calls to previousEntryTerm
        Iterator<CommitFuture> commitFuturesIter = commitFutures.iterator();
        while (commitFuturesIter.hasNext()) {
          CommitFuture commitFuture = commitFuturesIter.next();
          if (commitIndex >= commitFuture.index) {  // we've committed past this future
            // 4.1. try to get the term from the log
            @Nullable Long termAtCommit = termForIndex.get(commitFuture.index);
            if (termAtCommit == null) {
              termAtCommit = getEntryAtIndex(commitFuture.index).map(EloquentRaftProto.LogEntry::getTerm).orElse(null);
              if (termAtCommit != null) {
                termForIndex.put(commitFuture.index, termAtCommit);
              }
            }
            // 4.2. try to get the term from the snapshot
            if (termAtCommit == null && this.snapshot.isPresent() &&   // we didn't find the term in the log and have a snapshot
                commitFuture.index < this.snapshot.get().lastIndex &&  // ... and we're looking for something in the snapshot
                this.snapshot.get().lastTerm == commitFuture.term) {   // ... and the snapshot is at our term (pessimistic assumption)
              termAtCommit = commitFuture.term;  // ... then we can guarantee our term is correct
            }
            // 4.3. Get success
            boolean success = termAtCommit != null && termAtCommit == commitFuture.term;
            if (!success) {
              if (termAtCommit != null) {
                log.trace("Failing commit future (bad term; actual={} != expected={})", termAtCommit, commitFuture.term);
              } else {
                log.trace("Failing commit future (bad term; already compacted entry in snapshot)");
              }
            }
            // 4.4. fire the future
            long futureCompleteBegin = System.currentTimeMillis();
            try {
              commitFuture.complete.accept(success);
            } finally {
              long futureCompleteEnd = System.currentTimeMillis();
              if (futureCompleteEnd > futureCompleteBegin + 10000) {
                log.warn("Completing a future took >10ms ({}) -- this likely indicates that we didn't defer user code to a thread pool",
                    TimerUtils.formatTimeDifference(futureCompleteEnd - futureCompleteBegin));
              }
            }
            commitFuturesIter.remove();
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
   * @return true if the append command is in the log; false if we should "reply false" to the RPC.
   */
  @SuppressWarnings("ConstantConditions")
  public boolean appendEntries(long prevLogIndex, long prevLogTerm, List<EloquentRaftProto.LogEntry> entries) {
    Object timerStart = Prometheus.startTimer(summaryTiming);
    // Should assert that our entries are sorted
    assert entries.stream().sorted(Comparator.comparingLong(EloquentRaftProto.LogEntry::getIndex)).collect(Collectors.toList()).equals(entries) : "Entries we're appending should be sorted";

    try {
      // 1. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
      Optional<EloquentRaftProto.LogEntry> prevEntry = getEntryAtIndex(prevLogIndex);
      boolean allowEntry = false;
      if (prevEntry.isPresent() && prevEntry.get().getTerm() == prevLogTerm) {
        // Allow if we're appending correctly to the log
        allowEntry = entries.isEmpty() || prevEntry.get().getTerm() <= entries.get(0).getTerm();
      } else if (prevLogIndex == 0 && !prevEntry.isPresent()) {
        // Special case for the first index
        // basically always allow, unless there's a super-esoteric term mismatch.
        allowEntry = entries.isEmpty() || this.logEntries.isEmpty() || entries.get(0).getTerm() >= this.logEntries.getFirst().getTerm();
      } else if (snapshot.isPresent() && snapshot.get().lastTerm <= prevLogTerm && snapshot.get().lastIndex == prevLogIndex) {
        // Allow if we're appending correctly to a snapshot
        allowEntry = entries.isEmpty() || (
            entries.get(0).getIndex() == snapshot.get().lastIndex + 1 && entries.get(0).getTerm() >= snapshot.get().lastTerm);
      }
      if (!allowEntry) {
        if (!entries.isEmpty()) {
          log.info("Rejecting log transition: prevLogIndex={}  prevEntry.getTerm()={}  prevLogTerm={}  commitIndex={}  snapshot.lastTerm={}  snapshot.lastIndex={}",
              prevLogIndex, prevEntry.map(EloquentRaftProto.LogEntry::getTerm).orElse(-1L), prevLogTerm, commitIndex,
              snapshot.map(x -> x.lastTerm).orElse(-1L), snapshot.map(x -> x.lastIndex).orElse(-1L));
        }
        return false;
      }
      assert (entries.stream().noneMatch(entry -> this.getEntryAtIndex(entry.getIndex()).map(savedEntry -> savedEntry.getTerm() > entry.getTerm()).orElse(false))) : "We should never overwrite an entry with a lower term";

      // 2. Short circuit for if this is just a heartbeat
      if (entries.isEmpty()) {
        return true;
      }

      // 3. Bulkhead for assumptions
      assert !entries.isEmpty() : "Beyond this point, we should have entries being added";
      assert allowEntry : "We should be allowing our entries to be added";

      // 4. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all
      //    that follow it.
      if (entries.size() <= 2) {
        // 4.A. If we only have a few entries, it's faster use getEntryAtIndex()
        for (EloquentRaftProto.LogEntry entry : entries) {  // loop length is bounded to 2
          Optional<EloquentRaftProto.LogEntry> potentiallyConflictingEntry = getEntryAtIndex(entry.getIndex());
          // If we find a conflicting entry, then truncate after that
          if (potentiallyConflictingEntry.isPresent()) {
            if (potentiallyConflictingEntry.get().getTerm() != entry.getTerm()) {
              truncateLogAfterIndexInclusive(entry.getIndex());
              break;
            } else {
              // If they're the same term, this is an opportunity to assert that it's the same transition
              assert entry.toByteString().equals(potentiallyConflictingEntry.get().toByteString()) : "We're trying to commit a different transition at the same index+term";
            }
          }
        }
      } else {
        // 4.B. If we have multiple entries, it's faster to construct a map
        Map<Long, Long> termForIndex = new HashMap<>(this.logEntries.size());
        for (EloquentRaftProto.LogEntry entryInLog : this.logEntries) {
          termForIndex.put(entryInLog.getIndex(), entryInLog.getTerm());
        }
        for (EloquentRaftProto.LogEntry entry : entries) {
          Long potentiallyConflictingTerm = termForIndex.get(entry.getIndex());
          if (potentiallyConflictingTerm != null && potentiallyConflictingTerm != entry.getTerm()) {
            truncateLogAfterIndexInclusive(entry.getIndex());
            break;
          } else {
            //noinspection OptionalGetWithoutIsPresent
            assert potentiallyConflictingTerm == null || entry.toByteString().equals(getEntryAtIndex(entry.getIndex()).get().toByteString()) : "We're trying to commit a different transition at the same index+term";
          }
        }
      }

      // 4. Append any new entries not already in the log
      long lastGoodEntry = getLastEntryIndex();
      for (EloquentRaftProto.LogEntry toAdd : entries) {
        if (toAdd.getIndex() > lastGoodEntry) {
          assert toAdd.getTerm() >= getLastEntryTerm() : "Double check: we should not be going backwards in term";
          assert toAdd.getIndex() == getLastEntryIndex() + 1 : "Double check: we should be appending entries monotonically";
          // 4.1. Append the entry
          logEntries.add(toAdd);
          // 4.2. Reconfigure the cluster (if applicable)
          if (toAdd.getType() == EloquentRaftProto.LogEntryType.CONFIGURATION) {
            Optional<String> serverName = Optional.empty();
            if (stateMachine instanceof KeyValueStateMachine) {
              serverName = ((KeyValueStateMachine)stateMachine).serverName;
            }
            log.info("{} - Reconfiguring cluster to {}", serverName.orElse("?"), toAdd.getConfigurationList());
            latestQuorumMembers.clear();
            latestQuorumMembers.addAll(toAdd.getConfigurationList());
          }
        }
      }
      // 4.3. Force a compaction if our log is too long
      if (this.logEntries.size() >= COMPACTION_LIMIT && commitIndex >= this.logEntries.getFirst().getIndex()) {
        forceSnapshot();
      }

      // 5. Return
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
    try {
      // 1. If lastIndex is larger than the latest Snapshot's, then save the snapshot file and Raft state (lastIndex,
      //   lastTerm, lastConfig). Discard any existing or partial snapshots. Otherwise return.
      Optional<EloquentRaftProto.LogEntry> entryAtSnapshotIndex = this.getEntryAtIndex(snapshot.lastIndex);
      long minTerm = entryAtSnapshotIndex.map(EloquentRaftProto.LogEntry::getTerm).orElse(this.getLastEntryTerm());
      if ((
              !this.snapshot.isPresent() ||
              (snapshot.lastIndex > this.snapshot.get().lastIndex && snapshot.lastTerm >= this.snapshot.get().lastTerm) ||
              (snapshot.lastIndex >= this.snapshot.get().lastIndex && snapshot.lastTerm > this.snapshot.get().lastTerm)
          ) &&  // the snapshot is more recent than our current one
          (snapshot.lastTerm >= minTerm)  // The snapshot has a more recent term than the entry at that index
      ) {
        this.snapshot = Optional.of(snapshot);
      } else {
        return false;
      }

      // 2. Update the log based off of the snapshot
      truncateLogBeforeIndexInclusive(snapshot.lastIndex);
      if (!this.logEntries.isEmpty() && this.logEntries.getFirst().getTerm() < snapshot.lastTerm) {
        log.warn("Clearing Raft log ({} entries starting at index={} term={}) because of a snapshot with lastIndex={} lastTerm={}",
            this.logEntries.size(), this.logEntries.getFirst().getIndex(), this.logEntries.getFirst().getTerm(),
            snapshot.lastIndex, snapshot.lastTerm);
        this.logEntries.clear();
      }
      if (commitIndex <= snapshot.lastIndex) {  // don't commit backwards!
        setCommitIndex(snapshot.lastIndex, now);
        stateMachine.overwriteWithSerialized(snapshot.serializedStateMachine, now, this.pool);
        committedQuorumMembers.clear();
        committedQuorumMembers.addAll(snapshot.lastClusterMembership);
        if (snapshot.lastIndex >= this.getLastEntryIndex()) {  // don't overwrite latest membership, if log is more recent
          latestQuorumMembers.clear();
          latestQuorumMembers.addAll(snapshot.lastClusterMembership);
        }
      }
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
    try {
      // 1. Error check
      EloquentRaftProto.LogEntry earliestEntry = logEntries.peekFirst();
      Optional<EloquentRaftProto.LogEntry> optionalCommitEntry;
      if (earliestEntry == null || earliestEntry.getIndex() > commitIndex || !(optionalCommitEntry = getEntryAtIndex(commitIndex)).isPresent()) {
        if (logEntries.size() >= COMPACTION_LIMIT) {
          log.warn("Log has {} uncommitted entries (commitIndex={}; lastIndex={})", logEntries.size(), commitIndex, logEntries.peekLast().getIndex());
        } else {
          log.debug("Forced a snapshot on a small log, where we haven't finished committing any entries in the log yet");
        }
        if (snapshot.isPresent()) {
          // 1.1. If we've taken any previous snapshot, return that
          return snapshot.get();
        } else {
          // 1.2. Otherwise, create a snapshot starting with no entries
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
      log.warn("Index should be within the log's range, but is not in the log! {} <= {} <= {}",
          first.getIndex(), index, last.getIndex());
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
    try {
      // 1. Check conditions
      // 1.1. Index must be positive
      if (index < 0) {
        assert false : "Cannot truncate to a negative log index";
        index = 0;
      }
      // 1.2. Index must be past our commit index
      if (index < this.commitIndex) {
        assert false : "Cannot truncate back beyond our commit index";
        return;
      }
      // 1.3. We can't truncate if our log is empty
      if (logEntries.isEmpty()) {
        return;
      }

      // 2. Truncate the log
      Iterator<EloquentRaftProto.LogEntry> truncateIter = this.logEntries.descendingIterator();
      while (truncateIter.hasNext() && truncateIter.next().getIndex() >= index) {
        truncateIter.remove();
      }

      // 3. Recompute the cluster configuration
      Collection<String> latestConfiguration = null;
      // 3.1. Try to get the configuration from the log
      Iterator<EloquentRaftProto.LogEntry> configIter = logEntries.descendingIterator();
      while (configIter.hasNext()) {
        EloquentRaftProto.LogEntry entry = configIter.next();
        if (entry.getConfigurationCount() > 0) {
          latestConfiguration = entry.getConfigurationList();
          break;
        }
      }
      // 3.2. Try to get the configuration from the snapshot
      if (latestConfiguration == null && snapshot.isPresent()) {
        latestConfiguration = snapshot.get().lastClusterMembership;
      }
      // 3.3. Revert to the original configuration
      if (latestConfiguration == null) {
        latestConfiguration = initialQuorumMembers;
      }
      // 3.4. Update the quorum
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
    try {
      // 1. Basic checks
      // 1.1. Check if this request is out of bounds
      if (index < 0) {
        assert false : "Cannot truncate before a negative index";
        return;
      }
      // 1.2. If we have no log entries, then the one we're requesting has already been compacted
      if (logEntries.isEmpty()) {
        return;
      }
      // 1.3. If we're requesting to truncate up to an index that's earlier than our earliest log entry, then that's a no-op
      long earliestRecordedEntry = logEntries.getFirst().getIndex();
      if (index < earliestRecordedEntry) {
        return;
      }

      // 2. Truncate the log
      Iterator<EloquentRaftProto.LogEntry> iter = this.logEntries.iterator();
      while (iter.hasNext() && iter.next().getIndex() <= index) {
        iter.remove();
      }
      assert logEntries.isEmpty() || logEntries.getFirst().getIndex() == index + 1 : "The log should have gone in order after truncation";
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
    // 1. Tests on the commit index
    assert this.commitIndex <= this.getLastEntryIndex() : "We've marked ourselves committed past our last entry: commitIndex=" + this.commitIndex + "  lastEntry=" + this.getLastEntryIndex();

    // 2. Tests on the log
    assert this.logEntries.isEmpty() || this.logEntries.stream().reduce(
        Pair.makePair(this.logEntries.getFirst().getIndex() - 1, true),
        (lastIndex, entry) -> Pair.makePair(entry.getIndex(), lastIndex.first + 1== entry.getIndex()),
        (last, current) -> current).second : "The log entries list should be dense (i.e., each index should be one more than the previous)";
    assert this.logEntries.stream().reduce(
        Pair.makePair(0L, true),
        (lastTerm, entry) -> Pair.makePair(entry.getTerm(), lastTerm.first <= entry.getTerm()),
        (last, current) -> current).second : "The terms in the log should be monotonically increasing";
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
