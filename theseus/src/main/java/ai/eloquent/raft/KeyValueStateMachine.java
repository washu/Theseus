package ai.eloquent.raft;

import ai.eloquent.error.RaftErrorListener;
import ai.eloquent.io.IOUtils;
import ai.eloquent.monitoring.Prometheus;
import ai.eloquent.util.*;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.annotation.Nullable;
import javax.annotation.Nonnull;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.function.Predicate;
import java.util.stream.Collectors;


/**
 * This is the standard state machine we use to implement our RAFT distributed storage. It has two maps:
 *
 * - string to lock (for distributed locking)
 * - string to byte[] (for distributed state)
 *
 * The state machine implements locks as queues. All changes can be listened for with custom listeners on the state
 * machine.
 */
@SuppressWarnings("Duplicates")
public class KeyValueStateMachine extends RaftStateMachine {
  /**
   * An SLF4J Logger for this class.
   */
  private static final Logger log = LoggerFactory.getLogger(KeyValueStateMachine.class);

  /** The number of elements currently in the queue. */
  private static final Object gaugeNumListeners = Prometheus.gaugeBuild("kv_state_machine_listeners", "The number of listeners on Raft's Key/Value state machine");

  /** Keeps track of existing {@link RaftErrorListener} **/
  private static ArrayList<RaftErrorListener> errorListeners = new ArrayList<>();

  /**
   * Keeps track of an additional {@link RaftErrorListener} in this class
   *
   * @param errorListener The error listener to add.
   */
  protected void addErrorListener(RaftErrorListener errorListener) {
    errorListeners.add(errorListener);
  }

  /**
   * Stop listening from a specific {@link RaftErrorListener}
   * @param errorListener The error listener to be removed
   */
  protected void removeErrorListener(RaftErrorListener errorListener) {
    errorListeners.remove(errorListener);
  }

  /**
   * Clears all the {@link RaftErrorListener}s attached to this class.
   */
  protected void clearErrorListeners() {
    errorListeners.clear();
  }

  /**
   * Alert each of the {@link RaftErrorListener}s attached to this class.
   */
  protected void throwRaftError(String incidentKey, String debugMessage) {
    errorListeners.forEach(listener -> listener.accept(incidentKey, debugMessage, Thread.currentThread().getStackTrace()));
  }

  static {
    new KeyValueStateMachine("name").serialize();  // warm up the proto class loaders
  }

  /**
   * This holds a locally registered callback that will be called whenever the state machine changes. This lets code
   * wait for changes to the cluster, and respond appropriately.
   */
  @FunctionalInterface
  public interface ChangeListener {
    /**
     * Run on a value of the state machine changing.
     *
     * @param changedKey The key that changed.
     * @param newValue The new value of the changed key, or {@link Optional#empty()} if we are removing a key.
     * @param state The new complete state of the state machine.
     * @param pool A pool we can use to run sub-jobs. It's the same pool that called this cahgne listener.
     */
    void onChange(String changedKey, Optional<byte[]> newValue, Map<String, byte[]> state, ExecutorService pool);
  }

  /**
   * This holds a value with an optional owner, so that it can be cleaned up automatically when the owner disconnects.
   */
  static class ValueWithOptionalOwner {
    /**
     * The raw value on this Value object.
     */
    final byte[] value;

    /**
     * The optional owner of this value. This is the {@link RaftState#serverName} of the owner.
     */
    final Optional<String> owner;

    /**
     * The last time this element was accessed, as a millisecond value since the epoch.
     */
    long lastAccessed;

    /**
     * The timestamp this object was created, as a millisecond since the epock.
     */
    final long createdAt;

    /** Create a value without an owner. */
    public ValueWithOptionalOwner(byte[] value, long now) {
      this(value, null, now);
    }

    /** Create a value with an owner. */
    public ValueWithOptionalOwner(byte[] value, String owner, long now) {
      this(value, owner, now, now);
    }

    /** The straightforward constructor. */
    ValueWithOptionalOwner(byte[] value, String owner, long now, long createdAt) {
      this.value = value;
      this.owner = Optional.ofNullable(owner);
      this.lastAccessed = now;
      this.createdAt = createdAt;
    }

    /**
     * Get the value of the state machine, but also register the time at which we got it
     */
    public byte[] registerGet(long now) {
      this.lastAccessed = now;
      return value;
    }


    /**
     * Get the number of bytes it takes to represent this value with
     * {@link #serializeInto(byte[], int)}.
     */
    public int byteSize() {
      return 4 + value.length +
          IOUtils.stringSerializedSize(owner.orElse("")) +
          16;
    }


    /**
     * Serialize this value directly into a buffer.
     *
     * @param buffer The buffer we're serializing into.
     * @param begin The position at which we're writing this value.
     *
     * @return The serialized size written
     */
    public int serializeInto(byte[] buffer, int begin) {
      int position = begin;
      // 1. Value
      IOUtils.writeInt(buffer, position, value.length);
      System.arraycopy(value, 0, buffer, position + 4, value.length);
      // 2. Owner
      position += 4 + value.length + IOUtils.writeString(buffer, position + 4 + value.length, owner.orElse(""));
      // 3. Last accessed
      IOUtils.writeLong(buffer, position, this.lastAccessed);
      // 4. Created at
      IOUtils.writeLong(buffer, position + 8, this.createdAt);
      return (position + 16) - begin;
    }


    /**
     * Read a value from a given byte stream, starting at the specified position.
     *
     * @param buffer The byte stream to read this value from.
     * @param begin The position to start reading from.
     *
     * @return The read value.
     */
    public static ValueWithOptionalOwner readFrom(byte[] buffer, int begin) {
      int position = begin;
      // 1. Value
      int valueLength = IOUtils.readInt(buffer, position);
      position += 4;
      byte[] value = new byte[valueLength];
      System.arraycopy(buffer, position, value, 0, valueLength);
      position += valueLength;
      // 2. Owner
      String owner = IOUtils.readString(buffer, position);
      position += IOUtils.stringSerializedSize(owner);
      // 3. Last accessed
      long lastAccessed = IOUtils.readLong(buffer, position);
      position += 8;
      // 4. Created At
      long createdAt = IOUtils.readLong(buffer, position);
      return new ValueWithOptionalOwner(value, owner.length() > 0 ? owner : null, lastAccessed, createdAt);
    }


    /**
     * Write this value as a proto object that can be sent on the wire in, e.g., a snapshot.
     */
    public KeyValueStateMachineProto.ValueWithOptionalOwner serialize() {
      KeyValueStateMachineProto.ValueWithOptionalOwner.Builder builder = KeyValueStateMachineProto.ValueWithOptionalOwner.newBuilder();
      owner.ifPresent(builder::setOwner);
      return builder
          .setValue(ByteString.copyFrom(value))
          .setLastAccessed(this.lastAccessed)
          .setCreatedAt(this.createdAt)
          .build()
          ;
    }

    /**
     * Read this value from a serialized value -- e.g., reading from a snapshot.
     */
    public static ValueWithOptionalOwner deserialize(KeyValueStateMachineProto.ValueWithOptionalOwner value) {
      return new ValueWithOptionalOwner(
          value.getValue().toByteArray(),
          "".equals(value.getOwner()) ? null : value.getOwner(),
          value.getLastAccessed(),
          value.getCreatedAt());
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ValueWithOptionalOwner that = (ValueWithOptionalOwner) o;
      return Arrays.equals(value, that.value) && owner.equals(that.owner);
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
      int result = Arrays.hashCode(value);
      result = 31 * result + owner.hashCode();
      return result;
    }
  }


  /**
   * This is the private implementation for a QueueLock.
   */
  static class QueueLock {
    /** The holder of this lock */
    @Nullable LockRequest holder;

    /** The requesters waiting on this lock */
    final ConcurrentLinkedQueue<LockRequest> waiting;

    /** The set of requesters waiting on the lock. This is the unordered mirror of {@link #waiting}. */
    private final HashSet<LockRequest> waitingSet;

    /** Create a new lock */
    public QueueLock(@Nullable LockRequest holder, List<LockRequest> waiting) {
      this.holder = holder;
      this.waiting = new ConcurrentLinkedQueue<>(waiting);
      this.waitingSet = new HashSet<>(waiting);
    }


    /**
     * Acquire this lock if we don't already have it, or add ourselves to
     * the wait list.
     *
     * @param requestLockRequest The holder trying to acquire the lock.
     *
     * @return The new holder of the lock, if it changed. This is the lock to use for
     *         completing any futures waiting on this lock.
     */
    public synchronized @Nullable LockRequest acquire(LockRequest requestLockRequest) {
      if (holder == null) {
        // Case: no one holds the lock
        holder = requestLockRequest;
        return requestLockRequest;  // we straightforwardly have this lock
      } else if (Objects.equals(holder, requestLockRequest)) {
        // Case: we hold the lock.
        //       In this case, we want to return ourselves, to fire any listeners
        //       waiting on us to take the lock (these fire immediately)
        return holder;
      } else if(!waitingSet.contains(requestLockRequest)) {
        // Case: let's wait on the lock
        waiting.add(requestLockRequest);
        waitingSet.add(requestLockRequest);
        assert waiting.size() == waitingSet.size() : "Waiting and waitset should be in sync";
        return null;
      } else {
        // Case: we're already waiting on the lock
        return null;
      }
    }


    /**
     * Release this lock.
     *
     * @param requestLockRequest The holder releasing the lock
     *
     * @return The new holder of the lock, if it changed. This is the lock to use for
     *         completing any futures waiting on this lock.
     */
    public synchronized @Nullable LockRequest release(LockRequest requestLockRequest) {
      if (Objects.equals(holder, requestLockRequest)) {
        holder = waiting.poll();
        waitingSet.remove(holder);
        return holder;
      } else {
        waiting.remove(requestLockRequest);
        waitingSet.remove(requestLockRequest);
        assert waiting.size() == waitingSet.size() : "Waiting and waitset should be in sync";
        return null;
      }
    }


    /**
     * Release all of the following requests.
     * Note that you're on your own for firing the associated futures
     *
     * @param locks The locks to release
     */
    private synchronized void releaseAll(Collection<LockRequest> locks) {
      waiting.removeIf(locks::contains);
      waitingSet.removeAll(locks);
      if (locks.contains(holder)) {
        holder = waiting.poll();
      }
    }


    /**
     * Stop waiting on this lock if the given condition is true.
     *
     * @param condition The condition to check to determine if we should stop waiting on this lock.
     */
    public synchronized void stopWaitingIf(Predicate<LockRequest> condition) {
      this.waiting.removeIf(condition);
      this.waitingSet.removeIf(condition);
    }


    /**
     * Get the number of bytes it takes to represent this value with
     * {@link #serializeInto(byte[], int)}.
     */
    public int byteSize() {
      int size = 4;
      if (holder != null) {
        size += holder.byteSize();
      }
      for (LockRequest waiting : waitingSet) {
        size += waiting.byteSize();
      }
      return size;
    }


    /**
     * Serialize this value directly into a buffer.
     *
     * @param buffer The buffer we're serializing into.
     * @param begin The position at which we're writing this value.
     */
    public int serializeInto(byte[] buffer, int begin) {
      int position = begin;
      IOUtils.writeInt(buffer, position, (holder != null ? 1 : 0) + waitingSet.size());
      position += 4;
      if (holder != null) {
        position += holder.serializeInto(buffer, position);
      }
      for (LockRequest waiting : waiting) {
        position += waiting.serializeInto(buffer, position);
      }
      return (position - begin);
    }


    /**
     * Read a value from a given byte stream, starting at the specified position.
     *
     * @param buffer The byte stream to read this value from.
     * @param begin The position to start reading from.
     *
     * @return The read value.
     */
    public static QueueLock readFrom(byte[] buffer, int begin) {
      int position = begin;
      int numHolders = IOUtils.readInt(buffer, position);
      position += 4;
      if (numHolders == 0) {
        return new QueueLock(null, Collections.emptyList());
      } else {
        LockRequest holder = LockRequest.readFrom(buffer, position);
        position += holder.byteSize();
        List<LockRequest> waiting = new ArrayList<>();
        for (int i = 1; i < numHolders; ++i) {
          waiting.add(LockRequest.readFrom(buffer, position));
          position += waiting.get(waiting.size() - 1).byteSize();
        }
        return new QueueLock(holder, waiting);
      }
    }


    /** Serialize this lock to proto */
    public synchronized KeyValueStateMachineProto.QueueLock serialize() {
      KeyValueStateMachineProto.QueueLock.Builder builder = KeyValueStateMachineProto.QueueLock.newBuilder();
      if (holder != null) {
        builder.setHolder(holder.serialize());
      }
      builder.addAllWaiting(waiting.stream().map(LockRequest::serialize).collect(Collectors.toList()));
      return builder.build();
    }


    /** Read this lock from a proto */
    public static QueueLock deserialize(KeyValueStateMachineProto.QueueLock queueLock) {
      @Nullable LockRequest holder = null;
      if (queueLock.hasHolder() && queueLock.getHolder() != ai.eloquent.raft.KeyValueStateMachineProto.LockRequest.getDefaultInstance()) {
        holder = LockRequest.deserialize(queueLock.getHolder());
      }
      List<LockRequest> waitingList = queueLock.getWaitingList().stream().map(LockRequest::deserialize).collect(Collectors.toList());
      if (holder == null && !waitingList.isEmpty()) {
        log.warn("Deserialized lock with no holder but a waitlist. Granting lock to {}", waitingList.get(0));
        holder = waitingList.remove(0);
      }
      return new QueueLock(holder, waitingList);
    }


    /** {@inheritDoc} */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      QueueLock queueLock = (QueueLock) o;
      return Objects.equals(holder, queueLock.holder) &&
          Objects.equals(waitingSet, queueLock.waitingSet);
    }


    /** {@inheritDoc} */
    @Override
    public int hashCode() {
      return Objects.hash(holder, waitingSet);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
      return "[holder=" + holder + "; waiting=" + waiting + "]";
    }
  }



  /**
   * This is the private implementation for a LockRequest.
   */
  static class LockRequest {
    /** The server that holds this lock */
    String server;
    /** A unique hash specifying the particular instance of the lock grant this request encodes. */
    String uniqueHash;

    public LockRequest(String server, String uniqueHash) {
      this.server = server;
      this.uniqueHash = uniqueHash;
    }


    /**
     * Get the number of bytes it takes to represent this value with
     * {@link #serializeInto(byte[], int)}.
     */
    public int byteSize() {
      return IOUtils.stringSerializedSize(server) +
             IOUtils.stringSerializedSize(uniqueHash);
    }


    /**
     * Serialize this value directly into a buffer.
     *
     * @param buffer The buffer we're serializing into.
     * @param begin The position at which we're writing this value.
     *
     * @return The number of bytes written to the buffer
     */
    public int serializeInto(byte[] buffer, int begin) {
      int position = begin;
      position += IOUtils.writeString(buffer, position, server);
      position += IOUtils.writeString(buffer, position, uniqueHash);
      return position - begin;
    }


    /**
     * Read a value from a given byte stream, starting at the specified position.
     *
     * @param buffer The byte stream to read this value from.
     * @param begin The position to start reading from.
     *
     * @return The read value.
     */
    public static LockRequest readFrom(byte[] buffer, int begin) {
      int position = begin;
      String server = IOUtils.readString(buffer, position);
      position += IOUtils.stringSerializedSize(server);
      String uniqueHash = IOUtils.readString(buffer, position);
      return new LockRequest(server, uniqueHash);
    }


    /** Write this lock request to a proto */
    public KeyValueStateMachineProto.LockRequest serialize() {
      KeyValueStateMachineProto.LockRequest.Builder builder = KeyValueStateMachineProto.LockRequest.newBuilder();
      builder.setServer(server);
      builder.setUniqueHash(uniqueHash);
      return builder.build();
    }

    /** Read this lock request from a proto*/
    public static LockRequest deserialize(KeyValueStateMachineProto.LockRequest lockRequest) {
      return new LockRequest(lockRequest.getServer(), lockRequest.getUniqueHash());
    }

    @Override
    public String toString() {
      return "LockRequest from "+server+" with "+uniqueHash;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      LockRequest request = (LockRequest) o;
      return uniqueHash.equals(request.uniqueHash) && server.equals(request.server);
    }

    @Override
    public int hashCode() {
      return uniqueHash.hashCode();
    }
  }


  /**
   * This is how we track CompletableFutures that are waiting on a lock being acquired by a given requester.
   */
  private static class NamedLockRequest {
    /** The name of the lock we're taking */
    String lockName;
    /** The requester of the lock */
    String requester;
    /** The unique hash to identify this particular lock from the given server. */
    String uniqueHash;

    /** The straightforward constructor */
    public NamedLockRequest(String lockName, String requester, String uniqueHash) {
      this.lockName = lockName;
      this.requester = requester;
      this.uniqueHash = uniqueHash;
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      NamedLockRequest that = (NamedLockRequest) o;
      return
          Objects.equals(uniqueHash, that.uniqueHash) &&
          Objects.equals(lockName, that.lockName) &&
          Objects.equals(requester, that.requester);
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
      return uniqueHash.hashCode();
    }
  }


  private static class ValueWithOptionalOwnerMapView implements Map<String, byte[]> {
    final Map<String, ValueWithOptionalOwner> backingMap;

    public ValueWithOptionalOwnerMapView(Map<String, ValueWithOptionalOwner> backingMap) {
      this.backingMap = backingMap;
    }

    @Override
    public int size() {
      return this.backingMap.size();
    }

    @Override
    public boolean isEmpty() {
      return this.backingMap.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
      return this.backingMap.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
      return this.backingMap.containsValue(value);
    }

    @Override
    public byte[] get(Object key) {
      return this.backingMap.get(key).value;
    }

    @Nullable
    @Override
    public byte[] put(String key, byte[] value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public byte[] remove(Object key) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(@Nonnull Map<? extends String, ? extends byte[]> m) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
      throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public Set<String> keySet() {
      return this.backingMap.keySet();
    }

    @Nonnull
    @Override
    public Collection<byte[]> values() {
      return this.backingMap.values().stream().map(v -> v.value).collect(Collectors.toList());
    }

    @Nonnull
    @Override
    public Set<Entry<String, byte[]>> entrySet() {
      return this.backingMap.entrySet().stream().map(e -> new Entry<String, byte[]>() {
        @Override
        public String getKey() {
          return e.getKey();
        }

        @Override
        public byte[] getValue() {
          return e.getValue().value;
        }

        @Override
        public byte[] setValue(byte[] value) {
          throw new UnsupportedOperationException();
        }
      }).collect(Collectors.toSet());
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ValueWithOptionalOwnerMapView that = (ValueWithOptionalOwnerMapView) o;
      return Objects.equals(backingMap, that.backingMap);
    }

    @Override
    public int hashCode() {
      return Objects.hash(backingMap);
    }
  }


  /**
   * This is where we store our distributed state.
   */
  final Map<String, ValueWithOptionalOwner> values = new ConcurrentHashMap<>();

  /**
   * This is where we store out distributed locks
   */
  final Map<String, QueueLock> locks = new ConcurrentHashMap<>();

  /**
   * This is where we keep all the lock acquired futures that we need to check for
   */
  final Map<NamedLockRequest, Queue<CompletableFuture<Boolean>>> lockAcquiredFutures = new HashMap<>();

  /**
   * This is where we keep all of the listeners that fire whenever the current state changes
   */
  final Set<ChangeListener> changeListeners = new IdentityHashSet<>();

  /**
   * A map from change listeners to their creation stack trace.
   * Synchronized on {@link #changeListeners}.
   */
  Map<ChangeListener, StackTrace> changeListenerToTrace = new IdentityHashMap<>();

  /**
   * The server that this state machine is running on.
   * @see RaftState#serverName
   */
  public final Optional<String> serverName;

  /** For debugging -- the threads currently in a given function. This is a concurrent hash set.*/
  private final ConcurrentHashMap<Long, String> threadsInFunctions = new ConcurrentHashMap<>();

  /**
   * A function for recomputing the owners of values in this state machine.
   * This is, in general, a pretty slow function, so we defer it to another thread.
   */
  private final DeferredLazy<Set<String>> ownersLazy = new DeferredLazy<Set<String>>() {
    /** {@inheritDoc} */
    @Override
    protected boolean isValid(Set<String> value, long lastComputeTime, long callTime) {
      return callTime - lastComputeTime < 5000;
    }
    /** {@inheritDoc} */
    @Override
    protected Set<String> compute() {
      Set<String> seen = new HashSet<>();
      locks.forEach((key, lock) -> {
        if (lock.holder != null) {
          String holder = lock.holder.server;
          seen.add(holder);
        }
      });
      values.forEach((key, value) -> {
        if (value.owner.isPresent()) {
          String holder = value.owner.get();
          seen.add(holder);
        }
      });
      return seen;
    }
  };


  /** Create a new state machine, with knowledge of what node it's running on */
  public KeyValueStateMachine(String serverName) {
    this.serverName = Optional.ofNullable(serverName);
  }


  /**
   * This serializes the state machine's current state into a proto that can be read from {@link #overwriteWithSerialized(byte[], long, ExecutorService)}.
   *
   * @return a proto of this state machine
   */
  @SuppressWarnings("unused")  // deprecated Dec 29 2018; kept for backwards compatibility
  @Deprecated
  private ByteString serializeProto() {
    KeyValueStateMachineProto.KVStateMachine.Builder builder = KeyValueStateMachineProto.KVStateMachine.newBuilder();
    this.values.forEach((key, value) -> {
      builder.addValuesKeys(key);
      builder.addValuesValues(value.serialize());
    });
    this.locks.forEach((key, value) -> {
      builder.addLocksKeys(key);
      builder.addLocksValues(value.serialize());
    });
    return builder.build().toByteString();
  }


  /**
   * Serialize this state machine directly into a byte array.
   */
  private ByteString serializeBytes() {
    // 1. Compute the size
    int[] size = new int[]{8};  // initially allocate space for 2 4-byte integers
    this.values.forEach((key, value) -> size[0] += IOUtils.stringSerializedSize(key) + value.byteSize());
    this.locks.forEach((key, value) -> size[0] += IOUtils.stringSerializedSize(key) + value.byteSize());
    byte[] buffer = new byte[size[0]];

    // 2. Write the values
    IOUtils.writeInt(buffer, 0, this.values.size());
    int[] position = new int[]{4};  // we start at position 4
    this.values.forEach((key, value) -> {
      position[0] += IOUtils.writeString(buffer, position[0], key);
      position[0] += value.serializeInto(buffer, position[0]);
    });
    IOUtils.writeInt(buffer, position[0], this.locks.size());
    position[0] += 4;
    this.locks.forEach((key, value) -> {
      position[0] += IOUtils.writeString(buffer, position[0], key);
      position[0] += value.serializeInto(buffer, position[0]);
    });

    // 3. Return
    return ByteString.copyFrom(buffer);
  }


  /**
   * This serializes the state machine's current state into a proto that can be read from {@link #overwriteWithSerialized(byte[], long, ExecutorService)}.
   *
   * @return a proto of this state machine
   */
  @Override
  public ByteString serializeImpl() {
    if (!threadsInFunctions.isEmpty()) {
      log.warn("Serializing from {} ('{}') while other thread ({}) is in a critical function ({})", Thread.currentThread().getId(), Thread.currentThread().getName(), threadsInFunctions.keySet().iterator().next(), threadsInFunctions.values().iterator().next());
    }
    threadsInFunctions.put(Thread.currentThread().getId(), "serializeImpl");
    long begin = System.currentTimeMillis();
    try {
      return serializeBytes();
    } finally {
      if (!this.values.isEmpty() && System.currentTimeMillis() - begin > 50) {
        log.warn("Serialization of state machine took {}; {} entries and {} locks", TimerUtils.formatTimeSince(begin), this.values.size(), this.locks.size());
      }
      threadsInFunctions.remove(Thread.currentThread().getId());
    }
  }

  /**
   * Overwrite a given value from the serialized state machine.
   *
   * @param key The key we are overwriting
   * @param value The value we're overwriting
   */
  private void overwriteValue(String key, ValueWithOptionalOwner value) {
    this.values.put(key, value);
  }


  /**
   * Overwrite a given lock from the serialized state machine.
   *
   * @param lockName The name of the lock.
   * @param lock The lock itself.
   * @param pool The pool we should execute lock futures on.
   * @param oldLocks The old locks in the system.
   */
  private void overwriteLock(String lockName, QueueLock lock, ExecutorService pool, Map<String, QueueLock> oldLocks) {
    if (lock.holder != null) {
      this.locks.put(lockName, lock);
    } else {
      log.warn("Deserialized an unheld lock: {}", lockName);
    }
    // 2.2. Execute futures on the lock's holder
    executeFutures(lock.holder, lockName, pool, true);
    // 2.3. Fail futures for anyone no longer waiting
    Set<LockRequest> waiting = Optional.ofNullable(oldLocks.get(lockName)).map(x -> (Set<LockRequest>) new HashSet<>(x.waitingSet)).orElse(Collections.emptySet());
    if (!waiting.isEmpty()) {
      waiting.removeAll(lock.waitingSet);
      if (lock.holder != null) {
        waiting.remove(lock.holder);
      }
      for (LockRequest req : waiting) {
        executeFutures(req, lockName, pool, false);
      }
    }

  }

  /**
   * This overwrites the current state of the state machine with a serialized proto. All the current state of the state
   * machine is overwritten, and the new state is substituted in its place.
   *
   * @param serialized the state machine to overwrite this one with, in serialized form.
   *                   This should be a {@link KeyValueStateMachine}, not a {@link EloquentRaftProto.StateMachine}.
   */
  @SuppressWarnings("unused")  // deprecated Dec 29 2018; kept for backwards compatibility
  @Deprecated
  private void overwriteWithSerializedProto(byte[] serialized, ExecutorService pool) {
    try {
      KeyValueStateMachineProto.KVStateMachine serializedStateMachine = KeyValueStateMachineProto.KVStateMachine.parseFrom(serialized);

      // 1. Overwrite the values
      this.values.clear();
      for (int i = 0; i < serializedStateMachine.getValuesKeysCount(); ++i) {
        ValueWithOptionalOwner value = ValueWithOptionalOwner.deserialize(serializedStateMachine.getValuesValues(i));
        overwriteValue(serializedStateMachine.getValuesKeys(i), value);
      }

      // 2. Overwrite the locks
      Map<String, QueueLock> oldLocks = new HashMap<>(this.locks);
      this.locks.clear();
      for (int i = 0; i < serializedStateMachine.getLocksKeysCount(); ++i) {
        // 2.1. Set the lock
        String lockName = serializedStateMachine.getLocksKeys(i);
        QueueLock lock = QueueLock.deserialize(serializedStateMachine.getLocksValues(i));
        overwriteLock(lockName, lock, pool, oldLocks);
      }
    } catch (InvalidProtocolBufferException e) {
      log.error("Attempting to deserialize an invalid snapshot! This is very bad. Leaving current state unchanged.", e);
    }
  }


  /**
   * Overwrite this state machine with the serialized bytes.
   * Symmetric with {@link #serializeBytes()}.
   */
  private void overwriteWithSerializedBytes(byte[] serialized, ExecutorService pool) {
    int position = 0;

    // 1. Overwrite the values
    int numValues = IOUtils.readInt(serialized, position);
    position += 4;
    this.values.clear();
    for (int i = 0; i < numValues; ++i) {
      String key = IOUtils.readString(serialized, position);
      position += IOUtils.stringSerializedSize(key);
      ValueWithOptionalOwner value = ValueWithOptionalOwner.readFrom(serialized, position);
      position += value.byteSize();
      overwriteValue(key, value);
    }

    // 2. Overwrite the locks
    int numLocks = IOUtils.readInt(serialized, position);
    position += 4;
    Map<String, QueueLock> oldLocks = new HashMap<>(this.locks);
    this.locks.clear();
    for (int i = 0; i < numLocks; ++i) {
      String key = IOUtils.readString(serialized, position);
      position += IOUtils.stringSerializedSize(key);
      QueueLock value = QueueLock.readFrom(serialized, position);
      position += value.byteSize();
      overwriteLock(key, value, pool, oldLocks);
    }
  }


  /**
   * This overwrites the current state of the state machine with a serialized proto. All the current state of the state
   * machine is overwritten, and the new state is substituted in its place.
   *
   * @param serialized the state machine to overwrite this one with, in serialized form.
   *                   This should be a {@link KeyValueStateMachine}, not a {@link EloquentRaftProto.StateMachine}.
   * @param now the current time, for mocking.
   */
  @Override
  public void overwriteWithSerializedImpl(byte[] serialized, long now, ExecutorService pool) {
    if (!threadsInFunctions.isEmpty()) {
      log.warn("Overwriting from {} ('{}') while other thread ({}) is in a critical function ({})", Thread.currentThread().getId(), Thread.currentThread().getName(), threadsInFunctions.keySet().iterator().next(), threadsInFunctions.values().iterator().next());
    }
    threadsInFunctions.put(Thread.currentThread().getId(), "overwriteWithSerializedImpl");
    try {
      overwriteWithSerializedBytes(serialized, pool);
    } finally {
      threadsInFunctions.remove(Thread.currentThread().getId());
    }
  }


  /**
   * This is responsible for applying a transition to the state machine. The transition is assumed to be coming in a
   * proto, and so is serialized as a byte array.
   *
   * @param transition the transition to apply, in serialized form
   * @param now the current timestamp
   * @param pool the pool to run any listeners on
   */
  @Override
  public void applyTransition(byte[] transition, long now, ExecutorService pool) {
    try {
      KeyValueStateMachineProto.Transition serializedTransition = KeyValueStateMachineProto.Transition.parseFrom(transition);
      applyTransition(serializedTransition, now, pool, false);
    } catch (InvalidProtocolBufferException e) {
      log.warn("Attempting to deserialize an invalid transition! This is very bad. Leaving current state unchanged.", e);
    }
  }


  /** @see #applyTransition(byte[], long, ExecutorService) */
  private void applyTransition(KeyValueStateMachineProto.Transition serializedTransition, long now, ExecutorService pool, boolean bypassThreadSafety) {
    if (!bypassThreadSafety) {
      if (!threadsInFunctions.isEmpty()) {
        log.warn("Transitioning from {} ('{}') while other thread ({}) is in a critical function ({})", Thread.currentThread().getId(), Thread.currentThread().getName(), threadsInFunctions.keySet().iterator().next(), threadsInFunctions.values().iterator().next());
      }
      threadsInFunctions.put(Thread.currentThread().getId(), "applyTransition");
    }

    try {
      boolean shouldCallChangeListeners = false;
      @Nullable QueueLock lock;
      switch (serializedTransition.getType()) {

        case TRANSITION_GROUP:
          if (serializedTransition.getTransitionsCount() > 5 && serializedTransition.getTransitionsList().stream().allMatch(x -> x.getType() == KeyValueStateMachineProto.TransitionType.RELEASE_LOCK)) {
            // special case bulk lock releases
            Map<String, Set<LockRequest>> releases = new HashMap<>();
            for (KeyValueStateMachineProto.Transition transition : serializedTransition.getTransitionsList()) {
              KeyValueStateMachineProto.ReleaseLock serializedReleaseLock = transition.getReleaseLock();
              LockRequest releaseLockRequest = new LockRequest(serializedReleaseLock.getRequester(), serializedReleaseLock.getUniqueHash());
              releases.computeIfAbsent(serializedReleaseLock.getLock(), k -> new HashSet<>(serializedTransition.getTransitionsCount())).add(releaseLockRequest);
            }
            for (Map.Entry<String, Set<LockRequest>> entry : releases.entrySet()) {
              lock = locks.get(entry.getKey());
              if (lock != null) {
                lock.releaseAll(entry.getValue());
                for (LockRequest l : entry.getValue()) {
                  if (Objects.equals(l, lock.holder)) {
                    executeFutures(l, entry.getKey(), pool, true);
                  } else {
                    executeFutures(l, entry.getKey(), pool, false);
                  }
                }
                if (lock.holder == null) {
                  locks.remove(entry.getKey());
                }
              } else {
                log.debug("Received bulk release lock on unregistered lock: '{}'", entry.getKey());
              }

            }
          } else {
            // Other bulk transition
            for (KeyValueStateMachineProto.Transition transition : serializedTransition.getTransitionsList()) {
              applyTransition(transition, now, pool, true);
            }
          }
          break;

        case REQUEST_LOCK:
          KeyValueStateMachineProto.RequestLock serializedRequestLock = serializedTransition.getRequestLock();
          LockRequest requestLockRequest = new LockRequest(serializedRequestLock.getRequester(), serializedRequestLock.getUniqueHash());
          lock = locks.computeIfAbsent(serializedRequestLock.getLock(), lockName -> new QueueLock(requestLockRequest, Collections.emptyList()));
          executeFutures(lock.acquire(requestLockRequest), serializedRequestLock.getLock(), pool, true);
          break;

        case RELEASE_LOCK:
          KeyValueStateMachineProto.ReleaseLock serializedReleaseLock = serializedTransition.getReleaseLock();
          lock = locks.get(serializedReleaseLock.getLock());
          if (lock != null) {
            LockRequest releaseLockRequest = new LockRequest(serializedReleaseLock.getRequester(), serializedReleaseLock.getUniqueHash());
            executeFutures(lock.release(releaseLockRequest), serializedReleaseLock.getLock(), pool, true);
            if (lock.holder == null) {
              locks.remove(serializedReleaseLock.getLock());
            }
          } else {
            log.debug("Received release lock on unregistered lock: '{}' (server={}  hash={})",
                serializedReleaseLock.getLock(), serializedReleaseLock.getRequester(), serializedReleaseLock.getUniqueHash());
          }
          break;

        case TRY_LOCK:
          KeyValueStateMachineProto.TryLock serializedTryLock = serializedTransition.getTryLock();
          LockRequest requestTryLock = new LockRequest(serializedTryLock.getRequester(), serializedTryLock.getUniqueHash());
          lock = locks.computeIfAbsent(serializedTryLock.getLock(), lockName -> new QueueLock(requestTryLock, new ArrayList<>()));
          if (lock.holder != null) {
            executeFutures(lock.holder, serializedTryLock.getLock(), pool, true);
          }
          break;

        case SET_VALUE:
          // Set a value
          KeyValueStateMachineProto.SetValue serializedSetValue = serializedTransition.getSetValue();
          ValueWithOptionalOwner valueWithOptionalOwner;
          if (serializedSetValue.getOwner().equals("")) {
            valueWithOptionalOwner = new ValueWithOptionalOwner(serializedSetValue.getValue().toByteArray(), now);
          } else {
            valueWithOptionalOwner = new ValueWithOptionalOwner(serializedSetValue.getValue().toByteArray(), serializedSetValue.getOwner(), now);
          }
          if (!values.containsKey(serializedSetValue.getKey()) || !values.get(serializedSetValue.getKey()).equals(valueWithOptionalOwner)) {
            shouldCallChangeListeners = true;
          }
          values.put(serializedSetValue.getKey(), valueWithOptionalOwner);
          break;

        case REMOVE_VALUE:
          // Remove a value
          KeyValueStateMachineProto.RemoveValue serializedRemoveValue = serializedTransition.getRemoveValue();
          if (values.containsKey(serializedRemoveValue.getKey())) {
            shouldCallChangeListeners = true;
          }
          values.remove(serializedRemoveValue.getKey());
          break;

        case CLEAR_TRANSIENTS:
          // Clear transient entries for a user
          this.clearTransientsFor(serializedTransition.getClearTransients().getOwner(), pool);
          break;

        case UNRECOGNIZED:
        case INVALID:
          // Unknown transition
          log.warn("Unrecognized transition type " + serializedTransition.getType() + "! This is very bad. Leaving current state unchanged.");
          break;
      }

      // If we're changing the map value, then call all the change listeners
      if (shouldCallChangeListeners) {
        Set<ChangeListener> changeListenersCopy;
        synchronized (changeListeners) {
          changeListenersCopy = new HashSet<>(changeListeners);
        }
        if (changeListenersCopy.size() > 0) {
          Map<String, byte[]> asMap = new ValueWithOptionalOwnerMapView(this.values);
          for (ChangeListener listener : changeListenersCopy) {
            if (serializedTransition.getType() == KeyValueStateMachineProto.TransitionType.SET_VALUE) {
              // Set a value
              pool.execute(() -> {
                log.trace("Calling onChange listener (set) {}", listener);
                listener.onChange(serializedTransition.getSetValue().getKey(), Optional.of(serializedTransition.getSetValue().getValue().toByteArray()), asMap, pool);
              });
            } else if (serializedTransition.getType() == KeyValueStateMachineProto.TransitionType.REMOVE_VALUE) {
              // Clear a value
              pool.execute(() -> {
                log.trace("Calling onChange listener (clear) {}", listener);
                listener.onChange(serializedTransition.getRemoveValue().getKey(), Optional.empty(), asMap, pool);
              });
            } else {
              log.warn("We should be calling a change listener, but the transition doesn't seem to warrant an update");
            }
          }
        }
      }

    } finally {
      if (!bypassThreadSafety) {
        threadsInFunctions.remove(Thread.currentThread().getId());
      }
    }
  }


  /**
   * This registers a listener that will be called whenever the key-value store changes.
   *
   * @param changeListener the listener to register
   */
  public void addChangeListener(ChangeListener changeListener) {
    int numListeners;
    synchronized (this.changeListeners) {
      // Add the listener
      this.changeListeners.add(changeListener);
      numListeners = this.changeListeners.size();
      assert this.changeListeners.contains(changeListener);
      this.changeListenerToTrace.put(changeListener, new StackTrace());
      assert this.changeListenerToTrace.containsKey(changeListener);

      // Register the listener in Prometheus
      Prometheus.gaugeSet(gaugeNumListeners, (double) numListeners);
    }

    // Make sure we don't have too many listeners
    if (numListeners > 256) {
      throwRaftError("too-many-raft-listeners-" + SystemUtils.HOST, "Too many Raft listeners: Listener count at : " + numListeners);
    }
  }

  /**
   * This removes a listener that will be called whenever the key-value store changes.
   *
   * @param changeListener the listener to deregister
   */
  public void removeChangeListener(ChangeListener changeListener) {
    // Deregister the listener.
    int numListeners;
    synchronized (this.changeListeners) {
      if (!this.changeListeners.remove(changeListener)) {
        log.warn("Removing a change listener that isn't registered");
      }
      if (this.changeListenerToTrace.remove(changeListener) == null) {
        log.warn("Could not find change listener in stack trace mapping");
      }
      numListeners = this.changeListeners.size();
    }

    // Deregister the listener in Prometheus
    Prometheus.gaugeSet(gaugeNumListeners, (double) numListeners);
  }


  /**
   * This gets a value from the values map, if it's present. Otherwise returns empty.
   *
   * @param key The key to retrieve.
   * @param now The current time, so that we can mock transport time if appropriate.
   *
   * @return the value, or empty
   */
  public Optional<byte[]> get(String key, long now) {
    return Optional.ofNullable(values.getOrDefault(key, null)).map(v -> v.registerGet(now));
    /*
    if (values.containsKey(key)) return Optional.of(values.get(key).registerGet(now));
    else return Optional.empty();
    */
  }

  /**
   * This gets the current set of keys in the state machine.
   */
  public Collection<String> keys() {
    return values.keySet();
  }


  /**
   * This returns a copy of the key-&gt;value map in the state machine.
   */
  public Map<String, byte[]> map() {
    return new ValueWithOptionalOwnerMapView(new HashMap<>(this.values));
  }


  /**
   * Returns entries which have not been modified in at least |age| amount of time.
   *
   * @param idleTime The amount of time an entry must have been idle in the state machine.
   * @param now The current time.
   */
  public Set<String> keysIdleSince(Duration idleTime, long now) {
    Set<String> keys = new HashSet<>();
    for (Map.Entry<String, ValueWithOptionalOwner> entry : values.entrySet()) {
      if (entry.getValue().lastAccessed + idleTime.toMillis() < now) {
        keys.add(entry.getKey());
      }
    }
    return keys;
  }


  /**
   * Returns entries which have not been modified in at least |age| amount of time.
   *
   * @param timeInRaft The amount of time an entry must have been in the state machine.
   * @param now The current time.
   */
  public Set<String> keysPresentSince(Duration timeInRaft, long now) {
    Set<String> keys = new HashSet<>();
    for (Map.Entry<String, ValueWithOptionalOwner> entry : values.entrySet()) {
      if (entry.getValue().createdAt + timeInRaft.toMillis() < now) {
        keys.add(entry.getKey());
      }
    }
    return keys;
  }


  /**
   * This returns the full list of entries in the state machine.
   * This is threadsafe, insofar as it comes from a {@link ConcurrentHashMap}.
   */
  public Collection<Map.Entry<String, ValueWithOptionalOwner>> entries() {
    return values.entrySet();
  }


  /**
   * This gets called when we detect that a machine has gone down, and we should remove all of the transient entries
   * and locks pertaining to that machine.
   *
   * @param owner The machine that has gone down that we should clear.
   */
  private void clearTransientsFor(String owner, ExecutorService pool) {
    // 1. Error checks
    if (serverName.map(sn -> Objects.equals(sn, owner)).orElse(false)) {
      log.warn("Got a Raft transition telling us we're offline. We are, of course, not offline. All transient state owned by us is being cleared.");
    }

    // 2. Remove any values that are owned by people who are now disconnected
    Set<String> keysToRemove = new HashSet<>();
    values.forEach((key, value) -> {
      if (value.owner.map(x -> Objects.equals(x, owner)).orElse(false)) {
        keysToRemove.add(key);
      }
    });
    for (String key : keysToRemove) {
      values.remove(key);
    }

    // 3. Scrub any mention of people who are no longer in the committedClusterMembers from the locks
    for (Map.Entry<String, QueueLock> entry : this.locks.entrySet()) {
      QueueLock lock = entry.getValue();
      // 3.1. Stop waiting on the lock
      lock.stopWaitingIf(req -> Objects.equals(req.server, owner));
      // 3.2. Release the lock if we hold it
      if (lock.holder != null && Objects.equals(lock.holder.server, owner)) {
        executeFutures(lock.release(lock.holder), entry.getKey(), pool, true);
      }
    }
  }


  /** {@inheritDoc} */
  @Override
  public Set<String> owners(long now) {
    if (this.values.size() < 10 && this.locks.size() < 10) {  // for very small stat machines, may as well behave synchronously
      return ownersLazy.getSync(false, now);
    } else {
      return ownersLazy.get(false, now).orElse(Collections.emptySet());
    }
  }


  /** {@inheritDoc} */
  @Override
  public String debugTransition(byte[] transition) {
    try {
      KeyValueStateMachineProto.Transition serializedTransition = KeyValueStateMachineProto.Transition.parseFrom(transition);

      if (serializedTransition.getType() == KeyValueStateMachineProto.TransitionType.REQUEST_LOCK) {
        return serializedTransition.getRequestLock().getRequester()+" requests lock '"+serializedTransition.getRequestLock().getLock()+"' with hash "+serializedTransition.getRequestLock().getUniqueHash();
      } else if (serializedTransition.getType() == KeyValueStateMachineProto.TransitionType.RELEASE_LOCK) {
        return serializedTransition.getReleaseLock().getRequester()+" releases lock '"+serializedTransition.getReleaseLock().getLock()+"' with hash "+serializedTransition.getReleaseLock().getUniqueHash();
      } else if (serializedTransition.getType() == KeyValueStateMachineProto.TransitionType.SET_VALUE) {
        return "set "+serializedTransition.getSetValue().getKey()+" = '"+serializedTransition.getSetValue().getValue().toStringUtf8() + "'";
      } else if (serializedTransition.getType() == KeyValueStateMachineProto.TransitionType.REMOVE_VALUE) {
        return "remove "+serializedTransition.getRemoveValue().getKey();
      } else {
        return "Unrecognized - type "+serializedTransition.getType();
      }
    } catch (InvalidProtocolBufferException e) {
      return "Unrecognized - invalid proto";
    }
  }


  /**
   * Execute any outstanding futures on a newly acquired lock.
   *
   * @param holder The new lock holder.
   * @param lockName The name of the lock we're considering.
   * @param pool The pool to use to execute the futures on.
   * @param result The result to execute the future with.
   */
  private void executeFutures(@Nullable LockRequest holder, String lockName, ExecutorService pool, boolean result) {
    if (holder == null) {
      return;
    }
    Queue<CompletableFuture<Boolean>> futures;
    synchronized (lockAcquiredFutures) {
      futures = lockAcquiredFutures.remove(new NamedLockRequest(lockName, holder.server, holder.uniqueHash));
    }
    if (futures != null) {
      futures.forEach(future -> pool.execute(() -> {
        if (!future.isDone()) {
          if (!result) {
            log.warn("Failing future for lock '{}'", lockName);
          }
          future.complete(result);
        }
      }));
    }
  }


  /** {@inheritDoc} */
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    KeyValueStateMachine that = (KeyValueStateMachine) o;
    return Objects.equals(values, that.values) && Objects.equals(locks, that.locks);
  }


  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return Objects.hash(values, locks);
  }


  /**
   * <p>
   *   This creates a future that completes as soon as the specified requester is granted the specified lock in our
   *   committed view of the world. This completes with true once you've acquired the lock, or false if it appears that
   *   you are no longer waiting on the lock (for example, if you were disconnected from the cluster while waiting your
   *   request for the lock could have been deleted).
   * </p>
   *
   * <p>
   *   If this lock does not exist in {@link #locks} yet, we create the lock lazily and wait on it.
   *   This means that <b>waiting on a lock that has already been released or will never be acquired will wait forever</b>.
   * </p>
   *
   * @param lock the lock we're interested in
   * @param requester the requester who will acquire the lock
   * @param uniqueHash the unique hash to deduplicate requests on the same machine.
   *
   * @return a Future that completes when the lock is acquired by requester.
   */
  CompletableFuture<Boolean> createLockAcquiredFuture(String lock, String requester, String uniqueHash) {
    CompletableFuture<Boolean> lockAcquiredFuture = new CompletableFuture<>();
    NamedLockRequest request = new NamedLockRequest(lock, requester, uniqueHash);
    boolean shouldCompleteTrue = false;
    synchronized (this.lockAcquiredFutures) {
      // 1. Register the future
      // note[gabor] even if it's already completed, we want to register it here now in case
      // something changes before we check. We'll remove it later.
      // This relies on the fact that double-completing a future is harmless.
      //
      // In more detail, we can argue in cases:
      //   1. If at this point the lock is not held yet, then there is some future point when it will be
      //      acquired and the future will fire then.
      //   2. If at this point the lock is held, we'll pick it up below.
      //   3. DISALLOWED: if the lock releases before we get the lock, we have an error, but then
      //      we shouldn't fire the future anyways because we don't have the lock.
      this.lockAcquiredFutures.computeIfAbsent(request, k -> new ConcurrentLinkedQueue<>()).add(lockAcquiredFuture);

      // 2. Check for immediate completion
      QueueLock lockObj = locks.get(lock);
      if (lockObj != null) {
        LockRequest holder = lockObj.holder;
        if (holder != null) {
          if (Objects.equals(requester, holder.server) && Objects.equals(uniqueHash, holder.uniqueHash)) {
            // 3. If we just completed the future, remove it from the waitlist
            Queue<CompletableFuture<Boolean>> futures = this.lockAcquiredFutures.get(request);
            // 3.1. Remove the future from the queue
            if (futures != null) {
              futures.remove(lockAcquiredFuture);
              // 3.2. Remove the future from the map, if applicable
              if (futures.isEmpty()) {
                this.lockAcquiredFutures.remove(request);
              }
            }
            // 3.3. Mark for completion
            shouldCompleteTrue = true;
          }
        }
      }
    }

    // 3. Complete the future
    if (shouldCompleteTrue && !lockAcquiredFuture.isDone()) {
      lockAcquiredFuture.complete(true);
    }

    // 4. Return
    return lockAcquiredFuture;
  }


  /**
   * This creates a serialized RequestLock transition.
   *
   * @param lock the lock name
   * @param requester the requester of the lock
   * @param uniqueHash the unique hash to deduplicate requests on the same machine
   * @return a serialized RequestLock transition
   */
  public static byte[] createRequestLockTransition(String lock, String requester, String uniqueHash) {
    KeyValueStateMachineProto.Transition.Builder transitionBuilder = KeyValueStateMachineProto.Transition.newBuilder();
    transitionBuilder.setType(KeyValueStateMachineProto.TransitionType.REQUEST_LOCK);

    KeyValueStateMachineProto.RequestLock.Builder requestLockBuilder = KeyValueStateMachineProto.RequestLock.newBuilder();
    requestLockBuilder.setLock(lock);
    requestLockBuilder.setRequester(requester);
    requestLockBuilder.setUniqueHash(uniqueHash);
    transitionBuilder.setRequestLock(requestLockBuilder);

    return transitionBuilder.build().toByteArray();
  }

  /**
   * This creates a serialized TryLock transition.
   *
   * @param lock the lock name
   * @param requester the requester of the lock
   * @param uniqueHash the unique hash to deduplicate requests on the same machine
   * @return a serialized RequestLock transition
   */
  public static byte[] createTryLockTransition(String lock, String requester, String uniqueHash) {
    KeyValueStateMachineProto.Transition.Builder transitionBuilder = KeyValueStateMachineProto.Transition.newBuilder();
    transitionBuilder.setType(KeyValueStateMachineProto.TransitionType.TRY_LOCK);

    KeyValueStateMachineProto.TryLock.Builder tryLockBuilder = KeyValueStateMachineProto.TryLock.newBuilder();
    tryLockBuilder.setLock(lock);
    tryLockBuilder.setRequester(requester);
    tryLockBuilder.setUniqueHash(uniqueHash);
    transitionBuilder.setTryLock(tryLockBuilder);

    return transitionBuilder.build().toByteArray();
  }

  /**
   * This creates a serialized ReleaseLock transition.
   *
   * @param lock the lock name
   * @param requester the requester of the lock
   * @param uniqueHash the unique hash to deduplicate requests on the same machine
   * @return a serialized ReleaseLock transition
   */
  public static byte[] createReleaseLockTransition(String lock, String requester, String uniqueHash) {
    KeyValueStateMachineProto.Transition.Builder transitionBuilder = KeyValueStateMachineProto.Transition.newBuilder();
    transitionBuilder.setType(KeyValueStateMachineProto.TransitionType.RELEASE_LOCK);

    KeyValueStateMachineProto.ReleaseLock.Builder requestLockBuilder = KeyValueStateMachineProto.ReleaseLock.newBuilder();
    requestLockBuilder.setLock(lock);
    requestLockBuilder.setRequester(requester);
    requestLockBuilder.setUniqueHash(uniqueHash);
    transitionBuilder.setReleaseLock(requestLockBuilder);

    return transitionBuilder.build().toByteArray();
  }

  /**
   * Creates a grouped transition, which executes several transitions atomically.
   *
   * @param transitions the transitions (serialized) to group
   * @return a grouped transition
   */
  public static byte[] createGroupedTransition(byte[]... transitions) {
    KeyValueStateMachineProto.Transition.Builder transitionBuilder = KeyValueStateMachineProto.Transition.newBuilder();
    transitionBuilder.setType(KeyValueStateMachineProto.TransitionType.TRANSITION_GROUP);

    for (byte[] transition : transitions) {
      try {
        transitionBuilder.addTransitions(KeyValueStateMachineProto.Transition.parseFrom(transition));
      } catch (InvalidProtocolBufferException e) {
        log.warn("Unable to parse an element of a grouped transition: ", e);
      }
    }

    return transitionBuilder.build().toByteArray();
  }


  /**
   * This creates a serialized SetValue transition that will set an entry in the values map, with an "owner" who is
   * responsible for the value, which will be automatically cleaned up when the owner disconnects from the cluster.
   *
   * @param key the key to set
   * @param value the value, as a raw byte array
   * @param owner the owner of this key-value pair
   * @return a serialized SetValue transition
   */
  public static byte[] createSetValueTransitionWithOwner(String key, byte[] value, String owner) {
    KeyValueStateMachineProto.Transition.Builder transitionBuilder = KeyValueStateMachineProto.Transition.newBuilder();
    transitionBuilder.setType(KeyValueStateMachineProto.TransitionType.SET_VALUE);

    KeyValueStateMachineProto.SetValue.Builder setValueBuilder = KeyValueStateMachineProto.SetValue.newBuilder();
    setValueBuilder.setKey(key);
    setValueBuilder.setValue(ByteString.copyFrom(value));
    setValueBuilder.setOwner(owner);
    transitionBuilder.setSetValue(setValueBuilder);

    return transitionBuilder.build().toByteArray();
  }

  /**
   * This creates a serialized SetValue transition that will set an entry in the values map.
   *
   * @param key the key to set
   * @param value the value, as a raw byte array
   * @return a serialized SetValue transition
   */
  public static byte[] createSetValueTransition(String key, byte[] value) {
    KeyValueStateMachineProto.Transition.Builder transitionBuilder = KeyValueStateMachineProto.Transition.newBuilder();
    transitionBuilder.setType(KeyValueStateMachineProto.TransitionType.SET_VALUE);

    KeyValueStateMachineProto.SetValue.Builder setValueBuilder = KeyValueStateMachineProto.SetValue.newBuilder();
    setValueBuilder.setKey(key);
    setValueBuilder.setValue(ByteString.copyFrom(value));
    transitionBuilder.setSetValue(setValueBuilder);

    return transitionBuilder.build().toByteArray();
  }

  /**
   * This creates a serialized RemoveValue transition that will delete an entry in the map, if it's currently present.
   *
   * @param key the key to remove
   * @return a serialized RemoveValue transition
   */
  public static byte[] createRemoveValueTransition(String key) {
    KeyValueStateMachineProto.Transition.Builder transitionBuilder = KeyValueStateMachineProto.Transition.newBuilder();
    transitionBuilder.setType(KeyValueStateMachineProto.TransitionType.REMOVE_VALUE);

    KeyValueStateMachineProto.RemoveValue.Builder removeValueBuilder = KeyValueStateMachineProto.RemoveValue.newBuilder();
    removeValueBuilder.setKey(key);
    transitionBuilder.setRemoveValue(removeValueBuilder);

    return transitionBuilder.build().toByteArray();
  }


  /**
   * This creates a serialized ClearTransient transition.
   *
   * @param owner the owner we should clear transient values for.
   *
   * @return a serialized ClearTransient transition
   */
  public static byte[] createClearTransition(String owner) {
    return KeyValueStateMachineProto.Transition.newBuilder()
        .setType(KeyValueStateMachineProto.TransitionType.CLEAR_TRANSIENTS)
        .setClearTransients(KeyValueStateMachineProto.ClearTransients.newBuilder()
            .setOwner(owner)
        )
        .build().toByteArray();
  }
}
