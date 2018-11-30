package ai.eloquent.raft;

import ai.eloquent.error.RaftErrorListener;
import ai.eloquent.monitoring.Prometheus;
import ai.eloquent.util.IdentityHashSet;
import ai.eloquent.util.StackTrace;
import ai.eloquent.util.SystemUtils;
import ai.eloquent.util.TimerUtils;
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
import java.util.concurrent.ExecutorService;
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
    private ValueWithOptionalOwner(byte[] value, String owner, long now, long createdAt) {
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
    Optional<LockRequest> holder;
    List<LockRequest> waiting;

    public QueueLock(Optional<LockRequest> holder, List<LockRequest> waiting) {
      this.holder = holder;
      this.waiting = waiting;
    }

    public KeyValueStateMachineProto.QueueLock serialize() {
      KeyValueStateMachineProto.QueueLock.Builder builder = KeyValueStateMachineProto.QueueLock.newBuilder();
      holder.ifPresent(h -> builder.setHolder(h.serialize()));
      builder.addAllWaiting(waiting.stream().map(LockRequest::serialize).collect(Collectors.toList()));
      return builder.build();
    }

    public static QueueLock deserialize(KeyValueStateMachineProto.QueueLock queueLock) {
      Optional<LockRequest> holder;
      if (queueLock.hasHolder()) {
        holder = Optional.of(LockRequest.deserialize(queueLock.getHolder()));
      }
      else  {
        holder = Optional.empty();
      }
      List<LockRequest> waitingList = queueLock.getWaitingList().stream().map(LockRequest::deserialize).collect(Collectors.toList());
      return new QueueLock(holder, waitingList);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      QueueLock queueLock = (QueueLock) o;
      return Objects.equals(holder, queueLock.holder) &&
          Objects.equals(waiting, queueLock.waiting);
    }

    @Override
    public int hashCode() {
      return Objects.hash(holder, waiting);
    }
  }

  /**
   * This is the private implementation for a LockRequest.
   */
  static class LockRequest {
    String server;
    String uniqueHash;

    public LockRequest(String server, String uniqueHash) {
      this.server = server;
      this.uniqueHash = uniqueHash;
    }

    public KeyValueStateMachineProto.LockRequest serialize() {
      KeyValueStateMachineProto.LockRequest.Builder builder = KeyValueStateMachineProto.LockRequest.newBuilder();
      builder.setServer(server);
      builder.setUniqueHash(uniqueHash);
      return builder.build();
    }

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
      return server.equals(request.server) && uniqueHash.equals(request.uniqueHash);
    }

    @Override
    public int hashCode() {
      int result = server.hashCode();
      result = 31 * result + uniqueHash.hashCode();
      return result;
    }
  }


  /**
   * This is how we track CompletableFutures that are waiting on a lock being acquired by a given requester.
   */
  private static class LockAcquiredFuture {
    String lock;
    String requester;
    String uniqueHash;
    CompletableFuture<Boolean> future;

    public LockAcquiredFuture(String lock, String requester, String uniqueHash, CompletableFuture<Boolean> future) {
      this.lock = lock;
      this.requester = requester;
      this.uniqueHash = uniqueHash;
      this.future = future;
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
  final List<LockAcquiredFuture> lockAcquiredFutures = new ArrayList<>();

  /**
   * This is where we keep all of the listeners that fire whenever the current state changes
   */
  final Set<ChangeListener> changeListeners = new IdentityHashSet<>();

  /**
   * A map from change listeners to their creation stack trace.
   */
  Map<ChangeListener, StackTrace> changeListenerToTrace = new IdentityHashMap<>();

  /**
   * The server that this state machine is running on.
   * @see RaftState#serverName
   */
  public final Optional<String> serverName;


  /** Create a new state machine, with knowledge of what node it's running on */
  public KeyValueStateMachine(String serverName) {
    this.serverName = Optional.ofNullable(serverName);
  }


  /**
   * This serializes the state machine's current state into a proto that can be read from {@link #overwriteWithSerialized(byte[], long, ExecutorService)}.
   *
   * @return a proto of this state machine
   */
  @Override
  public synchronized ByteString serializeImpl() {
    long begin = System.currentTimeMillis();
    try {
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

    } finally {
      if (!this.values.isEmpty() && System.currentTimeMillis() - begin > 10) {
        log.warn("Serialization of state machine took {}; {} entries and {} locks", TimerUtils.formatTimeSince(begin), this.values.size(), this.locks.size());
      }
    }
  }

  /**
   * This overwrites the current state of the state machine with a serialized proto. All the current state of the state
   * machine is overwritten, and the new state is substituted in its place.
   *
   * @param serialized the state machine to overwrite this one with, in serialized form
   * @param now the current time, for mocking.
   */
  @Override
  public synchronized void overwriteWithSerializedImpl(byte[] serialized, long now, ExecutorService pool) {
    try {
      KeyValueStateMachineProto.KVStateMachine serializedStateMachine = KeyValueStateMachineProto.KVStateMachine.parseFrom(serialized);

      this.values.clear();
      for (int i = 0; i < serializedStateMachine.getValuesKeysCount(); ++i) {
        ValueWithOptionalOwner value = ValueWithOptionalOwner.deserialize(serializedStateMachine.getValuesValues(i));
        this.values.put(serializedStateMachine.getValuesKeys(i), value);
      }

      this.locks.clear();
      for (int i = 0; i < serializedStateMachine.getLocksKeysCount(); ++i) {
        this.locks.put(serializedStateMachine.getLocksKeys(i), QueueLock.deserialize(serializedStateMachine.getLocksValues(i)));
      }

      checkLocksAcquired(pool);
    } catch (InvalidProtocolBufferException e) {
      log.error("Attempting to deserialize an invalid snapshot! This is very bad. Leaving current state unchanged.", e);
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
      applyTransition(serializedTransition, now, pool);
    } catch (InvalidProtocolBufferException e) {
      log.warn("Attempting to deserialize an invalid transition! This is very bad. Leaving current state unchanged.", e);
    }
  }

  public void applyTransition(KeyValueStateMachineProto.Transition serializedTransition, long now, ExecutorService pool) {
    boolean shouldCallChangeListeners = false;
    switch (serializedTransition.getType()) {

      case TRANSITION_GROUP:
        for (KeyValueStateMachineProto.Transition transition : serializedTransition.getTransitionsList()) {
          applyTransition(transition, now, pool);
        }
        break;

      case REQUEST_LOCK:
        KeyValueStateMachineProto.RequestLock serializedRequestLock = serializedTransition.getRequestLock();
        LockRequest requestLockRequest = new LockRequest(serializedRequestLock.getRequester(), serializedRequestLock.getUniqueHash());
        synchronized (this) {
          // 1. If the QueueLock is not currently in the locks map in StateMachine (means it is currently not held), then the
          //    requester gets it immediately.
          if (!locks.containsKey(serializedRequestLock.getLock())) {
            QueueLock lock = new QueueLock(Optional.of(requestLockRequest), new ArrayList<>());
            locks.put(serializedRequestLock.getLock(), lock);
          }
          // 2. Otherwise, add the requester to the waiting list of the QueueLock
          else {
            QueueLock lock = locks.get(serializedRequestLock.getLock());
            if (!lock.waiting.contains(requestLockRequest) && (!lock.holder.isPresent() || !lock.holder.get().equals(requestLockRequest))) {
              lock.waiting.add(requestLockRequest);
            }
          }
          // 3. Check if there are any CompletableFutures outstanding waiting for this lock to be acquired
          checkLocksAcquired(pool, serializedRequestLock.getLock());
        }
        break;

      case RELEASE_LOCK:
        KeyValueStateMachineProto.ReleaseLock serializedReleaseLock = serializedTransition.getReleaseLock();
        synchronized (this) {
          if (locks.containsKey(serializedReleaseLock.getLock())) {
            QueueLock lock = locks.get(serializedReleaseLock.getLock());
            // 1. If the QueueLock is held by a different requester, this is a no-op
            //noinspection ConstantConditions,OptionalGetWithoutIsPresent
            if (lock.holder.isPresent() && lock.holder.get().server.equals(serializedReleaseLock.getRequester()) && lock.holder.get().uniqueHash.equals(serializedReleaseLock.getUniqueHash())) {
              // 2. Release the lock
              releaseLock(serializedReleaseLock.getLock(), pool);
            } else {
              // 3. Check if the list of waiters on the lock contains the releaser, if so stop waiting
              Optional<LockRequest> requestToRemove = Optional.empty();
              for (LockRequest request : lock.waiting) {
                if (request.server.equals(serializedReleaseLock.getRequester()) && request.uniqueHash.equals(serializedReleaseLock.getUniqueHash())) {
                  requestToRemove = Optional.of(request);
                }
              }
              requestToRemove.ifPresent(lockRequest -> lock.waiting.remove(lockRequest));

              if (!requestToRemove.isPresent()) {
                log.warn("Received a release lock command that will not result in any action - this is fine, but should be rare");
              }
            }
          } else {
            log.warn("Received a release lock command that will not result in any action - this is fine, but should be rare");
          }
        }
        break;

      case TRY_LOCK:
        KeyValueStateMachineProto.TryLock serializedTryLock = serializedTransition.getTryLock();
        LockRequest requestTryLock = new LockRequest(serializedTryLock.getRequester(), serializedTryLock.getUniqueHash());
        synchronized (this) {
          // 1. If the QueueLock is not currently in the locks map in StateMachine (means it is currently not held), then the
          //    requester gets it immediately.
          if (!locks.containsKey(serializedTryLock.getLock())) {
            QueueLock lock = new QueueLock(Optional.of(requestTryLock), new ArrayList<>());
            locks.put(serializedTryLock.getLock(), lock);
          }
          // 2. Check if there are any CompletableFutures outstanding waiting for this lock to be acquired
          checkLocksAcquired(pool, serializedTryLock.getLock());
        }
        break;

      case SET_VALUE:
        // Set a value
        KeyValueStateMachineProto.SetValue serializedSetValue = serializedTransition.getSetValue();
        ValueWithOptionalOwner valueWithOptionalOwner;
        if (serializedSetValue.getOwner().equals("")) {
          valueWithOptionalOwner = new ValueWithOptionalOwner(serializedSetValue.getValue().toByteArray(), now);
        }
        else {
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
        synchronized (this) {
          this.clearTransientsFor(serializedTransition.getClearTransients().getOwner(), pool);
        }
        break;

      case UNRECOGNIZED:
      case INVALID:
        // Unknown transition
        log.warn("Unrecognized transition type "+serializedTransition.getType()+"! This is very bad. Leaving current state unchanged.");
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
  public void removeChangeListener(ChangeListener changeListener) {synchronized (this.changeListeners) {
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
   * This releases a lock in the state machine.
   */
  private void releaseLock(String lockName, ExecutorService pool) {
    if (locks.containsKey(lockName)) {
      QueueLock lock = locks.get(lockName);

      if (lock.waiting.size() > 0) {
        // 1. If the QueueLock waiting list is non-empty, then the next in the waiting list gets the lock immediately, and is
        //    removed from the waiting list.
        LockRequest nextUp = lock.waiting.get(0);
        lock.waiting.remove(0);
        lock.holder = Optional.of(nextUp);
      } else {
        // 2. Otherwise, remove the lock from the locks map in StateMachine
        locks.remove(lockName);
      }
    }

    // 3. Releasing the lock may trigger a Future
    checkLocksAcquired(pool, lockName);
  }


  /**
   * This gets called when we detect that a machine has gone down, and we should remove all of the transient entries
   * and locks pertaining to that machine.
   *
   * @param owner The machine that has gone down that we should clear.
   */
  private synchronized void clearTransientsFor(String owner, ExecutorService pool) {
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
    Map<String, QueueLock> locks = new HashMap<>(this.locks); // copy the locks map to avoid ConcurrentModificationException when releaseLock() modifies locks below
    locks.values()
        .forEach(lock -> lock.waiting.removeIf(req -> Objects.equals(req.server, owner)));
    locks.entrySet().stream()
        .filter(lock -> lock.getValue().holder.map(h -> Objects.equals(h.server, owner)).orElse(false))
        .forEach(x -> releaseLock(x.getKey(), pool));
  }


  /** {@inheritDoc} */
  @Override
  public Set<String> owners() {
    Set<String> seen = new HashSet<>();
    locks.forEach((key, lock) -> {
      if (lock.holder.isPresent()) {
        String holder = lock.holder.get().server;
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
   * Release a single lock.
   */
  private void completeLockFuture(@Nullable ExecutorService pool, QueueLock lock, LockAcquiredFuture future) {
    if (lock.holder.isPresent() && lock.holder.get().server.equals(future.requester) && lock.holder.get().uniqueHash.equals(future.uniqueHash)) {
      synchronized (lockAcquiredFutures) {
        lockAcquiredFutures.remove(future);
      }
      if (pool == null) {
        future.future.complete(true);
      } else {
        pool.execute(() -> future.future.complete(true));
      }
    } else if (!lock.holder.isPresent()) {
      // Lock has no holder, so if we're waiting on it that implies we're hosed
      synchronized (lockAcquiredFutures) {
        lockAcquiredFutures.remove(future);
      }
      if (pool == null) {
        future.future.complete(false);
      } else {
        pool.execute(() -> future.future.complete(false));
      }
    }
    // Lock has a holder, and that holder isn't us. Continue waiting
  }


  private void checkLocksAcquired(ExecutorService pool, String lockName) {
    List<LockAcquiredFuture> originalLockAcquiredFutures;
    synchronized (lockAcquiredFutures) {
      originalLockAcquiredFutures = new ArrayList<>(lockAcquiredFutures);
    }
    for (LockAcquiredFuture future : new ArrayList<>(originalLockAcquiredFutures)) {
      if (lockName.equals(future.lock)) {
        QueueLock lock = locks.get(lockName);
        if (lock != null) {
          // Case: try to complete this lock's future
          completeLockFuture(pool, lock, future);
        } else {
          // Case: this lock no longer exists -- it must be released.
          pool.execute(() -> future.future.complete(false));
        }
      }

    }
  }


  /**
   * This iterates over our list of LockAcquiredFutures and completes and removes any of them that are now satisfied.
   */
  private void checkLocksAcquired(ExecutorService pool) {
    List<LockAcquiredFuture> originalLockAcquiredFutures;
    synchronized (lockAcquiredFutures) {
      originalLockAcquiredFutures = new ArrayList<>(lockAcquiredFutures);
    }
    List<LockAcquiredFuture> toRemove = new ArrayList<>();
    for (LockAcquiredFuture future : new ArrayList<>(originalLockAcquiredFutures)) {
      QueueLock lock = locks.get(future.lock);
      if (lock != null) {
        completeLockFuture(pool, lock, future);
      } else {
        // Lock was removed, so this is impossible to acquire
        toRemove.add(future);
        pool.execute(() -> future.future.complete(false));
      }
    }
    synchronized (lockAcquiredFutures) {
      lockAcquiredFutures.removeAll(toRemove);
    }
  }


  /** {@inheritDoc} */
  @Override
  public synchronized boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    KeyValueStateMachine that = (KeyValueStateMachine) o;
    return Objects.equals(values, that.values) && Objects.equals(locks, that.locks);
  }


  /** {@inheritDoc} */
  @Override
  public synchronized int hashCode() {
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
   * @param uniqueHash the unique hash to deduplicate requests on the same machine
   *
   * @return a Future that completes when the lock is acquired by requester.
   */
  CompletableFuture<Boolean> createLockAcquiredFuture(String lock, String requester, String uniqueHash) {
    // 1. Register the future
    CompletableFuture<Boolean> lockAcquiredFuture = new CompletableFuture<>();
    LockAcquiredFuture lockAcquiredFutureObj = new LockAcquiredFuture(lock, requester, uniqueHash, lockAcquiredFuture);
    synchronized (lockAcquiredFutures) {
      this.lockAcquiredFutures.add(lockAcquiredFutureObj);
    }

    // 2. Check for immediate completiong
    QueueLock lockObj = locks.get(lock);
    if (lockObj != null) {
      completeLockFuture(null, lockObj, lockAcquiredFutureObj);
    }

    // 3. Return
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
        log.warn("Unable to parse");
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
