package ai.eloquent.raft;

import ai.eloquent.util.*;
import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.lang.ref.WeakReference;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * A cache that keeps values in Raft, but saves them to a given persistence store
 * when they have been unmodified in Raft for too long.
 *
 * @author <a href="mailto:gabor@eloquent.ai">Gabor Angeli</a>
 */
public abstract class RaftBackedCache<V> implements Iterable<Map.Entry<String,V>>, AutoCloseable {
  /**
   * An SLF4J Logger for this class.
   */
  private static final Logger log = LoggerFactory.getLogger(RaftBackedCache.class);

  private static final int DEFAULT_MAX_SIZE_BYTES = 0x1 << 20; // 1MB by default

  /**
   * The implementing Raft for the cache.
   */
  public final Theseus raft;

  /**
   * This manages evicting elements from the RaftBackedCache. We hold onto a handle so we can cancel() it when we close
   * the RaftBackedCache.
   */
  private SafeTimerTask evictionTask;

  /**
   * This is where we keep all of the listeners that fire whenever the current state changes
   */
  private final Set<ChangeListener<V>> changeListeners = new IdentityHashSet<>();


  /**
   * Track the owner of a change listener, so we can make sure we close it if needed.
   */
  private final Map<ChangeListener<V>, WeakReference<Object>> changeListenerOwner = new ConcurrentHashMap<>();


  /**
   * This is a single entry in the RaftBackedCache.
   */
  static class Entry<V> implements Map.Entry<String, V> {
    /** If true, we have persisted this entry since the last time we wrote it locally. */
    public boolean isPersisted;

    public final String key;

    /** The value of this entry, parameterized by the type of the cache. */
    public final V value;

    /** The straightforward constructor */
    Entry(String key, boolean isPersisted, V value) {
      this.key = key;
      this.isPersisted = isPersisted;
      this.value = value;
    }

    /**
     * This deserializes a single entry from its raw bytes.
     */
    public static <V> Entry<V> deserialize(String key, byte[] raw, Function<ByteArrayInputStream, Optional<V>> deserialize) throws InvalidProtocolBufferException {
      byte persistedSinceLastWrite = raw[0];
      ByteArrayInputStream rest = new ByteArrayInputStream(raw, 1, raw.length - 1);
      Optional<V> value = deserialize.apply(rest);
      if (!value.isPresent()) {
        throw new InvalidProtocolBufferException("Deserialization returned Optional.empty()");
      }
      return new Entry<>(key, persistedSinceLastWrite == 1, value.get());
    }

    /**
     * This does a quick in-place check for whether we've persisted a value, without deserializing the whole thing.
     * @param raw the serialized value under examination
     * @return true if we've persisted the value since our last write
     */
    public static boolean readIsPersisted(byte[] raw) {
      return raw[0] == 1;
    }

    /**
     * This serializes a single entry to its raw bytes, with a header
     */
    public byte[] serialize(Function<V, byte[]> serialize) {
      byte[] valueSerialized = serialize.apply(value);
      byte[] complete = new byte[valueSerialized.length + 1];
      System.arraycopy(valueSerialized, 0, complete, 1, valueSerialized.length);
      complete[0] = (byte)(isPersisted ? 1 : 0);
      return complete;
    }

    @Override
    public String getKey() {
      return this.key;
    }

    @Override
    public V getValue() {
      return this.value;
    }

    @Override
    public V setValue(V value) {
      throw new UnsupportedOperationException();
    }
  }


  /**
   * This holds a locally registered callback that will be called whenever the cache changes. This lets code
   * wait for changes to the cluster, and respond appropriately.
   */
  @FunctionalInterface
  public interface ChangeListener<V> {
    /**
     * Run on a value of the state machine changing.
     *
     * @param changedKey The key that changed.
     * @param newValue The new value of the changed key, or {@link Optional#empty()} if we are removing a key.
     * @param rawValue The raw bytes of the value.
     */
    void onChange(String changedKey, Lazy<Optional<V>> newValue, Optional<byte[]> rawValue);
  }


  /**
   * Run every time an object is set in the cache.
   *
   * @param object The object we're setting in the cache.
   */
  protected void onSet(V object) { }


  /** The number of eviction tasks currently running, to make sure we don't spam the thread */
  final Set<CompletableFuture<Boolean>> evictionTasksRunning = ConcurrentHashMap.newKeySet();

  /**
   * Create a Raft backed cache with a particular Raft implementation.
   */
  protected RaftBackedCache(Theseus raft, Duration idleDuration, Duration elementLifetime, int maxSizeBytes) {
    this.raft = raft;

    // This task is responsible for saving changes to SQL. It is NOT, however, responsible for removing elements from Raft

    evictionTask = new SafeTimerTask() {
      @Override
      public void runUnsafe() {
        // Filter our eviction tasks to only include things that aren't done yet
        evictionTasksRunning.removeAll(evictionTasksRunning.stream().filter(f -> !f.isDone()).collect(Collectors.toList()));

        // 1. Find keys that have been idle, and write those changes to SQL
        // DO NOT evict these changes from Raft directly, but instead mark those entries as having been persisted out
        // since their last write.

        Set<String> keysToSave = raft.stateMachine.keysIdleSince(idleDuration, raft.node.transport.now());
        keysToSave.addAll(raft.stateMachine.keysPresentSince(elementLifetime, raft.node.transport.now()));
        for (String key : keysToSave) {
          if (key.startsWith(prefix())) {
            if (!valuePersistedSinceLastWrite(key)) {
              // 2. Get a lock on those keys
              evictionTasksRunning.add(raft.withDistributedLockAsync(key, () -> {
                // 3. Read the value in Raft
                Optional<byte[]> valueBytes = raft.getElement(key);
                if (valueBytes.isPresent()) {

                  // Catch the race condition where someone saves between our reading the value and getting the lock
                  if (Entry.readIsPersisted(valueBytes.get())) return CompletableFuture.completedFuture(true);

                  try {
                    String localKey = key.replace(prefix(), "");
                    Entry<V> entry = Entry.deserialize(localKey, valueBytes.get(), RaftBackedCache.this::deserialize);
                    try {
                      // 4. Save the value somewhere else
                      log.info("Persisting RaftBackedCache element with key {}" , localKey);
                      persist(localKey, entry.value, false);

                      // Save the fact that we've persisted this key back to raft, so we don't keep hitting SQL
                      entry.isPersisted = true;
                      return raft.setElementAsync(key, entry.serialize(RaftBackedCache.this::serialize), true, Duration.ofSeconds(30));
                    } catch (Throwable t) {
                      log.warn("Could not evict element from RaftBackedCache; not removing from Raft", t);
                    }
                  } catch (InvalidProtocolBufferException e) {
                    log.warn("Could not deserialize Raft value for key {}", key);
                  }
                }
                return CompletableFuture.completedFuture(false);
              }));
            }
          }
        }

        // 2. Determine if we've run over our size constraint. If we have, then find the keys that have been idle for
        // as long as possible, and evict them.

        Collection<Map.Entry<String, KeyValueStateMachine.ValueWithOptionalOwner>> entrySet = raft.stateMachine.entries();
        long sizeInBytes = entrySet.stream().mapToInt(entry -> entry.getValue().value.length).sum();
        if (sizeInBytes > maxSizeBytes) {
          List<Map.Entry<String, KeyValueStateMachine.ValueWithOptionalOwner>> entries = new ArrayList<>(entrySet);  // note[gabor]: This is copying from a ConcurrentHashMap entrySet
          Map<String, Long> cachedLastAccessed = new HashMap<>();  // note[gabor]: See #200. We need to ensure a fixed value of lastAccessed
          entries.sort(Comparator.comparingLong(entry -> cachedLastAccessed.computeIfAbsent(entry.getKey(), k -> entry.getValue().lastAccessed)));

          long sizeNeeded = maxSizeBytes - sizeInBytes;
          for (Map.Entry<String, KeyValueStateMachine.ValueWithOptionalOwner> keyAndvalue : entries) {
            if (sizeNeeded <= 0) break;
            sizeNeeded -= keyAndvalue.getValue().value.length;

            // Save the entry, and remove it
            String key = keyAndvalue.getKey();
            if (key.startsWith(prefix())) {
              String localKey = key.replace(prefix(), "");

              evictionTasksRunning.add(raft.withDistributedLockAsync(key, () -> {
                Optional<byte[]> valueBytes = raft.getElement(key);
                if (valueBytes.isPresent()) {
                  if (!Entry.readIsPersisted(valueBytes.get())) {
                    try {
                      Entry<V> entry = Entry.deserialize(localKey, valueBytes.get(), RaftBackedCache.this::deserialize);
                      // 4. Evict the value from Raft - opportunity to save the value somewhere else
                      log.info("Persisting RaftBackedCache element with key {} in preparation for eviction", localKey);
                      persist(localKey, entry.value, false);
                    } catch (InvalidProtocolBufferException e) {
                      log.warn("Could not deserialize Raft value for key {}", key);
                    }
                  }

                  log.info("Evicting RaftBackedCache element with key {}", localKey);
                  // 5. Remove the value from the Raft state machine.
                  return raft.removeElementAsync(key, Duration.ofSeconds(30));
                }
                return CompletableFuture.completedFuture(false);
              }));
            }
          }
        }
      }
    };
    this.raft.node.transport.scheduleAtFixedRate(evictionTask, 1000);

    // Create a single Raft listener for this cache, so we only parse the proto for our listeners once. Parsing proto
    // is slow (a few ms each) so doing it hundreds of times per raft change is not acceptable.
    // See #1129
    KeyValueStateMachine.ChangeListener keyValueListener = (key, value, state, pool) -> {
      if (key.startsWith(prefix()) && !changeListeners.isEmpty()) {  // note[gabor]: should be a threadsafe usage of changeListeners; not locking
        key = key.replace(prefix(), "");

        final String finalKey = key;
        Lazy<Optional<V>> lazyValue = Lazy.of(() -> {
          Optional<V> parsedValue = Optional.empty();
          if (value.isPresent()) {
            try {
              parsedValue = Optional.of(Entry.deserialize(finalKey, value.get(), this::deserialize).value);
            } catch (Throwable t) {
              log.warn("Could not parse entry in change listener", t);
            }
          }
          return parsedValue;
        });
        List<ChangeListener<V>> localChangeListeners;  // make a local copy to prevent concurrent modification exceptions
        synchronized (changeListeners) {  // See #1152
          localChangeListeners = new ArrayList<>(changeListeners);
        }
        for (ChangeListener<V> changeListener : localChangeListeners) {
          pool.execute(() -> changeListener.onChange(finalKey, lazyValue, value));
        }
      }
    };
    raft.addChangeListener(keyValueListener);
  }


  /**
   * Create a Raft backed cache with the default Raft implementation.
   */
  protected RaftBackedCache(Theseus raft, Duration idleDuration, Duration elementLifetime) {
    this(raft, idleDuration, elementLifetime, DEFAULT_MAX_SIZE_BYTES);
  }


  /**
   * This creates a new CompletableFuture for all the currently outstanding eviction tasks.
   *
   * @return a NEW CompletableFuture for all the outstanding eviction tasks. If none outstanding, returns a completed
   *         future.
   */
  public CompletableFuture<Void> allOutstandingEvictionsFuture() {
    synchronized (evictionTasksRunning) {
      return CompletableFuture.allOf(evictionTasksRunning.toArray(new CompletableFuture[0]));
    }
  }


  /**
   * This cleans up the eviction timer. It also
   */
  public void close() {
    evictionTask.cancel();
  }


  /**
   * The raft prefix to use on {@link #get(String)} and {@link #put(String, Object, boolean)} to translate from our
   * local namespace to Raft's global namespace
   */
  protected abstract String prefix();


  /**
   * Serialize an object of our value type into a byte array. We need to be able to do this
   * so that we can store it in the Raft state machine.
   * This must be able to be reread with {@link #deserialize(ByteArrayInputStream)}.
   *
   * @param object The object we are serializing.
   *
   * @return A byte[] representation of the object.
   *
   * @see #deserialize(ByteArrayInputStream)
   */
  public abstract byte[] serialize(V object);


  /**
   * Read an object in our cache, written with {@link #serialize(Object)}, into a value type.
   *
   * @param serialized The serialized blob we are reading.
   *
   * @return The value corresponding to the protocol buffer.
   *
   * @see #serialize(Object)
   */
  public abstract Optional<V> deserialize(ByteArrayInputStream serialized);


  /**
   * Get a value from wherever it was evicted to, if we can.
   * This is the inverse of {@link #persist(String, Object, boolean)}.
   *
   * @param key The key of the element we're creating / retrieving.
   *
   * @return The value of the element we've created / retrieved.
   *
   * @see #persist(String, Object, boolean)
   */
  public abstract Optional<V> restore(String key);


  /**
   * Evict an element from our cache. This can either actually throw it away, or save it to a
   * persistence store.
   * If you chose the latter, make sure to have {@link #restore(String)} first try to retrieve the
   * element from this persistence store.
   *
   * @param key The key we are evicting.
   * @param value The value we are evicting.
   * @param async If true, the save is allowed to be asynchronous.
   *              This is often the case where, e.g., we're saving on a creation when we
   *              should not be blocking the main thread.
   */
  public abstract void persist(String key, V value, boolean async);


  /**
   * This iterates through all the elements in the Raft backed cache and evicts them, using only a single Raft commit
   * to do it.
   */
  public CompletableFuture<Boolean> clearCache() {
    Set<String> toRemove = new HashSet<>();
    for (String key : raft.stateMachine.values.keySet()) {
      if (key.startsWith(prefix())) {
        toRemove.add(key);
      }
    }
    return this.raft.removeElementsAsync(toRemove, Duration.ofSeconds(30));
  }


  /**
   * This registers a listener that will be called whenever the key-value store changes.
   *
   * @param changeListener the listener to register
   * @param owner the owner of the change listener. This is a failsafe to make
   *              sure we remove the listener when the owner dies, but is not required.
   */
  public void addChangeListener(ChangeListener<V> changeListener, @Nullable Object owner) {
    // 2. Register the listener
    synchronized (changeListeners) {  // See #1152
      changeListeners.add(changeListener);
    }

    // 3. Register the owner
    if (owner != null) {
      changeListenerOwner.put(changeListener, new WeakReference<>(owner));

      // 4. Clean up GC'd listeners (this is a failsafe)
      Set<ChangeListener> toRemove = new HashSet<>();
      for (Map.Entry<ChangeListener<V>, WeakReference<Object>> entry : changeListenerOwner.entrySet()) {
        if (entry.getValue().get() == null) {
          toRemove.add(entry.getKey());
        }
      }
      for (ChangeListener listener : toRemove) {
        log.warn("Leaked Raft change listener {} on cache {}", listener, this.toString());
        removeChangeListener(listener);
      }
    }
  }


  /** @see #addChangeListener(ChangeListener, Object) */
  public void addChangeListener(ChangeListener<V> changeListener) {
    addChangeListener(changeListener, null);
  }


  /**
   * This registers a listener that will be called whenever the key-value store changes.
   *
   * @param changeListener the listener to register
   */
  public void removeChangeListener(ChangeListener changeListener) {
    synchronized (changeListeners) {  // See #1152
      changeListeners.remove(changeListener);
    }
    changeListenerOwner.remove(changeListener);
  }


  /**
   * Perform a computation on the given element.
   * The result of the mutation is then saved into raft, if there was any change.
   * Note that we can always return the input element if we do not want to save anything
   * back to Raft.
   * If the element does not exist, we will call {@link #restore(String)} to ensure that it
   * is created.
   *
   * @param key The key of the element we're computing on.
   * @param mutator The function to call on the value for the given key.
   *                This takes 2 arguments: the input item, and a function that can be called to sync intermediate
   *                states. It should return the final state to save.
   *                Note that saving intermediate states is done while this object still holds the lock.
   *                So, other processes can only access the result read-only,
   * @param createNew An optional method for creating a brand new object of the relevant type - only gets called if
   *                  element cannot be retrieved from storage
   * @param unlocked If true, we are not locking the underlying Raft call.
   */
  public CompletableFuture<Boolean> withElementAsync(String key, BiFunction<V, Consumer<V>, V> mutator, @Nullable Supplier<V> createNew, boolean unlocked) {
    log.trace("WithElement {}", key);
    Pointer<Boolean> wasCreated = new Pointer<>(false);

    // A. The mutator (for Raft)
    Function<byte[], byte[]> mutatorImpl = originalBytes -> {
      // A.1. Get the entry
      Entry<V> entry;
      try {
        entry = Entry.deserialize(key, originalBytes, this::deserialize);
      } catch (InvalidProtocolBufferException e) {
        log.warn("Could not deserialize value for key=" + prefix() + key);
        return originalBytes;
      }
      // A.2. Mutate the entry
      V mutatedValue = mutator.apply(entry.value, update -> {
        // Optionally, we can save intermediate states during our mutation. This implements those intermediate saves
        try {
          Entry<V> updatedEntry = new Entry<>(key, false, entry.value);
          this.raft.setElementAsync(prefix() + key, updatedEntry.serialize(this::serialize), true, Duration.ofSeconds(5)).get(6, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
          log.warn("Could not save intermediate state during withElementAsyc call in RaftBackedCache: ", e);
        }
      });
      // A.3. Check that we've mutated
      if (mutatedValue != entry.value) {
        // If we have, persist the element and set it
        onSet(mutatedValue);
        boolean persisted = false;
        if (wasCreated.dereference().orElse(false)) {
          persisted = true;
          persist(key, mutatedValue, false);
        }
        return new Entry<>(key, persisted, mutatedValue).serialize(this::serialize);
      } else {
        // Otherwise, nothing happens
        return originalBytes;
      }
    };

    // B. The creator (for Raft)
    Supplier<byte[]> creatorImpl = () -> {
      // B.1. Get the value from stable storage
          Optional<V> obj = restore(key);
          if (obj.isPresent()) {
            // If it exists, return that
            return new Entry<>(key, true, obj.get()).serialize(this::serialize);
          } else if (createNew != null) {
            // Create the value
            V value = createNew.get();
            // Register that we were created
            wasCreated.set(true);
            // Return the value
            return new Entry<>(key, false, value).serialize(this::serialize);
          } else {
            // Nothing to return
            return null;
          }
        };

    // Perform the operation on Raft
    if (unlocked) {
      return this.raft.withElementUnlockedAsync(prefix() + key, mutatorImpl, creatorImpl, true);
    } else {
      return this.raft.withElementAsync(prefix() + key, mutatorImpl, creatorImpl, true);
    }
  }


  /**
   * @see #withElementAsync(String, BiFunction, Supplier, boolean)
   */
  public CompletableFuture<Boolean> withElementAsync(String key, Function<V, V> mutator, @Nullable Supplier<V> createNew, boolean unlocked) {
    return withElementAsync(key, (v, save) -> mutator.apply(v), createNew, unlocked);
  }


  /**
   * @see #withElementAsync(String, Function, Supplier)
   */
  public CompletableFuture<Boolean> withElementAsync(String key, Function<V, V> mutator, Supplier<V> createNew) {
    return withElementAsync(key, mutator, createNew, false);
  }


  /**
   * @see #withElementAsync(String, Function, Supplier)
   */
  public CompletableFuture<Boolean> withElementAsync(String key, Function<V, V> mutator) {
    return withElementAsync(key, mutator, null, false);
  }


  /**
   * @see #withElementAsync(String, BiFunction, Supplier, boolean)
   */
  public CompletableFuture<Boolean> withElementAsync(String key, BiFunction<V, Consumer<V>, V> mutator) {
    return withElementAsync(key, mutator, null, false);
  }


  /**
   * This just returns whatever byte array we have stored in the cache for this key, if any.
   */
  public Optional<ByteArrayInputStream> getRawIfPresent(String key) {
    return raft.stateMachine.get(prefix() + key, TimerUtils.mockableNow().toEpochMilli()).flatMap(raw -> Optional.of(new ByteArrayInputStream(raw, 1, raw.length - 1)));
  }


  /**
   * Get an element from the cache.
   *
   * @param key The key of the element to get
   *
   * @return The element we are getting, if present.
   */
  public Optional<V> get(String key) {
    Optional<byte[]> simpleGet = raft.stateMachine.get(prefix() + key, TimerUtils.mockableNow().toEpochMilli());
    if (simpleGet.isPresent()) {
      try {
        return Optional.of(Entry.deserialize(key, simpleGet.get(), this::deserialize).value);
      } catch (InvalidProtocolBufferException e) {
        log.warn("Could not read Proto for Raft item");
        return Optional.empty();
      }
    } else {
      Optional<V> value = restore(key);
      value.ifPresent(v -> raft.setElementAsync(prefix() + key, new Entry<>(key, false, v).serialize(this::serialize), true, Duration.ofSeconds(5)));
      return value;
    }
  }


  /**
   * <p>
   *   Get an element from the cache, if it's present in the cache itself.
   *   This will not try to get the element from the persistent store.
   * </p>
   *
   * <p>
   *   If you do not need the actual value you're getting from
   *   the cache, consider using {@link #keyIsCached(String)} instead.
   * </p>
   *
   * @param key The key of the element to get
   *
   * @return The element we are getting, if present.
   */
  public Optional<V> getIfPresent(String key) {
    return raft.stateMachine.get(prefix() + key, TimerUtils.mockableNow().toEpochMilli()).flatMap(v -> {
      try {
        return Optional.of(Entry.deserialize(key, v, this::deserialize).value);
      } catch (InvalidProtocolBufferException e) {
        return Optional.empty();
      }
    });
  }


  /**
   * Get the raw bytes in Raft, if they are present.
   * This saves the serialization cost of {@link #getIfPresent(String)},
   * but is otherwise identical.
   *
   * @param key The key we are reading from Raft.
   *
   * @return The raw byte array of the entry in the Raft cache, if any.
   */
  public Optional<byte[]> getBytesIfPresent(String key) {
    return raft.stateMachine.get(prefix() + key, TimerUtils.mockableNow().toEpochMilli()).flatMap(v -> {
      byte[] value = new byte[v.length - 1];
      System.arraycopy(v, 1, value, 0, value.length);
      return Optional.of(value);
    });
  }


  /**
   * Returns whether the given key is present in Raft.
   * Note that this is not necessarily checking whether the given key
   * exists in the map in general, only whether it's cached in Raft.
   *
   * @param key The key we are looking up in Raft.
   *
   * @return True if the key is in the Raft state.
   */
  public boolean keyIsCached(String key) {
    return raft.stateMachine.get(prefix() + key, TimerUtils.mockableNow().toEpochMilli()).isPresent();
  }


  /**
   * Set an element in Raft. This is really just an alias for {@link Theseus#setElementAsync(String, byte[], boolean, Duration)}
   * with permanent set to true and a 5 second timeout.
   */
  public CompletableFuture<Boolean> put(String key, V value, boolean persist) {
    onSet(value);
    Entry<V> newEntry = new Entry<>(key, false, value);
    // Persist the key if we should -- blocks
    if (persist) {
      this.persist(key, value, false);
    }
    // Set the element in Raft
    return raft.setElementAsync(prefix() + key, newEntry.serialize(this::serialize), true, Duration.ofSeconds(5));
  }


  /**
   * Remove an element in Raft. This is really just an alias for {@link Theseus#removeElementAsync(String, Duration)}.
   */
  public CompletableFuture<Boolean> evictWithoutSaving(String key) {
    return raft.removeElementAsync(prefix() + key, Duration.ofSeconds(5));
  }


  /** {@inheritDoc} */
  @Nonnull
  @Override
  public Iterator<Map.Entry<String,V>> iterator() {
    String prefix = prefix();
    return raft.getMap().entrySet().stream()
        .filter(x -> x.getKey().startsWith(prefix))
        .map(x -> {
          try {
            return (Map.Entry<String,V>)Entry.deserialize(x.getKey(), x.getValue(), this::deserialize);
          } catch (InvalidProtocolBufferException e) {
            return null;
          }
        })
        .filter(Objects::nonNull)
        .iterator();
  }

  /**
   * Returns an iterable over the keys in the RaftBackedCache.
   */
  public Set<String> keySet() {
    String prefix = prefix();
    return raft.getMap().keySet().stream()
        .filter(x -> x.startsWith(prefix))
        .map(x -> x.substring(prefix.length()))
        .collect(Collectors.toSet());
  }

  /**
   * This checks whether a value has been saved since the last time we wrote a change to that value in Raft.
   *
   * @param key The key of the element we're getting
   * @return true if we've saved the element since our last write
   */
  private boolean valuePersistedSinceLastWrite(String key) {
    return raft.getElement(key).filter(Entry::readIsPersisted).isPresent();
  }
}
