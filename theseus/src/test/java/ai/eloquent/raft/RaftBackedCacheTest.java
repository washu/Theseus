package ai.eloquent.raft;

import ai.eloquent.util.Uninterruptably;
import ai.eloquent.util.ZipUtils;
import ai.eloquent.web.Lifecycle;
import com.google.common.base.Charsets;
import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

/**
 * Test the {@link RaftBackedCache}
 *
 * @author <a href="mailto:gabor@eloquent.ai">Gabor Angeli</a>
 */
public class RaftBackedCacheTest {


  private static final Duration IDLE_TIMEOUT = Duration.ofSeconds(10);

  private static final int CACHE_SIZE_BYTES = 100;


  /** A dummy cache implementation to test against */
  private class Cache extends RaftBackedCache<String> {
    /** The "permanent" store, passed in by each test */
    public Map<String, String>  permanentStore;

    protected Cache(EloquentRaft raft, Map<String, String>  permanentStore) {
      super(raft, IDLE_TIMEOUT, IDLE_TIMEOUT, CACHE_SIZE_BYTES);
      this.permanentStore = permanentStore;
    }

    @Override
    protected String prefix() {
      return "junit_";
    }

    @Override
    public byte[] serialize(String object) {
      return ZipUtils.gzip(object.getBytes(Charsets.UTF_8));
    }

    @Override
    public Optional<String> deserialize(byte[] serialized) {
      return Optional.of(new String(ZipUtils.gunzip(serialized), Charsets.UTF_8));
    }

    @Override
    public Optional<String> restore(String key) {
      return Optional.ofNullable(permanentStore.get(key));
    }

    @Override
    public synchronized void persist(String key, String value, boolean async) {
      permanentStore.put(key, value);
    }
  }

  /** A dummy raft node, that doesn't have to go out to Rabbit. */
  private EloquentRaft raft;

  private EloquentRaft[] nodes;


  protected LocalTransport transport;


  @Before
  public void createRaft() {
    this.transport = new LocalTransport(true);
    // Create a cluster
    nodes = new EloquentRaft[]{
        new EloquentRaft("L", transport, new HashSet<>(Arrays.asList("L", "A", "B")), RaftLifecycle.newBuilder().mockTime().build()),
        new EloquentRaft("A", transport, new HashSet<>(Arrays.asList("L", "A", "B")), RaftLifecycle.newBuilder().mockTime().build()),
        new EloquentRaft("B", transport, new HashSet<>(Arrays.asList("L", "A", "B")), RaftLifecycle.newBuilder().mockTime().build()),
    };
    raft = nodes[0];
    // Start the cluster
    Arrays.stream(nodes).forEach(EloquentRaft::start);
    // Wait for an election
    for (int i = 0;
         i < 1000 &&
             !( Arrays.stream(nodes).anyMatch(x -> x.node.algorithm.state().isLeader()) &&
                 Arrays.stream(nodes).allMatch(x -> x.node.algorithm.state().log.getQuorumMembers().containsAll(Arrays.stream(nodes).map(n -> n.node.algorithm.serverName()).collect(Collectors.toList()))) &&
                 Arrays.stream(nodes).allMatch(x -> x.node.algorithm.state().leader.isPresent())
             );
         ++i) {
      transport.sleep(100);
    }
    assertTrue("Someone should have gotten elected in 10 [virtual] seconds",
        Arrays.stream(nodes).anyMatch(x -> x.node.algorithm.state().isLeader()));
    assertTrue("Every node should see every other node in the quorum",
        Arrays.stream(nodes).allMatch(x -> x.node.algorithm.state().log.getQuorumMembers().containsAll(Arrays.stream(nodes).map(n -> n.node.algorithm.serverName()).collect(Collectors.toList()))));
    assertTrue("Every node should see a leader",
        Arrays.stream(nodes).allMatch(x -> x.node.algorithm.state().leader.isPresent()));
  }


  @After
  public void killRaft() {
    for (EloquentRaft raft : nodes) {
      if (raft.node.algorithm instanceof SingleThreadedRaftAlgorithm) {
        ((SingleThreadedRaftAlgorithm) raft.node.algorithm).flush(() -> {});
      }
      raft.lifecycle.shutdown(true);
    }
    transport.stop();
  }


  /**
   * A simple test for withElement
   */
  @Test
  public void withElement() throws ExecutionException, InterruptedException {
    Map<String, String>  permanentStore = new HashMap<>();
    try (Cache cache = new Cache(raft, permanentStore)) {
      assertEquals(Optional.empty(), cache.get("key"));
      cache.withElementAsync("key", str -> str + "B", () -> "A").get();
      assertEquals(Optional.of("AB"), cache.get("key"));
    }
  }


  /**
   * Test if we have an element already in the cache, and don't specify a creator
   */
  @Test
  public void withElementNoCreatorButInRaft() throws ExecutionException, InterruptedException {
    Map<String, String>  permanentStore = new HashMap<>();
    try (Cache cache = new Cache(raft, permanentStore)) {
      assertEquals(Optional.empty(), cache.get("key"));
      RaftBackedCache.Entry<String> entry = new RaftBackedCache.Entry<>("key", false, "A");
      raft.setElementRetryAsync("junit_key", entry.serialize(cache::serialize), true, Duration.ofSeconds(5)).get();  // manually set
      cache.withElementAsync("key", str -> str + "B").get();
      assertEquals(Optional.of("AB"), cache.get("key"));
    }
  }


  /**
   * Test if getIfPresent if the element is already in raft
   */
  @SuppressWarnings("ConstantConditions")
  @Test
  public void getIfPresentInRaft() throws ExecutionException, InterruptedException {
    Map<String, String>  permanentStore = new HashMap<>();
    try (Cache cache = new Cache(raft, permanentStore)) {
      assertEquals(Optional.empty(), cache.get("key"));
      RaftBackedCache.Entry<String> entry = new RaftBackedCache.Entry<>("key", false, "A");
      raft.setElementRetryAsync("junit_key", entry.serialize(cache::serialize), true, Duration.ofSeconds(5)).get();  // manually set
      assertEquals("A", cache.getIfPresent("key").get());
    }
  }

  /**
   * Test if getIfPresent if the element is already in raft
   */
  @Test
  public void getIfPresentNotInRaft() {
    Map<String, String>  permanentStore = new HashMap<>();
    try (Cache cache = new Cache(raft, permanentStore)) {
      assertEquals(Optional.empty(), cache.get("key"));
      assertFalse(cache.getIfPresent("key").isPresent());
    }
  }


  /**
   * Test if we have an element already in persistent storage, and don't specify a creator
   */
  @Test
  public void withElementNoCreatorButInStorage() throws ExecutionException, InterruptedException {
    Map<String, String>  permanentStore = new HashMap<>();
    try (Cache cache = new Cache(raft, permanentStore)) {
      assertEquals(Optional.empty(), cache.get("key"));
      permanentStore.put("key", "A");
      cache.withElementAsync("key", str -> str + "B").get();
      assertEquals(Optional.of("AB"), cache.get("key"));
    }
  }


  /**
   * If we don't have an element or a creator, then this is a noop action
   */
  @Test
  public void withElementNoCreatorNoop() throws ExecutionException, InterruptedException {
    Map<String, String>  permanentStore = new HashMap<>();
    try (Cache cache = new Cache(raft, permanentStore)) {
      assertEquals(Optional.empty(), cache.get("key"));
      cache.withElementAsync("key", str -> str + "B").get();
      assertEquals(Optional.empty(), cache.get("key"));
    }
  }


  @SuppressWarnings("ConstantConditions")
  @Test
  public void testRestore() {
    Map<String, String>  permanentStore = new HashMap<>();
    permanentStore.put("key", "A");
    try (Cache cache = new Cache(raft, permanentStore)) {
      // It should come from the permanent store
      assertEquals(cache.get("key").get(), "A");

      // It should now be in the cache
      Uninterruptably.sleep(100);
      assertEquals(cache.get("key").get(), "A");
    }
  }


  /**
   * A simple test for whether we actually persist our elements
   */
  @Test
  public void testPersisting() throws ExecutionException, InterruptedException {
    Map<String, String>  permanentStore = new HashMap<>();
    try (Cache cache = new Cache(raft, permanentStore)) {
      boolean success = false;
      for (int i = 0; i < 10; i++) {
        success = cache.withElementAsync("key", str -> str + "B", () -> "A").get();
        if (success) break;
      }
      if (!success) {
        System.err.println("Unable to write to RaftBackedCache after 10 consecutive tries. Ignoring test, but this means something is fishy in Raft.");
        return;
      }
      raft.node.transport.sleep(IDLE_TIMEOUT.plusSeconds(10).toMillis());

      // The flushing event occurs in another thread on Raft, so we have to wait for it here
      cache.allOutstandingEvictionsFuture().get();

      assertEquals("Should have evicted into the permanent store", Optional.of("AB"), Optional.ofNullable(permanentStore.get("key")));
      assertTrue("Should still be in Raft", raft.getElement(cache.prefix() + "key").isPresent());
      assertEquals("Should be able to retrieve it", Optional.of("AB"), cache.get("key"));
    }
  }


  /**
   * Test evicting from the cache
   */
  @Test
  public void testEviction() throws ExecutionException, InterruptedException {
    int NUM_ENTRIES = 100;

    Map<String, String>  permanentStore = new HashMap<>();
    try (Cache cache = new Cache(raft, permanentStore)) {

      for (int i = 0; i < NUM_ENTRIES; i++) {
        boolean success = cache.withElementAsync("key"+i, str -> str + "B", () -> "A").get();
        if (!success) {
          System.err.println("Unable to write to RaftBackedCache. Ignoring test, but this is suspicious");
          return;
        }
      }
      assertEquals("Raft map should be the right size", NUM_ENTRIES, raft.stateMachine.entries().size());
      raft.node.transport.sleep(IDLE_TIMEOUT.plusSeconds(10).toMillis());

      // The flushing event occurs in another thread on Raft, so we have to wait for it here
      // Flushes can fail because Raft changes fail, and then need to be retried, so we give it a couple of chances
      for (int i = 0; i < 10; i++) {
        cache.allOutstandingEvictionsFuture().get();
        if (permanentStore.size() >= NUM_ENTRIES) break;
        raft.node.transport.sleep(2000);
      }

      for (int i = 0; i < NUM_ENTRIES; i++) {
        assertTrue("\"key"+i+"\" should still be in Raft after timeout", raft.getElement(cache.prefix() + "key" + i).isPresent());
        assertEquals("\"key"+i+"\" elements should have the right value in Raft", Optional.of("AB"), cache.get("key" + i));
        assertEquals("\"key"+i+"\" should have the right value in storage", "AB", permanentStore.get("key" + i));
      }
      assertEquals("Storage map should be the right size", NUM_ENTRIES, permanentStore.size());
    }
  }


  /**
   * Test serialization stability for entry
   */
  @Test
  public void entrySerializationStable() throws InvalidProtocolBufferException {
    Random r = new Random();
    byte[] buffer = new byte[1000];
    r.nextBytes(buffer);

    byte[] serialized = new RaftBackedCache.Entry<>("key", false, buffer).serialize(x -> x);
    RaftBackedCache.Entry<byte[]> deserialized = RaftBackedCache.Entry.deserialize("key", serialized, Optional::of);
    assertArrayEquals("Simple serialization should be stable",
        buffer, deserialized.value);
  }


  /**
   * Test serialization stability for entry, even if we change the persisted value
   */
  @Test
  public void entrySerializationStableOnPersistChange() throws InvalidProtocolBufferException {
    Random r = new Random();
    byte[] buffer = new byte[1000];
    r.nextBytes(buffer);

    RaftBackedCache.Entry<byte[]> entry = new RaftBackedCache.Entry<>("key", false, buffer);
    entry.isPersisted = false;
    byte[] serialized = entry.serialize(Function.identity());
    RaftBackedCache.Entry<byte[]> deserialized = RaftBackedCache.Entry.deserialize("key", serialized, Optional::of);
    assertArrayEquals("Simple serialization should be stable",
        buffer, deserialized.value);
  }


  /**
   * Test serialization stability for entry
   */
  @Test
  public void entrySerializationStableGZip() throws InvalidProtocolBufferException {
    Random r = new Random();
    byte[] buffer = new byte[1000];
    r.nextBytes(buffer);

    byte[] serialized = new RaftBackedCache.Entry<>("key", false, buffer).serialize(ZipUtils::gzip);
    RaftBackedCache.Entry<byte[]> deserialized = RaftBackedCache.Entry.deserialize("key", serialized, x -> Optional.of(ZipUtils.gunzip(x)));
    assertArrayEquals("Simple serialization should be stable",
        buffer, deserialized.value);
  }
}