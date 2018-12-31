package ai.eloquent.raft;

import ai.eloquent.test.SlowTests;
import ai.eloquent.util.Pointer;
import ai.eloquent.util.TimerUtils;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.ByteString;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.*;
import static ai.eloquent.raft.KeyValueStateMachine.*;

/**
 * This is mostly just verifying the serializations are working correctly. Also transitions.
 */
@SuppressWarnings("ConstantConditions")
public class KeyValueStateMachineTest {

  private static final Logger log = LoggerFactory.getLogger(KeyValueStateMachineTest.class);

  private long now = TimerUtils.mockableNow().toEpochMilli();

  /**
   * Assert that a given state machine serializes correctly.
   */
  private void assertSerializable(KeyValueStateMachine original) {
    byte[] ser = original.serialize();
    KeyValueStateMachine reread = new KeyValueStateMachine(original.serverName.orElse(""));
    reread.overwriteWithSerialized(ser, 0L, MoreExecutors.newDirectExecutorService());
    assertEquals(original, reread);
  }


  @Test
  public void testGetSet() {
    KeyValueStateMachine x = new KeyValueStateMachine("name");
    x.applyTransition(KeyValueStateMachineProto.Transition.newBuilder()
            .setType(KeyValueStateMachineProto.TransitionType.SET_VALUE)
            .setSetValue(KeyValueStateMachineProto.SetValue.newBuilder().setKey("mykey").setValue(ByteString.copyFromUtf8("hello world!")))
            .build().toByteArray(), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());

    assertTrue("The state machine should have stored a value", x.get("mykey", 0L).isPresent());
    assertEquals("The state machine should have stored the correct answer", "hello world!", new String(x.get("mykey", 0L).get()));
    assertEquals("The state machine should not have other keys", Optional.empty(), x.get("otherkey", 0L));
    assertSerializable(x);

  }

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  @Test
  public void testChangeListeners() {
    KeyValueStateMachine stateMachine = new KeyValueStateMachine("name");
    Pointer<Integer> numChanges = new Pointer<>(0);
    Pointer<Map<String,byte[]>> lastChange = new Pointer<>(new HashMap<>());

    KeyValueStateMachine.ChangeListener changeListener = (key, value, map, pool) -> {
      numChanges.set(numChanges.dereference().get() + 1);
      lastChange.set(map);
    };

    stateMachine.addChangeListener(changeListener);
    stateMachine.applyTransition(KeyValueStateMachine.createSetValueTransition("test", new byte[]{2}), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());

    // The change listener should have been called
    assertEquals(1, (int)numChanges.dereference().get());
    assertEquals(1, lastChange.dereference().get().size());
    assertEquals(2, lastChange.dereference().get().get("test")[0]);

    stateMachine.removeChangeListener(changeListener);
    stateMachine.applyTransition(KeyValueStateMachine.createSetValueTransition("test", new byte[]{3}), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());

    // The change listener shouldn't be called, since it's been removed
    assertEquals(1, (int)numChanges.dereference().get());
    assertSerializable(stateMachine);
  }

  @Test
  public void testSerializeQueueLock() {
    KeyValueStateMachine.QueueLock emptyLock = new KeyValueStateMachine.QueueLock(null, new ArrayList<>());
    KeyValueStateMachine.QueueLock recoveredEmptyLock = KeyValueStateMachine.QueueLock.deserialize(emptyLock.serialize());
    assertNull(recoveredEmptyLock.holder);
    assertEquals(0, recoveredEmptyLock.waiting.size());

    KeyValueStateMachine.QueueLock heldNoWaiting = new KeyValueStateMachine.QueueLock(Optional.of(new LockRequest("holder", "hash1")).orElse(null), new ArrayList<>());
    KeyValueStateMachine.QueueLock recoveredHeldNoWaiting = KeyValueStateMachine.QueueLock.deserialize(heldNoWaiting.serialize());
    assertNotNull(recoveredHeldNoWaiting.holder);
    assertEquals(new KeyValueStateMachine.LockRequest("holder", "hash1"), recoveredHeldNoWaiting.holder);
    assertEquals(0, recoveredHeldNoWaiting.waiting.size());

    List<KeyValueStateMachine.LockRequest> waiting = new ArrayList<>();
    waiting.add(new KeyValueStateMachine.LockRequest("a", "hash2"));
    waiting.add(new KeyValueStateMachine.LockRequest("b", "hash3"));
    KeyValueStateMachine.QueueLock heldWaiting = new KeyValueStateMachine.QueueLock(Optional.of(new LockRequest("holder", "hash1")).orElse(null), waiting);
    KeyValueStateMachine.QueueLock recoveredHeldWaiting = KeyValueStateMachine.QueueLock.deserialize(heldWaiting.serialize());
    assertNotNull(recoveredHeldWaiting.holder);
    assertEquals(new KeyValueStateMachine.LockRequest("holder", "hash1"), recoveredHeldWaiting.holder);
    assertEquals(2, recoveredHeldWaiting.waiting.size());
    assertEquals(waiting, new ArrayList<>(recoveredHeldWaiting.waiting));
  }

  @Test
  public void testSerializeValueWithOptionalOwner() {
    KeyValueStateMachine.ValueWithOptionalOwner valueWithoutOwner = new KeyValueStateMachine.ValueWithOptionalOwner(new byte[]{42}, 0L);
    KeyValueStateMachine.ValueWithOptionalOwner recoveredValueWithoutOwner = KeyValueStateMachine.ValueWithOptionalOwner.deserialize(valueWithoutOwner.serialize());
    assertEquals(recoveredValueWithoutOwner.value[0], 42);
    assertFalse(recoveredValueWithoutOwner.owner.isPresent());

    KeyValueStateMachine.ValueWithOptionalOwner valueWithOwner = new KeyValueStateMachine.ValueWithOptionalOwner(new byte[]{42}, "owner", 0L);
    KeyValueStateMachine.ValueWithOptionalOwner recoveredValueWithOwner = KeyValueStateMachine.ValueWithOptionalOwner.deserialize(valueWithOwner.serialize());
    assertEquals(recoveredValueWithOwner.value[0], 42);
    assertTrue(recoveredValueWithOwner.owner.isPresent());
    assertEquals("owner", recoveredValueWithOwner.owner.get());
  }

  @Test
  public void testSerializeState() {
    KeyValueStateMachine emptyStateMachine = new KeyValueStateMachine("name");
    KeyValueStateMachine recoveredEmptyStateMachine = new KeyValueStateMachine("name");
    recoveredEmptyStateMachine.overwriteWithSerialized(emptyStateMachine.serialize(), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());
    assertEquals(0, recoveredEmptyStateMachine.locks.size());
    assertEquals(0, recoveredEmptyStateMachine.values.size());

    KeyValueStateMachine stateMachine = new KeyValueStateMachine("name");
    stateMachine.values.put("test1", new KeyValueStateMachine.ValueWithOptionalOwner(new byte[]{1}, 0L));
    stateMachine.values.put("test2", new KeyValueStateMachine.ValueWithOptionalOwner(new byte[]{2}, 0L));

    List<KeyValueStateMachine.LockRequest> waiting = new ArrayList<>();
    waiting.add(new KeyValueStateMachine.LockRequest("a", "hash2"));
    waiting.add(new KeyValueStateMachine.LockRequest("b", "hash3"));
    KeyValueStateMachine.QueueLock heldWaiting = new KeyValueStateMachine.QueueLock(Optional.of(new LockRequest("holder", "hash1")).orElse(null), waiting);
    stateMachine.locks.put("test1", heldWaiting);

    KeyValueStateMachine recoveredStateMachine = new KeyValueStateMachine("name");
    recoveredStateMachine.overwriteWithSerialized(stateMachine.serialize(), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());

    assertEquals(2, recoveredStateMachine.values.size());
    assertTrue(recoveredStateMachine.values.containsKey("test1"));
    assertEquals(1, (int)recoveredStateMachine.values.get("test1").value[0]);
    assertTrue(recoveredStateMachine.values.containsKey("test2"));
    assertEquals(2, (int)recoveredStateMachine.values.get("test2").value[0]);

    assertEquals(1, recoveredStateMachine.locks.size());
    assertTrue(recoveredStateMachine.locks.containsKey("test1"));
    KeyValueStateMachine.QueueLock recoveredHeldWaiting = recoveredStateMachine.locks.get("test1");
    assertNotNull(recoveredHeldWaiting.holder);
    assertEquals(new KeyValueStateMachine.LockRequest("holder", "hash1"), recoveredHeldWaiting.holder);
    assertEquals(2, recoveredHeldWaiting.waiting.size());
    assertEquals(waiting, new ArrayList<>(recoveredHeldWaiting.waiting));

    assertSerializable(emptyStateMachine);
    assertSerializable(stateMachine);
  }


  @Test
  public void testRequestLockTransition() {
    KeyValueStateMachine stateMachine = new KeyValueStateMachine("name");
    assertEquals(0, stateMachine.locks.size());

    stateMachine.applyTransition(KeyValueStateMachine.createRequestLockTransition("test", "host1", "hash1"), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());

    assertEquals(1, stateMachine.locks.size());
    KeyValueStateMachine.QueueLock lock = stateMachine.locks.get("test");
    assertEquals("host1", lock.holder.server);
    assertEquals(0, lock.waiting.size());

    CompletableFuture<Boolean> host1Acquired = stateMachine.createLockAcquiredFuture("test", "host1", "hash1");
    assertTrue(host1Acquired.isDone());
    assertTrue(host1Acquired.getNow(false));

    stateMachine.applyTransition(KeyValueStateMachine.createRequestLockTransition("test", "host2", "hash2"), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());

    stateMachine.applyTransition(KeyValueStateMachine.createRequestLockTransition("test", "host3", "hash3"), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());

    CompletableFuture<Boolean> host2Acquired = stateMachine.createLockAcquiredFuture("test", "host2", "hash2");
    CompletableFuture<Boolean> host3Acquired = stateMachine.createLockAcquiredFuture("test", "host3", "hash3");

    assertTrue(host1Acquired.isDone());
    assertFalse(host2Acquired.isDone());
    assertFalse(host3Acquired.isDone());

    assertEquals(1, stateMachine.locks.size());
    lock = stateMachine.locks.get("test");
    assertEquals("host1", lock.holder.server);
    assertEquals(2, lock.waiting.size());
    assertEquals("host2", lock.waiting.peek().server);
    assertEquals("host3", new ArrayList<>(lock.waiting).get(1).server);

    assertTrue(host1Acquired.isDone());
    assertFalse(host2Acquired.isDone());
    assertFalse(host3Acquired.isDone());

    stateMachine.applyTransition(KeyValueStateMachine.createRequestLockTransition("test2", "host4", "hash4"), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());

    assertEquals(2, stateMachine.locks.size());
    KeyValueStateMachine.QueueLock lock2 = stateMachine.locks.get("test2");
    assertEquals("host4", lock2.holder.server);
    assertEquals(0, lock2.waiting.size());

    assertSerializable(stateMachine);
  }



  @Test
  public void testTryLockTransition() {
    KeyValueStateMachine stateMachine = new KeyValueStateMachine("name");
    assertEquals(0, stateMachine.locks.size());

    // Take a lock on hash1
    stateMachine.applyTransition(KeyValueStateMachine.createTryLockTransition("test", "host1", "hash1"), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());
    assertEquals(1, stateMachine.locks.size());
    KeyValueStateMachine.QueueLock lock = stateMachine.locks.get("test");
    assertEquals("host1", lock.holder.server);
    assertEquals(0, lock.waiting.size());
    CompletableFuture<Boolean> hash1Future = stateMachine.createLockAcquiredFuture("test", "host1", "hash1");
    assertTrue("hash1 should be taken", hash1Future.getNow(false));

    // Try+fail taking hash2
    stateMachine.applyTransition(KeyValueStateMachine.createTryLockTransition("test", "host2", "hash2"), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());
    assertEquals(1, stateMachine.locks.size());
    lock = stateMachine.locks.get("test");
    assertEquals("host1 should still hold the lock", "host1", lock.holder.server);
    assertEquals("No one should be waiting on hash2", 0, lock.waiting.size());


    CompletableFuture<Boolean> hash2Future = stateMachine.createLockAcquiredFuture("test", "host1", "hash2");
    assertNull("hash2 should NOT be taken", hash2Future.getNow(null));

    assertSerializable(stateMachine);
  }


  @Test
  public void testReleaseLockTransition() throws ExecutionException, InterruptedException {
    KeyValueStateMachine stateMachine = new KeyValueStateMachine("name");
    assertEquals(0, stateMachine.locks.size());

    stateMachine.applyTransition(KeyValueStateMachine.createRequestLockTransition("test", "host1", "hash1"), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());


    CompletableFuture<Boolean> host1Acquired = stateMachine.createLockAcquiredFuture("test", "host1", "hash1");
    assertTrue(host1Acquired.isDone());
    assertTrue(host1Acquired.get());

    assertEquals(1, stateMachine.locks.size());
    KeyValueStateMachine.QueueLock lock = stateMachine.locks.get("test");
    assertEquals("host1", lock.holder.server);
    assertEquals(0, lock.waiting.size());

    // This should be a no-op since we're the wrong host
    stateMachine.applyTransition(KeyValueStateMachine.createReleaseLockTransition("test", "host2", "hash2"), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());

    assertEquals(1, stateMachine.locks.size());
    lock = stateMachine.locks.get("test");
    assertEquals("host1", lock.holder.server);
    assertEquals(0, lock.waiting.size());

    // This should clear out the last lock
    stateMachine.applyTransition(KeyValueStateMachine.createReleaseLockTransition("test", "host1", "hash1"), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());

    assertEquals(0, stateMachine.locks.size());

    // Queue up some lock holders
    stateMachine.applyTransition(KeyValueStateMachine.createRequestLockTransition("test", "host1", "hash1"), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());
    stateMachine.applyTransition(KeyValueStateMachine.createRequestLockTransition("test", "host2", "hash2"), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());
    stateMachine.applyTransition(KeyValueStateMachine.createRequestLockTransition("test", "host3", "hash3"), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());
    CompletableFuture<Boolean> host2Acquired = stateMachine.createLockAcquiredFuture("test", "host2", "hash2");
    CompletableFuture<Boolean> host3Acquired = stateMachine.createLockAcquiredFuture("test", "host3", "hash3");

    assertTrue(host1Acquired.isDone());
    assertFalse(host2Acquired.isDone());
    assertFalse(host3Acquired.isDone());

    assertEquals(1, stateMachine.locks.size());
    lock = stateMachine.locks.get("test");
    assertEquals("host1", lock.holder.server);
    assertEquals(2, lock.waiting.size());
    assertEquals("host2", lock.waiting.peek().server);
    assertEquals("host3", new ArrayList<>(lock.waiting).get(1).server);

    assertTrue(host1Acquired.isDone());
    assertFalse(host2Acquired.isDone());
    assertFalse(host3Acquired.isDone());

    stateMachine.applyTransition(KeyValueStateMachine.createReleaseLockTransition("test", "host1", "hash1"), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());

    assertTrue(host2Acquired.isDone());
    assertFalse(host3Acquired.isDone());

    assertEquals(1, stateMachine.locks.size());
    lock = stateMachine.locks.get("test");
    assertEquals("host2", lock.holder.server);
    assertEquals(1, lock.waiting.size());
    assertEquals("host3", lock.waiting.peek().server);

    stateMachine.applyTransition(KeyValueStateMachine.createReleaseLockTransition("test", "host2", "hash2"), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());

    assertTrue(host3Acquired.isDone());

    assertEquals(1, stateMachine.locks.size());
    lock = stateMachine.locks.get("test");
    assertEquals("host3", lock.holder.server);
    assertEquals(0, lock.waiting.size());

    stateMachine.applyTransition(KeyValueStateMachine.createReleaseLockTransition("test", "host3", "hash3"), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());

    assertEquals(0, stateMachine.locks.size());

    assertSerializable(stateMachine);
  }


  @Test
  public void testReleaseQueuedLockTransition() {
    KeyValueStateMachine stateMachine = new KeyValueStateMachine("name");
    assertEquals(0, stateMachine.locks.size());

    // Queue up some lock holders
    stateMachine.applyTransition(KeyValueStateMachine.createRequestLockTransition("test", "host1", "hash1"), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());
    stateMachine.applyTransition(KeyValueStateMachine.createRequestLockTransition("test", "host2", "hash2"), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());
    stateMachine.applyTransition(KeyValueStateMachine.createRequestLockTransition("test", "host3", "hash3"), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());
    assertEquals(1, stateMachine.locks.size());
    KeyValueStateMachine.QueueLock lock = stateMachine.locks.get("test");
    assertEquals("host1", lock.holder.server);
    assertEquals(2, lock.waiting.size());
    assertEquals("host2", lock.waiting.peek().server);
    assertEquals("host3", new ArrayList<>(lock.waiting).get(1).server);

    // Test that we can release a lock that's queued
    stateMachine.applyTransition(KeyValueStateMachine.createReleaseLockTransition("test", "host2", "hash2"), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());
    assertEquals(1, stateMachine.locks.size());
    lock = stateMachine.locks.get("test");
    assertEquals("host1", lock.holder.server);
    assertEquals(1, lock.waiting.size());
    assertEquals("host3", lock.waiting.peek().server);

    // This should be a no-op, since the hash is wrong
    stateMachine.applyTransition(KeyValueStateMachine.createReleaseLockTransition("test", "host3", "wrong-hash"), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());
    assertEquals(1, stateMachine.locks.size());
    lock = stateMachine.locks.get("test");
    assertEquals("host1", lock.holder.server);
    assertEquals(1, lock.waiting.size());
    assertEquals("host3", lock.waiting.peek().server);

    // Do that again
    stateMachine.applyTransition(KeyValueStateMachine.createReleaseLockTransition("test", "host3", "hash3"), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());
    assertEquals(1, stateMachine.locks.size());
    lock = stateMachine.locks.get("test");
    assertEquals("host1", lock.holder.server);
    assertEquals(0, lock.waiting.size());

    // Clean up the lock
    stateMachine.applyTransition(KeyValueStateMachine.createReleaseLockTransition("test", "host1", "hash1"), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());
    assertEquals(0, stateMachine.locks.size());

    assertSerializable(stateMachine);
  }


  @Test
  public void testSameHostDifferentLocks() {
    KeyValueStateMachine stateMachine = new KeyValueStateMachine("name");
    assertEquals(0, stateMachine.locks.size());
    stateMachine.applyTransition(KeyValueStateMachine.createRequestLockTransition("lockName", "host", "hash1"), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());
    CompletableFuture<Boolean> hash1Future = stateMachine.createLockAcquiredFuture("lockName", "host", "hash1");
    assertTrue("We should have taken our lock", hash1Future.getNow(false));
    stateMachine.applyTransition(KeyValueStateMachine.createRequestLockTransition("lockName", "host", "hash2"), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());
    CompletableFuture<Boolean> hash2Future = stateMachine.createLockAcquiredFuture("lockName", "host", "hash2");
    assertNull("We should not have a lock of the same name but different hash", hash2Future.getNow(null));
    stateMachine.applyTransition(KeyValueStateMachine.createReleaseLockTransition("lockName", "host", "hash1"), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());
    assertTrue("We should have taken lock on the different hash when the first hash's lock was released", hash2Future.getNow(false));

    assertSerializable(stateMachine);
  }


  @Test
  public void testDifferentHashUnlockDoesntUnlockOriginalLock() {
    KeyValueStateMachine stateMachine = new KeyValueStateMachine("name");
    assertEquals(0, stateMachine.locks.size());
    stateMachine.applyTransition(KeyValueStateMachine.createRequestLockTransition("lockName", "host", "hash1"), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());

    // Take a lock on hash1
    CompletableFuture<Boolean> hash1Future = stateMachine.createLockAcquiredFuture("lockName", "host", "hash1");
    assertTrue("We should have taken our lock", hash1Future.getNow(false));

    // Take a lock on hash2
    stateMachine.applyTransition(KeyValueStateMachine.createRequestLockTransition("lockName", "host", "hash2"), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());
    CompletableFuture<Boolean> hash2Future = stateMachine.createLockAcquiredFuture("lockName", "host", "hash2");
    assertNull("We should not have a lock of the same name but different hash", hash2Future.getNow(null));

    // Release a lock on hash2
    stateMachine.applyTransition(KeyValueStateMachine.createReleaseLockTransition("lockName", "host", "hash2"), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());
    CompletableFuture<Boolean> hash1FutureAgain = stateMachine.createLockAcquiredFuture("lockName", "host", "hash1");
    assertTrue("We should still have our lock", hash1FutureAgain.getNow(false));

    assertSerializable(stateMachine);
  }


  @Test
  public void testIdempotentLocks() {
    KeyValueStateMachine stateMachine = new KeyValueStateMachine("name");
    assertEquals(0, stateMachine.locks.size());

    stateMachine.applyTransition(KeyValueStateMachine.createRequestLockTransition("test", "host", "hash1"), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());

    assertEquals(1, stateMachine.locks.size());
    KeyValueStateMachine.QueueLock lock = stateMachine.locks.get("test");
    assertEquals("host", lock.holder.server);
    assertEquals(0, lock.waiting.size());

    stateMachine.applyTransition(KeyValueStateMachine.createRequestLockTransition("test", "host", "hash2"), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());

    stateMachine.applyTransition(KeyValueStateMachine.createRequestLockTransition("test", "host", "hash3"), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());

    assertEquals(1, stateMachine.locks.size());
    lock = stateMachine.locks.get("test");
    assertEquals("host", lock.holder.server);
    assertEquals("hash1", lock.holder.uniqueHash);
    assertEquals(2, lock.waiting.size());
    assertEquals("host", lock.waiting.peek().server);
    assertEquals("hash2", lock.waiting.peek().uniqueHash);
    assertEquals("host", new ArrayList<>(lock.waiting).get(1).server);
    assertEquals("hash3", new ArrayList<>(lock.waiting).get(1).uniqueHash);

    // This should be ignored
    stateMachine.applyTransition(KeyValueStateMachine.createRequestLockTransition("test", "host", "hash1"), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());

    assertEquals(1, stateMachine.locks.size());
    lock = stateMachine.locks.get("test");
    assertEquals("host", lock.holder.server);
    assertEquals("hash1", lock.holder.uniqueHash);
    assertEquals(2, lock.waiting.size());
    assertEquals("host", lock.waiting.peek().server);
    assertEquals("hash2", lock.waiting.peek().uniqueHash);
    assertEquals("host", new ArrayList<>(lock.waiting).get(1).server);
    assertEquals("hash3", new ArrayList<>(lock.waiting).get(1).uniqueHash);

    // This should be ignored
    stateMachine.applyTransition(KeyValueStateMachine.createRequestLockTransition("test", "host", "hash2"), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());

    assertEquals(1, stateMachine.locks.size());
    lock = stateMachine.locks.get("test");
    assertEquals("host", lock.holder.server);
    assertEquals("hash1", lock.holder.uniqueHash);
    assertEquals(2, lock.waiting.size());
    assertEquals("host", lock.waiting.peek().server);
    assertEquals("hash2", lock.waiting.peek().uniqueHash);
    assertEquals("host", new ArrayList<>(lock.waiting).get(1).server);
    assertEquals("hash3", new ArrayList<>(lock.waiting).get(1).uniqueHash);

    // This should be ignored
    stateMachine.applyTransition(KeyValueStateMachine.createRequestLockTransition("test", "host", "hash3"), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());

    assertEquals(1, stateMachine.locks.size());
    lock = stateMachine.locks.get("test");
    assertEquals("host", lock.holder.server);
    assertEquals("hash1", lock.holder.uniqueHash);
    assertEquals(2, lock.waiting.size());
    assertEquals("host", lock.waiting.peek().server);
    assertEquals("hash2", lock.waiting.peek().uniqueHash);
    assertEquals("host", new ArrayList<>(lock.waiting).get(1).server);
    assertEquals("hash3", new ArrayList<>(lock.waiting).get(1).uniqueHash);

    assertSerializable(stateMachine);
  }


  /**
   * Make sure transient values clear when we go offline.
   */
  @Test
  public void testClearTransientsFor() {
    KeyValueStateMachine stateMachine = new KeyValueStateMachine("name");
    stateMachine.applyTransition(createSetValueTransitionWithOwner("transient", new byte[]{42}, "me"), now, MoreExecutors.newDirectExecutorService());
    assertEquals("The state machine should know I'm an owner of something", Collections.singleton("me"), stateMachine.owners());
    assertArrayEquals("Should have set the entry",
        new byte[]{42}, Optional.ofNullable(stateMachine.values.get("transient")).map(x -> x.value).orElse(null));
    stateMachine.applyTransition(createClearTransition("me"), now, MoreExecutors.newDirectExecutorService());
    assertEquals("Should have cleared entries owned by this owner",
        Optional.empty(), Optional.ofNullable(stateMachine.values.get("transient")).map(x -> x.value));
    assertSerializable(stateMachine);
  }


  /**
   * Make sure we don't delete other's transient values.
   */
  @Test
  public void testClearTransientsForNotOwner() {
    KeyValueStateMachine stateMachine = new KeyValueStateMachine("name");
    stateMachine.applyTransition(createSetValueTransitionWithOwner("transient", new byte[]{42}, "me"), now, MoreExecutors.newDirectExecutorService());
    assertArrayEquals("Should have set the entry",
        new byte[]{42}, Optional.ofNullable(stateMachine.values.get("transient")).map(x -> x.value).orElse(null));
    stateMachine.applyTransition(createClearTransition("not_me"), now, MoreExecutors.newDirectExecutorService());
    assertArrayEquals("Should still have value after clearing entries for someone else",
        new byte[]{42}, Optional.ofNullable(stateMachine.values.get("transient")).map(x -> x.value).orElse(null));
    assertSerializable(stateMachine);
  }


  /**
   * Make sure we don't delete non-transient values
   */
  @Test
  public void testClearTransientsForNotTransient() {
    KeyValueStateMachine stateMachine = new KeyValueStateMachine("name");
    stateMachine.applyTransition(createSetValueTransition("transient", new byte[]{42}), now, MoreExecutors.newDirectExecutorService());
    assertArrayEquals("Should have set the entry",
        new byte[]{42}, Optional.ofNullable(stateMachine.values.get("transient")).map(x -> x.value).orElse(null));
    stateMachine.applyTransition(createClearTransition("me"), now, MoreExecutors.newDirectExecutorService());
    assertArrayEquals("Should still have value after clearing entries for someone else",
        new byte[]{42}, Optional.ofNullable(stateMachine.values.get("transient")).map(x -> x.value).orElse(null));
    assertSerializable(stateMachine);
  }


  /**
   * Ensure that taking a node down releases any locks it holds.
   */
  @Test
  public void testClearTransientsForLocksDeleted() {
    KeyValueStateMachine machine = new KeyValueStateMachine("name");
    // 1. Take the lock
    machine.applyTransition(createRequestLockTransition("lock", "me", "hash1"), now, MoreExecutors.newDirectExecutorService());
    assertEquals("The state machine should know I'm an owner of something", Collections.singleton("me"), machine.owners());


    assertTrue("Should be able to get the lock", machine.createLockAcquiredFuture("lock", "me", "hash1").getNow(false));
    // 2. Someone else should not be able to take the lock
    machine.applyTransition(createRequestLockTransition("lock", "other", "hash2"), now, MoreExecutors.newDirectExecutorService());


    assertFalse(machine.createLockAcquiredFuture("lock", "other", "hash2").getNow(false));
    // 3. Take us offline
    machine.applyTransition(createClearTransition("me"), now, MoreExecutors.newDirectExecutorService());
    // 4. Other person should be able to take the lock


    assertTrue(machine.createLockAcquiredFuture("lock", "other", "hash2").getNow(false));
    assertSerializable(machine);
  }


  /**
   * A node is waiting on a lock, then goes down. It should no longer be waiting on the lock.
   */
  @Test
  public void testClearTransientsForLocksDequeued() {
    KeyValueStateMachine machine = new KeyValueStateMachine("name");
    // 1. Someone takes a lock
    machine.applyTransition(createRequestLockTransition("lock", "holder", "hash1"), now, MoreExecutors.newDirectExecutorService());
    assertTrue("Should be able to get the lock", machine.createLockAcquiredFuture("lock", "holder", "hash1").getNow(false));
    // 2. I try to take a lock
    machine.applyTransition(createRequestLockTransition("lock", "me", "hash2"), now, MoreExecutors.newDirectExecutorService());
    assertFalse(machine.createLockAcquiredFuture("lock", "me", "hash2").getNow(false));
    // 3. Someone else tries to take the lock
    machine.applyTransition(createRequestLockTransition("lock", "other", "hash3"), now, MoreExecutors.newDirectExecutorService());
    assertFalse(machine.createLockAcquiredFuture("lock", "other", "hash3").getNow(false));
    // 4. Take us offline
    machine.applyTransition(createClearTransition("me"), now, MoreExecutors.newDirectExecutorService());
    // 5. Original holder releases the lock
    machine.applyTransition(createReleaseLockTransition("lock", "holder", "hash1"), now, MoreExecutors.newDirectExecutorService());
    // 6. Other person should be able to skip ahead of us to take the lock
    assertTrue(machine.createLockAcquiredFuture("lock", "other", "hash3").getNow(false));
    // Ensure serializable
    assertSerializable(machine);
  }



  /**
   * Test {@link KeyValueStateMachine#keysIdleSince(Duration, long)}
   */
  @Test
  public void testKeysIdleSince() {
    KeyValueStateMachine stateMachine = new KeyValueStateMachine("name");
    stateMachine.applyTransition(createSetValueTransition("foo", new byte[]{42}), now, MoreExecutors.newDirectExecutorService());
    assertEquals(Collections.emptySet(), stateMachine.keysIdleSince(Duration.ofMinutes(5), now + Duration.ofMinutes(5).toMillis() - 1));
    assertEquals(Collections.singleton("foo"), stateMachine.keysIdleSince(Duration.ofMinutes(5), now + Duration.ofMinutes(5).toMillis() + 1));
    // Ensure serializable
    assertSerializable(stateMachine);
  }


  /**
   * Test {@link KeyValueStateMachine#keysIdleSince(Duration, long)}
   */
  @Test
  public void testKeysPresentSince() {
    KeyValueStateMachine stateMachine = new KeyValueStateMachine("name");
    stateMachine.applyTransition(createSetValueTransition("foo", new byte[]{42}), now, MoreExecutors.newDirectExecutorService());
    assertEquals(Collections.emptySet(), stateMachine.keysPresentSince(Duration.ofMinutes(5), now + Duration.ofMinutes(5).toMillis() - 1));
    assertEquals(Collections.singleton("foo"), stateMachine.keysPresentSince(Duration.ofMinutes(5), now + Duration.ofMinutes(5).toMillis() + 1));
    // Ensure serializable
    assertSerializable(stateMachine);
  }



  @Test
  public void testInstallSnapshotAcquireLock() throws ExecutionException, InterruptedException {
    KeyValueStateMachine stateMachine1 = new KeyValueStateMachine("name");
    stateMachine1.applyTransition(KeyValueStateMachine.createRequestLockTransition("test", "host1", "hash1"), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());
    stateMachine1.applyTransition(KeyValueStateMachine.createRequestLockTransition("test", "host1", "hash2"), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());
    KeyValueStateMachine stateMachine2 = new KeyValueStateMachine("name");
    stateMachine2.applyTransition(KeyValueStateMachine.createRequestLockTransition("test", "host1", "hash1"), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());
    stateMachine2.applyTransition(KeyValueStateMachine.createRequestLockTransition("test", "host1", "hash2"), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());
    // Verify the initial state
    assertEquals(1, stateMachine1.locks.size());
    KeyValueStateMachine.QueueLock lock = stateMachine1.locks.get("test");
    assertEquals("hash1", lock.holder.uniqueHash);
    assertEquals(1, lock.waiting.size());
    assertEquals(1, stateMachine2.locks.size());
    lock = stateMachine2.locks.get("test");
    assertEquals("hash1", lock.holder.uniqueHash);
    assertEquals(1, lock.waiting.size());
    // Create a future in stateMachine2 for the hash2 request acquiring the lock
    CompletableFuture<Boolean> future = stateMachine2.createLockAcquiredFuture("test", "host1", "hash2");
    assertFalse(future.isDone());
    // Release the lock in stateMachine1
    stateMachine1.applyTransition(KeyValueStateMachine.createReleaseLockTransition("test", "host1", "hash1"), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());
    // Verify state
    assertFalse(future.isDone());
    assertEquals(1, stateMachine1.locks.size());
    lock = stateMachine1.locks.get("test");
    assertEquals("hash2", lock.holder.uniqueHash);
    assertEquals(0, lock.waiting.size());
    // Install the stateMachine1 snapshot on stateMachine2
    stateMachine2.overwriteWithSerialized(stateMachine1.serialize(), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());
    // Verify state
    assertEquals(1, stateMachine2.locks.size());
    lock = stateMachine2.locks.get("test");
    assertEquals("hash2", lock.holder.uniqueHash);
    assertEquals(0, lock.waiting.size());
    // Verify that the future has actually been completed correctly
    assertTrue(future.isDone());
    assertTrue(future.get());
    // Ensure serializable
    assertSerializable(stateMachine1);
    assertSerializable(stateMachine2);
  }

  @Test
  public void testSetValueTransition() {
    KeyValueStateMachine stateMachine = new KeyValueStateMachine("name");
    assertEquals(0, stateMachine.values.size());
    assertEquals(0, stateMachine.keys().size());
    Map<String, byte[]> map0 = stateMachine.map();
    stateMachine.applyTransition(KeyValueStateMachine.createSetValueTransition("test", new byte[]{2}), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());

    assertEquals(1, stateMachine.values.size());
    assertEquals(2, (int)stateMachine.values.get("test").value[0]);
    assertFalse(stateMachine.values.get("test").owner.isPresent());
    assertEquals(1, stateMachine.keys().size());
    Map<String, byte[]> map1 = stateMachine.map();
    assertTrue(stateMachine.keys().contains("test"));
    stateMachine.applyTransition(KeyValueStateMachine.createSetValueTransition("test2", new byte[]{4}), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());

    assertEquals(2, stateMachine.values.size());
    assertEquals(2, (int)stateMachine.values.get("test").value[0]);
    assertFalse(stateMachine.values.get("test").owner.isPresent());
    assertEquals(4, (int)stateMachine.values.get("test2").value[0]);
    assertFalse(stateMachine.values.get("test2").owner.isPresent());
    assertEquals(2, stateMachine.keys().size());
    Map<String, byte[]> map2 = stateMachine.map();
    assertTrue(stateMachine.keys().contains("test"));
    assertTrue(stateMachine.keys().contains("test2"));

    // Check the maps are valid, and passed by value, not reference
    assertEquals(0, map0.size());
    assertEquals(1, map1.size());
    assertTrue(map1.containsKey("test"));
    assertEquals(2, map1.get("test")[0]);
    assertEquals(2, map2.size());
    assertTrue(map2.containsKey("test"));
    assertEquals(2, map2.get("test")[0]);
    assertTrue(map2.containsKey("test2"));
    assertEquals(4, map2.get("test2")[0]);

    // Ensure serializable
    assertSerializable(stateMachine);
  }

  @Test
  public void testRemoveValueTransition() {
    KeyValueStateMachine stateMachine = new KeyValueStateMachine("name");
    assertEquals(0, stateMachine.values.size());
    assertEquals(0, stateMachine.keys().size());
    stateMachine.applyTransition(KeyValueStateMachine.createSetValueTransition("test", new byte[]{2}), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());

    assertEquals(1, stateMachine.values.size());
    assertEquals(2, (int)stateMachine.values.get("test").value[0]);
    stateMachine.applyTransition(KeyValueStateMachine.createSetValueTransition("test2", new byte[]{4}), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());

    assertEquals(2, stateMachine.values.size());
    assertEquals(2, (int)stateMachine.values.get("test").value[0]);
    assertEquals(4, (int)stateMachine.values.get("test2").value[0]);
    assertEquals(2, stateMachine.keys().size());
    assertTrue(stateMachine.keys().contains("test"));
    assertTrue(stateMachine.keys().contains("test2"));
    stateMachine.applyTransition(KeyValueStateMachine.createRemoveValueTransition("test"), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());

    assertEquals(1, stateMachine.values.size());
    assertEquals(4, (int)stateMachine.values.get("test2").value[0]);
    assertEquals(1, stateMachine.keys().size());
    assertTrue(stateMachine.keys().contains("test2"));
    stateMachine.applyTransition(KeyValueStateMachine.createRemoveValueTransition("test2"), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());

    assertEquals(0, stateMachine.values.size());
    assertEquals(0, stateMachine.keys().size());

    // Ensure serializable
    assertSerializable(stateMachine);
  }

  @Test
  public void testGroupedSetValueAndReleaseLockTransition() {
    // Create empty state machine
    KeyValueStateMachine stateMachine = new KeyValueStateMachine("name");
    // Create a lock
    stateMachine.applyTransition(KeyValueStateMachine.createRequestLockTransition("test", "host1", "hash1"), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());

    // Check lock exists
    assertEquals(0, stateMachine.values.size());
    assertEquals(0, stateMachine.keys().size());
    assertEquals(1, stateMachine.locks.size());

    // Set value and release lock in same transition
    stateMachine.applyTransition(KeyValueStateMachine.createGroupedTransition(
        KeyValueStateMachine.createSetValueTransition("test", new byte[]{2}),
        KeyValueStateMachine.createReleaseLockTransition("test", "host1", "hash1")
    ), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());

    // Check value was set and lock was released
    assertEquals(1, stateMachine.values.size());
    assertEquals(2, (int)stateMachine.values.get("test").value[0]);
    assertEquals(1, stateMachine.keys().size());
    assertEquals(0, stateMachine.locks.size());

    // Ensure serializable
    assertSerializable(stateMachine);
  }


  @Test
  public void testGroupedSetValueTransition() {
    // Create empty state machine
    KeyValueStateMachine stateMachine = new KeyValueStateMachine("name");
    assertEquals(0, stateMachine.values.size());
    assertEquals(0, stateMachine.keys().size());

    // Set two values at once
    stateMachine.applyTransition(KeyValueStateMachine.createGroupedTransition(KeyValueStateMachine.createSetValueTransition("test", new byte[]{2}), KeyValueStateMachine.createSetValueTransition("test2", new byte[]{4})), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());

    // Check values were set correctly
    assertEquals(2, stateMachine.values.size());
    assertEquals(2, (int)stateMachine.values.get("test").value[0]);
    assertEquals(4, (int)stateMachine.values.get("test2").value[0]);
    assertEquals(2, stateMachine.keys().size());

    // Ensure serializable
    assertSerializable(stateMachine);
  }


  @SuppressWarnings("OptionalGetWithoutIsPresent")
  @Test
  public void testSetValueWithOwnerTransition() {
    KeyValueStateMachine stateMachine = new KeyValueStateMachine("name");
    assertEquals(0, stateMachine.values.size());
    stateMachine.applyTransition(KeyValueStateMachine.createSetValueTransitionWithOwner("test", new byte[]{2}, "owner1"), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());

    assertEquals(1, stateMachine.values.size());
    assertEquals(2, (int)stateMachine.values.get("test").value[0]);
    assertTrue(stateMachine.values.get("test").owner.isPresent());
    assertEquals("owner1", stateMachine.values.get("test").owner.get());
    stateMachine.applyTransition(KeyValueStateMachine.createSetValueTransitionWithOwner("test2", new byte[]{4}, "owner2"), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());

    assertEquals(2, stateMachine.values.size());
    assertEquals(2, (int)stateMachine.values.get("test").value[0]);
    assertEquals(4, (int)stateMachine.values.get("test2").value[0]);
    assertTrue(stateMachine.values.get("test").owner.isPresent());
    assertEquals("owner1", stateMachine.values.get("test").owner.get());
    assertTrue(stateMachine.values.get("test2").owner.isPresent());
    assertEquals("owner2", stateMachine.values.get("test2").owner.get());

    // Ensure serializable
    assertSerializable(stateMachine);
  }


  @Test
  public void testEqualsHashCode() {
    KeyValueStateMachine x = new KeyValueStateMachine("name");
    assertEquals(x, x);
    assertEquals(x, new KeyValueStateMachine("name"));
    assertEquals(x.hashCode(), x.hashCode());
    assertEquals(x.hashCode(), new KeyValueStateMachine("name").hashCode());
  }


  @Test
  public void testLockRequestEqualsHashCode() {
    KeyValueStateMachine.LockRequest x = new KeyValueStateMachine.LockRequest("server", "hash");
    assertEquals(x, x);
    assertEquals(x, new KeyValueStateMachine.LockRequest("server", "hash"));
    assertEquals(x.hashCode(), x.hashCode());
    assertEquals(x.hashCode(), new KeyValueStateMachine.LockRequest("server", "hash").hashCode());
  }


  @Test
  public void testLockRequestToString() {
    KeyValueStateMachine.LockRequest x = new KeyValueStateMachine.LockRequest("server", "hash");
    assertEquals("LockRequest from server with hash", x.toString());
  }


  /**
   * Test that {@link KeyValueStateMachine#createLockAcquiredFuture(String, String, String)}
   * waits on the future if no lock is present in the machine
   */
  @Test
  public void testCreateLockAcquiredFutureWaitIfNoLock() {
    KeyValueStateMachine x = new KeyValueStateMachine("name");


    CompletableFuture<Boolean> lockFuture = x.createLockAcquiredFuture("lockName", "me", "hash");
    assertNull("The future should not be complete if no lock is held", lockFuture.getNow(null));
  }


  /**
   * Test that {@link KeyValueStateMachine#createLockAcquiredFuture(String, String, String)}
   * returns a lock immediately if the lock is held
   */
  @Test
  public void testCreateLockAcquiredFutureCompletesImmediatelyIfHeld() {
    KeyValueStateMachine x = new KeyValueStateMachine("name");
    x.applyTransition(KeyValueStateMachine.createRequestLockTransition("lockName", "me", "hash"), 0L, MoreExecutors.newDirectExecutorService());


    CompletableFuture<Boolean> lockFuture = x.createLockAcquiredFuture("lockName", "me", "hash");
    assertTrue("The future should complete immediately with our lock held", lockFuture.getNow(null));
  }


  /**
   * Test that {@link KeyValueStateMachine#createLockAcquiredFuture(String, String, String)}
   * waits for a lock.
   */
  @Test
  public void testCreateLockAcquiredFutureWaitsForLock() {
      KeyValueStateMachine x = new KeyValueStateMachine("name");


    CompletableFuture<Boolean> lockFuture = x.createLockAcquiredFuture("lockName", "me", "hash");
      assertNull("The future should not yet be complete", lockFuture.getNow(null));
      x.applyTransition(KeyValueStateMachine.createRequestLockTransition("lockName", "me", "hash"), 0L, MoreExecutors.newDirectExecutorService());
      assertTrue("The future should complete once the lock is held", lockFuture.getNow(null));
  }


  /**
   * Test {@link KeyValueStateMachine#createLockAcquiredFuture(String, String, String)}
   * where Server A takes a lock, Server B waits, Server A releases, and Server B should have the future fire
   *
   * Very similar to {@link #testCreateLockAcquiredFutureOtherReleased()}
   */
  @Test
  public void testCreateLockAcquiredFutureContentionScript() {
    KeyValueStateMachine x = new KeyValueStateMachine("name");


    CompletableFuture<Boolean> lockFuture = x.createLockAcquiredFuture("lockName", "me", "hash");
    assertNull("The future should not yet be complete.", lockFuture.getNow(null));
    x.applyTransition(KeyValueStateMachine.createRequestLockTransition("lockName", "other", "hash"), 0L, MoreExecutors.newDirectExecutorService());
    assertNull("The future should not have completed, because the other requester made the request.", lockFuture.getNow(null));
    x.applyTransition(KeyValueStateMachine.createRequestLockTransition("lockName", "me", "hash"), 0L, MoreExecutors.newDirectExecutorService());
    assertNull("The future should not have completed, because the other requester still has the lock.", lockFuture.getNow(null));
    x.applyTransition(KeyValueStateMachine.createReleaseLockTransition("lockName", "other", "hash"), 0L, MoreExecutors.newDirectExecutorService());
    assertTrue("We should have immediately taken a lock previously owned by the other thread.", lockFuture.getNow(null));
  }


  /**
   * Test {@link KeyValueStateMachine#createLockAcquiredFuture(String, String, String)}
   * where Server A holds a lock, Server B waits, Server A releases, and Server B should have the future fire
   *
   * Very similar to {@link #testCreateLockAcquiredFutureContentionScript()}
   */
  @Test
  public void testCreateLockAcquiredFutureOtherReleased() {
    KeyValueStateMachine x = new KeyValueStateMachine("name");
    x.applyTransition(KeyValueStateMachine.createRequestLockTransition("lockName", "other", "hash"), 0L, MoreExecutors.newDirectExecutorService());


    CompletableFuture<Boolean> lockFuture = x.createLockAcquiredFuture("lockName", "me", "hash");
    assertNull("The future should not have completed, because the other requester made the request.", lockFuture.getNow(null));
    x.applyTransition(KeyValueStateMachine.createRequestLockTransition("lockName", "me", "hash"), 0L, MoreExecutors.newDirectExecutorService());
    assertNull("The future should not have completed, because the other requester still has the lock.", lockFuture.getNow(null));
    x.applyTransition(KeyValueStateMachine.createReleaseLockTransition("lockName", "other", "hash"), 0L, MoreExecutors.newDirectExecutorService());
    assertTrue("We should have immediately taken a lock previously owned by the other thread.", lockFuture.getNow(null));
  }


  @Test
  public void testDebugTransition() {
    KeyValueStateMachine x = new KeyValueStateMachine("name");
    assertEquals("set mykey = 'hello world!'",
        x.debugTransition(KeyValueStateMachineProto.Transition.newBuilder()
            .setType(KeyValueStateMachineProto.TransitionType.SET_VALUE)
            .setSetValue(KeyValueStateMachineProto.SetValue.newBuilder().setKey("mykey").setValue(ByteString.copyFromUtf8("hello world!")))
            .build().toByteArray()));
    assertEquals("remove mykey",
        x.debugTransition(KeyValueStateMachineProto.Transition.newBuilder()
            .setType(KeyValueStateMachineProto.TransitionType.REMOVE_VALUE)
            .setRemoveValue(KeyValueStateMachineProto.RemoveValue.newBuilder().setKey("mykey"))
            .build().toByteArray()));
    assertEquals("rq requests lock 'lock_name' with hash hash_value",
        x.debugTransition(KeyValueStateMachineProto.Transition.newBuilder()
            .setType(KeyValueStateMachineProto.TransitionType.REQUEST_LOCK)
            .setRequestLock(KeyValueStateMachineProto.RequestLock.newBuilder().setRequester("rq").setLock("lock_name").setUniqueHash("hash_value"))
            .build().toByteArray()));
    assertEquals("rq releases lock 'lock_name' with hash hash_value",
        x.debugTransition(KeyValueStateMachineProto.Transition.newBuilder()
            .setType(KeyValueStateMachineProto.TransitionType.RELEASE_LOCK)
            .setReleaseLock(KeyValueStateMachineProto.ReleaseLock.newBuilder().setRequester("rq").setLock("lock_name").setUniqueHash("hash_value"))
            .build().toByteArray()));
    assertEquals("Unrecognized - type UNRECOGNIZED",
        x.debugTransition(KeyValueStateMachineProto.Transition.newBuilder()
            .setTypeValue(100).build().toByteArray()));
    assertEquals("Unrecognized - invalid proto",
        x.debugTransition(new byte[2]));
  }


  /**
   * Test that we can fuzz acquiring locks from multiple threads and still have all the futures fire
   * at the right times.
   */
  @Category(SlowTests.class)
  @Test
  public void fuzzLockFutures() throws InterruptedException {
    int numLocks = 10000;
    int numThreads = 4;
    KeyValueStateMachine sm = new KeyValueStateMachine("server");
    Queue<Throwable> exceptions = new ConcurrentLinkedQueue<>();
    ExecutorService pool = Executors.newFixedThreadPool(numThreads, new ThreadFactoryBuilder().setNameFormat("raftpool-%d").setDaemon(true).setUncaughtExceptionHandler((t, e) -> exceptions.offer(e)).build());
    try {

      // 1. Submit + release the locks
      List<CompletableFuture<Boolean>> futures = new ArrayList<>();
      for (int lockI = 0; lockI < numLocks; ++lockI) {
        final String hash = Integer.toString(lockI);
        sm.applyTransition(KeyValueStateMachine.createRequestLockTransition("fuzzLock", "me", hash), TimerUtils.mockableNow().toEpochMilli(), pool);
        futures.add(
            sm.createLockAcquiredFuture("fuzzLock", "me", hash)
                .thenApply(success -> {
                  assertEquals("We should hold the lock we thing we own", hash, sm.locks.get("fuzzLock").holder.uniqueHash);
                  sm.applyTransition(KeyValueStateMachine.createReleaseLockTransition("fuzzLock", "me", hash), TimerUtils.mockableNow().toEpochMilli(), pool);
                  return success;
                })
        );
      }

      // 2. Check the futures
      for (CompletableFuture<Boolean> future : futures) {
        try {
          assertTrue("Should eventually take the lock we requested", future.get(10, TimeUnit.SECONDS));
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
          throw new RuntimeException(e);
        }
      }

      // 3. Stray exceptions
      for (Throwable t : exceptions) {
        t.printStackTrace();
      }
      assertTrue("Should have no exceptions from test", exceptions.isEmpty());

    } finally {
      pool.shutdown();
      assertTrue("Pool should close", pool.awaitTermination(5, TimeUnit.SECONDS));
    }
  }


  /**
   * Test that a {@link LockRequest} has a well-behaved equals, hashCode, and serialize.
   */
  @Test
  public void lockRequestBasicContract() {
    LockRequest original = new LockRequest("server", "hash");
    LockRequest same = new LockRequest("server", "hash");
    assertEquals(original, original);
    assertEquals(original, same);
    assertEquals(original.hashCode(), same.hashCode());
    assertEquals(original, LockRequest.deserialize(original.serialize()));
  }


  /**
   * Test that a {@link QueueLock} has a well-behaved equals, hashCode, and serialize.
   */
  @Test
  public void queueLockBasicContract() {
    LockRequest a = new LockRequest("A", "hash");
    LockRequest b = new LockRequest("B", "hash");
    LockRequest c = new LockRequest("C", "hash");
    QueueLock original = new QueueLock(a, Arrays.asList(b, c));
    QueueLock same = new QueueLock(a, Arrays.asList(b, c));

    assertEquals(original, original);
    assertEquals(original, same);
    assertEquals(original.hashCode(), same.hashCode());
    assertEquals(original, QueueLock.deserialize(original.serialize()));
  }


  /**
   * Ensure that any waiting locks that got completely bypassed and clobbered on a snapshot
   * install have their futures completed.
   */
  @Test
  public void failWaitingLocksOnSnapshotInstall() {
    // Create an initial lock state
    KeyValueStateMachine me = new KeyValueStateMachine("me");
    LockRequest a = new LockRequest("A", "1");
    LockRequest b = new LockRequest("B", "2");
    LockRequest c = new LockRequest("C", "3");
    me.locks.put("lock", new QueueLock(a, Arrays.asList(b, c)));
    assertSerializable(me);

    // Create futures waiting on things
    CompletableFuture<Boolean> futureB = me.createLockAcquiredFuture("lock", b.server, b.uniqueHash);
    CompletableFuture<Boolean> futureC = me.createLockAcquiredFuture("lock", c.server, c.uniqueHash);

    // Some foreign machine forgets locks A and B
    KeyValueStateMachine foreign = new KeyValueStateMachine("me");
    foreign.locks.put("lock", new QueueLock(c, Collections.emptyList()));
    assertSerializable(foreign);
    byte[] snapshot = foreign.serializeImpl().toByteArray();

    // We overwrite our state with the foreign snapshot
    me.overwriteWithSerializedImpl(snapshot, 0L, MoreExecutors.newDirectExecutorService());

    // Check that all our futures fired
    assertTrue("lock C (the one newly acquired) should have its future executed", futureC.isDone());
    assertTrue("lock C (the one newly acquired) should have its future return True", futureC.getNow(false));
    assertTrue("lock B (the one forgotten) should have its future executed", futureB.isDone());
    assertFalse("lock B (the one forgotten) should have its future return False, since it was never obtained", futureB.getNow(true));
  }


  /**
   * Test that a {@link LockRequest} has a well-behaved equals, hashCode, and serialize.
   */
  @Test
  public void bulkRelease() {
    int count = 100;
    // Create the transitions
    KeyValueStateMachine sm = new KeyValueStateMachine("name");
    byte[][] acquires = IntStream.range(0, count).mapToObj(i ->
        KeyValueStateMachineProto.Transition.newBuilder().setType(KeyValueStateMachineProto.TransitionType.REQUEST_LOCK)
            .setRequestLock(KeyValueStateMachineProto.RequestLock.newBuilder().setLock("lock").setRequester("me").setUniqueHash(Integer.toString(i)).build())
            .build().toByteArray()
    ).toArray(byte[][]::new);
    byte[][] releases = IntStream.range(0, count).mapToObj(i ->
        KeyValueStateMachineProto.Transition.newBuilder().setType(KeyValueStateMachineProto.TransitionType.RELEASE_LOCK)
            .setReleaseLock(KeyValueStateMachineProto.ReleaseLock.newBuilder().setLock("lock").setRequester("me").setUniqueHash(Integer.toString(i)).build())
            .build().toByteArray()
    ).toArray(byte[][]::new);

    // Create futures for the locks
    List<CompletableFuture<Boolean>> futures = IntStream.range(0, count).mapToObj(i ->
        sm.createLockAcquiredFuture("lock", "me", Integer.toString(i))
    ).collect(Collectors.toList());

    // Acquire the locks
    sm.applyTransition(KeyValueStateMachine.createGroupedTransition(acquires), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());
    assertEquals("Our first request should have the lock", "0", sm.locks.get("lock").holder.uniqueHash);
    assertEquals("" + (count - 1) + " others should be waiting on locks", count - 1, sm.locks.get("lock").waiting.size());

    // Release the locks
    sm.applyTransition(KeyValueStateMachine.createGroupedTransition(releases), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());

    // Check that everything is ok
    assertNull("We should no longer track the lock", sm.locks.get("lock"));
    assertTrue("The first future should have succeeded", futures.get(0).getNow(false));
    for (int i = 1; i < count; ++i) {
      assertFalse("Later futures (" + i + ") should have failed", futures.get(i).getNow(true));
    }
  }


  // --------------------------------------------------------------------------
  // Custom Serialization
  // --------------------------------------------------------------------------


  /**
   * Test writing this value to a raw byte buffer.
   */
  @Test
  public void serializeValueToBytes() {
    ValueWithOptionalOwner value = new ValueWithOptionalOwner(new byte[]{1, 2, 3, 4}, "owner", 42L, Long.MAX_VALUE);
    int size = value.byteSize();
    assertEquals("The size of the buffer should match my hand-computed size", (4 + 4) + (4 + 5*2) + 16, size);
    // Write
    byte[] buffer = new byte[size];
    value.serializeInto(buffer, 0);
    byte[] tooSmall = new byte[size - 1];
    try {
      value.serializeInto(tooSmall, 0);
      fail("Size should be exact");
    } catch (IndexOutOfBoundsException ignored) {}
    // Read
    ValueWithOptionalOwner reread = ValueWithOptionalOwner.readFrom(buffer, 0);
    assertEquals(value, reread);
    assertArrayEquals(value.value, reread.value);
    assertEquals(value.owner, reread.owner);
    assertEquals(value.lastAccessed, reread.lastAccessed);
    assertEquals(value.createdAt, reread.createdAt);
  }


  /**
   * Test writing this value to a lock request.
   */
  @Test
  public void serializeLockRequestToBytes() {
    LockRequest value = new LockRequest("server", "hash");
    int size = value.byteSize();
    assertEquals("The size of the buffer should match my hand-computed size", (4 + 6*2) + (4 + 4*2), size);
    // Write
    byte[] buffer = new byte[size];
    value.serializeInto(buffer, 0);
    byte[] tooSmall = new byte[size - 1];
    try {
      value.serializeInto(tooSmall, 0);
      fail("Size should be exact");
    } catch (IndexOutOfBoundsException ignored) {}
    // Read
    LockRequest reread = LockRequest.readFrom(buffer, 0);
    assertEquals(value, reread);
    assertEquals(value.server, reread.server);
    assertEquals(value.uniqueHash, reread.uniqueHash);
  }


  /**
   * Test writing this value to a queue lock.
   */
  @Test
  public void serializeLockToBytes() {
    LockRequest holder = new LockRequest("server1", "hash1");
    LockRequest waiting1 = new LockRequest("server2", "hash2");
    LockRequest waiting2 = new LockRequest("server3", "hash3");
    QueueLock value = new QueueLock(holder, Arrays.asList(waiting1, waiting2));
    int size = value.byteSize();
    // Write
    byte[] buffer = new byte[size];
    value.serializeInto(buffer, 0);
    byte[] tooSmall = new byte[size - 1];
    try {
      value.serializeInto(tooSmall, 0);
      fail("Size should be exact");
    } catch (IndexOutOfBoundsException ignored) {}
    // Read
    QueueLock reread = QueueLock.readFrom(buffer, 0);
    assertEquals(value, reread);
    assertEquals(value.holder, reread.holder);
    assertEquals(value.holder.server, reread.holder.server);
    assertEquals(value.holder.uniqueHash, reread.holder.uniqueHash);
    assertEquals(2, reread.waiting.size());
    Iterator<LockRequest> iter = reread.waiting.iterator();
    assertEquals(waiting1, iter.next());
    assertEquals(waiting2, iter.next());
  }


  // --------------------------------------------------------------------------
  // Benchmarks
  // --------------------------------------------------------------------------

  /**
   * Benchmark raw lock + edit + unlock performance
   */
  @Ignore
  @Test
  public void benchmarkLockedUpdate() {
    // The number of iterations to run the system for to burn in the JIT
    int burnin = 1000;
    // The number of transitions to make
    int size = 1000000;
    // The size of the lock wait queue when we run the system
    int lockQueueSize = 100;
    // The number of change listeners on the state machine
    int changeListenerCount = 1000;

    KeyValueStateMachine x = new KeyValueStateMachine("name");
    for (int i = 0; i < lockQueueSize; ++i) {
      x.applyTransition(KeyValueStateMachineProto.Transition.newBuilder().setType(KeyValueStateMachineProto.TransitionType.REQUEST_LOCK)
              .setRequestLock(KeyValueStateMachineProto.RequestLock.newBuilder().setLock("lock").setRequester("me").setUniqueHash("hash").build())
              .build().toByteArray(),
          TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());

    }

    for (int i = 0; i < changeListenerCount; ++i) {
      x.addChangeListener((changedKey, newValue, state, pool) -> {
        // noop
      });
    }

    long startNanos = 0L;
    for (int i = -burnin; i < size; ++i) {
      if (i == 0) {
        startNanos = System.nanoTime();
      }
      // 1. Take the lock
      x.applyTransition(KeyValueStateMachineProto.Transition.newBuilder().setType(KeyValueStateMachineProto.TransitionType.REQUEST_LOCK)
          .setRequestLock(KeyValueStateMachineProto.RequestLock.newBuilder().setLock("lock").setRequester("me").setUniqueHash("hash").build())
          .build().toByteArray(),
          TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());
      // 2. Apply the transition
      x.applyTransition(KeyValueStateMachineProto.Transition.newBuilder().setType(KeyValueStateMachineProto.TransitionType.SET_VALUE)
          .setSetValue(KeyValueStateMachineProto.SetValue.newBuilder().setKey("" + i).setValue(ByteString.copyFromUtf8("hello world!")))
          .build().toByteArray(),
          TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());
      // 3. Release the lock
      x.applyTransition(KeyValueStateMachineProto.Transition.newBuilder().setType(KeyValueStateMachineProto.TransitionType.RELEASE_LOCK)
              .setReleaseLock(KeyValueStateMachineProto.ReleaseLock.newBuilder().setLock("lock").setRequester("me").setUniqueHash("hash").build())
              .build().toByteArray(),
          TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());
    }
    long endNanos = System.nanoTime();

    long aveNanos = (endNanos - startNanos) / size;
    double aveMillis = ((double) aveNanos) / 1000000.0;
    System.out.println("Ave. time / transition = " + aveNanos + " ns  (" + aveMillis + " ms)");
  }


  @Ignore
  @Test
  public void benchmarkManyKeys() {
    int[] sizes = new int[]{1000, 10000, 100000, 1000000};

    for (int size : sizes) {
      KeyValueStateMachine x = new KeyValueStateMachine("name");
      long startTime = System.currentTimeMillis();
      for (int i = 0; i < size; i++) {
        x.applyTransition(KeyValueStateMachineProto.Transition.newBuilder().setType(KeyValueStateMachineProto.TransitionType.SET_VALUE).setSetValue(KeyValueStateMachineProto.SetValue.newBuilder().setKey(""+i).setValue(ByteString.copyFromUtf8("hello world!"))).build().toByteArray(), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());
      }
      long entries = System.currentTimeMillis() - startTime;
      System.out.println("Applying "+size+" transitions: "+ TimerUtils.formatTimeDifference(entries));
      System.out.println("Avg per transition: "+((double)entries / size)+"ms");
      long startSerialization = System.currentTimeMillis();
      @SuppressWarnings("unused") byte[] serialized = x.serialize();
      long serialize = System.currentTimeMillis() - startSerialization;
      System.out.println("Serializing "+size+" keys: "+ TimerUtils.formatTimeDifference(serialize));
      System.out.println("Avg per key: "+((double)serialize / size)+"ms");
    }
  }

  @Ignore
  @Test
  public void benchmarkManyWrites() {
    int[] sizes = new int[]{1000, 10000, 100000, 1000000};

    for (int size : sizes) {
      KeyValueStateMachine x = new KeyValueStateMachine("name");
      long startTime = System.currentTimeMillis();
      for (int i = 0; i < size; i++) {
        int key = i % 20;
        x.applyTransition(KeyValueStateMachineProto.Transition.newBuilder().setType(KeyValueStateMachineProto.TransitionType.SET_VALUE).setSetValue(KeyValueStateMachineProto.SetValue.newBuilder().setKey(""+key).setValue(ByteString.copyFromUtf8("hello world!"))).build().toByteArray(), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());
      }
      long entries = System.currentTimeMillis() - startTime;
      System.out.println("Applying "+size+" transitions: "+ TimerUtils.formatTimeDifference(entries));
      System.out.println("Avg per transition: "+((double)entries / size)+"ms");
    }
  }

  @Ignore
  @Test
  public void benchmarkReleaseLock() {
    // Params
    int extraLocks = 1000;
    int numReleases = 6226;  // a real use case :(
    int iters = 10000;

    // Setup
    KeyValueStateMachine sm = new KeyValueStateMachine("name");
    byte[][] alreadyTaken = IntStream.range(0, extraLocks).mapToObj(i ->
      KeyValueStateMachineProto.Transition.newBuilder().setType(KeyValueStateMachineProto.TransitionType.REQUEST_LOCK)
        .setRequestLock(KeyValueStateMachineProto.RequestLock.newBuilder().setLock("lock").setRequester("me").setUniqueHash(Integer.toString(i)).build())
        .build().toByteArray()
    ).toArray(byte[][]::new);
    byte[][] acquires = IntStream.range(0, numReleases).mapToObj(i ->
        KeyValueStateMachineProto.Transition.newBuilder().setType(KeyValueStateMachineProto.TransitionType.REQUEST_LOCK)
            .setRequestLock(KeyValueStateMachineProto.RequestLock.newBuilder().setLock("lock").setRequester("me").setUniqueHash(Integer.toString(extraLocks + i)).build())
            .build().toByteArray()
    ).toArray(byte[][]::new);
    byte[][] releases = IntStream.range(0, numReleases).mapToObj(i ->
            KeyValueStateMachineProto.Transition.newBuilder().setType(KeyValueStateMachineProto.TransitionType.RELEASE_LOCK)
                .setReleaseLock(KeyValueStateMachineProto.ReleaseLock.newBuilder().setLock("lock").setRequester("me").setUniqueHash(Integer.toString(extraLocks + i)).build())
                .build().toByteArray()
    ).toArray(byte[][]::new);
    sm.applyTransition(KeyValueStateMachine.createGroupedTransition(alreadyTaken), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());

    // Burn-in
    for (int i = 0; i < 10; ++i) {
      sm.applyTransition(KeyValueStateMachine.createGroupedTransition(acquires), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());
      sm.applyTransition(KeyValueStateMachine.createGroupedTransition(releases), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());
    }

    // Test
    long sum = 0L;
    for (int i = 0; i < iters; ++i) {
      sm.applyTransition(KeyValueStateMachine.createGroupedTransition(acquires), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());
      long start = System.nanoTime();
      sm.applyTransition(KeyValueStateMachine.createGroupedTransition(releases), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());
      sum += System.nanoTime() - start;
    }

    // Report
    long aveNanos = (sum) / iters;
    double aveMillis = ((double) aveNanos) / 1000000.0;
    log.info("took {}ns = {}ms to release {} locks in bulk release", aveNanos, aveMillis, numReleases);

    // Asserts
    assertEquals(extraLocks - 1, sm.locks.get("lock").waiting.size());
  }
}