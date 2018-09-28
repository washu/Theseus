package ai.eloquent.raft;

import ai.eloquent.util.Pointer;
import ai.eloquent.util.TimerUtils;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import org.junit.Ignore;
import org.junit.Test;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.Assert.*;
import static ai.eloquent.raft.KeyValueStateMachine.*;

/**
 * This is mostly just verifying the serializations are working correctly. Also transitions.
 */
@SuppressWarnings("ConstantConditions")
public class KeyValueStateMachineTest {

  private long now = TimerUtils.mockableNow().toEpochMilli();

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

  }

  @Test
  public void testChangeListeners() {
    KeyValueStateMachine stateMachine = new KeyValueStateMachine("name");
    Pointer<Integer> numChanges = new Pointer<>(0);
    Pointer<Map<String,byte[]>> lastChange = new Pointer<>(new HashMap<>());

    KeyValueStateMachine.ChangeListener changeListener = (key, value, map) -> {
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
  }

  @Test
  public void testSerializeQueueLock() {
    KeyValueStateMachine.QueueLock emptyLock = new KeyValueStateMachine.QueueLock(Optional.empty(), new ArrayList<>());
    KeyValueStateMachine.QueueLock recoveredEmptyLock = KeyValueStateMachine.QueueLock.deserialize(emptyLock.serialize());
    assertFalse(recoveredEmptyLock.holder.isPresent());
    assertEquals(0, recoveredEmptyLock.waiting.size());

    KeyValueStateMachine.QueueLock heldNoWaiting = new KeyValueStateMachine.QueueLock(Optional.of(new KeyValueStateMachine.LockRequest("holder", "hash1")), new ArrayList<>());
    KeyValueStateMachine.QueueLock recoveredHeldNoWaiting = KeyValueStateMachine.QueueLock.deserialize(heldNoWaiting.serialize());
    assertTrue(recoveredHeldNoWaiting.holder.isPresent());
    assertEquals(new KeyValueStateMachine.LockRequest("holder", "hash1"), recoveredHeldNoWaiting.holder.get());
    assertEquals(0, recoveredHeldNoWaiting.waiting.size());

    List<KeyValueStateMachine.LockRequest> waiting = new ArrayList<>();
    waiting.add(new KeyValueStateMachine.LockRequest("a", "hash2"));
    waiting.add(new KeyValueStateMachine.LockRequest("b", "hash3"));
    KeyValueStateMachine.QueueLock heldWaiting = new KeyValueStateMachine.QueueLock(Optional.of(new KeyValueStateMachine.LockRequest("holder", "hash1")), waiting);
    KeyValueStateMachine.QueueLock recoveredHeldWaiting = KeyValueStateMachine.QueueLock.deserialize(heldWaiting.serialize());
    assertTrue(recoveredHeldWaiting.holder.isPresent());
    assertEquals(new KeyValueStateMachine.LockRequest("holder", "hash1"), recoveredHeldWaiting.holder.get());
    assertEquals(2, recoveredHeldWaiting.waiting.size());
    assertEquals(waiting, recoveredHeldWaiting.waiting);
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
    KeyValueStateMachine.QueueLock heldWaiting = new KeyValueStateMachine.QueueLock(Optional.of(new KeyValueStateMachine.LockRequest("holder", "hash1")), waiting);
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
    assertTrue(recoveredHeldWaiting.holder.isPresent());
    assertEquals(new KeyValueStateMachine.LockRequest("holder", "hash1"), recoveredHeldWaiting.holder.get());
    assertEquals(2, recoveredHeldWaiting.waiting.size());
    assertEquals(waiting, recoveredHeldWaiting.waiting);
  }


  @Test
  public void testRequestLockTransition() throws ExecutionException, InterruptedException {
    KeyValueStateMachine stateMachine = new KeyValueStateMachine("name");
    assertEquals(0, stateMachine.locks.size());

    stateMachine.applyTransition(KeyValueStateMachine.createRequestLockTransition("test", "host1", "hash1"), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());

    assertEquals(1, stateMachine.locks.size());
    KeyValueStateMachine.QueueLock lock = stateMachine.locks.get("test");
    assertEquals("host1", lock.holder.get().server);
    assertEquals(0, lock.waiting.size());

    CompletableFuture<Boolean> host1Acquired = stateMachine.createLockAcquiredFuture("test", "host1", "hash1");
    assertTrue(host1Acquired.isDone());
    assertTrue(host1Acquired.get());

    stateMachine.applyTransition(KeyValueStateMachine.createRequestLockTransition("test", "host2", "hash2"), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());

    stateMachine.applyTransition(KeyValueStateMachine.createRequestLockTransition("test", "host3", "hash3"), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());

    CompletableFuture<Boolean> host2Acquired = stateMachine.createLockAcquiredFuture("test", "host2", "hash2");
    CompletableFuture<Boolean> host3Acquired = stateMachine.createLockAcquiredFuture("test", "host3", "hash3");

    assertTrue(host1Acquired.isDone());
    assertFalse(host2Acquired.isDone());
    assertFalse(host3Acquired.isDone());

    assertEquals(1, stateMachine.locks.size());
    lock = stateMachine.locks.get("test");
    assertEquals("host1", lock.holder.get().server);
    assertEquals(2, lock.waiting.size());
    assertEquals("host2", lock.waiting.get(0).server);
    assertEquals("host3", lock.waiting.get(1).server);

    assertTrue(host1Acquired.isDone());
    assertFalse(host2Acquired.isDone());
    assertFalse(host3Acquired.isDone());

    stateMachine.applyTransition(KeyValueStateMachine.createRequestLockTransition("test2", "host4", "hash4"), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());

    assertEquals(2, stateMachine.locks.size());
    KeyValueStateMachine.QueueLock lock2 = stateMachine.locks.get("test2");
    assertEquals("host4", lock2.holder.get().server);
    assertEquals(0, lock2.waiting.size());
  }



  @Test
  public void testTryLockTransition() throws ExecutionException, InterruptedException {
    KeyValueStateMachine stateMachine = new KeyValueStateMachine("name");
    assertEquals(0, stateMachine.locks.size());

    // Take a lock on hash1
    stateMachine.applyTransition(KeyValueStateMachine.createTryLockTransition("test", "host1", "hash1"), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());
    assertEquals(1, stateMachine.locks.size());
    KeyValueStateMachine.QueueLock lock = stateMachine.locks.get("test");
    assertEquals("host1", lock.holder.get().server);
    assertEquals(0, lock.waiting.size());
    CompletableFuture<Boolean> hash1Future = stateMachine.createLockAcquiredFuture("test", "host1", "hash1");
    assertTrue("hash1 should be taken", hash1Future.getNow(false));

    // Try+fail taking hash2
    stateMachine.applyTransition(KeyValueStateMachine.createTryLockTransition("test", "host2", "hash2"), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());
    assertEquals(1, stateMachine.locks.size());
    lock = stateMachine.locks.get("test");
    assertEquals("host1 should still hold the lock", "host1", lock.holder.get().server);
    assertEquals("No one should be waiting on hash2", 0, lock.waiting.size());
    CompletableFuture<Boolean> hash2Future = stateMachine.createLockAcquiredFuture("test", "host1", "hash2");
    assertNull("hash2 should NOT be taken", hash2Future.getNow(null));
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
    assertEquals("host1", lock.holder.get().server);
    assertEquals(0, lock.waiting.size());

    // This should be a no-op since we're the wrong host
    stateMachine.applyTransition(KeyValueStateMachine.createReleaseLockTransition("test", "host2", "hash2"), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());

    assertEquals(1, stateMachine.locks.size());
    lock = stateMachine.locks.get("test");
    assertEquals("host1", lock.holder.get().server);
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
    assertEquals("host1", lock.holder.get().server);
    assertEquals(2, lock.waiting.size());
    assertEquals("host2", lock.waiting.get(0).server);
    assertEquals("host3", lock.waiting.get(1).server);

    assertTrue(host1Acquired.isDone());
    assertFalse(host2Acquired.isDone());
    assertFalse(host3Acquired.isDone());

    stateMachine.applyTransition(KeyValueStateMachine.createReleaseLockTransition("test", "host1", "hash1"), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());

    assertTrue(host2Acquired.isDone());
    assertFalse(host3Acquired.isDone());

    assertEquals(1, stateMachine.locks.size());
    lock = stateMachine.locks.get("test");
    assertEquals("host2", lock.holder.get().server);
    assertEquals(1, lock.waiting.size());
    assertEquals("host3", lock.waiting.get(0).server);

    stateMachine.applyTransition(KeyValueStateMachine.createReleaseLockTransition("test", "host2", "hash2"), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());

    assertTrue(host3Acquired.isDone());

    assertEquals(1, stateMachine.locks.size());
    lock = stateMachine.locks.get("test");
    assertEquals("host3", lock.holder.get().server);
    assertEquals(0, lock.waiting.size());

    stateMachine.applyTransition(KeyValueStateMachine.createReleaseLockTransition("test", "host3", "hash3"), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());

    assertEquals(0, stateMachine.locks.size());
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
    assertEquals("host1", lock.holder.get().server);
    assertEquals(2, lock.waiting.size());
    assertEquals("host2", lock.waiting.get(0).server);
    assertEquals("host3", lock.waiting.get(1).server);

    // Test that we can release a lock that's queued

    stateMachine.applyTransition(KeyValueStateMachine.createReleaseLockTransition("test", "host2", "hash2"), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());

    assertEquals(1, stateMachine.locks.size());
    lock = stateMachine.locks.get("test");
    assertEquals("host1", lock.holder.get().server);
    assertEquals(1, lock.waiting.size());
    assertEquals("host3", lock.waiting.get(0).server);

    // This should be a no-op, since the hash is wrong

    stateMachine.applyTransition(KeyValueStateMachine.createReleaseLockTransition("test", "host3", "wrong-hash"), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());

    assertEquals(1, stateMachine.locks.size());
    lock = stateMachine.locks.get("test");
    assertEquals("host1", lock.holder.get().server);
    assertEquals(1, lock.waiting.size());
    assertEquals("host3", lock.waiting.get(0).server);

    // Do that again

    stateMachine.applyTransition(KeyValueStateMachine.createReleaseLockTransition("test", "host3", "hash3"), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());

    assertEquals(1, stateMachine.locks.size());
    lock = stateMachine.locks.get("test");
    assertEquals("host1", lock.holder.get().server);
    assertEquals(0, lock.waiting.size());

    // Clean up the lock

    stateMachine.applyTransition(KeyValueStateMachine.createReleaseLockTransition("test", "host1", "hash1"), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());

    assertEquals(0, stateMachine.locks.size());
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
  }


  @Test
  public void testIdempotentLocks() {
    KeyValueStateMachine stateMachine = new KeyValueStateMachine("name");
    assertEquals(0, stateMachine.locks.size());

    stateMachine.applyTransition(KeyValueStateMachine.createRequestLockTransition("test", "host", "hash1"), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());

    assertEquals(1, stateMachine.locks.size());
    KeyValueStateMachine.QueueLock lock = stateMachine.locks.get("test");
    assertEquals("host", lock.holder.get().server);
    assertEquals(0, lock.waiting.size());

    stateMachine.applyTransition(KeyValueStateMachine.createRequestLockTransition("test", "host", "hash2"), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());

    stateMachine.applyTransition(KeyValueStateMachine.createRequestLockTransition("test", "host", "hash3"), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());

    assertEquals(1, stateMachine.locks.size());
    lock = stateMachine.locks.get("test");
    assertEquals("host", lock.holder.get().server);
    assertEquals("hash1", lock.holder.get().uniqueHash);
    assertEquals(2, lock.waiting.size());
    assertEquals("host", lock.waiting.get(0).server);
    assertEquals("hash2", lock.waiting.get(0).uniqueHash);
    assertEquals("host", lock.waiting.get(1).server);
    assertEquals("hash3", lock.waiting.get(1).uniqueHash);

    // This should be ignored
    stateMachine.applyTransition(KeyValueStateMachine.createRequestLockTransition("test", "host", "hash1"), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());

    assertEquals(1, stateMachine.locks.size());
    lock = stateMachine.locks.get("test");
    assertEquals("host", lock.holder.get().server);
    assertEquals("hash1", lock.holder.get().uniqueHash);
    assertEquals(2, lock.waiting.size());
    assertEquals("host", lock.waiting.get(0).server);
    assertEquals("hash2", lock.waiting.get(0).uniqueHash);
    assertEquals("host", lock.waiting.get(1).server);
    assertEquals("hash3", lock.waiting.get(1).uniqueHash);

    // This should be ignored
    stateMachine.applyTransition(KeyValueStateMachine.createRequestLockTransition("test", "host", "hash2"), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());

    assertEquals(1, stateMachine.locks.size());
    lock = stateMachine.locks.get("test");
    assertEquals("host", lock.holder.get().server);
    assertEquals("hash1", lock.holder.get().uniqueHash);
    assertEquals(2, lock.waiting.size());
    assertEquals("host", lock.waiting.get(0).server);
    assertEquals("hash2", lock.waiting.get(0).uniqueHash);
    assertEquals("host", lock.waiting.get(1).server);
    assertEquals("hash3", lock.waiting.get(1).uniqueHash);

    // This should be ignored
    stateMachine.applyTransition(KeyValueStateMachine.createRequestLockTransition("test", "host", "hash3"), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());

    assertEquals(1, stateMachine.locks.size());
    lock = stateMachine.locks.get("test");
    assertEquals("host", lock.holder.get().server);
    assertEquals("hash1", lock.holder.get().uniqueHash);
    assertEquals(2, lock.waiting.size());
    assertEquals("host", lock.waiting.get(0).server);
    assertEquals("hash2", lock.waiting.get(0).uniqueHash);
    assertEquals("host", lock.waiting.get(1).server);
    assertEquals("hash3", lock.waiting.get(1).uniqueHash);
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
    assertEquals("hash1", lock.holder.get().uniqueHash);
    assertEquals(1, lock.waiting.size());
    assertEquals(1, stateMachine2.locks.size());
    lock = stateMachine2.locks.get("test");
    assertEquals("hash1", lock.holder.get().uniqueHash);
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
    assertEquals("hash2", lock.holder.get().uniqueHash);
    assertEquals(0, lock.waiting.size());

    // Install the stateMachine1 snapshot on stateMachine2

    stateMachine2.overwriteWithSerialized(stateMachine1.serialize(), TimerUtils.mockableNow().toEpochMilli(), MoreExecutors.newDirectExecutorService());

    // Verify state

    assertEquals(1, stateMachine2.locks.size());
    lock = stateMachine2.locks.get("test");
    assertEquals("hash2", lock.holder.get().uniqueHash);
    assertEquals(0, lock.waiting.size());

    // Verify that the future has actually been completed correctly

    assertTrue(future.isDone());
    assertTrue(future.get());
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
  }

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
      byte[] serialized = x.serialize();
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
}