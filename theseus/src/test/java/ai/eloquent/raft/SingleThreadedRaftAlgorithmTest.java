package ai.eloquent.raft;

import static org.junit.Assert.*;

import org.junit.Test;

import ai.eloquent.raft.SingleThreadedRaftAlgorithm.RaftDeque;

import static ai.eloquent.raft.SingleThreadedRaftAlgorithm.TaskPriority.*;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Test {@link SingleThreadedRaftAlgorithm} and the nested subclasses
 *
 * @author <a href="mailto:gabor@eloquent.ai">Gabor Angeli</a>
 */
public class SingleThreadedRaftAlgorithmTest {


  /** Create a new RaftTask */
  private static SingleThreadedRaftAlgorithm.RaftTask task(String name, SingleThreadedRaftAlgorithm.TaskPriority priority) {
    return new SingleThreadedRaftAlgorithm.RaftTask(
        name,
        priority,
        () -> {},
        e -> {}
    );
  }

  /** Create a new RaftTask */
  private static SingleThreadedRaftAlgorithm.RaftTask task(String name) {
    return task(name, HIGH);
  }


  // --------------------------------------------------------------------------
  // RaftTask
  // --------------------------------------------------------------------------

  /**
   * Test that {@link ai.eloquent.raft.SingleThreadedRaftAlgorithm.RaftTask} has a {@link Object#toString()}.
   */
  @Test
  public void raftTaskToString() {
    assertEquals("foo", task("foo").toString());
  }


  // --------------------------------------------------------------------------
  // RaftDeque
  // Standard Tests
  // These are adapted from https://gist.github.com/aybabtme/3462387
  // --------------------------------------------------------------------------


  /** The actual Raft deque */
  private RaftDeque deque = new RaftDeque();

  @Test
  public void dequeDeque() {
    assertNotNull(deque);
  }

  @Test
  public void dequeIsEmpty() {
    assertTrue("Initialized queue should be empty", deque.isEmpty());
  }

  @Test
  public void dequeIsEmptyAfterAddRemoveFirst() {
    deque.addFirst(task("Something"));
    boolean empty = deque.isEmpty();
    assertFalse( empty );
    deque.removeFirst();

    empty = deque.isEmpty();
    assertTrue("Should be empty after adding then removing",
        empty);

  }

  @Test
  public void dequeIsEmptyAfterAddRemoveLast() {
    deque.addLast(task("Something"));
    assertFalse(deque.isEmpty());
    deque.removeLast();
    assertTrue("Should be empty after adding then removing",
        deque.isEmpty());

  }

  @Test
  public void dequeIsEmptyAfterAddFirstRemoveLast() {
    deque.addFirst(task("Something"));
    assertFalse(deque.isEmpty());
    deque.removeLast();
    assertTrue("Should be empty after adding then removing",
        deque.isEmpty());
  }

  @Test
  public void dequeIsEmptyAfterAddLastRemoveFirst() {
    deque.addLast(task("Something"));
    assertFalse(deque.isEmpty());
    deque.removeFirst();
    assertTrue("Should be empty after adding then removing",
        deque.isEmpty());
  }

  @Test
  public void dequeIsEmptyAfterMultipleAddRemove(){
    for(int i = 0; i < RaftDeque.MAX_SIZE; i++){
      deque.addFirst(task("Something"));
      assertFalse("Should not be empty after " + i + " item added",
          deque.isEmpty());
    }

    for(int i = 0; i < RaftDeque.MAX_SIZE; i++){
      assertFalse("Should not be empty after " + i + " item removed",
          deque.isEmpty());
      deque.removeLast();
    }

    assertTrue( "Should be empty after adding and removing "
            + RaftDeque.MAX_SIZE + " elements.",
        deque.isEmpty() );
  }

  @SuppressWarnings("ConstantConditions")
  @Test
  public void dequeMultipleFillAndEmpty(){
    for(int tries = 0; tries < 50; tries++){
      for(int i = 0; i < RaftDeque.MAX_SIZE; i++){
        deque.addFirst(task(String.valueOf(i)));
      }

      assertFalse( deque.isEmpty() );
      int i = 0;
      while( !deque.isEmpty() ){
        assertEquals( String.valueOf(i), deque.removeLast().debugString );
        i++;
      }

      assertTrue( deque.isEmpty() );

      for(int j = 0; j < RaftDeque.MAX_SIZE; j++){
        deque.addLast(task(String.valueOf(j)));
      }

      assertFalse( deque.isEmpty() );

      i = 0;
      while( !deque.isEmpty() ){
        assertEquals( String.valueOf(i), deque.removeFirst().debugString );
        i++;
      }

      assertTrue( deque.isEmpty() );
    }
  }

  @Test
  public void dequeSize() {
    assertEquals( 0, deque.size() );
    for(int i = 0; i < RaftDeque.MAX_SIZE; i++){
      deque.addFirst(task("Something"));
      assertEquals(i+1, deque.size() );
    }

    for(int i = RaftDeque.MAX_SIZE; i > 0; i--){
      assertEquals(i, deque.size() );
      deque.removeLast();
    }

    assertEquals( 0, deque.size() );
  }

  @Test
  public void dequeAddFirst() {
    String[] aBunchOfString = {
        "One",
        "Two",
        "Three",
        "Four"
    };

    for(String aString : aBunchOfString){
      deque.addFirst(task(aString));
    }

    for(int i = aBunchOfString.length - 1; i >= 0; i--){
      assertEquals(aBunchOfString[i], deque.removeFirst().debugString);
    }
  }

  @Test
  public void dequeAddLast() {
    String[] aBunchOfString = {
        "One",
        "Two",
        "Three",
        "Four"
    };

    for(String aString : aBunchOfString){
      deque.addLast(task(aString));
    }

    for(int i = aBunchOfString.length - 1; i >= 0; i--){
      assertEquals(aBunchOfString[i], deque.removeLast().debugString);
    }
  }

  @Test
  public void dequeAddNull(){
    try {
      deque.addFirst(null);
      fail("Should have thrown a NullPointerException");
    } catch (NullPointerException npe){
      // Continue
    } catch (Exception e){
      fail("Wrong exception catched." + e);
    }

    try {
      deque.addLast(null);
      fail("Should have thrown a NullPointerException");
    } catch (NullPointerException npe){
      // Continue
    } catch (Exception e){
      fail("Wrong exception catched." + e);
    }
  }

  @Test
  public void dequeRemoveFirst() {
    for(int i = 0; i < RaftDeque.MAX_SIZE; i++){
      deque.addFirst( task(String.valueOf(i)) );
      assertEquals(String.valueOf(i), deque.removeFirst().debugString);
    }

    deque = new RaftDeque();

    for(int i = 0; i < RaftDeque.MAX_SIZE; i++){
      deque.addLast( task(String.valueOf(i)) );
      assertEquals(String.valueOf(i), deque.removeFirst().debugString);
    }

    deque = new RaftDeque();

    for(int i = 0; i < RaftDeque.MAX_SIZE; i++){
      deque.addLast( task(String.valueOf(i)) );
    }

    for(int i = 0; i < RaftDeque.MAX_SIZE; i++){
      assertEquals(String.valueOf(i), deque.removeFirst().debugString);
    }

  }

  @Test
  public void dequeRemoveLast() {
    for(int i = 0; i < RaftDeque.MAX_SIZE; i++){
      deque.addFirst( task(String.valueOf(i)) );
      assertEquals(String.valueOf(i), deque.removeLast().debugString);
    }

    deque = new RaftDeque();

    for(int i = 0; i < RaftDeque.MAX_SIZE; i++){
      deque.addLast( task(String.valueOf(i)) );
      assertEquals(String.valueOf(i), deque.removeLast().debugString);
    }

    deque = new RaftDeque();

    for(int i = 0; i < RaftDeque.MAX_SIZE; i++){
      deque.addFirst( task(String.valueOf(i)) );
    }

    for(int i = 0; i < RaftDeque.MAX_SIZE; i++){
      assertEquals(String.valueOf(i), deque.removeLast().debugString);
    }
  }

  @Test
  public void dequeRemoveEmpty() {
    try {
      assertTrue(deque.isEmpty());
      deque.removeFirst();
      fail("Expected a NoSuchElementException");
    } catch ( NoSuchElementException nsee){
      // Continue
    } catch ( Exception e ){
      fail( "Unexpected exception : " + e );
    }

    try {
      assertTrue(deque.isEmpty());
      deque.removeLast();
      fail("Expected a NoSuchElementException");
    } catch ( NoSuchElementException nsee){
      // Continue
    } catch ( Exception e ){
      fail( "Unexpected exception : " + e );
    }

    try {
      assertTrue(deque.isEmpty());

      for(int i = 0; i < RaftDeque.MAX_SIZE; i ++ ){
        deque.addLast( task(String.valueOf(i)) );
      }
      for(int i = 0; i < RaftDeque.MAX_SIZE; i ++ ){
        deque.removeLast();
      }
      deque.removeLast();
      fail("Expected a NoSuchElementException");
    } catch ( NoSuchElementException nsee){
      // Continue
    } catch ( Exception e ){
      fail( "Unexpected exception : " + e );
    }
  }

  @Test
  public void dequeIterator() {

    Iterator<SingleThreadedRaftAlgorithm.RaftTask> anIterator = deque.iterator();
    assertFalse( anIterator.hasNext() );

    for(int i = 0; i < RaftDeque.MAX_SIZE; i++){
      deque.addFirst( task(String.valueOf(i)) );
    }

    anIterator = deque.iterator();

    assertTrue( anIterator.hasNext() );

    int i = RaftDeque.MAX_SIZE - 1;
    for(SingleThreadedRaftAlgorithm.RaftTask aString : deque){
      assertEquals( String.valueOf(i), aString.debugString);
      i--;
    }

    anIterator = deque.iterator();

    assertTrue( anIterator.hasNext() );

    int j = RaftDeque.MAX_SIZE - 1;
    while( anIterator.hasNext() ){
      assertEquals( String.valueOf(j), anIterator.next().debugString);
      j--;
    }
  }

  @Test
  public void dequeIteratorNoMoreItem() {
    Iterator<SingleThreadedRaftAlgorithm.RaftTask> anIterator = deque.iterator();
    while( anIterator.hasNext() ){
      anIterator.next();
    }
    try {
      anIterator.next();
      fail( "Should have thrown a NoSuchElementException.");
    } catch( NoSuchElementException nsee ){
      // Continue
    } catch( Exception e ){
      fail( "Should have thrown a NoSuchElementException, but received" +
          " : " + e);
    }
  }

  @Test
  public void dequeMultipleIterator(){
    for(int i = 0; i < RaftDeque.MAX_SIZE; i++){

      deque = new RaftDeque();
      for(int j = 0; j < i; j++){
        deque.addLast( task(String.valueOf(j)) );
      }

      @SuppressWarnings("rawtypes")
      Iterator[] someIterators = {
          deque.iterator(),
          deque.iterator(),
          deque.iterator(),
          deque.iterator(),
          deque.iterator(),
          deque.iterator()
      };

      @SuppressWarnings("unchecked")
      Iterator<SingleThreadedRaftAlgorithm.RaftTask>[] manyStringIterators =
          (Iterator<SingleThreadedRaftAlgorithm.RaftTask>[]) someIterators;

      for(int iterID = 0; iterID < manyStringIterators.length; iterID++){
        int index = 0;
        while( manyStringIterators[iterID].hasNext() ){
          assertEquals( "Iterator #" + iterID + " failed:\n",
              String.valueOf(index),
              manyStringIterators[iterID].next().debugString);
          index++;
        }
      }

    }
  }

  @Test
  public void dequeQueueBehavior(){

    String[] aBunchOfString = {
        "One", "Two", "Three", "Four"
    };

    for(String aString : aBunchOfString){
      deque.addFirst(task(aString));
    }

    for(String aString : aBunchOfString){
      assertEquals(aString, deque.removeLast().debugString);
    }
  }

  @Test
  public void dequeStackBehavior(){

    String[] aBunchOfString = {
        "One", "Two", "Three", "Four"
    };

    for(String aString : aBunchOfString){
      deque.addFirst(task(aString));
    }

    for(int i = aBunchOfString.length - 1; i >= 0; i--){
      assertEquals(aBunchOfString[i], deque.removeFirst().debugString);
    }
  }


  // --------------------------------------------------------------------------
  // RaftDeque
  // Stack Interface Tests
  // --------------------------------------------------------------------------

  /**
   * Test that push() and pop() are Stack-like
   */
  @Test
  public void dequePushPop(){
    SingleThreadedRaftAlgorithm.RaftTask t1 = task("1", LOW);
    SingleThreadedRaftAlgorithm.RaftTask t2 = task("2", LOW);
    SingleThreadedRaftAlgorithm.RaftTask t3 = task("3", LOW);
    deque.push(t1);
    deque.push(t2);
    deque.push(t3);
    assertEquals(t3, deque.pop());
    assertEquals(t2, deque.pop());
    assertEquals(t1, deque.pop());
    assertTrue(deque.isEmpty());
  }


  /**
   * Test that push() and peek() are Stack-like
   */
  @Test
  public void dequePushPeek(){
    SingleThreadedRaftAlgorithm.RaftTask t1 = task("1", LOW);
    SingleThreadedRaftAlgorithm.RaftTask t2 = task("2", LOW);
    SingleThreadedRaftAlgorithm.RaftTask t3 = task("3", LOW);
    deque.push(t1);
    deque.push(t2);
    deque.push(t3);
    assertEquals(t3, deque.peek());
    assertEquals(3, deque.size());
  }


  // --------------------------------------------------------------------------
  // RaftDeque
  // Queue Interface Tests
  // --------------------------------------------------------------------------

  /**
   * Test that add() and poll() are Queue-like
   */
  @Test
  public void dequeAddPoll(){
    SingleThreadedRaftAlgorithm.RaftTask t1 = task("1", LOW);
    SingleThreadedRaftAlgorithm.RaftTask t2 = task("2", LOW);
    SingleThreadedRaftAlgorithm.RaftTask t3 = task("3", LOW);
    deque.add(t1);
    deque.add(t2);
    deque.add(t3);
    assertEquals(t1, deque.poll());
    assertEquals(t2, deque.poll());
    assertEquals(t3, deque.poll());
    assertTrue(deque.isEmpty());
    assertNull(deque.poll());
  }


  /**
   * Test that forceAdd() and poll() are Queue-like
   */
  @Test
  public void dequeForceAddPoll(){
    SingleThreadedRaftAlgorithm.RaftTask t1 = task("1", LOW);
    SingleThreadedRaftAlgorithm.RaftTask t2 = task("2", LOW);
    SingleThreadedRaftAlgorithm.RaftTask t3 = task("3", LOW);
    deque.forceAdd(t1);
    deque.forceAdd(t2);
    deque.forceAdd(t3);
    assertEquals(t1, deque.poll());
    assertEquals(t2, deque.poll());
    assertEquals(t3, deque.poll());
    assertTrue(deque.isEmpty());
    assertNull(deque.poll());
  }


  /**
   * Test that add() and peek() are Queue-like
   */
  @Test
  public void dequeAddPeek(){
    SingleThreadedRaftAlgorithm.RaftTask t1 = task("1", LOW);
    SingleThreadedRaftAlgorithm.RaftTask t2 = task("2", LOW);
    SingleThreadedRaftAlgorithm.RaftTask t3 = task("3", LOW);
    deque.add(t1);
    deque.add(t2);
    deque.add(t3);
    assertEquals(t1, deque.peek());
    assertEquals(3, deque.size());
  }


  /**
   * Test that add() and remove() are Queue-like
   */
  @Test
  public void dequeAddRemove(){
    SingleThreadedRaftAlgorithm.RaftTask t1 = task("1", LOW);
    SingleThreadedRaftAlgorithm.RaftTask t2 = task("2", LOW);
    SingleThreadedRaftAlgorithm.RaftTask t3 = task("3", LOW);
    deque.add(t1);
    deque.add(t2);
    deque.add(t3);
    assertEquals(t1, deque.remove());
    assertEquals(t2, deque.remove());
    assertEquals(t3, deque.remove());
    assertTrue(deque.isEmpty());
    try {
      deque.remove();
      fail("remove() should not be defined on empty queues");
    } catch (NoSuchElementException ignored) {}
  }



  /**
   * Test that offer() and poll() are Stack-like
   */
  @Test
  public void dequeOfferPoll(){
    SingleThreadedRaftAlgorithm.RaftTask t1 = task("1", LOW);
    SingleThreadedRaftAlgorithm.RaftTask t2 = task("2", LOW);
    SingleThreadedRaftAlgorithm.RaftTask t3 = task("3", LOW);
    deque.offer(t1);
    deque.offer(t2);
    deque.offer(t3);
    assertEquals(t1, deque.poll());
    assertEquals(t2, deque.poll());
    assertEquals(t3, deque.poll());
    assertTrue(deque.isEmpty());
    assertNull(deque.poll());
  }


  /**
   * Test that add() and element() are Queue-like
   */
  @Test
  public void dequeAddElement(){
    SingleThreadedRaftAlgorithm.RaftTask t1 = task("task 1", LOW);
    SingleThreadedRaftAlgorithm.RaftTask t2 = task("task 2", LOW);
    SingleThreadedRaftAlgorithm.RaftTask t3 = task("task 3", LOW);
    try {
      //noinspection ResultOfMethodCallIgnored
      deque.element();
      fail("element() should throw an exception on an empty queue");
    } catch (NoSuchElementException ignored) {}
    deque.add(t1);
    deque.add(t2);
    deque.add(t3);
    assertEquals(t1, deque.element());
    assertEquals(3, deque.size());
  }


  // --------------------------------------------------------------------------
  // RaftDeque
  // Priority Tests
  // --------------------------------------------------------------------------

  /**
   * Test that elements are popped from the deque in priority order
   */
  @Test
  public void dequePrioritize(){
    // Reverse order push
    deque.push(task("low", LOW));
    deque.push(task("high", HIGH));
    deque.push(task("crit", CRITICAL));
    assertEquals("crit", deque.pop().debugString);
    assertEquals("high", deque.pop().debugString);
    assertEquals("low", deque.pop().debugString);

    // Push in the correct order as a sanity check
    deque.push(task("crit", CRITICAL));
    deque.push(task("high", HIGH));
    deque.push(task("low", LOW));
    assertEquals("crit", deque.pop().debugString);
    assertEquals("high", deque.pop().debugString);
    assertEquals("low", deque.pop().debugString);
  }


  // --------------------------------------------------------------------------
  // RaftDeque
  // Capacity Tests
  // --------------------------------------------------------------------------

  /**
   * Test that we reject an element if there are too many already in the deque.
   */
  @Test
  public void dequeRejectOffer(){
    for (int i = 0; i < RaftDeque.MAX_SIZE; ++i) {
      assertTrue("Should be able to add " + (i + 1) + " elements to deque", deque.offer(task(String.valueOf(i))));
    }
    assertFalse("Should reject after too many elements in queue", deque.offer(task("too_many")));
  }


  /**
   * Test that we reject an element if there are too many already in the deque.
   */
  @Test
  public void dequeAcceptCritical(){
    for (int i = 0; i < RaftDeque.MAX_SIZE; ++i) {
      assertTrue("Should be able to add " + (i + 1) + " elements to deque", deque.offer(task(String.valueOf(i))));
    }
    assertTrue("Raft should have extra room for a critical priority task", deque.offer(task("crit", CRITICAL)));
    assertFalse("Should reject after too many elements in queue", deque.offer(task("crit", CRITICAL)));
  }


  /**
   * See {@link #dequeRejectOffer()}
   */
  @Test
  public void dequeRejectOfferFirst(){
    for (int i = 0; i < RaftDeque.MAX_SIZE; ++i) {
      assertTrue("Should be able to add " + (i + 1) + " elements to deque", deque.offer(task(String.valueOf(i))));
    }
    assertFalse("Should reject after too many elements in queue", deque.offerFirst(task("too_many")));
  }


  /**
   * See {@link #dequeRejectOffer()}
   */
  @Test
  public void dequeRejectOfferLast(){
    for (int i = 0; i < RaftDeque.MAX_SIZE; ++i) {
      assertTrue("Should be able to add " + (i + 1) + " elements to deque", deque.offer(task(String.valueOf(i))));
    }
    assertFalse("Should reject after too many elements in queue", deque.offerLast(task("too_many")));
  }


  /**
   * Test that we reject an element if there are too many already in the deque.
   */
  private void dequeBlockWriteGeneralized(BiConsumer<RaftDeque, SingleThreadedRaftAlgorithm.RaftTask> push, Consumer<RaftDeque> pop){
    RaftDeque deque = new RaftDeque();  // create our own deque, since we call this often
    for (int i = 0; i < RaftDeque.MAX_SIZE; ++i) {
      assertTrue("Should be able to add " + (i + 1) + " elements to deque", deque.offer(task(String.valueOf(i))));
    }
    AtomicBoolean havePopped = new AtomicBoolean(false);
    Thread consumer = new Thread(() -> {
      for (int i = 0; i < 100; ++i) {
        Thread.yield();
      }
      havePopped.set(true);
      pop.accept(deque);
    });
    consumer.start();
    assertFalse(havePopped.get());
    push.accept(deque, task("should_wait"));
    assertTrue(havePopped.get());
  }


  /**
   * @see #dequeBlockWriteGeneralized(BiConsumer, Consumer)
   */
  @Test
  public void dequeBlockWrite(){
    dequeBlockWriteGeneralized(RaftDeque::push, RaftDeque::pop);
  }


  /**
   * @see #dequeBlockWriteGeneralized(BiConsumer, Consumer)
   */
  @Test
  public void dequeBlockWriteCrossProduct(){
    for (BiConsumer<RaftDeque, SingleThreadedRaftAlgorithm.RaftTask> push : Arrays.asList((BiConsumer<RaftDeque, SingleThreadedRaftAlgorithm.RaftTask>)
        RaftDeque::push, RaftDeque::add, RaftDeque::addFirst, RaftDeque::addLast
    )) {
      for (Consumer<RaftDeque> pop : Arrays.asList((Consumer<RaftDeque>)
          RaftDeque::pop,
          RaftDeque::remove, RaftDeque::removeFirst, RaftDeque::removeLast,
          RaftDeque::clear,
          RaftDeque::poll, RaftDeque::pollFirst, RaftDeque::pollLast
      )) {
        dequeBlockWriteGeneralized(push, pop);
      }
    }
  }

}