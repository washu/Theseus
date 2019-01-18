package ai.eloquent.util;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Test {@link LeakyBucket}.
 *
 * @author <a href="mailto:gabor@eloquent.ai">Gabor Angeli</a>
 */
public class LeakyBucketTest {


  /**
   * Test that the constructor sets fields correctly.
   */
  @Test
  public void testConstructor() {
    LeakyBucket bucket = new LeakyBucket(5, 1000000L);
    assertEquals(5, bucket.bucketCapacity);
    assertEquals(1000000L, bucket.nanosBetweenLeaks);
  }


  /**
   * Test the {@link LeakyBucket#submit()} function
   */
  @Test
  public void submit() {
    LeakyBucket bucket = new LeakyBucket(3, 1000000L);
    // Fill up the bucket
    assertTrue(bucket.submit(0L));
    assertTrue(bucket.submit(0L));
    assertTrue(bucket.submit(0L));
    // Try to submit
    assertFalse("Should not be able to submit at time 0", bucket.submit(0L));
    assertFalse("Still waiting for 1 more nanosecond", bucket.submit(999999L));
    // Should be able to add 1 element @ time 1M
    assertTrue("Should be able to submit again", bucket.submit(1000000L));
    assertFalse("Should not be able to submit two things at time 1", bucket.submit(1000000L));
    // Should be able to submit 3 elements @ time 4M
    assertTrue("Everything cleared -- should be able to submit elem 1", bucket.submit(4000000L));
    assertTrue("Everything cleared -- should be able to submit elem 2", bucket.submit(4000000L));
    assertTrue("Everything cleared -- should be able to submit elem 3", bucket.submit(4000000L));
  }


  /**
   * Test that we work even if there was a big delay before the first submit.
   */
  @Test
  public void submitCornerCaseJustStarted() {
    LeakyBucket bucket = new LeakyBucket(1, 5L);
    assertTrue(bucket.submit(100L));
    assertFalse(bucket.submit(104L));
    assertTrue(bucket.submit(105L));
  }


  /**
   * Test the {@link LeakyBucket#forceSubmit()} ()} function
   */
  @Test
  public void forceSubmit() {
    LeakyBucket bucket = new LeakyBucket(1, 5L);
    assertTrue(bucket.submit(0L));
    bucket.forceSubmit();
    bucket.forceSubmit();
    assertFalse(bucket.submit(0L));
    assertFalse(bucket.submit(5L));
    assertFalse(bucket.submit(10L));
    assertFalse(bucket.submit(14L));
    assertTrue("Should be able to submit after forced entry flushes", bucket.submit(15L));
  }


  /**
   * Test the {@link LeakyBucket#isFull()} function
   */
  @Test
  public void isFull() {
    LeakyBucket bucket = new LeakyBucket(1, 1000000L);
    assertFalse("Bucket should not start full", bucket.isFull(0L));
    assertTrue(bucket.submit(0L));
    assertTrue("Bucket with capacity 1 should be full after one submit", bucket.isFull(0L));
    assertFalse("Bucket should no longer be full when request exits", bucket.isFull(1000000L));
  }

  /**
   * Test the {@link LeakyBucket#isFull()} function with force submits
   */
  @Test
  public void isFullWithForcedSubmissions() {
    LeakyBucket bucket = new LeakyBucket(1, 5L);
    assertFalse("Bucket should not start full", bucket.isFull(0L));
    bucket.forceSubmit();
    bucket.forceSubmit();
    bucket.forceSubmit();
    assertTrue("Bucket should be full", bucket.isFull(5L));
    assertTrue("Bucket should be full", bucket.isFull(14L));
    assertFalse("Bucket should not be full after everything flushes", bucket.isFull(15L));
  }


  /**
   * Test the {@link LeakyBucket#submitBlocking()} function
   */
  @Test
  public void submitBlocking() throws InterruptedException {
    LeakyBucket bucket = new LeakyBucket(1, 5000000L);
    // Fill up the bucket
    long nanosBefore = System.nanoTime();  // a conservative estimate
    assertTrue(bucket.submit());
    // Submit
    bucket.submitBlocking();
    long nanosEnd = System.nanoTime();
    assertTrue("Ensure that we waited at least 1ms before submitting. We waited only " + (nanosEnd - nanosBefore),
        nanosEnd - nanosBefore >= 5000000L);

  }
}