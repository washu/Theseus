package ai.eloquent.monitoring;

import ai.eloquent.util.Uninterruptably;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

@SuppressWarnings("Duplicates")
public class PrometheusTest {

  /**
   * Clear all of the existing metrics in Prometheus
   */
  @Before
  public void resetMetrics() {
    Prometheus.resetMockMetrics();
  }


  /**
   * Test that we don't have prometheus in our classpath
   */
  @Test
  public void dontHavePrometheus() {
    assertFalse(Prometheus.havePrometheus());
  }


  /**
   * Test that we can make a summary at all
   */
  @Test
  public void summaryBuildCrashTest() {
    Object summary = Prometheus.summaryBuild("name", "help", "label");
    assertNotNull(summary);
  }


  /**
   * Test that the summary's equals() and hashCode() are stable.
   */
  @Test
  public void summaryBuildEqualsHashCode() {
    // The objects
    Object summary = Prometheus.summaryBuild("name", "help", "label");
    Object same = Prometheus.summaryBuild("name", "help", "label");
    Object differentName = Prometheus.summaryBuild("name2", "help", "label");
    Object differentLabels = Prometheus.summaryBuild("name", "help", "label2");
    Object differentHelp = Prometheus.summaryBuild("name", "help2", "label");
    Object noLabels = Prometheus.summaryBuild("name", "help2");
    // The tests
    assertEquals(summary, same);
    assertEquals(summary.hashCode(), same.hashCode());
    assertNotEquals(summary, differentName);
    assertNotEquals(summary, differentLabels);
    assertEquals(summary, differentHelp);
    assertEquals(summary.hashCode(), differentHelp.hashCode());
    assertNotEquals(summary, noLabels);
  }


  /**
   * Test that we can start a timer and not crash
   */
  @Test
  public void startTimerCrashTest() {
    Object summary = Prometheus.summaryBuild("name", "help", "label");
    assertNotNull(Prometheus.startTimer(summary, "label_value"));
    Object noLabels = Prometheus.summaryBuild("name", "help");
    assertNotNull(Prometheus.startTimer(noLabels));
  }


  /**
   * Test that we can start a timer with null input
   */
  @Test
  public void startTimerNullInput() {
    assertNotNull(Prometheus.startTimer(null));
  }


  /**
   * Test that the labels to a timer must match the labels to the summary.
   */
  @Test
  public void startTimerLabelLengthsMustMatch() {
    Object summary = Prometheus.summaryBuild("name", "help", "label1", "label2");
    try {
      Prometheus.startTimer(summary, "label1_value");
      fail();
    } catch (IllegalArgumentException ignored) {}
    try {
      Prometheus.startTimer(summary, "label1_value", "label2_value", "label3_value");
      fail();
    } catch (IllegalArgumentException ignored) {}
  }


  /**
   * Test that we can start a timer and not crash
   */
  @Test
  public void startTimerEqualsHashCode() {
    // The objects
    Object summary = Prometheus.summaryBuild("name", "help", "label");
    Object timer = Prometheus.startTimer(summary, "label_value");
    Object same = Prometheus.startTimer(summary, "label_value");
    Object other = Prometheus.summaryBuild("name2", "help", "label");
    Object differentSummary = Prometheus.startTimer(other, "label_value");
    Object differentLabel = Prometheus.startTimer(other, "label_value2");
    // The tests
    assertEquals(timer, same);
    assertEquals(timer.hashCode(), same.hashCode());
    assertNotEquals(timer, differentSummary);
    assertNotEquals(timer, differentLabel);
  }


  /**
   * Check that we can observe a duration
   */
  @Test
  public void observeDurationCrashTest() {
    Object summary = Prometheus.summaryBuild("name", "help", "label");
    Object timer = Prometheus.startTimer(summary, "label_value");
    Uninterruptably.sleep(1);
    double duration = Prometheus.observeDuration(timer);
    assertTrue(duration > 0);
  }


  /**
   * Check that we can observe a duration with null input
   */
  @Test
  public void observeDurationNullInput() {
    Object timer = Prometheus.startTimer(null);
    assertEquals(0.0, Prometheus.observeDuration(null), 1e-10);
    Uninterruptably.sleep(1);
    assertTrue(Prometheus.observeDuration(timer) > 0);
  }


  /**
   * Test that we can make a gauge at all
   */
  @Test
  public void gaugeBuildCrashTest() {
    Object gauge = Prometheus.gaugeBuild("name", "help");
    assertNotNull(gauge);
  }


  /**
   * Test that the gauge's equals() and hashCode() are stable.
   */
  @Test
  public void gaugeBuildEqualsHashCode() {
    // The objects
    Object gauge = Prometheus.gaugeBuild("name", "help");
    Object same = Prometheus.gaugeBuild("name", "help");
    Object differentName = Prometheus.gaugeBuild("name2", "help");
    Object differentHelp = Prometheus.gaugeBuild("name", "help2");
    // The tests
    assertEquals(gauge, same);
    assertEquals(gauge.hashCode(), same.hashCode());
    assertNotEquals(gauge, differentName);
    assertEquals(gauge, differentHelp);
    assertEquals(gauge.hashCode(), differentHelp.hashCode());
  }


  /**
   * Check that we can get an empty gauge value
   */
  @Test
  public void gaugeGetCrashTest() {
    Object gauge = Prometheus.gaugeBuild("name", "help");
    assertEquals(0.0, Prometheus.gaugeGet(gauge), 1e-10);
    assertEquals(0.0, Prometheus.gaugeGet(null), 1e-10);
  }


  /**
   * Check that we set a gauge value
   */
  @Test
  public void gaugeSetCrashTest() {
    Object gauge = Prometheus.gaugeBuild("name", "help");
    Prometheus.gaugeSet(gauge, 10.0);
    Prometheus.gaugeSet(null, 10.0);
  }


  /**
   * Check that we set a gauge value and then get it again
   */
  @Test
  public void gaugeSetGet() {
    Object gauge = Prometheus.gaugeBuild("name", "help");
    Prometheus.gaugeSet(gauge, 10.0);
    assertEquals(10.0, Prometheus.gaugeGet(gauge), 1e-10);
  }


  /**
   * Check that we increment a gauge value
   */
  @Test
  public void gaugeIncrementCrashTest() {
    Object gauge = Prometheus.gaugeBuild("name", "help");
    Prometheus.gaugeInc(gauge);
    Prometheus.gaugeInc(null);
  }


  /**
   * Check that we increment a gauge value and then get it again
   */
  @Test
  public void gaugeIncrementGet() {
    Object gauge = Prometheus.gaugeBuild("name", "help");
    Prometheus.gaugeInc(gauge);
    assertEquals(1.0, Prometheus.gaugeGet(gauge), 1e-10);
    Prometheus.gaugeInc(gauge);
    assertEquals(2.0, Prometheus.gaugeGet(gauge), 1e-10);
  }


  /**
   * Check that we decrement a gauge value
   */
  @Test
  public void gaugDecrementCrashTest() {
    Object gauge = Prometheus.gaugeBuild("name", "help");
    Prometheus.gaugeDec(gauge);
    Prometheus.gaugeDec(null);
  }


  /**
   * Check that we decrement a gauge value and then get it again
   */
  @Test
  public void gaugeDecrementGet() {
    Object gauge = Prometheus.gaugeBuild("name", "help");
    Prometheus.gaugeDec(gauge);
    assertEquals(-1.0, Prometheus.gaugeGet(gauge), 1e-10);
    Prometheus.gaugeDec(gauge);
    assertEquals(-2.0, Prometheus.gaugeGet(gauge), 1e-10);
  }


  /**
   * Test that we can make a counter at all
   */
  @Test
  public void counterBuildCrashTest() {
    Object counter = Prometheus.counterBuild("name", "help");
    assertNotNull(counter);
  }


  /**
   * Test that the counter's equals() and hashCode() are stable.
   */
  @Test
  public void counterBuildEqualsHashCode() {
    // The objects
    Object counter = Prometheus.counterBuild("name", "help");
    Object same = Prometheus.counterBuild("name", "help");
    Object differentName = Prometheus.counterBuild("name2", "help");
    Object differentHelp = Prometheus.counterBuild("name", "help2");
    // The tests
    assertEquals(counter, same);
    assertEquals(counter.hashCode(), same.hashCode());
    assertNotEquals(counter, differentName);
    assertEquals(counter, differentHelp);
    assertEquals(counter.hashCode(), differentHelp.hashCode());
  }


  /**
   * Check that we increment a counter value.
   * Since we have no need to ever get the value, this is the only
   * real test of counter incrementing we need.
   */
  @Test
  public void counterIncrementCrashTest() {
    Object counter = Prometheus.counterBuild("name", "help");
    Prometheus.counterInc(counter);
    Prometheus.counterInc(null);
  }
}