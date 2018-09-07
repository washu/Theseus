package ai.eloquent.monitoring;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class Prometheus {

  /**
   * An SLF4J Logger for this class.
   */
  private static final Logger log = LoggerFactory.getLogger(Prometheus.class);


  /**
   * This is a static utility class
   */
  private Prometheus() {
  }


  @Nullable
  private static Method SUMMARY_METHOD_BUILD;

  @Nullable
  private static Method SUMMARY_METHOD_LABELS;

  @Nullable
  private static Method SUMMARY_METHOD_STARTTIMER;

  @Nullable
  private static Method SUMMARYBUILDER_METHOD_LABELNAMES;

  @Nullable
  private static Method SUMMARYBUILDER_METHOD_REGISTER;

  @Nullable
  private static Method TIMER_METHOD_OBSERVE_DURATION;

  @Nullable
  private static Method GAUGE_METHOD_BUILD;

  @Nullable
  private static Method GAUGE_METHOD_GET;

  @Nullable
  private static Method GAUGE_METHOD_SET;

  @Nullable
  private static Method GAUGE_METHOD_INC;

  @Nullable
  private static Method GAUGE_METHOD_DEC;

  @Nullable
  private static Method GAUGEBUILDER_METHOD_REGISTER;

  @Nullable
  private static Method COUNTER_METHOD_BUILD;

  @Nullable
  private static Method COUNTER_METHOD_INC;

  @Nullable
  private static Method COUNTERBUILDER_METHOD_REGISTER;

  static {
    try {
      Class<?> SummaryClass = Class.forName("io.prometheus.client.Summary");
      Class<?> SummaryBuilderClass = Class.forName("io.prometheus.client.Summary$Builder");
      Class<?> SummaryTimerClass = Class.forName("io.prometheus.client.Summary$Timer");
      Class<?> GaugeClass = Class.forName("io.prometheus.client.Gauge");
      Class<?> GaugeBuilderClass = Class.forName("io.prometheus.client.Gauge$Builder");
      Class<?> CounterClass = Class.forName("io.prometheus.client.Gauge");
      Class<?> CounterBuilderClass = Class.forName("io.prometheus.client.Gauge$Builder");
      SUMMARY_METHOD_BUILD = SummaryClass.getMethod("build", String.class, String.class);
      SUMMARY_METHOD_LABELS = SummaryClass.getMethod("labels", String.class);
      SUMMARY_METHOD_STARTTIMER = SummaryClass.getMethod("startTimer");
      SUMMARYBUILDER_METHOD_LABELNAMES = SummaryBuilderClass.getMethod("labelNames", String.class);
      SUMMARYBUILDER_METHOD_REGISTER = SummaryBuilderClass.getMethod("register");
      TIMER_METHOD_OBSERVE_DURATION = SummaryTimerClass.getMethod("observeDuration");
      GAUGE_METHOD_BUILD = GaugeClass.getMethod("build", String.class, String.class);
      GAUGE_METHOD_GET = GaugeClass.getMethod("get");
      GAUGE_METHOD_SET = GaugeClass.getMethod("set", Double.class);
      GAUGE_METHOD_INC = GaugeClass.getMethod("inc");
      GAUGE_METHOD_DEC = GaugeClass.getMethod("dec");
      GAUGEBUILDER_METHOD_REGISTER = GaugeBuilderClass.getMethod("register");
      COUNTER_METHOD_INC = CounterClass.getMethod("inc");
      COUNTER_METHOD_BUILD = CounterClass.getMethod("build", String.class, String.class);
      COUNTERBUILDER_METHOD_REGISTER = CounterBuilderClass.getMethod("register");
    } catch (ClassNotFoundException e) {
      log.info("Could not find Prometheus in your classpath -- not logging statistics");
    } catch (NoSuchMethodException e) {
      log.info("Prometheus methods are not as expected (version mismatch?) -- not logging statistics");
    }
  }


  /**
   * Overloaded method
   * Builds a new Prometheus Summary, if possible.
   *
   * @param name The name of the metric
   * @param help The help string of the metric
   * @param label A label to attach to the metric
   *
   * @return The Summary object
   */
  public static Object summaryBuild(String name, String help, String label) {
    if (SUMMARY_METHOD_BUILD == null || SUMMARYBUILDER_METHOD_LABELNAMES == null || SUMMARYBUILDER_METHOD_REGISTER == null) {
      return null;
    }
    try {
      Object builder = SUMMARY_METHOD_BUILD.invoke(null, name, help);
      builder = SUMMARYBUILDER_METHOD_LABELNAMES.invoke(builder, label);
      return SUMMARYBUILDER_METHOD_REGISTER.invoke(builder);
    } catch (IllegalAccessException | InvocationTargetException e) {
      log.info("Prometheus methods could not be invoked (version mismatch?) -- not logging statistics");
      return null;
    }
  }


  /**
   * Overloaded method
   * Builds a new Prometheus Summary, if possible.
   *
   * @param name The name of the metric
   * @param help The help string of the metric
   *
   * @return The Summary object
   */
  public static Object summaryBuild(String name, String help) {
    if (SUMMARY_METHOD_BUILD == null || SUMMARYBUILDER_METHOD_REGISTER == null) {
      return null;
    }
    try {
      Object builder = SUMMARY_METHOD_BUILD.invoke(null, name, help);
      return SUMMARYBUILDER_METHOD_REGISTER.invoke(builder);
    } catch (IllegalAccessException | InvocationTargetException e) {
      log.info("Prometheus methods could not be invoked (version mismatch?) -- not logging statistics");
      return null;
    }
  }


  /**
   * Labels a Summary metric, then start its timer to track a duration
   * Call {@link Prometheus#observeDuration(Object)} at the end of what you want to measure the duration of.
   *
   * @param summary The Summary metric to start the timer on
   * @param label The label to attach to the Summary metric
   * @return the Prometheus Timer object
   */
  public static Object labelAndStartTimer(Object summary, String label) {
    if (summary == null || SUMMARY_METHOD_LABELS == null || SUMMARY_METHOD_STARTTIMER == null) {
      return null;
    }
    try {
      summary = SUMMARY_METHOD_LABELS.invoke(summary, label);
      return SUMMARY_METHOD_STARTTIMER.invoke(summary);
    } catch (IllegalAccessException | InvocationTargetException e) {
      log.info("Prometheus methods could not be invoked (version mismatch?) -- not logging statistics");
      return null;
    }
  }


  /**
   * Start a timer on a summary metric to track a duration
   * Call {@link Prometheus#observeDuration(Object)} at the end of what you want to measure the duration of.
   *
   * @param summary The Summary metric to start the timer on
   * @return the Prometheus Timer object
   */
  public static Object startTimer(Object summary) {
    if (summary == null || SUMMARY_METHOD_STARTTIMER == null) {
      return null;
    }
    try {
      return SUMMARY_METHOD_STARTTIMER.invoke(summary);
    } catch (IllegalAccessException | InvocationTargetException e) {
      log.info("Prometheus methods could not be invoked (version mismatch?) -- not logging statistics");
      return null;
    }
  }


  /**
   * Observe duration on a Prometheus timer.
   * @param timer The prometheus timer.
   */
  public static Double observeDuration(Object timer) {
    if (timer == null || TIMER_METHOD_OBSERVE_DURATION == null) {
      return null;
    }
    try {
      return (Double) TIMER_METHOD_OBSERVE_DURATION.invoke(timer);
    } catch (IllegalAccessException | InvocationTargetException e) {
      log.info("Prometheus methods could not be invoked (version mismatch?) -- not logging statistics");
      return null;
    }
  }


  /**
   * Builds a new Prometheus Gauge metric, if possible.
   * @param name The name of the metric
   * @param help The help string of the metric
   *
   * @return
   */
  public static Object gaugeBuild(String name, String help) {
    if (GAUGE_METHOD_BUILD == null || GAUGEBUILDER_METHOD_REGISTER == null) {
      return null;
    }
    try {
      Object builder = GAUGE_METHOD_BUILD.invoke(null, name, help);
      return GAUGEBUILDER_METHOD_REGISTER.invoke(builder);
    } catch (IllegalAccessException | InvocationTargetException e) {
      log.info("Prometheus methods could not be invoked (version mismatch?) -- not logging statistics");
      return null;
    }
  }


  /**
   * Gets the current value stored in the Prometheus Gauge, if possible.
   *
   * @param gauge The instance of the gauge
   * @return The value of the Gauge as a Double
   */
  public static Double gaugeGet(Object gauge) {
    if (gauge == null || GAUGE_METHOD_GET == null) {
      return null;
    }
    try {
      return (Double) GAUGE_METHOD_GET.invoke(gauge);
    } catch (IllegalAccessException | InvocationTargetException e) {
      log.info("Prometheus methods could not be invoked (version mismatch?) -- not logging statistics");
      return null;
    }
  }


  /**
   * Sets the Prometheus Gauge to a given value, if possible.
   */
  public static void gaugeSet(Object gauge, Integer val) {
    if (gauge == null || val == null || GAUGE_METHOD_SET == null) {
      return;
    }
    try {
      GAUGE_METHOD_SET.invoke(gauge, val);
    } catch (IllegalAccessException | InvocationTargetException e) {
      log.info("Prometheus methods could not be invoked (version mismatch?) -- not logging statistics");
      return;
    }
  }

  /**
   * Increments the value in the Prometheus Gauge by 1, if possible.
   */
  public static void gaugeInc(Object gauge) {
    if (gauge == null || GAUGE_METHOD_INC == null) {
      return;
    }
    try {
      GAUGE_METHOD_INC.invoke(gauge);
    } catch (IllegalAccessException | InvocationTargetException e) {
      log.info("Prometheus methods could not be invoked (version mismatch?) -- not logging statistics");
      return;
    }
  }

  /**
   * Decrements the value in the Prometheus Gauge by 1, if possible.
   */
  public static void gaugeDec(Object gauge) {
    if (gauge == null || GAUGE_METHOD_DEC == null) {
      return;
    }
    try {
      GAUGE_METHOD_DEC.invoke(gauge);
    } catch (IllegalAccessException | InvocationTargetException e) {
      log.info("Prometheus methods could not be invoked (version mismatch?) -- not logging statistics");
      return;
    }
  }



  /**
   * Builds a new Prometheus Counter metric, if possible.
   * @param name The name of the metric
   * @param help The help string of the metric
   *
   * @return
   */
  public static Object counterBuild(String name, String help) {
    if (COUNTER_METHOD_BUILD == null || COUNTERBUILDER_METHOD_REGISTER == null) {
      return null;
    }
    try {
      Object builder = COUNTER_METHOD_BUILD.invoke(null, name, help);
      return COUNTERBUILDER_METHOD_REGISTER.invoke(builder);
    } catch (IllegalAccessException | InvocationTargetException e) {
      log.info("Prometheus methods could not be invoked (version mismatch?) -- not logging statistics");
      return null;
    }
  }

  /**
   * Increments the value in the Prometheus Gauge by 1, if possible.
   */
  public static void counterInc(Object gauge) {
    if (gauge == null || COUNTER_METHOD_INC == null) {
      return;
    }
    try {
      COUNTER_METHOD_INC.invoke(gauge);
    } catch (IllegalAccessException | InvocationTargetException e) {
      log.info("Prometheus methods could not be invoked (version mismatch?) -- not logging statistics");
      return;
    }
  }

}