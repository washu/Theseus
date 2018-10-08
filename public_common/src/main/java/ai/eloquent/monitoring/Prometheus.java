package ai.eloquent.monitoring;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Objects;

/**
 * A facade for handling calls to Prometheus
 * When Prometheus is not found in the classpath, or a different version is found, we revert to a mocked version of Prometheus
 *
 * We enforce that 2 metrics with the same name refer to the same metric.
 * Author: zameschua
 */
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

  /** A flag to indicate if we are using prometheus or mocking it */
  private static boolean havePrometheus = false;

  /** Mocked gauges, in case we don't have Prometheus in our classpath */
  private static final HashMap<String, Object> summaries = new HashMap<>();

  /** Mocked gauges, in case we don't have Prometheus in our classpath */
  private static final HashMap<String, Object> gauges = new HashMap<>();

  /** Mocked counters, in case we don't have Prometheus in our classpath */
  private static final HashMap<String, Object> counters = new HashMap<>();

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
      Class<?> summaryChildClass = Class.forName("io.prometheus.client.Summary$Child");
      Class<?> SummaryBuilderClass = Class.forName("io.prometheus.client.Summary$Builder");
      Class<?> SummaryTimerClass = Class.forName("io.prometheus.client.Summary$Timer");
      Class<?> GaugeClass = Class.forName("io.prometheus.client.Gauge");
      Class<?> GaugeBuilderClass = Class.forName("io.prometheus.client.Gauge$Builder");
      Class<?> CounterClass = Class.forName("io.prometheus.client.Gauge");
      Class<?> CounterBuilderClass = Class.forName("io.prometheus.client.Gauge$Builder");
      SUMMARY_METHOD_BUILD = SummaryClass.getMethod("build", String.class, String.class);
      SUMMARY_METHOD_LABELS = SummaryClass.getMethod("labels", String[].class);
      SUMMARY_METHOD_STARTTIMER = summaryChildClass.getMethod("startTimer");
      SUMMARYBUILDER_METHOD_LABELNAMES = SummaryBuilderClass.getMethod("labelNames", String[].class);
      SUMMARYBUILDER_METHOD_REGISTER = SummaryBuilderClass.getMethod("register");
      TIMER_METHOD_OBSERVE_DURATION = SummaryTimerClass.getMethod("observeDuration");
      GAUGE_METHOD_BUILD = GaugeClass.getMethod("build", String.class, String.class);
      GAUGE_METHOD_GET = GaugeClass.getMethod("get");
      GAUGE_METHOD_SET = GaugeClass.getMethod("set", double.class);
      GAUGE_METHOD_INC = GaugeClass.getMethod("inc");
      GAUGE_METHOD_DEC = GaugeClass.getMethod("dec");
      GAUGEBUILDER_METHOD_REGISTER = GaugeBuilderClass.getMethod("register");
      COUNTER_METHOD_INC = CounterClass.getMethod("inc");
      COUNTER_METHOD_BUILD = CounterClass.getMethod("build", String.class, String.class);
      COUNTERBUILDER_METHOD_REGISTER = CounterBuilderClass.getMethod("register");
      havePrometheus = true;
    } catch (ClassNotFoundException e) {
      log.info("Could not find Prometheus in your classpath -- not logging statistics", e);
    } catch (NoSuchMethodException e) {
      log.warn("Prometheus methods are not as expected (version mismatch?) -- not logging statistics", e);
      e.printStackTrace();
      SUMMARY_METHOD_BUILD = null;
      SUMMARY_METHOD_STARTTIMER = null;
      SUMMARYBUILDER_METHOD_LABELNAMES = null;
      SUMMARYBUILDER_METHOD_REGISTER = null;
      TIMER_METHOD_OBSERVE_DURATION = null;
      SUMMARY_METHOD_LABELS = null;
      GAUGE_METHOD_BUILD = null;
      GAUGE_METHOD_GET = null;
      GAUGE_METHOD_SET = null;
      GAUGE_METHOD_INC = null;
      GAUGE_METHOD_DEC = null;
      GAUGEBUILDER_METHOD_REGISTER = null;
      COUNTER_METHOD_INC = null;
      COUNTER_METHOD_BUILD = null;
      COUNTERBUILDER_METHOD_REGISTER = null;
    }
  }


  /**
   * Clear all of our mock metrics
   */
  static void resetMetrics() {
    summaries.clear();
    gauges.clear();
    counters.clear();
  }

  /**
   * Builds and returns a new Prometheus Summary object, if possible.
   *
   * @param name The name of the metric
   * @param help The help string of the metric
   * @param labels The label to attach to the metric
   *
   * @return The Summary object
   */
  public static Object summaryBuild(String name, String help, String... labels) {
    Object result;
    if (havePrometheus && name != null) {
      try {

        // 1a: Using Prometheus and summary does not exist yet
        if (!summaries.containsKey(name)) {
          Object builder = SUMMARY_METHOD_BUILD.invoke(null, name, help);
          builder = SUMMARYBUILDER_METHOD_LABELNAMES.invoke(builder, new Object[]{labels});
          result = SUMMARYBUILDER_METHOD_REGISTER.invoke(builder);
          summaries.put(name, result);

        // 1b: Using Prometheus and summary already exists
        } else {
          result = summaries.get(name);
        }

      } catch (InvocationTargetException e) {
        if (e.getCause() != null && e.getCause() instanceof RuntimeException) {
          throw (RuntimeException) e.getCause();
        } else {
          log.warn("Invocation target exception", e);
          return new SummaryMock(name, labels);
        }
      } catch (IllegalAccessException e) {
        log.warn("Prometheus methods could not be invoked (version mismatch?) -- not logging statistics", e);
        result = new SummaryMock(name, labels);
      }
    } else {
      // 2a. Guard against null name
      if (name == null) {
        log.warn("Trying to build a Summary with null name");
        result = new SummaryMock(name, labels);
      // 2b. If we are mocking Prometheus and the Summary does not exist yet
      } else if (!summaries.containsKey(name) && name != null) {
        result = new SummaryMock(name, labels);
        summaries.put(name, result);

      // 2c: If we are mocking Prometheus and the Summary already exists
      } else {
        log.warn("Trying to build a Summary with duplicate names: ", name);
        result = summaries.get(name);
      }
    }
    return result;
  }


  /**
   * Start a timer on a summary metric to track a duration
   * Call {@link Prometheus#observeDuration(Object)} at the end of what you want to measure the duration of.
   *
   * @param summary The Summary metric to start the timer on
   * @param labels The labels to attach
   * @return the Prometheus Timer object
   */
  public static Object startTimer(@Nullable Object summary, String... labels) {
    Object timerMetric;
    if (havePrometheus && summary != null) {
      try {
        Object timer = null;
        Object withLabels = SUMMARY_METHOD_LABELS.invoke(summary, new Object[]{ labels });
        if (withLabels != null) {
          timer = SUMMARY_METHOD_STARTTIMER.invoke(withLabels);
        }
        return timer;
      } catch (InvocationTargetException e) {
        if (e.getCause() != null && e.getCause() instanceof RuntimeException) {
          throw (RuntimeException) e.getCause();
        } else {
          log.warn("Invocation target exception", e);
          timerMetric = new TimerMock(null, labels);
        }
      } catch (IllegalArgumentException | IllegalAccessException e) {
        log.warn("Prometheus methods could not be invoked (version mismatch?) -- not logging statistics", e);
        e.printStackTrace();
        //noinspection ConstantConditions
        timerMetric = new TimerMock(null, labels);
      }
    } else {
      // If we are mocking Prometheus
      if (summary != null && !(summary instanceof SummaryMock)) {
        log.error("Starting a timer on something other than a SummaryMock: {}", summary.getClass());
        //noinspection ConstantConditions
        timerMetric = new TimerMock(null, labels);
      // If we are mocking Prometheus and calling startTimer on something other than a SummaryMock
      } else {
        //noinspection ConstantConditions
        timerMetric = new TimerMock((SummaryMock) summary, labels);
      }
    }
    return timerMetric;
  }


  /**
   * Observe duration on a Prometheus timer.
   * @param timer The prometheus timer.
   */
  public static double observeDuration(@Nullable Object timer) {
    if (havePrometheus && timer != null) {
      try {
        Double rtn = (Double) TIMER_METHOD_OBSERVE_DURATION.invoke(timer);
        if (rtn == null) {
          return 0.;
        } else {
          return rtn;
        }
      } catch (InvocationTargetException e) {
        if (e.getCause() != null && e.getCause() instanceof RuntimeException) {
          throw (RuntimeException) e.getCause();
        } else {
          log.warn("Invocation target exception", e);
          return 0.;
        }
      } catch (IllegalAccessException e) {
        log.warn("Prometheus methods could not be invoked (version mismatch?) -- not logging statistics", e);
        return 0.;
      } catch (IllegalArgumentException e) {
        log.warn("Invalid parameter supplied: {}, should be of Prometheus.Summary.Timer or TimerMock class", timer.getClass());
        return 0.;
      }
    // If we are mocking Prometheus
    } else if (timer instanceof TimerMock) {
      return ((TimerMock)timer).observeDuration();
    // Fallback case where we are not sure what went wrong
    } else {
      log.warn("Something probably went wrong in Prometheus#observeDuration: Expected Timer or TimerMock but received {}", timer == null ? "null" : timer.getClass());
      return 0.;
    }
  }


  /**
   * Builds a new Prometheus Gauge metric, if possible.
   *
   * @param name The name of the metric
   * @param help The help string of the metric
   *
   * @return The Gauge object, or a mock if necessary.
   */
  public static Object gaugeBuild(String name, String help) {
    Object result;
    if (havePrometheus) {
      try {
        // 1a. Using Prometheus and we already have a Gauge with this name yet
        if (gauges.containsKey(name)) {
          result = gauges.get(name);
        // 1b. Using Prometheus and don't have a Gauge with this name yet
        } else {
          Object builder = GAUGE_METHOD_BUILD.invoke(null, name, help);
          result = GAUGEBUILDER_METHOD_REGISTER.invoke(builder);
        }
      } catch (InvocationTargetException e) {
        if (e.getCause() != null && e.getCause() instanceof RuntimeException) {
          throw (RuntimeException) e.getCause();
        } else {
          log.warn("Invocation target exception", e);
          result = new GaugeMock(name);
        }
      } catch (IllegalArgumentException | IllegalAccessException e) {
        log.warn("Prometheus methods could not be invoked (version mismatch?) -- not logging statistics", e);
        result = new GaugeMock(name);
      }
    } else {
      // 2a. We are mocking Prometheus and we already have a Gauge with this name
      if (gauges.containsKey(name)) {
        result = gauges.get(name);
      // 2b. We are mocking Prometheus and we don't have a Guage with this name yet
      } else {
        result = new GaugeMock(name);
      }
    }
    gauges.put(name, result);
    return result;
  }


  /**
   * Gets the current value stored in the Prometheus Gauge, if possible.
   *
   * @param gauge The instance of the gauge
   *
   * @return The value of the Gauge as a Double
   */
  public static double gaugeGet(Object gauge) {
    if (havePrometheus && gauge != null) {
      try {
        Double rtn = (Double) GAUGE_METHOD_GET.invoke(gauge);
        if (rtn == null) {
          return 0.0;
        } else {
          return rtn;
        }
      } catch (InvocationTargetException e) {
        if (e.getCause() != null && e.getCause() instanceof RuntimeException) {
          throw (RuntimeException) e.getCause();
        } else {
          log.warn("Invocation target exception", e);
          return 0.;
        }
      } catch (IllegalArgumentException | IllegalAccessException e) {
        log.warn("Prometheus methods could not be invoked (version mismatch?) -- not logging statistics", e);
        return 0.;
      }
    // If we are mocking Prometheus
    } else if (gauge instanceof GaugeMock) {
      return ((GaugeMock) gauge).get();
    } else {
      log.warn("Something probably went wrong in Prometheus#gaugeGet: Expected Gauge or GaugeMock but received {}", gauge == null ? "null" : gauge.getClass());
      return 0.;
    }
  }


  /**
   * Sets the Prometheus Gauge to a given value, if possible.
   */
  public static void gaugeSet(Object gauge, double val) {
    if (havePrometheus && gauge != null) {
      try {
        GAUGE_METHOD_SET.invoke(gauge, val);
      } catch (InvocationTargetException e) {
        if (e.getCause() != null && e.getCause() instanceof RuntimeException) {
          throw (RuntimeException) e.getCause();
        } else {
          log.warn("Invocation target exception", e);
        }
      } catch (IllegalArgumentException | IllegalAccessException e) {
        log.warn("Prometheus methods could not be invoked (version mismatch?) -- not logging statistics", e);
      }
    // If we are mocking Prometheus
    } else if (gauge instanceof GaugeMock) {
      ((GaugeMock) gauge).set(val);
    } else {
      log.warn("Something probably went wrong in Prometheus#gaugeSet: Expected Gauge or GaugeMock but received {}", gauge == null ? "null" : gauge.getClass());
    }
  }


  /**
   * Increments the value in the Prometheus Gauge by 1, if possible.
   */
  public static void gaugeInc(Object gauge) {
    if (havePrometheus && gauge != null) {
      try {
        GAUGE_METHOD_INC.invoke(gauge);
      } catch (InvocationTargetException e) {
        if (e.getCause() != null && e.getCause() instanceof RuntimeException) {
          throw (RuntimeException) e.getCause();
        } else {
          log.warn("Invocation target exception", e);
        }
      } catch (IllegalArgumentException | IllegalAccessException e) {
        log.warn("Prometheus methods could not be invoked (version mismatch?) -- not logging statistics", e);
      }
    // We are mocking Prometheus
    } else if (gauge instanceof GaugeMock){
      ((GaugeMock) gauge).inc();
    } else {
      log.warn("Something probably went wrong in Prometheus#gaugeInc: Expected Gauge or GaugeMock but received {}", gauge == null ? "null" : gauge.getClass());
    }
  }


  /**
   * Decrements the value in the Prometheus Gauge by 1, if possible.
   */
  public static void gaugeDec(Object gauge) {
    if (havePrometheus && gauge != null) {
      try {
        GAUGE_METHOD_DEC.invoke(gauge);
      } catch (InvocationTargetException e) {
        if (e.getCause() != null && e.getCause() instanceof RuntimeException) {
          throw (RuntimeException) e.getCause();
        } else {
          log.warn("Invocation target exception", e);
        }
      } catch (IllegalArgumentException | IllegalAccessException e) {
        log.warn("Prometheus methods could not be invoked (version mismatch?) -- not logging statistics", e);
      }
    // We are mocking Prometheus
    } else if (gauge instanceof GaugeMock) {
      ((GaugeMock) gauge).dec();
    } else {
      log.warn("Something probably went wrong in Prometheus#gaugeDec: Expected Gauge or GaugeMock but received {}", gauge == null ? "null" : gauge.getClass());
    }
  }


  /**
   * Builds a new Prometheus Counter metric, if possible.
   * @param name The name of the metric
   * @param help The help string of the metric
   *
   * @return The Prometheus counter, or a mock if necessary.
   */
  public static Object counterBuild(String name, String help) {
    Object result;
    if (havePrometheus) {
      try {
        Object builder = COUNTER_METHOD_BUILD.invoke(null, name, help);
        result =  COUNTERBUILDER_METHOD_REGISTER.invoke(builder);
      } catch (InvocationTargetException e) {
        if (e.getCause() != null && e.getCause() instanceof RuntimeException) {
          throw (RuntimeException) e.getCause();
        } else {
          log.warn("Invocation target exception", e);
          result =  new CounterMock(name);
        }
      } catch (IllegalArgumentException | IllegalAccessException e) {
        log.warn("Prometheus methods could not be invoked (version mismatch?) -- not logging statistics", e);
        result =  new CounterMock(name);
      }
    // If we are mocking Prometheus
    } else {
      result = new CounterMock(name);
    }
    counters.put(name, result);
    return result;
  }


  /**
   * Increments the value in the Prometheus Counter by 1, if possible.
   * We never do any other operations to Counter except increment it (for now)
   */
  public static void counterInc(Object counter) {
    if (havePrometheus && counter != null) {
      try {
        COUNTER_METHOD_INC.invoke(counter);
      } catch (InvocationTargetException e) {
        if (e.getCause() != null && e.getCause() instanceof RuntimeException) {
          throw (RuntimeException) e.getCause();
        } else {
          log.warn("Invocation target exception", e);
        }
      } catch (IllegalArgumentException | IllegalAccessException e) {
        log.warn("Prometheus methods could not be invoked (version mismatch?) -- not logging statistics", e);
      }
    // If we are mocking Prometheus
    } else if (counter instanceof CounterMock) {
      ((CounterMock) counter).inc();
    } else {
      log.warn("Something probably went wrong in Prometheus#counterInc: Expected Gauge or GaugeMock but received {}", counter == null ? "null" : counter.getClass());
    }
  }

  /**
   * Getter method for this.havePrometheus
   */
  public static boolean havePrometheus() {
    return havePrometheus;
  }


  /**
   * A dummy mock of a Prometheus Summary object.
   */
  private static class SummaryMock {
    /** The name of the summary */
    public final String name;
    /** The labels on the summary */
    public final String[] labels;

    /** A straightforward constructor */
    public SummaryMock(String name, String[] labels) {
      this.name = name;
      this.labels = labels;
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      SummaryMock that = (SummaryMock) o;
      return Objects.equals(name, that.name) &&
          Arrays.equals(labels, that.labels);
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
      int result = Objects.hash(name);
      result = 31 * result + Arrays.hashCode(labels);
      return result;
    }
  }


  /**
   * A dummy mock of a Prometheus Timer object.
   */
  private static class TimerMock {
    /** The summary this timer is attached to */
    public final SummaryMock summary;
    /** The label values on this timer */
    public final String[] labels;
    /** The time that this timer was created */
    public final long startTime;
    /** The straightforward constructor */
    public TimerMock(SummaryMock summary, String[] labels) {
      this.summary = summary;
      if (labels == null) {
        labels = new String[0];
      }
      this.labels = labels;
      if (summary != null && summary.labels != null && labels.length != summary.labels.length) {
        throw new IllegalArgumentException("Summary labels + timer label values should have the same length: " + summary.labels.length + " vs " + labels.length);
      }
      this.startTime = System.nanoTime();
    }

    /** Returns the time (in milliseconds) elapsed since this TimerMock was created */
    protected double observeDuration() {
      long timeNanoseconds = System.nanoTime() - this.startTime;
      return (double) timeNanoseconds / 1000000000.0;
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      TimerMock timerMock = (TimerMock) o;
      return Objects.equals(summary, timerMock.summary) &&
          Arrays.equals(labels, timerMock.labels);
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
      int result = Objects.hash(summary);
      result = 31 * result + Arrays.hashCode(labels);
      return result;
    }
  }


  /**
   * A dummy mock of a Prometheus Gauge object.
   */
  private static class GaugeMock {
    /** The name of the gauge */
    private final String name;
    /** The value stored in this Gauge **/
    private double value = 0;

    /** A straightforward constructor */
    public GaugeMock(String name) {
      this.name = name;
    }

    /** Gets the value of this mock */
    public double get() {
      return this.value;
    }

    /** Sets the value of this mock */
    public void set(double val) {
      this.value = val;
    }

    /** Increments the value of this mock */
    public void inc() {
      this.value += 1;
    }

    /** Decrements the value of this mock */
    public void dec() {
      this.value -= 1;
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      GaugeMock gaugeMock = (GaugeMock) o;
      return Objects.equals(name, gaugeMock.name);
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
      return Objects.hash(name);
    }
  }


  /**
   * A dummy mock of a Prometheus Counter object.
   */
  private static class CounterMock {
    /** The name of the counter */
    public final String name;
    /** The name of the counter */
    private double value = 0.;

    /** A straightforward constructor */
    public CounterMock(String name) {
      this.name = name;
    }

    /** Increments the value of this mock */
    public void inc() {
      this.value += 1;
    }

    /** Decrements the value of this mock */
    public void dec() {
      this.value -= 1;
    }

    /** Sets the value of this mock to `val`*/
    public void set(double val) {
      this.value = val;
    }

    /** Gets the value of this mock */
    public double get() {
      return this.value;
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      CounterMock that = (CounterMock) o;
      return Objects.equals(name, that.name);
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
      return Objects.hash(name);
    }
  }
}