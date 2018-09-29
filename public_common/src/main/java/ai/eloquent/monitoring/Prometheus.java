package ai.eloquent.monitoring;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Objects;

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

  /** Mocked gauges, in case we don't have Prometheus in our classpath */
  private static final HashMap<Object, Double> gaugeMocks = new HashMap<>();

  /** Mocked timers, in case we don't have Prometheus in our classpath */
  private static final HashMap<Object, Long> timerMocks = new HashMap<>();

  /** Mocked counters, in case we don't have Prometheus in our classpath */
  private static final HashMap<Object, Double> counterMocks = new HashMap<>();

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
  static void resetMockMetrics() {
    gaugeMocks.clear();
    timerMocks.clear();
    counterMocks.clear();
  }


  /**
   * If true, we have a working Prometheus instance in our classpath.
   * Otherwise, we are using a mocked version.
   */
  public static boolean havePrometheus() {
    return SUMMARY_METHOD_BUILD != null;
  }


  /**
   * Overloaded method
   * Builds a new Prometheus Summary, if possible.
   *
   * @param name The name of the metric
   * @param help The help string of the metric
   * @param labels The label to attach to the metric
   *
   * @return The Summary object
   */
  public static Object summaryBuild(String name, String help, String... labels) {
    if (SUMMARY_METHOD_BUILD == null || SUMMARYBUILDER_METHOD_LABELNAMES == null || SUMMARYBUILDER_METHOD_REGISTER == null) {
      return new SummaryMock(name, labels);
    }
    try {
      Object builder = SUMMARY_METHOD_BUILD.invoke(null, name, help);
      builder = SUMMARYBUILDER_METHOD_LABELNAMES.invoke(builder, new Object[]{labels});
      return SUMMARYBUILDER_METHOD_REGISTER.invoke(builder);
    } catch (InvocationTargetException e) {
      if (e.getCause() != null && e.getCause() instanceof RuntimeException) {
        throw (RuntimeException) e.getCause();
      } else {
        log.warn("Invocation target exception", e);
        return new SummaryMock(name, labels);
      }
    } catch (IllegalArgumentException | IllegalAccessException e) {
      log.warn("Prometheus methods could not be invoked (version mismatch?) -- not logging statistics", e);
      return new SummaryMock(name, labels);
    }
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
    if (summary == null || SUMMARY_METHOD_STARTTIMER == null || SUMMARY_METHOD_LABELS == null) {
      TimerMock timerMock;
      if (summary != null && !(summary instanceof SummaryMock)) {
        log.error("Starting a timer on something other than a SummaryMock: {}", summary.getClass());
        //noinspection ConstantConditions
        timerMock = new TimerMock(null, labels);
      } else {
        //noinspection ConstantConditions
        timerMock = new TimerMock((SummaryMock) summary, labels);
      }
      timerMocks.put(timerMock, System.nanoTime());
      return timerMock;
    }
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
        return new TimerMock(null, labels);
      }
    } catch (IllegalArgumentException | IllegalAccessException e) {
      log.warn("Prometheus methods could not be invoked (version mismatch?) -- not logging statistics", e);
      e.printStackTrace();
      //noinspection ConstantConditions
      return new TimerMock(null, labels);
    }
  }


  /**
   * Observe duration on a Prometheus timer.
   * @param timer The prometheus timer.
   */
  public static double observeDuration(@Nullable Object timer) {
    if (timer == null) {
      return 0.0;
    } else if (TIMER_METHOD_OBSERVE_DURATION == null) {
      long startTime = timerMocks.get(timer);
      long timeNanoseconds = System.nanoTime() - startTime;
      return (double) timeNanoseconds / 1000000000.0;
    } else {
      try {
        Double rtn = (Double) TIMER_METHOD_OBSERVE_DURATION.invoke(timer);
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
    if (GAUGE_METHOD_BUILD == null || GAUGEBUILDER_METHOD_REGISTER == null) {
      GaugeMock gaugeMock = new GaugeMock(name);
      gaugeMocks.put(gaugeMock, 0.0);
      return gaugeMock;
    } else {
      try {
        Object builder = GAUGE_METHOD_BUILD.invoke(null, name, help);
        return GAUGEBUILDER_METHOD_REGISTER.invoke(builder);
      } catch (InvocationTargetException e) {
        if (e.getCause() != null && e.getCause() instanceof RuntimeException) {
          throw (RuntimeException) e.getCause();
        } else {
          log.warn("Invocation target exception", e);
          return new GaugeMock(name);
        }
      } catch (IllegalArgumentException | IllegalAccessException e) {
        log.warn("Prometheus methods could not be invoked (version mismatch?) -- not logging statistics", e);
        return new GaugeMock(name);
      }
    }
  }


  /**
   * Gets the current value stored in the Prometheus Gauge, if possible.
   *
   * @param gauge The instance of the gauge
   *
   * @return The value of the Gauge as a Double
   */
  public static double gaugeGet(Object gauge) {
    if (gauge == null || GAUGE_METHOD_GET == null) {
      return gaugeMocks.getOrDefault(gauge, 0.0);
    } else {
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
    }
  }


  /**
   * Sets the Prometheus Gauge to a given value, if possible.
   */
  public static void gaugeSet(Object gauge, double val) {
    if (gauge == null || GAUGE_METHOD_SET == null) {
      gaugeMocks.put(gauge, val);
    } else {
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
    }
  }


  /**
   * Increments the value in the Prometheus Gauge by 1, if possible.
   */
  public static void gaugeInc(Object gauge) {
    if (gauge == null || GAUGE_METHOD_INC == null) {
      gaugeMocks.put(gauge, gaugeMocks.getOrDefault(gauge, 0.0) + 1);
    } else {
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
    }
  }


  /**
   * Decrements the value in the Prometheus Gauge by 1, if possible.
   */
  public static void gaugeDec(Object gauge) {
    if (gauge == null || GAUGE_METHOD_DEC == null) {
      gaugeMocks.put(gauge, gaugeMocks.getOrDefault(gauge, 0.0) - 1);
    } else {
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
    if (COUNTER_METHOD_BUILD == null || COUNTERBUILDER_METHOD_REGISTER == null) {
      CounterMock counterMock = new CounterMock(name);
      counterMocks.put(counterMock, 0.0);
      return counterMock;
    } else {
      try {
        Object builder = COUNTER_METHOD_BUILD.invoke(null, name, help);
        return COUNTERBUILDER_METHOD_REGISTER.invoke(builder);
      } catch (InvocationTargetException e) {
        if (e.getCause() != null && e.getCause() instanceof RuntimeException) {
          throw (RuntimeException) e.getCause();
        } else {
          log.warn("Invocation target exception", e);
          return new CounterMock(name);
        }
      } catch (IllegalArgumentException | IllegalAccessException e) {
        log.warn("Prometheus methods could not be invoked (version mismatch?) -- not logging statistics", e);
        return new CounterMock(name);
      }
    }
  }


  /**
   * Increments the value in the Prometheus Counter by 1, if possible.
   */
  public static void counterInc(Object counter) {
    if (counter == null || COUNTER_METHOD_INC == null) {
      counterMocks.put(counter, counterMocks.getOrDefault(counter, 0.0) + 1);
    } else {
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
    }
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
    public final String name;
    /** A straightforward constructor */
    public GaugeMock(String name) {
      this.name = name;
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
    /** A straightforward constructor */
    public CounterMock(String name) {
      this.name = name;
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