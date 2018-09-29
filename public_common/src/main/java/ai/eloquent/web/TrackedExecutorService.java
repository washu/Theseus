package ai.eloquent.web;

import ai.eloquent.error.RaftErrorListener;
import ai.eloquent.monitoring.Prometheus;
import ai.eloquent.stats.IntCounter;
import ai.eloquent.util.StackTrace;
import ai.eloquent.util.SystemUtils;
import ai.eloquent.util.TimerUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * A wrapper around an executor service that tracks our threads.
 */
public class TrackedExecutorService implements ExecutorService {
  /**
   * An SLF4J Logger for this class.
   */
  private static final Logger log = LoggerFactory.getLogger(TrackedExecutorService.class);

  /** Keeps track of existing {@link RaftErrorListener} **/
  private static ArrayList<RaftErrorListener> errorListeners = new ArrayList<>();

  /**
   * Keeps track of an additional {@link RaftErrorListener} in this class
   * @param errorListener
   */
  public void addErrorListener(RaftErrorListener errorListener) {
    errorListeners.add(errorListener);
  }

  /**
   * Stop listening from a specific {@link RaftErrorListener}
   * @param errorListener The error listener to be removed
   */
  public void removeErrorListener(RaftErrorListener errorListener) {
    errorListeners.remove(errorListener);
  }

  /**
   * Clears all the {@link RaftErrorListener}s attached to this class.
   */
  public void clearErrorListeners() {
    errorListeners.clear();
  }

  /**
   * Alert each of the {@link RaftErrorListener}s attached to this class.
   */
  private void throwRaftError(String incidentKey, String debugMessage) {
    errorListeners.forEach(listener -> listener.accept(incidentKey, debugMessage, Thread.currentThread().getStackTrace()));
  }

  /** The implementing executor */
  private final ExecutorService impl;

  /** The number of elements currently in the queue. */
  private final String name;

  /** The number of elements currently in the queue. */
  private final Object Gauge_queueSize;

  /** The number of tasks ever run on this executor. */
  private final Object Counter_taskCount;

  /** The runtime of tasks in this executor. */
  private final Object Summary_runtime;

  /** The amount of time threads spend in the queue before starting. */
  private final Object Summary_queueTime;

  /** The last time we issued a page, to prevent spamming PagerDuty. */
  private long lastPaged = 0L;

  /** The last time we issued a page, to prevent spamming PagerDuty. */
  private long pageAboveThreadCount = 128L;


  /** Create a tracked executor */
  public TrackedExecutorService(String name, ExecutorService impl) {
    this.impl = impl;
    this.name = name.replace('-', '_').replace(' ', '_');
    Gauge_queueSize = Prometheus.gaugeBuild(this.name + "_queue_size","The number of tasks currently in pool " + this.name);
    Counter_taskCount = Prometheus.counterBuild(this.name + "_total_tasks", "The number of tasks that have run in pool " + this.name);
    Summary_runtime = Prometheus.summaryBuild(this.name + "_runtime", "The time it takes for tasks to run in pool " + this.name);
    Summary_queueTime = Prometheus.summaryBuild(this.name + "_queuetime", "The time it takes for tasks to be scheduled in pool " + this.name);
  }


  /**
   * Page if we have more than the argument number of threads in the thread pool.
   *
   * @param count The number of threads above which to send a page (non-inclusive)
   */
  public void pageAboveThreadCount(int count) {
    this.pageAboveThreadCount = Math.max(1, count);
  }


  /**
   * Keep track of the queue size, paging if it gets too large.
   */
  private void checkQueueSize() {
    Double queueSize = Prometheus.gaugeGet(this.Gauge_queueSize);
    if (this.impl instanceof ThreadPoolExecutor && queueSize > 128) {
      synchronized (TrackedExecutorService.this) {
        // 1. Don't duplicate page
        if (queueSize < this.pageAboveThreadCount || System.currentTimeMillis() < lastPaged + 600000) {  // don't duplicate page
          return;
        }
        this.lastPaged = System.currentTimeMillis();
        log.warn("A queue has more than 64 threads waiting on it: {} -- paging PagerDuty", queueSize);
        // 2. Get all the threads in this pool
        Map<Thread, StackTraceElement[]> threads = Thread.getAllStackTraces();
        IntCounter<List<StackTraceElement>> inThisPool = new IntCounter<>();
        for (Map.Entry<Thread, StackTraceElement[]> entry : threads.entrySet()) {
          if (entry.getKey().getName().startsWith(this.name)) {
            inThisPool.incrementCount(Arrays.asList(entry.getValue()));
          }
        }
        // 3. Convert traces to Strings
        IntCounter<String> traces = new IntCounter<>();
        for (Map.Entry<List<StackTraceElement>, Integer> entry : inThisPool.entrySet()) {
          traces.setCount(new StackTrace(entry.getKey()).toString(), entry.getValue());
        }
        // 4. Page
        if (traces.totalIntCount() >= 128) {  // make sure we empirically have > 128 pages
          String incidentKey = "thread-overload-" + this.name + SystemUtils.HOST;
          throwRaftError(incidentKey, "Too many threads on " + this.name);
        }
      }
    }
  }


  /**
   * Wrap a runnable in our metrics
   */
  private Runnable wrap(Runnable task) {
    Object Timer_queueTimer = Prometheus.startTimer(Summary_queueTime);
    Prometheus.gaugeInc(Gauge_queueSize);
    Prometheus.counterInc(Counter_taskCount);
    checkQueueSize();
//      StackTraceElement[] callerTrace = Thread.currentThread().getStackTrace();
    return () -> {
      Prometheus.observeDuration(Timer_queueTimer);
      Object Timer_runTimer = Prometheus.startTimer(Summary_runtime);
      try {
        task.run();
      } finally {
        Prometheus.gaugeDec(Gauge_queueSize);
        Double duration = Prometheus.observeDuration(Timer_runTimer);
        if (duration > 60.0 && duration != null) {
          log.warn("Thread on executor {} took >1m to finish ({})",
              this.name, TimerUtils.formatTimeDifference(duration.longValue() * 1000));
//                this.name, TimeUtils.formatTimeDifference((long) duration * 1000), new StackTrace(callerTrace));
        }
      }
    };
  }


  /**
   * Wrap a callable in our metrics
   */
  private <T> Callable<T> wrap(Callable<T> task) {
    Object Timer_queueTimer = Prometheus.startTimer(Summary_queueTime);
    checkQueueSize();
    Prometheus.gaugeInc(Gauge_queueSize);
    Prometheus.counterInc(Counter_taskCount);
    return () -> {
      Prometheus.observeDuration(Timer_queueTimer);
      Object Summary_runTimer = Prometheus.startTimer(Summary_runtime);
      try {
        return task.call();
      } finally {
        Prometheus.gaugeDec(Gauge_queueSize);
        Prometheus.observeDuration((Summary_runTimer));
      }
    };
  }


  /** {@inheritDoc} */
  @Override
  public void shutdown() {
    impl.shutdown();
  }

  /** {@inheritDoc} */
  @Nonnull
  @Override
  public List<Runnable> shutdownNow() {
    return impl.shutdownNow();
  }

  /** {@inheritDoc} */
  @Override
  public boolean isShutdown() {
    return impl.isShutdown();
  }

  /** {@inheritDoc} */
  @Override
  public boolean isTerminated() {
    return impl.isTerminated();
  }

  /** {@inheritDoc} */
  @Override
  public boolean awaitTermination(long timeout, @Nonnull TimeUnit unit) throws InterruptedException {
    return impl.awaitTermination(timeout, unit);
  }

  /** {@inheritDoc} */
  @Nonnull
  @Override
  public <T> Future<T> submit(@Nonnull Callable<T> task) {
    return impl.submit(wrap(task));
  }

  /** {@inheritDoc} */
  @Nonnull
  @Override
  public <T> Future<T> submit(@Nonnull Runnable task, T result) {
    return impl.submit(wrap(task), result);
  }

  /** {@inheritDoc} */
  @Nonnull
  @Override
  public Future<?> submit(@Nonnull Runnable task) {
    return impl.submit(wrap(task));
  }

  /** {@inheritDoc} */
  @Nonnull
  @Override
  public <T> List<Future<T>> invokeAll(@Nonnull Collection<? extends Callable<T>> tasks) throws InterruptedException {
    return impl.invokeAll(tasks.stream().map(this::wrap).collect(Collectors.toList()));
  }

  /** {@inheritDoc} */
  @Nonnull
  @Override
  public <T> List<Future<T>> invokeAll(@Nonnull Collection<? extends Callable<T>> tasks, long timeout, @Nonnull TimeUnit unit) throws InterruptedException {
    return impl.invokeAll(tasks.stream().map(this::wrap).collect(Collectors.toList()), timeout, unit);
  }

  /** {@inheritDoc} */
  @Nonnull
  @Override
  public <T> T invokeAny(@Nonnull Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
    return impl.invokeAny(tasks.stream().map(this::wrap).collect(Collectors.toList()));
  }

  /** {@inheritDoc} */
  @Override
  public <T> T invokeAny(@Nonnull Collection<? extends Callable<T>> tasks, long timeout, @Nonnull TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
    return impl.invokeAny(tasks.stream().map(this::wrap).collect(Collectors.toList()), timeout, unit);
  }

  /** {@inheritDoc} */
  @Override
  public void execute(@Nonnull Runnable command) {
    impl.execute(wrap(command));
  }

  /** {@inheritDoc} */
  @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
  @Override
  public boolean equals(Object o) {
    return impl.equals(o);
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return impl.hashCode();
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return impl.toString();
  }
}
