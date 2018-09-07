package ai.eloquent.web;

import ai.eloquent.util.Lazy;
import ai.eloquent.util.SafeTimer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

abstract public class Lifecycle {

  public static Lifecycle global;

  /**
   * This indicates when we have closed the core thread pools, one of the last steps right before shutdown. This is here
   * so that mocks that simulate multiple Lifecycle objects on the same VM can stop performing actions that would have
   * executed on a core thread pool.
   */
  public final AtomicBoolean CORE_THREAD_POOLS_CLOSED = new AtomicBoolean(false);

  /**
   * A global timer that we can use.
   */
  public Lazy<SafeTimer> timer;

  /**
   * Create a thread pool that closes itself on program shutdown.
   *
   * @param numThreads The number of threads to allocate in the pool
   * @param threadPrefix When naming threads, use this prefix to identify threads in this pool vs. other pools.
   * @param core If true, this is a core system thread pool. Core thread pools are shut down after Rabbit and the database close,
   *             whereas non-core thread pools close with Rabbit and the database still open.
   * @param priority The priority of the threads scheduled on this pool.
   *
   * @return The thread pool created.
   */
  abstract public ExecutorService managedThreadPool(int numThreads, String threadPrefix, boolean core, int priority);


  /**
   * @see #managedThreadPool(int, String, boolean, int)
   */
  abstract public ExecutorService managedThreadPool(int numThreads, String threadPrefix, boolean core);


  /**
   * @see #managedThreadPool(int, String, boolean, int)
   */
  abstract public ExecutorService managedThreadPool(int numThreads, String threadPrefix, int priority);


  /**
   * @see #managedThreadPool(int, String, boolean)
   */
  abstract public ExecutorService managedThreadPool(int numThreads, String threadPrefix);

  /**
   * Create a managed cached thread pool.
   *
   * @param threadPrefix The prefix to apply to all the threads in the pool
   * @param core If true, this is a core system thread pool. Core thread pools are shut down after Rabbit and the database close,
   *             whereas non-core thread pools close with Rabbit and the database still open.
   * @param priority The priority of the threads scheduled on this pool.
   *
   * @return The thread pool.
   */
  abstract public ExecutorService managedThreadPool(String threadPrefix, boolean core, int priority);

  /**
   * @see #managedThreadPool(String, boolean, int)
   */
  abstract public ExecutorService managedThreadPool(String threadPrefix, boolean core);


  /**
   * Create a managed cached thread pool.
   *
   * @param threadPrefix The prefix to apply to all the threads in the pool
   *
   * @return The thread pool.
   */
  abstract public ExecutorService managedThreadPool(String threadPrefix);

  /**
   * Create a managed cached thread pool with a given priority.
   *
   * @param threadPrefix The prefix to apply to all the threads in the pool
   * @param priority The priority of the threads scheduled on this pool.
   *
   * @return The thread pool.
   */
  abstract public ExecutorService managedThreadPool(String threadPrefix, int priority);

}
