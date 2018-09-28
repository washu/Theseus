package ai.eloquent.util;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This holds all of our timer tasks in a mocked environment, so that when we run unit tests we can guarantee that
 * things are getting called.
 */
public class SafeTimerMock implements SafeTimer {
  /**
   * An SLF4J Logger for this class.
   */
  private static final Logger log = LoggerFactory.getLogger(SafeTimerMock.class);

  /**
   * This is the static, global time shared across all SafeTimeMocks
   */
  static long mockTime = 0;

  /**
   * This is the static, global list of all ScheduledTasks
   */
  static final List<SafeTimerMock> timerMocks = new ArrayList<>();

  /**
   * A lock for {@link #timerMocks}, so we can take a lock with a timeout
   */
  private static final ReentrantLock timerLock = new ReentrantLock();

  /**
   * A condition for some event having happened on the timer.
   */
  private static final Condition timerCondition = timerLock.newCondition();

  /**
   * This lets us pre-compute the tasks that will run at a given timestep
   */
  private final static Multimap<Long, ScheduledTask> queue = ArrayListMultimap.create();

  /**
   * This is the list of all ScheduledTasks on this SafeTimerMock
   */
  public final List<ScheduledTask> scheduled = Collections.synchronizedList(new ArrayList<>());

  public SafeTimerMock() {
    try {
      if (timerLock.tryLock(10, TimeUnit.SECONDS)) {
        try {
          timerMocks.add(this);
        } finally {
          timerLock.unlock();
        }
      }
      else throw new RuntimeException("Failed to get lock in tryLock()");
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * This holds all our tasks for the timer mock
   */
  public class ScheduledTask {
    public final SafeTimerTask task;
    public final long startTime;
    public final long period;
    public final long scheduledAt;

    public ScheduledTask(SafeTimerTask task, long startTime) {
      this(task, startTime, -1);
    }

    public ScheduledTask(SafeTimerTask task, long startTime, long period) {
      this.task = task;
      this.startTime = startTime;
      this.period = period;
      this.scheduledAt = now();
      synchronized (queue) {
        queue.put(startTime, this);
      }
    }

    /**
     * This returns true if the ScheduledTask should be removed from the list.
     *
     * @return true if should remove
     */
    public boolean runIfAppropriate() {
      if (this.task.cancelled) {
        return true;
      }

      // This indicated that this is a one-time thing
      if (this.period == -1) {
        if (mockTime >= this.startTime) {
          task.run(Optional.empty());
          return true;
        }
        return false;
      } else {
        // This means that this is a recurring task
        if (mockTime >= this.startTime) {
          long offset = (mockTime - this.startTime) % period;
          if (offset == 0) {
            task.run(Optional.empty());
            synchronized (queue) {
              queue.put(mockTime + period, this);
            }
          }
        }
        return false;
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ScheduledTask that = (ScheduledTask) o;
      return startTime == that.startTime &&
          period == that.period &&
          scheduledAt == that.scheduledAt &&
          Objects.equals(task, that.task);
    }

    @Override
    public int hashCode() {
      return Objects.hash(task, startTime, period, scheduledAt);
    }
  }

  /**
   * This moves time forward in the mock timer, which will trigger any timers that need to be triggered in a synchronous
   * manner on this thread.
   */
  public static void advanceTime(long millis) {
    boolean removedAny = false;

    long targetTime = mockTime + millis;
    while (true) {
      List<ScheduledTask> scheduledThisTick;

      try {
        if (timerLock.tryLock(10, TimeUnit.SECONDS)) {
          try {
            synchronized (queue) {
              scheduledThisTick = new ArrayList<>(queue.get(mockTime));
            }

            List<ScheduledTask> toRemove = new ArrayList<>();
            for (ScheduledTask task : scheduledThisTick) {
              if (task.startTime >= mockTime-1 && task.period <= 0) {
                toRemove.add(task);
              }
              synchronized (queue) {
                queue.remove(mockTime, task);
                queue.remove(mockTime-1, task);
              }
            }
            if (toRemove.size() > 0) {
              removedAny = true;
              for (SafeTimerMock timerMock : timerMocks) {
                timerMock.scheduled.removeAll(toRemove);
              }
            }
          } finally {
            timerLock.unlock();
          }
        } else {
          log.warn("Failed to get lock in tryLock()");
          return;
        }
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }

      // Run the tasks after we've released the main lock

      for (ScheduledTask task : scheduledThisTick) {
        task.runIfAppropriate();
      }

      if (mockTime < targetTime) {
        mockTime += 1;
      } else {
        break;
      }
    }

    try {
      if (timerLock.tryLock(10, TimeUnit.SECONDS)) {
        try {
          if (removedAny) {
            timerCondition.signalAll();
          }
        } finally {
          timerLock.unlock();
        }
      } else throw new RuntimeException("Failed to get lock in tryLock()");
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * This clears any previous timers we still had registered, and gets the system ready to restart
   */
  public static void resetMockTime() {
    try {
      if (timerLock.tryLock(10, TimeUnit.SECONDS)) {
        try {
          mockTime = 0;
          timerMocks.clear();
        } finally {
          timerLock.unlock();
        }

        synchronized (queue) {
          queue.clear();
        }
      }
      else throw new RuntimeException("Failed to get lock in tryLock()");
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * This returns the number of tasks we currently have scheduled on this SafeTimerMock.
   */
  public int numTasksScheduled() {
    return scheduled.size();
  }

  /**
   * This blocks the calling thread until there are no outstanding tasks on this SafeTimerMock running, then continues.
   *
   * This will time out after 5 virtual "minutes" of waiting period.
   */
  public void waitForSilence() {
    log.info("[{}] Waiting for SafeTimerMock to flush. scheduled={}", mockTime, numTasksScheduled());
    long startTime = now();
    long lastMessageTime = startTime;
    try {
      if (timerLock.tryLock(10, TimeUnit.SECONDS)) {
        try {
          int i = 0;
          while (numTasksScheduled() > 0 && i < 10000) {
            i += 1;
            try {
              timerCondition.await(1, TimeUnit.SECONDS);
              // Hooray, we've successfully got silence
              if (numTasksScheduled() == 0) {
                Uninterruptably.sleep(10);
                if (numTasksScheduled() == 0) {
                  break;
                }
              }
              // Log if the queue hasn't flushed in long enough [virtual time]
              if (now() > lastMessageTime + 60000) {
                log.warn("[{}] Have not flushed transport after {}; scheduled={}", mockTime, TimerUtils.formatTimeDifference(now() - startTime), numTasksScheduled());
                lastMessageTime = now();
              }
              // Exception if the queue doesn't flush in 5 [virtual] minutes seconds
              if (now() > startTime + Duration.ofMinutes(5).toMillis()) {
                throw new IllegalStateException("Transport hasn't flushed in 5 minutes. This almost certainly means a deadlock somewhere. " +
                    "now=" + mockTime + "; queue size=" + numTasksScheduled());
              }
            } catch (InterruptedException e) {
              log.warn("Interrupt waiting on transport. Transport still has {} scheduled events", numTasksScheduled());
            }
          }
          if (numTasksScheduled() > 0) {
            log.warn("There are still tasks scheduled after {} iterations of waiting; this is from a race condition on the timer", i);
          }
        } finally {
          timerLock.unlock();
        }
      }
      else throw new RuntimeException("Failed to get lock in tryLock()");
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    log.info("[{}] Transport is flushed (scheduled={} real_flush_time={})", mockTime, numTasksScheduled(), TimerUtils.formatTimeDifference(now() - startTime));
  }


  /**
   * Get the current time on the timer mock.
   */
  public static long time() {
    return mockTime;
  }


  /**
   * This retrieves the current time on this box. This is only here so that it can be mocked.
   */
  @Override
  public long now() {
    return mockTime;
  }

  /**
   * This allows outside code to access the timer lock, in case they need to do things that can't allow time to slip
   */
  public void withTimerLock(Runnable runnable) {
    try {
      if (timerLock.tryLock(10, TimeUnit.SECONDS)) {
        try {
          runnable.run();
        } finally {
          timerLock.unlock();
        }
      }
      else throw new RuntimeException("Failed to get lock in tryLock()");
    } catch (InterruptedException e) {
      throw new RuntimeInterruptedException(e);
    }
  }

  @Override
  public void schedule(SafeTimerTask task, long delay) {
    try {
      if (timerLock.tryLock(10, TimeUnit.SECONDS)) {
        try {

          // If we don't schedule this inside the lock, it's possible for time to slip past us while we're scheduling, and
          // then this event will never be run.

          ScheduledTask scheduledTask = new ScheduledTask(task, mockTime + delay);
          scheduled.add(scheduledTask);
        } finally {
          timerLock.unlock();
        }
      }
      else throw new RuntimeException("Failed to get lock in tryLock()");
    } catch (InterruptedException e) {
      throw new RuntimeInterruptedException(e);
    }
  }

  @Override
  public void schedule(SafeTimerTask task, long delay, long period) {
    try {
      if (timerLock.tryLock(10, TimeUnit.SECONDS)) {
        try {

          // If we don't schedule this inside the lock, it's possible for time to slip past us while we're scheduling, and
          // then this event will never be run.

          ScheduledTask scheduledTask = new ScheduledTask(task, mockTime + delay, period);
          task.registerCancelCallback(() -> {
            try {
              if (timerLock.tryLock(10, TimeUnit.SECONDS)) {
                try {
                  scheduled.remove(scheduledTask);
                  timerCondition.signalAll();
                } finally {
                  timerLock.unlock();
                }
              } else {
                throw new IllegalStateException("Could not take lock on timer cancel for 10 seconds");
              }
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
          });

          scheduled.add(scheduledTask);
        } finally {
          timerLock.unlock();
        }
      }
      else throw new RuntimeException("Failed to get lock in tryLock()");
    } catch (InterruptedException e) {
      throw new RuntimeInterruptedException(e);
    }
  }

  @Override
  public void scheduleAtFixedRate(SafeTimerTask task, long delay, long period) {
    schedule(task, delay, period);
  }

  @Override
  public void scheduleAtFixedRate(SafeTimerTask task, long period) {
    scheduleAtFixedRate(task, 0, period);
  }

  @Override
  public void cancel() {
    try {
      if (timerLock.tryLock(10, TimeUnit.SECONDS)) {
        try {
          timerMocks.remove(this);
          List<ScheduledTask> scheduledCopy = new ArrayList<>(scheduled);
          for (ScheduledTask task : scheduledCopy) {
            task.task.cancel();
          }
          scheduled.clear();
          timerCondition.signalAll();
        } finally {
          timerLock.unlock();
        }
      }
      else throw new RuntimeException("Failed to get lock in tryLock()");
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
