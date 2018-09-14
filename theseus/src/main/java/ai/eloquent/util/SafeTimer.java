package ai.eloquent.util;

import java.time.Duration;
import java.util.Timer;
import java.util.TimerTask;


/**
 * This is a Timer that only accepts SafeTimerTasks as tasks.
 */
public interface SafeTimer {
  /**
   * This retrieves the current time on this box. This is only here so that it can be mocked.
   */
  long now();

  /**
   * @see Timer#schedule(TimerTask, long)
   */
  void schedule(SafeTimerTask task, long delay);


  /**
   * @see Timer#schedule(TimerTask, long)
   */
  default void schedule(SafeTimerTask task, Duration delay) {
    schedule(task, delay.toMillis());
  }


  /**
   * @see Timer#schedule(TimerTask, long, long)
   */
  void schedule(SafeTimerTask task, long delay, long period);


  /**
   * @see Timer#schedule(TimerTask, long, long)
   */
  default void schedule(SafeTimerTask task, Duration delay, Duration period) {
    schedule(task, delay.toMillis(), period.toMillis());
  }


  /**
   * @see Timer#scheduleAtFixedRate(TimerTask, long, long)
   */
  void scheduleAtFixedRate(SafeTimerTask task, long delay, long period);


  /**
   * @see Timer#scheduleAtFixedRate(TimerTask, long, long)
   */
  default void scheduleAtFixedRate(SafeTimerTask task, Duration delay, Duration period) {
    scheduleAtFixedRate(task, delay.toMillis(), period.toMillis());
  }


  /**
   * Schedule at a fixed rate with no delay.
   *
   * @see Timer#scheduleAtFixedRate(TimerTask, long, long)
   */
  void scheduleAtFixedRate(SafeTimerTask task, long period);


  /**
   * Schedule at a fixed rate with no delay.
   *
   * @see Timer#scheduleAtFixedRate(TimerTask, long, long)
   */
  default void scheduleAtFixedRate(SafeTimerTask task, Duration period) {
    scheduleAtFixedRate(task, period.toMillis());
  }


  /**
   * Cancel this timer.
   */
  void cancel();
}
