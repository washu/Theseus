package ai.eloquent.util;

import ai.eloquent.util.RuntimeInterruptedException;

import java.time.Duration;

/**
 * Created by keenon on 11/27/16.
 *
 * It turns out Thread.sleep() isn't guaranteed to wait the whole time, and can get woken up at random moments. If you
 * really want the behavior that almost everyone wants (sleep as long as I told you to, not as long as you feel like)
 * then you need to wrap sleep in a while() loop that checks the clock. This class does that.
 */
public class Uninterruptably {

  /**
   * Sleep for a given amount of time, not allowing for interrupts to shorten the time.
   *
   * @param millis The amount of time to sleep for.
   */
  public static void sleep(long millis) {
    if (millis <= 0) {
      return;
    }
    long sleepTill = System.currentTimeMillis() + millis;
    while (System.currentTimeMillis() < sleepTill) {
      try {
        long sleepTime = sleepTill - System.currentTimeMillis();
        if (sleepTime > 0) {
          Thread.sleep(sleepTime);
        }
      } catch (InterruptedException e) {
        throw new RuntimeInterruptedException(e);  // we still want to be able to interrupt, we just don't care if it's a real exception.
      }
    }
  }


  /**
   * A helper for {@link #sleep(long)}
   */
  public static void sleep(Duration duration) {
    sleep(duration.toMillis());
  }


  /**
   * Join with a thread, retrying the join if we are interrupted.
   *
   * @param t The thread to join.
   */
  public static void join(Thread t) {
    // Try joining
    try {
      t.join();
    } catch (InterruptedException e) {
      // Retry if we were interrupted
      join(t);
    }
    // Retry if we are still alive
    if (t.isAlive()) {
      join(t);
    }
  }
}
