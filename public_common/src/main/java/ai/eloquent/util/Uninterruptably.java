package ai.eloquent.util;

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
  public static void sleep(long millis, int nanos) {
    if (millis == 0 && nanos != 0) {
      try {
        Thread.sleep(0, nanos);
      } catch (InterruptedException e) {
        throw new RuntimeInterruptedException(e);  // we still want to be able to interrupt, we just don't care if it's a real exception.
      }
    } else if (millis > 0) {
      long sleepTill = System.nanoTime() + millis * 1000000 + nanos;
      while (System.nanoTime() < sleepTill) {
        try {
          long sleepTime = sleepTill - System.nanoTime();
          if (sleepTime > 0) {
            Thread.sleep(sleepTime / 1000000, (int) (sleepTill % 1000000));
          }
        } catch (InterruptedException e) {
          throw new RuntimeInterruptedException(e);  // we still want to be able to interrupt, we just don't care if it's a real exception.
        }
      }
    }
  }


  /**
   * @see #sleep(long, int)
   */
  public static void sleep(long millis) {
    sleep(millis, 0);
  }


  /**
   * A helper for {@link #sleep(long)}
   */
  public static void sleep(Duration duration) {
    long nanos = duration.toNanos();
    sleep(nanos / 1000000, (int) (nanos % 1000000));
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
