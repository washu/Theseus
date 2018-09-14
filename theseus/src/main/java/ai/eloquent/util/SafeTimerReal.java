package ai.eloquent.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Timer;

/**
 * Created by keenon on 2/16/18.
 */
public class SafeTimerReal implements SafeTimer {
  /**
   * An SLF4J Logger for this class.
   */
  private static final Logger log = LoggerFactory.getLogger(SafeTimer.class);

  Timer timer;

  public SafeTimerReal() {
    timer = new Timer("SafeTimerReal", true);
  }

  /**
   * This retrieves the current time on this box. This is only here so that it can be mocked.
   */
  @Override
  public long now() {
    return System.currentTimeMillis();
  }

  @Override
  public void schedule(SafeTimerTask task, long delay) {
    timer.schedule(task, delay);
  }

  @Override
  public void schedule(SafeTimerTask task, long delay, long period) {
    timer.schedule(task, delay, period);
  }

  @Override
  public void scheduleAtFixedRate(SafeTimerTask task, long delay, long period) {
    timer.schedule(task, delay, period);
  }


  @Override
  public void scheduleAtFixedRate(SafeTimerTask task, long period) {
    scheduleAtFixedRate(task, 0, period);
  }


  @Override
  public void cancel() {
    timer.cancel();
  }
}
