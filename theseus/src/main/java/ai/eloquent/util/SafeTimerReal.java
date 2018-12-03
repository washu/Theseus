package ai.eloquent.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Timer;

/**
 * Created by keenon on 2/16/18.
 */
public class SafeTimerReal implements SafeTimer {
  /**
   * An SLF4J Logger for this class.
   */
  private static final Logger log = LoggerFactory.getLogger(SafeTimer.class);

  /** The implementing singleton timer */
  private final Timer timer;

  /** If true, this timer is still alive and kicking. */
  private boolean alive = true;

  /** Create a timer */
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

  /** {@inheritDoc} */
  @Override
  public void schedule(SafeTimerTask task, long delay) {
    if (!this.alive) {
      log.warn("Timer is cancelled; not queuing task", new RuntimeException());
      return;
    }
    timer.schedule(task, delay);
  }


  /** {@inheritDoc} */
  @Override
  public void schedule(SafeTimerTask task, long delay, long period) {
    if (!this.alive) {
      log.warn("Timer is cancelled; not queuing task", new RuntimeException());
      return;
    }
    timer.schedule(task, delay, period);
  }


  /** {@inheritDoc} */
  @Override
  public void scheduleAtFixedRate(SafeTimerTask task, long delay, long period) {
    if (!this.alive) {
      log.warn("Timer is cancelled; not queuing task", new RuntimeException());
      return;
    }
    timer.schedule(task, delay, period);
  }


  /** {@inheritDoc} */
  @Override
  public void scheduleAtFixedRate(SafeTimerTask task, long period) {
    if (!this.alive) {
      log.warn("Timer is cancelled; not queuing task", new RuntimeException());
      return;
    }
    scheduleAtFixedRate(task, 0, period);
  }


  /** {@inheritDoc} */
  @Override
  public void cancel() {
    if (!this.alive) {
      timer.cancel();
      this.alive = false;
    } else {
      log.warn("Ignoring duplicate timer cancel");
    }
  }
}
