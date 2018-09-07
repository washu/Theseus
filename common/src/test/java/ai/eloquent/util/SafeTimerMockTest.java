package ai.eloquent.util;

import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.*;

/**
 * This manages the global timer mocks.
 */
public class SafeTimerMockTest {
  @Before
  public void resetTimer() {
    SafeTimerMock.resetMockTime();
  }

  @Test
  public void now() {
    SafeTimerMock mock1 = new SafeTimerMock();
    assertEquals(0, mock1.now());

    SafeTimerMock.advanceTime(10);
    assertEquals(10, mock1.now());
    assertEquals(10, new SafeTimerMock().now());
  }

  @Test
  public void schedule() {
    SafeTimerMock mock = new SafeTimerMock();
    Pointer<Boolean> hasRun = new Pointer<>(false);
    mock.schedule(new SafeTimerTask() {
      @Override
      public void runUnsafe() {
        hasRun.set(true);
      }
    }, 1000);

    assertEquals(false, hasRun.dereference().get());
    SafeTimerMock.advanceTime(999);
    assertEquals(false, hasRun.dereference().get());
    SafeTimerMock.advanceTime(1);
    assertEquals(1000, mock.now());
    assertEquals(true, hasRun.dereference().get());
  }

  @Test
  public void scheduleImmediate() {
    SafeTimerMock mock = new SafeTimerMock();
    Pointer<Boolean> hasRun = new Pointer<>(false);
    mock.schedule(new SafeTimerTask() {
      @Override
      public void runUnsafe() {
        hasRun.set(true);
      }
    }, 0);

    assertEquals(false, hasRun.dereference().get());
    SafeTimerMock.advanceTime(1);
    assertEquals(1, mock.now());
    assertEquals(true, hasRun.dereference().get());
  }

  @Test
  public void scheduleWithPeriod() {
    SafeTimerMock mock = new SafeTimerMock();
    int[] runCount = new int[]{0};
    SafeTimerTask timerTask = new SafeTimerTask() {
      @Override
      public void runUnsafe() {
        runCount[0] += 1;
      }
    };
    mock.schedule(timerTask, 1000, 1000);

    assertEquals(0, runCount[0]);
    SafeTimerMock.advanceTime(999);
    assertEquals(0, runCount[0]);
    SafeTimerMock.advanceTime(1);
    assertEquals(1000, mock.now());
    assertEquals(1, runCount[0]);
    SafeTimerMock.advanceTime(500);
    assertEquals(1, runCount[0]);
    SafeTimerMock.advanceTime(500);
    assertEquals(2, runCount[0]);
    SafeTimerMock.advanceTime(1000);
    assertEquals(3, runCount[0]);
    SafeTimerMock.advanceTime(1900);
    assertEquals(4, runCount[0]);
    SafeTimerMock.advanceTime(100);
    assertEquals(5, runCount[0]);

    timerTask.cancel();
    SafeTimerMock.advanceTime(10000);
    assertEquals(5, runCount[0]);
  }

  @Test
  public void cancel() {
    SafeTimerMock mock = new SafeTimerMock();
    int[] runCount = new int[]{0};
    SafeTimerTask timerTask = new SafeTimerTask() {
      @Override
      public void runUnsafe() {
        runCount[0] += 1;
      }
    };
    mock.schedule(timerTask, 1000, 1000);

    SafeTimerMock.advanceTime(10000);
    assertEquals(10, runCount[0]);
    mock.cancel();
    SafeTimerMock.advanceTime(10000);
    assertEquals(10, runCount[0]);
  }

  @Test
  public void numTasksScheduled() {
    SafeTimerMock mock = new SafeTimerMock();
    assertEquals(0, mock.numTasksScheduled());

    // Add a recurring task
    int[] runCount = new int[]{0};
    SafeTimerTask recurringTimerTask = new SafeTimerTask() {
      @Override
      public void runUnsafe() {
        runCount[0] += 1;
      }
    };
    mock.schedule(recurringTimerTask, 1000, 1000);
    assertEquals(1, mock.numTasksScheduled());

    // Add a one-time task
    mock.schedule(new SafeTimerTask() {
      @Override
      public void runUnsafe() {
        // do nothing
      }
    }, 1000);
    assertEquals(2, mock.numTasksScheduled());
    SafeTimerMock.advanceTime(10000);
    assertEquals(1, mock.numTasksScheduled());
    SafeTimerMock.advanceTime(10000);
    assertEquals(1, mock.numTasksScheduled());
    recurringTimerTask.cancel();
    assertEquals(0, mock.numTasksScheduled());
  }

  @Test
  public void waitForSilence() {
    SafeTimerMock mock = new SafeTimerMock();
    assertEquals(0, mock.numTasksScheduled());

    int[] runCount = new int[]{0};
    SafeTimerTask task = new SafeTimerTask() {
      @Override
      public void runUnsafe() {
        runCount[0] += 1;
      }
    };
    mock.schedule(task, 1000);
    assertEquals(1, mock.numTasksScheduled());

    CompletableFuture<Boolean> gotSilence = new CompletableFuture<>();
    new Thread(() -> {
      mock.waitForSilence();
      gotSilence.complete(true);
    }).start();

    // Let the thread start and block
    Uninterruptably.sleep(100);

    assertFalse(gotSilence.isDone());
    SafeTimerMock.advanceTime(1000);

    // Give plenty of time for the other thread to run
    Uninterruptably.sleep(500);

    assertTrue(gotSilence.isDone());
  }
}