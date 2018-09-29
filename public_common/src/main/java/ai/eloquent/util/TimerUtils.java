package ai.eloquent.util;

import java.time.Instant;

/**
 * Utilities for working with time.
 * God I hate time.
 *
 * @author Gabor Angeli
 */
public class TimerUtils {

  /**
   * UNIT TESTS ONLY.
   * Setting this to true will set the time to Thu Nov 02 2017 15:50:00 PT
   */
  public static boolean mockNow = false;


  /**
   * A wrapper for {@link Instant#now()} that allows for mocking a particular time.
   */
  public static Instant mockableNow() {
    if (mockNow) {
      return Instant.ofEpochMilli(1509651000000L);
    } else {
      return Instant.now();
    }
  }

  /**
   * Utility method for formatting a time difference (maybe this should go to a util class?)
   * @param diff Time difference in milliseconds
   * @param b The string builder to append to
   */
  public static void formatTimeDifference(long diff, StringBuilder b){
    //--Short circuit for < 100ms
    if (diff < 100) {
      b.append(diff).append(" ms");
      return;
    }

    //--Get Values
    int mili = (int) diff % 1000;
    long rest = diff / 1000;
    int sec = (int) rest % 60;
    rest = rest / 60;
    int min = (int) rest % 60;
    rest = rest / 60;
    int hr = (int) rest % 24;
    rest = rest / 24;
    int day = (int) rest;
    //--Make String
    if(day > 0) b.append(day).append(day > 1 ? " days, " : " day, ");
    if(hr > 0) b.append(hr).append(hr > 1 ? " hours, " : " hour, ");
    if(min > 0) {
      if(min < 10){ b.append("0"); }
      b.append(min).append(":");
    }
    if(min > 0 && sec < 10){ b.append("0"); }
    if (day == 0 && hr == 0 && min == 0 && sec < 30) {
      b.append(sec).append(".");
      if (mili >= 100) {
        b.append(mili);
      } else if (mili >= 10) {
        b.append("0").append(mili);
      } else {
        b.append("00").append(mili);
      }
    } else {
      b.append(sec);
    }
    if(min > 0) b.append(" minutes");
    else b.append(" seconds");
  }


  /**
   * Format a time difference (duration) in a human-readable format.
   *
   * @param diff The difference in milliseconds between two timestamps.
   *
   * @return A human-readable debug string of this time difference.
   */
  public static String formatTimeDifference(long diff){
    StringBuilder b = new StringBuilder();
    formatTimeDifference(diff, b);
    return b.toString();
  }


  /**
   * Format the amount of time that has elapsed since the argument time.
   *
   * @param startTime The time we started measuring from.
   *
   * @return A human-readable debug string of the elapsed time.
   */
  public static String formatTimeSince(long startTime){
    return formatTimeDifference(System.currentTimeMillis() - startTime);
  }

}
