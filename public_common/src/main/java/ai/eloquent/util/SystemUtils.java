package ai.eloquent.util;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Optional;

public class SystemUtils {

  /**
   * The hostname of this machine
   */
  public static final String HOST;

  static {
    // Get the hostname
    String host;
    try {
      host = InetAddress.getLocalHost().toString();
    } catch (UnknownHostException e) {
      host = Optional.ofNullable(System.getenv("HOST")).orElse("unknown_host");
    }
    if (host.contains("/")) {
      host = host.substring(0, host.indexOf('/'));
    }
    HOST = host;
  }

}