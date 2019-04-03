package ai.eloquent.util;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Optional;

/**
 * A collection of methods for interacting with the underlying OS
 * @author <a href="mailto:zames@eloquent.ai">Zames Chua</a>
 */
public class SystemUtils {

  /**
   * The address of this local machine
   */
  public static final String LOCAL_HOST_ADDRESS;

  static {
    String host;
    try {
      host = InetAddress.getLocalHost().toString();
    } catch (UnknownHostException e) {
      host = Optional.ofNullable(System.getenv("HOST")).orElse("unknown_host");
    }
    if (host.contains("/")) {
      host = host.substring(0, host.indexOf('/'));
    }
    LOCAL_HOST_ADDRESS = host;
  }

}