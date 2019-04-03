package ai.eloquent.util;

/**
 * An unchecked variant of {@link java.io.IOException}
 *
 * @author <a href="mailto:gabor@eloquent.ai">Gabor Angeli</a>
 */
public class RuntimeIOException extends RuntimeException {

  /**
   * The default constructor
   */
  public RuntimeIOException() {
  }

  /**
   * Creates a new exception with an error message and the specified cause.
   *
   * @param message the message for the exception
   * @param cause   The cause for the exception
   */
  public RuntimeIOException(String message, Throwable cause) {
    super(message, cause);
  }


  /**
   * Creates a new exception with an error message
   *
   * @param message the error message
   */
  public RuntimeIOException(String message) {
    super(message);
  }


  /**
   * Creates a new exception with the specified cause.
   *
   * @param cause The cause for the exception
   */
  public RuntimeIOException(Throwable cause) {
    super(cause);
  }

}
