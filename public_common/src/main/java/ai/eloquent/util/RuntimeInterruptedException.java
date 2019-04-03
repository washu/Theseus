package ai.eloquent.util;

/**
 * An unchecked variant of {@link java.lang.InterruptedException}.
 *
 * @author <a href="mailto:gabor@eloquent.ai">Gabor Angeli</a>
 */
public class RuntimeInterruptedException extends RuntimeException {

  /**
   * The default constructor
   */
  public RuntimeInterruptedException() {
    super();
  }

  /**
   * Create a {@link RuntimeInterruptedException} that wraps around the original InterruptedException
   * @param e the original InterruptedException
   */
  public RuntimeInterruptedException(InterruptedException e) {
    super(e);
  }
}
