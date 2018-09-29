package ai.eloquent.util;

/**
 * A {@link Runnable} that can throw an exception.
 *
 * @author <a href="mailto:gabor@eloquent.ai">Gabor Angeli</a>
 */
@FunctionalInterface
public interface RunnableThrowsException {
  /** @see Runnable#run() */
  void run() throws Exception;

}
