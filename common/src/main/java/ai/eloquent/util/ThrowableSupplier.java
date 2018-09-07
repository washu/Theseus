package ai.eloquent.util;

import java.util.function.Supplier;

/**
 * A supplier which can throw an exception.
 *
 * @author <a href="mailto:gabor@eloquent.ai">Gabor Angeli</a>
 */
@FunctionalInterface
public interface ThrowableSupplier<E> {

  /**
   * @see Supplier#get()
   */
  E get() throws Throwable;

}
