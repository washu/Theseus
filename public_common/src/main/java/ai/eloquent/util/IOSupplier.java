package ai.eloquent.util;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * A supplier which can throw an IOexception.
 *
 * @author <a href="mailto:gabor@eloquent.ai">Gabor Angeli</a>
 */
@FunctionalInterface
public interface IOSupplier<E> {

  /**
   * @see Supplier#get()
   */
  E get() throws IOException;
}
