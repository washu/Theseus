package ai.eloquent.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A special case of {@link DeferredLazy} where we only ever compute the value once.
 *
 * @author <a href="mailto:gabor@eloquent.ai">Gabor Angeli</a>
 */
public abstract class Lazy<E> implements Supplier<E> {
  /**
   * An SLF4J Logger for this class.
   */
  private static final Logger log = LoggerFactory.getLogger(Lazy.class);

  /**
   * Since {@link DeferredLazy} is a more general case of Lazy, we use it as our Lazy class.
   */
  private final DeferredLazy<E> impl = new DeferredLazy<E>() {
    @Override
    protected boolean isValid(E value, long lastComputeTime, long callTime) {
      return true;
    }
    @Override
    protected E compute() throws Exception {
      return Lazy.this.compute();
    }
  };


  /** @see ai.eloquent.util.DeferredLazy#compute()  */
  protected abstract E compute() throws Exception;

  /**
   * Get our value, computing it if necessary.
   *
   * @return The value we computed.
   */
  public E get() {
    return impl.getSync();
  }


  /**
   * Lazy is a monad! Let's implement a map function :)
   *
   * @param mapper The mapping function
   * @param <F> The output type of the resulting Lazy.
   *
   * @return The mapped lazy. This will compute the value for this lazy, map it with the mapper, and store that value.
   */
  public <F> Lazy<F> map(Function<E, F> mapper) {
    return new Lazy<F>() {
      @Override
      protected F compute() {
        return mapper.apply(Lazy.this.get());
      }
    };
  }


  /**
   * Get our value only if it has already been computed.
   *
   * @return Our value if computed, or else {@link Optional#empty()}.
   */
  public Optional<E> getIfPresent() {
    if (impl.isValid(0L)) {
      return Optional.of(impl.getSync());
    } else {
      return Optional.empty();
    }
  }


  /**
   * Create a lazy from a given value (i.e., not a real lazy, just a value).
   */
  public static <X> Lazy<X> ofValue(X value) {
    Lazy<X> rtn = new Lazy<X>() {
      @Override
      protected X compute() {
        return value;
      }
    };
    rtn.get();  // need to explicitly cache it -- it should start life computed
    return rtn;
  }


  /**
   * Create a lazy from a given function.
   */
  public static <X> Lazy<X> ofSupplier(Supplier<X> value) {
    return new Lazy<X>() {
      @Override
      protected X compute() {
        return value.get();
      }
    };
  }

}
