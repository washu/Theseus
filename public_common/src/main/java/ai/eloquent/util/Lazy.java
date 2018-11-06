package ai.eloquent.util;

import java.lang.ref.SoftReference;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * An instantiation of a lazy object.
 *
 * @author <a href="mailto:gabor@eloquent.ai">Gabor Angeli</a>
 */
public abstract class Lazy<E> {
  /** If this lazy should cache, this is the cached value. */
  private SoftReference<E> implOrNullCache = null;
  /** If this lazy should not cache, this is the computed value */
  private E implOrNull = null;


  /** For testing only: simulate a GC event. */
  void simulateGC() {
    if (implOrNullCache != null) {
      implOrNullCache.clear();
    }
  }

  /**
   * Get the value of this {@link Lazy}, computing it if necessary.
   */
  public synchronized E get() {
    E orNull = getIfDefined();
    if (orNull == null) {
      orNull = compute();
      if (isCache()) {
        implOrNullCache = new SoftReference<>(orNull);
      } else {
        implOrNull = orNull;
      }
    }
    assert orNull != null;
    return orNull;
  }


  /**
   * Compute the value of this lazy.
   */
  protected abstract E compute();


  /**
   * Specify whether this lazy should garbage collect its value if needed,
   * or whether it should force it to be persistent.
   */
  public abstract boolean isCache();

  /**
   * Get the value of this {@link Lazy} if it's been initialized, or else
   * return null.
   */
  public E getIfDefined() {
    if (implOrNullCache != null) {
      assert implOrNull == null;
      return implOrNullCache.get();
    } else {
      return implOrNull;
    }
  }


  /**
   * Keep this value a Lazy, but when it is gotten apply the given function
   * to it.
   * This makes this a proper monad.
   *
   * @param fn The mapper function.
   * @param <F> The type of Lazy we're returning
   *
   * @return The mapped lazy.
   */
  public <F> Lazy<F> map(Function<E, F> fn) {
    return new Lazy<F>() {
      @Override
      protected F compute() {
        return fn.apply(Lazy.this.compute());
      }

      @Override
      public boolean isCache() {
        return Lazy.this.isCache();
      }
    };
  }


  /**
   * Create a degenerate {@link Lazy}, which simply returns the given pre-computed
   * value.
   */
  public static <E> Lazy<E> from(final E definedElement) {
    Lazy<E> rtn = new Lazy<E>() {
      @Override
      protected E compute() {
        return definedElement;
      }

      @Override
      public boolean isCache() {
        return false;
      }
    };
    rtn.implOrNull = definedElement;
    return rtn;
  }


  /**
   * Create a lazy value from the given provider.
   * The provider is only called once on initialization.
   */
  public static <E> Lazy<E> of(Supplier<E> fn) {
    return new Lazy<E>() {
      @Override
      protected E compute() {
        return fn.get();
      }

      @Override
      public boolean isCache() {
        return false;
      }
    };
  }


  /**
   * Create a lazy value from the given provider, allowing the value
   * stored in the lazy to be garbage collected if necessary.
   * The value is then re-created by when needed again.
   */
  public static <E> Lazy<E> cache(Supplier<E> fn) {
    return new Lazy<E>() {
      @Override
      protected E compute() {
        return fn.get();
      }

      @Override
      public boolean isCache() {
        return true;
      }
    };
  }
}