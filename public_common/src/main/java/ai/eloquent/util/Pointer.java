package ai.eloquent.util;

import java.util.Optional;

/**
 * A pointer to an object, to get around not being able to access non-final
 * variables within an anonymous function.
 *
 * @author <a href="mailto:gabor@eloquent.ai">Gabor Angeli</a>
 */
public class Pointer<T> {
  /**
   * The object that this pointer is pointing at
   */
  private T impl;

  /**
   * Create a pointer that points to nowhere
   */
  public Pointer() {
    this.impl = null;
  }

  /**
   * Create a pointer pointing at the given object.
   *
   * @param impl The object the pointer should point at.
   */
  public Pointer(T impl) {
    this.impl = impl;
  }


  /**
   * Dereference the pointer.
   * If the pointer is pointing somewhere, the {@linkplain Optional optional} will be set.
   * Otherwise, the optional will be {@linkplain Optional#empty() empty}.
   */
  public Optional<T> dereference() {
    return Optional.ofNullable(impl);
  }


  /**
   * Set the pointer.
   *
   * @param impl The value to set the pointer to. If this is null, the pointer is unset.
   */
  public void set(T impl) {
    this.impl = impl;
  }
}
