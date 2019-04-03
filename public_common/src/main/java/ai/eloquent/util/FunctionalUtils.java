package ai.eloquent.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Utility functions for working with monads (e.g., Java's {@link Optional}) or
 * creating monad-style operations.
 *
 * @author <a href="mailto:gabor@eloquent.ai">Gabor Angeli</a>
 */
public class FunctionalUtils {
  /**
   * An SLF4J Logger for this class.
   */
  private static final Logger log = LoggerFactory.getLogger(FunctionalUtils.class);



  /**
   * Return an empty concurrent map.
   *
   * @param <K> The key type of the map.
   * @param <V> The value type of the map.
   *
   * @return The map instance.
   */
  public static <K, V> ConcurrentHashMap<K,V> emptyConcurrentMap() {
    return new ConcurrentHashMap<>(0);
  }


  public static <E> Optional<E> ofThrowable(ThrowableSupplier<E> elem, boolean printErrors) {
    E value = null;
    try {
      value = elem.get();
    } catch (Throwable e) {
      if (printErrors) {
        log.warn("Exception in creating Optional: {}, {}", e.getClass().getSimpleName(), e.getMessage());
      }
    }
    return Optional.ofNullable(value);
  }


  public static <E> Optional<E> ofThrowable(ThrowableSupplier<E> elem) {
    return ofThrowable(elem, false);
  }


  /**
   * Returns either a singleton stream of the value computed by the argument, or an empty stream.
   */
  public static <E> Stream<E> streamOfThrowable(ThrowableSupplier<E> elem) {
    E value = null;
    try {
      value = elem.get();
    } catch (Throwable ignored) { }
    if (value != null) {
      return Stream.of(value);
    } else {
      return Stream.empty();
    }
  }


  /**
   * Like {@link Collections#unmodifiableMap(Map)}, but does not wrap already unmodifiable maps in the unmodifiable map class.
   * This avoids memory bloat and/or stack overflows for deep wrapping of immutable collections (e.g., if
   * a constructor makes something immutable).
   */
  public static <E, F> Map<E, F> immutable(Map<E, F> map) {
    if (map instanceof AbstractMap) {
      return Collections.unmodifiableMap(map);
    } else {
      return map;
    }
  }


  /**
   * Like {@link Collections#unmodifiableList(List)}, but does not wrap already unmodifiable lists in the unmodifiable list class.
   * This avoids memory bloat and/or stack overflows for deep wrapping of immutable collections (e.g., if
   * a constructor makes something immutable).
   */
  public static <E> List<E> immutable(List<E> lst) {
    if (lst instanceof AbstractList) {
      return Collections.unmodifiableList(lst);
    } else {
      return lst;
    }
  }


  /**
   * Like {@link Collections#unmodifiableSet(Set)}, but does not wrap already unmodifiable sets in the unmodifiable set class.
   * This avoids memory bloat and/or stack overflows for deep wrapping of immutable collections (e.g., if
   * a constructor makes something immutable).
   */
  public static <E> Set<E> immutable(Set<E> set) {
    if (set instanceof AbstractSet) {
      return Collections.unmodifiableSet(set);
    } else {
      return set;
    }
  }


  /**
   * I can't believe this isn't in the Java standard library for flat mapping between streams and optionals.
   */
  public static <E> Stream<E> streamFromOptional(Optional<E> opt) {
    return opt.map(Stream::of).orElseGet(Stream::empty);
  }

  public static <K, V> Map<K, V> toMap(Stream<Pair<K, V>> pairs) {
    Map<K, V> ret = new HashMap<>();
    pairs.forEach(pair -> ret.put(pair.first(), pair.second()));
    return ret;
  }
  public static <K, V> Map<K, V> toMap(Collection<Pair<K, V>> pairs) {
    return toMap(pairs.stream());
  }
  public static <K, V> Map<K, V> toMap(List<Pair<K, V>> pairs) {
    return toMap(pairs.stream());
  }

  public static <K, V, T> Map<K, T> map(Map<K, V> map, Function<V, T> valueMapper) {
    return map.entrySet().stream()
        .collect(Collectors.toMap(
            Map.Entry::getKey,
            e -> valueMapper.apply(e.getValue())
        ));
  }

  public static <K, V, T, U> Map<T, U> map(Map<K, V> map, Function<K, T> keyMapper, Function<V, U> valueMapper) {
    return map.entrySet().stream()
        .collect(Collectors.toMap(
            e -> keyMapper.apply(e.getKey()),
            e -> valueMapper.apply(e.getValue())
        ));
  }


  /**
   * Remove any duplicate values in @original while perserving ordering (in-place).
   * A list like 1 2 3 1 3 2 becomes 1 2 3.
   * @param original List with some duplicates.
   * @return original
   */
  public static <T> List<T> deduplicate(List<T> original) {
    HashSet<T> elems = new HashSet<>();
    Iterator<T> it = original.iterator();
    while (it.hasNext()) {
      T val = it.next();
      if (elems.contains(val)) {
        it.remove();
      } else {
        elems.add(val);
      }
    }

    return original;
  }

}
