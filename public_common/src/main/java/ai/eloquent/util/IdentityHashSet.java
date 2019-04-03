package ai.eloquent.util;

import java.util.*;

/**
 *  This class provides a <code>IdentityHashMap</code>-backed
 *  implementation of the <code>Set</code> interface.
 *
 *  This means that
 *  whether an object is an element of the set depends on whether it is ==
 *  (rather than <code>equals()</code>) to an element of the set.  This is
 *  different from a normal <code>HashSet</code>, where set membership
 *  depends on <code>equals()</code>, rather than ==.
 *
 *  @author <a href="mailto:zames@eloquent.ai">Zames Chua</a>
 */
public class IdentityHashSet<E> extends AbstractSet<E> {

  /**
   * the Set wrapper around IdentityHashMap which backs this set
   */
  private transient Set<E> set;


  /**
   * Construct a new, empty IdentityHashSet with the default expected maximum size of 21;
   */
  public IdentityHashSet() {
    set = Collections.newSetFromMap(new IdentityHashMap<>());
  }


  /**
   * Construct a new, empty IdentityHashSet with the specified expected maximum size.
   *
   * @param expectedMaxSize the expected maximum size of the set.
   */
  public IdentityHashSet(int expectedMaxSize) {
    set = Collections.newSetFromMap(new IdentityHashMap<>(expectedMaxSize));
  }


  /**
   * Construct a new IdentityHashSet containing the elements of the supplied Collection,
   * with the default expected maximum size of 21.
   *
   * @param c a Collection containing the elements with which this set will
   *          be initialized.
   */
  public IdentityHashSet(Collection<? extends E> c) {
    set = Collections.newSetFromMap(new IdentityHashMap<>());
    addAll(c);
  }


  /**
   * Adds the specified element to this set if it is not already present.
   *
   * @param       o             element to add to this set
   * @return      true          if the element was added,
   *              false         otherwise
   */
  @Override
  public boolean add(E o) {
    return set.add(o);
  }


  /**
   * Removes all of the elements from this set.
   */
  @Override
  public void clear() {
    set.clear();
  }


  /**
   * Returns a shallow copy of this <code>IdentityHashSet</code> instance --
   * the elements themselves are not cloned.
   *
   * @return a shallow copy of this set.
   */
  @Override
  public Object clone() {
    Iterator<E> it = iterator();
    IdentityHashSet<E> clone = new IdentityHashSet<>(size() * 2);
    while (it.hasNext()) {
      clone.add(it.next());
    }
    return clone;
  }


  /**
   * Returns true if this set contains the specified element.
   *
   * This set implementation uses == (not <code>equals()</code>) to test whether an element is present in the set.
   *
   * @param o Element whose presence in this set is to be tested.
   *
   * @return <code>true</code> if this set contains the specified element.
   */
  @Override
  public boolean contains(Object o) {
    return set.contains(o);
  }


  /**
   *  @return <code>true</code> if this set contains no elements.
   */
  @Override
  public boolean isEmpty() {
    return set.isEmpty();
  }


  /**
   * Returns an iterator over the elements in this set. The elements are
   * returned in no particular order.
   *
   * @return an <code>Iterator</code> over the elements in this set.
   */
  @Override
  public Iterator<E> iterator() {
    return set.iterator();
  }


  /**
   * Removes the specified element from this set if it is present.
   *
   * Remember that this set implementation uses == (not <code>equals()</code>)
   * to test whether an element is present in the set.
   *
   * @param o Object to be removed from this set, if present.
   *
   * @return <code>true</code> if the set contained the specified element.
   */
  @Override
  public boolean remove(Object o) {
    return (set.remove(o));
  }


  /**
   *  @return the number of elements in this set (its cardinality).
   */
  @Override
  public int size() {
    return set.size();
  }

}
