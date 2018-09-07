package ai.eloquent.raft;

import java.util.Objects;

/**
 * A little helper class for a log entry location for a Raft log.
 *
 * @author <a href="mailto:gabor@eloquent.ai">Gabor Angeli</a>
 */
public class RaftLogEntryLocation {

  /** The term of this log entry. */
  public final long term;

  /**
   * The index of this log entry.
   * This is the global index of the entry, although it's tagged with a given term.
   */
  public final long index;

  /**
   * The straightforward constructor.
   */
  public RaftLogEntryLocation(long index, long term) {
    this.term = term;
    this.index = index;
  }


  /** {@inheritDoc} */
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    RaftLogEntryLocation that = (RaftLogEntryLocation) o;
    return term == that.term &&
        index == that.index;
  }


  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return Objects.hash(term, index);
  }


  /** {@inheritDoc} */
  @Override
  public String toString() {
    return index + "@" + term;
  }
}
