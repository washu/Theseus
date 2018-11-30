package ai.eloquent.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * A stack trace, with the appropriate formatting and {@link Object#equals(Object)} and {@link Object#hashCode()}
 * defined so that we can, e.g., store them in a set.
 *
 * @author <a href="mailto:gabor@eloquent.ai">Gabor Angeli</a>
 */
public class StackTrace {
  /**
   * An SLF4J Logger for this class.
   */
  private static final Logger log = LoggerFactory.getLogger(StackTrace.class);


  /**
   * The actual stack trace.
   */
  private final List<StackTraceElement> trace;

  /**
   * A rendered representation of the stack trace.
   */
  private final String repr;


  /**
   * Create a stack trace from the current trace.
   */
  public StackTrace() {
    this(Thread.currentThread().getStackTrace());
  }

  /**
   * Create a stack trace from a given trace array.
   *
   * @param trace The stack trace we're representing
   */
  public StackTrace(StackTraceElement[] trace) {
    this(Arrays.asList(trace));
  }

  /**
   * Create a stack trace from an exception
   *
   * @param t The exception to create the stack trace from.
   */
  public StackTrace(Throwable t) {
    this(t.getStackTrace());
  }


  /**
   * Create a stack trace from a given trace array.
   *
   * @param trace The stack trace we're representing
   */
  public StackTrace(List<StackTraceElement> trace) {
    this.trace = trace;
    StringBuilder b = new StringBuilder();
    for (StackTraceElement elem : trace) {
      if (elem.getClassName().equals(StackTrace.class.getName())) {
        continue;  // ignore this class
      }
      if (elem.getClassName().equals(Thread.class.getName())) {
        continue;  // ignore Thread class
      }
      b.append(elem.getClassName()
          .replaceAll("^ai.eloquent.", "")
          .replaceAll("^java.lang.", "")
          .replaceAll("^java.util.", "j.u.")
          .replaceAll("^org.eclipse.jetty.", "jetty.")
      ).append('.').append(elem.getMethodName());
      if (elem.getLineNumber() >= 0) {
        b.append(" @ ").append(elem.getLineNumber());
      } else {
        b.append(" @ ???");
      }
      b.append("\n");
    }
    this.repr = b.toString().trim();
  }


  /** {@inheritDoc} */
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    StackTrace that = (StackTrace) o;
    return Objects.equals(repr, that.repr);
  }


  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return Objects.hash(repr);
  }


  /** {@inheritDoc} */
  @Override
  public String toString() {
    return this.repr;
  }
}
