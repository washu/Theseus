package ai.eloquent.error;

@FunctionalInterface
/**
 * Implements TriConsumer
 */
public interface RaftErrorListener {
  void accept(String incidentKey, String debugMessage, StackTraceElement[] stackTrace);
}