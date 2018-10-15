package ai.eloquent.error;

/**
 * An error listener, for when something goes wrong in Raft.
 */
@FunctionalInterface
public interface RaftErrorListener {
  /**
   * Register than an error has occurred in Raft.
   *
   * @param incidentKey A unique key for the incident type.
   * @param debugMessage A more detailed debug message for the incident.
   * @param stackTrace The stack trace of the incident.
   */
  void accept(String incidentKey, String debugMessage, StackTraceElement[] stackTrace);
}