package ai.eloquent.raft;

import ai.eloquent.data.UDPTransport;
import ai.eloquent.util.IOSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.time.Duration;
import java.util.Collections;


/**
 * This failsafe checks for the following very specific condition:
 *
 * - 1. We believe we're the leader
 * - 2. There are other members in our cluster config
 * - 3. It has been more than 30 seconds since we got messages from any other cluster member
 * - 4. We can confirm with Kubernetes that none of the other cluster members we think we have are still alive
 *
 * In that case, we go ahead and commit the change to the cluster that all the other cluster members are dead, so we
 * can proceed.
 */
public class UnsatisfiableQuorumFailsafe implements RaftFailsafe {
  /**
   * An SLF4J Logger for this class.
   */
  private static final Logger log = LoggerFactory.getLogger(UnsatisfiableQuorumFailsafe.class);

  /**
   * The method for getting our ground truth cluster state.
   */
  private final IOSupplier<String[]> getClusterState;

  /**
   * The timeout, in milliseconds, after which the failsafe should trigger.
   * A sensible default if you're using {@link EloquentRaftAlgorithm} is
   * {@link EloquentRaftAlgorithm#MACHINE_DOWN_TIMEOUT} + 15s
   */
  public final long timeout;


  /**
   * Create a new failsafe from the default Kubernetes state in {@link UDPTransport#readKubernetesState()}.
   */
  public UnsatisfiableQuorumFailsafe(Duration timeout) {
    this(() -> {
      InetAddress[] addresses = UDPTransport.readKubernetesState();
      String[] names = new String[addresses.length];
      for (int i = 0; i < addresses.length; i++) {
        names[i] = addresses[i].getHostAddress();
      }
      return names;
    }, timeout);
  }


  /**
   * Create a new failsafe from the given cluster state provider.
   */
  public UnsatisfiableQuorumFailsafe(IOSupplier<String[]> getClusterState, Duration timeout) {
    this.getClusterState = getClusterState;
    this.timeout = timeout.toMillis();
  }


  /** {@inheritDoc} */
  @Override
  public void heartbeat(RaftAlgorithm algorithm, long now) {
    RaftState state = algorithm.mutableState();
    // 1. We believe we're the leader (or a candidate to become one)
    if (state.isLeader()) {
      state.lastMessageTimestamp.ifPresent(lastMessageTimestamp -> {
        // 2. There are other members in our cluster config
        if (lastMessageTimestamp.isEmpty()) {
          return;
        }

        // 3. It has been too long since we got messages from any other cluster member
        for (String member : lastMessageTimestamp.keySet()) {
          long timeSinceHeard = now - lastMessageTimestamp.get(member);
          if (timeSinceHeard <= this.timeout) {
            return;
          }
        }

        // 4. We can confirm with Kubernetes that none of the other cluster members we think we have are still alive
        try {
          String[] cluster = this.getClusterState.get();
          for (String member : lastMessageTimestamp.keySet()) {
            if (!member.equalsIgnoreCase(algorithm.serverName())) {
              for (String hostname : cluster) {
                if (member.equals(hostname)) {
                  return;
                }
              }
            }
          }
        } catch (IOException e) {
          log.warn("Could not get Kubernetes state -- ignoring failsafe! ", e);
          return;  // don't reconfigure on Kubernetes glitches
        }

        // 5. If we reach here, we're in need of triggering the failsafe
        log.warn("UNSATISFIABLE QUORUM FAILSAFE TRIGGERED! This means we detected a deadlock or split brain in Raft, where all the other " +
            "boxes that are part of the cluster are now dead. This is obviously bad, and should never happen. We're now " +
            "going to attempt to recover automatically, but the root cause should be investigated (probably that boxes are " +
            "not respecting their shutdown locks). Good luck.");

        // 6. Immediately reconfigure to just us alone in a cluster
        state.reconfigure(Collections.singleton(algorithm.serverName()), true, now);
      });
    }
  }
}
