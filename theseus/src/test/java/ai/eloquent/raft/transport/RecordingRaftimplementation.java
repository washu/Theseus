package ai.eloquent.raft.transport;

import ai.eloquent.raft.*;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * A dummy raft implementation, that just records API calls made.
 *
 * @author <a href="mailto:gabor@eloquent.ai">Gabor Angeli</a>
 */
public class RecordingRaftimplementation implements RaftAlgorithm {
  /**
   * An SLF4J Logger for this class.
   */
  private static final Logger log = LoggerFactory.getLogger(RecordingRaftimplementation.class);

  public final List<EloquentRaftProto.InstallSnapshotRequest> installSnapshotsReceived = new ArrayList<>();

  public final List<EloquentRaftProto.InstallSnapshotReply> installSnapshotReplies = new ArrayList<>();

  public final List<EloquentRaftProto.AppendEntriesRequest> appendEntriesReceived = new ArrayList<>();

  public final List<EloquentRaftProto.AppendEntriesReply> appendEntriesReplies = new ArrayList<>();

  public final List<EloquentRaftProto.RequestVoteRequest> requestVotesReceived = new ArrayList<>();

  public final List<EloquentRaftProto.RequestVoteReply> requestVoteResplies = new ArrayList<>();

  public final List<EloquentRaftProto.AddServerRequest> addServersReceived = new ArrayList<>();

  public final List<EloquentRaftProto.RemoveServerRequest> removeServersReceived = new ArrayList<>();

  private final RaftState state;

  public RecordingRaftimplementation(String serverName) {
    this.state = new RaftState(serverName, new KeyValueStateMachine(serverName), MoreExecutors.newDirectExecutorService());
    this.state.elect(0L);
  }


  @Override
  public RaftState state() {
    return state.copy();
  }

  @Override
  public RaftState mutableState() {
    return state;
  }

  @Override
  public RaftStateMachine mutableStateMachine() {
    return state.log.stateMachine;
  }

  @Override
  public long term() {
    return -1L;
  }

  @Override
  public String serverName() {
    return state.serverName;
  }

  @Override
  public void receiveInstallSnapshotRPC(EloquentRaftProto.InstallSnapshotRequest snapshot, Consumer<EloquentRaftProto.RaftMessage> replyLeader, long now) {
    installSnapshotsReceived.add(snapshot);
  }

  @Override
  public void receiveInstallSnapshotReply(EloquentRaftProto.InstallSnapshotReply reply, long now) {
    installSnapshotReplies.add(reply);
  }

  @Override
  public void receiveAppendEntriesRPC(EloquentRaftProto.AppendEntriesRequest heartbeat, Consumer<EloquentRaftProto.RaftMessage> replyLeader, long now) {
    appendEntriesReceived.add(heartbeat);
  }

  @Override
  public void receiveAppendEntriesReply(EloquentRaftProto.AppendEntriesReply reply, long now) {
    appendEntriesReplies.add(reply);
  }

  @Override
  public void receiveRequestVotesReply(EloquentRaftProto.RequestVoteReply reply, long now) {
    requestVoteResplies.add(reply);
  }

  @Override
  public CompletableFuture<EloquentRaftProto.RaftMessage> receiveAddServerRPC(EloquentRaftProto.AddServerRequest addServerRequest, long now) {
    addServersReceived.add(addServerRequest);
    return CompletableFuture.completedFuture(EloquentRaftProto.RaftMessage.newBuilder()
        .setSender(this.serverName())
        .setAddServerReply(EloquentRaftProto.AddServerReply.newBuilder()
            .setStatus(EloquentRaftProto.MembershipChangeStatus.OK)
            .build())
        .build());
  }

  @Override
  public CompletableFuture<EloquentRaftProto.RaftMessage> receiveRemoveServerRPC(EloquentRaftProto.RemoveServerRequest removeServerRequest, long now) {
    removeServersReceived.add(removeServerRequest);
    return CompletableFuture.completedFuture(EloquentRaftProto.RaftMessage.newBuilder()
        .setSender(this.serverName())
        .setRemoveServerReply(EloquentRaftProto.RemoveServerReply.newBuilder()
            .setStatus(EloquentRaftProto.MembershipChangeStatus.OK)
            .build())
        .build());
  }

  @Override
  public void broadcastAppendEntries(long now) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void sendAppendEntries(String target, long nextIndex) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void receiveRequestVoteRPC(EloquentRaftProto.RequestVoteRequest voteRequest, Consumer<EloquentRaftProto.RaftMessage> replyLeader, long now) {
    requestVotesReceived.add(voteRequest);

  }

  @Override
  public void triggerElection(long now) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public CompletableFuture<EloquentRaftProto.RaftMessage> receiveApplyTransitionRPC(EloquentRaftProto.ApplyTransitionRequest transition, long now) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public boolean bootstrap(boolean force) {
    return true;
  }

  @Override
  public void receiveBadRequest(EloquentRaftProto.RaftMessage message) {
    log.info("Ignoring Raft message: {}", message);
  }

  @Override
  public Optional<RaftLifecycle> lifecycle() {
    return Optional.empty();
  }

  @Override
  public RaftTransport getTransport() {
    return null;
  }

  @Override
  public void stop(boolean kill) {
    // empty
  }

  @Override
  public boolean isRunning() {
    return true;
  }

  @Override
  public void heartbeat(long now) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    RecordingRaftimplementation that = (RecordingRaftimplementation) o;
    return Objects.equals(state, that.state);
  }

  @Override
  public int hashCode() {

    return Objects.hash(state);
  }
}
