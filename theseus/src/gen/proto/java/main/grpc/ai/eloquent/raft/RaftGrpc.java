package ai.eloquent.raft;

import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ClientCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ClientCalls.asyncClientStreamingCall;
import static io.grpc.stub.ClientCalls.asyncServerStreamingCall;
import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.blockingServerStreamingCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ServerCalls.asyncClientStreamingCall;
import static io.grpc.stub.ServerCalls.asyncServerStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.9.0)",
    comments = "Source: EloquentRaft.proto")
public final class RaftGrpc {

  private RaftGrpc() {}

  public static final String SERVICE_NAME = "ai.eloquent.raft.Raft";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  @java.lang.Deprecated // Use {@link #getRpcMethod()} instead. 
  public static final io.grpc.MethodDescriptor<ai.eloquent.raft.EloquentRaftProto.RaftMessage,
      ai.eloquent.raft.EloquentRaftProto.RaftMessage> METHOD_RPC = getRpcMethod();

  private static volatile io.grpc.MethodDescriptor<ai.eloquent.raft.EloquentRaftProto.RaftMessage,
      ai.eloquent.raft.EloquentRaftProto.RaftMessage> getRpcMethod;

  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static io.grpc.MethodDescriptor<ai.eloquent.raft.EloquentRaftProto.RaftMessage,
      ai.eloquent.raft.EloquentRaftProto.RaftMessage> getRpcMethod() {
    io.grpc.MethodDescriptor<ai.eloquent.raft.EloquentRaftProto.RaftMessage, ai.eloquent.raft.EloquentRaftProto.RaftMessage> getRpcMethod;
    if ((getRpcMethod = RaftGrpc.getRpcMethod) == null) {
      synchronized (RaftGrpc.class) {
        if ((getRpcMethod = RaftGrpc.getRpcMethod) == null) {
          RaftGrpc.getRpcMethod = getRpcMethod = 
              io.grpc.MethodDescriptor.<ai.eloquent.raft.EloquentRaftProto.RaftMessage, ai.eloquent.raft.EloquentRaftProto.RaftMessage>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "ai.eloquent.raft.Raft", "rpc"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  ai.eloquent.raft.EloquentRaftProto.RaftMessage.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  ai.eloquent.raft.EloquentRaftProto.RaftMessage.getDefaultInstance()))
                  .setSchemaDescriptor(new RaftMethodDescriptorSupplier("rpc"))
                  .build();
          }
        }
     }
     return getRpcMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static RaftStub newStub(io.grpc.Channel channel) {
    return new RaftStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static RaftBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new RaftBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static RaftFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new RaftFutureStub(channel);
  }

  /**
   */
  public static abstract class RaftImplBase implements io.grpc.BindableService {

    /**
     */
    public void rpc(ai.eloquent.raft.EloquentRaftProto.RaftMessage request,
        io.grpc.stub.StreamObserver<ai.eloquent.raft.EloquentRaftProto.RaftMessage> responseObserver) {
      asyncUnimplementedUnaryCall(getRpcMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getRpcMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                ai.eloquent.raft.EloquentRaftProto.RaftMessage,
                ai.eloquent.raft.EloquentRaftProto.RaftMessage>(
                  this, METHODID_RPC)))
          .build();
    }
  }

  /**
   */
  public static final class RaftStub extends io.grpc.stub.AbstractStub<RaftStub> {
    private RaftStub(io.grpc.Channel channel) {
      super(channel);
    }

    private RaftStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RaftStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new RaftStub(channel, callOptions);
    }

    /**
     */
    public void rpc(ai.eloquent.raft.EloquentRaftProto.RaftMessage request,
        io.grpc.stub.StreamObserver<ai.eloquent.raft.EloquentRaftProto.RaftMessage> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getRpcMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class RaftBlockingStub extends io.grpc.stub.AbstractStub<RaftBlockingStub> {
    private RaftBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private RaftBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RaftBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new RaftBlockingStub(channel, callOptions);
    }

    /**
     */
    public ai.eloquent.raft.EloquentRaftProto.RaftMessage rpc(ai.eloquent.raft.EloquentRaftProto.RaftMessage request) {
      return blockingUnaryCall(
          getChannel(), getRpcMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class RaftFutureStub extends io.grpc.stub.AbstractStub<RaftFutureStub> {
    private RaftFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private RaftFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RaftFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new RaftFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<ai.eloquent.raft.EloquentRaftProto.RaftMessage> rpc(
        ai.eloquent.raft.EloquentRaftProto.RaftMessage request) {
      return futureUnaryCall(
          getChannel().newCall(getRpcMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_RPC = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final RaftImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(RaftImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_RPC:
          serviceImpl.rpc((ai.eloquent.raft.EloquentRaftProto.RaftMessage) request,
              (io.grpc.stub.StreamObserver<ai.eloquent.raft.EloquentRaftProto.RaftMessage>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class RaftBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    RaftBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return ai.eloquent.raft.EloquentRaftProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("Raft");
    }
  }

  private static final class RaftFileDescriptorSupplier
      extends RaftBaseDescriptorSupplier {
    RaftFileDescriptorSupplier() {}
  }

  private static final class RaftMethodDescriptorSupplier
      extends RaftBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    RaftMethodDescriptorSupplier(String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (RaftGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new RaftFileDescriptorSupplier())
              .addMethod(getRpcMethod())
              .build();
        }
      }
    }
    return result;
  }
}
