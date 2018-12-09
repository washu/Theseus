package ai.eloquent.data;

import ai.eloquent.io.IOUtils;
import ai.eloquent.monitoring.Prometheus;
import ai.eloquent.raft.RaftLifecycle;
import ai.eloquent.util.*;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.regex.Pattern;


/**
 * A class (usually instantiated as a singleton) to send and receive messages over UDP.
 *
 * @author <a href="mailto:gabor@eloquent.ai">Gabor Angeli</a>
 */
public class UDPTransport implements Transport {
  /**
   * An SLF4J Logger for this class.
   */
  private static final Logger log = LoggerFactory.getLogger(UDPTransport.class);

  /**
   * A regexp for detecting an IP address.
   */
  private static final Pattern IP_REGEX = Pattern.compile("^(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$");

  /**
   * The port we are listening to UDP messages on.
   */
  private static final int DEFAULT_UDP_LISTEN_PORT = 42888;

  /**
   * The port we are listening to UDP messages on.
   */
  private static final int DEFAULT_TCP_LISTEN_PORT = 42888;

  /**
   * The maximum size of a UDP packet, before we should fault in TCP.
   */
  private static final int MAX_UDP_PACKET_SIZE = 65000;  // actually 65,507 bytes, will be reduced by the MTU of underlying layers

  /**
   * A little helper to prevent spamming errors when the network is down.
   */
  private static boolean networkSeemsDown = false;

  /**
   * The name we should assign ourselves on the transport.
   */
  public final InetAddress serverName;

  /**
   * {@link #serverName}'s {@link InetAddress#getHostAddress()}
   */
  private final String serverAddress;

  /**
   * If true, run the handling of messages on the transport in a new thread.
   */
  public final boolean thread;

  /**
   * If true, zip packets on the network. This takes ~2x as much CPU, but
   * lowers packet size.
   */
  public final boolean zip;

  /**
   * The addresses we should broadcast messages to.
   */
  private InetAddress[] broadcastAddrs;

  /**
   * If true, the broadcast address at {@link #broadcastAddrs} is a real broadcast address.
   * Otherwise, it's just a list of addresses in the cluster.
   */
  private final boolean isRealBroadcast;

  /**
   * The time when our last message was received. This is mostly for debugging.
   */
  private long lastMessageReceived = 0L;

  /**
   * The time when our last message was sent. This is mostly for debugging.
   */
  private long lastMessageSent = 0L;

  /**
   * The port we are listening to UDP messages on.
   */
  private final int udpListenPort;

  /**
   * The port we are listening to TCP messages on.
   */
  private final int tcpListenPort;

  /**
   * A shared socket to write messages to.
   */
  private DatagramSocket socket;

  /**
   * The server socket.
   */
  private DatagramSocket serverSocket;

  /**
   * The set of listeners on the broadcast.
   */
  private final Map<UDPBroadcastProtos.MessageType, IdentityHashSet<Consumer<byte[]>>> listeners = new HashMap<>();

  /**
   * Register the hosts that have received pings, and the times they received them.
   */
  private final Map<byte[], Long> pingsReceived = new HashMap<>();

  /**
   * The inferred MTU (Maximum Transmission Unit) of this transport
   */
  public final int mtu;

  /**
   * If true, allow sending of "jumbo packets" (fragmented packets) larger than
   * the MTU of the interface. On one hand, this allows for UDP with larger packet sizes;
   * on the other hand, this may be unstable / slower on the open internet, vs a known
   * local area network.
   */
  public final boolean allowJumboPackets;


  /**
   * A queue of messages to send asynchronously.
   */
  private final Queue<Runnable> sendQueue = new ArrayDeque<>();

  /**
   * Timing statistics for Raft.
   */
  Object summaryTimer = Prometheus.summaryBuild("udp_transport", "Statistics on the UDP Transport calls", "operation");


  /**
   * Create a UDP transport.
   *
   * @param udpListenPort The UDP port to listen on.
   * @param tcpListenPort The TCP port to listen on.
   * @param async If true, run messages on the transport in separate threads.
   * @param zip If true, zip packets on the network. This takes ~2x as much CPU, but
   *            lowers the size of packets sent.
   * @param allowJumboPackets If true, allow sending of "jumbo packets" (fragmented packets) larger than
   *                          the MTU of the interface. On one hand, this allows for UDP with larger packet sizes;
   *                          on the other hand, this may be unstable / slower on the open internet, vs a known
   *                          local area network.
   *
   * @throws UnknownHostException Thrown if we could not get our own hostname.
   */
  private UDPTransport(int udpListenPort, int tcpListenPort, boolean async, boolean zip, boolean allowJumboPackets) throws IOException {
    // I. Save some trivial variables
    this.udpListenPort = udpListenPort;
    this.tcpListenPort = tcpListenPort;
    this.serverName = InetAddress.getLocalHost();
    this.serverAddress = this.serverName.getHostAddress();
    this.socket = new DatagramSocket();
    this.socket.setSendBufferSize(MAX_UDP_PACKET_SIZE);
    this.socket.setReceiveBufferSize(MAX_UDP_PACKET_SIZE);
    this.serverSocket = new DatagramSocket(udpListenPort);
    this.thread = async;
    this.zip = zip;
    this.allowJumboPackets = allowJumboPackets;

    // II. Determine our broadcast address
    InetAddress broadcastAddr = null;
    Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces();
    int minMtu = MAX_UDP_PACKET_SIZE;
    while (en.hasMoreElements()) {
      NetworkInterface ni = en.nextElement();
      if (ni.isUp() && !ni.isLoopback()) {
        List<InterfaceAddress> list = ni.getInterfaceAddresses();
        if (!list.isEmpty()) {
          int mtu = ni.getMTU() - 20; // 20 bytes reserved for UDP header
          if (mtu < minMtu) {
            minMtu = mtu;
          }
        }
        for (InterfaceAddress ia : list) {
          if (ia.getBroadcast() != null) {
            broadcastAddr = ia.getBroadcast();
            log.info("Found broadcast address: {}", ia.getBroadcast());
          }
        }
      }
    }
    this.mtu = minMtu;
    log.info("Setting MTU to {}", this.mtu);
    if (broadcastAddr != null && !broadcastAddr.getHostAddress().equals("0.0.0.0")) {
      // Case: we have a broadcast address
      log.info("Using real broadcast address to broadcast");
      this.broadcastAddrs = new InetAddress[]{ broadcastAddr };
      this.isRealBroadcast = true;
    } else {
      // Case: we need to infer the address from Kubernetes
      log.info("Using Kubernetes state to broadcast");
      this.isRealBroadcast = false;
      this.broadcastAddrs = readKubernetesState();
      RaftLifecycle.global.timer.get().scheduleAtFixedRate(new SafeTimerTask() {  // occasionally
        /** {@inheritDoc} */
        @Override
        public void runUnsafe() throws Throwable {
          // Get new broadcast addresses
          InetAddress[] oldAddrs = broadcastAddrs;
          broadcastAddrs = Arrays.stream(readKubernetesState())
              .filter(x -> {
                try {
                  return x.isReachable(100);  // actually check if the addresses are reachable
                } catch (IOException e) {
                  return false;
                }
              })
              .toArray(InetAddress[]::new);
          if (broadcastAddrs.length == 0) {
            log.warn("Could not read Kubernetes configuration (no nodes present)");
            broadcastAddrs = oldAddrs;
          }
          if (!Arrays.equals(oldAddrs, broadcastAddrs)) {
            log.info("detected change in online nodes: {}  (from {})",
                StringUtils.join(broadcastAddrs, ","),
                StringUtils.join(oldAddrs, ","));
          }
          // Force refresh our sockets
          for (InetAddress addr : broadcastAddrs) {
            getTcpSocket(addr, true);
          }
          // Broadcast a ping
          broadcastTransport(UDPBroadcastProtos.MessageType.PING, UDPTransport.this.serverName.getAddress());
        }
      }, Duration.ofSeconds(10), Duration.ofSeconds(10));
    }
    log.info("Broadcast addresses are [{}] (is_inferred={})", StringUtils.join(this.broadcastAddrs, ", "), broadcastAddr != null);

    if (this.isRealBroadcast) {
      this.socket.setBroadcast(true);
    }
    this.socket.setTrafficClass(0x10);  // IPTOS_LOWDELAY

    // III. Configure thread for UDP listener
    log.info("Binding UDP to " + udpListenPort);
    Thread reader = new Thread(() -> {
      try {
        byte[] recvBuffer = new byte[MAX_UDP_PACKET_SIZE];
        DatagramPacket packet = new DatagramPacket(recvBuffer, recvBuffer.length);
        log.info("Started UDP listener thread");
        while (true) {
          try {
            // 1. Receive the packet
            if (serverSocket.isClosed()) {
              log.info("UDP socket was closed -- reopening");
              this.serverSocket = new DatagramSocket(udpListenPort);
            }
            serverSocket.receive(packet);

            Object prometheusBegin = Prometheus.startTimer(summaryTimer, "parse_upd_packet");
            // 2. Parse the packet
            byte[] datagram = new byte[packet.getLength()];
            System.arraycopy(packet.getData(), packet.getOffset(), datagram, 0, datagram.length);
            byte[] protoBytes;
            if (this.zip) {
              try {
                protoBytes = ZipUtils.gunzip(datagram);
              } catch (Throwable t) {
                protoBytes = datagram;  // fall back on unzipped bytes
              }
            } else {
              protoBytes = datagram;
            }
            UDPBroadcastProtos.UDPPacket proto = UDPBroadcastProtos.UDPPacket.parseFrom(protoBytes);
            if (proto == null) {
              continue;
            }
            Prometheus.observeDuration(prometheusBegin);

            // 3. Notify listeners
            if (proto.getType() == UDPBroadcastProtos.MessageType.PING) {
              this.pingsReceived.put(proto.getContents().toByteArray(), System.currentTimeMillis());
            } else if (!proto.getIsBroadcast() || !proto.getSender().equals(this.serverAddress)){
              IdentityHashSet<Consumer<byte[]>> listeners;
              synchronized (this.listeners) {
                listeners = this.listeners.get(proto.getType());
              }
              if (listeners != null) {
                for (Consumer<byte[]> listener : listeners) {
                  doAction(async, "inbound message of type " + proto.getType(), () -> listener.accept(proto.getContents().toByteArray()));
                }
              }
            }

          } catch (IOException e) {
            log.warn("IOException receiving packet from UDP: " + e.getClass().getSimpleName() + ": " + e.getMessage());
          } catch (Throwable t) {
            log.warn("Caught throwable in UDP RPC receive method with packet length "+packet.getLength()+" (at offset "+packet.getOffset()+" in the buffer of size "+packet.getData().length+") and with source "+packet.getAddress()+": ", t);
          } finally {
            lastMessageReceived = System.currentTimeMillis();
          }
        }
      } finally {
        log.error("Why did we shut down the transport listener thread?");
      }
    });

    // Start the thread
    reader.setDaemon(true);
    reader.setName("udp-listener");
    reader.setUncaughtExceptionHandler((t, e) -> log.warn("Uncaught exception on thread {}: ", t, e));
    reader.setPriority(Thread.MAX_PRIORITY - 1);
    reader.start();

    // IV. Configure thread for TCP listener
    log.info("Binding TCP to " + tcpListenPort);
    Thread tcpServer = new Thread(() -> {
      try {
        ServerSocket sock = new ServerSocket(tcpListenPort);
        log.info("Started TCP listener thread");
        while (true) {
          // 1. Accept the client
          try {
            Socket client = sock.accept();
            Thread clientReader = new Thread(() -> {
              try {
                UDPBroadcastProtos.UDPPacket proto;
                // 2. Receive the packets
                // If the client is closed, stop listening
                while (!client.isClosed()) {
                  proto = UDPBroadcastProtos.UDPPacket.parseDelimitedFrom(client.getInputStream());
                  // If no proto left to read, close the listener. Otherwise, we may infinite loop
                  if (proto == null) {
                    break;
                  }

                  // 3. Notify listeners
                  Object prometheusBegin = Prometheus.startTimer(summaryTimer, "parse_tcp_packet");
                  IdentityHashSet<Consumer<byte[]>> listeners;
                  synchronized (this.listeners) {
                    listeners = new IdentityHashSet<>(this.listeners.get(proto.getType()));  // copy, to prevent concurrency bugs
                  }
                  final byte[] bytes = proto.getContents().toByteArray();
                  Prometheus.observeDuration(prometheusBegin);

                  for (Consumer<byte[]> listener : listeners) {
                    doAction(async, "inbound TCP message of type " + proto.getType(), () -> listener.accept(bytes));
                  }
                }
              } catch (IOException e) {
                log.warn("IOException receiving packet from TCP: " + e.getClass().getSimpleName() + ": " + e.getMessage());
              } catch (Throwable t) {
                log.warn("Caught throwable in TCP RPC receive method: ", t);
              } finally {
                lastMessageReceived = System.currentTimeMillis();
                try {
                  client.close();
                } catch (IOException e) {
                  log.warn("Could not close TCP socket: ", e);
                }
              }
            });
            // 4. Start the thread
            clientReader.setDaemon(true);
            clientReader.setName("tcp-listener-"+ client.getInetAddress().getHostName());
            clientReader.setUncaughtExceptionHandler((t, e) -> log.warn("Uncaught exception on {}: ", t, e));
            clientReader.setPriority(Math.max(Thread.NORM_PRIORITY, Thread.MAX_PRIORITY - 1));
            clientReader.start();
          } catch (IOException e) {
            log.warn("IOException receiving new socket on TCP: " + e.getClass().getSimpleName() + ": " + e.getMessage());
          } catch (Throwable t) {
            log.warn("Caught throwable in TCP receive method: ", t);
          } finally {
            lastMessageReceived = System.currentTimeMillis();
          }
        }
      } catch (Throwable e) {
        log.error("Could not establish TCP socket -- this is an error!");
      } finally {
        log.error("Why did we shut down the transport listener thread?");
      }
    });

    // Start the thread
    tcpServer.setDaemon(true);
    tcpServer.setName("tcp-listener");
    tcpServer.setUncaughtExceptionHandler((t, e) -> log.warn("Uncaught exception on thread {}: ", t, e));
    tcpServer.setPriority(Thread.MAX_PRIORITY - 1);
    tcpServer.start();

    // V. Configure sender thread
    log.info("Starting sender thread" + tcpListenPort);
    final AtomicLong lastSendTime = new AtomicLong(-1L);
    // This thread waits for messages to show up on a send queue, and sends
    // them one by one. The motivation here is to allow the thread to be
    // interrupted by the timer task below if a message takes too long to send.
    Thread sender = new Thread(() -> {
      while (true) {
        try {
          // 1. Wait for a message
          synchronized (sendQueue) {
            while (sendQueue.isEmpty()) {
              try {
                sendQueue.wait(100);
              } catch (InterruptedException ignored) {}
            }
          }
          // 2. Get the message
          Runnable message = sendQueue.poll();
          try {
            // 3. Send the message
            if (message != null) {
              lastSendTime.set(System.currentTimeMillis());
              message.run();
              lastSendTime.set(-1L);
            }
          } catch (Exception e) {
            log.warn("Could not send message: {}: {}", e.getClass().getSimpleName(), e.getMessage());
          }
        } catch (Throwable t) {
          log.warn("Caught exception sending message on transport: ", t);
        }
      }

    });
    sender.setDaemon(true);
    sender.setName("transport-sender");
    sender.setUncaughtExceptionHandler((t, e) -> log.warn("Uncaught exception on thread {}: ", t, e));
    sender.setPriority(Thread.MAX_PRIORITY - 1);
    sender.start();

    // Every 100ms, make sure that the socket sender thread is not stuck.
    RaftLifecycle.global.timer.get().scheduleAtFixedRate(new SafeTimerTask() {
      @Override
      public void runUnsafe() {
        if (System.currentTimeMillis() - lastSendTime.get() > 100) {
          sender.interrupt();
        }
      }
    }, Duration.ofMillis(100));
  }


  /** @see #UDPTransport(int, int, boolean, boolean, boolean) */
  public UDPTransport() throws IOException {
    this(DEFAULT_UDP_LISTEN_PORT, DEFAULT_TCP_LISTEN_PORT, false, false, false);
  }


  /**
   * Read the Kubernetes broadcast state from a file.
   *
   * @return The addresses of all the nodes known to Kubernetes.
   *
   * @throws IOException Thrown if we could not read the Kubernetes state.
   */
  public static InetAddress[] readKubernetesState() throws IOException {
    String path = System.getenv("ELOQUENT_RAFT_MEMBERS");
    if (path == null || "".equals(path)) {
      throw new FileNotFoundException("Could not find Kubernetes file variable ELOQUENT_RAFT_MEMBERS");
    }

    String[] entries = IOUtils.slurpReader(IOUtils.readerFromString(path)).split("\n");
    return Arrays.stream(entries)
        .filter(x -> x.trim().length() > 0)
        .map(x -> x.split("\\t|\\s{2,}")) // split on tabs, or on 2 (or more) spaces
        .filter(x -> x.length > 0 && IP_REGEX.matcher(x[0]).matches())
        .map((String[] x) -> {
          try {
            return InetAddress.getByName(x[0]);
          } catch (UnknownHostException e) {
            log.warn("Unknown host: " + x[0]);
            return null;
          }
        }).filter(Objects::nonNull)
        .toArray(InetAddress[]::new);
  }


  /** {@inheritDoc} */
  public void bind(UDPBroadcastProtos.MessageType channel, Consumer<byte[]> listener) {
    synchronized (this.listeners) {
      IdentityHashSet<Consumer<byte[]>> listenersOnChannel = this.listeners.computeIfAbsent(channel, k -> new IdentityHashSet<>());
      listenersOnChannel.add(listener);
    }
  }


  /**
   * Send a message over the transport to a destination.
   *
   * @param destination The destination we're sending to.
   * @param messageType The type of message we're sending.
   * @param message The message we're sending.
   *
   * @return True if the message was sent; false if the message was too long.
   */
  public boolean sendTransport(String destination, UDPBroadcastProtos.MessageType messageType, byte[] message) {
    try {
      // 1. Create the packet
      UDPBroadcastProtos.UDPPacket proto = UDPBroadcastProtos.UDPPacket.newBuilder()
          .setIsBroadcast(false)
          .setType(messageType)
          .setSender(this.serverAddress)
          .setContents(ByteString.copyFrom(message))
          .build();
      byte[] datagram = this.zip ? ZipUtils.gzip(proto.toByteArray()) : proto.toByteArray();

      // 2. Send the packet
      if ( (this.allowJumboPackets && datagram.length < MAX_UDP_PACKET_SIZE) || (!this.allowJumboPackets && datagram.length < this.mtu) ) {
        // 2.A. Send over UDP
        // 2.1. Copy the packet into the UDP buffer
        DatagramPacket packet = new DatagramPacket(datagram, datagram.length, InetAddress.getByName(destination), udpListenPort);
        // 2.2. Reconnect if necessary
        if (this.socket.isClosed()) {
          synchronized (this) {
            if (this.socket.isClosed()) {
              log.info("UDP socket was closed (in send call) -- reopening");
              this.socket = new DatagramSocket();
              this.socket.setSendBufferSize(MAX_UDP_PACKET_SIZE);
              if (this.isRealBroadcast) {
                this.socket.setBroadcast(true);
              }
              this.socket.setTrafficClass(0x10);  // IPTOS_LOWDELAY
            }
          }
        }
        // 2.3. Send the packet
        long socketSendStart = System.currentTimeMillis();
        try {
          this.socket.send(packet);
        } finally {
          long socketSendEnd = System.currentTimeMillis();
          if (socketSendEnd > socketSendStart + 10) {
            log.warn("Sending a direct message on the UDP socket took {}", TimerUtils.formatTimeDifference(socketSendEnd - socketSendStart));
          }
        }
        if (networkSeemsDown) {
          log.info("Network is back up");
        }
        networkSeemsDown = false;

        // 3. Return
        return true;
      } else {
        // 2.B. Send over TCP
        log.debug("Message is too long to send as a single packet ({}); sending over TCP", datagram.length);
        Optional<Socket> tcpSocket = getTcpSocket(InetAddress.getByName(destination), false);
        tcpSocket.ifPresent(socket1 -> safeWrite(proto, socket1, "sendTransport"));
      }

    } catch (SocketException e) {
      log.warn("Could not create datagram socket: ", e);
    } catch (UnknownHostException e) {
      log.warn("No such destination: ", e);
    } catch (IOException e) {
      if (e.getMessage() != null &&
          (e.getMessage().equals("Network is unreachable") || e.getMessage().equals("Network is down"))) {
        if (!networkSeemsDown) {
          log.warn("Network seems to have disconnected! UDPTransport is not sending messages");
        }
        networkSeemsDown = true;
      } else {
        log.warn("Could not send message on datagram socket: ", e);
      }
    } finally {
      lastMessageSent = System.currentTimeMillis();
    }
    return false;
  }


  /**
   * A cache for open TCP sockets to various endpoints.
   */
  final Map<InetAddress, Socket> tcpSocketCache = new HashMap<>();


  /**
   * Gets a cached TCP socket to a box.
   * Note that this method can be slow -- up to 100ms -- in the case that
   * the receiving socket is not playing nice and does not complete the handshake
   * in a timely fashion.
   *
   * @param addr The address we're connecting to
   * @param forceRetry If true, force trying to reconnect to the socket.
   */
  private Optional<Socket> getTcpSocket(InetAddress addr, boolean forceRetry) {
    Socket candidate;
    synchronized (tcpSocketCache) {
      if ((candidate = tcpSocketCache.get(addr)) == null && !forceRetry && tcpSocketCache.containsKey(addr)) {
        // we've explicitly null'd this socket
        return Optional.empty();
      }
    }

    if (candidate == null || candidate.isClosed()) {
      // This socket doesn't exist, or is closed
      // 1. Check that the address is even alive
      try {
        if (!addr.isReachable(100)) {
          synchronized (tcpSocketCache) {
            tcpSocketCache.put(addr, null);
          }
          log.warn("{} is reachable: ", addr);
          return Optional.empty();
        }
      } catch (IOException e) {
        log.warn("Could not check if {} is reachable: ", addr, e);
        synchronized (tcpSocketCache) {
          tcpSocketCache.put(addr, null);
        }
        return Optional.empty();
      }

      // 2. Get the socket on a killable thread
      final CompletableFuture<Socket> asyncSocket = new CompletableFuture<>();
      Thread tcpGetter = new Thread( () -> {
        try {
          Socket impl = new Socket(addr, tcpListenPort);
          synchronized (asyncSocket) {
            asyncSocket.complete(impl);
          }
        } catch (IOException e) {
          asyncSocket.completeExceptionally(e);
        }
      });
      tcpGetter.setName("socket-creator-" + addr.getHostAddress());
      tcpGetter.setDaemon(true);
      tcpGetter.setUncaughtExceptionHandler((t, e) -> log.warn("Uncaught exception on {}: ", t, e));
      tcpGetter.start();

      // 3. Wait on the result of the thread
      try {
        candidate = asyncSocket.get(5, TimeUnit.SECONDS);  // a socket connection to Google from my home wifi takes 20ms

        // 4. Set the socket
        synchronized (tcpSocketCache) {
          Socket mostRecent = tcpSocketCache.get(addr);
          if (mostRecent != null && !mostRecent.isClosed()) {
            try {
              candidate.close();  // we encountered a race condition -- close our newer socket
            } catch (IOException ignored) {}
          } else {
            tcpSocketCache.put(addr, candidate);  // we have a new socket
          }
        }
        log.info("refreshed TCP socket for {}", addr);
      } catch (ExecutionException | InterruptedException | TimeoutException e) {
        if (e.getCause() != null && e.getCause() instanceof ConnectException) {
          log.warn("Remote machine {} is not accepting connections ({})", addr, e.getCause().getMessage());
        } else {
          log.warn("Could not create TCP socket to {} -- exception on future: ", addr, e);
        }
        return Optional.empty();
      } finally {
        if (tcpGetter.isAlive()) {  // kill the thread
          tcpGetter.interrupt();
        }
      }
    }

    // 5. Return
    return Optional.of(candidate);
  }


  /**
   * Send the given message to everyone on the network.
   *
   * @param messageType The type of message we're sending.
   * @param message The messge to send
   */
  public boolean broadcastTransport(UDPBroadcastProtos.MessageType messageType, byte[] message) {
    long startTime = System.currentTimeMillis();
    try {
      // 1. Create the packet
      UDPBroadcastProtos.UDPPacket proto = UDPBroadcastProtos.UDPPacket.newBuilder()
          .setIsBroadcast(true)
          .setType(messageType)
          .setSender(this.serverAddress)
          .setContents(ByteString.copyFrom(message))
          .build();
      byte[] datagram = this.zip ? ZipUtils.gzip(proto.toByteArray()) : proto.toByteArray();

      // 2. Broadcast the packet
      boolean allSuccess = true;
      if ( (this.allowJumboPackets && datagram.length < MAX_UDP_PACKET_SIZE) || (!this.allowJumboPackets && datagram.length < this.mtu) ) {
        // 2.A. The packet is small enough to go over UDP
        // 2.1. For each address in the broadcast...
        for (InetAddress broadcastAddr : this.broadcastAddrs) {
          long sendStart = System.currentTimeMillis();
          try {
            // 2.2. If it's not ourselves...
            if (!broadcastAddr.equals(this.serverName)) {
              // 2.3. Copy the packet into the UDP buffer
              DatagramPacket packet = new DatagramPacket(datagram, datagram.length, broadcastAddr, udpListenPort);
              // 2.4. Reconnect the socket if necessary
              if (this.socket.isClosed()) {
                synchronized (this) {
                  if (this.socket.isClosed()) {
                    log.info("UDP socket was closed (in broadcast call) -- reopening");
                    this.socket = new DatagramSocket();
                    this.socket.setSendBufferSize(MAX_UDP_PACKET_SIZE);
                    if (this.isRealBroadcast) {
                      this.socket.setBroadcast(true);
                    }
                    this.socket.setTrafficClass(0x10);  // IPTOS_LOWDELAY
                  }
                }
              }
              // 2.5. Send the packet
              long socketSendStart = System.currentTimeMillis();
              try {
                this.socket.send(packet);
              } finally {
                long socketSendEnd = System.currentTimeMillis();
                if (socketSendEnd > socketSendStart + 10) {
                  log.warn("Sending a broadcast to {} on the UDP socket took {} for packet of length {}",
                      broadcastAddr, TimerUtils.formatTimeDifference(socketSendEnd - socketSendStart), packet.getLength());
                }
              }
              if (networkSeemsDown) {
                log.info("Network is back up");
              }
              networkSeemsDown = false;
            }
          } catch (Throwable t) {
            // Case: we could not send a packet -- don't give up, but mark the failure
            allSuccess = false;
            if (t.getMessage() != null &&
                (t.getMessage().equals("Network is unreachable") || t.getMessage().equals("Network is down"))) {
              if (!networkSeemsDown) {
                log.warn("Network seems to have disconnected! UDPTransport is not sending messages");
              }
              networkSeemsDown = true;
            } else {
              log.warn("Could not broadcast to " + broadcastAddr + " (attempt took " + TimerUtils.formatTimeSince(sendStart) + ") -- still trying other addressed", t);
            }
          } finally {
            long sendEnd = System.currentTimeMillis();
            if (sendEnd > sendStart + 10) {
              log.warn("Sending broadcast to {} on transport took {}", broadcastAddr, TimerUtils.formatTimeDifference(sendEnd - sendStart));
            }
          }
        }

        // 3. Return
        return allSuccess;
      } else if (!this.isRealBroadcast) {

        // 2.B. Send over TCP
        log.debug("Message is too long to send as a single packet ({}) -- broadcasting over TCP", datagram.length);
        // 2.1. For each address in the broadcast...
        for (InetAddress addr : this.broadcastAddrs) {
          try {
            Optional<Socket> tcpSocket = getTcpSocket(addr, false);
            tcpSocket.ifPresent(socket1 -> safeWrite(proto, socket1, "broadcastTransport"));
          } catch (Throwable t) {
            log.warn("Unhandled exception sending message on TCP socket", t);

          }
        }
      } else {
        log.debug("Message is too long to send as a single packet ({}), and we only have a broadcast address", datagram.length);
      }
    } finally {
      lastMessageSent = System.currentTimeMillis();
      log.trace("Sending UDP broadcast took {}", TimerUtils.formatTimeSince(startTime));
    }
    return false;
  }


  /**
   * Write a message to a TCP socket, handling the associated errors gracefully.
   * This method is not guaranteed to write the packet, but is guaranteed to not exception.
   *
   * @param proto The message we want to send over the socket.
   * @param tcpSocket The socket we're sending the message over
   * @param source A debug source for when we log errors.
   */
  private void safeWrite(UDPBroadcastProtos.UDPPacket proto, Socket tcpSocket, String source) {
    synchronized (sendQueue) {
      if (sendQueue.size() < 10000) {
        sendQueue.offer(() -> {
          synchronized (tcpSocket) {
            try {
              proto.writeDelimitedTo(tcpSocket.getOutputStream());
            } catch (IOException e) {
              log.info("IO or Socket exception writing to TCP socket ({}): {}", source, e.getMessage());
              try {
                tcpSocket.close();
              } catch (Throwable ignored) {
              }
            } catch (Throwable e) {
              log.warn("Unknown exception writing to TCP socket (" + source + "): ", e);
            }
          }
        });
        sendQueue.notify();
      }
    }
  }


  /**
   * Get the set of servers which have been responding to ping requests in the last
   * threshold ms
   *
   * @param pingThreshold The threshold under which to consider a server alive.
   *
   * @return The set of servers alive within that threshold.
   */
  private Set<String> liveServers(@SuppressWarnings("SameParameterValue") long pingThreshold) {
    Set<String> servers = new HashSet<>();
    for (Map.Entry<byte[], Long> entry : this.pingsReceived.entrySet()) {
      if (Math.abs(entry.getValue() - System.currentTimeMillis()) < pingThreshold) {
        List<String> ipElem = new ArrayList<>();
        for (byte b : entry.getKey()) {
          ipElem.add(Integer.toString((((int) b) + 256) % 256));
        }
        servers.add(StringUtils.join(ipElem, "."));
      }
    }
    return servers;
  }


  /** {@inheritDoc} */
  @Override
  public String toString() {
    return "UDP@" + SystemUtils.HOST + " (" + this.serverAddress + ")" +
        "  <last message sent " + TimerUtils.formatTimeSince(lastMessageSent) + " ago; received " + TimerUtils.formatTimeSince(lastMessageReceived) + " ago>" +
        "  <broadcasting to " + Arrays.asList(this.broadcastAddrs) + ">" +
        "  <pings from " + StringUtils.join(liveServers(60000), ", ") + ">"
        ;
  }


  /**
   * The default UDP broadcast.
   */
  public static Lazy<Transport> DEFAULT = Lazy.of(() -> {
    try {
      return new UDPTransport();
    } catch (IOException e) {
      log.warn("UDPTransport already bound to address! Returning mock implementation");
      return new Transport(){
        /** {@inheritDoc} */
        @Override
        public void bind(UDPBroadcastProtos.MessageType channel, Consumer<byte[]> listener) {
          log.warn("UDP address is in use: cannot bind to transport");
        }
        /** {@inheritDoc} */
        @Override
        public boolean sendTransport(String destination, UDPBroadcastProtos.MessageType messageType, byte[] message) {
          log.warn("UDP address is in use: cannot send on transport");
          return false;
        }
        /** {@inheritDoc} */
        @Override
        public boolean broadcastTransport(UDPBroadcastProtos.MessageType messageType, byte[] message) {
          log.warn("UDP address is in use: cannot broadcast on transport");
          return false;
        }
      };
    }
  });


  public static void main(String[] args) throws IOException {
    new UDPTransport();
  }

}
