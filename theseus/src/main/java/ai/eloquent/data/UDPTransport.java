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
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
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
   * A histogram of timing for processing an incoming UDP message.
   */
  private static final Object inboundHandleTimeHistogram =
      Prometheus.histogramBuild("udp_transport_proctime", "The time it takes to process a UDP packet, in seconds",
          0, 0.1 / 1000.0, 0.5 / 1000.0, 1.0 / 1000.0, 5.0 / 1000.0, 10.0 / 1000.0, 50.0 / 1000.0, 100.0 / 1000.0, 200.0 / 1000.0, 300.0 / 1000.0, 1.0 / 2.0, 1.0, 10.0);

  /**
   * The number of listeners bound to this transport
   */
  private static final Object boundListenerCount =
      Prometheus.gaugeBuild("udp_transport_listeners", "The number of listeners bound to this transport");


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
            getTcpSocket(addr);
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
            long startTime = System.nanoTime();
            try {

              // 2. Parse the packet
              byte[] protoBytes;
              int offset;
              final int length = packet.getLength();
              if (this.zip) {
                // 2.A. Copy + unzip the bytes
                byte[] datagram = new byte[length];
                System.arraycopy(packet.getData(), packet.getOffset(), datagram, 0, length);
                try {
                  protoBytes = ZipUtils.gunzip(datagram);
                } catch (Throwable t) {
                  protoBytes = datagram;  // fall back on unzipped bytes
                }
                offset = 0;
              } else {
                // 2.B. Use the raw datagram bytes
                protoBytes = packet.getData();
                offset = packet.getOffset();
              }
              UDPBroadcastProtos.UDPPacket proto = UDPBroadcastProtos.UDPPacket.parseFrom(ByteBuffer.wrap(protoBytes, offset, length));
              if (proto == null) {
                continue;
              }

              // 3. Notify listeners
              if (proto.getType() == UDPBroadcastProtos.MessageType.PING) {
                this.pingsReceived.put(proto.getContents().toByteArray(), System.currentTimeMillis());
              } else if (!proto.getIsBroadcast() || !proto.getSender().equals(this.serverAddress)) {
                Set<Consumer<byte[]>> listeners;
                synchronized (this.listeners) {
                  listeners = new IdentityHashSet<>(this.listeners.getOrDefault(proto.getType(), new IdentityHashSet<>()));
                }
                for (Consumer<byte[]> listener : listeners) {
                  doAction(async, "inbound message of type " + proto.getType(), () -> listener.accept(proto.getContents().toByteArray()));
                }
              }
            } finally {
              // Log the time it took to execute the request.
              long finishTime = System.nanoTime();
              if (finishTime - startTime > 100000000) {
                log.warn("Took > 100ms to process an inbound UDP packet: {}", TimerUtils.formatTimeDifference((finishTime - startTime) / 1000000));
              }
              Prometheus.histogramObserve(inboundHandleTimeHistogram, ((double) (finishTime - startTime)) / 1000000000);
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

                  long startTime = System.nanoTime();
                  try {
                    // 3. Notify listeners
                    IdentityHashSet<Consumer<byte[]>> listeners;
                    synchronized (this.listeners) {
                      listeners = new IdentityHashSet<>(this.listeners.get(proto.getType()));  // copy, to prevent concurrency bugs
                    }
                    final byte[] bytes = proto.getContents().toByteArray();

                    for (Consumer<byte[]> listener : listeners) {
                      doAction(async, "inbound TCP message of type " + proto.getType(), () -> listener.accept(bytes));
                    }
                  } finally {
                    // Log the time it took to execute the request.
                    long finishTime = System.nanoTime();
                    if (finishTime - startTime > 100000000) {
                      log.warn("Took > 100ms to process an inbound TCP packet: {}", TimerUtils.formatTimeDifference((finishTime - startTime) / 1000000));
                    }
                    Prometheus.histogramObserve(inboundHandleTimeHistogram, ((double) (finishTime - startTime)) / 1000000000);
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
        log.error("Could not establish TCP socket -- this is an error!", e);
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

    String[] entries = IOUtils.slurp(IOUtils.getReader(path)).split("\n");
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
  @Override
  public void bind(UDPBroadcastProtos.MessageType channel, Consumer<byte[]> listener) {
    synchronized (this.listeners) {
      this.listeners.computeIfAbsent(channel, k -> new IdentityHashSet<>()).add(listener);
    }
    Prometheus.gaugeInc(boundListenerCount);
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
  @Override
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
          if (socketSendEnd > socketSendStart + 100) {
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
        long socketSendStart = System.currentTimeMillis();
        try {
          Optional<Socket> tcpSocket = getTcpSocket(InetAddress.getByName(destination));
          tcpSocket.ifPresent(socket1 -> safeWriteTcp(proto, socket1, "sendTransport"));  // drop the packet if the connection doesn't exist
        } finally {
          long socketSendEnd = System.currentTimeMillis();
          if (socketSendEnd > socketSendStart + 100) {
            log.warn("Sending a direct message on the TCP socket took {}", TimerUtils.formatTimeDifference(socketSendEnd - socketSendStart));
          }
        }
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
  final Map<InetAddress, DeferredLazy<Socket>> tcpSocketCache = new ConcurrentHashMap<>();


  /**
   * Gets a cached TCP socket to a box.
   * If the socket doesn't exist, we return {@link Optional#empty()}, and begin recomputing the
   * socket for -- hopefully -- the next call.
   *  @param addr The address we're connecting to.
   *
   */
  private Optional<Socket> getTcpSocket(InetAddress addr) {
    long startTime = System.currentTimeMillis();
    try {
      return tcpSocketCache.computeIfAbsent(addr, key -> new DeferredLazy<Socket>() {
        /** {@inheritDoc} */
        @Override
        protected Socket compute() throws IOException {
          log.info("Getting a new TCP socket to {}", addr);
          try {
            return new Socket(addr, tcpListenPort);
          } finally {
            log.info("TCP socket to {} connected", addr);
          }
        }
        /** {@inheritDoc} */
        @Override
        protected boolean isValid(Socket sock, long lastComputeTime, long currentTime) {
          return sock != null && !sock.isClosed();
        }
        /** {@inheritDoc} */
        @Override
        protected void onDiscard(Socket value) throws IOException {
          if (!value.isClosed()) {
            value.close();
          }
        }
      }).get();
    } finally {
      long endTime = System.currentTimeMillis();
      if (endTime - startTime > 10) {
        log.warn("Took {} to get a cached TCP socket. This is a pretty bad sign.", TimerUtils.formatTimeDifference(endTime - startTime));
      }
    }
  }


  /**
   * Send the given message to everyone on the network.
   *
   * @param messageType The type of message we're sending.
   * @param message The messge to send
   */
  @Override
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
            Optional<Socket> tcpSocket = getTcpSocket(addr);
            tcpSocket.ifPresent(socket1 -> safeWriteTcp(proto, socket1, "broadcastTransport"));
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
  private synchronized void safeWriteTcp(UDPBroadcastProtos.UDPPacket proto, final Socket tcpSocket, String source) {
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
    return "UDP@" + SystemUtils.LOCAL_HOST_ADDRESS + " (" + this.serverAddress + ")" +
        "  <last message sent " + TimerUtils.formatTimeSince(lastMessageSent) + " ago; received " + TimerUtils.formatTimeSince(lastMessageReceived) + " ago>" +
        "  <broadcasting to " + Arrays.asList(this.broadcastAddrs) + ">" +
        "  <pings from " + StringUtils.join(liveServers(60000), ", ") + ">"
        ;
  }


  /**
   * The default UDP broadcast.
   */
  public static Lazy<Transport> DEFAULT = Lazy.ofSupplier(() -> {
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
