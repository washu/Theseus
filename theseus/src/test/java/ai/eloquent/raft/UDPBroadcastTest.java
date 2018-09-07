package ai.eloquent.raft;

import ai.eloquent.data.UDPBroadcastProtos;
import ai.eloquent.data.UDPTransport;
import ai.eloquent.util.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicReference;

public class UDPBroadcastTest {
  /**
   * An SLF4J Logger for this class.
   */
  private static final Logger log = LoggerFactory.getLogger(UDPBroadcastTest.class);

  private static final byte REQUEST = 1;
  private static final byte REPLY = 2;

  public static void main(String[] args) throws IOException {
    byte myId = (byte)new Random().nextInt();
    System.out.println("Selected random ID: "+myId);

    UDPTransport transport = new UDPTransport();

    AtomicReference<Optional<Long>> launchTime = new AtomicReference<>(Optional.empty());

    transport.bind(UDPBroadcastProtos.MessageType.RAFT, (bytes) -> {
      if (bytes[0] == REQUEST && bytes[1] != myId) {
        System.out.println("Got ping request from "+myId);
        transport.broadcastTransport(UDPBroadcastProtos.MessageType.RAFT, new byte[]{REPLY, myId});
      }
      else if (bytes[0] == REPLY && bytes[1] != myId) {
        launchTime.get().ifPresent(aLong -> System.out.println("Time since launch: " + TimeUtils.formatTimeSince(aLong)));
        launchTime.set(Optional.empty());
      }
    });

    while (true) {
      System.out.print("Press enter to send a broadcast ping> ");
      new Scanner(System.in).nextLine();
      transport.broadcastTransport(UDPBroadcastProtos.MessageType.RAFT, new byte[]{REQUEST, myId});
      launchTime.set(Optional.of(System.currentTimeMillis()));
    }
  }
}
