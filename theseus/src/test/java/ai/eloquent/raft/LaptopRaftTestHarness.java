package ai.eloquent.raft;

import ai.eloquent.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.swing.*;
import java.awt.*;
import java.awt.event.KeyAdapter;
import java.awt.event.KeyEvent;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.*;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class LaptopRaftTestHarness {
  /**
   * An SLF4J Logger for this class.
   */
  private static final Logger log = LoggerFactory.getLogger(LaptopRaftTestHarness.class);

  public static class Terminal {
    private JFrame frm = new JFrame("Terminal");
    private JTextArea txtArea = new JTextArea();
    private JScrollPane scrollPane = new JScrollPane();
    private Supplier<String> hostname;
    private Consumer<String> commandProcessor;
    private final String LINE_SEPARATOR = System.lineSeparator();
    private Font font = new Font("SansSerif", Font.BOLD, 15);

    public Terminal(Supplier<String> hostname, Consumer<String> commandProcessor) {
      this.hostname = hostname;
      this.commandProcessor = commandProcessor;
      frm.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
      frm.getContentPane().add(scrollPane);
      scrollPane.setViewportView(txtArea);
      txtArea.addKeyListener(new KeyListener());
      txtArea.setFont(font);
      disableArrowKeys(txtArea.getInputMap());
    }

    private void disableArrowKeys(InputMap inputMap) {
      String[] keystrokeNames = { "UP", "DOWN", "LEFT", "RIGHT", "HOME" };
      for (int i = 0; i < keystrokeNames.length; ++i)
        inputMap.put(KeyStroke.getKeyStroke(keystrokeNames[i]), "none");
    }

    public void open(final int xLocation, final int yLocation, final int width,
                     final int height) {
      SwingUtilities.invokeLater(new Runnable() {
        public void run() {
          frm.setBounds(xLocation, yLocation, width, height);
          frm.setVisible(true);
          showPrompt();
        }
      });
    }

    public void close() {
      frm.dispose();
    }

    public void clear() {
      txtArea.setText("");
      showPrompt();
    }

    public void print(String text) {
      System.out.print(text);
      txtArea.setText(txtArea.getText() + text);
    }

    public void println() {
      println("");
    }

    public void println(String text) {
      System.out.println(text);
      txtArea.setText(txtArea.getText() + text + LINE_SEPARATOR);
    }

    private void showPrompt() {
      print(hostname.get()+"> ");
    }

    private class KeyListener extends KeyAdapter {
      private final int ENTER_KEY = KeyEvent.VK_ENTER;
      private final int BACK_SPACE_KEY = KeyEvent.VK_BACK_SPACE;
      private final String BACK_SPACE_KEY_BINDING = getKeyBinding(
          txtArea.getInputMap(), "BACK_SPACE");
      private final int INITIAL_CURSOR_POSITION = 2;

      private boolean isKeysDisabled;
      private int minCursorPosition = INITIAL_CURSOR_POSITION;

      private String getKeyBinding(InputMap inputMap, String name) {
        return (String) inputMap.get(KeyStroke.getKeyStroke(name));
      }

      public void keyPressed(KeyEvent evt) {
        int keyCode = evt.getKeyCode();
        if (keyCode == BACK_SPACE_KEY) {
          int cursorPosition = txtArea.getCaretPosition();
          if (cursorPosition == minCursorPosition && !isKeysDisabled) {
            disableBackspaceKey();
          } else if (cursorPosition > minCursorPosition && isKeysDisabled) {
            enableBackspaceKey();
          }
        } else if (keyCode == ENTER_KEY) {
          txtArea.setEnabled(false);
          String command = extractCommand();
          println();
          new Thread(() -> {
            executeCommand(command);
            showPrompt();
            setMinCursorPosition();
            txtArea.setEnabled(true);
          }).start();
          // Prevent this from propagating
          evt.consume();
        }
      }

      public void keyReleased(KeyEvent evt) {
        int keyCode = evt.getKeyCode();
        if (keyCode == ENTER_KEY) {
          // txtArea.setCaretPosition(txtArea.getCaretPosition() - 1);
        }
      }

      private void disableBackspaceKey() {
        isKeysDisabled = true;
        txtArea.getInputMap().put(KeyStroke.getKeyStroke("BACK_SPACE"),
            "none");
      }

      private void enableBackspaceKey() {
        isKeysDisabled = false;
        txtArea.getInputMap().put(KeyStroke.getKeyStroke("BACK_SPACE"),
            BACK_SPACE_KEY_BINDING);
      }

      private void setMinCursorPosition() {
        minCursorPosition = txtArea.getCaretPosition();
      }
    }

    private void executeCommand(String command) {
      commandProcessor.accept(command);
    }

    private String extractCommand() {
      removeLastLineSeparator();
      return stripPreviousCommands();
    }

    private void removeLastLineSeparator() {
      String terminalText = txtArea.getText();
      if (terminalText.endsWith(LINE_SEPARATOR)) {
        terminalText = terminalText.substring(0, terminalText.length() - 1);
      }
      txtArea.setText(terminalText);
    }

    private String stripPreviousCommands() {
      String terminalText = txtArea.getText();
      int lastPromptIndex = terminalText.lastIndexOf('>') + 2;
      if (lastPromptIndex < 0 || lastPromptIndex >= terminalText.length())
        return "";
      else
        return terminalText.substring(lastPromptIndex).replace(LINE_SEPARATOR, "");
    }
  }

  public static void main(String[] args) throws IOException {

    InstantTransport.burnInJIT();

    // 1. Create an algorithm
    String hostname = InetAddress.getLocalHost().getHostAddress();
    RaftLifecycle lifecycle = new RaftLifecycle(Lazy.ofValue(new PreciseSafeTimer()));

    ExecutorService pool = Executors.newFixedThreadPool(4);
    RaftState state = new RaftState(hostname, new KeyValueStateMachine(hostname), pool);
    RaftTransport transport = new NetRaftTransport(hostname);
    RaftAlgorithm algorithm = new SingleThreadedRaftAlgorithm(new EloquentRaftAlgorithm(state, transport, Optional.empty()), pool);
    Theseus raft = new Theseus(algorithm, transport, lifecycle);

    raft.node.start();

    Pointer<Terminal> terminal = new Pointer<>();
    terminal.set(new Terminal(() -> hostname + (state.isLeader() ? " [leader]" : " [follower]"),
        (String command) -> {
      try {
        if (command.equals("bootstrap")) {
          boolean success = algorithm.bootstrap(false);
          terminal.dereference().get().println("Bootstrap status: " + success);
        } else if (command.equals("state")) {
          // 1. Get common Raft state
          StringBuilder b = new StringBuilder();
          b.append("Raft State:\n")
              .append("----------\n\n")
              .append("  Server Name     ").append(state.serverName).append("\n")
              .append("  Transport       ").append(raft.node.transport).append("\n")
              .append("  Deploy Version  ").append(Optional.ofNullable(System.getenv("BUILD_DATE")).orElse("<unknown>")).append("\n")
              .append("  Quorum          ").append(state.log.getQuorumMembers().size())
          ;
          if (state.targetClusterSize < 0) {
            b.append("  (fixed cluster size)\n");
          } else {
            b.append("  (targeting ").append(state.targetClusterSize).append(" members)\n");
          }
          for (String member : state.log.getQuorumMembers()) {
            b.append("    ").append(member).append("\n");
          }
          b.append("  Leadership      ").append(state.leadership).append("\n")
              .append("  Leader          ").append(state.isLeader() ? "Me!" : state.leader.orElse("<unknown>")).append("\n")
              .append("  Term            ").append(state.currentTerm).append("\n")
              .append("  LastIndex       ").append(state.log.getLastEntryIndex()).append(" (in term ").append(state.log.getLastEntryTerm()).append(")\n")
              .append("  Commit Index    ").append(state.commitIndex()).append("\n")
              .append("  Log Length      ").append(state.log.getAllUncompressedEntries().size()).append("\n")
              .append("  Voted For       ").append(state.votedFor.orElse("<none>")).append("\n")
              .append("  Election Ckpt   ").append(state.isLeader() ? "<is leader>" : TimerUtils.formatTimeSince(state.electionTimeoutCheckpoint) + " ago").append("\n")
              .append("  Want Election   ").append(state.shouldTriggerElection(System.currentTimeMillis(), raft.node.algorithm.electionTimeoutMillisRange())).append("\n")
          ;
          b.append("\n  Errors:\n");
          List<String> errors = raft.errors();
          if (errors.isEmpty()) {
            b.append("    <empty>\n");
          } else {
            for (String error : errors) {
              b.append("    ").append(error).append("\n");
            }
          }

          // 2. Get leader state, if any
          if (state.isLeader()) {
            b.append("\n\nLeader State:\n")
                .append("------------\n\n")
            ;
            b.append("  Add Server      ").append(state.serverToAdd(System.currentTimeMillis(), EloquentRaftAlgorithm.MACHINE_DOWN_TIMEOUT).orElse("<none>")).append("\n")
                .append("  Remove Server   ").append(state.serverToRemove(System.currentTimeMillis(), EloquentRaftAlgorithm.MACHINE_DOWN_TIMEOUT).orElse("<none>")).append("\n")
            ;
            state.lastMessageTimestamp.ifPresent(timestampMap -> {
              List<Map.Entry<String, Long>> timestamps = new ArrayList<>(timestampMap.entrySet());
              timestamps.sort(Comparator.comparing(Map.Entry::getValue));
              Collections.reverse(timestamps);
              b.append("\n\n  Last Message Timestamp:\n");
              if (timestamps.isEmpty()) {
                b.append("    <empty>\n");
              }
              for (Map.Entry<String, Long> entry : timestamps) {
                String diff = TimerUtils.formatTimeSince(entry.getValue());
                b.append("    ")
                    .append(state.alreadyKilled.map(strings -> (strings.contains(entry.getKey()) ? "☠ " : "♥ ")).orElse(""))
                    .append(Instant.ofEpochMilli(entry.getValue()).toString())
                    .append("  ")
                    .append(entry.getKey())
                    .append("  (").append(diff).append(" ago)")
                    .append("\n");
              }
            });
            state.nextIndex.ifPresent(nextIndex -> {
              b.append("\n\n  Next Index:\n");
              if (nextIndex.isEmpty()) {
                b.append("    <empty>\n");
              }
              for (Map.Entry<String, Long> entry : nextIndex.entrySet()) {
                b.append("    ").append(entry.getValue()).append("  ").append(entry.getKey()).append("\n");
              }
            });
            state.matchIndex.ifPresent(matchIndex -> {
              b.append("\n\n  Match Index:\n");
              if (matchIndex.isEmpty()) {
                b.append("    <empty>\n");
              }
              for (Map.Entry<String, Long> entry : matchIndex.entrySet()) {
                b.append("    ").append(entry.getValue()).append("  ").append(entry.getKey()).append("\n");
              }
            });
          }

          // 3. Get candidate state, if any
          if (state.isCandidate()) {
            b.append("\n\nCandidate State:\n")
                .append("---------------\n")
                .append("  Votes Received    ").append(state.votesReceived)
            ;
          }

          terminal.dereference().get().println(b.toString());
        } else if (command.startsWith("sum")) {
          String[] parts = command.split(" ");
          int num = Integer.parseInt(parts[1]);
          long delay = parts.length > 2 ? Long.parseLong(parts[2]) : 0;

          Pointer<Integer> count = new Pointer<>();
          for (int i = 0; i < num; i++) {
            long start = System.nanoTime();
            boolean success = raft.withElementAsync("raft_sum", (oldBytes) -> {
              int value = ByteBuffer.wrap(oldBytes).getInt() + 1;
              count.set(value);
              return ByteBuffer.allocate(4).putInt(value).array();
            }, () -> ByteBuffer.allocate(4).putInt(0).array(), true).get(5, TimeUnit.SECONDS);
            long duration = System.nanoTime() - start;

            if (success) {
              terminal.dereference().get().println("["+i+"/"+num+"] Sum "+count.dereference().get()+" took " + ((double)duration/1000000) + "ms");
            }
            else {
              terminal.dereference().get().println("["+i+"/"+num+"] Sum FAILED!");
            }

            if (delay > 0) Uninterruptably.sleep(delay);
          }
        }
      } catch (Exception e) {
        // e.printStackTrace();
      }
    }));
    terminal.dereference().get().open(30, 30, 500, 400);
  }
}
