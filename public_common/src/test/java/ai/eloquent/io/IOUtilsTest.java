package ai.eloquent.io;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link IOUtils}.
 *
 * @author <a href="mailto:gabor@eloquent.ai">Gabor Angeli</a>
 */
public class IOUtilsTest {
  /**
   * An SLF4J Logger for this class.
   */
  @SuppressWarnings("unused")
  private static final Logger log = LoggerFactory.getLogger(IOUtilsTest.class);

  /**
   * Test the {@link IOUtils#writeInt(byte[], int, int)} function.
   */
  @Test
  public void writeInt() {
    byte[] buffer = new byte[4];
    // zero
    IOUtils.writeInt(buffer, 0, 0);
    assertEquals(0, IOUtils.readInt(buffer, 0));
    // normal value
    IOUtils.writeInt(buffer, 0, 42424242);
    assertEquals(42424242, IOUtils.readInt(buffer, 0));
    // negative value
    IOUtils.writeInt(buffer, 0, -42424242);
    assertEquals(-42424242, IOUtils.readInt(buffer, 0));
    // min value
    IOUtils.writeInt(buffer, 0, Integer.MIN_VALUE);
    assertEquals(Integer.MIN_VALUE, IOUtils.readInt(buffer, 0));
    // max value
    IOUtils.writeInt(buffer, 0, Integer.MAX_VALUE);
    assertEquals(Integer.MAX_VALUE, IOUtils.readInt(buffer, 0));
  }


  /**
   * Test the {@link IOUtils#writeInt(byte[], int, int)} function on random values.
   */
  @Test
  public void writeIntFuzz() {
    byte[] buffer = new byte[4];
    Random r = new Random();
    for (int i = 0; i < 1000; ++i) {
      int target = r.nextInt();
      IOUtils.writeInt(buffer, 0, target);
      assertEquals(target, IOUtils.readInt(buffer, 0));
    }
  }


  /**
   * Test the {@link IOUtils#writeInt(byte[], int, int)} function
   * at a random point in the buffer.
   */
  @Test
  public void writeIntMiddleOfStream() {
    byte[] buffer = new byte[100];
    // zero
    // normal value
    IOUtils.writeInt(buffer, 42, 42424242);
    assertEquals(42424242, IOUtils.readInt(buffer, 42));
    // Everything else is 0
    for (int i = 0; i < 42; ++i) {
      assertEquals(0, buffer[i]);
    }
    for (int i = 42 + 4; i < 100; ++i) {
      assertEquals(0, buffer[i]);
    }
  }


  /**
   * Test the {@link IOUtils#writeLong(byte[], int, long)} function.
   */
  @Test
  public void writeLong() {
    byte[] buffer = new byte[8];
    // zero
    IOUtils.writeLong(buffer, 0, 0);
    assertEquals(0, IOUtils.readLong(buffer, 0));
    // normal value
    IOUtils.writeLong(buffer, 0, 4242424242L);
    assertEquals(4242424242L, IOUtils.readLong(buffer, 0));
    // negative value
    IOUtils.writeLong(buffer, 0, -4242424242L);
    assertEquals(-4242424242L, IOUtils.readLong(buffer, 0));
    // min value
    IOUtils.writeLong(buffer, 0, Long.MIN_VALUE);
    assertEquals(Long.MIN_VALUE, IOUtils.readLong(buffer, 0));
    // max value
    IOUtils.writeLong(buffer, 0, Long.MAX_VALUE);
    assertEquals(Long.MAX_VALUE, IOUtils.readLong(buffer, 0));
  }


  /**
   * Test the {@link IOUtils#writeLong(byte[], int, long)} function on random values.
   */
  @Test
  public void writLongFuzz() {
    byte[] buffer = new byte[8];
    Random r = new Random();
    for (int i = 0; i < 1000; ++i) {
      long target = r.nextLong();
      IOUtils.writeLong(buffer, 0, target);
      assertEquals(target, IOUtils.readLong(buffer, 0));
    }
  }


  /**
   * Test the {@link IOUtils#writeLong(byte[], int, long)} function
   * at a random point in the buffer.
   */
  @Test
  public void writeLongMiddleOfStream() {
    byte[] buffer = new byte[100];
    // normal value
    IOUtils.writeLong(buffer, 42, 4242424242L);
    assertEquals(4242424242L, IOUtils.readLong(buffer, 42));
    // Everything else is 0
    for (int i = 0; i < 42; ++i) {
      assertEquals(0, buffer[i]);
    }
    for (int i = 42 + 8; i < 100; ++i) {
      assertEquals(0, buffer[i]);
    }
  }


  /**
   * Test the {@link IOUtils#writeString(byte[], int, String)} function.
   */
  @Test
  public void writeString() {
    byte[] buffer = new byte[IOUtils.stringSerializedSize("foo")];
    IOUtils.writeString(buffer, 0, "foo");
    assertEquals("foo", IOUtils.readString(buffer, 0));
  }


  /**
   * Test the {@link IOUtils#writeString(byte[], int, String)} function
   * on UTF characters.
   */
  @Test
  public void writeStringUTF() {
    String[] testCases = new String[]{
        "Ḽơᶉëᶆ ȋṕšᶙṁ ḍỡḽǭᵳ ʂǐť ӓṁệẗ, ĉṓɲṩḙċťᶒțûɾ ấɖḯƥĭṩčįɳġ ḝłįʈ, șếᶑ ᶁⱺ ẽḭŭŝḿꝋď ṫĕᶆᶈṓɍ ỉñḉīḑȋᵭṵńť ṷŧ ḹẩḇőꝛế éȶ đꝍꞎôꝛȇ ᵯáꞡᶇā ąⱡîɋṹẵ.",
        "����������������������������������������������������������������"
    };
    for (String str : testCases) {
      byte[] buffer = new byte[IOUtils.stringSerializedSize(str)];
      IOUtils.writeString(buffer, 0, str);
      assertEquals(str, IOUtils.readString(buffer, 0));
    }
  }
}
