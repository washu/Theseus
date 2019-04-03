package ai.eloquent.io;

import ai.eloquent.util.RuntimeIOException;
import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;


/**
 * Helper Class for various I/O related things.
 */

public class IOUtils  {

  // A class of static methods
  private IOUtils() { }

  /**
   * Get a reader to the given file or classpath entry.
   *
   * @param path The path to the file, either in the classpath or on the filesystem.
   *
   * @return A buffered reader to the file.
   *
   * @throws IOException Thrown if we could not read the file
   */
  public static BufferedReader getReader(String path) throws IOException {
    // 1. Clean the path
    path = path.replaceAll("/+", "/");  // just in case

    // 2. Try to read from the classpath
    InputStream fromClasspath = IOUtils.class.getClassLoader().getResourceAsStream(path);
    if (fromClasspath != null) {
      return new BufferedReader(new InputStreamReader(fromClasspath));
    } else {
      // 3. Read from a file
      if (path.endsWith(".gz")) {
        return new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(path))));  // note[gabor]: Jesus Christ, Java
      } else {
        return new BufferedReader(new FileReader(path));
      }
    }
  }


  /**
   * Fully read a the contents of a reader into a Java String.
   */
  public static String slurp(Reader reader) {
    StringBuilder b = new StringBuilder();
    char[] buffer = new char[0x1<<19];
    try {
      int read;
      while ((read = reader.read(buffer)) > 0) {
        b.append(buffer, 0, read);
      }
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
    return b.toString();
  }


  /**
   * Writes some String contents to a file.
   *
   * @param contents The contents to write.
   * @param path The path of the file to write to.
   *
   * @throws IOException If we couldn't write the contents.
   */
  public static void writeToFile(String contents, String path, Charset encoding) throws IOException {
    OutputStream os;
    if (path.endsWith(".gz")) {
      os = new GZIPOutputStream(new FileOutputStream(path));
    } else {
      os = new FileOutputStream(path);
    }
    os.write(contents.getBytes(encoding));
  }


  /**
   * The amount of space a string will take if we call
   * {@link #writeString(byte[], int, String)}.
   *
   * @param str The string we want to write.
   *
   * @return The size of the string, serialized to the byte array.
   */
  public static int stringSerializedSize(String str) {
    return str.length() * 2 + 4;
  }


  /**
   * Write a string to a buffer, starting at the specified position.
   * Symmetric with {@link #readString(byte[], int)}.
   *
   * @param buffer The buffer we're writing to
   * @param begin The position to write to.
   * @param value The value we're writing.
   *
   * @return The byte length of the written string in the buffer.
   */
  public static int writeString(byte[] buffer, int begin, String value) {
    int position = begin;
    writeInt(buffer, position, value.length());
    position += 4;
    int length = value.length();
    for (int i = 0; i < length; ++i) {
      char c = value.charAt(i);
      buffer[position++] = (byte) (c >>> 8);
      buffer[position++] = (byte) (c & 0xff);
    }
    return position - begin;
  }


  /**
   * Read a string from a byte buffer, starting at the given position.
   * This is symmetric with {@link #writeString(byte[], int, String)}.
   *
   * @param buffer The position we're reading from.
   * @param position The position to start reading from.
   *
   * @return The read string.
   */
  public static String readString(byte[] buffer, int position) {
    int length = readInt(buffer, position);
    position += 4;
    return new String(buffer, position, length * 2, StandardCharsets.UTF_16);
  }


  /**
   * Write an integer to a byte buffer.
   * Symmetric to  {@link #readInt(byte[], int}.
   *
   * @param buffer The buffer we're writing to
   * @param position The position to write to.
   * @param value The value we're writing.
   */
  public static void writeInt(byte[] buffer, int position, int value) {
    buffer[position] = (byte) (value & 0xff);
    buffer[position + 1] = (byte) ((value & 0xff00) >>> 8);
    buffer[position + 2] = (byte) ((value & 0xff0000) >>> 16);
    buffer[position + 3] = (byte) ((value & 0xff000000) >>> 24);
  }


  /**
   * Read an integer from a byte buffer.
   * Symmetric to {@link #writeInt(byte[], int, int)}.}
   *
   * @param buffer The buffer we're reading from.
   * @param position The position to read from.
   *
   * @return The read integer.
   */
  public static int readInt(byte[] buffer, int position) {
    int v = Byte.toUnsignedInt(buffer[position]);
    v |= (Byte.toUnsignedInt(buffer[position + 1]) << 8);
    v |= (Byte.toUnsignedInt(buffer[position + 2]) << 16);
    v |= (Byte.toUnsignedInt(buffer[position + 3]) << 24);
    return v;
  }


  /**
   * Write a long to a byte buffer.
   * Symmetric to  {@link #readLong(byte[], int}.
   *
   * @param buffer The buffer we're writing to
   * @param position The position to write to.
   * @param value The value we're writing.
   */
  public static void writeLong(byte[] buffer, int position, long value) {
    buffer[position] = (byte) (value & 0xff);
    buffer[position + 1] = (byte) ((value & 0xff00L) >>> 8);
    buffer[position + 2] = (byte) ((value & 0xff0000L) >>> 16);
    buffer[position + 3] = (byte) ((value & 0xff000000L) >>> 24);
    buffer[position + 4] = (byte) ((value & 0xff00000000L) >>> 32);
    buffer[position + 5] = (byte) ((value & 0xff0000000000L) >>> 40);
    buffer[position + 6] = (byte) ((value & 0xff000000000000L) >>> 48);
    buffer[position + 7] = (byte) (value >>> 56);
  }


  /**
   * Read a long from a byte buffer.
   * Symmetric to {@link #writeLong(byte[], int, long)}.}
   *
   * @param buffer The buffer we're reading from.
   * @param position The position to read from.
   *
   * @return The read integer.
   */
  public static long readLong(byte[] buffer, int position) {
    long v = Byte.toUnsignedLong(buffer[position]);
    v |= (Byte.toUnsignedLong(buffer[position + 1]) << 8);
    v |= (Byte.toUnsignedLong(buffer[position + 2]) << 16);
    v |= (Byte.toUnsignedLong(buffer[position + 3]) << 24);
    v |= (Byte.toUnsignedLong(buffer[position + 4]) << 32);
    v |= (Byte.toUnsignedLong(buffer[position + 5]) << 40);
    v |= (Byte.toUnsignedLong(buffer[position + 6]) << 48);
    v |= (Byte.toUnsignedLong(buffer[position + 7]) << 56);
    return v;
  }

}