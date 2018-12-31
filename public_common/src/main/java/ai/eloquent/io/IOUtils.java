package ai.eloquent.io;

import ai.eloquent.util.RuntimeIOException;
import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;


/**
 * Helper Class for various I/O related things.
 */

public class IOUtils  {

  private static final int SLURP_BUFFER_SIZE = 16384;

  // A class of static methods
  private IOUtils() { }

  /**
   * Open a BufferedReader to a file, class path entry or URL specified by a String name.
   * If the String starts with https?://, then it is first tried as a URL. It
   * is next tried as a resource on the CLASSPATH, and then it is tried
   * as a local file. Finally, it is then tried again in case it is some network-available
   * file accessible by URL. If the String ends in .gz, it
   * is interpreted as a gzipped file (and uncompressed). The file is then
   * interpreted as a utf-8 text file.
   * Note that this method uses the ClassLoader methods, so that classpath resources must be specified as
   * absolute resource paths without a leading "/".
   *
   * @param textFileOrUrl What to read from
   * @return The BufferedReader
   * @throws IOException If there is an I/O problem
   */
  public static BufferedReader readerFromString(String textFileOrUrl)
      throws IOException {
    return new BufferedReader(new InputStreamReader(
        getInputStreamFromURLOrClasspathOrFileSystem(textFileOrUrl), "UTF-8"));
  }

  /**
   * Open a BufferedReader to a file or URL specified by a String name. If the
   * String starts with https?://, then it is first tried as a URL, otherwise it
   * is next tried as a resource on the CLASSPATH, and then finally it is tried
   * as a local file or other network-available file . If the String ends in .gz, it
   * is interpreted as a gzipped file (and uncompressed), else it is interpreted as
   * a regular text file in the given encoding.
   * If the encoding passed in is null, then the system default encoding is used.
   *
   * @param textFileOrUrl What to read from
   * @param encoding CharSet encoding. Maybe be null, in which case the
   *         platform default encoding is used
   * @return The BufferedReader
   * @throws IOException If there is an I/O problem
   */
  public static BufferedReader readerFromString(String textFileOrUrl,
                                                String encoding) throws IOException {
    InputStream is = getInputStreamFromURLOrClasspathOrFileSystem(textFileOrUrl);
    if (encoding == null) {
      return new BufferedReader(new InputStreamReader(is));
    }
    return new BufferedReader(new InputStreamReader(is, encoding));
  }

  /**
   * Returns all the text from the given Reader.
   * Closes the Reader when done.
   *
   * @return The text in the file.
   */
  public static String slurpReader(Reader reader) {
    StringBuilder buff = new StringBuilder();
    try (BufferedReader r = new BufferedReader(reader)) {
      char[] chars = new char[SLURP_BUFFER_SIZE];
      while (true) {
        int amountRead = r.read(chars, 0, SLURP_BUFFER_SIZE);
        if (amountRead < 0) {
          break;
        }
        buff.append(chars, 0, amountRead);
      }
    } catch (Exception e) {
      throw new RuntimeIOException("slurpReader IO problem", e);
    }
    return buff.toString();
  }

  /**
   * Locates this file either using the given URL, or in the CLASSPATH, or in the file system
   * The CLASSPATH takes priority over the file system!
   * This stream is buffered and gunzipped (if necessary).
   *
   * @param textFileOrUrl The String specifying the URL/resource/file to load
   * @return An InputStream for loading a resource
   * @throws IOException On any IO error
   * @throws NullPointerException Input parameter is null
   */
  public static InputStream getInputStreamFromURLOrClasspathOrFileSystem(String textFileOrUrl)
      throws IOException, NullPointerException {
    InputStream in;
    if (textFileOrUrl == null) {
      throw new NullPointerException("Attempt to open file with null name");
    } else if (textFileOrUrl.matches("https?://.*")) {
      URL u = new URL(textFileOrUrl);
      URLConnection uc = u.openConnection();
      in = uc.getInputStream();
    } else {
      try {
        in = findStreamInClasspathOrFileSystem(textFileOrUrl);
      } catch (FileNotFoundException e) {
        try {
          // Maybe this happens to be some other format of URL?
          URL u = new URL(textFileOrUrl);
          URLConnection uc = u.openConnection();
          in = uc.getInputStream();
        } catch (IOException e2) {
          // Don't make the original exception a cause, since it is usually bogus
          throw new IOException("Unable to open \"" +
              textFileOrUrl + "\" as " + "class path, filename or URL"); // , e2);
        }
      }
    }

    // If it is a GZIP stream then ungzip it
    if (textFileOrUrl.endsWith(".gz")) {
      try {
        in = new GZIPInputStream(in);
      } catch (Exception e) {
        throw new RuntimeIOException("Resource or file looks like a gzip file, but is not: " + textFileOrUrl, e);
      }
    }

    // buffer this stream.  even gzip streams benefit from buffering,
    // such as for the shift reduce parser [cdm 2016: I think this is only because default buffer is small; see below]
    in = new BufferedInputStream(in);

    return in;
  }


  /**
   * Locates this file either in the CLASSPATH or in the file system. The CLASSPATH takes priority.
   * Note that this method uses the ClassLoader methods, so that classpath resources must be specified as
   * absolute resource paths without a leading "/".
   *
   * @param name The file or resource name
   * @throws FileNotFoundException If the file does not exist
   * @return The InputStream of name, or null if not found
   */
  private static InputStream findStreamInClasspathOrFileSystem(String name) throws FileNotFoundException {
    // ms 10-04-2010:
    // - even though this may look like a regular file, it may be a path inside a jar in the CLASSPATH
    // - check for this first. This takes precedence over the file system.
    InputStream is = IOUtils.class.getClassLoader().getResourceAsStream(name);
    // windows File.separator is \, but getting resources only works with /
    if (is == null) {
      is = IOUtils.class.getClassLoader().getResourceAsStream(name.replaceAll("\\\\", "/"));
      // Classpath doesn't like double slashes (e.g., /home/user//foo.txt)
      if (is == null) {
        is = IOUtils.class.getClassLoader().getResourceAsStream(name.replaceAll("\\\\", "/").replaceAll("/+", "/"));
      }
    }
    // if not found in the CLASSPATH, load from the file system
    if (is == null) {
      is = new FileInputStream(name);
    }
    return is;
  }


  /**
   * Writes a string to a file.
   *
   * @param contents The string to write
   * @param path The file path
   * @param encoding The encoding to encode in
   * @throws IOException In case of failure
   */
  public static void writeStringToFile(String contents, String path, String encoding) throws IOException {
    OutputStream writer = getBufferedOutputStream(path);
    writer.write(contents.getBytes(encoding));
    writer.close();
  }


  private static OutputStream getBufferedOutputStream(String path) throws IOException {
    OutputStream os = new BufferedOutputStream(new FileOutputStream(path));
    if (path.endsWith(".gz")) {
      os = new GZIPOutputStream(os);
    }
    return os;
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