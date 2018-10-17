package ai.eloquent.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.zip.DeflaterInputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.InflaterInputStream;

/**
 * Some utilities with working with GZip streams
 *
 * @author <a href="mailto:gabor@eloquent.ai">Gabor Angeli</a>
 */
public class ZipUtils {
  /**
   * An SLF4J Logger for this class.
   */
  private static final Logger log = LoggerFactory.getLogger(ZipUtils.class);


  /**
   * Zip a byte stream.
   */
  public static byte[] gzip(byte[] unzipped) throws RuntimeIOException {
    try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      try (GZIPOutputStream stream = new GZIPOutputStream(out)) {
        stream.write(unzipped);
      } catch (IOException e) {
        log.error("gzip threw an exception on a byte array stream", e);
        throw new RuntimeIOException(e);
      }
      return out.toByteArray();
    } catch (IOException e) {
      log.error("Could not close byte array output stream", e);
      throw new RuntimeIOException(e);
    }
  }


  /**
   * Zip a byte stream.
   */
  public static InputStream zipStream(InputStream unzipped) {
    return new DeflaterInputStream(unzipped);
  }


  /**
   * Unzip a byte stream.
   */
  public static InputStream unzipStream(InputStream zipped) {
    return new InflaterInputStream(zipped);
  }


  /** Copy from an input stream to an output stream */
  public static long copy(InputStream src, OutputStream sink) throws IOException {
    byte[] buf = new byte[1024];
    long total = 0L;
    while(true) {
      int count = src.read(buf);
      if (count == -1) {
        return total;
      }
      sink.write(buf, 0, count);
      total += (long) count;
    }
  }

  /**
   * Unzip a byte stream.
   */
  public static byte[] gunzip(byte[] zipped) throws RuntimeIOException {
    try (ByteArrayInputStream in = new ByteArrayInputStream(zipped)) {
      try (GZIPInputStream stream = new GZIPInputStream(in)) {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream(Math.max(32, stream.available()))) {
          copy(stream, out);
          return out.toByteArray();
        }
      }
    } catch (IOException e) {
      log.error("gunzip threw an exception on a byte array stream", e);
      throw new RuntimeIOException(e);
    }
  }


  /**
   * Unzip a byte stream.
   */
  public static byte[] gunzip(ByteArrayInputStream in) throws RuntimeIOException {
    try (GZIPInputStream stream = new GZIPInputStream(in)) {
      try (ByteArrayOutputStream out = new ByteArrayOutputStream(Math.max(32, stream.available()))) {
        copy(stream, out);
        return out.toByteArray();
      }
    } catch (IOException e) {
      log.error("gunzip threw an exception on a byte array stream", e);
      throw new RuntimeIOException(e);
    }
  }

}
