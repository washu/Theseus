package ai.eloquent.util;

import com.google.common.io.ByteStreams;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Random;

import static org.junit.Assert.*;

/**
 * Some braindead tests of the zip utilities
 *
 * @author <a href="mailto:gabor@eloquent.ai">Gabor Angeli</a>
 */
public class ZipUtilsTest {

  /**
   * Test zipping and unzipping
   */
  @Test
  public void gzip() {
    byte[] contents = new byte[1024];
    new Random().nextBytes(contents);
    byte[] zipped = ZipUtils.gzip(contents);
    assertArrayEquals(contents, ZipUtils.gunzip(zipped));
    assertNotEquals(contents.length, ZipUtils.gzip(contents).length);
  }


  /**
   * Test zipping and unzipping directly on streams
   */
  @Test
  public void zipStream() throws IOException {
    byte[] contents = new byte[1024];
    new Random().nextBytes(contents);
    InputStream in1 = ZipUtils.zipStream(new ByteArrayInputStream(contents));
    // Presize the ByteArrayOutputStream since we know how large it will need
    // to be, unless that value is less than the default ByteArrayOutputStream
    // size (32).
    ByteArrayOutputStream out1 = new ByteArrayOutputStream(Math.max(32, in1.available()));
    ByteStreams.copy(in1, out1);
    byte[] zipped = out1.toByteArray();
    InputStream in = ZipUtils.unzipStream(ZipUtils.zipStream(new ByteArrayInputStream(contents)));
    // Presize the ByteArrayOutputStream since we know how large it will need
    // to be, unless that value is less than the default ByteArrayOutputStream
    // size (32).
    ByteArrayOutputStream out = new ByteArrayOutputStream(Math.max(32, in.available()));
    ByteStreams.copy(in, out);
    byte[] reread = out.toByteArray();
    assertArrayEquals(contents, reread);
    assertNotEquals(contents.length, zipped.length);
  }
}