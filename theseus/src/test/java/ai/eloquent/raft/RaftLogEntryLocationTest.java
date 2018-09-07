package ai.eloquent.raft;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Test {@link RaftLogEntryLocation}
 *
 * @author <a href="mailto:gabor@eloquent.ai">Gabor Angeli</a>
 */
public class RaftLogEntryLocationTest {

  @Test
  public void equalsHashCode() throws Exception {
    RaftLogEntryLocation x = new RaftLogEntryLocation(42, 43);
    RaftLogEntryLocation copy = new RaftLogEntryLocation(42, 43);
    RaftLogEntryLocation other = new RaftLogEntryLocation(42, 44);
    RaftLogEntryLocation other2 = new RaftLogEntryLocation(43, 43);
    assertEquals(x, x);
    assertEquals(x.hashCode(), copy.hashCode());
    assertEquals(x, copy);
    assertEquals(x.hashCode(), copy.hashCode());
    assertNotEquals(x, other);
    assertNotEquals(x, other2);
  }

  @Test
  public void testToString() throws Exception {

    assertEquals("42@43", new RaftLogEntryLocation(42, 43).toString());

  }

}