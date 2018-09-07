package ai.eloquent.raft.transport;

import ai.eloquent.raft.LocalTransport;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An abstract class that creates and cleans up a local transport as appropriate
 *
 * @author <a href="mailto:gabor@eloquent.ai">Gabor Angeli</a>
 */
public class WithLocalTransport {
  /**
   * An SLF4J Logger for this class.
   */
  private static final Logger log = LoggerFactory.getLogger(WithLocalTransport.class);

  protected LocalTransport transport;


  @Before
  public void mkTransport() {
    log.info("Creating mock transport");
    this.transport = new LocalTransport(true);
  }


  @After
  public void killTransport() {
    this.transport.stop();
    log.info("Killed mock transport");
  }


}
