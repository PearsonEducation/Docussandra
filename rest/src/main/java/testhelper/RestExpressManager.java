
package testhelper;

import com.pearson.docussandra.Main;
import java.io.IOException;
import org.restexpress.RestExpress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for managing testing of RestExpress so we are not tearing down and
 * creating new server instances with every tests.
 *
 * For test use only!
 *
 * The alternate strange reason for creating this is that it seems once our
 * RestExpress server is started up, it won't let go of the HyperExpress plugin
 * and will disallow another RestExpress instance to startup, even after the
 * first has been shutdown.
 *
 * Generally, the usage is as follows:
 * RestExpressManager.getManager().ensureRestExpressRunning();
 *
 * @author https://github.com/JeffreyDeYoung
 */
public class RestExpressManager
{

    private final Logger LOGGER = LoggerFactory.getLogger(RestExpressManager.class);

    private static RestExpressManager manager = null;

    private static boolean restExpressRunning = false;

    private static RestExpress server;

    /**
     * Singleton.
     */
    private RestExpressManager()
    {
        ;
    }

    /**
     * Gets an instance of this class.
     *
     * @return
     */
    public static synchronized RestExpressManager getManager()
    {
        if (manager == null)
        {
            manager = new RestExpressManager();
        }
        return manager;
    }

    /**
     * Ensures RestExpress is presently running.
     *
     * Cassandra will be mocked instead of relying on a external process.
     *
     * @throws IOException
     */
    public synchronized void ensureRestExpressRunning() throws IOException
    {
        ensureRestExpressRunning(true);//default to mocking cassandra
    }

    /**
     * Ensures RestExpress is presently running.
     *
     * @param mockCassandra If true, Cassandra will be mocked instead of relying
     * on a external process.
     *
     * @throws IOException
     */
    public synchronized void ensureRestExpressRunning(boolean mockCassandra) throws IOException
    {
        if (restExpressRunning == false)
        {
            LOGGER.info("Starting RestExpress server...");
            if (mockCassandra)
            {
                String[] params = new String[1];
                params[0] = "local_test";
                server = Main.initializeServer(params);
            } else
            {
                server = Main.initializeServer(new String[0]);
            }
            restExpressRunning = true;
        }
    }

    /**
     * Shuts down the rest express instance.
     *
     * @throws Throwable
     */
    @Override
    protected void finalize() throws Throwable
    {
        super.finalize();
        server.shutdown();
        restExpressRunning = false;
    }

}
