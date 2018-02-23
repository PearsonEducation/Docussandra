/*
 * Copyright 2015/2016 https://github.com/JeffreyDeYoung.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.docussandra.testhelpers;

import com.pearson.docussandra.Main;
import com.pearson.docussandra.testhelper.Fixtures;
import java.io.IOException;
import org.restexpress.RestExpress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for managing testing of startup of Docussandra so we are not tearing down and creating new
 * RestExpress server instances with every test.
 *
 * Note, you can only create this object once; choose your init parameters carefully. Cannot be run
 * after this test can not run in the same JVM as a RestExpressManager instance for this reason.
 *
 * For test use only!
 *
 * The alternate strange reason for creating this is that it seems once our RestExpress server is
 * started up, it won't let go of the HyperExpress plugin and will disallow another RestExpress
 * instance to startup, even after the first has been shutdown.
 *
 * Generally, the usage is as follows:
 * TestDocussandraManger.getManager().ensureTestDocussandraRunning();
 *
 * @author https://github.com/JeffreyDeYoung
 */
public class TestDocussandraManager {


  /**
   * Logger for this class.
   */
  private static final Logger LOGGER = LoggerFactory.getLogger(TestDocussandraManager.class);

  /**
   * Instance. (Singleton.)
   */
  private static TestDocussandraManager manager = null;

  /**
   * Running flag.
   */
  private static boolean restExpressRunning = false;

  /**
   * Rest express server for Docussandra.
   */
  private static RestExpress server;

  /**
   * Singleton.
   */
  private TestDocussandraManager() {
    ;
  }

  /**
   * Gets an instance of this class.
   *
   * @return
   */
  public static synchronized TestDocussandraManager getManager() {
    if (manager == null) {
      manager = new TestDocussandraManager();
    }
    return manager;
  }


  /**
   * Ensures Docussandra is presently running. The rest endpoints will be exposed on port 19080.
   *
   * Cassandra will be mocked internally instead of relying on a external process; no data will be
   * saved past the JVM shutdown.
   *
   * @throws IOException
   */
  public synchronized void ensureTestDocussandraRunning() throws Exception {
    ensureTestDocussandraRunning(true);// default to mocking cassandra
  }

  /**
   * Ensures Docussandra is presently running. The rest endpoints will be exposed on port 19080.
   *
   * @param mockCassandra If true, Cassandra will be mocked instead of relying on a external
   *        process. If false, this test instance of Docussandra will hit an internal mocked version
   *        of Cassandra instead; no data will be saved past the JVM shutdown.
   *
   * @throws IOException
   */
  public synchronized void ensureTestDocussandraRunning(boolean mockCassandra) throws Exception {
    if (restExpressRunning == false) {
      LOGGER.info("Starting RestExpress server...");
      if (mockCassandra) {
        Fixtures.ensureMockCassandraRunningAndEstablished("docussandra");
        String[] params = new String[1];
        params[0] = "local_test";
        server = Main.initializeServer(params, null);
      } else {
        server = Main.initializeServer(new String[0], null);
      }
      restExpressRunning = true;
    }
  }

  /**
   * Ensures Docussandra is presently running. The rest endpoints will be exposed on port 19080.
   *
   * @param cassandraSeeds Cassandra seeds to use when starting up RestExpress. Will override any
   *        existing seeds.
   *
   * @throws IOException
   */
  public synchronized void ensureTestDocussandraRunning(String cassandraSeeds)
      throws IOException, IllegalAccessException, InstantiationException {
    if (restExpressRunning == false) {
      LOGGER.info("Starting RestExpress server...");
      if (cassandraSeeds != null) {
        String[] params = new String[1];
        params[0] = "local_test";
        server = Main.initializeServer(params, cassandraSeeds);
      } else {
        server = Main.initializeServer(new String[0], null);
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
  public void finalize() throws Throwable {
    super.finalize();
    if (server != null) {
      server.shutdown(true);
    }
    restExpressRunning = false;
  }

}
