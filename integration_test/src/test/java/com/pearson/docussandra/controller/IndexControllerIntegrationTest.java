package com.pearson.docussandra.controller;

import com.pearson.docussandra.abstracttests.AbstractIndexControllerTest;
import com.pearson.docussandra.testhelper.Fixtures;
import com.pearson.docussandra.testhelper.RestExpressManager;

/**
 * Integration tests for the IndexController.
 *
 * @author https://github.com/JeffreyDeYoung
 */
public class IndexControllerIntegrationTest extends AbstractIndexControllerTest {

  /**
   * Constructor. Creates a new implementation of this test to be an integration test (testing
   * against a real, local, Cassandra instance.)
   *
   * @throws Exception
   */
  public IndexControllerIntegrationTest() throws Exception {
    super(Fixtures.getInstance(false));
    RestExpressManager.getManager().ensureRestExpressRunning(false);
  }

}
