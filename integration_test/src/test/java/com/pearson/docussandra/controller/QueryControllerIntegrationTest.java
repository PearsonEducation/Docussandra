package com.pearson.docussandra.controller;

import com.pearson.docussandra.abstracttests.AbstractQueryControllerTest;
import com.pearson.docussandra.testhelper.Fixtures;
import com.pearson.docussandra.testhelper.RestExpressManager;

/**
 * Integration tests for the QueryController. The test file to test the queries functionality of
 * docussandra ROUTE : /databases/{database}/tables/{table}/queries
 *
 * @author https://github.com/JeffreyDeYoung
 */
public class QueryControllerIntegrationTest extends AbstractQueryControllerTest {

  /**
   * Constructor. Creates a new implementation of this test to be an integration test (testing
   * against a real, local, Cassandra instance.)
   *
   * @throws Exception
   */
  public QueryControllerIntegrationTest() throws Exception {
    super(Fixtures.getInstance(false));
    RestExpressManager.getManager().ensureRestExpressRunning(false);
  }
}
