package com.pearson.docussandra.controller;

import com.pearson.docussandra.abstracttests.AbstractQueryControllerTest;
import com.pearson.docussandra.testhelper.Fixtures;
import com.pearson.docussandra.testhelper.RestExpressManager;

/**
 * Functional tests for the query controller class.
 *
 * The test file to test the queries functionality of docussandra ROUTE :
 * /databases/{database}/tables/{table}/queries
 *
 *
 * @author https://github.com/JeffreyDeYoung
 */
public class QueryControllerTest extends AbstractQueryControllerTest {

  /**
   * Constructor. Creates a new implementation of this test to be a functional test (testing against
   * a mock Cassandra).
   *
   * @throws Exception
   */
  public QueryControllerTest() throws Exception {
    super(Fixtures.getInstance(true));
    RestExpressManager.getManager().ensureRestExpressRunning(true);
  }
}
