package com.pearson.docussandra.controller;

import com.pearson.docussandra.abstracttests.AbstractTableControllerTest;
import com.pearson.docussandra.testhelper.Fixtures;
import com.pearson.docussandra.testhelper.RestExpressManager;

/**
 * Functional tests for the table controller class. ROUTE : /databases/{database}/tables/{table}
 *
 * @author https://github.com/JeffreyDeYoung
 */
public class TableControllerTest extends AbstractTableControllerTest {

  /**
   * Constructor. Creates a new implementation of this test to be a functional test (testing against
   * a mock Cassandra).
   *
   * @throws Exception
   */
  public TableControllerTest() throws Exception {
    super(Fixtures.getInstance(true));
    RestExpressManager.getManager().ensureRestExpressRunning(true);
  }
}
