package com.pearson.docussandra.controller;

import com.pearson.docussandra.abstracttests.AbstractTableControllerTest;
import com.pearson.docussandra.testhelper.Fixtures;
import com.pearson.docussandra.testhelper.RestExpressManager;

/**
 * Integration test for the TableController. Tests the routes related to tables
 * ROUTE : /databases/{database}/tables/{table}
 *
 * @author https://github.com/JeffreyDeYoung
 */
public class TableControllerIntegrationTest extends AbstractTableControllerTest
{

    /**
     * Constructor. Creates a new implementation of this test to be an
     * integration test (testing against a real, local, Cassandra instance.)
     *
     * @throws Exception
     */
    public TableControllerIntegrationTest() throws Exception
    {
        super(Fixtures.getInstance(false));
        RestExpressManager.getManager().ensureRestExpressRunning(false);
    }

}
