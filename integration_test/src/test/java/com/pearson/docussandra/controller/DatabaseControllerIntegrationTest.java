package com.pearson.docussandra.controller;

import com.pearson.docussandra.abstracttests.AbstractDatabaseControllerTest;
import com.pearson.docussandra.testhelper.Fixtures;
import testhelper.RestExpressManager;

/**
 * Integration test file for testing the database routes. ROUTE: /databases/
 *
 * @author https://github.com/JeffreyDeYoung
 */
public class DatabaseControllerIntegrationTest extends AbstractDatabaseControllerTest
{

    /**
     * Constructor. Creates a new implementation of this test to be an
     * integration test (testing against a real, local, Cassandra instance.)
     *
     * @throws Exception
     */
    public DatabaseControllerIntegrationTest() throws Exception
    {
        super(Fixtures.getInstance(false));
        RestExpressManager.getManager().ensureRestExpressRunning(false);
    }
}
