package com.pearson.docussandra.controller;

import com.pearson.docussandra.abstracttests.AbstractDatabaseControllerTest;
import com.pearson.docussandra.testhelper.Fixtures;
import testhelper.RestExpressManager;

/**
 * Functional tests for the database controller tests.
 * ROUTE: /databases/
 *
 * @author https://github.com/JeffreyDeYoung
 */
public class DatabaseControllerTest extends AbstractDatabaseControllerTest
{
    /**
     * Constructor. Creates a new implementation of this test to be a functional
     * test (testing against a mock Cassandra).
     *
     * @throws Exception
     */
    public DatabaseControllerTest() throws Exception
    {
        super(Fixtures.getInstance(true));
        RestExpressManager.getManager().ensureRestExpressRunning(true);
    }
}
