package com.pearson.docussandra.controller;

import com.pearson.docussandra.abstracttests.AbstractIndexControllerTest;
import com.pearson.docussandra.testhelper.Fixtures;
import com.pearson.docussandra.testhelper.RestExpressManager;

/**
 * Functional tests for the index controller
 * @author https://github.com/JeffreyDeYoung
 */
public class IndexControllerTest extends AbstractIndexControllerTest
{

    /**
     * Constructor. Creates a new implementation of this test to be a functional
     * test (testing against a mock Cassandra).
     *
     * @throws Exception
     */    
    public IndexControllerTest() throws Exception
    {
        super(Fixtures.getInstance());
        RestExpressManager.getManager().ensureRestExpressRunning();
    }

}
