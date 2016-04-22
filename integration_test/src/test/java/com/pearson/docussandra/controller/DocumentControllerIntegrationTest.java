package com.pearson.docussandra.controller;

import com.pearson.docussandra.abstracttests.AbstractDocumentControllerTest;
import com.pearson.docussandra.testhelper.Fixtures;
import com.pearson.docussandra.testhelper.RestExpressManager;

/**
 * Integration tests for the documents routes. ROUTE :
 * /databases/{database}/tables/{table}/documents
 *
 * @author https://github.com/JeffreyDeYoung
 */
public class DocumentControllerIntegrationTest extends AbstractDocumentControllerTest
{

    /**
     * Constructor. Creates a new implementation of this test to be an
     * integration test (testing against a real, local, Cassandra instance.)
     *
     * @throws Exception
     */
    public DocumentControllerIntegrationTest() throws Exception
    {
        super(Fixtures.getInstance(false));
        RestExpressManager.getManager().ensureRestExpressRunning(false);
    }

}
