package com.pearson.docussandra.controller;

import com.pearson.docussandra.abstracttests.AbstractDocumentControllerTest;
import com.pearson.docussandra.testhelper.Fixtures;
import com.pearson.docussandra.testhelper.RestExpressManager;

/**
 * Functional tests for the document controller class. ROUTE :
 * /databases/{database}/tables/{table}/documents
 *
 * @author https://github.com/JeffreyDeYoung
 */
public class DocumentControllerTest extends AbstractDocumentControllerTest
{

    /**
     * Constructor. Creates a new implementation of this test to be a functional
     * test (testing against a mock Cassandra).
     *
     * @throws Exception
     */
    public DocumentControllerTest() throws Exception
    {
        super(Fixtures.getInstance(true));
        RestExpressManager.getManager().ensureRestExpressRunning(true);
    }

}
