
package com.pearson.docussandra.service;

import com.pearson.docussandra.domain.objects.Document;
import com.pearson.docussandra.exception.FieldNotIndexedException;
import com.pearson.docussandra.exception.IndexParseException;
import com.pearson.docussandra.persistence.impl.*;
import com.pearson.docussandra.testhelper.Fixtures;
import java.util.List;
import org.bson.BSONObject;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author https://github.com/JeffreyDeYoung
 */
public class QueryServiceTest
{

    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private QueryService instance;
    private static Fixtures f;

    public QueryServiceTest() throws Exception
    {
        f = Fixtures.getInstance();
    }

    @Before
    public void setUp()
    {
        f.clearTestTables();
        f.createTestITables();
        instance = new QueryService(new DatabaseRepositoryImpl(f.getSession()), new TableRepositoryImpl(f.getSession()), new QueryRepositoryImpl(f.getSession()));
        IndexRepositoryImpl indexRepo = new IndexRepositoryImpl(f.getSession());
        indexRepo.create(Fixtures.createTestIndexTwoField());
    }

    @AfterClass
    public static void tearDown()
    {
        f.clearTestTables();
    }

    /**
     * Test of query method, of class QueryService.
     */
    @Test
    public void testQuery() throws IndexParseException, FieldNotIndexedException
    {
        logger.debug("query");
        Document doc = Fixtures.createTestDocument();
        //put a test doc in
        DocumentRepositoryImpl docRepo = new DocumentRepositoryImpl(f.getSession());
        docRepo.create(doc);
        List<Document> result = instance.query(Fixtures.DB, Fixtures.createTestQuery());
        assertNotNull(result);
        assertTrue(!result.isEmpty());
        assertTrue(result.size() == 1);
        Document res = result.get(0);
        assertNotNull(res);
        assertNotNull(res.getCreatedAt());
        assertNotNull(res.getUpdatedAt());
        assertNotNull(res.getUuid());
        assertNotNull(res.getId());
        assertNotNull(res.object());
        BSONObject expected = doc.object();
        BSONObject actual = res.object();
        assertEquals(expected, actual);
    }

}
