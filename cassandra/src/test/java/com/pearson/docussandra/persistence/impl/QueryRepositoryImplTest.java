
package com.pearson.docussandra.persistence.impl;

import com.pearson.docussandra.domain.ParsedQuery;
import com.pearson.docussandra.domain.objects.Document;
import com.pearson.docussandra.domain.objects.QueryResponseWrapper;
import com.pearson.docussandra.exception.IndexParseException;
import com.pearson.docussandra.persistence.DocumentRepository;
import com.pearson.docussandra.persistence.QueryRepository;
import com.pearson.docussandra.testhelper.Fixtures;
import java.util.List;
import org.bson.BSONObject;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author https://github.com/JeffreyDeYoung
 */
public class QueryRepositoryImplTest {

  private Logger logger = LoggerFactory.getLogger(this.getClass());
  private static Fixtures f;

  public QueryRepositoryImplTest() throws Exception {
    f = Fixtures.getInstance();
  }

  @BeforeClass
  public static void setUpClass() {}

  @AfterClass
  public static void tearDownClass() {
    f.clearTestTables();
  }

  @Before
  public void setUp() {
    f.clearTestTables();
    f.createTestITables();
  }

  @After
  public void tearDown() {

  }

  /**
   * Test of query method, of class QueryDao.
   */
  @Test
  public void testDoQueryNoResults() throws IndexParseException {
    logger.debug("testDoQueryNoResults");
    QueryRepository instance = new QueryRepositoryImpl(f.getSession());
    QueryResponseWrapper result = instance.query(Fixtures.createTestParsedQuery());
    assertNotNull(result);
    assertTrue(result.isEmpty());// no data yet, should get an empty set
  }

  /**
   * Test of query method, of class QueryDao.
   */
  @Test
  public void testDoQueryWithResults() throws IndexParseException {
    logger.debug("testDoQueryWithResults");
    Document doc = Fixtures.createTestDocument();
    // put a test doc in
    DocumentRepository docRepo = new DocumentRepositoryImpl(f.getSession());
    docRepo.create(doc);
    QueryRepositoryImpl instance = new QueryRepositoryImpl(f.getSession());
    QueryResponseWrapper result = instance.query(Fixtures.createTestParsedQuery());
    assertNotNull(result);
    assertTrue(!result.isEmpty());
    assertTrue(result.size() == 1);
    Document res = result.get(0);
    assertNotNull(res);
    assertNotNull(res.getCreatedAt());
    assertNotNull(res.getUpdatedAt());
    assertNotNull(res.getUuid());
    assertNotNull(res.getId());
    assertNotNull(res.getObject());
    BSONObject expected = doc.getObject();
    BSONObject actual = res.getObject();
    assertEquals(expected, actual);
  }

  /**
   * Test of query method, of class QueryDao.
   */
  @Test
  public void testDoQueryWithDataButNoResults() throws IndexParseException {
    System.out.println("testDoQueryWithDataButNoResults");
    Document doc = Fixtures.createTestDocument();
    // put a test doc in
    DocumentRepository docRepo = new DocumentRepositoryImpl(f.getSession());
    docRepo.create(doc);
    QueryRepository instance = new QueryRepositoryImpl(f.getSession());
    QueryResponseWrapper result = instance.query(Fixtures.createTestParsedQuery2());
    assertNotNull(result);
    assertTrue(result.isEmpty());
    assertTrue(result.getNumAdditionalResults() == 0);
  }

  /**
   * Test of query method, of class QueryDao.
   */
  @Test
  public void testDoQueryWithLotsOfResults() throws Exception {
    System.out.println("testDoQueryWithLotsOfResults");
    // put in an index that we can use with bulk data
    f.insertIndex(Fixtures.createTestIndexWithBulkDataHit());
    List<Document> docs = Fixtures.getBulkDocuments();
    // put a bunch of test docs in
    f.insertDocuments(docs);
    QueryRepository instance = new QueryRepositoryImpl(f.getSession());
    QueryResponseWrapper result = instance.query(Fixtures.createTestParsedQueryBulkData());
    assertNotNull(result);
    assertTrue(!result.isEmpty());
    assertTrue(result.size() == 34);
    assertTrue(result.getNumAdditionalResults() == 0);
  }

  /**
   * Test of query method, of class QueryDao.
   */
  @Test
  public void testDoQueryWithPaging() throws Exception {
    System.out.println("testDoQueryWithPaging");
    // put in an index that we can use with bulk data
    f.insertIndex(Fixtures.createTestIndexWithBulkDataHit());
    List<Document> docs = Fixtures.getBulkDocuments();
    // put a bunch of test docs in
    f.insertDocuments(docs);
    // setup
    QueryRepository instance = new QueryRepositoryImpl(f.getSession());
    ParsedQuery query = Fixtures.createTestParsedQueryBulkData();
    // let's get the first 5
    QueryResponseWrapper result = instance.query(query, 5, 0);
    assertNotNull(result);
    assertTrue(!result.isEmpty());
    assertTrue(result.size() == 5);
    assertTrue(result.get(0).getObjectAsString()
        .contains("\"field2\" : \"this is some more random data32\""));
    assertTrue(result.get(1).getObjectAsString()
        .contains("\"field2\" : \"this is some more random data31\""));
    assertTrue(result.get(2).getObjectAsString()
        .contains("\"field2\" : \"this is some more random data30\""));
    assertTrue(result.get(3).getObjectAsString()
        .contains("\"field2\" : \"this is some more random data29\""));
    assertTrue(result.get(4).getObjectAsString()
        .contains("\"field2\" : \"this is some more random data28\""));
    assertTrue(result.getNumAdditionalResults() == null);

    // now lets get the second 5
    result = instance.query(query, 5, 5);
    assertNotNull(result);
    assertTrue(!result.isEmpty());
    assertTrue(result.size() == 5);
    assertTrue(result.get(0).getObjectAsString()
        .contains("\"field2\" : \"this is some more random data27\""));
    assertTrue(result.get(1).getObjectAsString()
        .contains("\"field2\" : \"this is some more random data26\""));
    assertTrue(result.get(2).getObjectAsString()
        .contains("\"field2\" : \"this is some more random data25\""));
    assertTrue(result.get(3).getObjectAsString()
        .contains("\"field2\" : \"this is some more random data24\""));
    assertTrue(result.get(4).getObjectAsString()
        .contains("\"field2\" : \"this is some more random data23\""));
    assertTrue(result.getNumAdditionalResults() == null);

    // now lets get the third 5
    result = instance.query(query, 5, 10);
    assertNotNull(result);
    assertTrue(!result.isEmpty());
    assertTrue(result.size() == 5);
    assertTrue(result.get(0).getObjectAsString()
        .contains("\"field2\" : \"this is some more random data22\""));
    assertTrue(result.get(1).getObjectAsString()
        .contains("\"field2\" : \"this is some more random data21\""));
    assertTrue(result.get(2).getObjectAsString()
        .contains("\"field2\" : \"this is some more random data20\""));
    assertTrue(result.get(3).getObjectAsString()
        .contains("\"field2\" : \"this is some more random data19\""));
    assertTrue(result.get(4).getObjectAsString()
        .contains("\"field2\" : \"this is some more random data18\""));
    assertTrue(result.getNumAdditionalResults() == null);

    // now lets get the last 4
    result = instance.query(query, 5, 30);
    assertNotNull(result);
    assertTrue(!result.isEmpty());
    assertTrue(result.size() == 4);
    assertTrue(result.get(0).getObjectAsString()
        .contains("\"field2\" : \"this is some more random data2\""));
    assertTrue(result.get(1).getObjectAsString()
        .contains("\"field2\" : \"this is some more random data1\""));
    assertTrue(result.get(2).getObjectAsString()
        .contains("\"field2\" : \"this is some more random data\""));
    assertTrue(
        result.get(3).getObjectAsString().contains("\"field2\" : \"this is some random data\""));
    assertTrue(result.getNumAdditionalResults() == 0);

  }

}
