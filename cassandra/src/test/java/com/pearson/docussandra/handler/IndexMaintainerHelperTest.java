package com.pearson.docussandra.handler;

import com.datastax.driver.core.BoundStatement;
import com.pearson.docussandra.bucketmanagement.PrimaryIndexBucketLocatorImpl;
import com.pearson.docussandra.cache.CacheFactory;
import com.pearson.docussandra.domain.objects.Document;
import com.pearson.docussandra.domain.objects.Index;
import com.pearson.docussandra.domain.objects.Table;
import com.pearson.docussandra.exception.IndexParseException;
import com.pearson.docussandra.persistence.impl.DocumentRepositoryImpl;
import com.pearson.docussandra.persistence.impl.IndexRepositoryImpl;
import com.pearson.docussandra.persistence.impl.TableRepositoryImpl;
import com.pearson.docussandra.testhelper.Fixtures;
import java.util.ArrayList;
import java.util.List;
import org.bson.BSON;
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
public class IndexMaintainerHelperTest {

  private Logger logger = LoggerFactory.getLogger(this.getClass());
  private IndexRepositoryImpl indexRepo;
  private DocumentRepositoryImpl docRepo;
  private TableRepositoryImpl tableRepo;
  // some test records
  private Index index1;
  private Index index2;
  private Index index3;
  private Table table;

  private static Fixtures f;

  public IndexMaintainerHelperTest() throws Exception {
    f = Fixtures.getInstance();
  }

  @BeforeClass
  public static void setUpClass() {}

  @AfterClass
  public static void tearDownClass() {
    f.clearTestTables();// clear anything that might be there already
    CacheFactory.clearAllCaches();
  }

  @Before
  public void setUp() {
    // IndexChangeObserver ico = new IndexChangeObserver(f.getSession());
    indexRepo = new IndexRepositoryImpl(f.getSession());
    docRepo = new DocumentRepositoryImpl(f.getSession());
    tableRepo = new TableRepositoryImpl(f.getSession());
    CacheFactory.clearAllCaches();// clear the caches so we don't grab an old record that is no
                                  // longer present
    table = Fixtures.createTestTable();// new Table();
    f.clearTestTables();// clear anything that might be there already
    // f.createTestITables();
    // clearTestData();
    // reinsert with some fresh data
    index1 = Fixtures.createTestIndexOneField();
    index2 = Fixtures.createTestIndexTwoField();
    index3 = Fixtures.createTestIndexAllFieldTypes();
    indexRepo.create(index1);
    indexRepo.create(index2);
  }

  @After
  public void tearDown() {

  }

  /**
   * Test of generateDocumentCreateIndexEntriesStatements method, of class IndexMaintainerHelper.
   */
  @Test
  public void testGenerateDocumentCreateIndexEntriesStatements() throws IndexParseException {
    System.out.println("generateDocumentCreateIndexEntriesStatements");
    Document entity = Fixtures.createTestDocument2();
    List<BoundStatement> result =
        IndexMaintainerHelper.generateDocumentCreateIndexEntriesStatements(f.getSession(), entity,
            PrimaryIndexBucketLocatorImpl.getInstance());
    assertEquals(result.size(), 2);// one for each of our indices
    BoundStatement one = result.get(0);
    assertNotNull(one);
    for (int i = 0; i < 5; i++) {
      assertTrue(one.isSet(i));// 0 is the id, 1 is the blob, 2 and 3 are dates, 4 is the single
                               // index field for index1
    }
    assertEquals("docussandra", one.getKeyspace());
    assertEquals(
        "INSERT INTO mydb_mytable_myindexwithonefield (bucket, id, object, created_at, updated_at, myindexedfield) VALUES (?, ?, ?, ?, ?, ?);",
        one.preparedStatement().getQueryString());
    BoundStatement two = result.get(1);
    assertNotNull(two);
    for (int i = 0; i < 6; i++) {
      assertTrue(two.isSet(i));// 0 is the id, 1 is the blob, 2 and 3 are dates, 4 and 5 are the
                               // indexed fields for index2
    }
    assertEquals("docussandra", two.getKeyspace());
    assertEquals(
        "INSERT INTO mydb_mytable_myindexwithtwofields (bucket, id, object, created_at, updated_at, myindexedfield1, myindexedfield2) VALUES (?, ?, ?, ?, ?, ?, ?);",
        two.preparedStatement().getQueryString());
  }

  /**
   * Test of generateDocumentCreateIndexEntriesStatements method, of class IndexMaintainerHelper.
   */
  @Test
  public void testGenerateDocumentCreateIndexEntriesStatementsWithDataTypes()
      throws IndexParseException {
    System.out.println("generateDocumentCreateIndexEntriesStatementsWithDataTypes");
    Document entity = Fixtures.createTestDocument3();
    f.insertIndex(index3);
    List<BoundStatement> result =
        IndexMaintainerHelper.generateDocumentCreateIndexEntriesStatements(f.getSession(), entity,
            PrimaryIndexBucketLocatorImpl.getInstance());
    assertEquals(result.size(), 1);// one for each of our indices
    BoundStatement one = result.get(0);
    assertNotNull(one);
    for (int i = 0; i < 12; i++)// 0 is the id, 1 is the blob, 2 and 3 are dates, 4 - 11 are the
                                // indexed fields
    {
      assertTrue(one.isSet(i));
    }
    // check the proper types were set
    assertNotNull(one.getLong(0));
    assertNotNull(one.getUUID(1));
    assertNotNull(one.getBytes(2));
    assertNotNull(one.getDate(3));
    assertNotNull(one.getDate(4));
    assertNotNull(one.getUUID(5));
    assertNotNull(one.getString(6));
    assertNotNull(one.getInt(7));
    assertNotNull(one.getDouble(8));
    assertNotNull(one.getBytes(9));
    assertNotNull(one.getBool(10));
    assertNotNull(one.getDate(11));
    assertEquals("docussandra", one.getKeyspace());
    assertEquals(
        "INSERT INTO mydb_mytable_myindexallfields (bucket, id,"
            + " object, created_at, updated_at, thisisauudid, thisisastring, "
            + "thisisanint, thisisadouble, thisisbase64, thisisaboolean, thisisadate, thisisalong)"
            + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
        one.preparedStatement().getQueryString());
  }

  /**
   * Test of generateDocumentCreateIndexEntriesStatements method, of class IndexMaintainerHelper.
   */
  @Test
  public void testGenerateDocumentCreateIndexEntriesStatementsNoIndexField()
      throws IndexParseException {
    System.out.println("testGenerateDocumentCreateIndexEntriesStatementsNoIndexField");
    Document entity = Fixtures.createTestDocument2();
    entity.setObjectAsString("{}");// good luck indexing that!
    List<BoundStatement> result =
        IndexMaintainerHelper.generateDocumentCreateIndexEntriesStatements(f.getSession(), entity,
            PrimaryIndexBucketLocatorImpl.getInstance());
    assertTrue(result.isEmpty());
  }

  /**
   * Test of generateDocumentCreateIndexEntriesStatements method, of class IndexMaintainerHelper.
   */
  @Test
  public void testGenerateDocumentCreateIndexEntriesStatementsBadIndexField() {
    System.out.println("testGenerateDocumentCreateIndexEntriesStatementsBadIndexField");
    Document entity = Fixtures.createTestDocument3();
    f.insertIndex(Fixtures.createTestIndexAllFieldTypes());
    entity.setObjectAsString(
        "{\"thisisastring\":\"hello\", \"thisisanint\": \"five\", \"thisisadouble\":\"five point five five five\","
            + " \"thisisbase64\":\"nope!\", \"thisisaboolean\":\"blah!\","
            + " \"thisisadate\":\"day 0\", \"thisisauudid\":\"z\"}");// completely botched field
                                                                     // types
    boolean expectedExceptionThrown = false;
    try {
      List<BoundStatement> result =
          IndexMaintainerHelper.generateDocumentCreateIndexEntriesStatements(f.getSession(), entity,
              PrimaryIndexBucketLocatorImpl.getInstance());
    } catch (IndexParseException e) {
      expectedExceptionThrown = true;
    }
    assertTrue("Expected exception was not thrown.", expectedExceptionThrown);

  }

  /**
   * Test of generateDocumentUpdateIndexEntriesStatements method, of class IndexMaintainerHelper.
   */
  @Test
  public void testGenerateDocumentUpdateIndexEntriesStatements() throws IndexParseException {
    System.out.println("generateDocumentUpdateIndexEntriesStatements");
    Document entity = Fixtures.createTestDocument2();
    tableRepo.create(table);// create the table so we have a place to store the test data
    docRepo.create(entity);// insert a document so we have something to reference
    List<BoundStatement> result =
        IndexMaintainerHelper.generateDocumentUpdateIndexEntriesStatements(f.getSession(), entity,
            PrimaryIndexBucketLocatorImpl.getInstance());
    assertEquals(2, result.size());// one for each of our indices
    BoundStatement one = result.get(0);
    assertNotNull(one);
    for (int i = 0; i < 3; i++) {
      assertTrue(one.isSet(i));// 0 is the blob, 1 is the date, 2 is the UUID
    }
    assertEquals("docussandra", one.getKeyspace());
    assertEquals(
        "UPDATE mydb_mytable_myindexwithonefield SET object = ?, updated_at = ? WHERE bucket = ? AND id = ? AND myindexedfield = ?;",
        one.preparedStatement().getQueryString());
    BoundStatement two = result.get(1);
    assertNotNull(two);
    for (int i = 0; i < 4; i++) {
      assertTrue(two.isSet(i));// 0 is the blob, 1 is the date, 2 and 3 are indexed fields
    }
    assertEquals("docussandra", two.getKeyspace());
    assertEquals(
        "UPDATE mydb_mytable_myindexwithtwofields SET object = ?, updated_at = ? WHERE bucket = ? AND id = ? AND myindexedfield1 = ? AND myindexedfield2 = ?;",
        two.preparedStatement().getQueryString());
  }

  /**
   * Test of generateDocumentUpdateIndexEntriesStatements method, of class IndexMaintainerHelper.
   */
  @Test
  public void testGenerateDocumentUpdateIndexEntriesStatementsWithDataTypes()
      throws IndexParseException {
    System.out.println("generateDocumentUpdateIndexEntriesStatements");
    Document entity = Fixtures.createTestDocument3();
    tableRepo.create(table);// create the table so we have a place to store the test data
    f.insertIndex(index3);
    docRepo.create(entity);// insert a document so we have something to reference
    List<BoundStatement> result =
        IndexMaintainerHelper.generateDocumentUpdateIndexEntriesStatements(f.getSession(), entity,
            PrimaryIndexBucketLocatorImpl.getInstance());
    assertEquals(1, result.size());// one for each of our indices
    BoundStatement one = result.get(0);
    assertNotNull(one);
    for (int i = 0; i < 11; i++) {
      assertTrue(one.isSet(i));// 0 is the blob, 1 is the date, 2 is the UUID, 3 is the doc id, 4-10
                               // are the other indexes
    }
    // check the proper types were set
    assertNotNull(one.getBytes(0));
    assertNotNull(one.getDate(1));
    assertNotNull(one.getLong(2));
    assertNotNull(one.getUUID(3));
    assertNotNull(one.getUUID(4));
    assertNotNull(one.getString(5));
    assertNotNull(one.getInt(6));
    assertNotNull(one.getDouble(7));
    assertNotNull(one.getBytes(8));
    assertNotNull(one.getBool(9));
    assertNotNull(one.getDate(10));
    assertEquals("docussandra", one.getKeyspace());
    assertEquals(
        "UPDATE mydb_mytable_myindexallfields SET object = ?, updated_at = ? "
            + "WHERE bucket = ? AND id = ? AND thisisauudid = ? AND thisisastring = ? AND thisisanint = ?"
            + " AND thisisadouble = ? AND thisisbase64 = ? AND thisisaboolean = ? AND thisisadate = ? AND thisisalong = ?;",
        one.preparedStatement().getQueryString());
  }

  /**
   * Test of generateDocumentUpdateIndexEntriesStatements method, of class IndexMaintainerHelper.
   * This test includes functionality for when an indexed field has changed.
   */
  @Test
  public void testGenerateDocumentUpdateIndexEntriesStatementsIndexChanged()
      throws IndexParseException {
    System.out.println("generateDocumentUpdateIndexEntriesStatementsIndexChanged");
    Document entity = Fixtures.createTestDocument2();
    tableRepo.create(table);// create the table so we have a place to store the test data
    docRepo.create(entity);// insert a document so we have something to reference
    String changedMyindexedfield = "this is NOT my field";
    entity.setObjectAsString("{'greeting':'hello', 'myindexedfield': '" + changedMyindexedfield
        + "', 'myindexedfield1':'my second field', 'myindexedfield2':'my third field'}");// change
                                                                                         // an
                                                                                         // indexed
                                                                                         // field
    List<BoundStatement> result =
        IndexMaintainerHelper.generateDocumentUpdateIndexEntriesStatements(f.getSession(), entity,
            PrimaryIndexBucketLocatorImpl.getInstance());
    assertEquals(3, result.size());// one for the create, one for the delete, one for the second
                                   // index

    // create statement (the first bound statement)
    BoundStatement one = result.get(0);
    assertNotNull(one);
    // check that all fields are set on the statement
    for (int i = 0; i < 6; i++) {
      assertTrue(one.isSet(i));// 0 is the bucket, 1 is the id, 2 is the blob, 3 and 4 are dates, 5
                               // is the single index field for index1
    }
    // check the specific fields are set correctly
    assertEquals(entity.getObject(), BSON.decode(one.getBytes(2).array()));// make sure the object
                                                                           // is set correctly
    assertEquals(changedMyindexedfield, one.getString(5));// make sure that the new indexed field is
                                                          // set
    // check keyspace and CQL
    assertEquals("docussandra", one.getKeyspace());
    assertEquals(
        "INSERT INTO mydb_mytable_myindexwithonefield (bucket, id, object, created_at, updated_at, myindexedfield) VALUES (?, ?, ?, ?, ?, ?);",
        one.preparedStatement().getQueryString());

    // delete statement (the second bound statement)
    BoundStatement two = result.get(1);
    assertNotNull(two);
    assertTrue(two.isSet(0));// the UUID
    assertTrue(two.isSet(1));// the indexed field
    assertEquals("this is my field", two.getString(1));// check that the delete statement is looking
                                                       // for the OLD index value (real test for
                                                       // issue #100)
    // check keyspace and CQL
    assertEquals("docussandra", two.getKeyspace());
    assertEquals(
        "DELETE FROM mydb_mytable_myindexwithonefield WHERE bucket = ? AND myindexedfield = ?;",
        two.preparedStatement().getQueryString());

    // the second index update should proceed like a normal update (the indexed field has not
    // changed for this index) (the third bound statement)
    BoundStatement three = result.get(2);
    assertNotNull(three);
    // check that all fields are set on the statement
    for (int i = 0; i < 6; i++) {
      assertTrue(three.isSet(i));// 0 is the blob, 1 is the date, 2 is the bucket, 3 3 is the id, 4
                                 // and 5 are indexed fields
    }
    // check that the fields are accurate
    assertEquals(entity.getObject(), BSON.decode(three.getBytes(0).array()));// make sure the object
                                                                             // is set correctly
    assertEquals(entity.getUuid(), three.getUUID(3));
    assertEquals("my second field", three.getString(4));
    assertEquals("my third field", three.getString(5));
    // check keyspace and CQL
    assertEquals("docussandra", three.getKeyspace());
    assertEquals(
        "UPDATE mydb_mytable_myindexwithtwofields SET object = ?, updated_at = ? WHERE bucket = ? AND id = ? AND myindexedfield1 = ? AND myindexedfield2 = ?;",
        three.preparedStatement().getQueryString());
  }

  /**
   * Test of generateDocumentUpdateIndexEntriesStatements method, of class IndexMaintainerHelper.
   * This test includes functionality for when an indexed field has changed.
   */
  @Test
  public void testGenerateDocumentUpdateIndexEntriesStatementsIndexChangedNewIndexNull()
      throws IndexParseException {
    System.out.println("generateDocumentUpdateIndexEntriesStatementsIndexChangedNewIndexNull");
    Document entity = Fixtures.createTestDocument2();
    tableRepo.create(table);// create the table so we have a place to store the test data
    docRepo.create(entity);// insert a document so we have something to reference
    entity.setObjectAsString(
        "{'greeting':'hello', 'myindexedfield': null, 'myindexedfield1':'my second field', 'myindexedfield2':'my third field'}");// change
                                                                                                                                 // an
                                                                                                                                 // indexed
                                                                                                                                 // field
    List<BoundStatement> result =
        IndexMaintainerHelper.generateDocumentUpdateIndexEntriesStatements(f.getSession(), entity,
            PrimaryIndexBucketLocatorImpl.getInstance());
    assertEquals(2, result.size());// NONE for the create (the first index now has a null value),
                                   // one for the delete, one for the second index

    // delete (the first bound statement)
    BoundStatement one = result.get(0);
    assertNotNull(one);
    assertTrue(one.isSet(0));// the bucket
    assertTrue(one.isSet(1));// the indexed field
    assertEquals("this is my field", one.getString(1));// check that the delete statement is looking
                                                       // for the OLD index value (real test for
                                                       // issue #100)
    // check keyspace and CQL
    assertEquals("docussandra", one.getKeyspace());
    assertEquals(
        "DELETE FROM mydb_mytable_myindexwithonefield WHERE bucket = ? AND myindexedfield = ?;",
        one.preparedStatement().getQueryString());

    // the second index update should proceed like a normal update (the indexed field has not
    // changed for this index) (the second bound statement)
    BoundStatement two = result.get(1);
    assertNotNull(two);
    // check that all fields are set on the statement
    for (int i = 0; i < 6; i++) {
      assertTrue(two.isSet(i));// 0 is the blob, 1 is the date, 2 is the bucket, 3 is the id, 4 and
                               // 5 are indexed fields
    }
    // check that the fields are accurate
    assertEquals(entity.getObject(), BSON.decode(two.getBytes(0).array()));// make sure the object
                                                                           // is set correctly
    assertEquals(entity.getUuid(), two.getUUID(3));
    assertEquals("my second field", two.getString(4));
    assertEquals("my third field", two.getString(5));
    // check keyspace and CQL
    assertEquals("docussandra", two.getKeyspace());
    assertEquals(
        "UPDATE mydb_mytable_myindexwithtwofields SET object = ?, updated_at = ? WHERE bucket = ? AND id = ? AND myindexedfield1 = ? AND myindexedfield2 = ?;",
        two.preparedStatement().getQueryString());
  }

  /**
   * Test of generateDocumentUpdateIndexEntriesStatements method, of class IndexMaintainerHelper.
   * This test includes functionality for when an indexed field has changed.
   */
  @Test
  public void testGenerateDocumentUpdateIndexEntriesStatementsIndexChangedOldIndexNull()
      throws IndexParseException {
    System.out.println("generateDocumentUpdateIndexEntriesStatementsIndexChangedOldIndexNull");
    Document entity = Fixtures.createTestDocument2();
    entity.setObjectAsString(
        "{'greeting':'hello', 'myindexedfield': null, 'myindexedfield1':'my second field', 'myindexedfield2':'my third field'}");// change
                                                                                                                                 // an
                                                                                                                                 // indexed
                                                                                                                                 // field
    tableRepo.create(table);// create the table so we have a place to store the test data
    docRepo.create(entity);// insert a document so we have something to reference
    entity = Fixtures.createTestDocument2();// pull the entitiy in from fixtures again

    List<BoundStatement> result =
        IndexMaintainerHelper.generateDocumentUpdateIndexEntriesStatements(f.getSession(), entity,
            PrimaryIndexBucketLocatorImpl.getInstance());
    assertEquals(2, result.size());// one for the create, NONE for the delete, one for the second
                                   // index

    // create statement (the first bound statement)
    BoundStatement one = result.get(0);
    assertNotNull(one);
    // check that all fields are set on the statement
    for (int i = 0; i < 6; i++) {
      assertTrue(one.isSet(i));// 0 is the bucket, 1 is the id, 2 is the blob, 3 and 4 are dates, 5
                               // is the single index field for index1
    }
    // check the specific fields are set correctly
    assertEquals(entity.getObject(), BSON.decode(one.getBytes(2).array()));// make sure the object
                                                                           // is set correctly
    assertEquals("this is my field", one.getString(5));// make sure that the new indexed field is
                                                       // set
    // check keyspace and CQL
    assertEquals("docussandra", one.getKeyspace());
    assertEquals(
        "INSERT INTO mydb_mytable_myindexwithonefield (bucket, id, object, created_at, updated_at, myindexedfield) VALUES (?, ?, ?, ?, ?, ?);",
        one.preparedStatement().getQueryString());

    // the second index update should proceed like a normal update (the indexed field has not
    // changed for this index) (the second bound statement)
    BoundStatement two = result.get(1);
    assertNotNull(two);
    // check that all fields are set on the statement
    for (int i = 0; i < 6; i++) {
      assertTrue(two.isSet(i));// 0 is the blob, 1 is the date, 2 is the bucket, 3 is the id, 4 and
                               // 5 are indexed fields
    }
    // check that the fields are accurate
    assertEquals(entity.getObject(), BSON.decode(two.getBytes(0).array()));// make sure the object
                                                                           // is set correctly
    assertEquals(entity.getUuid(), two.getUUID(3));
    assertEquals("my second field", two.getString(4));
    assertEquals("my third field", two.getString(5));
    // check keyspace and CQL
    assertEquals("docussandra", two.getKeyspace());
    assertEquals(
        "UPDATE mydb_mytable_myindexwithtwofields SET object = ?, updated_at = ? WHERE bucket = ? AND id = ? AND myindexedfield1 = ? AND myindexedfield2 = ?;",
        two.preparedStatement().getQueryString());
  }

  /**
   * Test of generateDocumentDeleteIndexEntriesStatements method, of class IndexMaintainerHelper.
   */
  @Test
  public void testGenerateDocumentDeleteIndexEntriesStatements() throws IndexParseException {
    System.out.println("generateDocumentDeleteIndexEntriesStatements");
    Document entity = Fixtures.createTestDocument2();
    entity.setObjectAsString(
        "{'greeting':'hello', 'myindexedfield': 'this is my field', 'myindexedfield1':'my second field', 'myindexedfield2':'my third field'}");
    List<BoundStatement> result =
        IndexMaintainerHelper.generateDocumentDeleteIndexEntriesStatements(f.getSession(), entity,
            PrimaryIndexBucketLocatorImpl.getInstance());
    assertEquals(2, result.size());// one for each of our indices (defined in the class setup
                                   // methods)
    BoundStatement one = result.get(0);
    assertNotNull(one);
    assertTrue(one.isSet(0));// the bucket
    assertEquals("docussandra", one.getKeyspace());
    assertEquals(
        "DELETE FROM mydb_mytable_myindexwithonefield WHERE bucket = ? AND myindexedfield = ?;",
        one.preparedStatement().getQueryString());
    BoundStatement two = result.get(1);
    assertNotNull(two);
    for (int i = 0; i < 1; i++) {
      assertTrue(two.isSet(i));// 0 and 1 are indexed fields
    }
    assertEquals("docussandra", two.getKeyspace());
    assertEquals(
        "DELETE FROM mydb_mytable_myindexwithtwofields WHERE bucket = ? AND myindexedfield1 = ? AND myindexedfield2 = ?;",
        two.preparedStatement().getQueryString());
  }

  /**
   * Test of generateDocumentDeleteIndexEntriesStatements method, of class IndexMaintainerHelper.
   */
  @Test
  public void testGenerateDocumentDeleteIndexEntriesStatementsWithDataTypes()
      throws IndexParseException {
    System.out.println("generateDocumentDeleteIndexEntriesStatementsWithDataTypes");
    Document entity = Fixtures.createTestDocument3();
    f.insertIndex(index3);
    entity.setObjectAsString(
        "{\"thisisastring\":\"hello\", \"thisisanint\": \"5\", \"thisisadouble\":\"5.555\","
            + " \"thisisbase64\":\"VGhpcyBpcyBhIGdvb2RseSB0ZXN0IG1lc3NhZ2Uu\", \"thisisaboolean\":\"f\","
            + " \"thisisadate\":\"Thu Apr 30 09:52:04 MDT 2015\", \"thisisauudid\":\"3d069a5a-ef51-11e4-90ec-1681e6b88ec1\", \"thisisalong\":\"378657657654654\"}");
    List<BoundStatement> result =
        IndexMaintainerHelper.generateDocumentDeleteIndexEntriesStatements(f.getSession(), entity,
            PrimaryIndexBucketLocatorImpl.getInstance());
    assertEquals(1, result.size());// one for each of our indices relevent to this document
    BoundStatement one = result.get(0);
    assertNotNull(one);
    assertTrue(one.isSet(0));// the bucket
    assertNotNull(one.getLong(0));
    assertNotNull(one.getUUID(1));
    assertNotNull(one.getString(2));
    assertNotNull(one.getInt(3));
    assertNotNull(one.getDouble(4));
    assertNotNull(one.getBytes(5));
    assertNotNull(one.getBool(6));
    assertNotNull(one.getDate(7));
    assertEquals("docussandra", one.getKeyspace());
    assertEquals(
        "DELETE FROM mydb_mytable_myindexallfields WHERE bucket = ? AND thisisauudid = ? AND thisisastring = ? AND"
            + " thisisanint = ? AND thisisadouble = ? AND thisisbase64 = ? AND thisisaboolean = ? AND thisisadate = ? AND thisisalong = ?;",
        one.preparedStatement().getQueryString());

  }

  /**
   * Test of getIndexForDocument method, of class IndexMaintainerHelper.
   */
  @Test
  public void testGetIndexForDocument() {
    System.out.println("getIndexForDocument");
    Document entity = Fixtures.createTestDocument2();
    ArrayList<Index> exp = new ArrayList<>(2);
    exp.add(index1);
    exp.add(index2);
    List<Index> result = IndexMaintainerHelper.getIndexForDocument(f.getSession(), entity);
    assertNotNull(result);
    assertTrue(!result.isEmpty());
    assertEquals(2, result.size());
    assertEquals(exp, result);
  }

  /**
   * Test of generateCQLStatementForInsert method, of class IndexMaintainerHelper.
   */
  @Test
  public void testGenerateCQLStatementForInsert() {
    System.out.println("generateCQLStatementForInsert");
    String expResult =
        "INSERT INTO mydb_mytable_myindexwithonefield (bucket, id, object, created_at, updated_at, myindexedfield) VALUES (?, ?, ?, ?, ?, ?);";
    String result = IndexMaintainerHelper.generateCQLStatementForInsert(index1);
    assertEquals(expResult, result);
    expResult =
        "INSERT INTO mydb_mytable_myindexwithtwofields (bucket, id, object, created_at, updated_at, myindexedfield1, myindexedfield2) VALUES (?, ?, ?, ?, ?, ?, ?);";
    result = IndexMaintainerHelper.generateCQLStatementForInsert(index2);
    assertEquals(expResult, result);
  }

  /**
   * Test of generateCQLStatementForInsert method, of class IndexMaintainerHelper.
   */
  @Test
  public void testGenerateCQLStatementForInsert2() {
    System.out.println("generateCQLStatementForInsert");
    String expResult =
        "INSERT INTO mydb_mytable_myindexwithonefield (bucket, id, object, created_at, updated_at, myindexedfield) VALUES (?, ?, ?, ?, ?, ?);";
    String result = IndexMaintainerHelper.getCQLStatementForInsert(index1);
    assertEquals(expResult, result);
    expResult =
        "INSERT INTO mydb_mytable_myindexwithtwofields (bucket, id, object, created_at, updated_at, myindexedfield1, myindexedfield2) VALUES (?, ?, ?, ?, ?, ?, ?);";
    result = IndexMaintainerHelper.getCQLStatementForInsert(index2);
    assertEquals(expResult, result);
  }

  /**
   * Test of generateCQLStatementForWhereClauses method, of class IndexMaintainerHelper.
   */
  @Test
  public void testGenerateCQLStatementForUpdate() {
    System.out.println("generateCQLStatementForUpdate");
    String expResult =
        "UPDATE mydb_mytable_myindexwithonefield SET object = ?, updated_at = ? WHERE bucket = ? AND id = ? AND myindexedfield = ?;";
    String result = IndexMaintainerHelper
        .generateCQLStatementForWhereClauses(IndexMaintainerHelper.ITABLE_UPDATE_CQL, index1);
    assertEquals(expResult, result);
    expResult =
        "UPDATE mydb_mytable_myindexwithtwofields SET object = ?, updated_at = ? WHERE bucket = ? AND id = ? AND myindexedfield1 = ? AND myindexedfield2 = ?;";
    result = IndexMaintainerHelper
        .generateCQLStatementForWhereClauses(IndexMaintainerHelper.ITABLE_UPDATE_CQL, index2);
    assertEquals(expResult, result);
  }

  /**
   * Test of generateCQLStatementForWhereClauses method, of class IndexMaintainerHelper.
   */
  @Test
  public void testGenerateCQLStatementForUpdate2() {
    System.out.println("generateCQLStatementForUpdate");
    String expResult =
        "UPDATE mydb_mytable_myindexwithonefield SET object = ?, updated_at = ? WHERE bucket = ? AND id = ? AND myindexedfield = ?;";
    String result = IndexMaintainerHelper
        .getCQLStatementForWhereClauses(IndexMaintainerHelper.ITABLE_UPDATE_CQL, index1);
    assertEquals(expResult, result);
    expResult =
        "UPDATE mydb_mytable_myindexwithtwofields SET object = ?, updated_at = ? WHERE bucket = ? AND id = ? AND myindexedfield1 = ? AND myindexedfield2 = ?;";
    result = IndexMaintainerHelper
        .getCQLStatementForWhereClauses(IndexMaintainerHelper.ITABLE_UPDATE_CQL, index2);
    assertEquals(expResult, result);
  }

  /**
   * Test of hasIndexedFieldChanged method, of class IndexMaintainerHelper.
   */
  @Test
  public void testHasIndexedFieldChanged() {
    System.out.println("hasIndexedFieldChanged");
    tableRepo.create(table);// create the table so we have a place to store the test data
    Document entity = Fixtures.createTestDocument2();
    docRepo.create(entity);// insert
    entity.setObjectAsString(
        "{'greeting':'hola', 'myindexedfield': 'this is my field', 'myindexedfield1':'my second field', 'myindexedfield2':'my third field'}");// change
                                                                                                                                              // a
                                                                                                                                              // non-index
                                                                                                                                              // field

    boolean result = IndexMaintainerHelper.hasIndexedFieldChanged(
        IndexMaintainerHelper.getOldObjectForUpdate(f.getSession(), entity), index1, entity);
    assertEquals(false, result);
    entity.setObjectAsString(
        "{'greeting':'hello', 'myindexedfield': 'this is NOT my field', 'myindexedfield1':'my second field', 'myindexedfield2':'my third field'}");// change
                                                                                                                                                   // an
                                                                                                                                                   // indexed
                                                                                                                                                   // field
    result = IndexMaintainerHelper.hasIndexedFieldChanged(
        IndexMaintainerHelper.getOldObjectForUpdate(f.getSession(), entity), index1, entity);
    assertEquals(true, result);
  }

}
