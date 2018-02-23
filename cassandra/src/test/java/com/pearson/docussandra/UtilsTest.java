package com.pearson.docussandra;

import com.datastax.driver.core.BoundStatement;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.pearson.docussandra.domain.objects.FieldDataType;
import com.pearson.docussandra.domain.objects.Index;
import com.pearson.docussandra.domain.objects.IndexField;
import com.pearson.docussandra.domain.objects.IndexIdentifier;
import com.pearson.docussandra.exception.IndexParseException;
import com.pearson.docussandra.testhelper.Fixtures;

import java.io.IOException;
import java.util.*;

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
public class UtilsTest {

  private Logger logger = LoggerFactory.getLogger(this.getClass());

  public UtilsTest() {}

  @BeforeClass
  public static void setUpClass() {}

  @AfterClass
  public static void tearDownClass() {}

  @Before
  public void setUp() {}

  @After
  public void tearDown() {}

  /**
   * Test of calculateITableName method, of class Utils.
   */
  @Test
  public void testCalculateITableName_3args() {
    logger.debug("calculateITableName");
    String databaseName = "myDb";
    String tableName = "myTable";
    String indexName = "yoIndex";
    String expResult = "mydb_mytable_yoindex";
    String result = Utils.calculateITableName(databaseName, tableName, indexName);
    assertEquals(expResult, result);
  }

  /**
   * Test of calculateITableName method, of class Utils.
   */
  @Test
  public void testCalculateITableName_Index() {
    logger.debug("calculateITableName");
    String databaseName = "myDb";
    String tableName = "myTable";
    String indexName = "yoIndex";
    Index index = new Index(indexName);
    index.setTable(databaseName, tableName);
    String expResult = "mydb_mytable_yoindex";
    String result = Utils.calculateITableName(index);
    assertEquals(expResult, result);
  }

  /**
   * Test of calculateITableName method, of class Utils.
   */
  @Test
  public void testCalculateITableName_IndexIdentifier() {
    logger.debug("calculateITableName");
    IndexIdentifier indexId = new IndexIdentifier(Fixtures.createTestIndexOneField().getId());
    String expResult = "mydb_mytable_myindexwithonefield";
    String result = Utils.calculateITableName(indexId);
    assertEquals(expResult, result);
  }


  @Test
  public void testInitDatabase() {
    // tests error case (bad replication string) only; none of our other tests will pass if this
    // method doesn't work, so its tested inherently by the other tests
    boolean expectedExceptionThrown = false;
    try {
      Utils.initDatabase(false, "this is a string", null);
    } catch (IllegalArgumentException e) {
      expectedExceptionThrown = true;
    }
    assertTrue("Expected exception not thrown.", expectedExceptionThrown);

    expectedExceptionThrown = false;
    try {
      Utils.initDatabase(false, "{this is a string}", null);
    } catch (IllegalArgumentException e) {
      expectedExceptionThrown = true;
    }
    assertTrue("Expected exception not thrown.", expectedExceptionThrown);

    expectedExceptionThrown = false;
    try {
      Utils.initDatabase(false, "{this is a string 'class'}", null);
    } catch (IllegalArgumentException e) {
      expectedExceptionThrown = true;
    }
    assertTrue("Expected exception not thrown.", expectedExceptionThrown);
  }

  /**
   * Tests to check the values returned from the method setField only tests negative values for now
   */
  @Test
  public void testSetField() throws Exception {
    // negative tests only for now
    logger.debug("setField");
    DBObject object = new BasicDBObject();
    BoundStatement bs = null;
    IndexField fieldData = new IndexField("testField");
    // (DBObject jsonObject, IndexField fieldData, BoundStatement bs, int index)
    boolean normal = Utils.setField(object, fieldData, bs, 0);
    assertFalse("Expected exception not thrown.", normal);

    fieldData = new IndexField("testField", FieldDataType.INTEGER);
    object.put("testField", "thisisnotaninteger");
    boolean expectedExceptionThrown = false;
    try {
      Utils.setField(object, fieldData, bs, 0);
    } catch (IndexParseException e) {
      expectedExceptionThrown = true;
      assertTrue(e.getMessage().contains("thisisnotaninteger"));
      assertTrue(e.getMessage().contains("testField"));
    }
    assertTrue("Expected exception not thrown.", expectedExceptionThrown);

  }

  /**
   * Test of join method, of class Utils.
   */
  @Test
  public void testJoin_String_ObjectArr() {
    logger.debug("join");
    String expResult = "one";
    String result = Utils.join(", ", "one");
    assertEquals(expResult, result);
    expResult = "one, two";
    result = Utils.join(", ", "one", "two");
    assertEquals(expResult, result);
  }

  /**
   * Test of join method, of class Utils.
   */
  @Test
  public void testJoin_String_Collection() {
    logger.debug("join");
    List<String> list = new ArrayList<>();
    String expResult = "";
    String result = Utils.join(", ", list);
    assertEquals(expResult, result);
    list.add("one");
    expResult = "one";
    result = Utils.join(", ", list);
    assertEquals(expResult, result);
    list.add("two");
    expResult = "one, two";
    result = Utils.join(", ", list);
    assertEquals(expResult, result);
  }

  /**
   * Test of equalLists method, of class Utils.
   */
  @Test
  public void testEqualLists() {
    logger.debug("equalLists");
    List<String> one = null;
    List<String> two = null;
    boolean result = Utils.equalLists(one, two);
    assertEquals(true, result);

    one = new ArrayList<>();
    two = new ArrayList<>();
    result = Utils.equalLists(one, two);
    assertEquals(true, result);

    one.add("test");
    two.add("test");
    result = Utils.equalLists(one, two);
    assertEquals(true, result);

    two.add("testtest");
    result = Utils.equalLists(one, two);
    assertEquals(false, result);

    two = new ArrayList<>();
    result = Utils.equalLists(one, two);
    assertEquals(false, result);
  }

  @Test
  public void testtraverse1RowFile() throws IOException {
    List list = Utils.traverse1RowFile("/buckets/text.buckets");
    assertNotNull(list);
    assertFalse(list.isEmpty());
  }


}
