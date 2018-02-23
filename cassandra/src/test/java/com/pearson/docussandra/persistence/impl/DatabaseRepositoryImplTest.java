
package com.pearson.docussandra.persistence.impl;

import com.pearson.docussandra.domain.objects.Database;
import com.pearson.docussandra.domain.objects.Identifier;
import com.pearson.docussandra.persistence.DatabaseRepository;
import com.pearson.docussandra.persistence.ITableRepository;
import com.pearson.docussandra.testhelper.Fixtures;
import java.util.List;
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
public class DatabaseRepositoryImplTest {

  private static Fixtures f;

  private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseRepositoryImplTest.class);

  public DatabaseRepositoryImplTest() throws Exception {
    f = Fixtures.getInstance();
  }

  @BeforeClass
  public static void setUpClass() {}

  @AfterClass
  public static void tearDownClass() {
    f.clearTestTables();// clear anything that might be there already
  }

  @Before
  public void setUp() {
    f.clearTestTables();// clear anything that might be there already
  }

  @After
  public void tearDown() {

  }

  /**
   * Test of create method, of class DatabaseRepositoryImpl.
   */
  @Test
  public void testCreate() {
    System.out.println("create");
    Database entity = Fixtures.createTestDatabase();
    DatabaseRepository instance = new DatabaseRepositoryImpl(f.getSession());
    Database result = instance.create(entity);
    assertEquals(entity, result);
  }

  /**
   * Test of update method, of class DatabaseRepositoryImpl.
   */
  @Test
  public void testUpdate() {
    System.out.println("update");
    // setup
    Database entity = Fixtures.createTestDatabase();
    f.insertDatabase(entity);
    // act
    DatabaseRepository instance = new DatabaseRepositoryImpl(f.getSession());
    entity.setDescription("This is a new description!");
    Database result = instance.update(entity);
    // assert
    assertEquals(entity, result);
  }

  /**
   * Test of delete method, of class DatabaseRepositoryImpl.
   */
  @Test
  public void testDelete() {
    System.out.println("delete");
    // setup
    Database entity = Fixtures.createTestDatabase();
    f.insertDatabase(entity);
    // act
    DatabaseRepository instance = new DatabaseRepositoryImpl(f.getSession());
    instance.delete(entity);
    // check
    List<Database> allRows = instance.readAll();
    assertFalse(allRows.contains(entity));
  }

  /**
   * Test of delete method, of class DatabaseRepositoryImpl.
   */
  @Test
  public void testDelete_Identifier() {
    System.out.println("delete");
    Database entity = Fixtures.createTestDatabase();
    Identifier identifier = entity.getId();
    f.insertDatabase(entity);
    // act
    DatabaseRepository instance = new DatabaseRepositoryImpl(f.getSession());
    instance.delete(identifier);
    // check
    List<Database> allRows = instance.readAll();
    assertFalse(allRows.contains(entity));
  }

  /**
   * Test of delete method, of class DatabaseRepositoryImpl.
   */
  @Test
  public void testDeleteWithDeleteCascade() throws InterruptedException {
    System.out.println("deleteWithDeleteCascade");
    // setup
    final Database entity = Fixtures.createTestDatabase();
    f.insertDatabase(entity);
    f.insertTable(Fixtures.createTestTable());
    f.insertIndex(Fixtures.createTestIndexOneField());
    f.insertDocument(Fixtures.createTestDocument());
    // act
    DatabaseRepository instance = new DatabaseRepositoryImpl(f.getSession());
    instance.delete(entity);
    // Thread.sleep(5000);
    // check DB deletion
    List<Database> allRows = instance.readAll();
    assertFalse(allRows.contains(entity));
    // check table deletion
    TableRepositoryImpl tableRepo = new TableRepositoryImpl(f.getSession());
    assertFalse(tableRepo.exists(Fixtures.createTestTable().getId()));
    // check index deletion
    IndexRepositoryImpl indexRepo = new IndexRepositoryImpl(f.getSession());
    assertFalse(indexRepo.exists(Fixtures.createTestIndexOneField().getId()));
    // check iTable deletion
    ITableRepository iTableRepo = new ITableRepositoryImpl(f.getSession());
    assertFalse(iTableRepo.iTableExists(Fixtures.createTestIndexOneField()));
    // check document deletion
    DocumentRepositoryImpl docRepo = new DocumentRepositoryImpl(f.getSession());
    boolean expectedExceptionThrown = false;
    try {
      docRepo.exists(Fixtures.createTestDocument().getId());
    } catch (Exception e)// should error because the entire table should no longer exist
    {
      assertTrue(e.getMessage().contains("unconfigured columnfamily"));
      assertTrue(e.getMessage().contains(Fixtures.createTestTable().toDbTable()));
      expectedExceptionThrown = true;
    }
    assertTrue(expectedExceptionThrown);
  }

  /**
   * Test of readAll method, of class DatabaseRepositoryImpl.
   */
  @Test
  public void testReadAll() {
    System.out.println("readAll");
    // setup
    Database entity = Fixtures.createTestDatabase();
    f.insertDatabase(entity);
    // act
    DatabaseRepository instance = new DatabaseRepositoryImpl(f.getSession());
    List<Database> result = instance.readAll();
    // check
    assertFalse(result.isEmpty());
    assertTrue(result.contains(entity));
  }

  /**
   * Test of exists method, of class DatabaseRepositoryImpl.
   */
  @Test
  public void testExists() {
    System.out.println("exists");
    Database entity = Fixtures.createTestDatabase();
    DatabaseRepository instance = new DatabaseRepositoryImpl(f.getSession());
    boolean result = instance.exists(entity.getId());
    assertEquals(false, result);
    instance.create(entity);
    result = instance.exists(entity.getId());
    assertEquals(true, result);
  }

  /**
   * Test of read method, of class DatabaseRepositoryImpl.
   */
  @Test
  public void testRead() {
    System.out.println("read");
    Database entity = Fixtures.createTestDatabase();
    DatabaseRepository instance = new DatabaseRepositoryImpl(f.getSession());
    assertNull(instance.read(entity.getId()));
    instance.create(entity);
    assertEquals(entity, instance.read(entity.getId()));
  }

}
