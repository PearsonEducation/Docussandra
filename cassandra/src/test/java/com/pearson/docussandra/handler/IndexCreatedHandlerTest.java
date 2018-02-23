
package com.pearson.docussandra.handler;

import com.pearson.docussandra.Utils;
import com.pearson.docussandra.cache.CacheFactory;
import com.pearson.docussandra.domain.event.IndexCreatedEvent;
import com.pearson.docussandra.domain.objects.Database;
import com.pearson.docussandra.domain.objects.Document;
import com.pearson.docussandra.domain.objects.Index;
import com.pearson.docussandra.domain.objects.Table;
import com.pearson.docussandra.persistence.impl.DocumentRepositoryImpl;
import com.pearson.docussandra.persistence.impl.IndexRepositoryImpl;
import com.pearson.docussandra.persistence.impl.IndexStatusRepositoryImpl;
import com.pearson.docussandra.persistence.impl.IndexStatusRepositoryImplTest;
import com.pearson.docussandra.testhelper.Fixtures;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import org.json.simple.parser.ParseException;
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
public class IndexCreatedHandlerTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(IndexStatusRepositoryImplTest.class);
  private Fixtures f;

  private IndexRepositoryImpl indexRepo;
  private IndexStatusRepositoryImpl statusRepo;
  private DocumentRepositoryImpl docRepo;

  public IndexCreatedHandlerTest() throws Exception {
    f = Fixtures.getInstance(true);
  }

  @BeforeClass
  public static void setUpClass() {}

  @AfterClass
  public static void tearDownClass() {}

  @Before
  public void setUp() throws IOException, InterruptedException, ParseException {
    Utils.initDatabaseSingleReplication(true, f.getSession());// hard clear of the test tables
    Thread.sleep(5000);
    CacheFactory.clearAllCaches();
    Database testDb = Fixtures.createTestDatabase();
    f.insertDatabase(testDb);
    f.insertTable(Fixtures.createTestTable());
    f.insertDocuments(Fixtures.getBulkDocuments());
    indexRepo = new IndexRepositoryImpl(f.getSession());
    statusRepo = new IndexStatusRepositoryImpl(f.getSession());
    docRepo = new DocumentRepositoryImpl(f.getSession());
  }

  @After
  public void tearDown() {}

  /**
   * Test of handles method, of class IndexCreatedHandler.
   */
  @Test
  public void testHandles() {
    System.out.println("handles");
    Class eventClass = IndexCreatedEvent.class;
    IndexCreatedHandler instance = new IndexCreatedHandler(indexRepo, statusRepo, docRepo);
    boolean result = instance.handles(eventClass);
    assertEquals(true, result);
    Object o = new Object();
    result = instance.handles(o.getClass());
    assertEquals(false, result);
  }

  /**
   * Test of handle method, of class IndexCreatedHandler.
   */
  @Test
  public void testHandle() throws Exception {
    System.out.println("handle");
    // datasetup
    Index testIndex = Fixtures.createTestIndexWithBulkDataHit();
    f.insertIndex(testIndex);
    IndexCreatedEvent entity = Fixtures.createTestIndexCreationStatusWithBulkDataHit();
    entity.setTotalRecords(34);
    statusRepo.create(entity);
    Object event = entity;
    // end data setup
    IndexCreatedHandler instance = new IndexCreatedHandler(indexRepo, statusRepo, docRepo);
    // call
    instance.handle(event);
    // verify
    assertTrue(statusRepo.exists(entity.getUuid()));
    IndexCreatedEvent storedStatus = statusRepo.read(entity.getUuid());
    assertNotNull(storedStatus);
    assertTrue(storedStatus.isDoneIndexing());
    assertEquals(storedStatus.getTotalRecords(), storedStatus.getRecordsCompleted());
    assertEquals(100, storedStatus.getPercentComplete(), 0);
    assertEquals(storedStatus.getEta(), 0);
    assertNotEquals(storedStatus.getDateStarted(), storedStatus.getStatusLastUpdatedAt());
    assertTrue(statusRepo.readAllCurrentlyIndexing().isEmpty());
    assertNotNull(storedStatus.getIndex());
    Index readIndex = indexRepo.read(testIndex.getId());
    assertNotNull(readIndex);
    assertTrue(readIndex.isActive());
  }

  /**
   * Test of handle method, of class IndexCreatedHandler.
   */
  @Test
  public void testHandleWithError() throws Exception {
    System.out.println("handleWithError");
    // datasetup
    // Index testIndex = Fixtures.createTestIndexWithBulkDataHit();
    // f.insertIndex(testIndex);//no index associated with this status; not likley to happen, but
    // easy way to cause an exception
    IndexCreatedEvent entity = Fixtures.createTestIndexCreationStatusWithBulkDataHit();
    entity.setTotalRecords(34);
    statusRepo.create(entity);
    Object event = entity;
    // end data setup
    IndexCreatedHandler instance = new IndexCreatedHandler(indexRepo, statusRepo, docRepo);
    // call
    boolean expectedExceptionThrown = false;
    try {
      instance.handle(event);
    } catch (Exception e) {
      expectedExceptionThrown = true;
    }
    assertTrue("Expected exception not thrown.", expectedExceptionThrown);
    // verify
    assertTrue(statusRepo.exists(entity.getUuid()));
    IndexCreatedEvent storedStatus = statusRepo.read(entity.getUuid());
    assertNotNull(storedStatus);
    assertFalse(storedStatus.isDoneIndexing());
    assertNotEquals(storedStatus.getTotalRecords(), storedStatus.getRecordsCompleted());
    assertEquals(0, storedStatus.getPercentComplete(), 0);
    assertEquals(storedStatus.getEta(), -1);
    assertNotEquals(storedStatus.getDateStarted(), storedStatus.getStatusLastUpdatedAt());
    assertFalse(statusRepo.readAllCurrentlyIndexing().isEmpty());
    assertEquals(
        "Could not complete indexing event for index: 'myindexbulkdata'. Please contact a system administrator to resolve this issue.",
        storedStatus.getFatalError());
  }

  /**
   * Test of handle method, of class IndexCreatedHandler.
   */
  @Test
  public void testHandleWithData() throws Exception {
    System.out.println("handleWithData");
    // datasetup
    // insert test docs and stuff
    Database testDb = Fixtures.createTestWorldBankDatabase();
    Table testTable = Fixtures.createTestWorldBankTable();
    f.insertDatabase(testDb);
    f.insertTable(testTable);
    List<Document> docs = Fixtures.getBulkDocumentsByLine("/world_bank_short.txt", testTable);
    f.insertDocuments(docs);// put in a ton of data directly into the db

    // insert index
    Index lastname = Fixtures.createTestBankIndexCountryName();
    f.insertIndex(lastname);

    IndexCreatedEvent entity =
        new IndexCreatedEvent(UUID.randomUUID(), new Date(), new Date(), lastname, docs.size(), 0);

    statusRepo.create(entity);
    Object event = entity;
    // end data setup
    IndexCreatedHandler instance = new IndexCreatedHandler(indexRepo, statusRepo, docRepo);
    // call
    instance.handle(event);
    // verify
    assertTrue(statusRepo.exists(entity.getUuid()));
    IndexCreatedEvent storedStatus = statusRepo.read(entity.getUuid());
    assertNotNull(storedStatus);
    assertTrue(storedStatus.isDoneIndexing());
    assertEquals(storedStatus.getTotalRecords(), storedStatus.getRecordsCompleted());
    assertEquals(100, storedStatus.getPercentComplete(), 0);
    assertEquals(storedStatus.getEta(), 0);
    assertNotEquals(storedStatus.getDateStarted(), storedStatus.getStatusLastUpdatedAt());
    assertTrue(statusRepo.readAllCurrentlyIndexing().isEmpty());
    assertNotNull(storedStatus.getIndex());
    Index readIndex = indexRepo.read(lastname.getId());
    assertNotNull(readIndex);
    assertTrue(readIndex.isActive());
  }
}
