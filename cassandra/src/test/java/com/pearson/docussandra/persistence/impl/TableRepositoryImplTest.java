
package com.pearson.docussandra.persistence.impl;

import com.pearson.docussandra.cache.CacheFactory;
import com.pearson.docussandra.domain.objects.Database;
import com.pearson.docussandra.domain.objects.Identifier;
import com.pearson.docussandra.domain.objects.Table;
import com.pearson.docussandra.persistence.ITableRepository;
import com.pearson.docussandra.persistence.IndexRepository;
import com.pearson.docussandra.persistence.TableRepository;
import com.pearson.docussandra.testhelper.Fixtures;
import java.util.ArrayList;
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
 * Test for the table repo
 *
 * @author https://github.com/JeffreyDeYoung
 */
public class TableRepositoryImplTest
{

    private static final Logger LOGGER = LoggerFactory.getLogger(TableRepositoryImplTest.class);
    private static Fixtures f;

    public TableRepositoryImplTest() throws Exception
    {
        f = Fixtures.getInstance(true);
    }

    @BeforeClass
    public static void setUpClass()
    {
    }

    @AfterClass
    public static void tearDownClass()
    {
        f.clearTestTables();
    }

    @Before
    public void setUp()
    {
        f.clearTestTables();
        CacheFactory.clearAllCaches();
        Database testDb = Fixtures.createTestDatabase();
        f.insertDatabase(testDb);
    }

    @After
    public void tearDown()
    {

    }

    /**
     * Test of exists method, of class TableRepositoryImpl.
     */
    @Test
    public void testExists()
    {
        System.out.println("exists");
        Table testTable = Fixtures.createTestTable();
        Identifier identifier = testTable.getId();
        TableRepository instance = new TableRepositoryImpl(f.getSession());
        boolean result = instance.exists(identifier);
        assertEquals(false, result);
        f.insertTable(testTable);
        result = instance.exists(identifier);
        assertEquals(true, result);
    }

    /**
     * Test of read method, of class TableRepositoryImpl.
     */
    @Test
    public void testReadById()
    {
        System.out.println("readById");
        Table testTable = Fixtures.createTestTable();
        Identifier identifier = testTable.getId();
        TableRepository instance = new TableRepositoryImpl(f.getSession());
        f.insertTable(testTable);
        Table result = instance.read(identifier);
        assertNotNull(result);
        assertEquals(testTable, result);
    }

    /**
     * Test of create method, of class TableRepositoryImpl.
     */
    @Test
    public void testCreate()
    {
        System.out.println("create");
        Table entity = Fixtures.createTestTable();
        TableRepository instance = new TableRepositoryImpl(f.getSession());
        Table result = instance.create(entity);
        assertEquals(entity, result);

    }

    /**
     * Test of update method, of class TableRepositoryImpl.
     */
    @Test
    public void testUpdate()
    {
        System.out.println("update");
        Table entity = Fixtures.createTestTable();
        TableRepository instance = new TableRepositoryImpl(f.getSession());
        Table created = instance.create(entity);
        assertEquals(entity, created);
        String newDesciption = "this is a new description";
        created.description(newDesciption);
        Table result = instance.update(entity);
        assertEquals(created, result);
        result.name("new_name1");
        Table resultNew = instance.update(entity);
        assertEquals(result, resultNew);
        instance.delete(resultNew.getId());
    }

    /**
     * Test of delete method, of class TableRepositoryImpl.
     */
    @Test
    public void testDelete()
    {
        System.out.println("delete");
        Table testTable = Fixtures.createTestTable();
        Identifier identifier = testTable.getId();
        f.insertTable(testTable);
        TableRepository instance = new TableRepositoryImpl(f.getSession());
        boolean result = instance.exists(identifier);
        assertEquals(true, result);
        instance.delete(identifier);
        result = instance.exists(identifier);
        assertEquals(false, result);
    }

    /**
     * Test of delete method, of class TableRepositoryImpl.
     */
    @Test
    public void testDeleteByTable()
    {
        System.out.println("deleteByTable");
        Table testTable = Fixtures.createTestTable();
        Identifier identifier = testTable.getId();
        f.insertTable(testTable);
        TableRepository instance = new TableRepositoryImpl(f.getSession());
        boolean result = instance.exists(identifier);
        assertEquals(true, result);
        instance.delete(testTable);
        result = instance.exists(identifier);
        assertEquals(false, result);
    }

    /**
     * Test of delete method, of class TableRepositoryImpl.
     */
    @Test
    public void testDeleteWithDeleteCascade() throws InterruptedException
    {
        System.out.println("deleteWithDeleteCascade");
        //setup
        f.insertTable(Fixtures.createTestTable());
        f.insertIndex(Fixtures.createTestIndexOneField());
        f.insertDocument(Fixtures.createTestDocument());
        //act
        TableRepository tableRepo = new TableRepositoryImpl(f.getSession());
        tableRepo.delete(Fixtures.createTestTable());
        //Thread.sleep(5000);

        //check table deletion
        assertFalse(tableRepo.exists(Fixtures.createTestTable().getId()));
        //check index deletion
        IndexRepository indexRepo = new IndexRepositoryImpl(f.getSession());
        assertFalse(indexRepo.exists(Fixtures.createTestIndexOneField().getId()));
        //check iTable deletion
        ITableRepository iTableRepo = new ITableRepositoryImpl(f.getSession());
        assertFalse(iTableRepo.iTableExists(Fixtures.createTestIndexOneField()));
        //check document deletion
        DocumentRepositoryImpl docRepo = new DocumentRepositoryImpl(f.getSession());
        boolean expectedExceptionThrown = false;
        try
        {
            docRepo.exists(Fixtures.createTestDocument().getId());
        } catch (Exception e)//should error because the entire table should no longer exist
        {
            assertTrue(e.getMessage().contains("unconfigured columnfamily"));
            assertTrue(e.getMessage().contains(Fixtures.createTestTable().toDbTable()));
            expectedExceptionThrown = true;
        }
        assertTrue(expectedExceptionThrown);
    }

    /**
     * Test of readAll method, of class TableRepositoryImpl.
     */
    @Test
    public void testReadAll()
    {
        System.out.println("readAll");
        Table testTable = Fixtures.createTestTable();
        f.insertTable(testTable);
        String database = testTable.databaseName();
        TableRepository instance = new TableRepositoryImpl(f.getSession());
        List<Table> expResult = new ArrayList<>();
        expResult.add(testTable);
        List<Table> result = instance.readAll(new Identifier(database));
        assertEquals(expResult, result);
    }

    /**
     * Test of countAllTables method, of class TableRepositoryImpl.
     */
    @Test
    public void testCountAllTables()
    {
        System.out.println("countAllTables");
        TableRepository instance = new TableRepositoryImpl(f.getSession());
        Table testTable = Fixtures.createTestTable();
        String database = testTable.databaseName();
        long result = instance.countAllTables(database);
        long expResult = 0L;
        assertEquals(expResult, result);
        f.insertTable(testTable);
        expResult = 1L;
        result = instance.countAllTables(database);
        assertEquals(expResult, result);
    }

    /**
     * Test of countTableSize method, of class TableRepositoryImpl.
     */
    @Test
    public void testCountTableSize()
    {
        System.out.println("countTableSize");
        TableRepository instance = new TableRepositoryImpl(f.getSession());
        Table testTable = Fixtures.createTestTable();
        String database = testTable.databaseName();
        String tableName = testTable.name();
        f.insertTable(testTable);
        long expResult = 0L;
        long result = instance.countTableSize(database, tableName);
        assertEquals(expResult, result);

        f.insertDocument(Fixtures.createTestDocument());
        expResult = 1L;
        result = instance.countTableSize(database, tableName);
        assertEquals(expResult, result);

        f.insertDocument(Fixtures.createTestDocument2());
        expResult = 2L;
        result = instance.countTableSize(database, tableName);
        assertEquals(expResult, result);
    }

}
