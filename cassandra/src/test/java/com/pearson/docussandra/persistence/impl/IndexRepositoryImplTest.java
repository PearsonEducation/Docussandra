
package com.pearson.docussandra.persistence.impl;

import com.pearson.docussandra.Utils;
import com.pearson.docussandra.cache.CacheFactory;
import com.pearson.docussandra.domain.objects.Database;
import com.pearson.docussandra.domain.objects.Identifier;
import com.pearson.docussandra.domain.objects.Index;
import com.pearson.docussandra.domain.objects.IndexIdentifier;
import com.pearson.docussandra.domain.objects.Table;
import com.pearson.docussandra.exception.ItemNotFoundException;
import com.pearson.docussandra.persistence.ITableRepository;
import com.pearson.docussandra.persistence.IndexRepository;
import com.pearson.docussandra.testhelper.Fixtures;
import java.io.IOException;
import java.util.List;
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
public class IndexRepositoryImplTest
{

    private static final Logger LOGGER = LoggerFactory.getLogger(IndexRepositoryImplTest.class);
    private static Fixtures f;
    private Table testTable = Fixtures.createTestTable();
    private ITableRepository iTableRepo;

    public IndexRepositoryImplTest() throws Exception
    {
        f = Fixtures.getInstance(true);
        iTableRepo = new ITableRepositoryImpl(f.getSession());
    }

    @AfterClass
    public static void tearDownClass()
    {
        f.clearTestTables();
    }

    @Before
    public void setUp() throws IOException, InterruptedException
    {
        Utils.initDatabaseSingleReplication(true, f.getSession());//hard clear of the test tables
        //Thread.sleep(5000);
        CacheFactory.clearAllCaches();
        Database testDb = Fixtures.createTestDatabase();
        f.insertDatabase(testDb);
        f.insertTable(testTable);
    }

    /**
     * Test of exists method, of class IndexRepositoryImpl.
     */
    @Test
    public void testExists()
    {
        System.out.println("exists");
        Index testIndex = Fixtures.createTestIndexOneField();
        Identifier identifier = testIndex.getId();
        IndexRepositoryImpl instance = new IndexRepositoryImpl(f.getSession());
        boolean result = instance.exists(identifier);
        assertEquals(false, result);
        f.insertIndex(testIndex);
        result = instance.exists(identifier);
        assertEquals(true, result);
    }

    /**
     * Test of read method, of class IndexRepositoryImpl.
     */
    @Test
    public void testReadById()
    {
        System.out.println("readById");
        Index testIndex = Fixtures.createTestIndexOneField();
        Identifier identifier = testIndex.getId();
        IndexRepositoryImpl instance = new IndexRepositoryImpl(f.getSession());
        boolean expectedExceptionThrown = false;
        try
        {
            instance.read(identifier);
        } catch (ItemNotFoundException e)
        {
            expectedExceptionThrown = true;
        }
        assertTrue("Expected exception not thrown", expectedExceptionThrown);
        f.insertIndex(testIndex);
        Index result = instance.read(identifier);
        assertEquals(testIndex, result);
    }

    /**
     * Test of create method, of class IndexRepositoryImpl.
     */
    @Test
    public void testCreateEntity()
    {
        System.out.println("create");
        Index entity = Fixtures.createTestIndexOneField();
        IndexRepositoryImpl instance = new IndexRepositoryImpl(f.getSession());
        Index expResult = entity;
        Index result = instance.create(entity);
        assertEquals(expResult, result);
        result = instance.read(result.getId());
        assertEquals(expResult, result);

        //check to make sure that the iTable got created        
        assertTrue("ITable was not created.", iTableRepo.iTableExists(entity));
    }

    /**
     * Test of markActive method, of class IndexRepositoryImpl.
     */
    @Test
    public void testMarkActive()
    {
        System.out.println("markActive");
        Index entity = Fixtures.createTestIndexOneField();
        f.insertIndex(entity);
        IndexRepositoryImpl instance = new IndexRepositoryImpl(f.getSession());
        Index result = instance.read(entity.getId());
        assertFalse(result.isActive());
        instance.markActive(entity);
        result = instance.read(entity.getId());
        assertTrue(result.isActive());
    }

    /**
     * Test of update method, of class IndexRepositoryImpl.
     */
    @Test
    public void testUpdate()
    {
        System.out.println("update");
        IndexRepositoryImpl instance = new IndexRepositoryImpl(f.getSession());
        boolean expectedExceptionThrown = false;
        try
        {
            Index result = instance.update(Fixtures.createTestIndexOneField());
        } catch (UnsupportedOperationException e)
        {
            expectedExceptionThrown = true;
        }
        assertTrue(expectedExceptionThrown);
    }

    /**
     * Test of delete method, of class IndexRepositoryImpl.
     */
    @Test
    public void testDelete()
    {
        System.out.println("delete");
        Index entity = Fixtures.createTestIndexOneField();
        f.insertIndex(entity);
        IndexRepositoryImpl instance = new IndexRepositoryImpl(f.getSession());
        instance.delete(entity);
        boolean expectedExceptionThrown = false;
        try
        {
            instance.read(entity.getId());
        } catch (ItemNotFoundException e)
        {
            expectedExceptionThrown = true;
        }
        assertTrue("Expected exception not thrown", expectedExceptionThrown);
        //check to make sure that the iTable got deleted
        assertFalse("ITable was not deleted.", iTableRepo.iTableExists(entity));
    }

    /**
     * Test of delete method, of class IndexRepositoryImpl.
     */
    @Test
    public void testDeleteById()
    {
        System.out.println("deleteById");
        Index entity = Fixtures.createTestIndexOneField();
        f.insertIndex(entity);
        IndexRepositoryImpl instance = new IndexRepositoryImpl(f.getSession());
        instance.delete(entity.getId());
        boolean expectedExceptionThrown = false;
        try
        {
            instance.read(entity.getId());
        } catch (ItemNotFoundException e)
        {
            expectedExceptionThrown = true;
        }
        assertTrue("Expected exception not thrown", expectedExceptionThrown);
        //check to make sure that the iTable got deleted
        assertFalse("ITable was not deleted.", iTableRepo.iTableExists(new IndexIdentifier(entity.getId())));//for the heck of it, we check by ID here too
        assertFalse("ITable was not deleted.", iTableRepo.iTableExists(entity));
    }

    /**
     * Test of delete method, of class IndexRepositoryImpl.
     */
    @Test
    public void testDeleteWithDeleteCascade() throws InterruptedException
    {
        System.out.println("deleteWithDeleteCascade");
        //setup
        f.insertIndex(Fixtures.createTestIndexOneField());
        //act
        IndexRepository indexRepo = new IndexRepositoryImpl(f.getSession());
        indexRepo.delete(Fixtures.createTestIndexOneField());
        //Thread.sleep(5000);
        //check index deletion        
        assertFalse(indexRepo.exists(Fixtures.createTestIndexOneField().getId()));
        //check iTable deletion
        assertFalse(iTableRepo.iTableExists(Fixtures.createTestIndexOneField()));
    }

    /**
     * Test of readAll method, of class IndexRepositoryImpl.
     */
    @Test
    public void testReadAll()
    {
        System.out.println("readAll");
        Index testIndex = Fixtures.createTestIndexOneField();
        Index testIndex2 = Fixtures.createTestIndexTwoField();
        f.insertIndex(testIndex);
        f.insertIndex(testIndex2);
        String database = testIndex.getDatabaseName();
        String table = testIndex.getTableName();
        IndexRepository instance = new IndexRepositoryImpl(f.getSession());
        List<Index> result = instance.readAll(new Identifier(database, table));
        assertEquals(2, result.size());
        assertEquals(result.get(0), testIndex);
        assertEquals(result.get(1), testIndex2);

    }

    /**
     * Test of readAllCached method, of class IndexRepositoryImpl.
     */
    @Test
    public void testReadAllCached()
    {
        System.out.println("readAllCached");
        Index testIndex = Fixtures.createTestIndexOneField();
        Index testIndex2 = Fixtures.createTestIndexTwoField();
        f.insertIndex(testIndex);
        f.insertIndex(testIndex2);
        String database = testIndex.getDatabaseName();
        String table = testIndex.getTableName();
        IndexRepository instance = new IndexRepositoryImpl(f.getSession());
        List<Index> result = instance.readAllCached(new Identifier(database, table));
        assertEquals(2, result.size());
        assertEquals(result.get(0), testIndex);
        assertEquals(result.get(1), testIndex2);
    }

    /**
     * Test of countAll method, of class IndexRepositoryImpl.
     */
    @Test
    public void testCountAll()
    {
        System.out.println("countAll");
        Index testIndex = Fixtures.createTestIndexOneField();
        Index testIndex2 = Fixtures.createTestIndexTwoField();
        f.insertIndex(testIndex);
        f.insertIndex(testIndex2);
        String database = testIndex.getDatabaseName();
        String table = testIndex.getTableName();
        IndexRepository instance = new IndexRepositoryImpl(f.getSession());
        long expResult = 2L;
        long result = instance.countAll(new Identifier(database, table));
        assertEquals(expResult, result);
    }

}
