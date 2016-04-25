package com.pearson.docussandra.abstracttests;

import com.jayway.restassured.RestAssured;
import static com.jayway.restassured.RestAssured.expect;
import static com.jayway.restassured.RestAssured.given;
import com.pearson.docussandra.domain.objects.Database;
import com.pearson.docussandra.persistence.impl.DocumentRepositoryImpl;
import com.pearson.docussandra.persistence.ITableRepository;
import com.pearson.docussandra.persistence.impl.ITableRepositoryImpl;
import com.pearson.docussandra.persistence.impl.IndexRepositoryImpl;
import com.pearson.docussandra.persistence.impl.TableRepositoryImpl;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.pearson.docussandra.testhelper.Fixtures;
import static org.hamcrest.Matchers.*;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;
import com.pearson.docussandra.testhelper.RestExpressManager;

/**
 * Tests for the database controller tests
 * ROUTE: /databases/
 * @author https://github.com/JeffreyDeYoung
 */
public abstract class AbstractDatabaseControllerTest
{

    private static final Logger logger = LoggerFactory.getLogger(AbstractDatabaseControllerTest.class);
    private static final String BASE_URI = "http://localhost";
    private static final int PORT = 19080;
    private static Fixtures f;

    public AbstractDatabaseControllerTest(Fixtures f) throws Exception
    {
        this.f = f;
    }

    /**
     * Initialization that is performed once before any of the tests in this
     * class are executed.
     *
     * @throws Exception
     */
    @BeforeClass
    public static void beforeClass() throws Exception
    {
        RestAssured.baseURI = BASE_URI;
        RestAssured.port = PORT;
        RestAssured.basePath = "/databases";

/*        String testEnv = System.getProperty("TEST_ENV") != null ? System.getProperty("TEST_ENV") : "local";
        String[] env = {testEnv};
        Thread.sleep(10000);*/
        logger.debug("Loading RestExpress Environment... ");
        f = Fixtures.getInstance();
        RestExpressManager.getManager().ensureRestExpressRunning();
    }

    @Before
    public void beforeTest()
    {
        f.clearTestTables();
    }

    /**
     * Cleanup that is performed once after all of the tests in this class are
     * executed.
     */
    @AfterClass
    public static void afterClass()
    {
        f.clearTestTables();
    }

    /**
     * Cleanup that is performed after each test is executed.
     */
    @After
    public void afterTest()
    {

    }

    /**
     * Tests that the GET / properly retrieves all existing database
     */
    @Test
    public void getDatabasesTest() {
        Database testDb = Fixtures.createTestDatabase();
        f.insertDatabase(testDb);
        //not the best test
        expect().statusCode(200)
                .get();
    }
    /**
     * Tests that the GET /{databases} properly retrieves an existing database.
     */
    @Test
    public void getDatabaseTest()
    {
        Database testDb = Fixtures.createTestDatabase();
        f.insertDatabase(testDb);
        expect().statusCode(200)
                .body("name", equalTo(testDb.name()))
                .body("description", equalTo(testDb.description()))
                .body("createdAt", notNullValue())
                .body("updatedAt", notNullValue()).when()
                .get("/" + testDb.name());
    }

    /**
     * Tests that the POST /{databases} endpoint properly creates a database.
     */
    @Test
    public void postDatabaseTest()
    {
        Database testDb = Fixtures.createTestDatabase();
        String dbStr = "{" + "\"description\" : \"" + testDb.description()
                + "\"," + "\"name\" : \"" + testDb.name() + "\"}";
        //act
        given().body(dbStr).expect().statusCode(201)
                //.header("Location", startsWith(RestAssured.basePath + "/"))
                .body("name", equalTo(testDb.name()))
                .body("description", equalTo(testDb.description()))
                .body("createdAt", notNullValue())
                .body("updatedAt", notNullValue())
                .when().post(testDb.name());
        //check
        expect().statusCode(200)
                .body("name", equalTo(testDb.name()))
                .body("description", equalTo(testDb.description()))
                .body("createdAt", notNullValue())
                .body("updatedAt", notNullValue()).when()
                .get("/" + testDb.name());
    }

    /**
     * Tests that the PUT /{databases} endpoint properly updates a database.
     */
    @Test
    public void putDatabaseTest()
    {
        Database testDb = Fixtures.createTestDatabase();
        f.insertDatabase(testDb);
        String newDesciption = "this is a new description";
        String dbStr = "{" + "\"description\" : \"" + newDesciption
                + "\"," + "\"name\" : \"" + testDb.name() + "\"}";
        //act
        given().body(dbStr).expect().statusCode(204)
                .when().put(testDb.name());
        //check
        expect().statusCode(200)
                .body("name", equalTo(testDb.name()))
                .body("description", equalTo(newDesciption))
                .body("createdAt", notNullValue())
                .body("updatedAt", notNullValue()).when()
                .get("/" + testDb.name());

    }

    /**
     * Tests that the DELETE /{databases} endpoint properly deletes a database.
     */
    @Test
    public void deleteDatabaseTest()
    {
        Database testDb = Fixtures.createTestDatabase();
        f.insertDatabase(testDb);
        //act
        given().expect().statusCode(204)
                .when().delete(testDb.name());
        //check
        expect().statusCode(404).when()
                .get(testDb.name());
    }

    /**
     * Tests that the DELETE /{databases} endpoint properly deletes a database
     * and then cascades that delete to any child tables.
     */
    @Test
    public void deleteDatabaseTestWithDeleteCascade() throws InterruptedException
    {
        System.out.println("deleteEntityWithDeleteCascade");
        //setup
        final Database testDb = Fixtures.createTestDatabase();
        f.insertDatabase(testDb);
        f.insertTable(Fixtures.createTestTable());
        f.insertIndex(Fixtures.createTestIndexOneField());
        f.insertDocument(Fixtures.createTestDocument());
        //act
        given().expect().statusCode(204)
                .when().delete(testDb.name());
        //Thread.sleep(5000);
        //check DB deletion
        expect().statusCode(404).when()
                .get(testDb.name());
        //check table deletion (using direct db calls instead of REST-- being slightly lazy here)
        TableRepositoryImpl tableRepo = new TableRepositoryImpl(f.getSession());
        assertFalse(tableRepo.exists(Fixtures.createTestTable().getId()));
        //check index deletion (using direct db calls instead of REST-- being slightly lazy here)
        IndexRepositoryImpl indexRepo = new IndexRepositoryImpl(f.getSession());
        assertFalse(indexRepo.exists(Fixtures.createTestIndexOneField().getId()));
        //check iTable deletion (using direct db calls instead of REST-- being slightly lazy here)
        ITableRepository iTableRepo = new ITableRepositoryImpl(f.getSession());
        assertFalse(iTableRepo.iTableExists(Fixtures.createTestIndexOneField()));
        //check document deletion (using direct db calls instead of REST-- being slightly lazy here)
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
}
