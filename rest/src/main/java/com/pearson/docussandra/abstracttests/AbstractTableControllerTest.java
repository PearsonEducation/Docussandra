package com.pearson.docussandra.abstracttests;

import com.jayway.restassured.RestAssured;
import static com.jayway.restassured.RestAssured.expect;
import static com.jayway.restassured.RestAssured.given;
import com.pearson.docussandra.domain.objects.Database;
import com.pearson.docussandra.domain.objects.Table;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.pearson.docussandra.testhelper.Fixtures;
import static org.hamcrest.Matchers.*;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import testhelper.RestExpressManager;

/**
 * Tests for the TableController class. ROUTE :
 * /databases/{database}/tables/{table}
 *
 * @author https://github.com/JeffreyDeYoung
 */
public abstract class AbstractTableControllerTest
{

    private static final Logger logger = LoggerFactory.getLogger(AbstractTableControllerTest.class);
    private static final String BASE_URI = "http://localhost";
    private static final int PORT = 19080;
    private static Fixtures f;

    public AbstractTableControllerTest(Fixtures f) throws Exception
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
        f = Fixtures.getInstance();
        RestExpressManager.getManager().ensureRestExpressRunning();
    }

    @Before
    public void beforeTest()
    {
        f.clearTestTables();
        Database testDb = Fixtures.createTestDatabase();
        f.insertDatabase(testDb);
        RestAssured.basePath = "/databases/" + testDb.name() + "/tables/";
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
     * Tests that the GET /databases/{databases}/tables/{table} properly
     * retrieves an existing table.
     */
    @Test
    public void getTableTest()
    {
        Table testTable = Fixtures.createTestTable();
        f.insertTable(testTable);
        expect().statusCode(200)
                .body("name", equalTo(testTable.name()))
                .body("description", equalTo(testTable.description()))
                .body("createdAt", notNullValue())
                .body("updatedAt", notNullValue()).when()
                .get(testTable.name());
    }

    /**
     * Tests that the POST /databases/{databases}/tables/{table} endpoint
     * properly creates a table.
     */
    @Test
    public void postTableTest()
    {
        Table testTable = Fixtures.createTestTable();
        String tableStr = "{" + "\"description\" : \"" + testTable.description()
                + "\"," + "\"name\" : \"" + testTable.name() + "\"}";
        //act
        given().body(tableStr).expect().statusCode(201)
                //.header("Location", startsWith(RestAssured.basePath + "/"))
                .body("name", equalTo(testTable.name()))
                .body("description", equalTo(testTable.description()))
                .body("createdAt", notNullValue())
                .body("updatedAt", notNullValue())
                .when().post("/" + testTable.name());
        //check
        expect().statusCode(200)
                .body("name", equalTo(testTable.name()))
                .body("description", equalTo(testTable.description()))
                .body("createdAt", notNullValue())
                .body("updatedAt", notNullValue()).when()
                .get(testTable.name());
    }

    /**
     * Tests that the PUT /databases/{databases}/tables/{table} endpoint
     * properly updates a table.
     */
    @Test
    public void putTableTest()
    {
        Table testTable = Fixtures.createTestTable();
        f.insertTable(testTable);
        String newDesciption = "this is a new description";
        String tableStr = "{" + "\"description\" : \"" + newDesciption
                + "\"," + "\"name\" : \"" + testTable.name() + "\"}";

        //act
        given().body(tableStr).expect().statusCode(204)
                .when().put(testTable.name());

        //check
        expect().statusCode(200)
                .body("name", equalTo(testTable.name()))
                .body("description", equalTo(newDesciption))
                .body("createdAt", notNullValue())
                .body("updatedAt", notNullValue()).when()
                .get(testTable.name());
    }

    /**
     * Tests that the DELETE /{databases}/{table} endpoint properly deletes a
     * table.
     */
    @Test
    public void deleteTableTest()
    {
        Table testTable = Fixtures.createTestTable();
        f.insertTable(testTable);
        //act
        given().expect().statusCode(204)
                .when().delete(testTable.name());
        //check
        expect().statusCode(404).when()
                .get(testTable.name());
    }
}
