package com.pearson.docussandra.abstracttests;

import com.pearson.docussandra.controller.*;
import com.jayway.restassured.RestAssured;
import static com.jayway.restassured.RestAssured.given;
import com.jayway.restassured.response.Response;
import com.pearson.docussandra.cache.CacheFactory;
import com.pearson.docussandra.domain.objects.Database;
import com.pearson.docussandra.domain.objects.Document;
import com.pearson.docussandra.domain.objects.Query;
import com.pearson.docussandra.domain.objects.Table;
import com.pearson.docussandra.persistence.impl.DocumentRepositoryImpl;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.pearson.docussandra.testhelper.Fixtures;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import static org.hamcrest.Matchers.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import org.junit.Before;
import org.junit.Test;
import testhelper.RestExpressManager;

/**
 * Tests for the query controller class. Located in the main test code so
 * it can be implemented outside of this project if needed.
 *
 * @author https://github.com/JeffreyDeYoung
 */
public abstract class AbstractQueryControllerTest
{

    private static final Logger logger = LoggerFactory.getLogger(AbstractQueryControllerTest.class);
    private static final String BASE_URI = "http://localhost";
    private static final int PORT = 19080;
    protected static Fixtures f;

    private JSONParser parser = new JSONParser();

    public AbstractQueryControllerTest(Fixtures f) throws Exception
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
    public void beforeTest() throws Exception
    {
        CacheFactory.clearAllCaches();//kill the cache and make it re-create for the purposes of this test.
        f.clearTestTables();
        Database testDb = Fixtures.createTestDatabase();
        f.insertDatabase(testDb);
        Table testTable = Fixtures.createTestTable();
        f.insertTable(testTable);
        f.insertIndex(Fixtures.createTestIndexOneField());
        f.insertIndex(Fixtures.createTestIndexTwoField());
        f.insertIndex(Fixtures.createTestIndexWithBulkDataHit());
        f.insertDocument(Fixtures.createTestDocument());
        Document onePrime = Fixtures.createTestDocument();
        onePrime.setUuid(new UUID(onePrime.getUuid().getMostSignificantBits() + 2, 1L));
        f.insertDocument(onePrime);
        f.insertDocument(Fixtures.createTestDocument2());
        Document twoPrime = Fixtures.createTestDocument2();
        twoPrime.setUuid(new UUID(twoPrime.getUuid().getMostSignificantBits() + 3, 2L));
        f.insertDocument(twoPrime);
        f.insertDocuments(Fixtures.getBulkDocuments());
        RestAssured.basePath = "/databases/" + testDb.name() + "/tables/" + testTable.name() + "/queries";
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
     * Tests that the POST /databases/{databases}/tables/{table}/queries
     * endpoint properly runs a query.
     */
    @Test
    public void postQueryTest()
    {
        Query q = Fixtures.createTestQuery();
        //act
        Response r = given()
                .header("Accept", "application/hal+json")
                .body("{\"where\":\"" + q.getWhere() + "\"}").expect()
                .statusCode(200)
                //                //.header("Location", startsWith(RestAssured.basePath + "/"))
                .body("", notNullValue())
                .body("_embedded.linkabledocuments", notNullValue())
                .body("_embedded.linkabledocuments[0].id", equalTo("00000000-0000-0000-0000-000000000001"))
                .body("_embedded.linkabledocuments[0].object", notNullValue())
                .body("_embedded.linkabledocuments[0].object.greeting", containsString("hello"))
                .body("_embedded.linkabledocuments[0]._links.self.href", containsString("00000000-0000-0000-0000-000000000001"))
                .when().post("").andReturn();
        logger.info(r.getBody().prettyPrint());
    }

    /**
     * Tests that the POST /databases/{databases}/tables/{table}/queries
     * endpoint properly runs a query.
     */
    @Test
    public void postQueryTestNoHAL()
    {
        Query q = Fixtures.createTestQuery();
        //act
        Response r = given()
                .body("{\"where\":\"" + q.getWhere() + "\"}").expect()
                .statusCode(200)
                .body("", notNullValue())
                .body("id[0]", equalTo("00000000-0000-0000-0000-000000000001"))
                .body("object[0]", notNullValue())
                .body("object[0].greeting", containsString("hello"))
                .when().post("").andReturn();
        logger.info(r.getBody().prettyPrint());
    }

    /**
     * Tests that the POST /databases/{databases}/tables/{table}/queries
     * endpoint properly runs a query with limits.
     */
    @Test
    public void postQueryTestOnNonIndexedField()
    {
        Query q = new Query();
        q.setWhere("field9999 = 'this is my data'");
        q.setDatabase(Fixtures.DB);
        q.setTable("mytable");
        //act
        given().body("{\"where\":\"" + q.getWhere() + "\"}").expect().statusCode(206)
                .expect().statusCode(400)
                .body("", notNullValue())
                .body("error", containsString("field9999"))
                .when().post("");
    }

    /**
     * Tests that the POST /databases/{databases}/tables/{table}/queries
     * endpoint properly runs a query with limits.
     */
    @Test
    public void postQueryTestWithLimit()
    {
        Query q = new Query();
        q.setWhere("field1 = 'this is my data'");
        q.setDatabase(Fixtures.DB);
        q.setTable("mytable");
        Response r = given().header("limit", "1").header("offset", "0").header("Accept", "application/hal+json")
                .body("{\"where\":\"" + q.getWhere() + "\"}").expect().statusCode(206)
                //.header("Location", startsWith(RestAssured.basePath + "/"))
                .body("", notNullValue())
                .body("_embedded.linkabledocuments", notNullValue())
                .body("_embedded.linkabledocuments[0].id", equalTo(new UUID(Long.MAX_VALUE - 33, 1l).toString()))
                .body("_embedded.linkabledocuments[0].object", notNullValue())
                .body("_embedded.linkabledocuments[0].object.field2", containsString("this is some more random data32"))
                .when().post("").andReturn();
        logger.info(r.getBody().prettyPrint());;
    }

    /**
     * Tests that the POST /databases/{databases}/tables/{table}/queries
     * endpoint properly runs a query with limits.
     */
    @Test
    public void postQueryTestWithLimitSameAsResponse()
    {
        Query q = new Query();
        q.setWhere("field1 = 'this is my data'");
        q.setDatabase(Fixtures.DB);
        q.setTable("mytable");
        //act
        given().header("limit", "34").header("offset", "0").header("Accept", "application/hal+json").body("{\"where\":\"" + q.getWhere() + "\"}").expect().statusCode(200)
                //.header("Location", startsWith(RestAssured.basePath + "/"))
                .body("", notNullValue())
                .body("_embedded.linkabledocuments", notNullValue())
                .body("_embedded.linkabledocuments[0].id", equalTo(new UUID(Long.MAX_VALUE - 33, 1l).toString()))
                .body("_embedded.linkabledocuments[0].object", notNullValue())
                .body("_embedded.linkabledocuments[0].object.field2", containsString("this is some more random data32"))
                .when().post("");
    }

    /**
     * Tests that the POST /databases/{databases}/tables/{table}/queries
     * endpoint properly runs a query with limits.
     */
    @Test
    public void postQueryTestWithLimitMoreThanResponse()
    {
        Query q = new Query();
        q.setWhere("field1 = 'this is my data'");
        q.setDatabase(Fixtures.DB);
        q.setTable("mytable");
        //act
        given().header("limit", "10000").header("offset", "0").header("Accept", "application/hal+json").body("{\"where\":\"" + q.getWhere() + "\"}").expect().statusCode(200)
                //.header("Location", startsWith(RestAssured.basePath + "/"))
                .body("", notNullValue())
                .body("_embedded.linkabledocuments", notNullValue())
                .body("_embedded.linkabledocuments[0].id", equalTo(new UUID(Long.MAX_VALUE - 33, 1l).toString()))
                .body("_embedded.linkabledocuments[0].object", notNullValue())
                .body("_embedded.linkabledocuments[0].object.field2", containsString("this is some more random data32"))
                .when().post("");
    }

    /**
     * Tests that the POST /databases/{databases}/tables/{table}/queries
     * endpoint properly runs a query with limits.
     */
    @Test
    public void postQueryTestWithLimitAndOffset()
    {
        Query q = new Query();
        q.setWhere("field1 = 'this is my data'");
        q.setDatabase(Fixtures.DB);
        q.setTable("mytable");
        //act
        given().header("limit", "1").header("offset", "1").header("Accept", "application/hal+json").body("{\"where\":\"" + q.getWhere() + "\"}").expect().statusCode(206)
                //.header("Location", startsWith(RestAssured.basePath + "/"))
                .body("", notNullValue())
                .body("_embedded.linkabledocuments", notNullValue())
                .body("_embedded.linkabledocuments[0].id", equalTo(new UUID(Long.MAX_VALUE - 32, 1l).toString()))
                .body("_embedded.linkabledocuments[0].object", notNullValue())
                .body("_embedded.linkabledocuments[0].object.field2", containsString("this is some more random data31"))
                .when().post("");
    }

    /**
     * Tests that the POST /databases/{databases}/tables/{table}/queries
     * endpoint properly runs a query with limits.
     */
    @Test
    public void postQueryTestWithLimitAndOffset2()
    {
        Query q = new Query();
        q.setWhere("field1 = 'this is my data'");
        q.setDatabase(Fixtures.DB);
        q.setTable("mytable");
        //act
        given().header("limit", "2").header("offset", "2").header("Accept", "application/hal+json").body("{\"where\":\"" + q.getWhere() + "\"}").expect().statusCode(206)
                //.header("Location", startsWith(RestAssured.basePath + "/"))
                .body("", notNullValue())
                .body("_embedded.linkabledocuments", notNullValue())
                .body("_embedded.linkabledocuments[0].id", equalTo(new UUID(Long.MAX_VALUE - 31, 1l).toString()))
                .body("_embedded.linkabledocuments[0].object", notNullValue())
                .body("_embedded.linkabledocuments[0].object.field2", containsString("this is some more random data30"))
                .body("_embedded.linkabledocuments[1].id", equalTo(new UUID(Long.MAX_VALUE - 30, 1l).toString()))
                .body("_embedded.linkabledocuments[1].object", notNullValue())
                .body("_embedded.linkabledocuments[1].object.field2", containsString("this is some more random data29"))
                .when().post("");
    }

    /**
     * Tests querying on integer types.
     *
     * @throws IOException
     * @throws ParseException
     */
    @Test
    public void testQueryOnIntegerType() throws IOException, ParseException
    {
        String restAssuredBasePath = RestAssured.basePath;
        Database testDb = Fixtures.createTestWorldBankDatabase();
        Table testTable = Fixtures.createTestWorldBankTable();
        List<Document> docs = Fixtures.getBulkDocumentsByLine("/world_bank_short.txt", testTable);
        try
        {
            //data setup
            RestAssured.basePath = "/databases/" + testTable.databaseName() + "/tables/" + testTable.name() + "/queries";
            f.insertDatabase(testDb);
            f.insertTable(testTable);
            //throw a few indexes in (including the one we are testing)
            f.insertIndex(Fixtures.createTestBankIndexLoanYear());
            f.insertIndex(Fixtures.createTestBankIndexApprovedOn());
            f.insertIndex(Fixtures.createTestBankIndexCountryName());
            f.insertDocuments(docs);//put in a ton of data directly into the db
            //end setup

            //act
            Response response = given().body("{\"where\":\"approvalfy = '1999'\"}").header("limit", "2000").header("Accept", "application/hal+json").expect().statusCode(200)
                    //.header("Location", startsWith(RestAssured.basePath + "/"))
                    .body("", notNullValue())
                    .body("_embedded.linkabledocuments", notNullValue())
                    .body("_embedded.linkabledocuments[0].id", notNullValue())
                    .body("_embedded.linkabledocuments[0].object", notNullValue())
                    .body("_embedded.linkabledocuments[0].object.approvalfy", equalTo(1999))
                    .when().post("").andReturn();

            String body = response.getBody().prettyPrint();
            logger.debug("Status Response: " + body);

            JSONObject bodyObject = (JSONObject) parser.parse(body);
            JSONObject embeddedObject = (JSONObject) bodyObject.get("_embedded");
            JSONArray bodyArray = (JSONArray) embeddedObject.get("linkabledocuments");
            for (Object responseElement : bodyArray)
            {
                JSONObject responseElementJson = (JSONObject) responseElement;
                assertNotNull(responseElementJson);
                assertNotNull(responseElementJson.get("id"));
                JSONObject object = (JSONObject) responseElementJson.get("object");
                assertNotNull(object);
                assertEquals(1999l, object.get("approvalfy"));
            }
        } finally
        {
            //clean up
            RestAssured.basePath = restAssuredBasePath;

            DocumentRepositoryImpl docrepo = new DocumentRepositoryImpl(f.getSession());
            for (Document d : docs)
            {
                try
                {
                    docrepo.delete(d);
                } catch (Exception e)
                {
                    logger.error("Error in cleaning up, probably the doc never got created");
                }
            }
        }
    }

    /**
     * Tests querying on integer types, with a query that doesn't contain an
     * integer.
     *
     * @throws IOException
     * @throws ParseException
     */
    @Test
    public void testQueryOnIntegerTypeBadFormat() throws IOException, ParseException
    {
        String restAssuredBasePath = RestAssured.basePath;
        Database testDb = Fixtures.createTestWorldBankDatabase();
        Table testTable = Fixtures.createTestWorldBankTable();
        List<Document> docs = Fixtures.getBulkDocumentsByLine("/world_bank_short.txt", testTable);
        try
        {
            //data setup
            RestAssured.basePath = "/databases/" + testTable.databaseName() + "/tables/" + testTable.name() + "/queries";
            f.insertDatabase(testDb);
            f.insertTable(testTable);
            //throw a few indexes in (including the one we are testing)
            f.insertIndex(Fixtures.createTestBankIndexLoanYear());
            f.insertIndex(Fixtures.createTestBankIndexApprovedOn());
            f.insertIndex(Fixtures.createTestBankIndexCountryName());
            f.insertDocuments(docs);//put in a ton of data directly into the db
            //end setup

            //act
            Response response = given().body("{\"where\":\"approvalfy = 'Two Thousand and One'\"}").header("Accept", "application/hal+json").header("limit", "2000")
                    .expect().statusCode(400)
                    .body("", notNullValue())
                    .body("error", containsString("Two Thousand and One"))
                    .body("error", containsString("approvalfy"))
                    .when().post("").andReturn();

            String body = response.getBody().prettyPrint();
            logger.debug("Status Response: " + body);

        } finally
        {
            //clean up
            RestAssured.basePath = restAssuredBasePath;

            DocumentRepositoryImpl docrepo = new DocumentRepositoryImpl(f.getSession());
            for (Document d : docs)
            {
                try
                {
                    docrepo.delete(d);
                } catch (Exception e)
                {
                    logger.error("Error in cleaning up, probably the doc never got created");
                }
            }
        }
    }

    /**
     * Tests querying on date types.
     *
     * @throws IOException
     * @throws ParseException
     */
    @Test
    public void testQueryOnDateType() throws IOException, ParseException
    {
        String restAssuredBasePath = RestAssured.basePath;
        Database testDb = Fixtures.createTestWorldBankDatabase();
        Table testTable = Fixtures.createTestWorldBankTable();
        List<Document> docs = Fixtures.getBulkDocumentsByLine("/world_bank_short.txt", testTable);
        try
        {
            //data setup
            RestAssured.basePath = "/databases/" + testTable.databaseName() + "/tables/" + testTable.name() + "/queries";
            f.insertDatabase(testDb);
            f.insertTable(testTable);
            //throw a few indexes in (including the one we are testing)
            f.insertIndex(Fixtures.createTestBankIndexLoanYear());
            f.insertIndex(Fixtures.createTestBankIndexApprovedOn());
            f.insertIndex(Fixtures.createTestBankIndexCountryName());
            f.insertDocuments(docs);//put in a ton of data directly into the db
            //end setup

            //act
            Response response = given().body("{\"where\":\"boardapprovaldate = '2013-10-31T00:00:00Z'\"}").header("limit", "2000").header("Accept", "application/hal+json").expect().statusCode(200)
                    //.header("Location", startsWith(RestAssured.basePath + "/"))
                    .body("", notNullValue())
                    .body("_embedded.linkabledocuments", notNullValue())
                    .body("_embedded.linkabledocuments[0].id", notNullValue())
                    .body("_embedded.linkabledocuments[0].object", notNullValue())
                    .body("_embedded.linkabledocuments[0].object.boardapprovaldate", containsString("2013-10-31T00:00:00Z"))
                    .when().post("").andReturn();

            String body = response.getBody().prettyPrint();
            logger.debug("Status Response: " + body);

            JSONObject bodyObject = (JSONObject) parser.parse(body);
            JSONObject embeddedObject = (JSONObject) bodyObject.get("_embedded");
            JSONArray bodyArray = (JSONArray) embeddedObject.get("linkabledocuments");
            for (Object responseElement : bodyArray)
            {
                JSONObject responseElementJson = (JSONObject) responseElement;
                assertNotNull(responseElementJson);
                assertNotNull(responseElementJson.get("id"));
                JSONObject object = (JSONObject) responseElementJson.get("object");
                assertNotNull(object);
                assertEquals("2013-10-31T00:00:00Z", object.get("boardapprovaldate"));
            }
        } finally
        {
            //clean up
            RestAssured.basePath = restAssuredBasePath;

            DocumentRepositoryImpl docrepo = new DocumentRepositoryImpl(f.getSession());
            for (Document d : docs)
            {
                try
                {
                    docrepo.delete(d);
                } catch (Exception e)
                {
                    logger.error("Error in cleaning up, probably the doc never got created");
                }
            }
        }
    }

    /**
     * Tests querying on timepoint types.
     *
     * @throws IOException
     * @throws ParseException
     */
    @Test
    public void testQueryOnTimepointType() throws IOException, ParseException
    {
        String restAssuredBasePath = RestAssured.basePath;
        Database testDb = Fixtures.createTestWorldBankDatabase();
        Table testTable = Fixtures.createTestWorldBankTable();
        List<Document> docs = Fixtures.getBulkDocumentsByLine("/world_bank_short.txt", testTable);
        try
        {
            //data setup
            RestAssured.basePath = "/databases/" + testTable.databaseName() + "/tables/" + testTable.name() + "/queries";
            f.insertDatabase(testDb);
            f.insertTable(testTable);
            //throw a few indexes in (including the one we are testing)
            f.insertIndex(Fixtures.createTestBankIndexLoanYear());
            f.insertIndex(Fixtures.createTestBankIndexApprovedOnTimepoint());
            f.insertIndex(Fixtures.createTestBankIndexCountryName());
            f.insertDocuments(docs);//put in a ton of data directly into the db
            //end setup

            //act
            Response response = given().body("{\"where\":\"boardapprovaldate = '2013-10-31T00:00:00Z'\"}").header("limit", "2000").header("Accept", "application/hal+json").expect().statusCode(200)
                    //.header("Location", startsWith(RestAssured.basePath + "/"))
                    .body("_embedded.linkabledocuments[0]", notNullValue())
                    .body("_embedded.linkabledocuments[0].id", notNullValue())
                    .body("_embedded.linkabledocuments[0].object", notNullValue())
                    .body("_embedded.linkabledocuments[0].object.boardapprovaldate", containsString("2013-10-31T00:00:00Z"))
                    .when().post("").andReturn();

            String body = response.getBody().prettyPrint();
            logger.debug("Status Response: " + body);

            JSONObject bodyObject = (JSONObject) parser.parse(body);
            JSONObject embeddedObject = (JSONObject) bodyObject.get("_embedded");
            JSONArray bodyArray = (JSONArray) embeddedObject.get("linkabledocuments");
            for (Object responseElement : bodyArray)
            {
                JSONObject responseElementJson = (JSONObject) responseElement;
                assertNotNull(responseElementJson);
                assertNotNull(responseElementJson.get("id"));
                JSONObject object = (JSONObject) responseElementJson.get("object");
                assertNotNull(object);
                assertEquals("2013-10-31T00:00:00Z", object.get("boardapprovaldate"));
            }
        } finally
        {
            //clean up
            RestAssured.basePath = restAssuredBasePath;

            DocumentRepositoryImpl docrepo = new DocumentRepositoryImpl(f.getSession());
            for (Document d : docs)
            {
                try
                {
                    docrepo.delete(d);
                } catch (Exception e)
                {
                    logger.error("Error in cleaning up, probably the doc never got created");
                }
            }
        }
    }

}
