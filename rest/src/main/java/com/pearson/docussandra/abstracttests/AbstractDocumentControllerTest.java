package com.pearson.docussandra.abstracttests;

import com.jayway.restassured.RestAssured;
import static com.jayway.restassured.RestAssured.expect;
import static com.jayway.restassured.RestAssured.given;
import com.jayway.restassured.response.Response;
import com.mongodb.util.JSON;
import com.pearson.docussandra.cache.CacheFactory;
import com.pearson.docussandra.domain.objects.Database;
import com.pearson.docussandra.domain.objects.Document;
import com.pearson.docussandra.domain.objects.Table;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.pearson.docussandra.testhelper.Fixtures;
import java.util.UUID;
import org.bson.BSONObject;
import static org.hamcrest.Matchers.*;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import com.pearson.docussandra.testhelper.RestExpressManager;

/**
 * Tests for the document controller class.
 * ROUTE : /databases/{database}/tables/{table}/documents
 *
 * @author https://github.com/JeffreyDeYoung
 */
public abstract class AbstractDocumentControllerTest
{

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractDocumentControllerTest.class);
    private static final String BASE_URI = "http://localhost";
    private static final int PORT = 19080;
    private static Fixtures f;

    public AbstractDocumentControllerTest(Fixtures f) throws Exception
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
        f = Fixtures.getInstance();
        RestAssured.baseURI = BASE_URI;
        RestAssured.port = PORT;
        RestExpressManager.getManager().ensureRestExpressRunning();
    }

    @Before
    public void beforeTest()
    {
        f.clearTestTables();
        //clear all caches for the sake of this test, we will be creating and
        //deleting more frequently than a real world operation causing the cache
        //to hit no longer relevent objects
        CacheFactory.clearAllCaches();
        Database testDb = Fixtures.createTestDatabase();
        f.insertDatabase(testDb);
        Table testTable = Fixtures.createTestTable();
        f.insertTable(testTable);
        RestAssured.basePath = "/databases/" + testDb.getName() + "/tables/" + testTable.getName() + "/documents";
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
     * Tests that the GET
     * /databases/{databases}/tables/{table}/documents/{document} properly
     * retrieves an existing document.
     */
    @Test
    public void getDocumentTest()
    {
        Document testDocument = Fixtures.createTestDocument();
        f.insertDocument(testDocument);
        expect().statusCode(200)
                .body("_links", notNullValue())
                .body("_links.up", notNullValue())
                .body("_links.self", notNullValue())
                .body("id", equalTo(testDocument.getUuid().toString()))
                .body("object", notNullValue())
                .body("object.greeting", containsString("hello"))
                .body("createdAt", notNullValue())
                .body("updatedAt", notNullValue()).when()
                .get(testDocument.getUuid().toString());
        //cleanup the random uuid'ed doc
        f.deleteDocument(testDocument);
    }

    /**
     * Tests that the POST /databases/{databases}/tables/{table}/documents
     * endpoint properly creates a document.
     */
    @Test
    public void postDocumentTest()
    {
        Document testDocument = Fixtures.createTestDocument();
        String documentStr = testDocument.getObjectAsString();

        //act
        Response r = given().body(documentStr).expect().statusCode(201)
                .body("id", notNullValue())
                .body("object", notNullValue())
                .body("object.greeting", containsString("hello"))
                .body("createdAt", notNullValue())
                .body("updatedAt", notNullValue())
                .when().post().andReturn();

        BSONObject bson = (BSONObject) JSON.parse(r.getBody().asString());
        String id = (String) bson.get("id");
        //check
        expect().statusCode(200)
                .body("id", equalTo(id))
                .body("object", notNullValue())
                .body("object.greeting", containsString("hello"))
                .body("createdAt", notNullValue())
                .body("updatedAt", notNullValue())
                .get(id);
        testDocument.setUuid(UUID.fromString(id));
        //cleanup the random uuid'ed doc
        f.deleteDocument(testDocument);
    }

    /**
     * Tests that the POST /databases/{databases}/tables/{table}/documents
     * endpoint properly creates a document.
     */
    @Test
    public void postDocumentWithAllDataTypesTest()
    {
        //create the index first so we are sure it will get parsed
        f.insertIndex(Fixtures.createTestIndexAllFieldTypes());
        Document testDocument = Fixtures.createTestDocument3();
        String documentStr = testDocument.getObjectAsString();

        //act
        Response r = given().body(documentStr).expect().statusCode(201)
                .body("id", notNullValue())
                .body("object", notNullValue())
                .body("object.thisisastring", containsString("hello"))
                .body("createdAt", notNullValue())
                .body("updatedAt", notNullValue())
                .when().post().andReturn();

        BSONObject bson = (BSONObject) JSON.parse(r.getBody().asString());
        String id = (String) bson.get("id");
        //check
        expect().statusCode(200)
                .body("id", equalTo(id))
                .body("object", notNullValue())
                .body("object.thisisastring", containsString("hello"))
                .body("createdAt", notNullValue())
                .body("updatedAt", notNullValue())
                .get(id);
        testDocument.setUuid(UUID.fromString(id));
        //cleanup the random uuid'ed doc
        f.deleteDocument(testDocument);
    }

    /**
     * Tests that the POST /databases/{databases}/tables/{table}/documents
     * endpoint properly creates a document.
     */
    @Test
    public void postDocumentWithInvalidDataTypesTest()
    {
        //create the index first so we are sure it will get parsed
        f.insertIndex(Fixtures.createTestIndexAllFieldTypes());

        String documentStr = "{\"thisisastring\":\"hello\", \"thisisanint\": \"five\", \"thisisadouble\":\"five point five five five\","
                + " \"thisisbase64\":\"nope!\", \"thisisaboolean\":\"blah!\","
                + " \"thisisadate\":\"day 0\", \"thisisauudid\":\"x\"}";//completely botched field types

        //act
        given().body(documentStr).expect().statusCode(400)
                .body("error", notNullValue())
                .body("error", containsString("could not be parsed"))
                .when().post().andReturn();
    }

    /**
     * Tests that the PUT
     * /databases/{databases}/tables/{table}/documents/{document} endpoint
     * properly updates a document.
     */
    @Test
    public void putDocumentTest()
    {
        Document testDocument = Fixtures.createTestDocument();
        f.insertDocument(testDocument);
        String newObject = "{\"newjson\": \"object\"}";
        //act
        given().body(newObject).expect().statusCode(204)
                .when().put(testDocument.getUuid().toString());

        //check
        Response response = expect().statusCode(200)
                .body("id", equalTo(testDocument.getUuid().toString()))
                .body("object", notNullValue())
                .body("object.newjson", equalTo("object"))
                .body("createdAt", notNullValue())
                .body("updatedAt", notNullValue()).when()
                .get(testDocument.getUuid().toString()).andReturn();
        LOGGER.debug("body for put response: " + response.getBody().prettyPrint());
        //cleanup the random uuid'ed doc
        f.deleteDocument(testDocument);
    }

    /**
     * Tests that the PUT
     * /databases/{databases}/tables/{table}/documents/{document} endpoint
     * properly updates a document.
     */
    @Test
    public void putDocumentWithAllDataTypesTest()
    {
        Document testDocument = Fixtures.createTestDocument3();
        f.insertDocument(testDocument);
        String newObject = "{\"newjson\": \"object\"}";
        //act
        given().body(newObject).expect().statusCode(204)
                .when().put(testDocument.getUuid().toString());

        //check
        Response response = expect().statusCode(200)
                .body("id", equalTo(testDocument.getUuid().toString()))
                .body("object", notNullValue())
                .body("object.newjson", equalTo("object"))
                .body("createdAt", notNullValue())
                .body("updatedAt", notNullValue()).when()
                .get(testDocument.getUuid().toString()).andReturn();
        LOGGER.debug("body for put response: " + response.getBody().prettyPrint());
        //cleanup the random uuid'ed doc
        f.deleteDocument(testDocument);
    }

    /**
     * Tests that the DELETE
     * /databases/{databases}/tables/{table}/documents/{document} endpoint
     * properly deletes a document.
     */
    @Test
    public void deleteDocumentTest()
    {
        Document testDocument = Fixtures.createTestDocument();
        f.insertDocument(testDocument);
        //act
        given().expect().statusCode(204)
                .when().delete(testDocument.getUuid().toString());
        //check
        expect().statusCode(404).when()
                .get(testDocument.getUuid().toString());
    }

    /**
     * Tests that the PUT /{databases}/{table}/{document} endpoint properly
     * updates a document. When there are fields that were previously indexed,
     * but are no longer, and fields that need to be updated.
     */
    @Test
    public void putDocumentWithAllDataTypesIndexedFieldTest()
    {
        //create the index first so we are sure it will get parsed
        f.insertIndex(Fixtures.createTestIndexAllFieldTypes());

        Document testDocument = Fixtures.createTestDocument3();
        f.insertDocument(testDocument);
        String newObject = "{\"thisisastring\": \"object\", \"thisisanint\": \"6\", \"thisisadouble\":\"7.777\"}";
        //act
        given().body(newObject).expect().statusCode(204)
                .when().put(testDocument.getUuid().toString());

        //check
        Response response = expect().statusCode(200)
                .body("id", equalTo(testDocument.getUuid().toString()))
                .body("object", notNullValue())
                .body("object.thisisastring", equalTo("object"))
                .body("object.thisisanint", equalTo("6"))
                .body("object.thisisadouble", equalTo("7.777"))
                .body("createdAt", notNullValue())
                .body("updatedAt", notNullValue()).when()
                .get(testDocument.getUuid().toString()).andReturn();
        LOGGER.debug("body for put response: " + response.getBody().prettyPrint());
        //cleanup the random uuid'ed doc
        f.deleteDocument(testDocument);
    }

    /**
     * Tests that the PUT /{databases}/{table}/{document} endpoint properly
     * updates a document. When there is an update, but no fields change.
     */
    @Test
    public void putDocumentWithAllDataTypesIndexedFieldNoChangeTest()
    {
        //create the index first so we are sure it will get parsed
        f.insertIndex(Fixtures.createTestIndexAllFieldTypes());

        Document testDocument = Fixtures.createTestDocument3();
        f.insertDocument(testDocument);
        String newObject = testDocument.getObjectAsString();
        //act
        given().body(newObject).expect().statusCode(204)
                .when().put(testDocument.getUuid().toString());

        //check
        Response response = expect().statusCode(200)
                .body("id", equalTo(testDocument.getUuid().toString()))
                .body("object", notNullValue())
                .body("object.thisisastring", equalTo("hello"))
                .body("object.thisisanint", equalTo("5"))
                .body("object.thisisadouble", equalTo("5.555"))
                .body("createdAt", notNullValue())
                .body("updatedAt", notNullValue()).when()
                .get(testDocument.getUuid().toString()).andReturn();
        LOGGER.debug("body for put response: " + response.getBody().prettyPrint());
        //cleanup the random uuid'ed doc
        f.deleteDocument(testDocument);
    }

    /**
     * Tests that the DELETE /{databases}/{table}/{document} endpoint properly
     * deletes a document.
     */
    @Test
    public void deleteDocumentWithIndexesTest()
    {
        //create the index first so we are sure it will get parsed
        f.insertIndex(Fixtures.createTestIndexAllFieldTypes());

        Document testDocument = Fixtures.createTestDocument3();
        f.insertDocument(testDocument);
        //act
        given().expect().statusCode(204)
                .when().delete(testDocument.getUuid().toString());
        //check
        expect().statusCode(404).when()
                .get(testDocument.getUuid().toString());
    }
}
