package com.pearson.docussandra.abstracttests;

import com.jayway.restassured.RestAssured;
import static com.jayway.restassured.RestAssured.expect;
import static com.jayway.restassured.RestAssured.given;
import com.jayway.restassured.response.ResponseOptions;
import com.pearson.docussandra.cache.CacheFactory;
import com.pearson.docussandra.domain.objects.Database;
import com.pearson.docussandra.domain.objects.Document;
import com.pearson.docussandra.domain.objects.Index;
import com.pearson.docussandra.domain.objects.Table;
import com.pearson.docussandra.persistence.impl.DocumentRepositoryImpl;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.pearson.docussandra.testhelper.Fixtures;
import java.util.List;
import org.apache.commons.lang3.time.StopWatch;
import static org.hamcrest.Matchers.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for the IndexController. Located in the main test code so it can be
 * implemented outside of this project if needed.
 *
 * @author https://github.com/JeffreyDeYoung
 */
public abstract class AbstractIndexControllerTest
{

    private static final Logger logger = LoggerFactory.getLogger(AbstractIndexControllerTest.class);
    private static final String BASE_URI = "http://localhost";
    private static final int PORT = 19080;
    protected static Fixtures f;
    protected JSONParser parser = new JSONParser();

    public AbstractIndexControllerTest(Fixtures f) throws Exception
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
    }

    @Before
    public void beforeTest()
    {
        f.clearTestTables();
        CacheFactory.clearAllCaches();
        Database testDb = Fixtures.createTestDatabase();
        f.insertDatabase(testDb);
        Table testTable = Fixtures.createTestTable();
        f.insertTable(testTable);
        RestAssured.basePath = "/databases/" + testDb.getName() + "/tables/" + testTable.getName() + "/indexes";
    }


    /**
     * Cleanup that is performed after each test is executed.
     */
    @After
    public void afterTest()
    {
        f.clearTestTables();
    }

    /**
     * Tests that the GET /databases/{databases}/tables/{table}/indexes/{index}
     * properly retrieves an existing index.
     */
    @Test
    public void getIndexTest()
    {
        Index testIndex = Fixtures.createTestIndexOneField();
        f.insertIndex(testIndex);
        ResponseOptions result = expect().statusCode(200)
                .body("name", equalTo(testIndex.getName()))
                .body("fields", notNullValue())
                .body("createdAt", notNullValue())
                .body("updatedAt", notNullValue()).when()
                .get("/" + testIndex.getName()).andReturn();

        logger.info(result.body().prettyPrint());
    }

    /**
     * Tests that the POST /{databases}/{setTable}/indexes/ endpoint properly
     * creates a index.
     */
    @Test
    public void postIndexTest() throws InterruptedException
    {
        Index testIndex = Fixtures.createTestIndexOneField();
        String indexStr = "{" + "\"fields\" : [\"" + testIndex.getFieldsValues().get(0)
                + "\"], \"name\" : \"" + testIndex.getName() + "\"}";

        //act
        given().body(indexStr).expect().statusCode(201)
                .body("index.name", equalTo(testIndex.getName()))
                .body("index.fields", notNullValue())
                .body("index.createdAt", notNullValue())
                .body("index.updatedAt", notNullValue())
                .body("index.active", equalTo(false))
                .body("id", notNullValue())
                .body("dateStarted", notNullValue())
                .body("statusLastUpdatedAt", notNullValue())
                .body("totalRecords", equalTo(0))
                .body("recordsCompleted", equalTo(0))
                .when().post("/" + testIndex.getName());

        Thread.sleep(100);//sleep for a hair to let the indexing complete

        //check self (index endpoint)
        expect().statusCode(200)
                .body("name", equalTo(testIndex.getName()))
                .body("fields", notNullValue())
                .body("createdAt", notNullValue())
                .body("updatedAt", notNullValue())
                .body("active", equalTo(true))
                .get("/" + testIndex.getName());
    }

    /**
     * Tests that the POST /{databases}/{setTable}/indexes/ endpoint properly
     * creates a index.
     */
    @Test
    public void postIndexAllFieldsTest() throws InterruptedException
    {
        Index testIndex = Fixtures.createTestIndexAllFieldTypes();
        String indexStr = Fixtures.generateIndexCreationStringWithFields(testIndex);

        //act
        given().body(indexStr).expect().statusCode(201)
                .body("index.name", equalTo(testIndex.getName()))
                .body("index.fields", notNullValue())
                .body("index.fields[0].field", equalTo(testIndex.getFields().get(0).getField()))
                .body("index.fields[0].type", equalTo(testIndex.getFields().get(0).getType().toString()))
                .body("index.fields[1].field", equalTo(testIndex.getFields().get(1).getField()))
                .body("index.fields[1].type", equalTo(testIndex.getFields().get(1).getType().toString()))
                .body("index.fields[2].field", equalTo(testIndex.getFields().get(2).getField()))
                .body("index.fields[2].type", equalTo(testIndex.getFields().get(2).getType().toString()))
                .body("index.fields[3].field", equalTo(testIndex.getFields().get(3).getField()))
                .body("index.fields[3].type", equalTo(testIndex.getFields().get(3).getType().toString()))
                .body("index.fields[4].field", equalTo(testIndex.getFields().get(4).getField()))
                .body("index.fields[4].type", equalTo(testIndex.getFields().get(4).getType().toString()))
                .body("index.fields[5].field", equalTo(testIndex.getFields().get(5).getField()))
                .body("index.fields[5].type", equalTo(testIndex.getFields().get(5).getType().toString()))
                .body("index.fields[6].field", equalTo(testIndex.getFields().get(6).getField()))
                .body("index.fields[6].type", equalTo(testIndex.getFields().get(6).getType().toString()))
                .body("index.createdAt", notNullValue())
                .body("index.updatedAt", notNullValue())
                .body("index.active", equalTo(false))
                .body("id", notNullValue())
                .body("dateStarted", notNullValue())
                .body("statusLastUpdatedAt", notNullValue())
                .body("totalRecords", equalTo(0))
                .body("recordsCompleted", equalTo(0))
                .when().log().ifValidationFails().post("/" + testIndex.getName());

        Thread.sleep(100);//sleep for a hair to let the indexing complete

        //check self (index endpoint)
        expect().statusCode(200)
                .body("name", equalTo(testIndex.getName()))
                .body("fields", notNullValue())
                .body("fields[0].field", equalTo(testIndex.getFields().get(0).getField()))
                .body("fields[0].type", equalTo(testIndex.getFields().get(0).getType().toString()))
                .body("fields[1].field", equalTo(testIndex.getFields().get(1).getField()))
                .body("fields[1].type", equalTo(testIndex.getFields().get(1).getType().toString()))
                .body("fields[2].field", equalTo(testIndex.getFields().get(2).getField()))
                .body("fields[2].type", equalTo(testIndex.getFields().get(2).getType().toString()))
                .body("fields[3].field", equalTo(testIndex.getFields().get(3).getField()))
                .body("fields[3].type", equalTo(testIndex.getFields().get(3).getType().toString()))
                .body("fields[4].field", equalTo(testIndex.getFields().get(4).getField()))
                .body("fields[4].type", equalTo(testIndex.getFields().get(4).getType().toString()))
                .body("fields[5].field", equalTo(testIndex.getFields().get(5).getField()))
                .body("fields[5].type", equalTo(testIndex.getFields().get(5).getType().toString()))
                .body("fields[6].field", equalTo(testIndex.getFields().get(6).getField()))
                .body("fields[6].type", equalTo(testIndex.getFields().get(6).getType().toString()))
                .body("createdAt", notNullValue())
                .body("updatedAt", notNullValue())
                .body("active", equalTo(true))
                .get("/" + testIndex.getName());
    }

    /**
     * Tests that the POST /{databases}/{setTable}/indexes/ endpoint properly
     * creates a index and that the
     * GET/{database}/{setTable}/index_status/{status_id} endpoint is working.
     */
    @Test
    public void postIndexAndCheckStatusTest() throws InterruptedException
    {
        Index testIndex = Fixtures.createTestIndexOneField();
        String indexStr = "{" + "\"fields\" : [\"" + testIndex.getFieldsValues().get(0)
                + "\"]," + "\"name\" : \"" + testIndex.getName() + "\"}";

        //act
        ResponseOptions response = given().body(indexStr).expect().statusCode(201)
                .body("index.name", equalTo(testIndex.getName()))
                .body("index.fields", notNullValue())
                .body("index.createdAt", notNullValue())
                .body("index.updatedAt", notNullValue())
                .body("index.active", equalTo(false))//should not yet be active
                .body("id", notNullValue())
                .body("dateStarted", notNullValue())
                .body("statusLastUpdatedAt", notNullValue())
                .body("eta", notNullValue())
                .body("percentComplete", notNullValue())
                .body("totalRecords", equalTo(0))
                .body("recordsCompleted", equalTo(0))
                .when().post("/" + testIndex.getName()).andReturn();

        Thread.sleep(100);//sleep for a hair to let the indexing complete

        String restAssuredBasePath = RestAssured.basePath;
        try
        {
            //get the uuid from the response
            String uuidString = response.getBody().jsonPath().get("id");
            RestAssured.basePath = "/databases/" + testIndex.getDatabaseName() + "/tables/" + testIndex.getTableName() + "/index_status/";
            ResponseOptions res = expect().statusCode(200)
                    .body("id", equalTo(uuidString))
                    .body("dateStarted", notNullValue())
                    .body("statusLastUpdatedAt", notNullValue())
                    .body("eta", notNullValue())
                    .body("percentComplete", notNullValue())
                    .body("index", notNullValue())
                    .body("index.active", equalTo(true))//should now be active
                    .body("totalRecords", notNullValue())
                    .body("recordsCompleted", notNullValue())
                    .when().get(uuidString).andReturn();//check status (index_status endpoint)
            logger.debug("Status Response: " + res.getBody().prettyPrint());
        } finally
        {
            RestAssured.basePath = restAssuredBasePath;
        }
    }

    /**
     * Tests that the POST /databases/{databases}/tables/{table}/indexes
     * endpoint properly creates a index and that the
     * GET/databases/{database}/tables/{table}/index_status/{status_id} endpoint
     * is working.
     */
    @Test
    public void createDataThenPostIndexAndCheckStatusTest() throws InterruptedException, Exception
    {
        String restAssuredBasePath = RestAssured.basePath;
        Database testDb = Fixtures.createTestWorldBankDatabase();
        Table testTable = Fixtures.createTestWorldBankTable();
        List<Document> docs = Fixtures.getBulkDocumentsByLine("/world_bank_short.txt", testTable);
        try
        {
            //data insert          
            f.insertDatabase(testDb);
            f.insertTable(testTable);
            f.insertDocuments(docs);//put in a ton of data directly into the db
            Index lastname = Fixtures.createTestBankIndexCountryName();
            String indexStr = "{" + "\"fields\" : [\"" + lastname.getFieldsValues().get(0)
                    + "\"]," + "\"name\" : \"" + lastname.getName() + "\"}";
            RestAssured.basePath = "/databases/" + lastname.getDatabaseName() + "/tables/" + lastname.getTableName() + "/indexes";
            //act -- create index
            ResponseOptions response = given().body(indexStr).expect().statusCode(201)
                    .body("index.name", equalTo(lastname.getName()))
                    .body("index.fields", notNullValue())
                    .body("index.createdAt", notNullValue())
                    .body("index.updatedAt", notNullValue())
                    .body("index.active", equalTo(false))//should not yet be active
                    .body("id", notNullValue())
                    .body("dateStarted", notNullValue())
                    .body("statusLastUpdatedAt", notNullValue())
                    .body("eta", notNullValue())
                    .body("percentComplete", notNullValue())
                    .body("totalRecords", equalTo(444))
                    .body("recordsCompleted", equalTo(0))
                    .when().post("/" + lastname.getName()).andReturn();

            //start a timer
            StopWatch sw = new StopWatch();
            sw.start();

            //check the status endpoint to make sure it got created
            //get the uuid from the response
            String uuidString = response.getBody().jsonPath().get("id");
            RestAssured.basePath = "/databases/" + lastname.getDatabaseName() + "/tables/" + lastname.getTableName() + "/index_status/";
            ResponseOptions res = expect().statusCode(200)
                    .body("id", equalTo(uuidString))
                    .body("dateStarted", notNullValue())
                    .body("statusLastUpdatedAt", notNullValue())
                    .body("eta", notNullValue())
                    .body("percentComplete", notNullValue())
                    .body("index", notNullValue())
                    .body("index.active", notNullValue())
                    .body("index.active", equalTo(false))//should not yet be active
                    .body("recordsCompleted", notNullValue())
                    .when().get(uuidString).andReturn();
            logger.debug("Status Response: " + res.getBody().prettyPrint());

            boolean active = false;
            while (!active)
            {
                //poll the status until it is active to make sure an index did in fact get created
                res = expect().statusCode(200)
                        .body("id", equalTo(uuidString))
                        .body("dateStarted", notNullValue())
                        .body("statusLastUpdatedAt", notNullValue())
                        .body("eta", notNullValue())
                        .body("percentComplete", notNullValue())
                        .body("index", notNullValue())
                        .body("index.active", notNullValue())
                        .body("recordsCompleted", notNullValue())
                        .when().get(uuidString).andReturn();
                logger.debug("Status Response: " + res.getBody().prettyPrint());
                active = res.getBody().jsonPath().get("index.active");
                if (active)
                {
                    sw.stop();
                    break;
                }
                logger.debug("Waiting for index to go active for: " + sw.getTime());
                if (sw.getTime() >= 60000)
                {
                    fail("Index took too long to create");
                }
                Thread.sleep(2000);
            }
            logger.info("It took: " + (sw.getTime() / 1000) + " seconds to create the index.");

        } finally
        {
            RestAssured.basePath = restAssuredBasePath;
            //clean up
            DocumentRepositoryImpl docrepo = new DocumentRepositoryImpl(f.getSession());
            for (Document d : docs)
            {
                try
                {
                    docrepo.delete(d);
                } catch (Exception e)
                {
                    ;//eh -- the doc probably never got created
                }
            }
        }
    }

    /**
     * Tests that the POST /databases/{databases}/tables/{table}/indexes
     * endpoint properly creates a index, setting errors to the index status
     * table and that the GET
     * /databases/{database}/tables/{table}/index_status/{status_id} endpoint is
     * working.
     */
    @Test
    public void createBadDataThenPostIndexAndCheckStatusTest() throws Exception
    {
        String restAssuredBasePath = RestAssured.basePath;
        Database testDb = Fixtures.createTestWorldBankDatabase();
        Table testTable = Fixtures.createTestWorldBankTable();
        List<Document> docs = Fixtures.getBulkDocumentsByLine("/world_bank_short.txt", testTable);
        //botch a doc
        Document badDoc = docs.get(0);
        //the year field is now text
        badDoc.setObjectAsString("{     \"_id\":{        \"$oid\":\"52b213b38594d8a2be17c780\"   },   \"approvalfy\":\"Nineteen Hundred and Ninety Nine\",   \"board_approval_month\":\"November\",   \"boardapprovaldate\":\"2013-11-12T00:00:00Z\",   \"borrower\":\"FEDERAL DEMOCRATIC REPUBLIC OF ETHIOPIA\",   \"closingdate\":\"2018-07-07T00:00:00Z\",  \"country_namecode\":\"Federal Democratic Republic of Ethiopia!$!ET\",   \"countrycode\":\"ET\",   \"countryname\":\"Federal Democratic Republic of Ethiopia\",  \"countryshortname\":\"Ethiopia\",   \"docty\":\"Project Information Document,Indigenous Peoples Plan,Project Information Document\",   \"envassesmentcategorycode\":\"C\",   \"grantamt\":0,   \"ibrdcommamt\":0,   \"id\":\"P129828\",   \"idacommamt\":130000000,   \"impagency\":\"MINISTRY OF EDUCATION\",   \"lendinginstr\":\"Investment Project Financing\",   \"lendinginstrtype\":\"IN\",   \"lendprojectcost\":550000000,   \"majorsector_percent\":[        {           \"Name\":\"Education\",         \"Percent\":46      },      {           \"Name\":\"Education\",         \"Percent\":26      },      {           \"Name\":\"Public Administration, Law, and Justice\",         \"Percent\":16      },      {           \"Name\":\"Education\",         \"Percent\":12      }   ],   \"mjsector_namecode\":[        {           \"name\":\"Education\",         \"code\":\"EX\"      },      {           \"name\":\"Education\",         \"code\":\"EX\"      },      {           \"name\":\"Public Administration, Law, and Justice\",         \"code\":\"BX\"      },      {           \"name\":\"Education\",         \"code\":\"EX\"      }   ],   \"mjtheme\":[        \"Human development\"   ],   \"mjtheme_namecode\":[        {           \"name\":\"Human development\",         \"code\":\"8\"      },      {           \"name\":\"\",         \"code\":\"11\"      }   ],   \"mjthemecode\":\"8,11\",   \"prodline\":\"PE\",   \"prodlinetext\":\"IBRD/IDA\",   \"productlinetype\":\"L\",   \"project_abstract\":{        \"cdata\":\"The development objective of the Second Phase of General Education Quality Improvement Project for Ethiopia is to improve learning conditions in primary and secondary schools and strengthen institutions at different levels of educational administration. The project has six components. The first component is curriculum, textbooks, assessment, examinations, and inspection. This component will support improvement of learning conditions in grades KG-12 by providing increased access to teaching and learning materials and through improvements to the curriculum by assessing the strengths and weaknesses of the current curriculum. This component has following four sub-components: (i) curriculum reform and implementation; (ii) teaching and learning materials; (iii) assessment and examinations; and (iv) inspection. The second component is teacher development program (TDP). This component will support improvements in learning conditions in both primary and secondary schools by advancing the quality of teaching in general education through: (a) enhancing the training of pre-service teachers in teacher education institutions; and (b) improving the quality of in-service teacher training. This component has following three sub-components: (i) pre-service teacher training; (ii) in-service teacher training; and (iii) licensing and relicensing of teachers and school leaders. The third component is school improvement plan. This component will support the strengthening of school planning in order to improve learning outcomes, and to partly fund the school improvement plans through school grants. It has following two sub-components: (i) school improvement plan; and (ii) school grants. The fourth component is management and capacity building, including education management information systems (EMIS). This component will support management and capacity building aspect of the project. This component has following three sub-components: (i) capacity building for education planning and management; (ii) capacity building for school planning and management; and (iii) EMIS. The fifth component is improving the quality of learning and teaching in secondary schools and universities through the use of information and communications technology (ICT). It has following five sub-components: (i) national policy and institution for ICT in general education; (ii) national ICT infrastructure improvement plan for general education; (iii) develop an integrated monitoring, evaluation, and learning system specifically for the ICT component; (iv) teacher professional development in the use of ICT; and (v) provision of limited number of e-Braille display readers with the possibility to scale up to all secondary education schools based on the successful implementation and usage of the readers. The sixth component is program coordination, monitoring and evaluation, and communication. It will support institutional strengthening by developing capacities in all aspects of program coordination, monitoring and evaluation; a new sub-component on communications will support information sharing for better management and accountability. It has following three sub-components: (i) program coordination; (ii) monitoring and evaluation (M and E); and (iii) communication.\"   },   \"project_name\":\"Ethiopia General Education Quality Improvement Project II\",   \"projectdocs\":[        {           \"DocTypeDesc\":\"Project Information Document (PID),  Vol.\",         \"DocType\":\"PID\",         \"EntityID\":\"090224b081e545fb_1_0\",         \"DocURL\":\"http://www-wds.worldbank.org/servlet/WDSServlet?pcont=details&eid=090224b081e545fb_1_0\",         \"DocDate\":\"28-AUG-2013\"      },      {           \"DocTypeDesc\":\"Indigenous Peoples Plan (IP),  Vol.1 of 1\",         \"DocType\":\"IP\",         \"EntityID\":\"000442464_20130920111729\",         \"DocURL\":\"http://www-wds.worldbank.org/servlet/WDSServlet?pcont=details&eid=000442464_20130920111729\",         \"DocDate\":\"01-JUL-2013\"      },      {           \"DocTypeDesc\":\"Project Information Document (PID),  Vol.\",         \"DocType\":\"PID\",         \"EntityID\":\"090224b0817b19e2_1_0\",         \"DocURL\":\"http://www-wds.worldbank.org/servlet/WDSServlet?pcont=details&eid=090224b0817b19e2_1_0\",         \"DocDate\":\"22-NOV-2012\"      }   ],   \"projectfinancialtype\":\"IDA\",   \"projectstatusdisplay\":\"Active\",   \"regionname\":\"Africa\",   \"sector\":[        {           \"Name\":\"Primary education\"      },      {           \"Name\":\"Secondary education\"      },      {           \"Name\":\"Public administration- Other social services\"      },      {           \"Name\":\"Tertiary education\"      }   ],   \"sector1\":{        \"Name\":\"Primary education\",      \"Percent\":46   },   \"sector2\":{        \"Name\":\"Secondary education\",      \"Percent\":26   },   \"sector3\":{        \"Name\":\"Public administration- Other social services\",      \"Percent\":16   },   \"sector4\":{        \"Name\":\"Tertiary education\",      \"Percent\":12   },   \"sector_namecode\":[        {           \"name\":\"Primary education\",         \"code\":\"EP\"      },      {           \"name\":\"Secondary education\",         \"code\":\"ES\"      },      {           \"name\":\"Public administration- Other social services\",         \"code\":\"BS\"      },      {           \"name\":\"Tertiary education\",         \"code\":\"ET\"      }   ],   \"sectorcode\":\"ET,BS,ES,EP\",   \"source\":\"IBRD\",   \"status\":\"Active\",   \"supplementprojectflg\":\"N\",   \"theme1\":{        \"Name\":\"Education for all\",      \"Percent\":100   },   \"theme_namecode\":[        {           \"name\":\"Education for all\",         \"code\":\"65\"      }   ],   \"themecode\":\"65\",   \"totalamt\":130000000,   \"totalcommamt\":130000000,   \"url\":\"http://www.worldbank.org/projects/P129828/ethiopia-general-education-quality-improvement-project-ii?lang=en\"}");
        docs.set(0, badDoc);
        try
        {
            //data insert          
            f.insertDatabase(testDb);
            f.insertTable(testTable);
            f.insertDocuments(docs);//put in a ton of data directly into the db
            Index approvedyear = Fixtures.createTestBankIndexLoanYear();

            String indexString = Fixtures.generateIndexCreationStringWithFields(approvedyear);
            RestAssured.basePath = "/databases/" + approvedyear.getDatabaseName() + "/tables/" + approvedyear.getTableName() + "/indexes";
            //act -- create index
            ResponseOptions response = given().body(indexString).expect().statusCode(201)
                    .body("index.name", equalTo(approvedyear.getName()))
                    .body("index.fields", notNullValue())
                    .body("index.createdAt", notNullValue())
                    .body("index.updatedAt", notNullValue())
                    .body("index.active", equalTo(false))//should not yet be active
                    .body("id", notNullValue())
                    .body("dateStarted", notNullValue())
                    .body("statusLastUpdatedAt", notNullValue())
                    .body("eta", notNullValue())
                    .body("percentComplete", notNullValue())
                    .body("totalRecords", equalTo(444))
                    .body("recordsCompleted", equalTo(0))
                    .when().post("/" + approvedyear.getName()).andReturn();

            //start a timer
            StopWatch sw = new StopWatch();
            sw.start();

            //check the status endpoint to make sure it got created
            //get the uuid from the response
            String uuidString = response.getBody().jsonPath().get("id");
            RestAssured.basePath = "/databases/" + approvedyear.getDatabaseName() + "/tables/" + approvedyear.getTableName() + "/index_status/";
            ResponseOptions res = expect().statusCode(200)
                    .body("id", equalTo(uuidString))
                    .body("dateStarted", notNullValue())
                    .body("statusLastUpdatedAt", notNullValue())
                    .body("eta", notNullValue())
                    .body("percentComplete", notNullValue())
                    .body("index", notNullValue())
                    .body("index.active", notNullValue())
                    .body("index.active", equalTo(false))//should not yet be active -- we are using some smaller test data now, so it may have actually completed -- if this line errors, up the size of the dataset?
                    .body("recordsCompleted", notNullValue())
                    .when().get(uuidString).andReturn();
            logger.debug("Status Response: " + res.getBody().prettyPrint());

            boolean active = false;
            while (!active)
            {
                //poll the status until it is active to make sure an index did in fact get created
                res = expect().statusCode(200)
                        .body("id", equalTo(uuidString))
                        .body("dateStarted", notNullValue())
                        .body("statusLastUpdatedAt", notNullValue())
                        .body("eta", notNullValue())
                        .body("percentComplete", notNullValue())
                        .body("index", notNullValue())
                        .body("index.active", notNullValue())
                        .body("recordsCompleted", notNullValue())
                        .when().get(uuidString).andReturn();
                logger.debug("Status Response: " + res.getBody().prettyPrint());
                active = res.getBody().jsonPath().get("index.active");
                if (active)
                {
                    sw.stop();
                    break;
                }
                logger.debug("Waiting for index to go active for: " + sw.getTime());
                if (sw.getTime() >= 60000)
                {
                    fail("Index took too long to create: " + sw.getTime());
                }
                Thread.sleep(2000);
            }
            logger.info("It took: " + (sw.getTime() / 1000) + " seconds to create the index.");

            //once it is active, lets check and make sure we have an error in the status table for our bad doc -- side note: there are tons more errors than our intentional one in this dataset
            res = expect().statusCode(200)
                    .body("id", equalTo(uuidString))
                    .body("dateStarted", notNullValue())
                    .body("statusLastUpdatedAt", notNullValue())
                    .body("eta", notNullValue())
                    .body("percentComplete", notNullValue())
                    .body("index", notNullValue())
                    .body("index.active", notNullValue())
                    .body("recordsCompleted", notNullValue())
                    .when().get(uuidString).andReturn();
            logger.debug("Status Response: " + res.getBody().prettyPrint());
            Assert.assertTrue(res.getBody().prettyPrint().contains("error"));
            Assert.assertTrue(res.getBody().prettyPrint().contains("52b213b38594d8a2be17c780"));//make sure our (internal) doc id is displayed in the error

        } finally
        {
            RestAssured.basePath = restAssuredBasePath;
            //clean up
            DocumentRepositoryImpl docrepo = new DocumentRepositoryImpl(f.getSession());
            for (Document d : docs)
            {
                try
                {
                    docrepo.delete(d);
                } catch (Exception e)
                {
                    ;//eh -- the doc probably never got created
                }
            }
        }
    }

    /**
     * Tests that the POST /databases/{databases}/tables/{table}/indexes
     * endpoint properly creates a index and that the
     * GET/databases/{database}/tables/{table}/index_status/{index} endpoint is
     * working.
     */
    @Test
    public void postIndexAndCheckStatusAllTest() throws Exception
    {
        String restAssuredBasePath = RestAssured.basePath;
        try
        {
            //data insert
            Database testDb = Fixtures.createTestWorldBankDatabase();
            Table testTable = Fixtures.createTestWorldBankTable();
            f.insertDatabase(testDb);
            f.insertTable(testTable);
            List<Document> docs = Fixtures.getBulkDocumentsByLine("/world_bank_short.txt", testTable);
            f.insertDocuments(docs);//put in a ton of data directly into the db
            Index lastname = Fixtures.createTestBankIndexCountryName();
            String indexString = "{" + "\"fields\" : [\"" + lastname.getFieldsValues().get(0)
                    + "\"]," + "\"name\" : \"" + lastname.getName() + "\"}";
            RestAssured.basePath = "/databases/" + lastname.getDatabaseName() + "/tables/" + lastname.getTableName() + "/indexes";
            //act -- create index
            given().body(indexString).expect().statusCode(201)
                    .body("index.name", equalTo(lastname.getName()))
                    .body("index.fields", notNullValue())
                    .body("index.createdAt", notNullValue())
                    .body("index.updatedAt", notNullValue())
                    .body("index.active", equalTo(false))//should not yet be active
                    .body("id", notNullValue())
                    .body("dateStarted", notNullValue())
                    .body("statusLastUpdatedAt", notNullValue())
                    .body("eta", notNullValue())
                    .body("percentComplete", notNullValue())
                    .body("totalRecords", equalTo(444))
                    .body("recordsCompleted", equalTo(0))
                    .when().log().ifValidationFails().post("/" + lastname.getName());

            //start a timer
            StopWatch sw = new StopWatch();
            sw.start();

            RestAssured.basePath = "admin/index_status";

            //check to make sure it shows as present at least once
            ResponseOptions res = expect().statusCode(200)
                    .body("_embedded.indexcreatedevents[0].id", notNullValue())
                    .body("_embedded.indexcreatedevents[0].dateStarted", notNullValue())
                    .body("_embedded.indexcreatedevents[0].statusLastUpdatedAt", notNullValue())
                    .body("_embedded.indexcreatedevents[0].eta", notNullValue())
                    .body("_embedded.indexcreatedevents[0].index", notNullValue())
                    .body("_embedded.indexcreatedevents[0].index.active", equalTo(false))//should not yet be active
                    .body("_embedded.indexcreatedevents[0].totalRecords", notNullValue())
                    .body("_embedded.indexcreatedevents[0].recordsCompleted", notNullValue())
                    .body("_embedded.indexcreatedevents[0].percentComplete", notNullValue())
                    .when().log().ifValidationFails().get("").andReturn();
            logger.debug("Status Response: " + res.getBody().prettyPrint());
            //wait for it to dissapear (meaning it's gone active)
            boolean active = false;
            while (!active)
            {
                res = expect().statusCode(200).when().get("").andReturn();
                String body = res.getBody().prettyPrint();
                logger.debug("Status Response: " + body);

                JSONObject bodyObject = (JSONObject) parser.parse(body);
                JSONObject embedded = (JSONObject) bodyObject.get("_embedded");
                JSONArray resultSet = (JSONArray) embedded.get("indexcreatedevents");
                if (resultSet.isEmpty())
                {
                    active = true;
                    sw.stop();
                    break;
                }
                logger.debug("Waiting for index to go active for: " + sw.getTime());
                if (sw.getTime() >= 60000)
                {
                    fail("Index took too long to create");
                }
                Thread.sleep(2000);
            }
            logger.info("It took: " + (sw.getTime() / 1000) + " seconds to create the index.");

        } finally
        {
            RestAssured.basePath = restAssuredBasePath;
        }
    }

    /**
     * Tests that the DELETE
     * /databases/{databases}/tables/{table}/indexes/{index} endpoint properly
     * deletes a index.
     */
    @Test
    public void deleteIndexTest()
    {
        Index testIndex = Fixtures.createTestIndexOneField();
        f.insertIndex(testIndex);
        //act
        given().expect().statusCode(204)
                .when().delete(testIndex.getName());
        //check
        expect().statusCode(404).when()
                .get(testIndex.getName());
    }

}
