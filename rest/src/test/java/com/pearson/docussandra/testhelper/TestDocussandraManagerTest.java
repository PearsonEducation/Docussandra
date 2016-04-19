package com.pearson.docussandra.testhelper;

import com.jayway.restassured.RestAssured;
import static com.jayway.restassured.RestAssured.expect;
import static org.hamcrest.Matchers.equalTo;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Ignore;

/**
 *
 * Test for the DocussandraManager. This test can not run in the same JVM as a
 * RestExpressManager instance, so it must be run ad-hoc.
 *
 * @author Jeffrey DeYoung
 */
@Ignore // this test can not run in the same JVM as a RestExpressManager instance, so set as ignored until we want to run it explicitly.
public class TestDocussandraManagerTest
{

    public TestDocussandraManagerTest()
    {
    }

    @BeforeClass
    public static void setUpClass()
    {
    }

    @AfterClass
    public static void tearDownClass()
    {
    }

    @Before
    public void setUp()
    {
    }

    @After
    public void tearDown()
    {
    }

    /**
     * Test of getManager method, of class TestDocussandraManager.
     */
    @Test
    public void testGetManager()
    {
        System.out.println("getManager");
        TestDocussandraManager result = TestDocussandraManager.getManager();
        assertNotNull(result);
        TestDocussandraManager result2 = TestDocussandraManager.getManager();
        assertNotNull(result2);
        assertEquals(result, result2);
    }

    /**
     * Test of ensureTestDocussandraRunningWithMockCassandra method, of class
     * TestDocussandraManager.
     */
    @Test
    public void testEnsureTestDocussandraRunningWithMockCassandra() throws Exception
    {
        System.out.println("ensureTestDocussandraRunningWithMockCassandra");
        //String keyspace = "docussandra";
        TestDocussandraManager instance = TestDocussandraManager.getManager();
        instance.ensureTestDocussandraRunning(true);
        RestAssured.baseURI = "http://localhost";
        RestAssured.port = 19080;
        RestAssured.basePath = "/";
        expect().statusCode(200)
                .body("isHealthy", equalTo(true))
                .get("/admin/health");
    }

}
