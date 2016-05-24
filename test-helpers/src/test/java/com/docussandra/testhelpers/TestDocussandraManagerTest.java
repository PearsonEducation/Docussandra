package com.docussandra.testhelpers;

import com.jayway.restassured.RestAssured;
import static com.jayway.restassured.RestAssured.expect;
import static org.hamcrest.Matchers.equalTo;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * Test for the DocussandraManager. This test can not run in the same JVM as a
 * RestExpressManager instance, so it must be run ad-hoc.
 *
 * @author Jeffrey DeYoung
 */
public class TestDocussandraManagerTest
{

    public TestDocussandraManagerTest()
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
