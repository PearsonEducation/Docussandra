package com.pearson.docussandra.preprocessor;

import com.pearson.docussandra.plugins.PluginHolder;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.restexpress.Request;
import org.restexpress.Response;
import org.restexpress.route.RouteResolver;

/**
 *
 * @author https://github.com/JeffreyDeYoung
 */
public class SecurityPluginPreproccessorTest
{

    /**
     * Plugin holder for this test. Global and static for better reuse.
     */
    private static PluginHolder ph;

    public SecurityPluginPreproccessorTest()
    {
    }

    @BeforeClass
    public static void setUpClass() throws Exception
    {
        File pluginJar = new File("./src/test/resources", "plugin-test-notify.jar");
        File[] jars = new File[1];
        jars[0] = pluginJar;
        try
        {
            ph = PluginHolder.build(jars);
        } catch (IllegalStateException e)
        {
            ph = PluginHolder.getInstance();
        }
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
     * Test of process method, of class SecurityPluginPreproccessor.
     */
    @Test
    public void testProcess()
    {
        System.out.println("process");
        Request request = createTestRequest();
        Response response = null;
        SecurityPluginPreproccessor instance = new SecurityPluginPreproccessor();
        instance.process(request, response);
        //check to see that our test plugins actually ran like we wanted them to (these properties are set in the test plugin)
        assertEquals("test4", System.getProperty("com.pearson.docussandra.testHeader_2"));
        assertEquals("test4", System.getProperty("com.pearson.docussandra.testHeader_2.secondtest"));

    }

    /**
     * Test of extractHeadersFromRequest method, of class
     * SecurityPluginPreproccessor.
     */
    @Test
    public void testExtractHeadersFromRequest()
    {
        System.out.println("extractHeadersFromRequest");
        Request request = createTestRequest();
        HashMap<String, List<String>> result = SecurityPluginPreproccessor.extractHeadersFromRequest(request);
        assertNotNull(result);
        assertTrue(result.containsKey("testHeader_1"));
        assertTrue(result.containsKey("testHeader_2"));
        assertNotNull(result.get("testHeader_1"));
        assertNotNull(result.get("testHeader_2"));
        assertNotNull(result.get("testHeader_1").get(0));
        assertNotNull(result.get("testHeader_1").get(1));
        assertNotNull(result.get("testHeader_1").get(2));
        assertNotNull(result.get("testHeader_2").get(0));
        assertEquals("test1", result.get("testHeader_1").get(0));
        assertEquals("test2", result.get("testHeader_1").get(1));
        assertEquals("test3", result.get("testHeader_1").get(2));
    }

    private Request createTestRequest()
    {
        Request r = new Request(new DefaultFullHttpRequest(HttpVersion.HTTP_1_0, HttpMethod.GET, "/test/path/here"), new RouteResolver(null));
        r.addHeader("testHeader_1", "test1");
        r.addHeader("testHeader_1", "test2");
        r.addHeader("testHeader_1", "test3");
        r.addHeader("testHeader_2", "test4");
        return r;
    }

}
