package com.pearson.docussandra.plugins;

import com.pearson.docussandra.plugininterfaces.NotifierPlugin;
import com.pearson.docussandra.plugininterfaces.Plugin;
import java.io.File;
import java.util.ArrayList;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author https://github.com/JeffreyDeYoung
 */
public class PluginUtilsTest
{
    
    public PluginUtilsTest()
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
     * Tests the getPluginJars() method. We can't really have any presupposed
     * data in the users home dir, so this really isn't much of a test. We will
     * just check to make sure this doesn't error when called and doesn't return
     * null.
     */
    @Test
    public void testGetPluginJars()
    {
        File[] res = PluginUtils.getPluginJars();
        assertNotNull(res);
    }

    /**
     * Test of getPluginsFromExternalJar method, of class PluginUtils.
     */
    @Test
    public void testGetPluginsFromExternalJar() throws Exception
    {
        System.out.println("getPluginsFromExternalJar");
        File pluginJar = new File("./src/test/resources", "plugin-test-notify.jar");
        System.out.print("Looking for plugin jar at: " + pluginJar.getAbsolutePath());
        assertTrue(pluginJar.exists());//make sure we can actually read from it
        Class moduleType = NotifierPlugin.class;
        ArrayList<Class<? extends Plugin>> result = PluginUtils.getPluginsFromExternalJar(pluginJar, moduleType);
        assertNotNull(result);
        assertEquals(2, result.size());
        assertNotNull(result.get(0));
        assertTrue(result.get(0).getCanonicalName().startsWith("com.patriotcoder.testdocussandraplugin.TestNotifierPlugin"));
    }
    
}
