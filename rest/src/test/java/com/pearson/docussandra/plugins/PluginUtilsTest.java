package com.pearson.docussandra.plugins;

import com.pearson.docussandra.plugininterfaces.NotifierPluginInterface;
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
        File deputyJar = new File(".", "plugin-test-notify.jar");
        Class moduleType = NotifierPluginInterface.class;
        ArrayList<Class<? extends Plugin>> result = PluginUtils.getPluginsFromExternalJar(deputyJar, moduleType);
        assertNotNull(result);
        //TODO: This is not done!
    }
    
}
