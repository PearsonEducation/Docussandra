package com.pearson.docussandra;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Unit tests for the main class
 *
 * @author https://github.com/JeffreyDeYoung
 */
public class MainUnitTest
{

    public MainUnitTest()
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
     * Test of calculatePort method, of class Main.
     */
    @Test
    public void testCalculatePort()
    {
        System.out.println("calculatePort");
        String projectVersion = "1.0-SNAPSHOT";
        int expResult = 41001;
        int result = Main.calculatePort(projectVersion);
        assertEquals(expResult, result);
        projectVersion = "1.0";
        expResult = 41000;
        result = Main.calculatePort(projectVersion);
        assertEquals(expResult, result);
        projectVersion = "1.1-SNAPSHOT";
        expResult = 41101;
        result = Main.calculatePort(projectVersion);
        assertEquals(expResult, result);
        projectVersion = "2.1-SNAPSHOT";
        expResult = 42101;
        result = Main.calculatePort(projectVersion);
        assertEquals(expResult, result);
        projectVersion = "2.1";
        expResult = 42100;
        result = Main.calculatePort(projectVersion);
        assertEquals(expResult, result);
        projectVersion = "20.1-SNAPSHOT";
        expResult = 42011;
        result = Main.calculatePort(projectVersion);
        assertEquals(expResult, result);
        projectVersion = "21.1";
        expResult = 42110;
        result = Main.calculatePort(projectVersion);
        assertEquals(expResult, result);
        projectVersion = "21.10-SNAPSHOT";
        expResult = 41101;
        result = Main.calculatePort(projectVersion);
        assertEquals(expResult, result);
        projectVersion = "21.10";
        expResult = 41100;
        result = Main.calculatePort(projectVersion);
        assertEquals(expResult, result);
        projectVersion = "21.101";
        expResult = 41010;
        result = Main.calculatePort(projectVersion);
        assertEquals(expResult, result);
        projectVersion = "21.101-SNAPSHOT";
        expResult = 41011;
        result = Main.calculatePort(projectVersion);
        assertEquals(expResult, result);

        projectVersion = "11.11";
        expResult = 41110;
        result = Main.calculatePort(projectVersion);
        assertEquals(expResult, result);

        projectVersion = "11.11-SNAPSHOT";
        expResult = 41111;
        result = Main.calculatePort(projectVersion);
        assertEquals(expResult, result);
    }

}
