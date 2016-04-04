
package com.pearson.docussandra.persistence.helper;

import com.pearson.docussandra.persistence.helper.PreparedStatementFactory;
import com.datastax.driver.core.PreparedStatement;
import com.pearson.docussandra.testhelper.Fixtures;
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
public class PreparedStatementFactoryTest
{

    private Fixtures f;

    public PreparedStatementFactoryTest() throws Exception
    {
        f = Fixtures.getInstance();
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
     * Test of getPreparedStatement method, of class PreparedStatementFactory.
     */
    @Test
    public void testGetPreparedStatement()
    {
        System.out.println("getPreparedStatement");
        String query = "select * from docussandra.sys_db";
        PreparedStatement result = PreparedStatementFactory.getPreparedStatement(query, f.getSession());
        assertNotNull(result);
        result = PreparedStatementFactory.getPreparedStatement(query, f.getSession());
        assertNotNull(result);
    }

}
