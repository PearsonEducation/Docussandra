package com.pearson.docussandra.docussandra.domain;

import com.pearson.docussandra.domain.objects.WhereClause;
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
public class WhereClauseTest
{

    public WhereClauseTest()
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

    @Test
    public void testIt0()
    {
        WhereClause wc = new WhereClause("blah = 'nonsense'");
        assertEquals("blah = ?", wc.getBoundStatementSyntax());
        assertEquals("blah", wc.getFields().get(0));
        assertEquals("nonsense", wc.getValues().get(0));
    }

    @Test
    public void testIt1()
    {
        WhereClause wc = new WhereClause("blah = 'blah' AND foo = 'bar'");
        assertEquals("blah = ? AND foo = ?", wc.getBoundStatementSyntax());
        assertEquals("blah", wc.getFields().get(0));
        assertEquals("foo", wc.getFields().get(1));
        assertEquals("blah", wc.getValues().get(0));
        assertEquals("bar", wc.getValues().get(1));
    }

    @Test
    public void testIt2()
    {
        WhereClause wc = new WhereClause("blah = 'blah' AND foo = 'bar' ORDER BY foo");
        assertEquals("blah = ? AND foo = ? ORDER BY foo", wc.getBoundStatementSyntax());
        assertEquals("blah", wc.getFields().get(0));
        assertEquals("foo", wc.getFields().get(1));
        assertEquals("blah", wc.getValues().get(0));
        assertEquals("bar", wc.getValues().get(1));
    }

    @Test
    public void testIt3()
    {
        WhereClause wc = new WhereClause("blah = 'blah' AND foo < 'bar' ORDER BY foo");
        assertEquals("blah = ? AND foo < ? ORDER BY foo", wc.getBoundStatementSyntax());
        assertEquals("blah", wc.getFields().get(0));
        assertEquals("foo", wc.getFields().get(1));
        assertEquals("blah", wc.getValues().get(0));
        assertEquals("bar", wc.getValues().get(1));
    }

    @Test
    public void testIt4()
    {
        WhereClause wc = new WhereClause("blah = 'non sense'");
        assertEquals("blah = ?", wc.getBoundStatementSyntax());
        assertEquals("blah", wc.getFields().get(0));
        assertEquals("non sense", wc.getValues().get(0));
    }

    @Test
    public void testIt5()
    {
        WhereClause wc = new WhereClause("blah = 'blah blah' AND foo = 'bar bar bar'");
        assertEquals("blah = ? AND foo = ?", wc.getBoundStatementSyntax());
        assertEquals("blah", wc.getFields().get(0));
        assertEquals("foo", wc.getFields().get(1));
        assertEquals("blah blah", wc.getValues().get(0));
        assertEquals("bar bar bar", wc.getValues().get(1));
    }
}
