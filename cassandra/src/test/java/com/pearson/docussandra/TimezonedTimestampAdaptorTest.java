package com.pearson.docussandra;

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
public class TimezonedTimestampAdaptorTest {

  public TimezonedTimestampAdaptorTest() {}

  @BeforeClass
  public static void setUpClass() {}

  @AfterClass
  public static void tearDownClass() {}

  @Before
  public void setUp() {}

  @After
  public void tearDown() {}

  /**
   * Test of getTimestampFormats method, of class TimezonedTimestampAdaptor.
   */
  @Test
  public void testGetTimestampFormats() {
    System.out.println("getTimestampFormats");
    TimezonedTimestampAdaptor instance = new TimezonedTimestampAdaptor();
    String[] result = instance.getTimestampFormats();
    assertNotNull(result);
    assertEquals(17, result.length);
    for (String format : result) {
      assertNotNull(format);// make sure each one was populated
      assertTrue(format.length() > 1);// make sure each string is actually there
    }
  }

}
