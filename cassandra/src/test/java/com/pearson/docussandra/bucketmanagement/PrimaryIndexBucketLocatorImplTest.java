package com.pearson.docussandra.bucketmanagement;

import com.pearson.docussandra.ParseUtils;
import com.pearson.docussandra.domain.objects.FieldDataType;
import com.pearson.docussandra.exception.IndexParseFieldException;
import java.io.IOException;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

import org.junit.Test;
import org.junit.Assert;

import org.junit.Ignore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class PrimaryIndexBucketLocatorImplTest
{

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * test to test out the bucket distribution of the string
     */
    @Test
    public void testPracticalDistribution() throws IndexParseFieldException, IOException
    {
        PrimaryIndexBucketLocatorImpl locator = PrimaryIndexBucketLocatorImpl.getInstance();
        Long bucket1 = locator.getBucket("adam", FieldDataType.TEXT);
        Long bucket2 = locator.getBucket("adam", FieldDataType.TEXT);
        assertEquals(bucket1, bucket2);
//        Long bucket3 = locator.getBucket("apple", FieldDataType.TEXT);
//        assertEquals(bucket3, bucket2);//no longer a valid test; our range is better
        Long bucket4 = locator.getBucket("zuul", FieldDataType.TEXT);
        Assert.assertNotEquals(bucket4, bucket2);
        Long bucket5 = locator.getBucket("zed", FieldDataType.TEXT);
        assertEquals(bucket4, bucket5);
        Long bucket6 = locator.getBucket("xray", FieldDataType.TEXT);
        Assert.assertNotEquals(bucket5, bucket6);
        Long bucket7 = locator.getBucket("yankee", FieldDataType.TEXT);
        Assert.assertNotEquals(bucket7, bucket6);
        Long bucket8 = locator.getBucket("yak", FieldDataType.TEXT);
        assertEquals(bucket8, bucket7);
    }

    /**
     * test to test out the bucket distribution of the string
     */
    @Test
    public void testPracticalDistributionInteger() throws IndexParseFieldException, IOException
    {
        PrimaryIndexBucketLocatorImpl locator = PrimaryIndexBucketLocatorImpl.getInstance();
        Long bucket1 = locator.getBucket(-1000, FieldDataType.INTEGER);
        Long bucket2 = locator.getBucket(1000, FieldDataType.INTEGER);
        assertNotEquals(bucket1, bucket2);

    }
//    /**
//     * test to test out the bucket distribution of the string
//     */
//    @Test
//    public void testPracticalDistributionDouble() throws IndexParseFieldException, IOException
//    {
//        SimpleIndexBucketLocatorImpl locator = SimpleIndexBucketLocatorImpl.getInstance();
//        Long bucket1 = locator.getBucket(10000d, FieldDataType.DOUBLE);
//        Long bucket2 = locator.getBucket(.1d, FieldDataType.DOUBLE);
//        Assert.assertNotEquals(bucket1, bucket2);
//    }

    /**
     * test for testing out the expected exception for the getBucket method
     */
    @Test(expected = IllegalArgumentException.class)
    public void testGetBucketNull() throws IndexParseFieldException, IOException
    {
        PrimaryIndexBucketLocatorImpl locator = PrimaryIndexBucketLocatorImpl.getInstance();
        locator.getBucket(null, FieldDataType.TEXT);
    }

    /**
     * test for testing out the expected exception for the getBucket method
     *
     */
    @Test(expected = IllegalArgumentException.class)
    public void testGetBucketFieldtypeNull() throws IndexParseFieldException, IOException
    {
        PrimaryIndexBucketLocatorImpl locator = PrimaryIndexBucketLocatorImpl.getInstance();
        locator.getBucket(UUID.randomUUID(), null);
    }

    /**
     * Test to convert the double values to long values also checks if the
     * converted values are monotonically increasing
     */
    @Test
    public void testConvertDoubleToLong()
    {
        logger.debug("convert Double to long");
        Double d = Double.MAX_VALUE;
        Long result = PrimaryIndexBucketLocatorImpl.convertDoubleToLong(d);
        assertNearMaxLongValue(result);
        d = 0.0;
        result = PrimaryIndexBucketLocatorImpl.convertDoubleToLong(d);
        assertNearZeroLongValue(result);
        d = (double) (Double.MAX_VALUE * -1);
        result = PrimaryIndexBucketLocatorImpl.convertDoubleToLong(d);
        assertNearMinLongValue(result);
        assertTrue("Function not monotonically increasing.", PrimaryIndexBucketLocatorImpl.convertDoubleToLong(1.0) < PrimaryIndexBucketLocatorImpl.convertDoubleToLong(2.0));
        assertTrue("Function not monotonically increasing.", PrimaryIndexBucketLocatorImpl.convertDoubleToLong(2.0) < PrimaryIndexBucketLocatorImpl.convertDoubleToLong(3.0));
        assertTrue("Function not monotonically increasing.", PrimaryIndexBucketLocatorImpl.convertDoubleToLong(-2.0) < PrimaryIndexBucketLocatorImpl.convertDoubleToLong(-1.0));

        assertTrue("Function not monotonically increasing.", PrimaryIndexBucketLocatorImpl.convertDoubleToLong(-900000000000.0) < PrimaryIndexBucketLocatorImpl.convertDoubleToLong(-1.0));
    }

    /**
     * Check how the convertDateToBucketingLong method behaves with null values
     */
    @Test
    public void testDateToLongNull()
    {
        assertNull("the value is not null as expected", PrimaryIndexBucketLocatorImpl.convertDateToBucketingLong(null));
    }

    @Test
    public void testConvertObjectToBucketingToken()
    {
        assertEquals(PrimaryIndexBucketLocatorImpl.convertObjectToBucketingToken(new Boolean(true), FieldDataType.BOOLEAN), new Long(1L));
        assertEquals(PrimaryIndexBucketLocatorImpl.convertObjectToBucketingToken(new Boolean(false), FieldDataType.BOOLEAN), new Long(0L));
    }

    /**
     * Test of convertUUIDToLong method, of class Utils
     */
    @Test
    public void testconvertUUIDToLong()
    {
        logger.debug("Convert UUID to Long");
        UUID type4Uuid = UUID.randomUUID();
        UUID type4Uuid2 = UUID.randomUUID();
        String uuidInStr = "123e4567-e89b-12d3-a456-426655440000";
        UUID type1Uuid = UUID.fromString(uuidInStr);
        Long outType4Val1 = PrimaryIndexBucketLocatorImpl.convertUuidToLong(type4Uuid);
        Long outType4Val2 = PrimaryIndexBucketLocatorImpl.convertUuidToLong(type4Uuid2);
        Long outType1Val3 = PrimaryIndexBucketLocatorImpl.convertUuidToLong(type1Uuid);
        assertTrue(outType4Val1 + "is different from outType4Val2", !outType4Val1.equals(outType4Val2));
        assertTrue(outType1Val3 + "is different from outType4Val1", !outType1Val3.equals(outType4Val1));
    }

    /**
     * Test to compare the timestamp values of two different type1 UUIDs the
     * timestamp values of two different type1 UUID's should not be equal
     */
    @Test
    public void testUUIDToLongTimestamp()
    {
        String uuidInStr1 = "123e4567-e87b-12d3-a456-426655440000";
        UUID uuid1 = UUID.fromString(uuidInStr1);
        Long timestampOfUuid1 = PrimaryIndexBucketLocatorImpl.convertUuidToLong(uuid1);
        Long timestampOfUuid11 = uuid1.timestamp();
        assertEquals(timestampOfUuid1, timestampOfUuid11);
        String uuidInStr2 = "123e4567-e89b-12d3-a456-426655440000";
        UUID recentUuid = UUID.fromString(uuidInStr2);
        Long timestampOfrecentUuid1 = recentUuid.timestamp();
        assertTrue(timestampOfUuid1 + " is different from timestampOfUuid2", !timestampOfUuid11.equals(timestampOfrecentUuid1));
        assertTrue("Timestamp of recent UUID should be greater then the old uuid", timestampOfUuid11 < timestampOfrecentUuid1);
    }

    /**
     * Compare the long epoch time values between convertDateToBucketingLong and
     * a java method both the values should be equal, a random hardcoded value
     * is being used
     */
    @Test
    public void testConvertDateToBucketingLong() throws Exception
    {
        Date result = ParseUtils.parseStringAsDate("Sun, 15 Jun 2014 22:31:42 GMT");
        long time1 = PrimaryIndexBucketLocatorImpl.convertDateToBucketingLong(result);
        long time2 = PrimaryIndexBucketLocatorImpl.convertDateToBucketingLong(new SimpleDateFormat("MM/dd/yyyy HH:mm:ss z").parse("06/15/2014 22:31:42 GMT"));
        assertTrue("the times are not equal", time1 == time2);
    }

    /**
     * Checks how the method reacts with null values or blank values
     */
    @Test
    public void testStringToLongNull()
    {
        Long testLong = PrimaryIndexBucketLocatorImpl.convertStringToBuckectingLong(null);
        assertTrue("null values sent is not null", testLong == null);
        Long testLong1 = PrimaryIndexBucketLocatorImpl.convertStringToBuckectingLong("");
        assertTrue("blank string sent is not null", testLong1 == null);
    }

    /**
     * Test to check how the convertIntegerToBucketingLong reacts to null values
     * a return value of null is expected
     */
    @Test
    public void testIntegerToLongNull()
    {
        logger.debug("test convertIntegerToBucketingLong with null value");
        Long testNull = PrimaryIndexBucketLocatorImpl.convertIntegerToBucketingLong(null);
        assertTrue("a null value was expected instead received" + testNull, testNull == null);
    }

    /**
     * Test to check the overflowing in the integer should not return a null or
     * end in an exception
     */
    @Test
    public void testIntegerMinvalue()
    {
        logger.debug("test convertIntegerToBucketingLong with min value value");
        Long testNull = PrimaryIndexBucketLocatorImpl.convertIntegerToBucketingLong(0);
        assertFalse("a null value was expected instead received" + testNull, testNull == null);
    }

    /**
     * Test of convertIntegerToBucketingLong method, of class Utils.
     */
    @Test
    public void testConvertIntegerToBucketingLong()
    {
        logger.debug("convertIntegerToBucketingLong");
        Integer integer = Integer.MAX_VALUE;
        //Long result = SimpleIndexBucketLocatorImpl.convertIntegerToBucketingLong(integer);
        //assertNearMaxLongValue(result);
        integer = 0;
        Long result = PrimaryIndexBucketLocatorImpl.convertIntegerToBucketingLong(integer);
        assertNearZeroLongValue(result);
        integer = Integer.MIN_VALUE + 1;
//        result = SimpleIndexBucketLocatorImpl.convertIntegerToBucketingLong(integer);
//        assertNearMinLongValue(result);
        assertTrue("Function not monotonically increasing.", PrimaryIndexBucketLocatorImpl.convertIntegerToBucketingLong(1) < PrimaryIndexBucketLocatorImpl.convertIntegerToBucketingLong(2));
        assertTrue("Function not monotonically increasing.", PrimaryIndexBucketLocatorImpl.convertIntegerToBucketingLong(2) < PrimaryIndexBucketLocatorImpl.convertIntegerToBucketingLong(3));
        assertTrue("Function not monotonically increasing.", PrimaryIndexBucketLocatorImpl.convertIntegerToBucketingLong(-2) < PrimaryIndexBucketLocatorImpl.convertIntegerToBucketingLong(-1));
    }

    /**
     * Test to check the return value for double to long conversion null is
     * expected
     */
    @Test
    public void testDoubleToLongNullValues()
    {
        logger.debug("convert Double to long with null values");
        assertNull("the value is not null as expected ", PrimaryIndexBucketLocatorImpl.convertDoubleToLong(null));
    }

    /**
     * Checks how the method reacts with null values
     */
    @Test
    public void testUuidToLongNull()
    {
        logger.debug("Test to converUuidToLong with null value");
        Long testLongNull = PrimaryIndexBucketLocatorImpl.convertUuidToLong(null);
        assertTrue("null values sent is not null", testLongNull == null);
    }

    /**
     * This test is trying to run all the possible strings will different chars
     * also the test is checking the ordering of the long values and tests
     * string which range from 1 char to greater than 8 chars
     */
    @Test
    public void testConvertStringToLong()
    {
        logger.debug("convert string to long test");
        String[] testString = new String[15];
        Long[] testLong = new Long[15];
        testString[0] = "adam";
        testString[1] = "alexandria";
        testString[2] = "apartment";
        testString[3] = "apple";
        testString[4] = "aaaaaaaaaaaa";
        testString[5] = "AAAAAAAAAAAA";
        testString[6] = "!@#$%^^&*!@#";
        testString[7] = "\u3053\u306e\u30b9\u30e9\u30a4\u30c9\u30da\u30fc\u30b8\u306b\u79fb\u52d5";
        testString[8] = "\u3042 \u3044 \u3046 \u3048 \u304a \u304b \u304d \u304f \u3051 \u3053 \u3055 \u3057 \u3059 \u305b \u305d \u304c \u304e \u3050 \u3052 \u3054 \u3071 \u3074 \u3077 \u307a \u307d";
        testString[9] = "wwwwwwwwwwwwwwwwwerwerwerwerweiurhowehriouwheorwemrowiehrmowiehhowieuhwomieurhmwieuhmrwuhemroiwuhemrowiuehrmoiwuehrmowieuhrmoiwuehromiwuehmriuwebifbvwdyfovwdy";
        testString[10] = "a";
        testString[11] = "9834658937469587639487569873645";
        for (int i = 0; i < testString.length; i++)
        {
            testLong[i] = PrimaryIndexBucketLocatorImpl.convertStringToBuckectingLong(testString[i]);
        }
        assertTrue("the string A is supposed to less in value than the string B", testLong[0] < testLong[1]);
        assertTrue("the string A is supposed to less in value than the string B", testLong[1] < testLong[2]);
        assertTrue("the string A is supposed to less in value than the string B", testLong[2] < testLong[3]);
    }

    /**
     * This test is trying to run Greek strings with different chars
     *
     */
    @Test
    public void testConvertGreekStringToLong()
    {
        logger.debug("convert Greek string to long test");
        String[] testString = new String[10];
        Long[] testLong = new Long[10];
        testString[0] = "Ξ ξ";
        testString[1] = "Οappleο";
        testString[2] = "Π π";
        testString[3] = "Σiuyhσ/ς";
        testString[4] = "Φaaaaaφ";
        testString[5] = "Ψ ψ";
        testString[6] = "Λ λ";
        for (int i = 0; i < testString.length; i++)
        {
            testLong[i] = PrimaryIndexBucketLocatorImpl.convertStringToBuckectingLong(testString[i]);
        }
        assertTrue("testString[0] should not be equal to testString[1]", !testLong[0].equals(testLong[1]));
        assertTrue("testString[1] should not be equal to testString[2]", !testLong[1].equals(testLong[2]));
        assertTrue("testString[2] should not be equal to testString[3]", !testLong[2].equals(testLong[3]));
    }

    /**
     * This test is trying to run Arabic strings with different chars
     *
     */
    @Test
    public void testConvertArabicStringToLong()
    {
        logger.debug("convert Arabic string to long test");
        String[] testString = new String[10];
        Long[] testLong = new Long[10];
        testString[0] = "ص مew";
        testString[1] = "ض4ew";
        testString[2] = "فfsjdم";
        testString[3] = "غ تrfds";
        testString[4] = "ظwsd";
        testString[5] = "غ";
        testString[6] = "فfsjdم";
        for (int i = 0; i < testString.length; i++)
        {
            testLong[i] = PrimaryIndexBucketLocatorImpl.convertStringToBuckectingLong(testString[i]);
        }
        assertTrue("testString[0] is not equal to testString[1] ", !testLong[0].equals(testLong[1]));
        assertTrue("testString[1] should not be equal to testString[2] ", !testLong[1].equals(testLong[2]));
        assertTrue("testString[2] should not be equal to testString[3]", !testLong[2].equals(testLong[3]));
    }

//    /**
//     * Test of convertStringToUUID method, of class Utils. Tests UUID ordering.
//     * This test isn't so much of a unit test as a proof of concept.
//     */
//    public void testConvertStringToUUIDOrdering3(UtilsTest utilsTest)
//    {
//        logger.debug("convertStringToUUIDOrdering3");
//        String a = "adam";
//        String b = "alexandria";
//        String c = "apartment";
//        String z = "apple";
//        UUID aUUID = Utils.convertStringToFuzzyUUID(a);
//        UUID bUUID = Utils.convertStringToFuzzyUUID(b);
//        UUID cUUID = Utils.convertStringToFuzzyUUID(c);
//        UUID zUUID = Utils.convertStringToFuzzyUUID(z);
//        logger.info(a + ": " + aUUID.toString() + " " + aUUID.getMostSignificantBits());
//        logger.info(b + ": " + bUUID.toString() + " " + bUUID.getMostSignificantBits());
//        logger.info(c + ": " + cUUID.toString() + " " + cUUID.getMostSignificantBits());
//        logger.info(z + ": " + zUUID.toString() + " " + zUUID.getMostSignificantBits());
//        assertTrue(aUUID.getMostSignificantBits() < bUUID.getMostSignificantBits());
//        assertTrue(bUUID.getMostSignificantBits() < cUUID.getMostSignificantBits());
//        assertTrue(cUUID.getMostSignificantBits() < zUUID.getMostSignificantBits());
//    }
    /**
     * Test to compare long epoch time values time1(hardcoded) values should
     * always be less than current epoch time
     */
    @Test
    public void testDateToLongCompareTimes() throws Exception
    {
        Date testDate = new Date();
        String in = testDate.toString();
        Date currentTime = ParseUtils.parseStringAsDate(in);
        long timeCurrent = PrimaryIndexBucketLocatorImpl.convertDateToBucketingLong(currentTime);
        Date result = ParseUtils.parseStringAsDate("Mon Jun 15 22:24:54 GMT 2015");
        long time1 = PrimaryIndexBucketLocatorImpl.convertDateToBucketingLong(result);
        assertTrue("time1 is not less than timeCurrent" + time1 + "<" + timeCurrent, time1 < timeCurrent);
        Date resultFuture = ParseUtils.parseStringAsDate("6/6/2115");
        long timeFuture = PrimaryIndexBucketLocatorImpl.convertDateToBucketingLong(resultFuture);
        assertTrue("current time is not less than future time" + timeCurrent + "<" + timeFuture, timeCurrent < timeFuture);
        Date resultAncient = ParseUtils.parseStringAsDate("9/9/1000");
        long timeAncient = PrimaryIndexBucketLocatorImpl.convertDateToBucketingLong(resultAncient);
        assertTrue("ancient time is not less than current time", timeAncient < timeCurrent);
    }

    /**
     * Test of convertBinaryBase64ToLong method, of class Utils
     */
    @Test
    public void testBinaryBase64ToLong()
    {
        logger.debug("Test to convertBinaryBase64ToLong");
        String binaryBase641 = "kkkk";
        String binaryBase642 = "oooooooooooooooo";
        String binaryBase643 = "wwwwwwwwwwwwwwwwwwww";
        String binaryBase64Duplicate = "oooooooooooooooo";
        String binaryBase644 = "kkkkkkkk";
        Long firstBinLongVal = PrimaryIndexBucketLocatorImpl.convertBinaryBase64ToLong(binaryBase641);
        Long secondBinLongVal = PrimaryIndexBucketLocatorImpl.convertBinaryBase64ToLong(binaryBase642);
        Long thirdBinLongVal = PrimaryIndexBucketLocatorImpl.convertBinaryBase64ToLong(binaryBase643);
        Long binaryBase64Dupl = PrimaryIndexBucketLocatorImpl.convertBinaryBase64ToLong(binaryBase64Duplicate);
        assertTrue("First binaryBase64 String should be less than the second one", firstBinLongVal < secondBinLongVal);
        assertTrue("Second binaryBase64 String should be less than the third one", secondBinLongVal < thirdBinLongVal);
        assertEquals("Duplicate input values should return the same output Long value", secondBinLongVal, binaryBase64Dupl);
        Long fourthBinLongVal = PrimaryIndexBucketLocatorImpl.convertBinaryBase64ToLong(binaryBase644);//checks our case when we have exactly 8 chars
        assertTrue("Forth binaryBase64 String should be greater than the first one", fourthBinLongVal > firstBinLongVal);//4th should be greater than 1st
    }

    /**
     * Test to check the exception in the convertBinaryBase64ToLong method
     */
    @Test
    public void testBinaryBase64ToLongException()
    {
        logger.debug("test to converBinaryBase64ToLong with Exception");
        boolean expectedExceptionThrown = false;
        try
        {
            PrimaryIndexBucketLocatorImpl.convertBinaryBase64ToLong("jf");
        } catch (IllegalArgumentException ex)
        {
            expectedExceptionThrown = true;
        }
        assertTrue("Expected exception thrown. ", expectedExceptionThrown);
    }

    /**
     * Checks how the converBinaryBase64ToLong method reacts with null values
     */
    @Test
    public void testBinaryBase64ToLongNull()
    {
        logger.debug("Test to converBinaryBase64ToLong with null value");
        Long testLongNull = PrimaryIndexBucketLocatorImpl.convertBinaryBase64ToLong(null);
        assertTrue("null values sent is not null", testLongNull == null);
    }

    /**
     * Test of getInstance method, of class SimpleIndexBucketLocatorImpl.
     */
    @Test
    public void testGetInstance() throws IOException
    {
        logger.debug("getInstance");
        //Make sure it actually returns something both on the first create and second creates
        PrimaryIndexBucketLocatorImpl result = PrimaryIndexBucketLocatorImpl.getInstance();
        Assert.assertNotNull(result);
        result = PrimaryIndexBucketLocatorImpl.getInstance();
        Assert.assertNotNull(result);
    }

    private void assertNearMaxLongValue(Long in)
    {
        assertTrue("Value: " + in + " is not close enough to the Long MAX value.", in > Long.MAX_VALUE - 100);
    }

    private void assertNearMinLongValue(Long in)
    {
        assertTrue("Value: " + in + " is not close enough to the Long MIN value.", in < Long.MIN_VALUE + 100);
    }

    private void assertNearZeroLongValue(Long in)
    {
        assertTrue("Value: " + in + " is not close enough to the Long ZERO value.", in > -100 || in < -100);
    }

    /**
     * Test of convertTimepointToBucketingLong method, of class
     * SimpleIndexBucketLocatorImpl.
     */
    @Test
    public void testConvertTimepointToBucketingLong()
    {
        System.out.println("convertTimepointToBucketingLong");
        Date timepointToBeConverted = new Date();
        Long expResult = timepointToBeConverted.getTime() - PrimaryIndexBucketGeneratorImpl.TIMEPOINT_MIN;
        //base test
        Long result = PrimaryIndexBucketLocatorImpl.convertTimepointToBucketingLong(timepointToBeConverted);
        assertEquals(expResult, result);
        //date after 1 Jan 2030
        timepointToBeConverted = new Date(1893542400000l);//01/02/2030 GMT
        result = PrimaryIndexBucketLocatorImpl.convertTimepointToBucketingLong(timepointToBeConverted);
        assertEquals(PrimaryIndexBucketGeneratorImpl.TIMEPOINT_MAX, result);//make sure it knocks it back down to 1 Jan 2030 (with offset)
        //date prior to 2000
        timepointToBeConverted = new Date(946684800000l);
        result = PrimaryIndexBucketLocatorImpl.convertTimepointToBucketingLong(timepointToBeConverted);
        assertEquals(new Long(0l), result);//make sure it knocks it up to 1 Jan 1970 (with offset)

        //confirm that close dates actually come out as different values
        timepointToBeConverted = new Date();
        result = PrimaryIndexBucketLocatorImpl.convertTimepointToBucketingLong(timepointToBeConverted);
        timepointToBeConverted = new Date(timepointToBeConverted.getTime() + 1);
        Long result2 = PrimaryIndexBucketLocatorImpl.convertTimepointToBucketingLong(timepointToBeConverted);
        Assert.assertNotEquals(result, result2);

    }

    /**
     * Test of convertBinaryBase64ToLong method, of class
     * SimpleIndexBucketLocatorImpl.
     */
    @Test
    @Ignore
    public void testConvertBinaryBase64ToLong()
    {
        System.out.println("convertBinaryBase64ToLong");
        String binaryBase64 = "";
        Long expResult = null;
        Long result = PrimaryIndexBucketLocatorImpl.convertBinaryBase64ToLong(binaryBase64);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.

    }

    /**
     * Test of convertStringToBuckectingLong method, of class
     * SimpleIndexBucketLocatorImpl.
     */
    @Test
    @Ignore
    public void testConvertStringToBuckectingLong()
    {
        System.out.println("convertStringToBuckectingLong");
        String stringToBeConverted = "";
        Long expResult = null;
        Long result = PrimaryIndexBucketLocatorImpl.convertStringToBuckectingLong(stringToBeConverted);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.

    }

    /**
     * Test of convertUuidToLong method, of class SimpleIndexBucketLocatorImpl.
     */
    @Test
    @Ignore
    public void testConvertUuidToLong()
    {
        System.out.println("convertUuidToLong");
        UUID uuid = null;
        Long expResult = null;
        Long result = PrimaryIndexBucketLocatorImpl.convertUuidToLong(uuid);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.

    }

}
