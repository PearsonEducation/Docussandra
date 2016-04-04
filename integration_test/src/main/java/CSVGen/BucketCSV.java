package CSVGen;

import com.pearson.docussandra.Utils;
import com.pearson.docussandra.bucketmanagement.PrimaryIndexBucketGeneratorImpl;
import com.pearson.docussandra.bucketmanagement.PrimaryIndexBucketLocatorImpl;
import com.pearson.docussandra.domain.objects.FieldDataType;
import com.pearson.docussandra.exception.IndexParseFieldException;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.testhelpers.RunStats;
import utils.testhelpers.TestUtils;
import static utils.testhelpers.TestUtils.calculate;

/**
 * Ugly as crud bucket gen main class.
 *
 * @author https://github.com/JeffreyDeYoung
 */
public class BucketCSV
{

    private static Logger logger = LoggerFactory.getLogger(BucketCSV.class);

    public static void main(String args[]) throws Exception
    {
        //generateDateBucketsAndTest();
        //generateStringBucketsAndTest();
        //generateLongBucketsAndTest();
        //generateStringBucketsAndTest();
        //generateTimepointBucketsAndTest();
        //generateBinaryBucketsAndTest();
        //generateLongBucketsAndTest();
        //generateIntegerBucketsAndTest();
        generateDoubleBucketsAndTest();
    }

    public static void generateIntegerBucketsAndTest() throws Exception
    {
        int numBuckets = PrimaryIndexBucketGeneratorImpl.INTEGER_BUCKET_SIZE;
        RunStats res = delegateGetBucketForIntegers(numBuckets);
        Utils.writeFile(res.printDistAsCSV(), "IntegerWith" + numBuckets + "buckets.csv");
        Utils.writeFile(res.printDistStatsAsCSV(), "IntegerStatsWith" + numBuckets + "buckets.csv");
        StringBuilder sb = new StringBuilder();
        for (Long l : PrimaryIndexBucketLocatorImpl.getBUCKETS()[FieldDataType.INTEGER.getIndexForDataType()])
        {
            sb.append(l).append(",");
        }
        Utils.writeFile(sb.toString(), "bucketsInteger.csv");
    }

    public static void generateStringBucketsAndTest() throws Exception
    {
        int numBuckets = 500000;
        RunStats res = delegateGetBucketForString(numBuckets);
        Utils.writeFile(res.printDistAsCSV(), "FourthousandYearStringWith" + numBuckets + "buckets.csv");
        Utils.writeFile(res.printDistStatsAsCSV(), "FourthousandYearStringStatsWith" + numBuckets + "buckets.csv");
        StringBuilder sb = new StringBuilder();
        for (Long l : PrimaryIndexBucketLocatorImpl.getBUCKETS()[FieldDataType.TEXT.getIndexForDataType()])
        {
            sb.append(l).append(",");
        }
        Utils.writeFile(sb.toString(), "bucketsString.csv");
    }

    public static RunStats delegateGetBucketForString(int buckets) throws FileNotFoundException, Exception
    {
        Long startTime = System.currentTimeMillis();
        PrimaryIndexBucketGeneratorImpl ibg = new PrimaryIndexBucketGeneratorImpl(FieldDataType.TEXT, buckets);
        PrimaryIndexBucketLocatorImpl locator = new PrimaryIndexBucketLocatorImpl(ibg.generateBuckets(), null, null, null, null, null, null, null, null);
        logger.debug("test to getBucket for the bucket distribution");
        String file = "/MostCommon20kEnglishWords.txt";
        String englishFile = Utils.readFile(file);
        String[] englishLines = englishFile.split("\n");
        HashMap<Long, List<Object>> hm = new HashMap<>();
        SummaryStatistics stats = new SummaryStatistics();
        for (String line : englishLines)
        {
            line = line.trim();
            Long bucketId = locator.getBucket(line, FieldDataType.TEXT);
            calculate(hm, stats, bucketId, line);
        }
        Long runTime = System.currentTimeMillis() - startTime;
        RunStats runStats = new RunStats("BucketForStrings", FieldDataType.TEXT, runTime, stats, hm);
        logger.debug(runStats.toString());
        return runStats;
    }

    public static void generateDateBucketsAndTest() throws Exception
    {
        int numBuckets = Integer.MAX_VALUE / 96;
        RunStats res = delegateGetBucketForFourthousandYearDates(numBuckets);
        Utils.writeFile(res.printDistAsCSV(), "FourthousandYearDatesWith" + numBuckets + "buckets.csv");
        Utils.writeFile(res.printDistStatsAsCSV(), "FourthousandYearDatesStatsWith" + numBuckets + "buckets.csv");
        StringBuilder sb = new StringBuilder();
        for (Long l : PrimaryIndexBucketLocatorImpl.getBUCKETS()[FieldDataType.DATE_TIME.getIndexForDataType()])
        {
            sb.append(l).append(",");
        }
        Utils.writeFile(sb.toString(), "bucketsDate.csv");
    }

    public static void generateDoubleBucketsAndTest() throws Exception
    {
        int numBuckets = PrimaryIndexBucketGeneratorImpl.DOUBLE_BUCKET_SIZE;
        RunStats res = delegateGetBucketForDouble(numBuckets);
        Utils.writeFile(res.printDistAsCSV(), "doubleWith" + numBuckets + "buckets.csv");
        Utils.writeFile(res.printDistStatsAsCSV(), "doubleStatsWith" + numBuckets + "buckets.csv");
        StringBuilder sb = new StringBuilder();
        for (Long l : PrimaryIndexBucketLocatorImpl.getBUCKETS()[FieldDataType.DOUBLE.getIndexForDataType()])
        {
            sb.append(l).append(",");
        }
        Utils.writeFile(sb.toString(), "bucketsDouble.csv");
    }

    public static void generateLongBucketsAndTest() throws Exception
    {
        int numBuckets = Integer.MAX_VALUE / 96;
        RunStats res = delegateGetBucketForLong(numBuckets);
        Utils.writeFile(res.printDistAsCSV(), "LongWith" + numBuckets + "buckets.csv");
        Utils.writeFile(res.printDistStatsAsCSV(), "LongStatsWith" + numBuckets + "buckets.csv");
        StringBuilder sb = new StringBuilder();
        for (Long l : PrimaryIndexBucketLocatorImpl.getBUCKETS()[FieldDataType.LONG.getIndexForDataType()])
        {
            sb.append(l).append(",");
        }
        Utils.writeFile(sb.toString(), "bucketsLong.csv");
    }

    public static void generateTimepointBucketsAndTest() throws Exception
    {
        int numBuckets = PrimaryIndexBucketGeneratorImpl.TIMEPOINT_BUCKET_SIZE;
        RunStats res = delegateGetBucketForFourthousandYearTimepoint(numBuckets);
        Utils.writeFile(res.printDistAsCSV(), "FourthousandYearTimepointWith" + numBuckets + "buckets.csv");
        Utils.writeFile(res.printDistStatsAsCSV(), "FourthousandYearTimepointStatsWith" + numBuckets + "buckets.csv");

        res = delegateGetBucketPerRecentDateForMinute(numBuckets, FieldDataType.TIMEPOINT);
        Utils.writeFile(res.printDistAsCSV(), "recentTimepointWith" + numBuckets + "buckets.csv");
        Utils.writeFile(res.printDistStatsAsCSV(), "recentTimepointStatsWith" + numBuckets + "buckets.csv");

    }

    public static void generateBinaryBucketsAndTest() throws Exception
    {
        int numBuckets = 500000;
        RunStats res = delegateGetBucketForBinaryBase64(numBuckets);
        Utils.writeFile(res.printDistAsCSV(), "binaryWith" + numBuckets + "buckets.csv");
        Utils.writeFile(res.printDistStatsAsCSV(), "binaryStatsWith" + numBuckets + "buckets.csv");
        StringBuilder sb = new StringBuilder();
        for (Long l : PrimaryIndexBucketLocatorImpl.getBUCKETS()[FieldDataType.BINARY.getIndexForDataType()])
        {
            sb.append(l).append(",");
        }
        Utils.writeFile(sb.toString(), "bucketsBinary.csv");
    }

    public static RunStats delegateGetBucketForFourthousandYearTimepoint(int buckets) throws ParseException, IndexParseFieldException, Exception
    {
        long startTime = System.currentTimeMillis();
        PrimaryIndexBucketGeneratorImpl sbg = new PrimaryIndexBucketGeneratorImpl(FieldDataType.TIMEPOINT, buckets);
        PrimaryIndexBucketLocatorImpl locator = new PrimaryIndexBucketLocatorImpl(null, null, null, null, null, null, null, null, sbg.generateAndWrite());
        logger.debug("test to getBucket for the bucket distribution on FourthousandYear timepoint");
        SimpleDateFormat simpleDtFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
        //Calendar calendar = Calendar.getInstance();
        String startDate = "0015-07-14T00:00:00.000-06:00";
        HashMap<Long, List<Object>> hm = new HashMap<>();
        SummaryStatistics stats = new SummaryStatistics();
        Date date = null;
        for (int i = 0; i < 1460000 * 2; i++)	// There are approximately 1460000 days from 0015 year to 4015 year
        {
            date = simpleDtFormat.parse(startDate);
            final Date TIME = new Date((long) (Math.random() * 86400000l));
            date.setTime(date.getTime() + TIME.getTime());//set the time of day to a random time
            Long bucketId = locator.getBucket(startDate, FieldDataType.TIMEPOINT);
            calculate(hm, stats, bucketId, startDate);
            date.setTime(date.getTime() + 86400000l - TIME.getTime()); // add a day (86400000 ms = 1 day) and subtract the random time we added 
            startDate = simpleDtFormat.format(date.getTime());
        }
        long runTime = System.currentTimeMillis() - startTime;
        RunStats runStats = new RunStats("BucketForFourthousandYearDates", FieldDataType.TIMEPOINT, runTime, stats, hm);
        logger.debug(runStats.toString());
        return runStats;
    }

    public static RunStats delegateGetBucketForFourthousandYearDates(int buckets) throws ParseException, IndexParseFieldException, Exception
    {
        long startTime = System.currentTimeMillis();
        PrimaryIndexBucketGeneratorImpl sbg = new PrimaryIndexBucketGeneratorImpl(FieldDataType.DATE_TIME, buckets);
        PrimaryIndexBucketLocatorImpl locator = new PrimaryIndexBucketLocatorImpl(null, sbg.generateAndWrite(), null, null, null, null, null, null, null);
        logger.debug("test to getBucket for the bucket distribution on FourthousandYear dates");
        SimpleDateFormat simpleDtFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
        //Calendar calendar = Calendar.getInstance();
        String startDate = "0015-07-14T00:00:00.000-06:00";
        HashMap<Long, List<Object>> hm = new HashMap<>();
        SummaryStatistics stats = new SummaryStatistics();
        Date date = null;
        for (int i = 0; i < 1460000 * 2; i++)	// There are approximately 1460000 days from 0015 year to 4015 year
        {
            date = simpleDtFormat.parse(startDate);
            final Date TIME = new Date((long) (Math.random() * 86400000l));
            date.setTime(date.getTime() + TIME.getTime());//set the time of day to a random time
            Long bucketId = locator.getBucket(startDate, FieldDataType.DATE_TIME);
            calculate(hm, stats, bucketId, startDate);
            date.setTime(date.getTime() + 86400000l - TIME.getTime()); // add a day (86400000 ms = 1 day) and subtract the random time we added 
            startDate = simpleDtFormat.format(date.getTime());
        }
        long runTime = System.currentTimeMillis() - startTime;
        RunStats runStats = new RunStats("BucketForFourthousandYearDates", FieldDataType.DATE_TIME, runTime, stats, hm);
        logger.debug(runStats.toString());
        return runStats;
    }

//    private static void printBucketList() throws Exception
//    {
//        List<Long> bucketList = SimpleIndexBucketLocatorImpl.getBUCKETS()[FieldDataType.DATE_TIME.getIndexForDataType()];
//        StringBuilder sb = new StringBuilder();
//        for (Long l : bucketList)
//        {
//            sb.append(l).append(RunStats.DE);
//        }
//        TestUtils.writeFile(sb.toString(), "bucketListCSV.csv");
//    }
    /**
     * Test to check the Bucket Distribution for the Greek and English values
     * together
     *
     * @throws IndexParseFieldException
     */
    public static RunStats delegateGetBucketForGreekAndEnglishString(int buckets) throws Exception
    {
        Long startTime = System.currentTimeMillis();
        PrimaryIndexBucketGeneratorImpl ibg = new PrimaryIndexBucketGeneratorImpl(FieldDataType.TEXT, buckets);
        PrimaryIndexBucketLocatorImpl locator = new PrimaryIndexBucketLocatorImpl(ibg.generateBuckets(), null, null, null, null, null, null, null, null);
        logger.debug("test to getBucket for the bucket distribution on English and Greek Strings");
        String fileEnglish = "/MostCommon20kEnglishWords.txt";
        String fileGreek = "/MostCommon10kGreekWords.txt";
        String greekFile = Utils.readFile(fileGreek);
        String[] greekLines = greekFile.split("\n");
        String englishFile = Utils.readFile(fileEnglish);
        String[] englishLines = englishFile.split("\n");
        try
        {
            HashMap<Long, List<Object>> hm = new HashMap<>();
            SummaryStatistics stats = new SummaryStatistics();
            for (String line : englishLines)
            {
                line = line.trim();
                Long bucketId = locator.getBucket(line, FieldDataType.TEXT);
                calculate(hm, stats, bucketId, line);
            }
            for (String line : greekLines)
            {
                line = line.trim();
                Long bucketId = locator.getBucket(line, FieldDataType.TEXT);
                calculate(hm, stats, bucketId, line);
            }
            Long runTime = System.currentTimeMillis() - startTime;
            RunStats runStats = new RunStats("BucketForStrings", FieldDataType.TEXT, runTime, stats, hm);
            logger.debug(runStats.toString());
            return runStats;
        } catch (Exception e)
        {
            logger.debug(e.toString(), e);
            throw e;
        }
    }

    /**
     * checks the Bucket Distribution for the Boolean values
     *
     * @throws IndexParseFieldException
     */
    public static RunStats delegateGetBucketForBoolean(int buckets) throws Exception
    {
        Long startTime = System.currentTimeMillis();
        PrimaryIndexBucketGeneratorImpl ibg = new PrimaryIndexBucketGeneratorImpl(FieldDataType.BOOLEAN, buckets);
        PrimaryIndexBucketLocatorImpl locator = new PrimaryIndexBucketLocatorImpl(null, null, null, null, ibg.generateBuckets(), null, null, null, null);
        logger.debug("Test to getBucket for the bucket distribution on Boolean values");
        HashMap<Long, List<Object>> hm = new HashMap<>();
        SummaryStatistics stats = new SummaryStatistics();
        for (int i = 0; i < 20000; i++)
        {
            Boolean randomBoolean = RandomUtils.nextBoolean();
            Long bucketId = locator.getBucket(randomBoolean, FieldDataType.BOOLEAN);
            TestUtils.calculate(hm, stats, bucketId, randomBoolean);
        }
        Long runTime = System.currentTimeMillis() - startTime;
        RunStats runStats = new RunStats("BucketForBoolean", FieldDataType.BOOLEAN, runTime, stats, hm);
        logger.debug(runStats.toString());
        return runStats;
    }

    /**
     * Test to check the Bucket Distribution for the UUID values
     *
     * @throws IndexParseFieldException
     */
    public static RunStats delegateGetBucketForUuids(int buckets) throws Exception
    {
        Long startTime = System.currentTimeMillis();
        PrimaryIndexBucketGeneratorImpl ibg = new PrimaryIndexBucketGeneratorImpl(FieldDataType.UUID, buckets);
        PrimaryIndexBucketLocatorImpl locator = new PrimaryIndexBucketLocatorImpl(null, null, null, null, null, ibg.generateBuckets(), null, null, null);
        logger.debug("test to getBucket for the bucket distribution on UUIDs");
        HashMap<Long, List<Object>> hm = new HashMap<>();
        SummaryStatistics stats = new SummaryStatistics();
        UUID[] uuid = new UUID[10000];
        for (int i = 0; i < 10000; i++)
        {
            uuid[i] = UUID.randomUUID();
        }
        for (int inputUuid = 0; inputUuid < 10000; inputUuid++)
        {
            Long bucketId = locator.getBucket(uuid[inputUuid], FieldDataType.UUID);
            calculate(hm, stats, bucketId, inputUuid);
        }
        Long runTime = System.currentTimeMillis() - startTime;
        RunStats runStats = new RunStats("BucketForUUIDs", FieldDataType.UUID, runTime, stats, hm);
        logger.debug(runStats.toString());
        return runStats;
    }

    /**
     * Test to check the Bucket Distribution for the common Integer values
     *
     * @throws IndexParseFieldException
     */
    public static RunStats delegateGetBucketForIntegers(int buckets) throws Exception
    {
        Long startTime = System.currentTimeMillis();
        PrimaryIndexBucketGeneratorImpl ibg = new PrimaryIndexBucketGeneratorImpl(FieldDataType.INTEGER, buckets);
        PrimaryIndexBucketLocatorImpl locator = new PrimaryIndexBucketLocatorImpl(null, null, null, ibg.generateBuckets(), null, null, null, null, null);
        logger.debug("test to getBucket for the bucket distribution");
        HashMap<Long, List<Object>> hm = new HashMap<>();
        SummaryStatistics stats = new SummaryStatistics();
        for (int inputInt = -2000000; inputInt < 2000000; inputInt++)
        {
            Long bucketId = locator.getBucket(inputInt, FieldDataType.INTEGER);
            calculate(hm, stats, bucketId, inputInt);
        }
        Long runTime = System.currentTimeMillis() - startTime;
        RunStats runStats = new RunStats("BucketForIntegers", FieldDataType.INTEGER, runTime, stats, hm);
        logger.debug(runStats.toString());
        return runStats;
    }

    /**
     * Test to check the Bucket Distribution for BinaryBase64 Values
     *
     * @throws IndexParseFieldException
     */
    public static RunStats delegateGetBucketForBinaryBase64(int buckets) throws Exception
    {
        Long startTime = System.currentTimeMillis();
        PrimaryIndexBucketGeneratorImpl ibg = new PrimaryIndexBucketGeneratorImpl(FieldDataType.BINARY, buckets);
        PrimaryIndexBucketLocatorImpl locator = new PrimaryIndexBucketLocatorImpl(null, null, null, null, null, null, ibg.generateBuckets(), null, null);
        logger.debug("test to getBucket for the bucket distribution on BinaryBase64 Values");
        HashMap<Long, List<Object>> hm = new HashMap<>();
        SummaryStatistics stats = new SummaryStatistics();
        for (int i = 0; i < 40000; i++)
        {
            String randomString = RandomStringUtils.randomAlphanumeric(40);
            byte[] bytesEncoded = Base64.encodeBase64(randomString.getBytes());
            String line = new String(bytesEncoded);
            Long bucketId = locator.getBucket(line, FieldDataType.BINARY);
            TestUtils.calculate(hm, stats, bucketId, line);
        }
        Long runTime = System.currentTimeMillis() - startTime;
        RunStats runStats = new RunStats("BucketForBinaryBase64", FieldDataType.BINARY, runTime, stats, hm);
        logger.debug(runStats.toString());
        return runStats;
    }

    /**
     * Test to check the Bucket Distribution for the Long values
     *
     * @throws IndexParseFieldException
     */
    public static RunStats delegateGetBucketForLong(int buckets) throws Exception
    {
        Long startTime = System.currentTimeMillis();
        PrimaryIndexBucketGeneratorImpl ibg = new PrimaryIndexBucketGeneratorImpl(FieldDataType.LONG, buckets);
        PrimaryIndexBucketLocatorImpl locator = new PrimaryIndexBucketLocatorImpl(null, null, null, null, null, null, null, ibg.generateBuckets(), null);
        logger.debug("test to getBucket for the bucket distribution on Long values");
        HashMap<Long, List<Object>> hm = new HashMap<>();
        SummaryStatistics stats = new SummaryStatistics();
        Random r = new Random();
        for (int i = -200000; i < 200000; i++)
        {
            Long randomLong = Long.MIN_VALUE + r.nextLong() * Long.MAX_VALUE;
            Long bucketId = locator.getBucket(randomLong, FieldDataType.LONG);
            TestUtils.calculate(hm, stats, bucketId, randomLong);
        }
        Long runTime = System.currentTimeMillis() - startTime;
        RunStats runStats = new RunStats("BucketForLong", FieldDataType.LONG, runTime, stats, hm);
        logger.debug(runStats.toString());
        return runStats;
    }

    public static RunStats delegateGetBucketForGreekString(int buckets) throws Exception
    {
        Long startTime = System.currentTimeMillis();
        PrimaryIndexBucketGeneratorImpl ibg = new PrimaryIndexBucketGeneratorImpl(FieldDataType.TEXT, buckets);
        PrimaryIndexBucketLocatorImpl locator = new PrimaryIndexBucketLocatorImpl(ibg.generateBuckets(), null, null, null, null, null, null, null, null);
        logger.debug("test to getBucket for the bucket distribution on Greek Strings");
        String file = "src/test/resources/MostCommon10kGreekWords.txt";
        try
        {
            InputStream inStream = new FileInputStream(file);
            InputStreamReader inStreamReader = new InputStreamReader(inStream);
            BufferedReader br = new BufferedReader(inStreamReader);
            try
            {
                String line;
                HashMap<Long, List<Object>> hm = new HashMap<>();
                SummaryStatistics stats = new SummaryStatistics();
                while ((line = br.readLine()) != null)
                {
                    line = line.trim();
                    Long bucketId = locator.getBucket(line, FieldDataType.TEXT);
                    calculate(hm, stats, bucketId, line);
                }
                Long runTime = System.currentTimeMillis() - startTime;
                RunStats runStats = new RunStats("BucketForGreekString", FieldDataType.TEXT, runTime, stats, hm);
                logger.debug(runStats.toString());
                return runStats;
            } finally
            {
                br.close();
                inStream.close();
            }
        } catch (Exception e)
        {
            logger.debug(e.toString(), e);
            throw e;
        }
    }

    /**
     * Test to check the Bucket Distribution for the double values
     *
     * @throws IndexParseFieldException
     */
    public static RunStats delegateGetBucketForDouble(int buckets) throws Exception
    {
        Long startTime = System.currentTimeMillis();
        PrimaryIndexBucketGeneratorImpl ibg = new PrimaryIndexBucketGeneratorImpl(FieldDataType.DOUBLE, buckets);
        PrimaryIndexBucketLocatorImpl locator = new PrimaryIndexBucketLocatorImpl(null, null, ibg.generateBuckets(), null, null, null, null, null, null);
        logger.debug("test to getBucket for the bucket distribution on Double values");
        HashMap<Long, List<Object>> hm = new HashMap<>();
        SummaryStatistics stats = new SummaryStatistics();
        NormalDistribution dist = new NormalDistribution(0, 6);
        Random r = new Random();
        for (int i = 0; i < 20000; i++)
        {
            //double randomDouble = (-1) * Double.MIN_NORMAL + r.nextDouble() * Double.MAX_VALUE * Double.MAX_VALUE;
            double normalRandom = dist.sample();
            //randomly scale a bit more:
            double scaleRandom = Math.random();
            if (scaleRandom < .25)
            {
                normalRandom = normalRandom * 10;
            } else if (scaleRandom < .5)
            {
                normalRandom = normalRandom * 500;
            } else if (scaleRandom < .75)
            {
                normalRandom = normalRandom * 125000;
            } else
            {
                normalRandom = normalRandom * 1250000;
            }
            Long bucketId = locator.getBucket(normalRandom, FieldDataType.DOUBLE);
            calculate(hm, stats, bucketId, normalRandom);
        }
        Long runTime = System.currentTimeMillis() - startTime;
        RunStats runStats = new RunStats("BucketForDouble", FieldDataType.DOUBLE, runTime, stats, hm);
        logger.debug(runStats.toString());
        return runStats;
    }

    public static RunStats delegateGetBucketPerRecentDateForMinute(int buckets, FieldDataType type) throws Exception
    {
        long startTime = System.currentTimeMillis();
        PrimaryIndexBucketGeneratorImpl ibg = new PrimaryIndexBucketGeneratorImpl(type, buckets);
        List<Long> bucketList = ibg.generateBuckets();
        PrimaryIndexBucketLocatorImpl locator = new PrimaryIndexBucketLocatorImpl(null, bucketList, null, null, null, null, null, null, bucketList);
        logger.debug("test to getBucket for the bucket distribution on Date per minute");
        SimpleDateFormat simpleDtFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
        HashMap<Long, List<Object>> hm = new HashMap<>();
        SummaryStatistics stats = new SummaryStatistics();
        String startDate = "2015-06-29T00:00:00.000-06:00";
        Date date = simpleDtFormat.parse(startDate);
        for (int week = 0; week <= 14; week++)
        {
            for (int hourInDay = 0; hourInDay < 24; hourInDay++)
            {
                for (int minute = 0; minute < 60; minute++)
                {
                    Long bucketId = locator.getBucket(startDate, type);
                    calculate(hm, stats, bucketId, startDate);
                    long timeInMillis = date.getTime() + 60000L;
                    date.setTime(timeInMillis);
                    startDate = simpleDtFormat.format(date);
                }
            }
        }
        long runTime = System.currentTimeMillis() - startTime;
        RunStats runStats = new RunStats("BucketPerRecentDateForMinute", type, runTime, stats, hm);
        logger.debug(runStats.toString());
        return runStats;
    }
}
