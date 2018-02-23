package com.pearson.docussandra.bucketmanagement;

import CSVGen.BucketCSV;
import com.pearson.docussandra.Utils;
import com.pearson.docussandra.domain.objects.FieldDataType;
import com.pearson.docussandra.exception.IndexParseFieldException;

import java.io.FileNotFoundException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.testhelpers.*;
import static utils.testhelpers.TestUtils.calculate;

/**
 * Class for experimenting with bucketing concepts. Written as a test file, however these methods
 * were written to be run ad-hoc, one at a time to experiment with different bucketing settings.
 * These are not really tests at all -- no asserts. As such, they are all marked as ignored until
 * bucketing changes are needed again.
 *
 * @author https://github.com/JeffreyDeYoung
 */
@Ignore
public class IndexBucketLocatorExperiment {

  private static Logger logger = LoggerFactory.getLogger(IndexBucketLocatorExperiment.class);

  /**
   * Checks that bucket pruning is working as expected.
   */
  @Test
  @Ignore
  public void testBucketPruning() throws Exception {
    // SimpleIndexBucketLocatorImpl locator = new SimpleIndexBucketLocatorImpl(null,
    // Integer.MAX_VALUE/64, null, null, null, null, null, null);
    int numBuckets = Integer.MAX_VALUE / 128;
    RunStats res = delegateGetBucketForFourthousandYearDates(numBuckets);
    Utils.writeFile(res.printDistAsCSV(), "FourthousandYearDatesWith" + numBuckets + "buckets.csv");
    Utils.writeFile(res.printDistStatsAsCSV(),
        "FourthousandYearDatesStatsWith" + numBuckets + "buckets.csv");
    StringBuilder sb = new StringBuilder();
    for (Long l : PrimaryIndexBucketLocatorImpl.getBUCKETS()[FieldDataType.DATE_TIME
        .getIndexForDataType()]) {
      sb.append(l).append(",");
    }
    Utils.writeFile(sb.toString(), "bucketsDate.csv");
    // writeFile(RunStats.getCSVHeader() + csv.toString(), "fourThousandRecentDate.csv");
    // int max = 100000;
    // int by = 5000;
    // StringBuilder csv = new StringBuilder(20 * (max / by));
    // for (int numBuckets = by; numBuckets < max; numBuckets += by)
    // {
    // RunStats res = delegateGetBucketForIntegers(numBuckets);
    // csv.append(res.toCSVRow());
    // if (numBuckets % 10000 == null)
    // {
    // writeFile(res.printDistAsCSV(), "integersWith" + numBuckets + "buckets.csv");
    // writeFile(res.printDistStatsAsCSV(), "integersWithStatsWith" + numBuckets + "buckets.csv");
    // }
    // }
    // writeFile(RunStats.getCSVHeader() + csv.toString(), "integer.csv");
  }

  /**
   * checks the Bucket Distribution for the common Integer values prints out the run time
   *
   * @throws IndexParseFieldException
   */
  @Test
  public void testGetBucketForIntegers() throws Exception {
    int max = 5000000;
    int by = 100000;
    StringBuilder csv = new StringBuilder(20 * (max / by));
    for (int numBuckets = by; numBuckets < max; numBuckets += by) {
      RunStats res = BucketCSV.delegateGetBucketForIntegers(numBuckets);
      csv.append(res.toCSVRow());
      Utils.writeFile(res.printDistAsCSV(), "integersWith" + numBuckets + "buckets.csv");
      Utils.writeFile(res.printDistStatsAsCSV(),
          "integersWithStatsWith" + numBuckets + "buckets.csv");
    }
    Utils.writeFile(RunStats.getCSVHeader() + csv.toString(), "integer.csv");
  }

  /**
   * checks the Bucket Distribution for the common string values prints out runtime,maxBucketValue,
   * avgNumberOfItems, standardDeviation
   *
   * @throws FileNotFoundException
   */
  @Test
  @Ignore
  public void testGetBucketForString() throws FileNotFoundException, Exception {
    int max = 10000000;
    int by = 5000;
    StringBuilder csv = new StringBuilder(20 * (max / by));
    for (int numBuckets = 1; numBuckets < max; numBuckets += by) {
      RunStats res = BucketCSV.delegateGetBucketForString(numBuckets);
      csv.append(res.toCSVRow());
    }
    Utils.writeFile(RunStats.getCSVHeader() + csv.toString(), "string.csv");
  }

  /**
   * Test to check the Bucket Distribution for the common string values
   *
   * @throws FileNotFoundException
   */
  @Test
  @Ignore
  public void testGetBucketForGreekString() throws FileNotFoundException, Exception {
    int max = 100000;
    int by = 25;
    StringBuilder csv = new StringBuilder(20 * (max / by));
    for (int numBuckets = 25; numBuckets < max; numBuckets += by) {
      RunStats res = BucketCSV.delegateGetBucketForGreekString(numBuckets);
      csv.append(res.toCSVRow());
      if (numBuckets % 1000 == 0) {
        Utils.writeFile(res.printDistAsCSV(), "greekStringWith" + numBuckets + "buckets.csv");
        Utils.writeFile(res.printDistStatsAsCSV(),
            "greekStringStatsWith" + numBuckets + "buckets.csv");
      }
    }
    Utils.writeFile(RunStats.getCSVHeader() + csv.toString(), "greekString.csv");
  }

  /**
   * Test to check the Bucket Distribution for the past and future 100 year Date values A random
   * Date value per day is considered prints out runtime,maxBucketValue, avgNumberOfItems,
   * standardDeviation
   *
   * @throws ParseException
   * @throws IndexParseFieldException
   */
  @Test
  @Ignore
  public void testGetBucketForRecentDates() throws Exception {
    int max = 10000000;
    int by = 1000000;
    StringBuilder csv = new StringBuilder(20 * (max / by));
    for (int numBuckets = by; numBuckets < max; numBuckets += by) {
      RunStats res = delegateGetBucketForRecentDates(numBuckets);
      csv.append(res.toCSVRow());
      if (numBuckets % 1000000 == 0) {
        Utils.writeFile(res.printDistAsCSV(), "recentDatesWith" + numBuckets + "buckets.csv");
        Utils.writeFile(res.printDistStatsAsCSV(),
            "recentDatesStatsWith" + numBuckets + "buckets.csv");
      }
    }
    Utils.writeFile(RunStats.getCSVHeader() + csv.toString(), "recentDate.csv");
  }

  /**
   * Test to check the Bucket Distribution for the past and future one week date per minute values
   * prints out runtime,maxBucketValue, avgNumberOfItems, standardDeviation
   *
   * @throws ParseException
   * @throws IndexParseFieldException
   */
  @Test
  @Ignore
  public void testGetBucketPerRecentDateForMinute() throws Exception {
    int max = 10000000;
    int by = 100000;
    StringBuilder csv = new StringBuilder(20 * (max / by));
    for (int numBuckets = by; numBuckets < max; numBuckets += by) {
      RunStats res =
          BucketCSV.delegateGetBucketPerRecentDateForMinute(numBuckets, FieldDataType.DATE_TIME);
      csv.append(res.toCSVRow());
      if (numBuckets % 100000 == 0) {
        Utils.writeFile(res.printDistAsCSV(),
            "recentDateForMinuteWith" + numBuckets + "buckets.csv");
        Utils.writeFile(res.printDistStatsAsCSV(),
            "recentDateStatsForMinuteWith" + numBuckets + "buckets.csv");
      }
    }
    Utils.writeFile(RunStats.getCSVHeader() + csv.toString(), "recentDateForMin.csv");
  }

  /**
   * Test to check the long scaling methodology for dates.
   *
   * @throws ParseException
   * @throws IndexParseFieldException
   */
  @Test
  @Ignore
  public void testConvertDateToBucketingLong() throws Exception {
    SimpleDateFormat simpleDtFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
    SimpleDateFormat shortFormat = new SimpleDateFormat("yyyy-MM-dd");
    String startDate = "0015-06-29T00:00:00.000-06:00";
    Date date = null;
    StringBuilder csv = new StringBuilder(20 * 73000);
    for (int i = 0; i < 1460000; i++) // There are approximately 1460000 days from 0015 year to 4015
                                      // year
    {
      date = simpleDtFormat.parse(startDate);
      final Date TIME = new Date((long) (Math.random() * 86400000l));
      date.setTime(date.getTime() + TIME.getTime());// set the time of day to a random time
      Long bucketingToken = PrimaryIndexBucketLocatorImpl.convertDateToBucketingLong(date);
      csv.append(shortFormat.format(date)).append(",").append(date.getTime()).append(",")
          .append(bucketingToken).append("\n");
      date.setTime(date.getTime() + 86400000l - TIME.getTime()); // add a day (86400000 ms = 1 day)
                                                                 // and subtract the random time we
                                                                 // added
      startDate = simpleDtFormat.format(date.getTime());
    }
    Utils.writeFile(csv.toString(), "dateToLongDistro.csv");
  }

  public RunStats delegateGetBucketForRecentDates(int buckets) throws Exception {
    long startTime = System.currentTimeMillis();
    PrimaryIndexBucketGeneratorImpl ibg =
        new PrimaryIndexBucketGeneratorImpl(FieldDataType.DATE_TIME, buckets);
    PrimaryIndexBucketLocatorImpl locator = new PrimaryIndexBucketLocatorImpl(null,
        ibg.generateBuckets(), null, null, null, null, null, null, null);
    logger.debug("test to getBucket for the bucket distribution");
    SimpleDateFormat simpleDtFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
    // Calendar calendar = Calendar.getInstance();
    String startDate = "1915-06-29T00:00:00.000-06:00";
    HashMap<Long, List<Object>> hm = new HashMap<>();
    SummaryStatistics stats = new SummaryStatistics();
    Date date = null;
    for (int i = 0; i < 73000; i++) {
      date = simpleDtFormat.parse(startDate);
      final Date TIME = new Date((long) (Math.random() * 86400000l));
      date.setTime(date.getTime() + TIME.getTime());// set the time of day to a random time
      Long bucketId = locator.getBucket(startDate, FieldDataType.DATE_TIME);
      calculate(hm, stats, bucketId, startDate);
      date.setTime(date.getTime() + 86400000l - TIME.getTime()); // add a day (86400000 ms = 1 day)
                                                                 // and subtract the random time we
                                                                 // added
      startDate = simpleDtFormat.format(date.getTime());
    }
    long runTime = System.currentTimeMillis() - startTime;
    RunStats runStats =
        new RunStats("BucketForRecentDates", FieldDataType.DATE_TIME, runTime, stats, hm);
    logger.debug(runStats.toString());
    return runStats;
  }

  /**
   * Test to check the Bucket Distribution for the past and future 2000 year Date values A random
   * Date value per day is considered prints out runtime,maxBucketValue, avgNumberOfItems,
   * standardDeviation
   *
   * @throws ParseException
   * @throws IndexParseFieldException
   */
  @Test
  @Ignore
  public void testGetBucketForFourthousandYearDates() throws Exception {
    int max = 100000;
    int by = 10000;
    StringBuilder csv = new StringBuilder(20 * (max / by));
    for (int numBuckets = by; numBuckets < max; numBuckets += by) {
      RunStats res = delegateGetBucketForFourthousandYearDates(numBuckets);
      csv.append(res.toCSVRow());
      if (numBuckets % 10000 == 0) {
        Utils.writeFile(res.printDistAsCSV(),
            "FourthousandYearDatesWith" + numBuckets + "buckets.csv");
        Utils.writeFile(res.printDistStatsAsCSV(),
            "FourthousandYearDatesStatsWith" + numBuckets + "buckets.csv");
      }
    }
    Utils.writeFile(RunStats.getCSVHeader() + csv.toString(), "fourThousandRecentDate.csv");
  }

  /**
   * checks the Bucket Distribution for four thousand year Date values A random Date value per day
   * is considered
   *
   * @throws IndexParseFieldException
   */
  public RunStats delegateGetBucketForFourthousandYearDates(int buckets) throws Exception {
    long startTime = System.currentTimeMillis();
    PrimaryIndexBucketGeneratorImpl ibg =
        new PrimaryIndexBucketGeneratorImpl(FieldDataType.DATE_TIME, buckets);
    PrimaryIndexBucketLocatorImpl locator = new PrimaryIndexBucketLocatorImpl(null,
        ibg.generateBuckets(), null, null, null, null, null, null, null);
    logger.debug("test to getBucket for the bucket distribution on FourthousandYear dates");
    SimpleDateFormat simpleDtFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
    // Calendar calendar = Calendar.getInstance();
    String startDate = "0015-07-14T00:00:00.000-06:00";
    HashMap<Long, List<Object>> hm = new HashMap<>();
    SummaryStatistics stats = new SummaryStatistics();
    Date date = null;
    for (int i = 0; i < 1460000; i++) // There are approximately 1460000 days from 0015 year to 4015
                                      // year
    {
      date = simpleDtFormat.parse(startDate);
      final Date TIME = new Date((long) (Math.random() * 86400000l));
      date.setTime(date.getTime() + TIME.getTime());// set the time of day to a random time
      Long bucketId = locator.getBucket(startDate, FieldDataType.DATE_TIME);
      calculate(hm, stats, bucketId, startDate);
      date.setTime(date.getTime() + 86400000l - TIME.getTime()); // add a day (86400000 ms = 1 day)
                                                                 // and subtract the random time we
                                                                 // added
      startDate = simpleDtFormat.format(date.getTime());
    }
    long runTime = System.currentTimeMillis() - startTime;
    RunStats runStats =
        new RunStats("BucketForFourthousandYearDates", FieldDataType.DATE_TIME, runTime, stats, hm);
    logger.debug(runStats.toString());
    return runStats;
  }

  /**
   * checks the Bucket Distribution for the common English and Greek string values prints out
   * runtime,maxBucketValue, avgNumberOfItems, standardDeviation
   *
   * @throws FileNotFoundException
   */
  @Test
  @Ignore
  public void testGetBucketForGreekAndEnglishString() throws FileNotFoundException, Exception {
    int max = 100000;
    int by = 25;
    StringBuilder csv = new StringBuilder(20 * (max / by));
    for (int numBuckets = by; numBuckets < max; numBuckets += by)// try bucket values from one to
                                                                 // may by the value of "by"
    {
      RunStats res = BucketCSV.delegateGetBucketForGreekAndEnglishString(numBuckets);
      csv.append(res.toCSVRow());
      if (numBuckets % 10000 == 0) {
        Utils.writeFile(res.printDistAsCSV(), "greekAndEnglishWith" + numBuckets + "buckets.csv");
        Utils.writeFile(res.printDistStatsAsCSV(),
            "greekAndEnglishStatsWith" + numBuckets + "buckets.csv");
      }
    }
    Utils.writeFile(RunStats.getCSVHeader() + csv.toString(), "string.csv");
  }

  /**
   * checks the Bucket Distribution for the double values prints out the run time
   *
   * @throws IndexParseFieldException
   */
  @Test
  @Ignore
  public void testGetBucketForDouble() throws Exception {
    int max = 1990000000;
    int by = 1000000;
    StringBuilder csv = new StringBuilder(20 * (max / by));
    for (int numBuckets = by; numBuckets < max; numBuckets += by)// try bucket values from one to
                                                                 // may by the value of "by"
    {
      RunStats res = BucketCSV.delegateGetBucketForDouble(numBuckets);
      csv.append(res.toCSVRow());
      // if (numBuckets % 1000 == 0)
      // {
      Utils.writeFile(res.printDistAsCSV(), "doubleWith" + numBuckets + "buckets.csv");
      Utils.writeFile(res.printDistStatsAsCSV(), "doubleStatsWith" + numBuckets + "buckets.csv");
      // }
    }
    Utils.writeFile(RunStats.getCSVHeader() + csv.toString(), "double.csv");
  }

  /**
   * checks the Bucket Distribution for the UUID values prints out the run time
   *
   * @throws IndexParseFieldException
   */
  @Test
  @Ignore
  public void testGetBucketForUuids() throws Exception {
    int max = 100000000;
    int by = 25;
    StringBuilder csv = new StringBuilder(20 * (max / by));
    for (int numBuckets = 1; numBuckets < max; numBuckets += by)// try bucket values from one to may
                                                                // by the value of "by"
    {
      RunStats res = BucketCSV.delegateGetBucketForUuids(numBuckets);
      csv.append(res.toCSVRow());
    }
    Utils.writeFile(RunStats.getCSVHeader() + csv.toString(), "uuid.csv");
  }

  /**
   * checks the Bucket Distribution for BinaryBase64 Values values prints out
   * runtime,maxBucketValue, avgNumberOfItems, standardDeviation
   *
   * @throws FileNotFoundException
   */
  @Test
  @Ignore
  public void testGetBucketForBinaryBase64() throws FileNotFoundException, Exception {
    int max = 500000;
    int by = 1000;
    StringBuilder csv = new StringBuilder(20 * (max / by));
    for (int numBuckets = by; numBuckets < max; numBuckets += by)// try bucket values from one to
                                                                 // may by the value of "by"
    {
      RunStats res = BucketCSV.delegateGetBucketForBinaryBase64(numBuckets);
      csv.append(res.toCSVRow());
      if (numBuckets % 10000 == 0) {
        Utils.writeFile(res.printDistAsCSV(), "binaryWith" + numBuckets + "buckets.csv");
        Utils.writeFile(res.printDistStatsAsCSV(), "binaryStatsWith" + numBuckets + "buckets.csv");
      }
      // writeFile(res.printDistAsCSV(), "BinaryBase64With" + numBuckets + "buckets.csv");
    }
    Utils.writeFile(RunStats.getCSVHeader() + csv.toString(), "Binary.csv");
  }

  /**
   * checks the Bucket Distribution for the Long values prints out the run time
   *
   * @throws IndexParseFieldException
   */
  @Test
  @Ignore
  public void testGetBucketForLong() throws Exception {
    int max = 10000;
    int by = 1000;
    StringBuilder csv = new StringBuilder(20 * (max / by));
    for (int numBuckets = 1; numBuckets < max; numBuckets += by)// try bucket values from one to may
                                                                // by the value of "by"
    {
      RunStats res = BucketCSV.delegateGetBucketForLong(numBuckets);
      csv.append(res.toCSVRow());
      Utils.writeFile(res.printDistStatsAsCSV(), "LongStatsWith" + numBuckets + "buckets.csv");
    }
    Utils.writeFile(RunStats.getCSVHeader() + csv.toString(), "long.csv");
  }

  /**
   * checks the Bucket Distribution for the Boolean values prints out the run time
   *
   * @throws IndexParseFieldException
   */
  @Test
  @Ignore
  public void testGetBucketForBoolean() throws Exception {
    int max = 100000000;
    int by = 25;
    StringBuilder csv = new StringBuilder(20 * (max / by));
    for (int numBuckets = 1; numBuckets < max; numBuckets += by)// try bucket values from one to may
                                                                // by the value of "by"
    {
      RunStats res = BucketCSV.delegateGetBucketForBoolean(numBuckets);
      csv.append(res.toCSVRow());
    }
    Utils.writeFile(RunStats.getCSVHeader() + csv.toString(), "Boolean.csv");
  }

}
