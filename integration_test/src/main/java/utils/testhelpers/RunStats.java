package utils.testhelpers;

import com.pearson.docussandra.bucketmanagement.PrimaryIndexBucketLocatorImpl;
import com.pearson.docussandra.domain.objects.FieldDataType;
import java.util.HashMap;
import java.util.List;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

/**
 * Class for storing the results of a run. Just a test helper class, so javadoc is light.
 */
public class RunStats {

  private long runtime;
  private String testType;
  private FieldDataType dataType;
  private SummaryStatistics stats;
  private HashMap<Long, List<Object>> bucketMap;

  public static final String DE = ",";// delimiter
  public static final String N = "\n";// new line

  private int bucketSize;

  public RunStats() {}

  // public RunStats(String testType, FieldDataType dataType)
  // {
  // this.testType = testType;
  // this.dataType = dataType;
  // retrieveBucketSize(dataType);
  // }
  public RunStats(String testType, FieldDataType dataType, long runtime, SummaryStatistics stats,
      HashMap<Long, List<Object>> bucketMap) {
    this.runtime = runtime;
    this.stats = stats;
    this.testType = testType;
    this.dataType = dataType;
    this.bucketMap = bucketMap;
    retrieveBucketSize(dataType);
  }

  public void retrieveBucketSize(FieldDataType dataType) {
    this.bucketSize =
        PrimaryIndexBucketLocatorImpl.getBUCKETS()[dataType.getIndexForDataType()].size();
  }

  /**
   * @return the runtime
   */
  public long getRuntime() {
    return runtime;
  }

  /**
   * @param runtime the runtime to set
   */
  public void setRuntime(long runtime) {
    this.runtime = runtime;
  }

  /**
   * @return the testType
   */
  public String getTestType() {
    return testType;
  }

  /**
   * @param testType the testType to set
   */
  public void setTestType(String testType) {
    this.testType = testType;
  }

  /**
   * @return the dataType
   */
  public FieldDataType getDataType() {
    return dataType;
  }

  /**
   * @param dataType the dataType to set
   */
  public void setDataType(FieldDataType dataType) {
    this.dataType = dataType;
  }

  /**
   * @return the bucketSize
   */
  public int getBucketSize() {
    return bucketSize;
  }

  @Override
  public String toString() {
    return "RunStats: \n-testType:\t" + testType + ",\n-dataType:\t" + dataType
        + ",\n-bucketSize:\t" + bucketSize + "\n-runtime (ms):\t" + runtime + "\n-buckets used: "
        + bucketMap.size() + "\n-stats: \n" + stats.toString();
  }

  public String toCSVRow() {
    StringBuilder sb = new StringBuilder();
    sb.append(bucketSize).append(DE);
    sb.append(runtime).append(DE);
    sb.append(bucketMap.size()).append(DE);
    sb.append(stats.getN()).append(DE);
    sb.append(stats.getMin()).append(DE);
    sb.append(stats.getMax()).append(DE);
    sb.append(stats.getMean()).append(DE);
    sb.append(stats.getGeometricMean()).append(DE);
    sb.append(stats.getVariance()).append(DE);
    sb.append(stats.getSumsq()).append(DE);
    sb.append(stats.getStandardDeviation()).append(DE);
    sb.append(stats.getSumOfLogs()).append(N);
    return sb.toString();
  }

  public static String getCSVHeader() {
    StringBuilder sb = new StringBuilder();
    sb.append("Bucket Size").append(DE);
    sb.append("Runtime").append(DE);
    sb.append("Buckets Used").append(DE);
    sb.append("Number of Records").append(DE);
    sb.append("Min").append(DE);
    sb.append("Max").append(DE);
    sb.append("Mean").append(DE);
    sb.append("Geo Mean").append(DE);
    sb.append("Variance").append(DE);
    sb.append("Sum of Squares").append(DE);
    sb.append("Standard Deviation").append(DE);
    sb.append("Sum of Logs").append(N);
    return sb.toString();
  }

  public String printDistAsCSV() {
    StringBuilder sb = new StringBuilder();
    for (Long token : bucketMap.keySet()) {
      sb.append(token).append(DE);
      for (Object o : bucketMap.get(token)) {
        sb.append(o).append(DE);
      }
      sb.append(N);
    }
    return sb.toString();
  }

  public String printDistStatsAsCSV() {
    StringBuilder sb = new StringBuilder();
    for (Long token : bucketMap.keySet()) {
      sb.append(token).append(DE);
      sb.append(bucketMap.get(token).size());
      sb.append(N);
    }
    return sb.toString();
  }

  /**
   * @return the stats
   */
  public SummaryStatistics getStats() {
    return stats;
  }

  /**
   * @param stats the stats to set
   */
  public void setStats(SummaryStatistics stats) {
    this.stats = stats;
  }

  /**
   * @return the bucketMap
   */
  public HashMap<Long, List<Object>> getBucketMap() {
    return bucketMap;
  }

  /**
   * @param bucketMap the bucketMap to set
   */
  public void setBucketMap(HashMap<Long, List<Object>> bucketMap) {
    this.bucketMap = bucketMap;
  }

}
