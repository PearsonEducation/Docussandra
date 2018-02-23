package utils.testhelpers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

/**
 *
 * @author https://github.com/JeffreyDeYoung
 */
public class TestUtils {

  // helper method

  public static void calculate(HashMap<Long, List<Object>> hm, SummaryStatistics stats,
      long bucketId, Object value) {
    if (hm.containsKey(bucketId)) {
      List<Object> currentList = hm.get(bucketId);
      currentList.add(value);
      hm.put(bucketId, currentList);
    } else {
      List<Object> newList = new ArrayList<>(1);
      newList.add(value);
      hm.put(bucketId, newList);
    }
    stats.addValue((double) bucketId);
  }
}
