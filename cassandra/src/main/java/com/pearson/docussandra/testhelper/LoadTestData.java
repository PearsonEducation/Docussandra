
package com.pearson.docussandra.testhelper;

import com.pearson.docussandra.domain.objects.Database;
import com.pearson.docussandra.domain.objects.Document;
import com.pearson.docussandra.domain.objects.Index;
import com.pearson.docussandra.domain.objects.IndexField;
import com.pearson.docussandra.domain.objects.Table;
import com.pearson.docussandra.exception.DuplicateItemException;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Just used to load test data.
 *
 * @author https://github.com/JeffreyDeYoung
 */
public class LoadTestData {

  private static Logger logger = LoggerFactory.getLogger(LoadTestData.class);

  private Fixtures f;

  public LoadTestData() throws Exception {
    f = Fixtures.getInstance(false);// don't mock cassandra here
  }

  public void go() throws Exception {
    System.out.println("Not yet implemented with our current test data.");
    System.exit(0);
  }

  public static void main(String[] args) throws Exception {
    System.out.println("Loading test data...");
    LoadTestData ltd = new LoadTestData();
    ltd.go();

  }

}
