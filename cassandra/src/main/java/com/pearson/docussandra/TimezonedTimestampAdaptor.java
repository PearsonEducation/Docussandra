package com.pearson.docussandra;

import com.strategicgains.util.date.DateAdapter;
import com.strategicgains.util.date.DateAdapterConstants;
import static com.strategicgains.util.date.DateAdapterConstants.DATE_OUTPUT_FORMAT;

/**
 * A timestamp adaptor for DateAdaptorJ that supports timezones.
 *
 * @author https://github.com/JeffreyDeYoung
 */
public class TimezonedTimestampAdaptor extends DateAdapter {

  /**
   * Timestamp formats to use.
   */
  private static String[] timestampFormats;

  public TimezonedTimestampAdaptor() {
    super(DATE_OUTPUT_FORMAT, createTimestampFormats());
  }

  /**
   * Gets the timestamp formats; creates them if they do not already exist.
   *
   * @return
   */
  private static String[] createTimestampFormats() {
    timestampFormats = new String[DateAdapterConstants.TIMESTAMP_INPUT_FORMATS.length
        + DateAdapterConstants.DATE_INPUT_FORMATS.length + 3];
    // note: these are tried in order! start with more specific formats and work your way to broader
    timestampFormats[0] = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";// millisecond with timezone
    timestampFormats[1] = "yyyy-MM-dd'T'HH:mm:ssXXX";// second with timezone
    timestampFormats[2] = "yyyy-MM-dd'T'HH:mmXXX";// minute with timezone
    // can add more here in the future if we want
    System.arraycopy(DateAdapterConstants.TIMESTAMP_INPUT_FORMATS, 0, timestampFormats, 3,
        DateAdapterConstants.TIMESTAMP_INPUT_FORMATS.length);
    System.arraycopy(DateAdapterConstants.DATE_INPUT_FORMATS, 0, timestampFormats,
        DateAdapterConstants.TIMESTAMP_INPUT_FORMATS.length + 3,
        DateAdapterConstants.DATE_INPUT_FORMATS.length);
    return timestampFormats;
  }

  /**
   * Timestamp formats to use.
   *
   * @return the timestampFormats
   */
  public String[] getTimestampFormats() {
    return timestampFormats;
  }
}
