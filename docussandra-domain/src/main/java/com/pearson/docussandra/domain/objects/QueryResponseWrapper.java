package com.pearson.docussandra.domain.objects;

import java.util.ArrayList;
import java.util.List;

/**
 * Wrapper for returning queries. Contains metadata about the response in addition to the actual
 * response.
 *
 * @author https://github.com/JeffreyDeYoung
 */
public class QueryResponseWrapper extends ArrayList<Document> {

  /**
   * Number of additional results that exist. Null if there are additional results, but the number
   * is unknown.
   */
  private final Long numAdditionalResults;

  /**
   * Default constructor for JSON deserializing.
   */
  public QueryResponseWrapper() {
    this.numAdditionalResults = null;
  }

  /**
   * Constructor.
   *
   * @param responseData The actual response data.
   * @param numAdditionalResults Number of additional results that exist. Null if there are
   *        additional results, but the number is unknown.
   */
  public QueryResponseWrapper(List<Document> responseData, Long numAdditionalResults) {
    super(responseData);
    this.numAdditionalResults = numAdditionalResults;
  }

  /**
   * Number of additional results that exist. Null if there are additional results, but the number
   * is unknown.
   *
   * @return the numAdditionalResults
   */
  public Long getNumAdditionalResults() {
    return numAdditionalResults;
  }

}
