
package com.pearson.docussandra.domain;

import com.pearson.docussandra.Utils;
import com.pearson.docussandra.domain.objects.Index;
import com.pearson.docussandra.domain.objects.Query;
import com.pearson.docussandra.domain.objects.WhereClause;
import java.util.Objects;

/**
 * Object that represents a query that has been parsed for processing.
 *
 * @author https://github.com/JeffreyDeYoung
 */
public class ParsedQuery {

  /**
   * Original query that was passed in.
   */
  private Query query;

  /**
   * Parsed where clause for this query.
   */
  private WhereClause whereClause;

  /**
   * Index for this query.
   */
  private Index index;

  /**
   * The iTable (index table) that needs to be queried in order to retrieve results.
   */
  private String iTable;

  /**
   * Constructor.
   *
   * @param query Original query that was passed in.
   * @param whereClause Parsed where clause for this query.
   * @param index The index that needs to be queried in order to retrieve results.
   */
  public ParsedQuery(Query query, WhereClause whereClause, Index index) {
    this.query = query;
    this.whereClause = whereClause;
    this.index = index;
    this.iTable = Utils.calculateITableName(index);
  }

  /**
   * Original query that was passed in.
   *
   * @return
   */
  public Query getQuery() {
    return query;
  }

  /**
   * Parsed where clause for this query.
   *
   * @return
   */
  public WhereClause getWhereClause() {
    return whereClause;
  }

  /**
   * The iTable (index table) that needs to be queried in order to retrieve results.
   *
   * @return
   */
  public String getITable() {
    return iTable;
  }

  @Override
  public int hashCode() {
    int hash = 5;
    hash = 17 * hash + Objects.hashCode(this.query);
    hash = 17 * hash + Objects.hashCode(this.whereClause);
    hash = 17 * hash + Objects.hashCode(this.iTable);
    return hash;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final ParsedQuery other = (ParsedQuery) obj;
    if (!Objects.equals(this.query, other.query)) {
      return false;
    }
    if (!Objects.equals(this.whereClause, other.whereClause)) {
      return false;
    }
    if (!Objects.equals(this.iTable, other.iTable)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return "ParsedQuery{" + "query=" + query + ", whereClause=" + whereClause + ", iTable=" + iTable
        + '}';
  }

  /**
   * Index for this query.
   *
   * @return the index
   */
  public Index getIndex() {
    return index;
  }

}
