package com.pearson.docussandra.domain.objects;

import java.util.List;

/**
 * Domain object that represents a query against Docussandra.
 *
 * @author Jeffrey DeYoung
 */
public class Query {

  /**
   * Default limit for the number of records to return. This is likely overridden elsewhere.
   */
  private static int DEFAULT_LIMIT = 100;

  /**
   * Database to query against.
   */
  private DatabaseReference database;
  // public Map<String, String> variables;
  // @Required("Table name")

  /**
   * Table to query against.
   */
  private TableReference tables;

  /**
   * Columns to query on.
   */
  // @Required("Columns")
  private List<String> columns;// TODO: https://github.com/PearsonEducation/Docussandra/issues/12

  /**
   * Where clause of the Query.
   */
  private String where;

  /**
   * Set our limit to the default until it is overridden.
   */
  private int limit = DEFAULT_LIMIT;
  /**
   * Offset index to start returning the results from.
   */
  private int offset;

  /**
   * Default constructor (needed for automatic serialization).
   */
  public Query() {}

  /**
   * @return the DEFAULT_LIMIT
   */
  public static int getDEFAULT_LIMIT() {
    return DEFAULT_LIMIT;
  }

  /**
   * @param aDEFAULT_LIMIT the DEFAULT_LIMIT to set
   */
  public static void setDEFAULT_LIMIT(int aDEFAULT_LIMIT) {
    DEFAULT_LIMIT = aDEFAULT_LIMIT;
  }

  /**
   * Creates a new database reference with the name provided
   *
   * @param name Database to associate with this query.
   */
  public void setDatabase(String name) {
    this.database = new DatabaseReference(name);
  }

  /**
   * get the database name
   *
   */
  public String getDatabase() {
    return (this.getDatabaseAsObject().getName());
  }

  /**
   * get database details as an object
   *
   */
  public Database getDatabaseAsObject() {
    return database.asObject();
  }

  /**
   * Get table details as an object
   *
   * @return Table object for this query.
   */
  public Table getTableAsObject() {
    return tables.asObject();
  }

  /**
   * @return the table
   */
  public String getTable() {
    return tables.name();
  }

  /**
   * @param table the table to set
   */
  public void setTable(String table) {
    this.tables = new TableReference(getDatabase(), table);
  }

  /**
   * @return the columns
   */
  public List<String> getColumns() {
    return columns;
  }

  /**
   * @param columns the columns to set
   */
  public void setColumns(List<String> columns) {
    this.columns = columns;
  }

  /**
   * @return the where
   */
  public String getWhere() {
    return where;
  }

  /**
   * @param where the where to set
   */
  public void setWhere(String where) {
    this.where = where;
  }

  /**
   * @return the limit
   */
  public int getLimit() {
    return limit;
  }

  /**
   * @param limit the limit to set
   */
  public void setLimit(int limit) {
    this.limit = limit;
  }

  /**
   * @return the offset
   */
  public int getOffset() {
    return offset;
  }

  /**
   * @param offset the offset to set
   */
  public void setOffset(int offset) {
    this.offset = offset;
  }

  @Override
  public String toString() {
    return "Query{" + "table=" + tables + ", columns=" + columns + ", where=" + where + ", limit="
        + limit + ", offset=" + offset + '}';
  }

}
