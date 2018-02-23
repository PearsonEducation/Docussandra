package com.pearson.docussandra.persistence.impl;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.pearson.docussandra.Utils;
import com.pearson.docussandra.domain.objects.Index;
import com.pearson.docussandra.domain.objects.IndexField;
import com.pearson.docussandra.domain.objects.IndexIdentifier;
import com.pearson.docussandra.persistence.ITableRepository;
import com.pearson.docussandra.persistence.helper.PreparedStatementFactory;
import java.util.Iterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Data Access Object for manipulating iTables (tables that contain the actual index data). This is
 * a normal DAO instead of part of the persistence framework due to the dynamic nature of creating
 * these tables.
 *
 * @author https://github.com/JeffreyDeYoung
 * @since Feb 12, 2015
 */
public class ITableRepositoryImpl implements ITableRepository {

  /**
   * Session for interacting with the Cassandra database.
   */
  private Session session;

  /**
   * Logger for this class.
   */
  private Logger logger = LoggerFactory.getLogger(this.getClass());

  /**
   * CQL statement for determining if a iTable exists.
   */
  private static final String TABLE_EXISTENCE_CQL =
      "select columnfamily_name from system.schema_columnfamilies where columnfamily_name = ? ALLOW FILTERING;";

  /**
   * CQL statement for dynamically creating an iTable.
   */
  private static final String TABLE_CREATE_CQL =
      "CREATE TABLE IF NOT EXISTS %s (bucket bigint, id uuid, object blob, created_at timestamp, updated_at timestamp, %s, PRIMARY KEY ((bucket), %s));";

  /**
   * CQL statement for deleting an iTable (or for that matter, any setTable).
   */
  private static final String TABLE_DELETE_CQL = "DROP TABLE %s;";

  /**
   * Constructor. Creates a new ITableDao.
   *
   * @param session Session for interacting with the Cassandra database.
   */
  public ITableRepositoryImpl(Session session) {
    this.session = session;
  }

  /**
   * Checks to see if an iTable exists for the specified index.
   *
   * @param index Index that you want to check if it has a corresponding iTable.
   * @return True if the iTable exists for the index, false otherwise.
   */
  @Override
  public boolean iTableExists(Index index) {
    logger.info("Checking for existance of iTable for index: " + index.toString());
    return iTableExists(Utils.calculateITableName(index));
  }

  /**
   * Checks to see if an iTable exists for the specified index.
   *
   * @param indexId Index Id that you want to check if it has a corresponding iTable.
   * @return True if the iTable exists for the index, false otherwise.
   */
  @Override
  public boolean iTableExists(IndexIdentifier indexId) {
    String iTableName = Utils.calculateITableName(indexId);
    logger.info("Checking for existance of iTable: " + iTableName);
    return iTableExists(iTableName);
  }

  private boolean iTableExists(String iTableName) {
    PreparedStatement createStmt =
        PreparedStatementFactory.getPreparedStatement(TABLE_EXISTENCE_CQL, session);
    BoundStatement bs = new BoundStatement(createStmt);
    bs.bind(iTableName);
    ResultSet rs = session.execute(bs);
    Iterator ite = rs.iterator();
    while (ite.hasNext()) {
      logger.debug(ite.next().toString());
      return true;
    }
    return false;
  }

  /**
   * Creates an iTable for the specified index.
   *
   * @param index Index that needs an iTable created for it.
   */
  @Override
  public void createITable(Index index) {
    logger.info("Creating iTable for index: " + index.toString());
    PreparedStatement createStmt =
        PreparedStatementFactory.getPreparedStatement(generateTableCreationSyntax(index), session);
    BoundStatement bs = new BoundStatement(createStmt);
    session.execute(bs);
  }

  /**
   * Dynamically generates a setTable creation command for an iTable based on an index. This would
   * be private, the only reason it is protected is for testing.
   *
   * @param index Index that needs an iTable generated for it.
   * @return A CQL setTable creation command that will create the specified iTable.
   */
  protected String generateTableCreationSyntax(Index index) {
    String newTableName = Utils.calculateITableName(index);
    StringBuilder fieldCreateStatement = new StringBuilder();
    StringBuilder primaryKeyCreateStatement = new StringBuilder();
    // if (!index.isUnique()) {
    // primaryKeyCreateStatement.append("(id), ");//if the index is not unique, set the pk to
    // include the id
    // }
    boolean first = true;

    for (IndexField field : index.getFields()) {
      if (!first) {
        fieldCreateStatement.append(", ");
        primaryKeyCreateStatement.append(", ");
      } else {
        first = false;
      }
      fieldCreateStatement.append(field.getField()).append(" ")
          .append(field.getType().mapToCassandaraDataType());
      primaryKeyCreateStatement.append(field.getField());
    }
    if (!index.isUnique()) {
      primaryKeyCreateStatement.append(", ").append("id");
    }
    String finalStatement = String.format(TABLE_CREATE_CQL, newTableName, fieldCreateStatement,
        primaryKeyCreateStatement);
    logger.debug("For index: " + index.toString() + ", the table create SQL is: " + finalStatement);
    return finalStatement;
  }

  /**
   * Deletes an iTable
   *
   * @param index index whose iTable should be deleted
   */
  @Override
  public void deleteITable(Index index) {
    String tableToDelete = Utils.calculateITableName(index);
    deleteITable(tableToDelete);
  }

  /**
   * Deletes an iTable
   *
   * @param indexId index id whose iTable should be deleted
   */
  @Override
  public void deleteITable(IndexIdentifier indexId) {
    String tableToDelete = Utils.calculateITableName(indexId);
    deleteITable(tableToDelete);
  }

  /**
   * Deletes an iTable
   *
   * @param tableName iTable getIndexName to delete.
   */
  @Override
  public void deleteITable(String tableName) {
    logger.info("Deleting iTable: " + tableName);
    String stmt = String.format(TABLE_DELETE_CQL, tableName);
    PreparedStatement createStmt = PreparedStatementFactory.getPreparedStatement(stmt, session);
    BoundStatement bs = new BoundStatement(createStmt);
    session.execute(bs);
  }

}
