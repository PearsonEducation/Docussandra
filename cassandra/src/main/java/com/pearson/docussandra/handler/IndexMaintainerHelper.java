package com.pearson.docussandra.handler;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.pearson.docussandra.Utils;
import com.pearson.docussandra.cache.CacheFactory;
import com.pearson.docussandra.domain.objects.Document;
import com.pearson.docussandra.domain.objects.Index;
import com.pearson.docussandra.domain.objects.IndexField;
import com.pearson.docussandra.exception.IndexParseException;
import com.pearson.docussandra.exception.IndexParseFieldException;
import com.pearson.docussandra.persistence.impl.DocumentRepositoryImpl;
import com.pearson.docussandra.persistence.impl.IndexRepositoryImpl;
import com.pearson.docussandra.persistence.helper.PreparedStatementFactory;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;
import org.bson.BSON;
import org.bson.BSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.pearson.docussandra.bucketmanagement.BucketLocator;

/**
 * EventHandler for maintaining indices (really just additional tables with the same data) after
 * CRUD events on documents.
 *
 * @author https://github.com/JeffreyDeYoung
 */
public class IndexMaintainerHelper {

  private static Logger logger = LoggerFactory.getLogger(IndexMaintainerHelper.class);

  public static final String ITABLE_INSERT_CQL =
      "INSERT INTO %s (bucket, id, object, created_at, updated_at, %s) VALUES (?, ?, ?, ?, ?, %s);";
  public static final String ITABLE_UPDATE_CQL =
      "UPDATE %s SET object = ?, updated_at = ? WHERE bucket = ? AND id = ? AND %s;";
  public static final String ITABLE_DELETE_CQL = "DELETE FROM %s WHERE bucket = ? AND %s;";

  private IndexMaintainerHelper() {
    // don't instantiate; call static methods only
  }

  public static List<BoundStatement> generateDocumentCreateIndexEntriesStatements(Session session,
      Document entity, BucketLocator bucketLocator) throws IndexParseException {
    // check for any indices that should exist on this setTable per the index setTable
    List<Index> indices = getIndexForDocument(session, entity);
    ArrayList<BoundStatement> statementList = new ArrayList<>(indices.size());
    // for each index
    for (Index index : indices) {
      // add row to the iTable(s)
      BoundStatement bs =
          generateDocumentCreateIndexEntryStatement(session, index, entity, bucketLocator);
      if (bs != null) {
        statementList.add(bs);
      }
    }
    // return a list of commands to accomplish all of this
    return statementList;
  }

  public static BoundStatement generateDocumentCreateIndexEntryStatement(Session session,
      Index index, Document entity, BucketLocator bucketLocator) throws IndexParseException {
    // determine which getFields need to write as PKs
    List<IndexField> fieldsData = index.getFields();
    String finalCQL = getCQLStatementForInsert(index);
    PreparedStatement ps = PreparedStatementFactory.getPreparedStatement(finalCQL, session);
    BoundStatement bs = new BoundStatement(ps);
    // pull the index fieldsData out of the document for binding
    DBObject jsonObject = new BasicDBObject();
    jsonObject.putAll(entity.getObject());
    // set the bucket
    Object fieldToBucketOnObject = jsonObject.get(fieldsData.get(0).getField());// pull the field to
                                                                                // bucket on out of
                                                                                // the document
    if (fieldToBucketOnObject == null) {
      // we do not have an indexable field in our document -- therefore, it shouldn't be added to an
      // index! (right?) -- is this right Todd?
      logger.trace("Warning: document: " + entity.toString()
          + " does not have an indexed field for index: " + index.toString());
      return null;
    }
    Long bucketId;
    try {
      bucketId = bucketLocator.getBucket(fieldToBucketOnObject, fieldsData.get(0).getType());
    } catch (IndexParseFieldException ex) {
      throw new IndexParseException(fieldsData.get(0), ex);
    }
    if (logger.isTraceEnabled()) {
      logger.trace("Bucket ID for entity: " + entity.toString() + "for index: " + index.toString()
          + " is: " + bucketId);
    }
    bs.setLong(0, bucketId);
    // set the id
    bs.setUUID(1, entity.getUuid());
    // set the blob
    BSONObject bson = (BSONObject) entity.getObject();
    bs.setBytes(2, ByteBuffer.wrap(BSON.encode(bson)));
    // set the dates
    bs.setDate(3, entity.getCreatedAt());
    bs.setDate(4, entity.getUpdatedAt());
    for (int i = 0; i < fieldsData.size(); i++) {
      boolean normal = Utils.setField(jsonObject, fieldsData.get(i), bs, i + 5);// offset from the
                                                                                // first five
                                                                                // non-dynamic
                                                                                // getFields
      if (!normal) {
        logger.debug("Unable to create index for null field. For index: " + index.toString());// consider
                                                                                              // reducing
                                                                                              // this
                                                                                              // to
                                                                                              // trace
        // take no action: this document has a null value for a field that
        // was supposed to be indexed, we will simply not create an index
        // entry for this document (for this index; the other indexes
        // should still be created)
        return null;// don't use this batch statement
      }
    }
    return bs;
  }

  public static List<BoundStatement> generateDocumentUpdateIndexEntriesStatements(Session session,
      Document entity, BucketLocator bucketLocator) throws IndexParseException {
    // check for any indices that should exist on this setTable per the index setTable
    List<Index> indices = getIndexForDocument(session, entity);
    ArrayList<BoundStatement> statementList = new ArrayList<>(indices.size());
    // for each index
    for (Index index : indices) {
      // determine which getFields need to use as PKs
      List<IndexField> fields = index.getFields();

      // we need to be able to update indexed fields as well,
      // which will require us to:
      // 1. determine if an indexed field has changed
      BSONObject oldObject = getOldObjectForUpdate(session, entity);
      if (hasIndexedFieldChanged(oldObject, index, entity)) {
        // 2a. if the field has changed, create a new index entry
        BoundStatement createBS =
            generateDocumentCreateIndexEntryStatement(session, index, entity, bucketLocator);
        if (createBS != null) {
          statementList.add(createBS);
        }
        // 2b. after creating the new index entry, we must delete the old one
        BoundStatement deleteBS =
            generateDocumentDeleteIndexEntryStatement(session, index, oldObject, bucketLocator);
        if (deleteBS != null) {
          statementList.add(deleteBS);
        }
      } else {// 3. if an indexed field has not changed, do a normal CQL update
        String finalCQL = getCQLStatementForWhereClauses(ITABLE_UPDATE_CQL, index);
        PreparedStatement ps = PreparedStatementFactory.getPreparedStatement(finalCQL, session);
        BoundStatement updateBS = new BoundStatement(ps);

        // set the blob
        BSONObject bson = (BSONObject) entity.getObject();
        updateBS.setBytes(0, ByteBuffer.wrap(BSON.encode(bson)));
        // set the date
        updateBS.setDate(1, entity.getUpdatedAt());
        // pull the index getFields out of the document for binding
        DBObject jsonObject = (DBObject) entity.getObject();
        // set the bucket
        Object bucketField = jsonObject.get(fields.get(0).getField());
        if (bucketField == null) {// we can't even bucket, there isn't a field for this document to
                                  // index on
          break;
        }
        Long bucketId;
        try {
          bucketId = bucketLocator.getBucket(bucketField, fields.get(0).getType());
        } catch (IndexParseFieldException ex) {
          throw new IndexParseException(fields.get(0), ex);
        }
        if (logger.isTraceEnabled()) {
          logger.trace("Bucket ID for entity: " + entity.toString() + " for index: "
              + index.toString() + " is: " + bucketId);
        }
        updateBS.setLong(2, bucketId);
        updateBS.setUUID(3, entity.getUuid());

        boolean normal = true;
        for (int i = 0; i < fields.size(); i++) {
          normal = Utils.setField(jsonObject, fields.get(i), updateBS, i + 4);// offset from the
                                                                              // first four
                                                                              // non-dynamic
                                                                              // getFields
          if (!normal) {
            logger.debug("Unable to update index for null field. For index: " + index.toString());// consider
                                                                                                  // reducing
                                                                                                  // this
                                                                                                  // to
                                                                                                  // trace
            // take no action; don't try to update this index; just break out of the loop and go
            // onto the next index
            break;
          }
        }
        if (normal) {
          // add row to the iTable(s)
          statementList.add(updateBS);
        }
      }
    }
    // return a list of commands to accomplish all of this
    return statementList;
  }

  public static List<BoundStatement> generateDocumentDeleteIndexEntriesStatements(Session session,
      Document entity, BucketLocator bucketLocator) throws IndexParseException {
    // check for any indices that should exist on this setTable per the index setTable
    List<Index> indices = getIndexForDocument(session, entity);
    ArrayList<BoundStatement> statementList = new ArrayList<>(indices.size());
    // for each index
    for (Index index : indices) {
      BoundStatement bs = generateDocumentDeleteIndexEntryStatement(session, index,
          entity.getObject(), bucketLocator);
      if (bs != null) {
        statementList.add(bs);
      }
    }
    return statementList;
  }

  private static BoundStatement generateDocumentDeleteIndexEntryStatement(Session session,
      Index index, BSONObject docToDeleteJson, BucketLocator bucketLocator)
      throws IndexParseException {
    // determine which getFields need to write as PKs
    List<IndexField> fields = index.getFields();
    String finalCQL = getCQLStatementForWhereClauses(ITABLE_DELETE_CQL, index);
    PreparedStatement ps = PreparedStatementFactory.getPreparedStatement(finalCQL, session);
    BoundStatement bs = new BoundStatement(ps);
    // pull the index getFields out of the document for binding
    DBObject jsonObject = new BasicDBObject();
    jsonObject.putAll(docToDeleteJson);
    Object fieldToBucketOnObject = jsonObject.get(fields.get(0).getField());
    if (fieldToBucketOnObject == null) {
      // we do not have an indexable field in our document -- therefore, it shouldn't need to be
      // removed an index! (right?) -- is this right Todd?
      logger.trace("Warning: document: " + docToDeleteJson
          + " does not have an indexed field for index: " + index.toString());
      return null;
    }
    // set the bucket
    Long bucketId;
    try {
      bucketId = bucketLocator.getBucket(fieldToBucketOnObject, fields.get(0).getType());
    } catch (IndexParseFieldException ex) {
      throw new IndexParseException(fields.get(0), ex);
    }
    if (logger.isTraceEnabled()) {
      logger.trace("Bucket ID for entity: " + docToDeleteJson + " for index: " + index.toString()
          + " is: " + bucketId);
    }
    bs.setLong(0, bucketId);
    for (int i = 0; i < fields.size(); i++) {
      boolean normalField = Utils.setField(jsonObject, fields.get(i), bs, i + 1);
      if (!normalField) {
        logger.debug("Unable to delete index for null field. For index: " + index.toString());// consider
                                                                                              // reducing
                                                                                              // this
                                                                                              // to
                                                                                              // trace
        // take no action: this document has a null value for a field that
        // was supposed to be indexed, we will simply not delete an index
        // entry for this document (for this index; the other indexes
        // should still be deleted)
        return null;
      }
    }
    return bs;
  }

  /**
   * Gets all the indexes that a document is or needs to be stored in. Note that this actually makes
   * a database call.
   *
   * @param session Cassandra session for interacting with the database.
   * @param entity Document that we are trying to determine which indices it is or should be stored
   *        in.
   * @return A list of Index objects where the document is or should be stored in.
   */
  public static List<Index> getIndexForDocument(Session session, Document entity) {
    IndexRepositoryImpl indexRepo = new IndexRepositoryImpl(session);
    return indexRepo.readAllCached(entity.getId());
  }

  /**
   * Determines if an indexed field has changed as part of an update. This would be private but
   * keeping public for ease of testing.
   *
   * @param oldObject the old BSON object.
   * @param index Index containing the getFields to check for changes.
   * @param entity New version of a document.
   * @return True if an indexed field has changed. False if there is no change of indexed getFields.
   */
  public static boolean hasIndexedFieldChanged(BSONObject oldObject, Index index, Document entity) {
    // DocumentRepository docRepo = new DocumentRepositoryImpl(session);
    BSONObject newObject = entity.getObject();
    // BSONObject oldObject = (BSONObject) JSON.parse(docRepo.read(entity.getId()).object());
    for (IndexField indexField : index.getFields()) {
      String field = indexField.getField();
      if (newObject.get(field) == null && oldObject.get(field) == null)// this shouldn't happen?
      {// if there is not a field in either index
        return false;// if it's not in ether doc, it couldn't have changed
      } else if (newObject.get(field) == null || oldObject.get(field) == null) {// there is a field
                                                                                // in one of the
                                                                                // indexes, but not
                                                                                // the other.
        return true;// the index field must have changed, it either went from missing to present or
                    // present to missing.
      }
      if (!newObject.get(field).equals(oldObject.get(field))) {
        return true;// fail early
      }
    }
    return false;
  }

  // only public for testing
  public static BSONObject getOldObjectForUpdate(Session session, Document entity) {
    DocumentRepositoryImpl docRepo = new DocumentRepositoryImpl(session);
    return docRepo.read(entity.getId()).getObject();
  }

  /**
   * Helper for generating insert CQL statements for iTables. This would be private but keeping
   * public for ease of testing. Same as generateCQLStatementForInsert but will retrieve from cache
   * if available.
   *
   * @param index Index to generate the statement for.
   * @return CQL statement.
   */
  public static String getCQLStatementForInsert(Index index) {
    String key = index.getDatabaseName() + ":" + index.getTableName() + ":" + index.getName();
    Cache iTableCQLCache = CacheFactory.getCache("iTableInsertCQL");
    // synchronized (CacheSynchronizer.getLockingObject(key, "iTableInsertCQL"))
    // {
    Element e = iTableCQLCache.get(key);
    if (e == null || e.getObjectValue() == null)// if its not set, or set, but null, re-read
    {
      // not cached; let's create it
      e = new Element(key, generateCQLStatementForInsert(index));// save it back to the cache
      iTableCQLCache.put(e);
    } else {
      logger.trace("Pulling iTableInsertCQL from Cache: " + e.getObjectValue().toString());
    }
    return (String) e.getObjectValue();
    // }
  }

  /**
   * Helper for generating insert CQL statements for iTables. This would be private but keeping
   * public for ease of testing.
   *
   * @param index Index to generate the statement for.
   * @return CQL statement.
   */
  public static String generateCQLStatementForInsert(Index index) {
    // determine which iTables need to be written to
    String iTableToUpdate = Utils.calculateITableName(index);
    // determine which getFields need to write as PKs
    List<String> fields = index.getFieldsValues();
    String fieldNamesInsertSyntax = Utils.join(", ", fields);
    // calculate the number of '?'s we need to append on the values clause
    StringBuilder fieldValueInsertSyntax = new StringBuilder();
    for (int i = 0; i < fields.size(); i++) {
      if (i != 0) {
        fieldValueInsertSyntax.append(", ");
      }
      fieldValueInsertSyntax.append("?");
    }
    // create final CQL statement for adding a row to an iTable(s)
    return String.format(ITABLE_INSERT_CQL, iTableToUpdate, fieldNamesInsertSyntax,
        fieldValueInsertSyntax);
  }

  /**
   * Helper for generating update CQL statements for iTables. This would be private but keeping
   * public for ease of testing. Same as generateCQLStatementForWhereClauses but will retrieve from
   * cache when available.
   *
   * @param CQL statement that is not yet formatted.
   * @param index Index to generate the statement for.
   * @return CQL statement.
   */
  public static String getCQLStatementForWhereClauses(String CQL, Index index) {
    String key = index.getDatabaseName() + ":" + index.getTableName() + ":" + index.getName();
    String whereClause;
    String iTableToUpdate = Utils.calculateITableName(index);
    Cache whereCache = CacheFactory.getCache("iTableWhere");
    // synchronized (CacheSynchronizer.getLockingObject(key, "iTableWhere"))
    // {
    Element e = whereCache.get(key);
    if (e == null || e.getObjectValue() == null)// if its not set, or set, but null, re-read
    {
      // not cached; let's create it
      e = new Element(key, getWhereClauseHelper(index));// save it back to the cache
      whereCache.put(e);
    } else {
      logger.trace("Pulling WHERE statement info from Cache: " + e.getObjectValue().toString());
    }
    whereClause = (String) e.getObjectValue();
    // }
    // create final CQL statement for updating a row in an iTable(s)
    return String.format(CQL, iTableToUpdate, whereClause);
  }

  /**
   * Helper for generating update CQL statements for iTables. This would be private but keeping
   * public for ease of testing.
   *
   * @param CQL statement that is not yet formatted.
   * @param index Index to generate the statement for.
   * @return CQL statement.
   */
  public static String generateCQLStatementForWhereClauses(String CQL, Index index) {
    // determine which iTables need to be updated
    String iTableToUpdate = Utils.calculateITableName(index);
    // create final CQL statement for updating a row in an iTable(s)
    return String.format(CQL, iTableToUpdate, getWhereClauseHelper(index));
  }

  private static String getWhereClauseHelper(Index index) {
    // determine which getFields need to write as PKs
    List<IndexField> fields = index.getFields();
    // determine the where clause
    StringBuilder setValues = new StringBuilder();
    for (int i = 0; i < fields.size(); i++) {
      String field = fields.get(i).getField();
      if (i != 0) {
        setValues.append(" AND ");
      }
      setValues.append(field).append(" = ?");
    }
    return setValues.toString();
  }

}
