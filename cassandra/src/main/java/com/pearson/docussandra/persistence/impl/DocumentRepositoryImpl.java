package com.pearson.docussandra.persistence.impl;

import com.datastax.driver.core.BatchStatement;
import java.nio.ByteBuffer;
import java.util.List;

import org.bson.BSON;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.pearson.docussandra.bucketmanagement.PrimaryIndexBucketLocatorImpl;
import com.pearson.docussandra.domain.objects.Document;
import com.pearson.docussandra.domain.objects.DocumentIdentifier;
import com.pearson.docussandra.domain.objects.Identifier;
import com.pearson.docussandra.domain.objects.QueryResponseWrapper;
import com.pearson.docussandra.domain.objects.Table;
import com.pearson.docussandra.exception.DuplicateItemException;
import com.pearson.docussandra.exception.IndexParseException;
import com.pearson.docussandra.exception.InvalidObjectIdException;
import com.pearson.docussandra.exception.ItemNotFoundException;
import com.pearson.docussandra.handler.IndexMaintainerHelper;
import com.pearson.docussandra.persistence.DocumentRepository;
import com.pearson.docussandra.persistence.helper.DocumentPersistanceUtils;
import com.pearson.docussandra.persistence.helper.PreparedStatementFactory;
import com.pearson.docussandra.persistence.parent.AbstractCRUDRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.pearson.docussandra.bucketmanagement.BucketLocator;

/**
 * Repository for interacting with documents.
 *
 * @author https://github.com/JeffreyDeYoung
 */
public class DocumentRepositoryImpl extends AbstractCRUDRepository<Document>
    implements DocumentRepository {

  public class Columns {

    public static final String ID = "id";
    public static final String OBJECT = "object";
    public static final String CREATED_AT = "created_at";
    public static final String UPDATED_AT = "updated_at";
  }

  private static final String EXISTENCE_CQL = "select count(*) from %s where %s = ?";
  private static final String READ_CQL = "select * from %s where %s = ? ORDER BY updated_at DESC";
  private static final String READ_ALL_CQL = "select * from %s LIMIT %d";

  private static final String DELETE_CQL = "delete from %s where %s = ?";
  // private static final String UPDATE_CQL = "update %s set object = ?, updated_at = ? where %s =
  // ?";
  private static final String CREATE_CQL =
      "insert into %s (%s, object, created_at, updated_at) values (?, ?, ?, ?)";

  private final BucketLocator bucketLocator;
  private static Logger logger = LoggerFactory.getLogger(DocumentRepositoryImpl.class);

  /**
   * Constructor.
   *
   * @param session
   */
  public DocumentRepositoryImpl(Session session) {
    super(session);
    this.bucketLocator = PrimaryIndexBucketLocatorImpl.getInstance();
  }

  @Override
  public Document create(Document entity) {
    if (exists(entity.getId()))// This is expensive; any way around this? IF NOT EXISTS doesn't work
                               // in this case
    {
      throw new DuplicateItemException(
          entity.getClass().getSimpleName() + " ID already exists: " + entity.getId().toString());
    }

    Table table = entity.getTable();
    PreparedStatement createStmt = PreparedStatementFactory.getPreparedStatement(
        String.format(CREATE_CQL, table.toDbTable(), Columns.ID), getSession());
    try {
      BoundStatement bs = new BoundStatement(createStmt);
      bindCreate(bs, entity);
      BatchStatement batch = new BatchStatement(BatchStatement.Type.LOGGED);
      batch.add(bs);// the actual create
      List<BoundStatement> indexStatements = IndexMaintainerHelper
          .generateDocumentCreateIndexEntriesStatements(getSession(), entity, bucketLocator);
      for (BoundStatement boundIndexStatement : indexStatements) {
        batch.add(boundIndexStatement);// the index creates
      }
      getSession().execute(batch);
      return entity;
    } catch (IndexParseException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Document read(Identifier identifier) {
    Table table = identifier.getTable();
    PreparedStatement readStmt = PreparedStatementFactory
        .getPreparedStatement(String.format(READ_CQL, table.toDbTable(), Columns.ID), getSession());

    BoundStatement bs = new BoundStatement(readStmt);
    bindIdentifier(bs, identifier);
    Document item = DocumentPersistanceUtils.marshalRow(getSession().execute(bs).one());

    if (item == null) {
      throw new ItemNotFoundException("ID not found: " + identifier.toString());
    }
    // item.setId(identifier);
    item.setTable(table);
    return item;
  }

  @Override
  public QueryResponseWrapper readAll(String database, String tableString, int limit, long offset) {
    Table table = new Table();
    table.setDatabaseByString(database);
    table.setName(tableString);
    long maxIndex = offset + limit;
    PreparedStatement readStmt = PreparedStatementFactory.getPreparedStatement(
        String.format(READ_ALL_CQL, table.toDbTable(), maxIndex + 1), getSession());// we do one
                                                                                    // plus here so
                                                                                    // we know if
                                                                                    // there are
                                                                                    // additional
                                                                                    // results
    BoundStatement bs = new BoundStatement(readStmt);
    // run the query
    ResultSet results = getSession().execute(bs);

    return DocumentPersistanceUtils.parseResultSetWithLimitAndOffset(results, limit, offset);
  }

  @Override
  public Document update(Document entity) {
    Document old = read(entity.getId()); // will throw exception of doc is not found
    entity.setCreatedAt(old.getCreatedAt());// copy over the original create date
    Table table = entity.getTable();
    PreparedStatement updateStmt = PreparedStatementFactory.getPreparedStatement(
        String.format(CREATE_CQL, table.toDbTable(), Columns.ID), getSession());

    BoundStatement bs = new BoundStatement(updateStmt);
    bindCreate(bs, entity);
    BatchStatement batch = new BatchStatement(BatchStatement.Type.LOGGED);
    batch.add(bs);// the actual update
    try {
      List<BoundStatement> indexStatements = IndexMaintainerHelper
          .generateDocumentUpdateIndexEntriesStatements(getSession(), entity, bucketLocator);
      for (BoundStatement boundIndexStatement : indexStatements) {
        batch.add(boundIndexStatement);// the index updates
      }
      getSession().execute(batch);
      return entity;
    } catch (IndexParseException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void delete(Document entity) {
    try {
      Table table = entity.getTable();
      PreparedStatement deleteStmt = PreparedStatementFactory.getPreparedStatement(
          String.format(DELETE_CQL, table.toDbTable(), Columns.ID), getSession());

      BoundStatement bs = new BoundStatement(deleteStmt);
      bindIdentifier(bs, entity.getId());
      BatchStatement batch = new BatchStatement(BatchStatement.Type.LOGGED);
      batch.add(bs);// the actual delete
      try {
        List<BoundStatement> indexStatements = IndexMaintainerHelper
            .generateDocumentDeleteIndexEntriesStatements(getSession(), entity, bucketLocator);
        for (BoundStatement boundIndexStatement : indexStatements) {
          batch.add(boundIndexStatement);// the index deletes
        }
        getSession().execute(batch);
      } catch (IndexParseException e) {
        throw new RuntimeException(e);// this shouldn't actually happen outside of tests
      }
    } catch (InvalidObjectIdException e) {
      throw new ItemNotFoundException("ID not found: " + entity.getId().toString());
    }
  }

  @Override
  public void delete(Identifier id) {
    // ok, this is kinda messed up; we actually need to FETCH the document in
    // order to delete it, otherwise we can't determine what iTables need to
    // be updated
    Document entity = this.read(id);
    try {
      Table table = entity.getTable();
      PreparedStatement deleteStmt = PreparedStatementFactory.getPreparedStatement(
          String.format(DELETE_CQL, table.toDbTable(), Columns.ID), getSession());

      BoundStatement bs = new BoundStatement(deleteStmt);
      bindIdentifier(bs, id);
      BatchStatement batch = new BatchStatement(BatchStatement.Type.LOGGED);
      batch.add(bs);// the actual delete
      try {
        List<BoundStatement> indexStatements = IndexMaintainerHelper
            .generateDocumentDeleteIndexEntriesStatements(getSession(), entity, bucketLocator);
        for (BoundStatement boundIndexStatement : indexStatements) {
          batch.add(boundIndexStatement);// the index deletes
        }
        getSession().execute(batch);
      } catch (IndexParseException e) {
        throw new RuntimeException(e);// this shouldn't actually happen outside of tests
      }
    } catch (InvalidObjectIdException e) {
      throw new ItemNotFoundException("ID not found: " + entity.getId().toString());
    }
  }

  @Override
  public boolean exists(Identifier identifier) {
    if (identifier == null || identifier.isEmpty()) {
      return false;
    }

    Table table = identifier.getTable();
    PreparedStatement existStmt = PreparedStatementFactory.getPreparedStatement(
        String.format(EXISTENCE_CQL, table.toDbTable(), Columns.ID), getSession());

    BoundStatement bs = new BoundStatement(existStmt);
    bindIdentifier(bs, identifier);
    return (getSession().execute(bs).one().getLong(0) > 0);
  }

  @Override
  protected void bindIdentifier(BoundStatement bs, Identifier identifier) {
    DocumentIdentifier docId = new DocumentIdentifier(identifier);
    bs.bind(docId.getUUID());
  }

  private void bindCreate(BoundStatement bs, Document entity) {
    bs.bind(entity.getUuid(), ByteBuffer.wrap(BSON.encode(entity.getObject())),
        entity.getCreatedAt(), entity.getUpdatedAt());
  }

}
