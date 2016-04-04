package com.pearson.docussandra.persistence.impl;

import com.datastax.driver.core.BatchStatement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.pearson.docussandra.domain.objects.Identifier;
import com.pearson.docussandra.domain.objects.Index;
import com.pearson.docussandra.domain.objects.UUIDIdentifier;
import com.pearson.docussandra.event.IndexCreatedEvent;
import com.pearson.docussandra.exception.ItemNotFoundException;
import com.pearson.docussandra.persistence.IndexStatusRepository;
import com.pearson.docussandra.persistence.helper.PreparedStatementFactory;
import com.pearson.docussandra.persistence.parent.AbstractCRUDRepository;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Repository for interacting with the sys_idx_status and sys_idx_not_done
 * tables.
 *
 * @author https://github.com/JeffreyDeYoung
 */
public class IndexStatusRepositoryImpl extends AbstractCRUDRepository<IndexCreatedEvent> implements IndexStatusRepository
{

    /**
     * Logger for this class.
     */
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * IndexRepositoryImpl to use for lookups.
     */
    private IndexRepositoryImpl indexRepo;

    /**
     * Class that defines the Cassandra tables that this repository manages.
     */
    public class Tables
    {

        public static final String BY_ID = "sys_idx_status";
        public static final String BY_NOT_DONE = "sys_idx_not_done";
    }

    /**
     * Class that defines the database columns that this repository manages.
     */
    private class Columns
    {

        static final String ID = "id";
        static final String DATABASE = "db_name";
        static final String TABLE = "tbl_name";
        static final String INDEX_NAME = "index_name";
        static final String TOTAL_RECORDS = "total_records";
        static final String RECORDS_COMPLETED = "records_completed";
        static final String STARTED_AT = "started_at";
        static final String UPDATED_AT = "updated_at";
        static final String FATAL_ERROR = "fatal_error";
        static final String ERRORS = "errors";
    }

    private static final String IDENTITY_CQL = " where id = ?";
    private static final String EXISTENCE_CQL = "select count(*) from " + Tables.BY_ID + IDENTITY_CQL;
    private static final String DELETE_FROM_NOT_DONE = "delete from " + Tables.BY_NOT_DONE + IDENTITY_CQL;
    private static final String CREATE_CQL = "insert into " + Tables.BY_ID + " (" + Columns.ID + ", " + Columns.DATABASE + ", " + Columns.TABLE + ", " + Columns.INDEX_NAME + ", " + Columns.RECORDS_COMPLETED + ", " + Columns.TOTAL_RECORDS + ", " + Columns.STARTED_AT + ", " + Columns.UPDATED_AT + ", " + Columns.ERRORS + ", " + Columns.FATAL_ERROR + ") values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS";
    private static final String READ_CQL = "select * from " + Tables.BY_ID + IDENTITY_CQL;
    private static final String UPDATE_CQL = "update " + Tables.BY_ID + " set " + Columns.RECORDS_COMPLETED + " = ?, " + Columns.UPDATED_AT + " = ?, " + Columns.ERRORS + " = ?, " + Columns.FATAL_ERROR + " = ?" + IDENTITY_CQL;
    private static final String MARK_INDEXING_CQL = "insert into " + Tables.BY_NOT_DONE + "(" + Columns.ID + ") values (?) IF NOT EXISTS";

    private static final String READ_ALL_CQL = "select * from " + Tables.BY_ID;
    private static final String READ_ALL_CURRENTLY_INDEXING_CQL = "select * from " + Tables.BY_NOT_DONE;
    private static final String IS_CURRENTLY_INDEXING_CQL = "select count(*) from " + Tables.BY_NOT_DONE + IDENTITY_CQL;//records that are currently indexing (not yet done)

    private PreparedStatement existStmt;
    private PreparedStatement readStmt;
    private PreparedStatement createStmt;
    private PreparedStatement updateStmt;
    private PreparedStatement readAllStmt;
    private PreparedStatement markIndexingStmt;
    private PreparedStatement readAllCurrentlyIndexingStmt;
    private PreparedStatement deleteFromNotDoneStmt;
    private PreparedStatement isCurrentlyIndexingStmt;

    /**
     * Constructor.
     *
     * @param session
     */
    public IndexStatusRepositoryImpl(Session session)
    {
        super(session, Tables.BY_ID);
        initialize();
        indexRepo = new IndexRepositoryImpl(session);
    }

    /**
     * Sets up our prepared statements for this repository.
     */
    protected void initialize()
    {
        existStmt = PreparedStatementFactory.getPreparedStatement(EXISTENCE_CQL, getSession());
        readStmt = PreparedStatementFactory.getPreparedStatement(READ_CQL, getSession());
        createStmt = PreparedStatementFactory.getPreparedStatement(CREATE_CQL, getSession());
        updateStmt = PreparedStatementFactory.getPreparedStatement(UPDATE_CQL, getSession());
        readAllStmt = PreparedStatementFactory.getPreparedStatement(READ_ALL_CQL, getSession());
        readAllCurrentlyIndexingStmt = PreparedStatementFactory.getPreparedStatement(READ_ALL_CURRENTLY_INDEXING_CQL, getSession());
        deleteFromNotDoneStmt = PreparedStatementFactory.getPreparedStatement(DELETE_FROM_NOT_DONE, getSession());
        markIndexingStmt = PreparedStatementFactory.getPreparedStatement(MARK_INDEXING_CQL, getSession());
        isCurrentlyIndexingStmt = PreparedStatementFactory.getPreparedStatement(IS_CURRENTLY_INDEXING_CQL, getSession());
    }

    /**
     * Checks to see if an IndexCreatedEvent exists by UUID.
     *
     * @param uuid Index status UUID to check for existence.
     * @return True if the index status exists, false if it does not.
     */
    @Override
    public boolean exists(UUID uuid)
    {
        if (uuid == null)
        {
            return false;
        }
        BoundStatement bs = new BoundStatement(existStmt);
        bindUUIDWhere(bs, uuid);
        return (getSession().execute(bs).one().getLong(0) > 0);
    }

    @Override
    public boolean exists(Identifier id)
    {
        UUIDIdentifier uuidId = (UUIDIdentifier) id;
        return exists(uuidId.getUUID());
    }

    /**
     * Reads an IndexCreatedEvent by UUID.
     *
     * @param uuid UUID to fetch.
     * @return an IndexCreatedEvent for that UUID.
     */
    @Override
    public IndexCreatedEvent read(UUID uuid)
    {
        if (uuid == null)
        {
            return null;
        }
        BoundStatement bs = new BoundStatement(readStmt);
        bindUUIDWhere(bs, uuid);
        return marshalRow(getSession().execute(bs).one());
    }

    @Override
    public IndexCreatedEvent read(Identifier id)
    {
        UUIDIdentifier uuidId = (UUIDIdentifier) id;
        return read(uuidId.getUUID());
    }

    /**
     * Creates an index status in the db.
     *
     * @param entity IndexCreatedEvent to store as a status in the DB.
     * @return The created IndexCreatedEvent.
     */
    @Override
    public IndexCreatedEvent create(IndexCreatedEvent entity)
    {
        BoundStatement create = new BoundStatement(createStmt);
        bindCreate(create, entity);
        BatchStatement batch = new BatchStatement(BatchStatement.Type.LOGGED);
        if (!entity.isDoneIndexing())
        {
            markIndexing(entity.getUuid());
        }
        batch.add(create);
        getSession().execute(batch);
        return entity;
    }

    /**
     * Updates the status for an IndexCreatedEvent in the database. Take note:
     * only a few getFields are updatable: numberOfRecordsCompleted,
     * statusLastUpdatedAt, and error. This is a logical decision; there should
     * not be a reason to update any other getFields. This will also mark the
     * record as done indexing or not as appropriate.
     *
     * @param entity The IndexCreatedEvent to update with the proper getFields
     * set.
     * @return The updated IndexCreatedEvent.
     */
    @Override
    public IndexCreatedEvent update(IndexCreatedEvent entity)
    {
        BoundStatement bs = new BoundStatement(updateStmt);
        bindUpdate(bs, entity);
        if (entity.isDoneIndexing())
        {
            markDone(entity.getUuid());
        } else
        {
            markIndexing(entity.getUuid());
        }
        getSession().execute(bs);
        return entity;
    }

    @Override
    public void delete(Identifier id)
    {
        throw new UnsupportedOperationException("Not a valid call for this class. IndexStatuses cannot be deleted.");
    }

    @Override
    public void delete(IndexCreatedEvent entity)
    {
        throw new UnsupportedOperationException("Not a valid call for this class. IndexStatuses cannot be deleted.");
    }

    /**
     * Marks a index as currently indexing.
     *
     * @param id
     */
    private void markIndexing(UUID id)
    {
        BoundStatement activeStatement = new BoundStatement(markIndexingStmt);
        activeStatement.bind(id);
        getSession().execute(activeStatement);
    }

    /**
     * Marks an index as done indexing.
     *
     * @param id
     */
    private void markDone(UUID id)
    {
        BoundStatement delete = new BoundStatement(deleteFromNotDoneStmt);
        bindUUIDWhere(delete, id);
        getSession().execute(delete);
    }

    /**
     * Reads all IndexCreatedEvents. This is quite possibly a very expensive
     * operation; use with care.
     *
     * @return All IndexCreatedEvents that the database has a record of.
     */
    @Override
    public List<IndexCreatedEvent> readAll()
    {
        BoundStatement bs = new BoundStatement(readAllStmt);
        return (marshalAll(getSession().execute(bs)));
    }

    @Override
    public List<IndexCreatedEvent> readAll(Identifier id)
    {
        throw new UnsupportedOperationException("Not valid for this class.");
    }

    /**
     * Gets all IndexCreatedEvents that are currently indexing. This method is
     * preferred to readAll(). This provides a sense of the indexing load on the
     * database.
     *
     * @return All currently indexing IndexCreatedEvents.
     */
    @Override
    public List<IndexCreatedEvent> readAllCurrentlyIndexing()
    {
        BoundStatement bs = new BoundStatement(readAllCurrentlyIndexingStmt);
        List<UUID> ids = marshalActiveUUIDs(getSession().execute(bs));
        List<IndexCreatedEvent> toReturn = new ArrayList<>(ids.size());
        for (UUID id : ids)
        {
            toReturn.add(read(id));
        }
        return toReturn;
    }

    /**
     * Determines if a index is currently indexing or not. Not presently used,
     * but the method should work.
     *
     * @param id
     * @return
     */
    private boolean isCurrentlyIndexing(UUID id)
    {
        BoundStatement bs = new BoundStatement(isCurrentlyIndexingStmt);
        bindUUIDWhere(bs, id);
        return (getSession().execute(bs).one().getLong(0) > 0);
    }

//    public long countAll(String database, String table)
//    {
//        BoundStatement bs = new BoundStatement(readAllCountStmt);
//        bs.bind(database, table);
//        return (getSession().execute(bs).one().getLong(0));
//    }
    private void bindCreate(BoundStatement bs, IndexCreatedEvent entity)
    {
        bs.bind(entity.getUuid(),
                entity.getIndex().getDatabaseName(),
                entity.getIndex().getTableName(),
                entity.getIndex().getName(),
                entity.getRecordsCompleted(),
                entity.getTotalRecords(),
                entity.getDateStarted(),
                entity.getStatusLastUpdatedAt(),
                entity.getErrors(),
                entity.getFatalError());
    }

    private void bindUpdate(BoundStatement bs, IndexCreatedEvent entity)
    {
        bs.bind(entity.getRecordsCompleted(),
                entity.getStatusLastUpdatedAt(),
                entity.getErrors(),
                entity.getFatalError(),
                entity.getUuid());
    }

    protected void bindUUIDWhere(BoundStatement bs, UUID uuid)
    {
        bs.bind(uuid);
    }

    private List<IndexCreatedEvent> marshalAll(ResultSet rs)
    {
        List<IndexCreatedEvent> indexes = new ArrayList<>();
        Iterator<Row> i = rs.iterator();
        while (i.hasNext())
        {
            IndexCreatedEvent status = marshalRow(i.next());
            indexes.add(status);
        }
        return indexes;
    }

    private List<UUID> marshalActiveUUIDs(ResultSet rs)
    {
        List<UUID> activeIds = new ArrayList<>();
        Iterator<Row> i = rs.iterator();
        while (i.hasNext())
        {
            activeIds.add(i.next().getUUID(Columns.ID));
        }
        return activeIds;
    }

    protected IndexCreatedEvent marshalRow(Row row)
    {
        if (row == null)
        {
            return null;
        }
        //look up index here
        Index index = new Index();
        index.setName(row.getString(Columns.INDEX_NAME));
        index.setTable(row.getString(Columns.DATABASE), row.getString(Columns.TABLE));
        Index toUse;
        try
        {
            toUse = indexRepo.read(index.getId());
        } catch (ItemNotFoundException e)//this should only happen in tests that do not have full test data established; errors will be evident if this happens in the actual app
        {
            toUse = index;
        }
        IndexCreatedEvent i = new IndexCreatedEvent(row.getUUID(Columns.ID), row.getDate(Columns.STARTED_AT), row.getDate(Columns.UPDATED_AT), toUse, row.getLong(Columns.TOTAL_RECORDS), row.getLong(Columns.RECORDS_COMPLETED));
        i.setFatalError(row.getString(Columns.FATAL_ERROR));
        i.setErrors(row.getList(Columns.ERRORS, String.class));
        i.calculateValues();
        return i;
    }

    @Override
    public Session getSession()
    {
        return super.getSession();
    }

}
