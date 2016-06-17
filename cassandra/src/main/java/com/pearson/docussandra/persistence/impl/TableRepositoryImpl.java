package com.pearson.docussandra.persistence.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.pearson.docussandra.domain.objects.Identifier;
import com.pearson.docussandra.domain.objects.Index;
import com.pearson.docussandra.domain.objects.Table;
import com.pearson.docussandra.exception.ItemNotFoundException;
import com.pearson.docussandra.persistence.TableRepository;
import com.pearson.docussandra.persistence.helper.PreparedStatementFactory;
import com.pearson.docussandra.persistence.parent.AbstractCRUDRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableRepositoryImpl extends AbstractCRUDRepository<Table> implements TableRepository
{

    /**
     * Logger for this class.
     */
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private class Tables
    {

        static final String BY_ID = "sys_tbl";
    }

    private class Columns
    {

        static final String NAME = "tbl_name";
        static final String DATABASE = "db_name";
        static final String DESCRIPTION = "description";
        static final String CREATED_AT = "created_at";
        static final String UPDATED_AT = "updated_at";
    }

    private static final String IDENTITY_CQL = " where " + Columns.DATABASE + " = ? and " + Columns.NAME + " = ?";
    private static final String EXISTENCE_CQL = "select count(*) from %s" + IDENTITY_CQL;
    private static final String CREATE_CQL = "insert into %s (%s, " + Columns.DATABASE + ", " + Columns.DESCRIPTION + ", " + Columns.CREATED_AT + ", " + Columns.UPDATED_AT + ") values (?, ?, ?, ?, ?) IF NOT EXISTS";
    private static final String READ_CQL = "select * from %s" + IDENTITY_CQL;
    private static final String DELETE_CQL = "delete from %s" + IDENTITY_CQL;
    private static final String UPDATE_CQL = "update %s set " + Columns.DESCRIPTION + " = ?, " + Columns.UPDATED_AT + " = ?" + IDENTITY_CQL;
    private static final String READ_ALL_CQL = "select * from %s where " + Columns.DATABASE + " = ?";
    private static final String READ_ALL_COUNT_CQL = "select count(*) from %s where " + Columns.DATABASE + " = ?";
    private static final String READ_COUNT_TABLE_SIZE_CQL = "select count(*) from %s";

    private static final String CREATE_DOC_TABLE_CQL = "create table %s"
            + " (id uuid, object blob, " + Columns.CREATED_AT + " timestamp, " + Columns.UPDATED_AT + " timestamp,"
            + " primary key ((id), " + Columns.UPDATED_AT + "))"
            + " with clustering order by (" + Columns.UPDATED_AT + " DESC);";
    private static final String DROP_DOC_TABLE_CQL = "drop table if exists %s;";

    private PreparedStatement existStmt;
    private PreparedStatement readStmt;
    private PreparedStatement createStmt;
    private PreparedStatement deleteStmt;
    private PreparedStatement updateStmt;
    private PreparedStatement readAllStmt;
    private PreparedStatement readAllCountStmt;

    private IndexRepositoryImpl indexRepo;

    public TableRepositoryImpl(Session session)
    {
        super(session, Tables.BY_ID);
        initialize();
        indexRepo = new IndexRepositoryImpl(session);
    }

    protected void initialize()
    {
        existStmt = PreparedStatementFactory.getPreparedStatement(String.format(EXISTENCE_CQL, getTable()), getSession());
        readStmt = PreparedStatementFactory.getPreparedStatement(String.format(READ_CQL, getTable()), getSession());
        createStmt = PreparedStatementFactory.getPreparedStatement(String.format(CREATE_CQL, getTable(), Columns.NAME), getSession());
        deleteStmt = PreparedStatementFactory.getPreparedStatement(String.format(DELETE_CQL, getTable()), getSession());
        updateStmt = PreparedStatementFactory.getPreparedStatement(String.format(UPDATE_CQL, getTable()), getSession());
        readAllStmt = PreparedStatementFactory.getPreparedStatement(String.format(READ_ALL_CQL, getTable()), getSession());
        readAllCountStmt = PreparedStatementFactory.getPreparedStatement(String.format(READ_ALL_COUNT_CQL, getTable()), getSession());
    }

    @Override
    public boolean exists(Identifier identifier)
    {
        if (identifier == null || identifier.isEmpty())
        {
            return false;
        }

        BoundStatement bs = new BoundStatement(existStmt);
        bindIdentifier(bs, identifier);
        return (getSession().execute(bs).one().getLong(0) > 0);
    }

    @Override
    public Table read(Identifier identifier)
    {
        if (identifier == null || identifier.isEmpty())
        {
            return null;
        }

        BoundStatement bs = new BoundStatement(readStmt);
        bindIdentifier(bs, identifier);
        Table response = marshalRow(getSession().execute(bs).one());
        if (response == null)
        {
            throw new ItemNotFoundException("ID not found: " + identifier.toString());
        }
        return response;
    }

    @Override
    public Table create(Table entity)
    {
        // Create the actual table for the documents.
        Statement s = new SimpleStatement(String.format(CREATE_DOC_TABLE_CQL, entity.toDbTable()));
        getSession().execute(s);

        // Create the metadata for the table.
        BoundStatement bs = new BoundStatement(createStmt);
        bindCreate(bs, entity);
        getSession().execute(bs);
        return entity;
    }

    @Override
    public Table update(Table entity)
    {
        BoundStatement bs = new BoundStatement(updateStmt);
        bindUpdate(bs, entity);
        getSession().execute(bs);
        return entity;
    }

    @Override
    public void delete(Table entity)
    {
        // Delete the actual table for the documents.
        Statement s = new SimpleStatement(String.format(DROP_DOC_TABLE_CQL, entity.toDbTable()));
        getSession().execute(s);

        BoundStatement bs = new BoundStatement(deleteStmt);
        bindIdentifier(bs, entity.getId());
        getSession().execute(bs);
        cascadeDelete(entity.getId());
    }

    @Override
    public void delete(Identifier id)
    {
        // Delete the actual table for the documents.
        Statement s = new SimpleStatement(String.format(DROP_DOC_TABLE_CQL, id.getDatabaseName() + "_" + id.getTableName()));
        getSession().execute(s);

        BoundStatement bs = new BoundStatement(deleteStmt);
        bindIdentifier(bs, id);
        getSession().execute(bs);
        cascadeDelete(id);
    }

    @Override
    public List<Table> readAll(Identifier id)
    {
        BoundStatement bs = new BoundStatement(readAllStmt);
        bs.bind(id.getDatabaseName());
        return (marshalAll(getSession().execute(bs)));
    }

    @Override
    public List<Table> readAll()
    {
        throw new UnsupportedOperationException("Not valid for this class.");
    }

    @Override
    public long countAllTables(String database)
    {
        BoundStatement bs = new BoundStatement(readAllCountStmt);
        bs.bind(database);
        return (getSession().execute(bs).one().getLong(0));
    }

    @Override
    public long countTableSize(String database, String tableName)
    {
        PreparedStatement readCountTableSizeStmt = PreparedStatementFactory.getPreparedStatement(String.format(READ_COUNT_TABLE_SIZE_CQL, database + "_" + tableName), getSession());
        BoundStatement bs = new BoundStatement(readCountTableSizeStmt);
        return (getSession().execute(bs).one().getLong(0));
    }

    private void cascadeDelete(Identifier id)
    {
        String dbName = id.getDatabaseName();
        String tableName = id.getTableName();
        logger.info("Cleaning up Indexes for table: " + dbName + "/" + tableName);
        //remove all the tables and all the documents in that table.
        //TODO: version instead of delete 
        //Delete all indexes
        List<Index> indexes = indexRepo.readAll(id);//get all indexes
        for (Index i : indexes)
        {
            indexRepo.delete(i);// then delete them
        }
    }

    private void bindCreate(BoundStatement bs, Table entity)
    {
        bs.bind(entity.getName(),
                entity.getDatabase().getName(),
                entity.getDescription(),
                entity.getCreatedAt(),
                entity.getUpdatedAt());
    }

    private void bindUpdate(BoundStatement bs, Table entity)
    {
        bs.bind(entity.getDescription(),
                entity.getUpdatedAt(),
                entity.getDatabase().getName(),
                entity.getName());
    }

    private List<Table> marshalAll(ResultSet rs)
    {
        List<Table> tables = new ArrayList<>();
        Iterator<Row> i = rs.iterator();

        while (i.hasNext())
        {
            tables.add(marshalRow(i.next()));
        }

        return tables;
    }

    protected Table marshalRow(Row row)
    {
        if (row == null)
        {
            return null;
        }

        Table c = new Table();
        c.setName(row.getString(Columns.NAME));
        c.setDatabase(row.getString(Columns.DATABASE));
        c.setDescription(row.getString(Columns.DESCRIPTION));
        c.setCreatedAt(row.getDate(Columns.CREATED_AT));
        c.setUpdatedAt(row.getDate(Columns.UPDATED_AT));
        return c;
    }
}
