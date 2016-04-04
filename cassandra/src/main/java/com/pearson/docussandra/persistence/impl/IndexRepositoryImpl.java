package com.pearson.docussandra.persistence.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.pearson.docussandra.cache.CacheFactory;
import com.pearson.docussandra.cache.CacheSynchronizer;
import com.pearson.docussandra.domain.objects.FieldDataType;
import com.pearson.docussandra.domain.objects.Identifier;
import com.pearson.docussandra.domain.objects.Index;
import com.pearson.docussandra.domain.objects.IndexField;
import com.pearson.docussandra.domain.objects.IndexIdentifier;
import com.pearson.docussandra.domain.objects.Table;
import com.pearson.docussandra.exception.ItemNotFoundException;
import com.pearson.docussandra.persistence.ITableRepository;
import com.pearson.docussandra.persistence.IndexRepository;
import com.pearson.docussandra.persistence.helper.PreparedStatementFactory;
import com.pearson.docussandra.persistence.parent.AbstractCRUDRepository;
import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexRepositoryImpl extends AbstractCRUDRepository<Index> implements IndexRepository
{

    /**
     * Logger for this class.
     */
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * Class that defines the Cassandra tables that this repository manages.
     */
    private class Tables
    {

        static final String BY_ID = "sys_idx";
    }

    /**
     * Class that defines the database columns that this repository manages.
     */
    private class Columns
    {

        static final String DATABASE = "db_name";
        static final String TABLE = "tbl_name";
        static final String NAME = "name";
        static final String IS_UNIQUE = "is_unique";
        static final String FIELDS = "fields";
        static final String FIELDS_TYPE = "fields_type";
        static final String ONLY = "only";
        static final String CREATED_AT = "created_at";
        static final String UPDATED_AT = "updated_at";
        static final String IS_ACTIVE = "is_active";
    }

    private static final String IDENTITY_CQL = " where db_name = ? and tbl_name = ? and name = ?";
    private static final String EXISTENCE_CQL = "select count(*) from %s" + IDENTITY_CQL;
    private static final String CREATE_CQL = "insert into %s (%s, db_name, tbl_name, is_unique, fields, fields_type, only, is_active, created_at, updated_at) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS";
    private static final String READ_CQL = "select * from %s" + IDENTITY_CQL;
    private static final String DELETE_CQL = "delete from %s" + IDENTITY_CQL;
    private static final String MARK_ACTIVE_CQL = "update %s set is_active = true" + IDENTITY_CQL;
    private static final String READ_ALL_CQL = "select * from %s where db_name = ? and tbl_name = ?";
    private static final String READ_ALL_COUNT_CQL = "select count(*) from %s where db_name = ? and tbl_name = ?";

    private PreparedStatement existStmt;
    private PreparedStatement readStmt;
    private PreparedStatement createStmt;
    private PreparedStatement deleteStmt;
    private PreparedStatement markActiveStmt;
    private PreparedStatement readAllStmt;
    private PreparedStatement readAllCountStmt;

    private ITableRepository iTableRepo;

    /**
     * Constructor.
     *
     * @param session
     */
    public IndexRepositoryImpl(Session session)
    {
        super(session, Tables.BY_ID);
        initialize();
        iTableRepo = new ITableRepositoryImpl(session);
    }

    /**
     * Sets up our prepared statements for this repository.
     */
    protected void initialize()
    {
        existStmt = PreparedStatementFactory.getPreparedStatement(String.format(EXISTENCE_CQL, getTable()), getSession());
        readStmt = PreparedStatementFactory.getPreparedStatement(String.format(READ_CQL, getTable()), getSession());
        createStmt = PreparedStatementFactory.getPreparedStatement(String.format(CREATE_CQL, getTable(), Columns.NAME), getSession());
        deleteStmt = PreparedStatementFactory.getPreparedStatement(String.format(DELETE_CQL, getTable()), getSession());
        markActiveStmt = PreparedStatementFactory.getPreparedStatement(String.format(MARK_ACTIVE_CQL, getTable()), getSession());
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
    public Index read(Identifier identifier)
    {
        if (identifier == null || identifier.isEmpty())
        {
            return null;
        }

        BoundStatement bs = new BoundStatement(readStmt);
        bindIdentifier(bs, identifier);
        Index response = marshalRow(getSession().execute(bs).one());
        if (response == null)
        {
            throw new ItemNotFoundException("ID not found: " + identifier.toString());
        }
        return response;
    }

    @Override
    public Index create(Index entity)
    {
        BoundStatement bs = new BoundStatement(createStmt);
        bindCreate(bs, entity);
        getSession().execute(bs);
        //maintain cache  
        try//we do this in a try/catch because we don't want to cause an app error if this fails
        {
            Cache c = CacheFactory.getCache("index");
            String key = entity.getDatabaseName() + ":" + entity.getTableName();
            synchronized (CacheSynchronizer.getLockingObject(key, Index.class))
            {
                List<Index> currentIndex = this.readAll(entity.getId());
                Element e = new Element(key, currentIndex);
                c.put(e);
            }
        } catch (Exception e)
        {
            logger.error("Could not update index cache upon index create.", e);
        }
        iTableRepo.createITable(entity);
        //-----check to see if it is correct, suggest the user delete and try again if it's not -- probably
        //-----automatically re-index; hard to actually do, it would need a different name if the index was in use -- probably not
        //-----do nothing -- maybe?
        return entity;
    }

    /**
     * Marks an index as "active" meaning that indexing has completed on it.
     *
     * @param entity Index to mark active.
     */
    @Override
    public void markActive(Index entity)
    {
        BoundStatement bs = new BoundStatement(markActiveStmt);
        bindIdentifier(bs, entity.getId());
        getSession().execute(bs);
    }

    @Override
    public Index update(Index entity)
    {
        throw new UnsupportedOperationException("Updates are not supported on indices; create a new one and delete the old one if you would like this functionality.");
    }

    @Override
    public void delete(Identifier id)
    {
        BoundStatement bs = new BoundStatement(deleteStmt);
        bindIdentifier(bs, id);
        getSession().execute(bs);
        //maintain cache
        try//we do this in a try/catch because we don't want to cause an app error if this fails
        {
            Cache c = CacheFactory.getCache("index");
            String key = id.getDatabaseName() + ":" + id.getTableName();
            synchronized (CacheSynchronizer.getLockingObject(key, Index.class))
            {
                List<Index> currentIndex = this.readAll(id);
                if (!currentIndex.isEmpty())
                {
                    Element e = new Element(key, currentIndex);
                    c.put(e);
                } else
                {
                    c.put(null);
                }
            }
        } catch (Exception e)
        {
            logger.error("Could not update index cache upon index delete.", e);
        }
        cascadeDelete(new IndexIdentifier(id));
        //TODO: delete the index status (IndexCreationEvents) (or at least mark it permantly inactive/disabled with a new timestamp, ensuring that /index_status won't return it) and halt any active indexing to save processor time -- issue https://github.com/PearsonEducation/Docussandra/issues/3
    }

    @Override
    public void delete(Index entity)
    {
        delete(entity.getId());
    }

    @Override
    public List<Index> readAll(Identifier id)
    {
        BoundStatement bs = new BoundStatement(readAllStmt);
        bs.bind(id.getDatabaseName(), id.getTableName());
        return (marshalAll(getSession().execute(bs)));
    }

    @Override
    public List<Index> readAll()
    {
        throw new UnsupportedOperationException("Call not valid for this class.");
    }

    /**
     * Same as readAll, but will read from the cache if available.
     *
     * @return
     */
    @Override
    public List<Index> readAllCached(Identifier id)
    {
        String key = id.getDatabaseName() + ":" + id.getTableName();
        //logger.info("Reading all indexes from cache for " + key);
        Cache c = CacheFactory.getCache("index");
//        synchronized (CacheSynchronizer.getLockingObject(key, Index.class))
//        {
        Element e = c.get(key);
        if (e == null || e.getObjectValue() == null)//if its not set, or set, but null, re-read
        {
            List<Index> all = readAll(id);
            if (all != null && !all.isEmpty())//don't store empty or null index lists; could cause problems if an index gets created and we are not treating it as created
            {
                e = new Element(key, all);
                c.put(e);
            } else
            {
                return all;//it's empty or null; just return it now and not mess with the cache
            }
        } else
        {
            if (logger.isTraceEnabled())
            {
                logger.trace("Pulling Index from Cache: " + e.getObjectValue().toString());
            }
        }
        return (List<Index>) e.getObjectValue();
        //}
        //return readAll(database, table);
    }

    @Override
    public long countAll(Identifier id)
    {
        BoundStatement bs = new BoundStatement(readAllCountStmt);
        bs.bind(id.getDatabaseName(), id.getTableName());
        return (getSession().execute(bs).one().getLong(0));
    }

    private void cascadeDelete(IndexIdentifier id)
    {
        logger.info("Cleaning up ITables for index: " + id.getDatabaseName() + "/" + id.getTableName() + "/" + id.getIndexName());

        if (iTableRepo.iTableExists(id))
        {
            iTableRepo.deleteITable(id);
        }
    }

    private void bindCreate(BoundStatement bs, Index entity)
    {
        bs.bind(entity.getName(),
                entity.getDatabaseName(),
                entity.getTableName(),
                entity.isUnique(),
                entity.getFieldsValues(),
                entity.getFieldsTypes(),
                entity.getIncludeOnly(),
                entity.isActive(),
                entity.getCreatedAt(),
                entity.getUpdatedAt());
    }

    private List<Index> marshalAll(ResultSet rs)
    {
        List<Index> indexes = new ArrayList<>();
        Iterator<Row> i = rs.iterator();

        while (i.hasNext())
        {
            indexes.add(marshalRow(i.next()));
        }

        return indexes;
    }

    protected Index marshalRow(Row row)
    {
        if (row == null)
        {
            return null;
        }

        Index i = new Index();
        i.setName(row.getString(Columns.NAME));
        Table t = new Table();
        t.database(row.getString(Columns.DATABASE));
        t.name(row.getString(Columns.TABLE));
        i.setTable(t);
        i.isUnique(row.getBool(Columns.IS_UNIQUE));
        List<String> fields = row.getList(Columns.FIELDS, String.class);
        List<String> types = row.getList(Columns.FIELDS_TYPE, String.class);
        ArrayList<IndexField> indexFields = new ArrayList<>(fields.size());
        for (int j = 0; j < fields.size(); j++)
        {
            indexFields.add(new IndexField(fields.get(j), FieldDataType.valueOf(types.get(j))));
        }
        i.setFields(indexFields);
        i.setIncludeOnly(row.getList(Columns.ONLY, String.class));
        i.setActive(row.getBool(Columns.IS_ACTIVE));
        i.setCreatedAt(row.getDate(Columns.CREATED_AT));
        i.setUpdatedAt(row.getDate(Columns.UPDATED_AT));
        return i;
    }
}
