package com.pearson.docussandra.service;

import java.util.List;

import com.pearson.docussandra.domain.objects.Identifier;
import com.pearson.docussandra.domain.objects.Index;
import com.pearson.docussandra.domain.event.IndexCreatedEvent;
import com.pearson.docussandra.exception.ItemNotFoundException;
import com.pearson.docussandra.persistence.IndexRepository;
import com.pearson.docussandra.persistence.IndexStatusRepository;
import com.pearson.docussandra.persistence.TableRepository;
import com.strategicgains.eventing.DomainEvents;
import com.strategicgains.syntaxe.ValidationEngine;
import java.util.Date;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service for interacting with indexes.
 *
 * @author https://github.com/JeffreyDeYoung
 */
public class IndexService
{

    /**
     * Table repository to use for interacting with tables.
     */
    private TableRepository tablesRepo;

    /**
     * Index repository to use for interacting with indexes.
     */
    private IndexRepository indexesRepo;

    /**
     * IndexStatus repository to use for interacting with index statuses.
     */
    private IndexStatusRepository statusRepo;

    /**
     * Logger for this class.
     */
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * Constructor.
     *
     * @param tableRepository Table repository to use for interacting with
     * tables.
     * @param indexRepository Index repository to use for interacting with
     * indexes.
     * @param status IndexStatus repository to use for interacting with index
     * statuses.
     */
    public IndexService(TableRepository tableRepository, IndexRepository indexRepository, IndexStatusRepository status)
    {
        super();
        this.indexesRepo = indexRepository;
        this.tablesRepo = tableRepository;
        this.statusRepo = status;
    }

    /**
     * Creates an index. Index will be created synchronously, but it will be
     * populated asynchronously, and it will not be "active" (meaning it can't
     * be used for querying) until indexing is complete. Inactive indexes will
     * still be populated by NEW records that are being inserted.
     *
     * @param index Index to create.
     * @return An IndexCreatedEvent that contains the index and some metadata
     * about it's creation status.
     */
    public IndexCreatedEvent create(Index index)
    {
        verifyTable(index.getDatabaseName(), index.getTableName());
        ValidationEngine.validateAndThrow(index);

        index.setActive(false);//we default to not active when being created; we don't allow the user to change this; only the app can change this
        logger.debug("Creating index: " + index.toString());
        Index created = indexesRepo.create(index);
        long dataSize = tablesRepo.countTableSize(index.getDatabaseName(), index.getTableName());
        Date now = new Date();
        UUID uuid = UUID.randomUUID();//TODO: is this right?
        IndexCreatedEvent toReturn = new IndexCreatedEvent(uuid, now, now, created, dataSize, 0l);
        statusRepo.create(toReturn);
        toReturn.calculateValues();
        DomainEvents.publish(toReturn);
        return toReturn;
    }

    /**
     * Gets the status of an index creation event. Allows a user to check on the
     * status of the indexing for an index.
     *
     * @param id Id to get the status for.
     * @return an IndexCreatedEvent for this id.
     */
    public IndexCreatedEvent status(UUID id)
    {
        logger.debug("Checking index creation status: " + id.toString());
        IndexCreatedEvent toReturn = statusRepo.read(id);
        return toReturn;
    }

    /**
     * Gets statuses for ALL pending index creations. Allows users and admins to
     * check the indexing load on the system.
     *
     * @return a list of IndexCreatedEvent.
     */
    public List<IndexCreatedEvent> getAllCurrentlyIndexing()
    {
        logger.debug("Checking index creation status.");
        return statusRepo.readAllCurrentlyIndexing();
    }

    /**
     * Reads an index.
     *
     * @param identifier
     * @return
     */
    public Index read(Identifier identifier)
    {
        return indexesRepo.read(identifier);
    }

    /**
     * Deletes an index. Will also remove the associated iTables.
     *
     * @param identifier
     */
    public void delete(Identifier identifier)
    {
        logger.debug("Deleting index: " + identifier.toString());
        indexesRepo.delete(identifier);
    }

    /**
     * Deletes an index. Will also remove the associated iTables.
     *
     * @param index
     */
    public void delete(Index index)
    {
        logger.debug("Deleting index: " + index.toString());
        indexesRepo.delete(index);
    }

    /**
     * Reads all indexes for the given database and table.
     *
     * @param database
     * @param table
     * @return
     */
    public List<Index> readAll(String database, String table)
    {
        return indexesRepo.readAllCached(new Identifier(database, table));
    }

    /**
     * Counts the number of indexes for this database and table.
     *
     * @param database
     * @param table
     * @return
     */
    public long count(String database, String table)
    {
        return indexesRepo.countAll(new Identifier(database, table));
    }

    /**
     * Verifies if a setTable exists or not.
     *
     * @param database
     * @param table
     */
    private void verifyTable(String database, String table)
    {
        Identifier tableId = new Identifier(database, table);

        if (!tablesRepo.exists(tableId))
        {
            throw new ItemNotFoundException("Table not found: " + tableId.toString());
        }
    }
}
