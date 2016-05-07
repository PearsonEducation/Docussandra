
package com.pearson.docussandra.handler;

import com.datastax.driver.core.BoundStatement;
import com.pearson.docussandra.bucketmanagement.PrimaryIndexBucketLocatorImpl;
import com.pearson.docussandra.domain.objects.Document;
import com.pearson.docussandra.domain.objects.Index;
import com.pearson.docussandra.domain.event.IndexCreatedEvent;
import com.pearson.docussandra.domain.objects.QueryResponseWrapper;
import com.pearson.docussandra.exception.IndexParseException;
import com.pearson.docussandra.persistence.DocumentRepository;
import com.pearson.docussandra.persistence.IndexRepository;
import com.pearson.docussandra.persistence.IndexStatusRepository;
import com.strategicgains.eventing.EventHandler;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles our background indexing tasks.
 *
 * @author https://github.com/JeffreyDeYoung
 */
public class IndexCreatedHandler implements EventHandler
{

    private static Logger logger = LoggerFactory.getLogger(IndexCreatedHandler.class);

    private final static int CHUNK = 1000;

    private IndexRepository indexRepo;
    private IndexStatusRepository indexStatusRepo;
    private DocumentRepository docRepo;

    public IndexCreatedHandler(IndexRepository indexRepo, IndexStatusRepository indexStatusRepo, DocumentRepository docRepo)
    {
        this.indexRepo = indexRepo;
        this.indexStatusRepo = indexStatusRepo;
        this.docRepo = docRepo;
    }

    @Override
    public boolean handles(Class<?> eventClass)
    {
        return eventClass.equals(IndexCreatedEvent.class);
    }

    @Override
    public void handle(Object event) throws Exception
    {
        logger.debug("Handler received background indexing event: " + event.toString());
        //Thread.sleep(1000);//pause for a second to ensure the iTable gets created before proceeding (Todd: thoughts on this?)
        IndexCreatedEvent status = null;
        try
        {
            status = (IndexCreatedEvent) event;
            Index index = indexRepo.read(status.getIndex().getId());
            long offset = 0;
            long recordsCompleted = 0;
            QueryResponseWrapper responseWrapper = docRepo.readAll(index.getDatabaseName(), index.getTableName(), CHUNK, offset);
            boolean hasMore;
            if (responseWrapper.isEmpty())
            {
                hasMore = false;
            } else
            {
                hasMore = true;
            }
            while (hasMore)
            {
                for (Document toIndex : responseWrapper)
                {
                    try
                    {
                        //actually index here
                        BoundStatement statement = IndexMaintainerHelper.generateDocumentCreateIndexEntryStatement(indexStatusRepo.getSession(), index, toIndex, PrimaryIndexBucketLocatorImpl.getInstance());
                        if (statement != null)
                        {
                            indexStatusRepo.getSession().execute(statement);
                        }
                        recordsCompleted++;
                    } catch (IndexParseException e)
                    {
                        //we couldn't parse this document for an index; make a note, and move on
                        recordsCompleted++;//we will still call this record "complete" for the sake of time calculation, and percent done
                        status.setRecordsCompleted(recordsCompleted);
                        //this will have to change if threading
                        List<String> errors = status.getErrors();
                        if (errors == null)
                        {
                            errors = new ArrayList<>();
                        }
                        errors.add(e.toString() + " In document: " + toIndex.toString());//may want to reduce the verbosity here eventually -- or break some meta-data out into columns
                        status.setErrors(errors);
                        status.setStatusLastUpdatedAt(new Date());
                        //indexStatusRepo.update(status);//lets not save every time for now
                    }
                }
                offset = offset + CHUNK;
                //update status
                status.setRecordsCompleted(recordsCompleted);
                status.setStatusLastUpdatedAt(new Date());
                indexStatusRepo.update(status);
                //get the next chunk
                responseWrapper = docRepo.readAll(index.getDatabaseName(), index.getTableName(), CHUNK, offset);
                if (responseWrapper.isEmpty())
                {
                    hasMore = false;
                }
            }
            //update index as done
            index.setActive(true);
            status.setIndex(index);
            indexRepo.markActive(index);
            //final update       
            indexStatusRepo.update(status);
        } catch (Exception e)
        {
            String indexName = "Cannot be determined.";
            if (status != null && status.getIndex() != null)
            {
                indexName = status.getIndex().getName();
            }
            String errorMessage = "Could not complete indexing event for index: '" + indexName + "'.";
            logger.error(errorMessage, e);
            if (status != null)
            {
                //intentionally a separate clause so our error prints in case this throws.
                status.setFatalError(errorMessage + " Please contact a system administrator to resolve this issue.");
                status.setStatusLastUpdatedAt(new Date());
                indexStatusRepo.update(status);
            }
            throw e;
        }
    }

}
