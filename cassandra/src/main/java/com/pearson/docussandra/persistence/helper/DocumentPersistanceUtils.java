
package com.pearson.docussandra.persistence.helper;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.pearson.docussandra.domain.objects.Document;
import com.pearson.docussandra.domain.objects.QueryResponseWrapper;
import com.pearson.docussandra.persistence.impl.DocumentRepositoryImpl;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import org.bson.BSON;
import org.bson.BSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Util class for repositories (DAOs) that deal with Documents or
 * QueryResponseWrapper types.
 *
 * @author https://github.com/JeffreyDeYoung
 */
public class DocumentPersistanceUtils
{

    private static Logger logger = LoggerFactory.getLogger(DocumentPersistanceUtils.class);

    /**
     * Marshals a Cassandra row into a Document object.
     *
     * @param row Row to marshal.
     * @return A document based on the provided row.
     */
    public static Document marshalRow(Row row)
    {
        if (row == null)
        {
            return null;
        }
        Document d = new Document();
        d.setUuid(row.getUUID(DocumentRepositoryImpl.Columns.ID));
        ByteBuffer b = row.getBytes(DocumentRepositoryImpl.Columns.OBJECT);
        if (b != null && b.hasArray())
        {
            byte[] result = new byte[b.remaining()];
            b.get(result);
            BSONObject o = BSON.decode(result);
            d.setObject(o);
        }
        d.setCreatedAt(row.getDate(DocumentRepositoryImpl.Columns.CREATED_AT));
        d.setUpdatedAt(row.getDate(DocumentRepositoryImpl.Columns.UPDATED_AT));
        return d;
    }

    /**
     * Parses a result set into a QueryResponseWrapper.
     *
     * @param results ResultSet to parse.
     * @param limit Limit at which to stop parsing the results.
     * @param offset Offest at which to start parsing the results.
     * @return a Populated QueryResponseWrapper that can be returned to a
     * calling application or user.
     */
    public static QueryResponseWrapper parseResultSetWithLimitAndOffset(ResultSet results, int limit, long offset)
    {
        long maxIndex = offset + limit;
        ArrayList<Document> toReturn = new ArrayList<>(limit);
        Iterator<Row> ite = results.iterator();
        long offsetCounter = 0;
        Long additionalResults = 0L;
        while (ite.hasNext())
        {
            Row row = ite.next();
            if (offsetCounter >= maxIndex)
            {
                additionalResults = null;
                break; //we are done; don't bother processing anymore, it's not going to be used anyway
            } else if (offsetCounter >= offset)
            {
                toReturn.add(DocumentPersistanceUtils.marshalRow(row));
            } else
            {
                if (logger.isTraceEnabled())
                {
                    logger.trace("We are probably wasting processor time by processing a query inefficently");
                }
            }
            offsetCounter++;
        }
        return new QueryResponseWrapper(toReturn, additionalResults);
    }

}
