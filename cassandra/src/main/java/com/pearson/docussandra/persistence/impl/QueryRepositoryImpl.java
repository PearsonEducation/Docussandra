package com.pearson.docussandra.persistence.impl;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.pearson.docussandra.Utils;
import com.pearson.docussandra.bucketmanagement.PrimaryIndexBucketLocatorImpl;
import com.pearson.docussandra.domain.objects.Document;
import com.pearson.docussandra.domain.ParsedQuery;
import com.pearson.docussandra.domain.objects.QueryResponseWrapper;
import com.pearson.docussandra.exception.IndexParseException;
import com.pearson.docussandra.exception.IndexParseFieldException;
import com.pearson.docussandra.persistence.QueryRepository;
import com.pearson.docussandra.persistence.helper.DocumentPersistanceUtils;
import com.pearson.docussandra.persistence.helper.PreparedStatementFactory;
import java.util.ArrayList;
import java.util.Iterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.pearson.docussandra.bucketmanagement.BucketLocator;

/**
 * Repository for querying for records.
 *
 * @author https://github.com/JeffreyDeYoung
 */
public class QueryRepositoryImpl implements QueryRepository
{

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private static final String QUERY_CQL = "select * from %s where bucket = ? AND %s";
    private static final String QUERY_CQL_LIMIT = "select * from %s where bucket = ? AND %s LIMIT %s";//we use the limit as to not put any more stress on cassandra than we need to (even though our algorithm will discard the data anyway)

    private BucketLocator ibl;

    private Session session;

    public QueryRepositoryImpl(Session session)
    {
        this.session = session;
        ibl = PrimaryIndexBucketLocatorImpl.getInstance();
    }

    private BoundStatement generateQueryStatement(ParsedQuery query, long maxIndex) throws IndexParseException
    {
        String finalQuery;
        //format QUERY_CQL
        if (maxIndex == -1)//no artifical limit
        {
            finalQuery = String.format(QUERY_CQL, query.getITable(), query.getWhereClause().getBoundStatementSyntax());
        } else //with a limit
        {
            finalQuery = String.format(QUERY_CQL_LIMIT, query.getITable(), query.getWhereClause().getBoundStatementSyntax(), maxIndex);
        }
        //run query
        PreparedStatement ps = PreparedStatementFactory.getPreparedStatement(finalQuery, getSession());
        BoundStatement bs = new BoundStatement(ps);
        //set the bucket
        Long bucket;
        try
        {
            bucket = PrimaryIndexBucketLocatorImpl.getInstance().getBucket(query.getWhereClause().getValues().get(0), query.getIndex().getFields().get(0).getType());
        } catch (IndexParseFieldException ex)
        {
            throw new IndexParseException(query.getIndex().getFields().get(0), ex);
        }
        bs.setLong(0, bucket);
        int i = 1;
        for (String bindValue : query.getWhereClause().getValues())
        {
            Utils.setField(bindValue, query.getIndex().getFields().get(i - 1), bs, i);
            i++;
        }
        return bs;
    }

    /**
     * Do a query without limit or offset.
     *
     * @param query ParsedQuery to execute.
     * @return A query response.
     * @throws IndexParseException If the query is not on a valid index.
     */
    @Override
    public QueryResponseWrapper query(ParsedQuery query) throws IndexParseException
    {
        //run the query
        ResultSet results = session.execute(generateQueryStatement(query, -1));
        //process result(s)
        ArrayList<Document> toReturn = new ArrayList<>();
        Iterator<Row> ite = results.iterator();
        while (ite.hasNext())
        {
            Row row = ite.next();
            toReturn.add(DocumentPersistanceUtils.marshalRow(row));
        }
        return new QueryResponseWrapper(toReturn, 0l);
    }

    /**
     * Do a query with limit and offset.
     *
     * @param query ParsedQuery to execute.
     * @param limit Maximum number of results to return.
     * @param offset Number of records at the beginning of the results to
     * discard.
     * @return A query response.
     * @throws IndexParseException If the query is not on a valid index.
     */
    @Override
    public QueryResponseWrapper query(ParsedQuery query, int limit, long offset) throws IndexParseException
    {
        //run the query
        long maxIndex = offset + limit;
        ResultSet results = session.execute(generateQueryStatement(query, maxIndex + 1));//we do one plus here so we know if there are additional results
        return DocumentPersistanceUtils.parseResultSetWithLimitAndOffset(results, limit, offset);
    }

    /**
     * @return the session
     */
    @Override
    public Session getSession()
    {
        return session;
    }

}
