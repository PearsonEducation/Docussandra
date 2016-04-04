
package com.pearson.docussandra.persistence;

import com.datastax.driver.core.Session;
import com.pearson.docussandra.domain.ParsedQuery;
import com.pearson.docussandra.domain.objects.QueryResponseWrapper;
import com.pearson.docussandra.exception.IndexParseException;

/**
 * Repository for querying for records.
 *
 * @author https://github.com/JeffreyDeYoung
 */
public interface QueryRepository
{

    /**
     * @return the session
     */
    Session getSession();

    /**
     * Do a query without limit or offset.
     *
     * @param query ParsedQuery to execute.
     * @return A query response.
     * @throws IndexParseException If the query is not on a valid index.
     */
    public QueryResponseWrapper query(ParsedQuery query) throws IndexParseException;

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
    public QueryResponseWrapper query(ParsedQuery query, int limit, long offset) throws IndexParseException;

}
