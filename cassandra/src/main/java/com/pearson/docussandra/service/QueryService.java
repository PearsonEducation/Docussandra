package com.pearson.docussandra.service;

import com.pearson.docussandra.domain.ParsedQuery;
import com.pearson.docussandra.domain.objects.Identifier;
import com.pearson.docussandra.domain.objects.Query;
import com.pearson.docussandra.domain.objects.QueryResponseWrapper;
import com.pearson.docussandra.exception.FieldNotIndexedException;
import com.pearson.docussandra.exception.IndexParseException;
import com.pearson.docussandra.persistence.DatabaseRepository;
import com.pearson.docussandra.persistence.QueryRepository;
import com.pearson.docussandra.persistence.TableRepository;

/**
 * Service for performing a query.
 *
 * @author https://github.com/JeffreyDeYoung
 */
public class QueryService
{
    /**
     * Query Repository for accessing the database.
     */
    private QueryRepository queries;
    private TableRepository tables;
    private DatabaseRepository databases;

    /**
     * Constructor.
     *
     * @param queryRepository QueryRepositoryImpl to use to perform the query.
     */
    public QueryService(DatabaseRepository databaseRepository, TableRepository tableRepository, QueryRepository queryRepository)
    {
        super();
        this.databases = databaseRepository;
        this.tables = tableRepository;
        this.queries = queryRepository;
    }

    /**
     * Does a query with no limit or offset.
     *
     * @param db Database to query.
     * @param toQuery Query perform.
     * @return A query response object containing a list of documents and some
     * metadata about the query.
     * @throws FieldNotIndexedException If the field that was attempted to be
     * queried on is not part of an index.
     * @throws IndexParseException If the field that was attempted to be queried
     * on was not in a recognized format.
     */
    public QueryResponseWrapper query(String db, Query toQuery) throws IndexParseException, FieldNotIndexedException
    {
        ParsedQuery parsedQuery = ParsedQueryFactory.getParsedQuery(db, toQuery, queries.getSession());
        return queries.query(parsedQuery);
    }

    /**
     * Does a query with limit and offset.
     *
     * @param toQuery Query perform.
     * @param limit max number of results to return
     * @param offset offset of the query results
     * @return A query response object containing a list of documents and some
     * metadata about the query.
     * @throws FieldNotIndexedException If the field that was attempted to be
     * queried on is not part of an index.
     * @throws IndexParseException If the field that was attempted to be queried
     * on was not in a recognized format.
     */
    public QueryResponseWrapper query(Query toQuery, int limit, long offset) throws IndexParseException, FieldNotIndexedException
    {
        ParsedQuery parsedQuery = ParsedQueryFactory.getParsedQuery(toQuery.getDatabase(), toQuery, queries.getSession());
        return queries.query(parsedQuery, limit, offset);
    }

    /**
     * method to check if the database provided in the query url exists
     * */
    public boolean checkDatabase(Identifier id){
        boolean dbExists;
        dbExists=databases.exists(id);
        return dbExists;
    }

    /**
     * method to check if the table provided in the query url exists
     * */
    public boolean checkTable(Identifier id){
        boolean dbExists;
        dbExists=tables.exists(id);
        return dbExists;
    }
}
