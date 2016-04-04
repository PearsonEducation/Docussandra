package com.pearson.docussandra.controller;

import com.pearson.docussandra.exception.ItemNotFoundException;
import com.strategicgains.restexpress.plugin.swagger.annotations.ApiModelRequest;
import com.wordnik.swagger.annotations.ApiOperation;
import org.restexpress.Request;
import org.restexpress.Response;

import com.pearson.docussandra.Constants;
import com.pearson.docussandra.ServiceUtils;
import com.pearson.docussandra.domain.objects.Document;
import com.pearson.docussandra.domain.objects.LinkableDocument;
import com.pearson.docussandra.domain.objects.Query;
import com.pearson.docussandra.domain.objects.QueryResponseWrapper;
import com.pearson.docussandra.exception.FieldNotIndexedException;
import com.pearson.docussandra.exception.IndexParseException;
import com.pearson.docussandra.service.QueryService;
import com.strategicgains.hyperexpress.HyperExpress;
import com.strategicgains.hyperexpress.builder.TokenBinder;
import com.strategicgains.hyperexpress.builder.TokenResolver;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.restexpress.common.query.QueryRange;
import org.restexpress.query.QueryRanges;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the 'controller' layer, where HTTP details are converted to domain
 * concepts and passed to the service layer. Then service layer response
 * information is enhanced with HTTP details, if applicable, for the response.
 * <p/>
 * This controller demonstrates how to process a Cassandra entity that is
 * identified by a single, primary row key such as a UUID.
 */
public class QueryController
{

    private static final int DEFAULT_LIMIT = 20;
    private static final String ACCEPT_HEADER = "Accept";
    private static final String HAL_HEADER_VALUE = "application/hal+json";
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private QueryService service;

    public QueryController(QueryService queryService)
    {
        super();
        this.service = queryService;
    }

    @ApiOperation(value = "search for queries in the database",
            notes = "please provide what you want to search for in the database in the request body",
            response = Document.class)
    @ApiModelRequest(model = Document.class, required = true, modelName = "Document")
    public Document[] query(Request request, Response response) throws IndexParseException
    {
//        Set<String> headers = request.getHeaderNames();
//        for(String header : headers){
//            logger.info(header + ":" + request.getHeader(header));
//        }
        String database = request.getHeader(Constants.Url.DATABASE, "No database provided");
        String table = request.getHeader(Constants.Url.TABLE, "No table provided");
        boolean returnHAL = false;
        String acceptHeader = request.getHeader(ACCEPT_HEADER);
        if (acceptHeader != null && acceptHeader.equalsIgnoreCase(HAL_HEADER_VALUE))
        {
            returnHAL = true;
        }
        Query toQuery = request.getBodyAs(Query.class, "Query details not provided");
        toQuery.setDatabase(database);
        toQuery.setTable(table);//change of plans, no longer getting it from the query object, but from the URL instead

        QueryRange range = QueryRanges.parseFrom(request);

        int limit = DEFAULT_LIMIT;//TODO: enforce maximum limit https://github.com/PearsonEducation/Docussandra/issues/14
        long offset = 0;
        if (range != null)
        {
            if (range.hasLimit())
            {
                limit = range.getLimit();
            } else
            {
                range.setLimit(limit);
            }
            if (range.hasOffset())
            {
                offset = range.getOffset();
            } else
            {
                range.setOffset(offset);
            }
        }
        try
        {
            QueryResponseWrapper queryResponse = service.query(toQuery, limit, offset);
            if (queryResponse.isEmpty())
            {
                response.setCollectionResponse(range, 0, 0);
            } else if (queryResponse.getNumAdditionalResults() == null)
            {//we have more results, but an unknown number more
                response.setCollectionResponse(range, queryResponse.size(), -1);
                response.setResponseStatus(HttpResponseStatus.PARTIAL_CONTENT);
            } else if (queryResponse.getNumAdditionalResults() == 0l)
            {//we have no more results, the amount returned is the number that exists
                response.setCollectionResponse(range, queryResponse.size(), queryResponse.size());
            } else// not likely to actually happen given our implementation, but coding in case our backend changes
            {
                response.setCollectionResponse(range, queryResponse.size(), queryResponse.size() + queryResponse.getNumAdditionalResults());
            }
            logger.debug("Query: " + toQuery.toString() + " returned " + queryResponse.size() + " documents.");
            if (returnHAL)
            {
                HyperExpress.tokenBinder(new TokenBinder<LinkableDocument>()
                {
                    @Override
                    public void bind(LinkableDocument object, TokenResolver resolver)
                    {
                        resolver.bind(Constants.Url.DOCUMENT_ID, object.getUuid().toString());
                    }
                });
                //HyperExpress will return this properly as an array, but not an arraylist!?
                LinkableDocument[] toReturn = new LinkableDocument[queryResponse.size()];
                for (int i = 0; i < queryResponse.size(); i++)
                {
                    toReturn[i] = new LinkableDocument(queryResponse.get(i));
                }
                return toReturn;
            } else
            {
                return queryResponse.toArray(new Document[0]);
            }

        } catch (IndexParseException | FieldNotIndexedException e)
        {
            if (!service.checkDatabase(toQuery.database().getId()))
            {
                throw new ItemNotFoundException("Database not found: " + toQuery.getDatabase());
            }
            //check if the table exits
            if (!service.checkTable(toQuery.getTableAsObject().getId()))
            {
                throw new ItemNotFoundException("Table not found: " + toQuery.getTable());
            }
            //Explicitly throw an 400
            ServiceUtils.setBadRequestExceptionToResponse(e, response);
            return null;
        } catch (Exception e) {
            logger.error("Problem querying ", e);
            throw e;
        }
    }
}
