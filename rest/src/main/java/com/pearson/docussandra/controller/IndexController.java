package com.pearson.docussandra.controller;

import com.strategicgains.restexpress.plugin.swagger.annotations.ApiModelRequest;
import com.wordnik.swagger.annotations.ApiOperation;
import io.netty.handler.codec.http.HttpMethod;

import java.util.List;

import org.restexpress.Request;
import org.restexpress.Response;

import com.pearson.docussandra.Constants;
import com.pearson.docussandra.domain.event.IndexCreatedEvent;
import com.pearson.docussandra.domain.objects.Identifier;
import com.pearson.docussandra.domain.objects.Index;
import com.pearson.docussandra.domain.objects.Table;
import com.pearson.docussandra.service.IndexService;
import com.strategicgains.hyperexpress.HyperExpress;
import com.strategicgains.hyperexpress.builder.TokenBinder;
import com.strategicgains.hyperexpress.builder.TokenResolver;
import com.strategicgains.hyperexpress.builder.UrlBuilder;
import java.util.ArrayList;
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
public class IndexController
{

    private static final UrlBuilder LOCATION_BUILDER = new UrlBuilder();
    private static final Logger LOGGER = LoggerFactory.getLogger(IndexController.class);

    private IndexService indexes;

    public IndexController(IndexService indexService)
    {
        super();
        this.indexes = indexService;
    }

    @ApiOperation(value = "create an index",
            notes = "use this route to create a record",
            response = IndexCreatedEvent.class)
    @ApiModelRequest(model = IndexCreatedEvent.class, required = false, modelName = "IndexCreatedEvent")
    public IndexCreatedEvent create(Request request, Response response) throws Exception
    {
        String database = request.getHeader(Constants.Url.DATABASE, "No database provided");
        String table = request.getHeader(Constants.Url.TABLE, "No table provided");
        String name = request.getHeader(Constants.Url.INDEX, "No index name provided");
        //TODO: check name passed in with url and name in domain object https://github.com/PearsonEducation/Docussandra/issues/11

        Index entity = request.getBodyAs(Index.class, "Resource details not provided");

        Table t = new Table();
        t.setDatabaseByString(database);
        t.setName(table);
        entity.setTable(t);
        entity.setName(name);
        if (entity.getIncludeOnly() == null)
        {
            entity.setIncludeOnly(new ArrayList<String>(0));
        }
        IndexCreatedEvent status;
        try
        {
            status = indexes.create(entity);
        } catch (Exception e)
        {
            LOGGER.error("Could not save index", e);
            throw e;
        }
        // Construct the response for create...
        response.setResponseCreated();

        // enrich the resource with links, etc. here...
        TokenResolver resolver = HyperExpress.bind(Constants.Url.TABLE, status.getIndex().getTableName())
                .bind(Constants.Url.DATABASE, status.getIndex().getDatabaseName())
                .bind(Constants.Url.INDEX, status.getIndex().getName())
                .bind(Constants.Url.INDEX_STATUS, status.getUuid().toString());

        // Include the Location header...
        String locationPattern = request.getNamedUrl(HttpMethod.GET, Constants.Routes.INDEX);
        response.addLocationHeader(LOCATION_BUILDER.build(locationPattern, resolver));
        // Return the newly-created resource...
        return status;
    }

    @ApiOperation(value = "read an index",
            notes = "this route is for getting the details for an index",
            response = Index.class)
    @ApiModelRequest(model = Index.class, required = false, modelName = "Index")
    public Index read(Request request, Response response)
    {
        String database = request.getHeader(Constants.Url.DATABASE, "No database provided");
        String table = request.getHeader(Constants.Url.TABLE, "No table provided");
        String name = request.getHeader(Constants.Url.INDEX, "No index name provided");
        Index entity = indexes.read(new Identifier(database, table, name));

        // enrich the entity with links, etc. here...
        HyperExpress.bind(Constants.Url.TABLE, entity.getTableName())
                .bind(Constants.Url.DATABASE, entity.getDatabaseName())
                .bind(Constants.Url.INDEX, entity.getName());
//add: .bind(Constants.Url.INDEX_STATUS, status.getUuid().toString());//we don't have this information without doing another lookup
        return entity;
    }

    @ApiOperation(value = "read all the indexes",
            notes = "use this route to get all the indexes",
            response = Index.class)
    @ApiModelRequest(model = Index.class, required = false, modelName = "Index")
    public List<Index> readAll(Request request, Response response)
    {
        String database = request.getHeader(Constants.Url.DATABASE, "No database provided");
        String table = request.getHeader(Constants.Url.TABLE, "No table provided");

        HyperExpress.tokenBinder(new TokenBinder<Index>()
        {
            @Override
            public void bind(Index object, TokenResolver resolver)
            {
                resolver.bind(Constants.Url.TABLE, object.getTableName())
                        .bind(Constants.Url.DATABASE, object.getDatabaseName())
                        .bind(Constants.Url.INDEX, object.getName());
                //add: .bind(Constants.Url.INDEX_STATUS, status.getUuid().toString());//we don't have this information without doing another lookup

            }
        });

        return indexes.readAll(database, table);
    }

    @ApiOperation(value = "delete the index",
            notes = "delete the index Warning: once the index is deleted cant be restored")
    @ApiModelRequest(model = Index.class, required = false, modelName = "Index")
    public void delete(Request request, Response response)
    {
        String database = request.getHeader(Constants.Url.DATABASE, "No database provided");
        String table = request.getHeader(Constants.Url.TABLE, "No table provided");
        String name = request.getHeader(Constants.Url.INDEX, "No index name provided");
        indexes.delete(new Identifier(database, table, name));
        response.setResponseNoContent();
    }
}
