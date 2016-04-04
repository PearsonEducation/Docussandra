package com.pearson.docussandra.controller;

import java.util.List;

import com.strategicgains.restexpress.plugin.swagger.annotations.ApiModelRequest;
import com.wordnik.swagger.annotations.ApiOperation;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.restexpress.Request;
import org.restexpress.Response;

import com.pearson.docussandra.Constants;
import com.pearson.docussandra.event.IndexCreatedEvent;
import com.pearson.docussandra.service.IndexService;
import com.strategicgains.hyperexpress.HyperExpress;
import com.strategicgains.hyperexpress.builder.TokenBinder;
import com.strategicgains.hyperexpress.builder.TokenResolver;
import com.strategicgains.hyperexpress.builder.UrlBuilder;
import java.util.UUID;

import org.restexpress.exception.NotFoundException;
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
public class IndexStatusController
{

    private static final UrlBuilder LOCATION_BUILDER = new UrlBuilder();
    private static final Logger logger = LoggerFactory.getLogger(IndexStatusController.class);

    private IndexService indexes;

    public IndexStatusController(IndexService indexService)
    {
        super();
        this.indexes = indexService;
    }

    /**
     * Gets the status for an index creation request.
     *
     * @param request
     * @param response
     * @return
     */
    @ApiOperation(value = "get the index status",
            notes = " ",
            response = IndexCreatedEvent.class)
    @ApiModelRequest(model = IndexCreatedEvent.class, required = true, modelName = "IndexCreatedEvent")
    public IndexCreatedEvent read(Request request, Response response)
    {
        String id = request.getHeader(Constants.Url.INDEX_STATUS, "No index status id provided.");

        IndexCreatedEvent status = indexes.status(UUID.fromString(id));

        HyperExpress.bind(Constants.Url.TABLE, status.getIndex().getTableName())
                .bind(Constants.Url.DATABASE, status.getIndex().getDatabaseName())
                .bind(Constants.Url.INDEX, status.getIndex().getName())
                .bind(Constants.Url.INDEX_STATUS, status.getUuid().toString());
        return status;
    }

    /**
     * Gets all presently active index creation requests.
     *
     * @param request
     * @param response
     * @return
     */
    @ApiOperation(value = "get all the index statues",
            notes = " ",
            response = IndexCreatedEvent.class)
    @ApiModelRequest(model = IndexCreatedEvent.class, required = true, modelName = "IndexCreatedEvent")
    public List<IndexCreatedEvent> readAll(Request request, Response response)
    {
        List<IndexCreatedEvent> status = indexes.getAllCurrentlyIndexing();
        HyperExpress.tokenBinder(new TokenBinder<IndexCreatedEvent>()
        {
            @Override
            public void bind(IndexCreatedEvent object, TokenResolver resolver)
            {
                resolver.bind(Constants.Url.TABLE, object.getIndex().getTableName())
                        .bind(Constants.Url.DATABASE, object.getIndex().getDatabaseName())
                        .bind(Constants.Url.INDEX, object.getIndex().getName())
                        .bind(Constants.Url.INDEX_STATUS, object.getUuid().toString());

            }
        });
        return status;
    }

    /**
     * Gets all presently active index creation requests
     *
     * @param request
     * @param response
     * @return
     */
    @ApiOperation(value = "gets all presently active index creation requests",
            notes = "use this route to get all presently active index creation requests ",
            response = IndexCreatedEvent.class)
    @ApiModelRequest(model = IndexCreatedEvent.class, required = true, modelName = "IndexCreatedEvent")
    public List<IndexCreatedEvent> getTableIndexStatus(Request request, Response response)
    {
        logger.error("This route "+ request.getUrl() +" has not been completed yet");
        response.setResponseStatus(HttpResponseStatus.NOT_FOUND);
        throw new NotFoundException("this route in under construction!!");
    }

}
