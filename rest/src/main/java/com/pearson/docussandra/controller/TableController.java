package com.pearson.docussandra.controller;

import com.strategicgains.restexpress.plugin.swagger.annotations.ApiModelRequest;
import com.wordnik.swagger.annotations.ApiOperation;
import io.netty.handler.codec.http.HttpMethod;

import java.util.List;

import org.restexpress.Request;
import org.restexpress.Response;

import com.pearson.docussandra.Constants;
import com.pearson.docussandra.domain.objects.Identifier;
import com.pearson.docussandra.domain.objects.Table;
import com.pearson.docussandra.service.TableService;
import com.strategicgains.hyperexpress.HyperExpress;
import com.strategicgains.hyperexpress.builder.TokenBinder;
import com.strategicgains.hyperexpress.builder.TokenResolver;
import com.strategicgains.hyperexpress.builder.UrlBuilder;

public class TableController
{

    private static final UrlBuilder LOCATION_BUILDER = new UrlBuilder();

    private TableService service;

    public TableController(TableService collectionsService)
    {
        super();
        this.service = collectionsService;
    }

    @ApiOperation(value = "create a  table",
            notes = "This  method creates a  table it accepts name and description in the request body but both are optional",
            response =  Table.class)
    @ApiModelRequest(model =  Table.class, required = false, modelName = " Table")
    public Table create(Request request, Response response)
    {
        String databaseName = request.getHeader(Constants.Url.DATABASE, "No database provided");
        String tableName = request.getHeader(Constants.Url.TABLE, "No table provided");
        Table table = request.getBodyAs(Table.class);

        if (table == null)
        {
            table = new Table();
        }

        table.database(databaseName);
        table.name(tableName);
        Table saved = service.create(table);

        // Construct the response for create...
        response.setResponseCreated();

        // enrich the resource with links, etc. here...
        TokenResolver resolver = HyperExpress.bind(Constants.Url.TABLE, saved.getId().getTableName());

        // Include the Location header...
        String locationPattern = request.getNamedUrl(HttpMethod.GET, Constants.Routes.TABLE);
        response.addLocationHeader(LOCATION_BUILDER.build(locationPattern, resolver));

        // Return the newly-created resource...
        return saved;
    }

    @ApiOperation(value = "read a particular  table",
            notes = "This will return the details of the table provided in the route",
            response =  Table.class)
    @ApiModelRequest(model =  Table.class, required = false, modelName = " Table")
    public Table read(Request request, Response response)
    {
        String databaseName = request.getHeader(Constants.Url.DATABASE, "No database provided");
        String tableName = request.getHeader(Constants.Url.TABLE, "No table supplied");

        Table table = service.read(databaseName, tableName);

        // enrich the entity with links, etc. here...
        HyperExpress.bind(Constants.Url.TABLE, table.getId().getTableName());

        return table;
    }

    @ApiOperation(value = "read all the  tables",
            notes = "This route will return all the table created",
            response =  Table.class)
    @ApiModelRequest(model =  Table.class, required = false, modelName = " Table")
    public List<Table> readAll(Request request, Response response)
    {
        String databaseName = request.getHeader(Constants.Url.DATABASE, "No database provided");

        HyperExpress.tokenBinder(new TokenBinder<Table>()
        {
            @Override
            public void bind(Table object, TokenResolver resolver)
            {
                resolver.bind(Constants.Url.TABLE, object.getId().getTableName());
            }
        });
        return service.readAll(databaseName);
    }

    @ApiOperation(value = "update the  table",
            notes = "This route should be used to update the details of the table",
            response =  Table.class)
    @ApiModelRequest(model =  Table.class, required = false, modelName = " Table")
    public void update(Request request, Response response)
    {
        String databaseName = request.getHeader(Constants.Url.DATABASE, "No database provided");
        String tableName = request.getHeader(Constants.Url.TABLE, "No table provided");
        Table table = request.getBodyAs(Table.class, "Table details not provided");
        table.database(databaseName);
        table.name(tableName);
        service.update(table);
        response.setResponseNoContent();
    }

    @ApiOperation(value = "delete the  table",
            notes = "delete the table Warning: once the table is deleted cant be restored",
            response =  Table.class)
    @ApiModelRequest(model =  Table.class, required = false, modelName = " Table")
    public void delete(Request request, Response response)
    {
        String databaseName = request.getHeader(Constants.Url.DATABASE, "No database provided");
        String tableName = request.getHeader(Constants.Url.TABLE, "No table provided");
        service.delete(new Identifier(databaseName, tableName));
        response.setResponseNoContent();
    }
}
