package com.pearson.docussandra.controller;

import static com.pearson.docussandra.Constants.Routes.DATABASE;
import static com.pearson.docussandra.Constants.Routes.DATABASES;

import com.strategicgains.restexpress.plugin.swagger.annotations.ApiModelRequest;
import com.wordnik.swagger.annotations.ApiOperation;

import java.util.List;

import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import org.restexpress.Request;
import org.restexpress.Response;

import com.pearson.docussandra.Constants;
import com.pearson.docussandra.domain.objects.Database;
import com.pearson.docussandra.service.DatabaseService;
import com.strategicgains.hyperexpress.HyperExpress;
import com.strategicgains.hyperexpress.builder.TokenBinder;
import com.strategicgains.hyperexpress.builder.TokenResolver;
import com.strategicgains.hyperexpress.builder.UrlBuilder;

/**
 * REST controller for Database entities.
 */
public class DatabaseController
{

    private static final UrlBuilder LOCATION_BUILDER = new UrlBuilder();

    private DatabaseService databases;

    public DatabaseController(DatabaseService databaseService)
    {
        super();
        this.databases = databaseService;
    }

    @ApiOperation(value = "OPTIONS database",
            notes = "this http verb is used give us what other verbs can be used")
    public void options(Request request, Response response)
    {
        if (DATABASES.equals(request.getResolvedRoute().getName()))
        {
            response.addHeader(HttpHeaders.Names.ALLOW, "GET");
        } else if (DATABASE.equals(request.getResolvedRoute().getName()))
        {
            response.addHeader(HttpHeaders.Names.ALLOW, "GET, DELETE, PUT, POST");
        }
    }

    @ApiOperation(value = "Create database",
            notes = "This  method creates a  table it accepts name and description in the request body but both are optional",
            response = Database.class)
    @ApiModelRequest(model = Database.class, required = false, modelName = "Database")
    public Database create(Request request, Response response)
    {
        String name = request.getHeader(Constants.Url.DATABASE, "No database name provided");
        Database database = request.getBodyAs(Database.class);

        if (database == null)
        {
            database = new Database();
        }

        database.name(name);
        Database saved = databases.create(database);

        // Construct the response for create...
        response.setResponseCreated();

        // enrich the resource with links, etc. here...
        TokenResolver resolver = HyperExpress.bind(Constants.Url.DATABASE, saved.name());

        // Include the Location header...
        String locationPattern = request.getNamedUrl(HttpMethod.GET, Constants.Routes.DATABASE);
        response.addLocationHeader(LOCATION_BUILDER.build(locationPattern, resolver));

        // Return the newly-created resource...
        return saved;
    }

    @ApiOperation(value = "read a database",
            notes = "This will return the details of the database provided in the route",
            response = Database.class)
    @ApiModelRequest(model = Database.class, required = false, modelName = "Database")
    public Database read(Request request, Response response)
    {
        String name = request.getHeader(Constants.Url.DATABASE, "No database provided");
        Database database = databases.read(name);

        // enrich the entity with links, etc. here...
        HyperExpress.bind(Constants.Url.DATABASE, database.name());

        return database;
    }

    @ApiOperation(value = "read all the databases",
            notes = "This route will return all the database created",
            response = Database.class)
    @ApiModelRequest(model = Database.class, required = false, modelName = "Database")
    public List<Database> readAll(Request request, Response response)
    {
        HyperExpress.tokenBinder(new TokenBinder<Database>()
        {
            @Override
            public void bind(Database object, TokenResolver resolver)
            {
                resolver.bind(Constants.Url.DATABASE, object.name());
            }
        });
        return databases.readAll();
    }

    @ApiOperation(value = "read all the databases",
            notes = "This route should be used to update the details of the database",
            response = Database.class)
    @ApiModelRequest(model = Database.class, required = false, modelName = "Database")
    public void update(Request request, Response response)
    {
        String name = request.getHeader(Constants.Url.DATABASE, "No database name provided");
        Database database = request.getBodyAs(Database.class, "Database details not provided");

        database.name(name);
        databases.update(database);
        response.setResponseNoContent();
    }

    @ApiOperation(value = "read all the databases",
            notes = "delete the  database Warning: once the  database is deleted cant be restored",
            response = Database.class)
    @ApiModelRequest(model = Database.class, required = false, modelName = "Database")
    public void delete(Request request, Response response)
    {
        String name = request.getHeader(Constants.Url.DATABASE, "No database name provided");
        databases.delete(name);
        response.setResponseNoContent();
    }
}
