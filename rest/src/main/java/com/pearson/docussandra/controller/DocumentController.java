package com.pearson.docussandra.controller;

import com.strategicgains.restexpress.plugin.swagger.annotations.ApiModelRequest;
import com.wordnik.swagger.annotations.ApiOperation;
import io.netty.handler.codec.http.HttpMethod;

import org.restexpress.ContentType;
import org.restexpress.Request;
import org.restexpress.Response;
import org.restexpress.exception.BadRequestException;

import com.pearson.docussandra.Constants;
import com.pearson.docussandra.ServiceUtils;
import com.pearson.docussandra.domain.objects.Database;
import com.pearson.docussandra.domain.objects.Document;
import com.pearson.docussandra.domain.objects.Identifier;
import com.pearson.docussandra.domain.objects.LinkableDocument;
import com.pearson.docussandra.exception.IndexParseException;
import com.pearson.docussandra.plugininterfaces.NotifierPluginInterface;
import com.pearson.docussandra.plugins.PluginHolder;
import com.pearson.docussandra.service.DocumentService;
import com.strategicgains.hyperexpress.HyperExpress;
import com.strategicgains.hyperexpress.builder.TokenResolver;
import com.strategicgains.hyperexpress.builder.UrlBuilder;
import java.util.ArrayList;
import java.util.UUID;

public class DocumentController
{

    private static final UrlBuilder LOCATION_BUILDER = new UrlBuilder();

    private DocumentService documents;
    
    private ArrayList<NotifierPluginInterface> plugins;

    public DocumentController(DocumentService documentsService)
    {
        super();
        this.documents = documentsService;
        this.plugins = PluginHolder.getInstance().getNotifierPlugins();
    }

    @ApiOperation(value = "create document",
            notes = "This method creates a document",
            response = Document.class)
    @ApiModelRequest(model = Document.class, required = false, modelName = "Document")
    public Document create(Request request, Response response)
    {
        String database = request.getHeader(Constants.Url.DATABASE, "No database provided");
        String table = request.getHeader(Constants.Url.TABLE, "No table provided");
        String data = request.getBody().toString(ContentType.CHARSET);

        if (data == null || data.isEmpty())
        {
            throw new BadRequestException("No document data provided");
        }

        try
        {
            Document saved = documents.create(database, table, data);

            // Construct the response for create...
            response.setResponseCreated();

            // enrich the resource with links, etc. here...
            TokenResolver resolver = HyperExpress.bind(Constants.Url.DOCUMENT_ID, saved.getUuid().toString());

            // Include the Location header...
            String locationPattern = request.getNamedUrl(HttpMethod.GET, Constants.Routes.DOCUMENT);
            response.addLocationHeader(LOCATION_BUILDER.build(locationPattern, resolver));

            // Return the newly-created resource...
            return new LinkableDocument(saved);
        } catch (IndexParseException e)
        {
            ServiceUtils.setBadRequestExceptionToResponse(e, response);
            return null;
        }
    }

    @ApiOperation(value = "read document",
            notes = "This will return the details of the document provided in the route",
            response = Document.class)
    @ApiModelRequest(model = Document.class, required = true, modelName = "Document")
    public Document read(Request request, Response response)
    {
        String database = request.getHeader(Constants.Url.DATABASE, "No database provided");
        String table = request.getHeader(Constants.Url.TABLE, "No table provided");
        String id = request.getHeader(Constants.Url.DOCUMENT_ID, "No document ID supplied");
        Document document = documents.read(database, table, new Identifier(database, table, UUID.fromString(id)));

        // enrich the entity with links, etc. here...
        HyperExpress.bind(Constants.Url.DOCUMENT_ID, document.getUuid().toString());

        return new LinkableDocument(document);
    }

//	public List<Document> readAll(Request request, Response response)
//	{
//		String database = request.getHeader(Constants.Url.DATABASE, "No database provided");
//		String collection = request.getHeader(Constants.Url.TABLE, "No collection provided");
//
//		HyperExpress.tokenBinder(new TokenBinder<Document>()
//		{
//			@Override
//            public void bind(Document object, TokenResolver resolver)
//            {
//				resolver.bind(Constants.Url.DOCUMENT_ID, object.getUuid().toString());
//            }
//		});
//
//		return service.readAll(database, collection);
//	}

    @ApiOperation(value = "update a document",
            notes = "This route should be used to update the details of the document",
            response = Document.class)
    @ApiModelRequest(model = Document.class, required = true, modelName = "Document")
    public void update(Request request, Response response)
    {
        String database = request.getHeader(Constants.Url.DATABASE, "No database provided");
        String table = request.getHeader(Constants.Url.TABLE, "No table provided");
        String id = request.getHeader(Constants.Url.DOCUMENT_ID, "No document ID supplied");
        String data = request.getBody().toString(ContentType.CHARSET);

        if (data == null || data.isEmpty())
        {
            throw new BadRequestException("No document data provided");
        }

        Document document = new Document();
        document.setUuid(UUID.fromString(id));
        document.table(database, table);
        document.objectAsString(data);
        documents.update(document);
        response.setResponseNoContent();
    }

    @ApiOperation(value = "read all the databases",
            notes = "delete the document Warning: once the document is deleted cant be restored",
            response = Database.class)
    @ApiModelRequest(model = Database.class, required = true, modelName = "Database")
    public void delete(Request request, Response response)
    {
        String database = request.getHeader(Constants.Url.DATABASE, "No database provided");
        String table = request.getHeader(Constants.Url.TABLE, "No table provided");
        String id = request.getHeader(Constants.Url.DOCUMENT_ID, "No document ID supplied");
        documents.delete(database, table, new Identifier(database, table, UUID.fromString(id)));
        response.setResponseNoContent();
    }
}
