package com.pearson.docussandra;

import static io.netty.handler.codec.http.HttpMethod.DELETE;
import static io.netty.handler.codec.http.HttpMethod.GET;
import static io.netty.handler.codec.http.HttpMethod.POST;
import static io.netty.handler.codec.http.HttpMethod.PUT;

import org.restexpress.RestExpress;

import com.pearson.docussandra.config.Configuration;

public abstract class Routes
{

    public static void define(Configuration config, RestExpress server)
    {
        //health check        
        server.uri("/admin/health", config.getHealthController())
                .action("getHealth", GET)
                .name(Constants.Routes.HEALTH).noSerialization();
        //build info via GET
        server.uri("/admin/buildInfo", config.getBuildInfoController())
                .alias("/admin/buildinfo")
                .action("getBuildInfo", GET)
                .name(Constants.Routes.BUILD_INFO).noSerialization();

        //note: the /index_status route is for ALL index status, not isolated to a specific dataset;
        //this is intentional, it will allow ALL teams to see what ongoing operations that are occuring
        //to better understand the current load on the system and help with estimating completion of tasks
        server.uri("/admin/index_status", config.getIndexStatusController())
                .action("readAll", GET)
                .name(Constants.Routes.INDEX_STATUS_ALL);

        /**
         * route to get all the database information
         */
        server.uri("/databases", config.getDatabaseController())
                .alias("/d")
                .action("readAll", GET)
                .name(Constants.Routes.DATABASES);

        /**
         * route to operate on the {database} depending on the HTTP verb
         */
        server.uri("/databases/{database}", config.getDatabaseController())
                .alias("/d/{database}")
                .method(GET, DELETE, PUT, POST)
                .name(Constants.Routes.DATABASE);

        /**
         * route to get info on all the tables
         */
        server.uri("/databases/{database}/tables", config.getTableController())
                .alias("/d/{database}/t")
                .action("readAll", GET)
                .name(Constants.Routes.TABLES);

        /**
         * route to operate on the {table} depending on the HTTP verb
         */
        server.uri("/databases/{database}/tables/{table}", config.getTableController())
                .alias("/d/{database}/t/{table}")
                .method(GET, DELETE, PUT, POST)
                .name(Constants.Routes.TABLE);

        /**
         * route to post info to documents in the table
         */
        server.uri("/databases/{database}/tables/{table}/documents", config.getDocumentController())
                .alias("/d/{database}/t/{table}/d")
                .method(POST)
                .name(Constants.Routes.DOCUMENTS);

        /**
         * route to get info on all the indexes in the table
         */
        server.uri("/databases/{database}/tables/{table}/indexes", config.getIndexController())
                .alias("/d/{database}/t/{table}/i")
                .action("readAll", GET)
                .name(Constants.Routes.INDEXES);

        /**
         * route to operate on the {table} depending on the HTTP verb
         */
        server.uri("/databases/{database}/tables/{table}/indexes/{index}", config.getIndexController())
                .alias("/d/{database}/t/{table}/i/{index}")
                .method(GET, DELETE, POST)
                .name(Constants.Routes.INDEX);

        /**
         * route to get the indexstatus by the status id
         */
        server.uri("/databases/{database}/tables/{table}/index_status/{status_id}", config.getIndexStatusController())
                .alias("/d/{database}/t/{table}/index_status/{status_id}")
                .method(GET)
                .name(Constants.Routes.INDEX_STATUS);

        /**
         * route to give us the status of a particular index
         */
        server.uri("/databases/{database}/tables/{table}/indexes/{index}/status" , config.getIndexStatusController())
                .alias("/d/{database}/t/{table}/i/{index}/status")
                .action("getTableIndexStatus", GET)
                .name(Constants.Routes.INDEX_TABLE_STATUS);

        /**
         * route to operate on the documents in the tables
         */
        server.uri("/databases/{database}/tables/{table}/documents/{documentId}", config.getDocumentController())
                .alias("/d/{database}/t/{table}/d/{documentId}")
                .method(GET, PUT, DELETE)
                .name(Constants.Routes.DOCUMENT);

        /**
         * route to search for things in the tables
         */
        server.uri("/databases/{database}/tables/{table}/queries", config.getQueryController())
                .alias("/d/{database}/t/{table}/q")
                .action("query", POST)
                .name(Constants.Routes.QUERY);

/*      Possible routes

        server.uri("/databases/{database}/tables/{table}/index_status/", config.getIndexStatusController())
                .action("readAll", GET)
                .name(Constants.Routes.INDEX_STATUS_ALL);//this route isn't supported; we don't have the level of info needed presently

        server.uri("/databases/{database}/tables/{table}/index_status", config.getIndexStatusController())
                .action("readAll", GET)
                .name(Constants.Routes.INDEX_STATUS_ALL);//this route isn't supported we don't have the level of info needed presently

		server.uri("/queries/{queryId}.{format}", config.getQueryController())
			.action("executeSavedQuery", POST)
			.method(GET, PUT, DELETE)
			.name(Constants.Routes.QUERY);

		server.uri("/samples/uuid/{uuid}.{format}", config.getSampleUuidEntityController())
		    .method(GET, PUT, DELETE)
		    .name(Constants.Routes.SINGLE_UUID_SAMPLE);

		server.uri("/samples/uuid.{format}", config.getSampleUuidEntityController())
		    .method(POST)
		    .name(Constants.Routes.SAMPLE_UUID_COLLECTION);

		server.uri("/samples/compound/{key1}/{key2}/{key3}.{format}", config.getSampleCompoundIdentifierEntityController())
		    .method(GET, PUT, DELETE)
		    .name(Constants.Routes.SINGLE_COMPOUND_SAMPLE);

		server.uri("/samples/compound/{key1}/{key2}.{format}", config.getSampleCompoundIdentifierEntityController())
		    .action("readAll", GET)
		    .method(POST)
		    .name(Constants.Routes.SAMPLE_COMPOUND_COLLECTION);
	*/

    }
}
