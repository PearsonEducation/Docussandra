
package com.pearson.docussandra;

import static com.pearson.docussandra.Constants.Routes.DATABASE;
import static com.pearson.docussandra.Constants.Routes.DATABASES;
import static com.pearson.docussandra.Constants.Routes.DOCUMENT;
import static com.pearson.docussandra.Constants.Routes.DOCUMENTS;
import static com.pearson.docussandra.Constants.Routes.INDEX;
import static com.pearson.docussandra.Constants.Routes.TABLE;
import static com.pearson.docussandra.Constants.Routes.TABLES;
import static com.pearson.docussandra.Constants.Routes.INDEXES;
import static com.pearson.docussandra.Constants.Routes.INDEX_STATUS;
import static com.strategicgains.hyperexpress.RelTypes.SELF;
import static com.strategicgains.hyperexpress.RelTypes.UP;

import java.util.Map;

import org.restexpress.RestExpress;

import com.pearson.docussandra.domain.event.IndexCreatedEvent;
import com.pearson.docussandra.domain.objects.Database;
import com.pearson.docussandra.domain.objects.Index;
import com.pearson.docussandra.domain.objects.LinkableDocument;
import com.pearson.docussandra.domain.objects.Table;
import com.strategicgains.hyperexpress.HyperExpress;

/**
 * Warning: Do not code format this file, it will make it harder to read.
 * @author https://github.com/tfredrich
 * @since Jun 12, 2014
 */
public class Relationships
{
	public static void define(RestExpress server)
	{
		Map<String, String> routes = server.getRouteUrlsByName();

		HyperExpress.relationships()
		.forCollectionOf(Database.class)
			.rel(SELF, routes.get(DATABASES))

		.forClass(Database.class)
			.rel(SELF, routes.get(DATABASE))
			.rel(UP, routes.get(DATABASES))
			.rel("collections", routes.get(TABLES))
				.title("The tables in this database")

		.forCollectionOf(Table.class)
			.rel(SELF, routes.get(TABLES))
			.rel(UP, routes.get(DATABASE))
				.title("The database containing this table")

		.forClass(Table.class)
			.rel(SELF, routes.get(TABLE))
			.rel(UP, routes.get(TABLES))
				.title("The entire list of tables in this database")

                .forCollectionOf(Index.class)
			.rel(SELF, routes.get(INDEXES))
			.rel(UP, routes.get(TABLE))
			.title("The collection that this index was created on.")
        
                .forClass(Index.class)
			.rel(SELF, routes.get(INDEX))
			.rel(UP, routes.get(INDEXES))
				.title("The list of indexes for this table.")
                
                   //N/A -- this is a global status of all current indexing operations
//                .forCollectionOf(IndexCreatedEvent.class)
//			.rel(SELF, routes.get(INDEX))
//			.rel(UP, routes.get(INDEXES))
                        
                .forClass(IndexCreatedEvent.class)
			.rel(SELF, routes.get(INDEX_STATUS))
			.rel(UP, routes.get(INDEX))
                        .rel("index", routes.get(INDEX))
				.title("The index for this status.")
                
			.rel("documents", routes.get(DOCUMENTS))
				.title("The documents in this collection")
                        
		.forCollectionOf(LinkableDocument.class)
//			.rel(SELF, routes.get(DOCUMENTS))
			.rel(UP, routes.get(TABLE))
				.title("The collection containing these documents")

		.forClass(LinkableDocument.class)
			.rel(SELF, routes.get(DOCUMENT))
			.rel(UP, routes.get(DOCUMENTS))
				.title("The entire list of documents in this collection");
	}
}
