package com.pearson.docussandra;

public class Constants
{

    /**
     * These define the URL parameters used in the route definition strings
     * (e.g. '{userId}').
     */
    public static class Url
    {

        public static final String UUID = "uuid";
        public static final String KEY1 = "key1";
        public static final String KEY2 = "key2";
        public static final String KEY3 = "key3";
        public static final String DATABASE = "database";
        public static final String TABLE = "table";
        public static final String INDEX = "index";
        public static final String INDEX_STATUS = "status_id";
        public static final String DOCUMENT_ID = "documentId";
        public static final String QUERY_ID = "queryId";
    }

    /**
     * These define the route names used in naming each route definitions. These
     * names are used to retrieve URL patterns within the controllers by name to
     * create links in responses.
     */
    public static class Routes
    {

        public static final String SINGLE_UUID_SAMPLE = "sample.uuid.single";
        public static final String SINGLE_COMPOUND_SAMPLE = "sample.compound.single";
        public static final String SAMPLE_UUID_COLLECTION = "sample.uuid.collection";
        public static final String SAMPLE_COMPOUND_COLLECTION = "sample.compound.collection";
        public static final String DATABASES = "database.collection";
        public static final String DATABASE = "database.single";
        public static final String TABLES = "table.collection";
        public static final String TABLE = "table.single";
        public static final String DOCUMENTS = "document.collection";
        public static final String DOCUMENT = "document.single";
        public static final String INDEXES = "index.collection";
        public static final String INDEX = "index.single";
        public static final String INDEX_STATUS = "index.status";
        public static final String INDEX_STATUS_ALL = "index.status.collection";
        public static final String INDEX_TABLE_STATUS = "index.status.collection";
        public static final String QUERIES = "query.collection";
        public static final String QUERY = "query.single";
        public static final String HEALTH = "health";
        public static final String BUILD_INFO = "build.info";
    }
}
