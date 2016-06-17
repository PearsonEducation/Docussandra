package com.pearson.docussandra.testhelper;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.DriverException;
import com.pearson.docussandra.Utils;
import com.pearson.docussandra.domain.objects.Database;
import com.pearson.docussandra.domain.objects.Document;
import com.pearson.docussandra.domain.objects.FieldDataType;
import com.pearson.docussandra.domain.objects.Index;
import com.pearson.docussandra.domain.objects.IndexField;
import com.pearson.docussandra.domain.event.IndexCreatedEvent;
import com.pearson.docussandra.domain.ParsedQuery;
import com.pearson.docussandra.domain.objects.Query;
import com.pearson.docussandra.domain.objects.Table;
import com.pearson.docussandra.domain.objects.WhereClause;
import com.pearson.docussandra.exception.IndexParseException;
import com.pearson.docussandra.handler.IndexCreatedHandler;
import com.pearson.docussandra.persistence.impl.DatabaseRepositoryImpl;
import com.pearson.docussandra.persistence.impl.DocumentRepositoryImpl;
import com.pearson.docussandra.persistence.ITableRepository;
import com.pearson.docussandra.persistence.impl.ITableRepositoryImpl;
import com.pearson.docussandra.persistence.impl.IndexRepositoryImpl;
import com.pearson.docussandra.persistence.impl.IndexStatusRepositoryImpl;
import com.pearson.docussandra.persistence.impl.TableRepositoryImpl;
import com.strategicgains.eventing.DomainEvents;
import com.strategicgains.eventing.EventBus;
import com.strategicgains.eventing.local.LocalEventBusBuilder;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used for testing only! Allows us to quickly establish a database connection
 * and work with test data.
 *
 * TODO: clean this up a bit; it was hacked together quick
 * https://github.com/PearsonEducation/Docussandra/issues/5
 *
 * @author https://github.com/JeffreyDeYoung
 */
public class Fixtures
{

    private static Fixtures INSTANCE = null;
    public static final String DB = "mydb";
    public static final String DB_CQL = "/docussandra.cql";

    private Session session;
    private final String[] cassandraSeeds;
    private final String cassandraKeyspace;

    private static Logger logger = LoggerFactory.getLogger(Fixtures.class);

    private static IndexRepositoryImpl indexRepo;
    private static ITableRepositoryImpl cleanUpInstance;
    private static DatabaseRepositoryImpl databaseRepo;
    private static DocumentRepositoryImpl docRepo;
    private static TableRepositoryImpl tableRepo;
    private static IndexStatusRepositoryImpl indexStatusRepo;

    /**
     * Private constructor as this is a singleton object
     */
    private Fixtures(String seedsList, boolean mockCassandra) throws Exception
    {
        cassandraKeyspace = "docussandra";
        cassandraSeeds = seedsList.split(",");

        Cluster cluster;
        if (mockCassandra)//using cassandra-unit for testing
        {
            cluster = Fixtures.ensureMockCassandraRunningAndEstablished(cassandraKeyspace);
        } else if (seedsList.startsWith("172.17."))
        {
            cluster = Fixtures.ensureDockerCassandraRunningAndEstablished(cassandraKeyspace, cassandraSeeds[0]);
        } else //using a remote or local server for testing
        {
            cluster = Cluster.builder().addContactPoints(cassandraSeeds).build();
        }
        final Metadata metadata = cluster.getMetadata();
        session = cluster.connect(this.getCassandraKeyspace());
        logger.info("Connected to cluster: " + metadata.getClusterName() + '\n');
        indexRepo = new IndexRepositoryImpl(session);
        cleanUpInstance = new ITableRepositoryImpl(getSession());
        databaseRepo = new DatabaseRepositoryImpl(getSession());
        docRepo = new DocumentRepositoryImpl(getSession());
        tableRepo = new TableRepositoryImpl(getSession());
        indexStatusRepo = new IndexStatusRepositoryImpl(getSession());

        //set up bus just like rest express would
        EventBus bus = new LocalEventBusBuilder()
                .subscribe(new IndexCreatedHandler(indexRepo, indexStatusRepo, docRepo))
                .build();
        DomainEvents.addBus("local", bus);
    }

    /**
     * Get this singleton instance. THIS CLASS IS FOR TESTING ONLY.
     *
     * @return the singleton instance
     */
    public static Fixtures getInstance(String seeds, boolean mockCassandra) throws Exception
    {
        if (INSTANCE == null)
        {
            INSTANCE = new Fixtures(seeds, mockCassandra);
        }
        return INSTANCE;
    }

    /**
     * Get this singleton instance. THIS CLASS IS FOR TESTING ONLY.
     *
     * @return the singleton instance
     */
    public static Fixtures getInstance() throws Exception
    {
        if (INSTANCE == null)
        {
            INSTANCE = new Fixtures("127.0.0.1", true);
        }
        return INSTANCE;
    }

    /**
     * Get this singleton instance. THIS CLASS IS FOR TESTING ONLY.
     *
     * @return the singleton instance
     */
    public static Fixtures getInstance(boolean mockCassandra) throws Exception
    {
        if (INSTANCE == null)
        {
            INSTANCE = new Fixtures("127.0.0.1", mockCassandra);
        }
        return INSTANCE;
    }

    public String getCassandraSeedString()
    {
        return "127.0.0.1";
    }

    public String getCassandraKeyspace()
    {
        return cassandraKeyspace;
    }

    /**
     * Ensures that the Mock Cassandra instance is up and running. Will reinit
     * the database every time it is called.
     *
     * @param cassandraKeyspace Cassandra keyspace to setup.
     * @return A cluster object.
     * @throws ConfigurationException
     * @throws IOException
     * @throws InterruptedException
     * @throws TTransportException
     */
    public static Cluster ensureMockCassandraRunningAndEstablished(String cassandraKeyspace) throws ConfigurationException, IOException, InterruptedException, TTransportException
    {
        Cluster cluster;
        long timeout = 60000;
        EmbeddedCassandraServerHelper.startEmbeddedCassandra(timeout);
        cluster = Cluster.builder().addContactPoints("127.0.0.1").withPort(9142).build();
        //Thread.sleep(20000);//time to let cassandra startup
        final Metadata metadata = cluster.getMetadata();

        Session session = cluster.connect();
        Utils.initDatabase(DB_CQL, session);
        session = cluster.connect(cassandraKeyspace);

        logger.info("Connected to cluster: " + metadata.getClusterName() + '\n');
        return cluster;
    }

    /**
     * Ensures that the Docker Cassandra instance is up and running. Will reinit
     * the database every time it is called.
     *
     * @param cassandraKeyspace Cassandra keyspace to setup.
     * @return A cluster object.
     * @throws ConfigurationException
     * @throws IOException
     * @throws InterruptedException
     * @throws TTransportException
     */
    public static Cluster ensureDockerCassandraRunningAndEstablished(String cassandraKeyspace, String seed) throws ConfigurationException, IOException, InterruptedException, TTransportException
    {
        Cluster cluster = Cluster.builder().addContactPoints(seed).withPort(9042).build();
        //Thread.sleep(20000);//time to let cassandra startup
        final Metadata metadata = cluster.getMetadata();

        Session session = cluster.connect();
        Utils.initDatabase(DB_CQL, session);
        session = cluster.connect(cassandraKeyspace);

        logger.info("Connected to cluster: " + metadata.getClusterName() + '\n');
        return cluster;
    }

    /**
     * Load properties from a property file
     */
    private Properties loadTestProperties() throws IOException
    {
        FileInputStream fis = null;
        try
        {
            String testEnv = System.getProperty("TEST_ENV") != null ? System.getProperty("TEST_ENV") : "local";
            File envFile = new File("config/" + testEnv + "/environment.properties");
            Properties properties = new Properties();
            fis = new FileInputStream(envFile);
            properties.load(fis);
            return properties;
        } finally
        {
            try
            {
                if (fis != null)
                {
                    fis.close();
                }
            } catch (IOException e)
            {
                // too late to care at this point
                throw new IOException("could not load the test environment properties");
            }
        }
    }

    /**
     * Gets our standard bulk documents for testing.
     *
     * @return A list of test documents.
     * @throws IOException If the file containing the documents can't be read.
     * @throws ParseException If the file containing the document isn't valid
     * json.
     */
    public static List<Document> getBulkDocuments() throws IOException, ParseException
    {
        return getBulkDocuments("./src/test/resources/documents.json", createTestTable());
    }

    /**
     * Loads a JSON document array as a list of individual documents.
     *
     * @param path File path where the json document is located.
     * @param t table that the documents should be associated with\.
     * @return A list of documents from the file.
     * @throws IOException If the file can't be read.
     * @throws ParseException If the file is not valid JSON.
     */
    public static List<Document> getBulkDocuments(String path, Table t) throws IOException, ParseException
    {
        //if (bulkDocs == null)
        //{
        JSONParser parser = new JSONParser();
        logger.info("Data path: " + new File(path).getAbsolutePath());
        JSONObject jsonObject = (JSONObject) parser.parse(new FileReader(path));
        JSONArray docs = (JSONArray) jsonObject.get("documents");
        List<Document> toReturn = new ArrayList<>(docs.size());
        for (int i = 0; i < docs.size(); i++)
        {
            Document doc = new Document();
            doc.setTable(t);
            doc.setUuid(new UUID(Long.MAX_VALUE - i, 1));//give it a UUID that we will recognize
            JSONObject object = (JSONObject) docs.get(i);
            doc.setObjectAsString(object.toJSONString());
            toReturn.add(doc);
        }
        return toReturn;
    }

    /**
     * Loads bulk documents, assuming that there is one document per line in the
     * supplied file path.
     *
     * @param path path (relative to the classpath!) where the input document
     * resides.
     * @param t What table these documents should be associated.
     * @return A list of JSON documents from the file.
     * @throws IOException If the file can't be read.
     * @throws ParseException If one of the documents can't be parsed.
     */
    public static List<Document> getBulkDocumentsByLine(String path, Table t) throws IOException, ParseException
    {
        JSONParser parser = new JSONParser();
        logger.info("Data path: " + new File(path).getAbsolutePath());
        String bulkFile = Utils.readFile(path);
        String[] lines = bulkFile.split("\\Q\n\\E");
        List<Document> toReturn = new ArrayList<>(lines.length);
        for (int i = 0; i < lines.length; i++)
        {
            Document doc = new Document();
            doc.setTable(t);
            doc.setUuid(new UUID(Long.MAX_VALUE - i, 1));//give it a UUID that we will recognize
            JSONObject object = (JSONObject) parser.parse(lines[i]);
            doc.setObjectAsString(object.toJSONString());
            toReturn.add(doc);
        }
        return toReturn;
    }

    /**
     * Creates at test index with two getFields.
     *
     * @return
     */
    public static final Index createTestIndexTwoField()
    {
        Index index = new Index("myindexwithtwofields");
        index.setTable(DB, "mytable");
        ArrayList<IndexField> fields = new ArrayList<>();
        fields.add(new IndexField("myindexedfield1"));
        fields.add(new IndexField("myindexedfield2"));
        index.setFields(fields);
        index.isUnique(false);
        return index;
    }

    /**
     * Creates at test index for world bank loans short country name.
     *
     * @return
     */
    public static final Index createTestBankIndexCountryName()
    {
        Index lastname = new Index("countryname");
        lastname.isUnique(false);
        ArrayList<IndexField> fields = new ArrayList<>(1);
        fields.add(new IndexField("countryshortname"));
        lastname.setFields(fields);
        lastname.setTable(Fixtures.createTestWorldBankTable());
        return lastname;
    }

    /**
     * Creates at test index for loan issue year. Integer type index.
     *
     * @return
     */
    public static final Index createTestBankIndexLoanYear()
    {
        Index lastname = new Index("issueyear");
        lastname.isUnique(false);
        ArrayList<IndexField> fields = new ArrayList<>(1);
        fields.add(new IndexField("approvalfy", FieldDataType.INTEGER));
        lastname.setFields(fields);
        lastname.setTable(Fixtures.createTestWorldBankTable());
        return lastname;
    }

    /**
     * Creates at test index for bank load approved on date. Date type index.
     *
     * @return
     */
    public static final Index createTestBankIndexApprovedOn()
    {
        Index lastname = new Index("approvedon");
        lastname.isUnique(false);
        ArrayList<IndexField> fields = new ArrayList<>(1);
        fields.add(new IndexField("boardapprovaldate", FieldDataType.DATE_TIME));
        lastname.setFields(fields);
        lastname.setTable(Fixtures.createTestWorldBankTable());
        return lastname;
    }

    /**
     * Creates at test index for bank loan approved on date. Timepoint type
     * index.
     *
     * @return
     */
    public static final Index createTestBankIndexApprovedOnTimepoint()
    {
        Index lastname = new Index("createdon");
        lastname.isUnique(false);
        ArrayList<IndexField> fields = new ArrayList<>(1);
        fields.add(new IndexField("boardapprovaldate", FieldDataType.TIMEPOINT));
        lastname.setFields(fields);
        lastname.setTable(Fixtures.createTestWorldBankTable());
        return lastname;
    }

    /**
     * Creates at test index with one field.
     *
     * @return
     */
    public static final Index createTestIndexOneField()
    {
        Index index = new Index("myindexwithonefield");
        index.setTable(DB, "mytable");
        ArrayList<IndexField> fields = new ArrayList<>();
        fields.add(new IndexField("myindexedfield"));
        index.setFields(fields);
        index.isUnique(false);
        return index;
    }

    /**
     * Creates at test index with a numeric field.
     *
     * @return
     */
    public static final Index createTestIndexNumericField()
    {
        Index index = new Index("myindexnumericfield");
        index.setTable(DB, "mytable");
        ArrayList<IndexField> fields = new ArrayList<>();
        fields.add(new IndexField("myindexedfield3", FieldDataType.INTEGER));
        index.setFields(fields);
        index.isUnique(false);
        return index;
    }

    /**
     * Creates at test index with a UUID field.
     *
     * @return
     */
    public static final Index createTestIndexUUIDField()
    {
        Index index = new Index("myindexuuidfield");
        index.setTable(DB, "mytable");
        ArrayList<IndexField> fields = new ArrayList<>();
        fields.add(new IndexField("myindexedfield4", FieldDataType.UUID));
        index.setFields(fields);
        index.isUnique(false);
        return index;
    }

    /**
     * Creates at test index with a all possible field types.
     *
     * @return
     */
    public static final Index createTestIndexAllFieldTypes()
    {
        Index index = new Index("myindexallfields");
        index.setTable(DB, "mytable");
        ArrayList<IndexField> fields = new ArrayList<>();
        fields.add(new IndexField("thisisauudid", FieldDataType.UUID));
        fields.add(new IndexField("thisisastring", FieldDataType.TEXT));
        fields.add(new IndexField("thisisanint", FieldDataType.INTEGER));
        fields.add(new IndexField("thisisadouble", FieldDataType.DOUBLE));
        fields.add(new IndexField("thisisbase64", FieldDataType.BINARY));
        fields.add(new IndexField("thisisaboolean", FieldDataType.BOOLEAN));
        fields.add(new IndexField("thisisadate", FieldDataType.DATE_TIME));
        //fields.add(new IndexField("thisisatimepoint", FieldDataType.TIMEPOINT));
        fields.add(new IndexField("thisisalong", FieldDataType.LONG));
        index.setFields(fields);
        index.isUnique(false);
        return index;
    }

    /**
     * Creates at test IndexCreatedEvent.
     *
     * @return
     */
    public static final IndexCreatedEvent createTestIndexCreationStatus()
    {
        IndexCreatedEvent toReturn = new IndexCreatedEvent(UUID.randomUUID(), new Date(), new Date(), createTestIndexOneField(), 1000, 0);
        toReturn.calculateValues();
        return toReturn;
    }

    /**
     * Creates at test IndexCreatedEvent.
     *
     * @return
     */
    public static final IndexCreatedEvent createTestIndexCreationStatusWithBulkDataHit()
    {
        IndexCreatedEvent toReturn = new IndexCreatedEvent(UUID.randomUUID(), new Date(), new Date(), createTestIndexWithBulkDataHit(), 1000, 0);
        toReturn.calculateValues();
        return toReturn;
    }

    /**
     * Creates at test index with one field that will hit every row of our bulk
     * data.
     *
     * @return
     */
    public static final Index createTestIndexWithBulkDataHit()
    {
        Index index = new Index("myindexbulkdata");
        index.setTable(DB, "mytable");
        ArrayList<IndexField> fields = new ArrayList<>();
        fields.add(new IndexField("field1"));
        index.setFields(fields);
        index.isUnique(false);
        return index;
    }

    public void insertIndex(Index index)
    {
        indexRepo.create(index);
    }

    public void clearTestTables()
    {
//        try
//        {
//            Utils.initDatabase(DB_CQL, session);
//            CacheFactory.clearAllCaches();//if we reinit, we need to clear our caches or else we will get prepared statements that are no longer valid
//        } catch (IOException e)
//        {
//            logger.error("Couldn't re-init db.", e);
//        }

        try
        {
            cleanUpInstance.deleteITable("mydb_mytable_myindexwithonefield");
        } catch (DriverException e)
        {
            //logger.debug("Not dropping iTable, probably doesn't exist.");
        }
        try
        {
            cleanUpInstance.deleteITable("mydb_mytable_myindexwithtwofields");
        } catch (DriverException e)
        {
            //logger.debug("Not dropping iTable, probably doesn't exist.");
        }
        try
        {
            cleanUpInstance.deleteITable("mydb_mytable_myindexbulkdata");
        } catch (DriverException e)
        {
            //logger.debug("Not dropping iTable, probably doesn't exist.");
        }

        try
        {
            cleanUpInstance.deleteITable("worldbank_worldbankloans_countryname");
        } catch (DriverException e)
        {
            //logger.debug("Not dropping iTable, probably doesn't exist.");
        }
        try
        {
            docRepo.delete(Fixtures.createTestDocument());
            docRepo.delete(Fixtures.createTestDocument2());
            docRepo.delete(Fixtures.createTestDocument3());
        } catch (DriverException e)
        {
            //logger.debug("Not dropping document, probably doesn't exist.");
        }
        try
        {
            List<Document> toDelete = getBulkDocuments();
            for (Document d : toDelete)
            {
                try
                {
                    docRepo.delete(d);
                } catch (DriverException e)
                {
                    //logger.debug("Not dropping bulk document, probably doesn't exist.");
                }
            }
        } catch (Exception e)
        {
            throw new RuntimeException(e);
        }

//        try
//        {
//            List<Document> toDelete = Fixtures.getBulkDocuments("./src/test/resources/world_bank_short.txt", Fixtures.createTestBankTable());
//            for (Document d : toDelete)
//            {
//                try
//                {
//                    docRepo.delete(d);
//                } catch (Exception e)
//                {
//                    //logger.debug("Not dropping bulk document, probably doesn't exist.");
//                }
//            }
//        } catch (Exception e)
//        {
//            throw new RuntimeException(e);
//        }
        try
        {

            tableRepo.delete(Fixtures.createTestTable());
        } catch (DriverException e)
        {
            //logger.debug("Not dropping setTable, probably doesn't exist.");
        }
        try
        {

            tableRepo.delete(Fixtures.createTestWorldBankTable());
        } catch (DriverException e)
        {
            //logger.debug("Not dropping setTable, probably doesn't exist.");
        }
        try
        {
            indexRepo.delete(Fixtures.createTestIndexOneField());
        } catch (DriverException e)
        {
            //logger.debug("Not dropping setTable, probably doesn't exist.");
        }
        try
        {
            indexRepo.delete(Fixtures.createTestIndexTwoField());
        } catch (DriverException e)
        {
            //logger.debug("Not deleting index, probably doesn't exist.");
        }
        try
        {
            indexRepo.delete(Fixtures.createTestIndexAllFieldTypes());
        } catch (DriverException e)
        {
            //logger.debug("Not deleting index, probably doesn't exist.");
        }
        try
        {
            indexRepo.delete(Fixtures.createTestIndexNumericField());
        } catch (DriverException e)
        {
            //logger.debug("Not deleting index, probably doesn't exist.");
        }
        try
        {
            indexRepo.delete(Fixtures.createTestIndexUUIDField());
        } catch (DriverException e)
        {
            //logger.debug("Not deleting index, probably doesn't exist.");
        }
        try
        {
            indexRepo.delete(Fixtures.createTestIndexWithBulkDataHit());
        } catch (DriverException e)
        {
            //logger.debug("Not deleting index, probably doesn't exist.");
        }
        try
        {
            databaseRepo.delete(Fixtures.createTestDatabase());
        } catch (DriverException e)
        {
            //logger.debug("Not deleting database, probably doesn't exist.");
        }
        try
        {
            databaseRepo.delete(Fixtures.createTestWorldBankDatabase());
        } catch (DriverException e)
        {
            //logger.debug("Not deleting database, probably doesn't exist.");
        }
        try
        {
            indexRepo.delete(Fixtures.createTestBankIndexCountryName());
        } catch (DriverException e)
        {
            //logger.debug("Not deleting database, probably doesn't exist.");
        }
        try
        {
            indexRepo.delete(Fixtures.createTestBankIndexApprovedOn());
        } catch (DriverException e)
        {
            //logger.debug("Not deleting database, probably doesn't exist.");
        }
        try
        {
            indexRepo.delete(Fixtures.createTestBankIndexApprovedOnTimepoint());
        } catch (DriverException e)
        {
            //logger.debug("Not deleting database, probably doesn't exist.");
        }
        try
        {
            indexRepo.delete(Fixtures.createTestBankIndexLoanYear());
        } catch (DriverException e)
        {
            //logger.debug("Not deleting database, probably doesn't exist.");
        }

    }

    public void createTestITables()
    {
        ITableRepository iTableDao = new ITableRepositoryImpl(getSession());
        Index index = Fixtures.createTestIndexOneField();
        Index index2 = Fixtures.createTestIndexTwoField();
        Index index3 = Fixtures.createTestIndexWithBulkDataHit();
        indexRepo.create(index);
        //indexRepo.create(index2);
        iTableDao.createITable(index2);
        iTableDao.createITable(index3);
        tableRepo.create(Fixtures.createTestTable());
    }

    public static final Document createTestDocument()
    {
        Document entity = new Document();
        entity.setTable("mydb", "mytable");
        entity.setObjectAsString("{\"greeting\":\"hello\", \"myindexedfield\": \"thisismyfield\", \"myindexedfield1\":\"my second field\", \"myindexedfield2\":\"my third field\"}");
        entity.setUuid(new UUID(0L, 1L));
        entity.setCreatedAt(new Date());
        entity.setUpdatedAt(new Date());
        return entity;
    }

    /**
     * Creates a test document.
     *
     * @return
     */
    public static final Document createTestDocument2()
    {
        Document entity = new Document();
        entity.setTable("mydb", "mytable");
        entity.setObjectAsString("{\"greeting\":\"hello\", \"myindexedfield\": \"this is my field\", \"myindexedfield1\":\"my second field\", \"myindexedfield2\":\"my third field\"}");
        entity.setUuid(new UUID(0L, 2L));
        entity.setCreatedAt(new Date());
        entity.setUpdatedAt(new Date());
        return entity;
    }

    /**
     * Creates a test document with multiple datatype getFields.
     *
     * @return
     */
    public static final Document createTestDocument3()
    {
        Document entity = new Document();
        entity.setTable("mydb", "mytable");
        entity.setObjectAsString("{\"thisisastring\":\"hello\", \"thisisanint\": \"5\", \"thisisadouble\":\"5.555\","
                + " \"thisisbase64\":\"VGhpcyBpcyBhIGdvb2RseSB0ZXN0IG1lc3NhZ2Uu\", \"thisisaboolean\":\"f\","
                + " \"thisisadate\":\"Thu Apr 30 09:52:04 MDT 2015\", \"thisisauudid\":\"3d069a5a-ef51-11e4-90ec-1681e6b88ec1\", \"thisisalong\":\"378657657654654\"}");
        entity.setUuid(new UUID(0L, 3L));
        entity.setCreatedAt(new Date());
        entity.setUpdatedAt(new Date());
        return entity;
    }

    public void insertDocument(Document document)
    {
        DocumentRepositoryImpl documentRepo = new DocumentRepositoryImpl(getSession());
        documentRepo.create(document);
    }

    public void insertDocuments(List<Document> documents)
    {
        DocumentRepositoryImpl documentRepo = new DocumentRepositoryImpl(getSession());
        for (Document document : documents)
        {
            try
            {
                documentRepo.create(document);
            } catch (RuntimeException e)
            {
                if (e.getCause() != null && e.getCause() instanceof IndexParseException)
                {
                    ;// we had a bad a record; ignore it for the purposes of this test
                } else
                {
                    throw e;
                }

            }
        }
    }

//    public void insertDocumentsTemp(List<Document> documents) throws Exception
//    {
//        EmbeddedCassandraServerHelper.startEmbeddedCassandra(15000);
//        DocumentRepositoryImpl documentRepo = new DocumentRepositoryImpl(getSession());
//        int i = 0;
//        for (Document document : documents)
//        {
//            logger.debug("Inserting test document: " + i++);
//            try
//            {
//                documentRepo.create(document);
//            } catch (Exception e)
//            {                
//                logger.error("Could not insert test document.", e);
//            }
//        }
//    }
    public void deleteDocument(Document document)
    {
        DocumentRepositoryImpl documentRepo = new DocumentRepositoryImpl(getSession());
        documentRepo.delete(document);
    }

    /**
     * Creates a simple query based on a single index for testing.
     *
     * @return
     */
    public static final Query createTestQuery()
    {
        Query query = new Query();
        query.setWhere("myindexedfield = 'thisismyfield'");
        query.setDatabase(Fixtures.DB);
        query.setTable("mytable");
        return query;
    }

    /**
     * Creates a simple query based on a single index for testing.
     *
     * @return
     */
    public static final Query createTestQuery2()
    {
        Query query = new Query();
        query.setWhere("myindexedfield1 = 'thisismyfield' AND myindexedfield2 = 'blah'");
        query.setDatabase(Fixtures.DB);
        query.setTable("mytable");
        return query;
    }

    /**
     * Creates a simple parsed query based on a single index for testing.
     *
     * @return
     */
    public static final ParsedQuery createTestParsedQuery()
    {
        Query query = createTestQuery();
        WhereClause whereClause = new WhereClause(query.getWhere());
        //String iTable = "mydb_mytable_myindexwithonefield";
        return new ParsedQuery(query, whereClause, createTestIndexOneField());
    }

    /**
     * Creates a simple parsed query based on a single index for testing.
     *
     * @return
     */
    public static final ParsedQuery createTestParsedQuery2()
    {
        Query query = new Query();
        query.setWhere("myindexedfield = 'foo'");
        query.setDatabase(Fixtures.DB);
        query.setTable("mytable");
        WhereClause whereClause = new WhereClause(query.getWhere());
        //String iTable = "mydb_mytable_myindexwithonefield";
        return new ParsedQuery(query, whereClause, createTestIndexOneField());
    }

    /**
     * Creates a simple parsed query based on a single index for testing.
     *
     * @return
     */
    public static final ParsedQuery createTestParsedQueryBulkData()
    {
        Query query = new Query();
        query.setWhere("field1 = 'this is my data'");
        query.setDatabase(Fixtures.DB);
        query.setTable("mytable");
        WhereClause whereClause = new WhereClause(query.getWhere());
        //String iTable = "mydb_mytable_myindexbulkdata";
        return new ParsedQuery(query, whereClause, createTestIndexWithBulkDataHit());
    }

    /**
     * Creates a simple setTable for testing.
     *
     * @return
     */
    public static final Table createTestTable()
    {
        Table t = new Table();
        t.setName("mytable");
        t.setDatabase(Fixtures.DB);
        t.setDescription("My Table stores a lot of data.");
        return t;
    }

    /**
     * Creates a simple setTable for testing.
     *
     * @return
     */
    public static final Table createTestWorldBankTable()
    {
        Table testTable = new Table();
        testTable.setName("worldbankloans");
        testTable.setDatabase(createTestWorldBankDatabase().getName());
        testTable.setDescription("This table stores data about World Bank loans.");
        return testTable;
    }

    public static Database createTestWorldBankDatabase()
    {
        Database testDb = new Database("worldbank");
        testDb.setDescription("A database about the World Bank.");
        return testDb;
    }

    public void insertTable(Table table)
    {
        tableRepo.create(table);
    }

    public static Database createTestDatabase()
    {
        Database database = new Database(DB);
        database.setDescription("This is a test database.");
        return database;
    }

    public void insertDatabase(Database database)
    {
        databaseRepo.create(database);
    }

    /**
     * @return the session
     */
    public Session getSession()
    {
        return session;
    }

    public static String generateIndexCreationStringWithFields(Index index)
    {
        StringBuilder indexStr = new StringBuilder("{\"name\" : \"" + index.getName() + "\", \"fields\" : [");
        boolean first = true;
        for (IndexField f : index.getFields())
        {
            if (!first)
            {
                indexStr.append(", ");
            }
            first = false;
            indexStr.append("{\"field\" : \"");
            indexStr.append(f.getField());
            indexStr.append("\",\"type\": \"");
            indexStr.append(f.getType().toString());
            indexStr.append("\"}");
        }
        indexStr.append("]}");
        return indexStr.toString();
    }
}
