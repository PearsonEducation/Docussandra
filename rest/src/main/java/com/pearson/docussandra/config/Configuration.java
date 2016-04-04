package com.pearson.docussandra.config;

import com.datastax.driver.core.Session;
import com.pearson.docussandra.Utils;
import com.pearson.docussandra.controller.BuildInfoController;
import java.io.IOException;
import java.net.URL;
import java.util.Properties;
import java.util.jar.Manifest;

import org.restexpress.common.exception.ConfigurationException;
import org.restexpress.util.Environment;

import com.pearson.docussandra.controller.DatabaseController;
import com.pearson.docussandra.controller.DocumentController;
import com.pearson.docussandra.controller.HealthCheckController;
import com.pearson.docussandra.controller.IndexController;
import com.pearson.docussandra.controller.IndexStatusController;
import com.pearson.docussandra.controller.QueryController;
import com.pearson.docussandra.controller.TableController;
import com.pearson.docussandra.handler.IndexCreatedHandler;
import com.pearson.docussandra.persistence.DatabaseRepository;
import com.pearson.docussandra.persistence.DocumentRepository;
import com.pearson.docussandra.persistence.IndexRepository;
import com.pearson.docussandra.persistence.IndexStatusRepository;
import com.pearson.docussandra.persistence.QueryRepository;
import com.pearson.docussandra.persistence.TableRepository;
import com.pearson.docussandra.persistence.impl.DatabaseRepositoryImpl;
import com.pearson.docussandra.persistence.impl.DocumentRepositoryImpl;
import com.pearson.docussandra.persistence.impl.IndexRepositoryImpl;
import com.pearson.docussandra.persistence.impl.IndexStatusRepositoryImpl;
import com.pearson.docussandra.persistence.impl.QueryRepositoryImpl;
import com.pearson.docussandra.persistence.impl.TableRepositoryImpl;
import com.pearson.docussandra.service.DatabaseService;
import com.pearson.docussandra.service.DocumentService;
import com.pearson.docussandra.service.IndexService;
import com.pearson.docussandra.service.QueryService;
import com.pearson.docussandra.service.TableService;
import com.strategicgains.eventing.DomainEvents;
import com.strategicgains.eventing.EventBus;
import com.strategicgains.eventing.local.LocalEventBusBuilder;
import com.strategicgains.restexpress.plugin.metrics.MetricsConfig;
import java.net.InetAddress;
import java.net.UnknownHostException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Configuration
        extends Environment
{

    private static final String DEFAULT_EXECUTOR_THREAD_POOL_SIZE = "20";

    private static final String PORT_PROPERTY = "port";
    private static final String REPLICATION_PROPERTY = "replication.string";
    private static final String BASE_URL_PROPERTY = "base.url";
    private static final String EXECUTOR_THREAD_POOL_SIZE = "executor.threadPool.size";

    private int port;
    private String baseUrl;
    private String replicationFactorString;
    private int executorThreadPoolSize;
    private MetricsConfig metricsSettings;
    private Manifest manifest;

    private DatabaseController databaseController;
    private TableController tableController;
    private DocumentController documentController;
    private IndexController indexController;
    private IndexStatusController indexStatusController;
    private QueryController queryController;
    private HealthCheckController healthController;
    private BuildInfoController buildInfoController;

    private static final Logger LOGGER = LoggerFactory.getLogger(Configuration.class);
    @Override
    public void fillValues(Properties p)
    {
        this.port = Integer.parseInt(p.getProperty(PORT_PROPERTY, "0"));
        this.baseUrl = p.getProperty(BASE_URL_PROPERTY, "http://localhost:" + String.valueOf(port));
        this.executorThreadPoolSize = Integer.parseInt(p.getProperty(EXECUTOR_THREAD_POOL_SIZE, DEFAULT_EXECUTOR_THREAD_POOL_SIZE));
        this.metricsSettings = new MetricsConfig(p);
        this.replicationFactorString = p.getProperty(REPLICATION_PROPERTY, "{ 'class' : 'SimpleStrategy', 'replication_factor' : 1}");        
        try
        {
            //TODO: consider re-working this section
            if (p.get("cassandra.contactPoints").equals("localhost"))//if localhost, assume cassandra is being co-hosted on the same box with cassandra
            {
                String currentHostName = InetAddress.getLocalHost().getHostAddress();//get the boxes actual ip and use that; for some reason (firewalls?) some machines don't like to actually use 127.0.0.1 and need their external ip referenced
                if (currentHostName.equals("127.0.1.1"))//something odd with ubuntu; hack fix for now 
                {
                    currentHostName = "127.0.0.1";//use localhost; it should work in this case
                }
                p.setProperty("cassandra.contactPoints", currentHostName);//using localhost for cassandra seed -- we are going to try cohosting; TODO: ensure thalassa health check checks DB as well
            }
        } catch (UnknownHostException e)
        {
            LOGGER.error("Could not determine Cassandra IP.");
        }
        CassandraConfigWithGenericSessionAccess dbConfig = new CassandraConfigWithGenericSessionAccess(p);
        initialize(dbConfig);
        loadManifest();
    }

    private void initialize(CassandraConfigWithGenericSessionAccess dbConfig)
    {
        Utils.initDatabase(false, replicationFactorString, dbConfig.getGenericSession());//DO NOT SET THE FLAG TO TRUE; IT WILL ERASE EVERYTHING

        DatabaseRepository databaseRepository = new DatabaseRepositoryImpl(dbConfig.getSession());
        TableRepository tableRepository = new TableRepositoryImpl(dbConfig.getSession());
        DocumentRepository documentRepository = new DocumentRepositoryImpl(dbConfig.getSession());
        IndexRepository indexRepository = new IndexRepositoryImpl(dbConfig.getSession());
        QueryRepository queryRepository = new QueryRepositoryImpl(dbConfig.getSession());
        IndexStatusRepository indexStatusRepository = new IndexStatusRepositoryImpl(dbConfig.getSession());

        DatabaseService databaseService = new DatabaseService(databaseRepository);
        TableService tableService = new TableService(databaseRepository, tableRepository);
        DocumentService documentService = new DocumentService(tableRepository, documentRepository);
        IndexService indexService = new IndexService(tableRepository, indexRepository, indexStatusRepository);
        QueryService queryService = new QueryService(databaseRepository, tableRepository, queryRepository);

        databaseController = new DatabaseController(databaseService);
        tableController = new TableController(tableService);
        documentController = new DocumentController(documentService);
        indexController = new IndexController(indexService);
        indexStatusController = new IndexStatusController(indexService);
        queryController = new QueryController(queryService);
        healthController = new HealthCheckController();
        buildInfoController = new BuildInfoController();
        // TODO: create service and repository implementations for these...
//		entitiesController = new EntitiesController(SampleUuidEntityService);
        EventBus bus = new LocalEventBusBuilder()
                .subscribe(new IndexCreatedHandler(indexRepository, indexStatusRepository, documentRepository))
                .build();
        DomainEvents.addBus("local", bus);

    }

    public int getPort()
    {
        return port;
    }

    public String getBaseUrl()
    {
        return baseUrl;
    }

    public int getExecutorThreadPoolSize()
    {
        return executorThreadPoolSize;
    }

    public MetricsConfig getMetricsConfig()
    {
        return metricsSettings;
    }

    public DatabaseController getDatabaseController()
    {
        return databaseController;
    }

    public TableController getTableController()
    {
        return tableController;
    }

    public DocumentController getDocumentController()
    {
        return documentController;
    }

    public IndexController getIndexController()
    {
        return indexController;
    }

    public IndexStatusController getIndexStatusController()
    {
        return indexStatusController;
    }

    public QueryController getQueryController()
    {
        return queryController;
    }

    public String getProjectName(String defaultName)
    {
        if (hasManifest())
        {
            String name = manifest.getMainAttributes().getValue("Project-Name");

            if (name != null)
            {
                return name;
            }
        }

        return defaultName;
    }

    public String getProjectVersion()
    {
        if (hasManifest())
        {
            String version = manifest.getMainAttributes().getValue("version");

            if (version != null)
            {
                return version;
            }

            return "0.0 (Project version not found in manifest)";
        }

        return "0.0";
    }

    private void loadManifest()
    {
        Class<?> type = this.getClass();
        String name = type.getSimpleName() + ".class";
        URL classUrl = type.getResource(name);

        if (classUrl != null && classUrl.getProtocol().startsWith("jar"))
        {
            String path = classUrl.toString();
            String manifestPath = path.substring(0, path.lastIndexOf("!") + 1) + "/META-INF/MANIFEST.MF";
            try
            {
                manifest = new Manifest(new URL(manifestPath).openStream());
            } catch (IOException e)
            {
                throw new ConfigurationException(e);
            }
        }
    }

    private boolean hasManifest()
    {
        return (manifest != null);
    }

    /**
     * @return the healthController
     */
    public HealthCheckController getHealthController()
    {
        return healthController;
    }

    /**
     * @return the buildInfoController
     */
    public BuildInfoController getBuildInfoController()
    {
        return buildInfoController;
    }

    /**
     * CassandraConfig object that we can get a session separate from the
     * keyspace.
     */
    private class CassandraConfigWithGenericSessionAccess extends CassandraConfig
    {

        private Session genericSession;

        public CassandraConfigWithGenericSessionAccess(Properties p)
        {
            super(p);
            LOGGER.info("Using cassandra ips: " + p.getProperty("cassandra.contactPoints"));
        }

        public Session getGenericSession()
        {
            if (genericSession == null)
            {
                genericSession = getCluster().connect();
            }

            return genericSession;
        }
    }
}
