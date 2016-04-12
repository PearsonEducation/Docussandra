package com.pearson.docussandra;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import com.pearson.docussandra.postprocessor.LastModifiedHeaderPostprocessor;
import com.pearson.docussandra.preprocessor.RequestXAuthCheck;
import com.strategicgains.restexpress.plugin.cors.CorsHeaderPlugin;
import com.pearson.docussandra.preprocessor.RequestApplicationJsonPreprocessor;
import com.strategicgains.restexpress.plugin.swagger.SwaggerPlugin;
import org.restexpress.RestExpress;
import org.restexpress.exception.BadRequestException;
import org.restexpress.exception.ConflictException;
import org.restexpress.exception.NotFoundException;
import org.restexpress.plugin.hyperexpress.HyperExpressPlugin;
import org.restexpress.plugin.hyperexpress.Linkable;
import org.restexpress.plugin.version.VersionPlugin;
import org.restexpress.util.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.pearson.docussandra.cache.CacheFactory;
import com.pearson.docussandra.config.Configuration;
import com.pearson.docussandra.exception.DuplicateItemException;
import com.pearson.docussandra.exception.InvalidObjectIdException;
import com.pearson.docussandra.exception.ItemNotFoundException;
import com.pearson.docussandra.plugins.PluginHolder;
import com.pearson.docussandra.plugins.PluginUtils;
import com.pearson.docussandra.serialization.SerializationProvider;
import com.strategicgains.restexpress.plugin.metrics.MetricsConfig;
import com.strategicgains.restexpress.plugin.metrics.MetricsPlugin;
import com.strategicgains.syntaxe.ValidationException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Properties;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import static io.netty.handler.codec.http.HttpHeaders.Names.*;
import static io.netty.handler.codec.http.HttpHeaders.Names.LOCATION;
import static org.restexpress.Flags.Auth.PUBLIC_ROUTE;

/**
 * Main class for this application. Starts up the app.
 */
public class Main
{

    private static final String SERVICE_NAME = "Docussandra API";
    private static final Logger logger = LoggerFactory.getLogger(SERVICE_NAME);

    /**
     * Main method.
     *
     * @param args Command line arguments. Should be one parameter with either
     * an environment or a path to a URL with the proper configuration via a
     * REST GET.
     * @throws Exception
     */
    public static void main(String[] args) throws Exception
    {
        try
        {
            RestExpress server = initializeServer(args);
            logger.info("Server started up on port: " + server.getPort() + "!");
            server.awaitShutdown();
            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable()
            {
                @Override
                public void run()
                {
                    logger.info("Shutting down Docussandra...");
                    CacheFactory.shutdownCacheManger();
                }
            }, "Shutdown-thread"));
        } catch (RuntimeException e)
        {
            logger.error("Runtime exception when starting/running Docussandra/RestExpress. Could not start.", e);
        }
    }

    /**
     * Starts up the server for the specified environment.
     * @param args Command line arguments. Should be one parameter with either
     * an environment or a path to a URL with the proper configuration via a
     * REST GET.
     * @return A running RestExpress REST server.
     * @throws IOException
     * @throws IllegalAccessException
     * @throws InstantiationException 
     */
    public static RestExpress initializeServer(String[] args) throws IOException, IllegalAccessException, InstantiationException
    {
        //args = new String[]{"http://docussandra-dev-webw-1.openclass.com:8080/config/A"};
        RestExpress.setSerializationProvider(new SerializationProvider());
        //Identifiers.UUID.useShortUUID(true);
        //load our plugins first
        PluginHolder.build(PluginUtils.getPluginJars());

        Configuration config = loadEnvironment(args);
        logger.info("-----Attempting to start up Docussandra server for version: " + config.getProjectVersion() + "-----");
        RestExpress server = new RestExpress()
                .setName(config.getProjectName(SERVICE_NAME))
                .setBaseUrl(config.getBaseUrl())
                .setExecutorThreadCount(config.getExecutorThreadPoolSize())
                .addPostprocessor(new LastModifiedHeaderPostprocessor())
                .addMessageObserver(new SimpleLogMessageObserver())
                .addPreprocessor(new RequestApplicationJsonPreprocessor())
                .addPreprocessor(new RequestXAuthCheck())
                .setMaxContentSize(6000000);

        new VersionPlugin(config.getProjectVersion())
                .register(server);

        new SwaggerPlugin()
                .register(server);

        Routes.define(config, server);
        Relationships.define(server);
        configurePlugins(config, server);
        mapExceptions(server);

        if (config.getPort() == 0)
        {//no port? calculate it off of the version number
            server.setPort(calculatePort(config.getProjectVersion()));
        } else
        {
            server.setPort(config.getPort());
        }
        server.bind(server.getPort());
        logger.info("-----Docussandra server initialized for version: " + config.getProjectVersion() + "-----");
        return server;
    }

    /**
     * Calculates a (semi-unique) four or five digit port based off of the
     * version number. Will always start with a four.
     *
     * @param projectVersion Version to calculate the port number off of.
     * @return An port to run this version on.
     */
    protected static int calculatePort(String projectVersion)
    {
        int basePort = 4;
        String number;
        int dashIndex = projectVersion.indexOf('-');
        boolean snapshot = false;
        if (dashIndex > -1)//check if this is a snapshot
        {
            number = projectVersion.substring(0, dashIndex);
            snapshot = true;
        } else
        {
            number = projectVersion;
        }
        number = number.replaceAll("\\Q.\\E", "");//drop the decimal point
        int numLength = number.length();
        if (number.length() >= 3)
        {
            number = number.substring(numLength - 3, numLength);//grab the three least significate digits
        }
        while (number.length() < 3)
        {
            number += "0"; //if less than three digits long, pad with zeros
        }

        if (snapshot)
        {
            number += "1";//snapshots have +1 to indicate that they are snapshots
        } else
        {
            number += "0";//non-snapshots have +0
        }
        number = basePort + number;//note string concat, not addition
        return Integer.parseInt(number);//convert it to an integer

    }

    /**
     * Configures RestExpress plugins (not Docussandra plugins).
     * @param config Configuration object.
     * @param server RestExpress server object.
     */
    private static void configurePlugins(Configuration config, RestExpress server)
    {
        configureMetrics(config, server);

        new HyperExpressPlugin(Linkable.class)
                .register(server);

        new CorsHeaderPlugin("*")
                .flag(PUBLIC_ROUTE)
                .allowHeaders(CONTENT_TYPE, ACCEPT, LOCATION)
                .exposeHeaders(LOCATION)
                .register(server);
    }

    private static void configureMetrics(Configuration config, RestExpress server)
    {
        MetricsConfig mc = config.getMetricsConfig();

        if (mc.isEnabled())
        {
            MetricRegistry registry = new MetricRegistry();
            new MetricsPlugin(registry)
                    .register(server);

            if (mc.isGraphiteEnabled())
            {
                final Graphite graphite = new Graphite(new InetSocketAddress(mc.getGraphiteHost(), mc.getGraphitePort()));
                final GraphiteReporter reporter = GraphiteReporter.forRegistry(registry)
                        .prefixedWith(mc.getPrefix())
                        .convertRatesTo(TimeUnit.SECONDS)
                        .convertDurationsTo(TimeUnit.MILLISECONDS)
                        .filter(MetricFilter.ALL)
                        .build(graphite);
                reporter.start(mc.getPublishSeconds(), TimeUnit.SECONDS);
            } else
            {
                logger.warn("*** Graphite Metrics Publishing is Disabled ***");
            }
        } else
        {
            logger.warn("*** Metrics Generation is Disabled ***");
        }
    }

    private static void mapExceptions(RestExpress server)
    {
        server
                .mapException(ItemNotFoundException.class, NotFoundException.class)
                .mapException(DuplicateItemException.class, ConflictException.class)
                .mapException(ValidationException.class, BadRequestException.class)
                .mapException(InvalidObjectIdException.class, BadRequestException.class);
    }

    private static Configuration loadEnvironment(String[] args)
            throws FileNotFoundException, IOException
    {
        logger.info("Loading environment with " + args.length + " arguments.");
        if (args.length > 0)
        {
            logger.info("-args[0]: " + args[0]);
            if (args[0].startsWith("http") || args[0].startsWith("HTTP"))//if we are fetching props by URL
            {
                Configuration config = new Configuration();
                config.fillValues(fetchPropertiesFromServer(args[0]));
                return config;
            } else //load from standard config
            {
                return Environment.from(args[0], Configuration.class);
            }
        }
        return Environment.fromDefault(Configuration.class);
    }

    private static Properties fetchPropertiesFromServer(String url)
    {
        Properties properties = new Properties();
        if (url != null)
        {
            HttpClient client = HttpClientBuilder.create().build();
            JSONParser parser = new JSONParser();
            HttpGet request = new HttpGet(url);
            RequestConfig rc = RequestConfig.custom().setConnectTimeout(60000).setSocketTimeout(3600000).setConnectionRequestTimeout(60000).build();;
            request.setConfig(rc);
            // add request header
            request.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
//            //add auth if specified
//            if (authToken != null)
//            {
//                request.addHeader(HttpHeaders.AUTHORIZATION, "Bearer " + authToken);
//            }
            BufferedReader rd = null;
            InputStreamReader isr = null;
            try
            {
                HttpResponse response = client.execute(request);
                int responseCode = response.getStatusLine().getStatusCode();
                if (responseCode != 200)
                {
                    throw new RuntimeException("Cannot fetch properties: Error when doing a GET call agaist: " + url + ". Error code: " + responseCode + " Status code: " + response.getStatusLine().getStatusCode());
                }
                isr = new InputStreamReader(response.getEntity().getContent());
                rd = new BufferedReader(isr);
                properties.putAll((JSONObject) parser.parse(rd));
            } catch (ParseException pe)
            {
                throw new RuntimeException("Cannot fetch properties: Could not parse JSON", pe);
            } catch (IOException e)
            {
                throw new RuntimeException("Cannot fetch properties: Problem contacting REST service for GET, URL: " + url, e);
            } finally
            {
                if (rd != null)
                {
                    try
                    {
                        rd.close();
                    } catch (IOException e)
                    {
                        logger.debug("Could not close BufferedReader...", e);
                    }
                }
                if (isr != null)
                {
                    try
                    {
                        isr.close();
                    } catch (IOException e)
                    {
                        logger.debug("Could not close InputStreamReader...", e);
                    }
                }
                request.reset();
            }
        }
        return properties;
    }
}
