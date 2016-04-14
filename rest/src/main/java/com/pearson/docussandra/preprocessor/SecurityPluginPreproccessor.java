package com.pearson.docussandra.preprocessor;

import com.pearson.docussandra.plugininterfaces.PermissionDeniedException;
import com.pearson.docussandra.plugininterfaces.SecurityPlugin;
import com.pearson.docussandra.plugininterfaces.SecurityPlugin.HttpMethod;
import com.pearson.docussandra.plugins.PluginHolder;
import java.util.HashSet;
import java.util.List;
import org.restexpress.Request;
import org.restexpress.Response;
import org.restexpress.pipeline.Postprocessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Preproccessor that runs all of our security plugins against each request.
 *
 * @author https://github.com/JeffreyDeYoung
 */
public class SecurityPluginPreproccessor implements Postprocessor
{

    /**
     * Logger for this class.
     */
    private static final Logger logger = LoggerFactory.getLogger(SecurityPluginPreproccessor.class);
    /**
     * Security plugins that we will execute against every request.
     */
    private final List<SecurityPlugin> securityPlugins;

    /**
     * Constructor.
     */
    public SecurityPluginPreproccessor()
    {
        PluginHolder ph = PluginHolder.getInstance();
        securityPlugins = ph.getSecurityPlugins();
    }

    /**
     * Processes our request based on the security plugins.
     *
     * @param request
     * @param response
     */
    @Override
    public void process(Request request, Response response)
    {
        HashSet<List<String>> headers = extractHeadersFromRequest(request);
        for (SecurityPlugin plugin : securityPlugins)
        {
            logger.debug("Running security plugin: " + plugin.getPluginName());
            try
            {
                plugin.doValidate(headers, request.getPath(), HttpMethod.forString(request.getHttpMethod().name()));
            } catch (PermissionDeniedException e)
            {
                throw new SecurityException(e.getMessage());
            }
        }
    }

    /**
     * Extracts the headers from a org.restexpress.Request object and returns
     * them as a HashSet containing a list of Strings.
     *
     * @param request Request to extract the headers from.
     * @return A HashSet<List<String>> containing the headers.
     */
    public static HashSet<List<String>> extractHeadersFromRequest(Request request)
    {
        HashSet<List<String>> headers = new HashSet<>();
        for (String headerName : request.getHeaderNames())
        {
            headers.add(request.getHeaders(headerName));
        }
        return headers;
    }

}
