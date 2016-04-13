package com.pearson.docussandra.preprocessor;

import com.pearson.docussandra.plugininterfaces.PermissionDeniedException;
import com.pearson.docussandra.plugininterfaces.SecurityPlugin;
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
     * @param request
     * @param response 
     */
    @Override
    public void process(Request request, Response response)
    {
        //TODO: break this out into a seperate method 
        HashSet<List<String>> headers = new HashSet<>();
        for (String headerName : request.getHeaderNames())
        {
            headers.add(request.getHeaders(headerName));
        }
        //end break out
        for (SecurityPlugin plugin : securityPlugins)
        {
            logger.debug("Running security plugin: " + plugin.getPluginName());
            try
            {
                plugin.doValidate(headers);
            } catch (PermissionDeniedException e)
            {
                throw new SecurityException(e.getMessage());
            }
        }
    }

}
