package com.pearson.docussandra.controller;

import com.pearson.docussandra.plugininterfaces.NotifierPluginInterface;
import com.pearson.docussandra.plugininterfaces.SecurityPluginInterface;
import com.pearson.docussandra.plugins.PluginHolder;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.io.IOUtils;
import org.restexpress.ContentType;
import org.restexpress.Request;
import org.restexpress.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Displays the build info and loaded plugins on /admin/buildInfo.
 * @author jeffrey
 */
public class BuildInfoController
{

    private static final Logger LOGGER = LoggerFactory.getLogger(BuildInfoController.class);

    public Object getBuildInfo(Request request, Response response)
    {
        response.setResponseStatus(HttpResponseStatus.OK);
        response.setContentType(ContentType.JSON);
        try
        {
            InputStream propsStream = this.getClass().getResourceAsStream("/git.properties");
            StringBuilder buildInfo = new StringBuilder(IOUtils.toString(propsStream));
            buildInfo.append("\n--------------\nActivated Plugins: \nNotifiers: \n");
            PluginHolder ph = PluginHolder.getInstance();
            for (NotifierPluginInterface plugin : ph.getNotifierPlugins())
            {
                buildInfo.append(plugin.getPluginName()).append(":").append(plugin.getClass().getCanonicalName()).append("\n");
            }
            buildInfo.append("Security: \n");
            for (SecurityPluginInterface plugin : ph.getSecurityPlugins())
            {
                buildInfo.append(plugin.getPluginName()).append(":").append(plugin.getClass().getCanonicalName()).append("\n");
            }
            buildInfo.append("\n");
            LOGGER.debug("Get build info called: " + buildInfo);
            return buildInfo;
        } catch (IOException e)
        {
            String message = "Could not read build info file.";
            LOGGER.error(message, e);
            return message;
        }
    }

}
