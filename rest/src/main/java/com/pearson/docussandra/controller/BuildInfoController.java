package com.pearson.docussandra.controller;

import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.io.IOUtils;
import org.restexpress.ContentType;
import org.restexpress.Request;
import org.restexpress.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
            String buildInfo = IOUtils.toString(propsStream);
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
