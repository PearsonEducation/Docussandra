package com.pearson.docussandra;

import org.restexpress.Request;
import org.restexpress.Response;
import org.restexpress.pipeline.MessageObserver;
import org.restexpress.pipeline.SimpleConsoleLogMessageObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * class to overwrite the onException method in the MessageObserver
 * to write errors to the log file of docussandra and not just to the console
 */
public class SimpleLogMessageObserver extends MessageObserver {

    private Logger logger = LoggerFactory.getLogger(getClass());

    public SimpleLogMessageObserver() {
        new SimpleConsoleLogMessageObserver();
    }

    protected void onException(Throwable exception, Request request, Response response) {
        logger.error(exception.toString());
        logger.error(request.getEffectiveHttpMethod().toString() + " " + request.getUrl() + " threw exception: " + exception.getClass().getSimpleName(), exception);
    }
}