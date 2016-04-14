package com.pearson.docussandra.plugininterfaces;

import java.util.HashMap;
import java.util.List;

/**
 * Interface for handling security.
 *
 * Although this is an abstract class, it should be treated like an interface.
 *
 * All implementing classes should be thread safe and provide a no argument
 * constructor.
 *
 * Multiple implementations are allowed, however, they will run in an arbitrary
 * order.
 *
 * @author https://github.com/JeffreyDeYoung
 */
public abstract class SecurityPlugin implements Plugin
{

    /**
     * Enum representing supported Http methods.
     */
    public enum HttpMethod
    {
        GET,
        POST,
        PUT,
        DELETE;

        /**
         * Friendly method for getting the HttpMethod based off of a String.
         * Cleans up the string so it should work regardless of case or extra
         * whitespace.
         *
         * @param in String to convert to an HttpMethod.
         * @return A HttpMethod based on the String.
         */
        public static HttpMethod forString(String in)
        {
            return HttpMethod.valueOf(in.toUpperCase().trim());
        }
    };

    /**
     * Method that performs a validation based on the passed in headers. Should
     * throw a PermissionDeniedException if the client is not authorized.
     *
     * @param headers Map of the headers.
     * @param requestedPath Path that is being requested.
     * @param method Type of Http method that was requested.
     * @throws PermissionDeniedException If the client is not authorized to make
     * this call.
     */
    public abstract void doValidate(HashMap<String, List<String>> headers, String requestedPath, HttpMethod method) throws PermissionDeniedException;//TODO: Add path and type of call (GET, PUT, etc)
}
