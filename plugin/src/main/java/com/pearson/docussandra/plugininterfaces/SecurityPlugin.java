package com.pearson.docussandra.plugininterfaces;

import java.util.HashSet;
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
     * Method that performs a validation based on the passed in headers. Should throw a PermissionDeniedException if the client is not authorized.
     * @param headers Map of the headers.
     * @throws PermissionDeniedException If the client is not authorized to make this call.
     */
    public abstract void doValidate(HashSet<List<String>> headers) throws PermissionDeniedException;//TODO: Add path and type of call (GET, PUT, etc)
}
