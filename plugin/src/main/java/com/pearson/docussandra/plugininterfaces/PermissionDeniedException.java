package com.pearson.docussandra.plugininterfaces;

/**
 * Exception that gets thrown when a request is attempted that is not permitted.
 * Thrown by the SecurityPlugin.
 *
 * @author https://github.com/JeffreyDeYoung
 */
public class PermissionDeniedException extends Exception
{

    /**
     * Constructor.
     * @param message Message indicating why the user was denied access.
     */
    public PermissionDeniedException(String message)
    {
        super(message);
    }

}
