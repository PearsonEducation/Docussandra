package com.pearson.docussandra.exception;

/**
 * Thrown from an IdentifierAdapter.convert() when the ID cannot be converted
 * into an identifier.
 *
 * @author https://github.com/tfredrich
 * @since Mar 24, 2011
 */
public class InvalidObjectIdException
        extends RepositoryException
{

    private static final long serialVersionUID = -8427649738145349078L;

    public InvalidObjectIdException()
    {
    }

    /**
     * @param message
     */
    public InvalidObjectIdException(String message)
    {
        super(message);
    }

    /**
     * @param cause
     */
    public InvalidObjectIdException(Throwable cause)
    {
        super(cause);
    }

    /**
     * @param message
     * @param cause
     */
    public InvalidObjectIdException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
