package com.pearson.docussandra.exception;

/**
 * @author https://github.com/tfredrich
 * @since Oct 13, 2010
 */
public class RepositoryException
        extends RuntimeException
{

    private static final long serialVersionUID = 3017766856659675935L;

    public RepositoryException()
    {
        super();
    }

    /**
     * @param message
     */
    public RepositoryException(String message)
    {
        super(message);
    }

    /**
     * @param cause
     */
    public RepositoryException(Throwable cause)
    {
        super(cause);
    }

    /**
     * @param message
     * @param cause
     */
    public RepositoryException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
