
package com.pearson.docussandra.exception;

/**
 * @author https://github.com/tfredrich
 * @since Jun 28, 2010
 */
public class DuplicateItemException
        extends RepositoryException
{

    private static final long serialVersionUID = 7569348250967993221L;

    public DuplicateItemException()
    {
        super();
    }

    /**
     * @param message
     */
    public DuplicateItemException(String message)
    {
        super(message);
    }

    /**
     * @param cause
     */
    public DuplicateItemException(Throwable cause)
    {
        super(cause);
    }

    /**
     * @param message
     * @param cause
     */
    public DuplicateItemException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
