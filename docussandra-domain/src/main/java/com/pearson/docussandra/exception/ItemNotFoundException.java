package com.pearson.docussandra.exception;

/**
 * @author https://github.com/tfredrich
 * @since Jun 28, 2010
 */
public class ItemNotFoundException extends RepositoryException {

  private static final long serialVersionUID = -1937211508363434084L;

  public ItemNotFoundException() {
    super();
  }

  /**
   * @param message
   */
  public ItemNotFoundException(String message) {
    super(message);
  }

  /**
   * @param cause
   */
  public ItemNotFoundException(Throwable cause) {
    super(cause);
  }

  /**
   * @param message
   * @param cause
   */
  public ItemNotFoundException(String message, Throwable cause) {
    super(message, cause);
  }
}
