
package com.pearson.docussandra;

import io.netty.handler.codec.http.HttpResponseStatus;
import org.restexpress.Response;

/**
 * Utility class for the service layer.
 *
 * @author https://github.com/JeffreyDeYoung
 */
public class ServiceUtils {
  /**
   * Sets an exception to the response object as a bad request exception.
   *
   * @param t
   * @param response
   */
  public static void setBadRequestExceptionToResponse(Throwable t, Response response) {
    response.setResponseStatus(HttpResponseStatus.BAD_REQUEST);
    response.noSerialization();
    response.setContentType("application/json");
    response.setBody("{\"error\": \"" + t.getMessage() + "\" }");
  }
}
