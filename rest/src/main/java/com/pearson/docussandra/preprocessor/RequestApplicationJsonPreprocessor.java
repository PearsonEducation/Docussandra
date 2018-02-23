package com.pearson.docussandra.preprocessor;

import org.restexpress.Request;
import org.restexpress.exception.NotAcceptableException;
import org.restexpress.pipeline.Preprocessor;


/**
 * Pre processor method to check if the content-type is either the recommended application/json or
 * text/plain which appears to be default in restassured
 */
public class RequestApplicationJsonPreprocessor implements Preprocessor {

  @Override
  public void process(Request request) {

    String contentType = request.getHeader("Content-Type");
    if (contentType == null || contentType.isEmpty()) {
      return;
    } else if (!contentType.contains("application/json") && !contentType.contains("text/plain")
        && !contentType.contains("application/hal+json")) {
      throw new NotAcceptableException(
          "Content-Type header not as expected, received Content-Type : " + contentType
              + " please use Content-Type : application/json");
    }
  }
}
