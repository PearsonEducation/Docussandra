package com.pearson.docussandra.preprocessor;

import org.restexpress.Request;
import org.restexpress.pipeline.Preprocessor;

/**
 * Preprocessor to check if the the X-Authorization header is present and then validate it; not
 * done, just a test so far.
 */
// Will likely be deprecated
public class RequestXAuthCheck implements Preprocessor {

  @Override
  public void process(Request request) {
    if (request.getHeader("X-Authorization") != null) {
      // TODO: modulerize security https://github.com/PearsonEducation/Docussandra/issues/15
    }

    // boolean isValid = false;
    // String xauth = request.getHeader("X-Authorization");
    //
    // //TODO: needs to be changed, also need to remove this or add env checks before it goes to
    // prod
    // if (xauth.equalsIgnoreCase("letmein") || xauth.equalsIgnoreCase("nevermind")
    // || xauth.equalsIgnoreCase("hello")) {
    // isValid = true;
    // } else {
    // try {
    // Main.getPreprocessor().process(request);
    // isValid = true;
    // } catch (Exception ex) {
    // throw new UnauthorizedException("Token is invalid.");
    // }
    // }
    // if(isValid != true)
    // throw new UnauthorizedException("the token provided in the X-Authorization " +
    // "header is not valid");
    // }
  }
}
