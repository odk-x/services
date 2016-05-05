package org.opendatakit.sync.service.exceptions;

import org.opendatakit.httpclientandroidlib.HttpRequest;
import org.opendatakit.httpclientandroidlib.HttpResponse;

/**
 * Created by clarice on 5/4/16.
 */
public class HttpClientWebException extends RuntimeException {
  private static final long serialVersionUID = 1L;
  private HttpRequest request;
  private HttpResponse response;

  public HttpClientWebException(HttpRequest request, HttpResponse response) {
    super();
    this.request = request;
    this.response = response;
  }

  public HttpRequest getRequest() {
    return this.request;
  }

  public HttpResponse getResponse() {
    return this.response;
  }
}
