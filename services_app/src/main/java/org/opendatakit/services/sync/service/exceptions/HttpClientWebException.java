/*
 * Copyright (C) 2016 University of Washington
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.opendatakit.services.sync.service.exceptions;

import org.opendatakit.httpclientandroidlib.HttpRequest;
import org.opendatakit.httpclientandroidlib.HttpResponse;

/**
 * @author clarlars@gmail.com
 */
public class HttpClientWebException extends RuntimeException {
  private static final long serialVersionUID = 1L;
  private HttpRequest request;
  private HttpResponse response;

  public HttpClientWebException(String message,
      HttpRequest request, HttpResponse response) {
    super(message);
    this.request = request;
    this.response = response;
  }

  public HttpClientWebException(String message, Throwable e,
                                HttpRequest request, HttpResponse response) {
    super(message, e);
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
