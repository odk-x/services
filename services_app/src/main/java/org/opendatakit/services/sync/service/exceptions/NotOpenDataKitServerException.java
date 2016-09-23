/*
 * Copyright (C) 2016 University of Washington
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.opendatakit.services.sync.service.exceptions;

import org.opendatakit.aggregate.odktables.rest.ApiConstants;
import org.opendatakit.httpclientandroidlib.HttpRequest;
import org.opendatakit.httpclientandroidlib.HttpResponse;

public class NotOpenDataKitServerException extends HttpClientWebException {

  private static final long serialVersionUID = 1L;

  public NotOpenDataKitServerException(HttpRequest request, HttpResponse response) {
    super("Response is missing required header: " + ApiConstants.OPEN_DATA_KIT_VERSION_HEADER,
        request, response);
  }

}
