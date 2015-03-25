/*
 * Copyright (C) 2015 University of Washington
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

package org.opendatakit;

import android.content.ContentResolver;

public class ProviderConsts {
  /**
   * Submission Provider
   */
  
  // for XML formatted submissions
  public static final String XML_SUBMISSION_AUTHORITY = "org.opendatakit.common.android.provider.submission.xml";
  // the full content provider prefix
  public static final String XML_SUBMISSION_URL_PREFIX = ContentResolver.SCHEME_CONTENT + "://"
      + XML_SUBMISSION_AUTHORITY;
  // for JSon formatted submissions
  public static final String JSON_SUBMISSION_AUTHORITY = "org.opendatakit.common.android.provider.submission.json";
  // the full content provider prefix
  public static final String JSON_SUBMISSION_URL_PREFIX = ContentResolver.SCHEME_CONTENT + "://"
      + JSON_SUBMISSION_AUTHORITY;


}
