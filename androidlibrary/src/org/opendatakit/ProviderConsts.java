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
