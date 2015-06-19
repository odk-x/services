/*
 * Copyright (C) 2013 University of Washington
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

package org.opendatakit.common.android.provider;

import android.provider.BaseColumns;

/**
 * ODK Survey (only)
 *
 * Configuration table for ODK Survey. Holds per-application settings for ODK
 * Survey.
 */
public final class SurveyConfigurationColumns implements BaseColumns {
  // This class cannot be instantiated
  private SurveyConfigurationColumns() {
  }

  public static final String CONTENT_TYPE = "vnd.android.cursor.dir/vnd.opendatakit.survey";
  public static final String CONTENT_ITEM_TYPE = "vnd.android.cursor.item/vnd.opendatakit.survey";

  public static final String KEY = "key";
  public static final String VALUE = "value";

  /**
   * The well-known KEYs...
   */

  /* the path to the 'default' form */
  public static final String KEY_COMMON_JAVASCRIPT_PATH = "common_javascript_path";
  /*
   * whether we are talking to ODK Aggregate ODK1; ODK Aggregate ODK2, or other
   * server
   */
  public static final String KEY_SERVER_PLATFORM = "server_platform";
  /* Base URL to server */
  public static final String KEY_SERVER_URL = "server_url";
  /* username for contacting server */
  public static final String KEY_USERNAME = "username";
  /* password for contacting server */
  public static final String KEY_PASSWORD = "password";
  /* Google account authorization */
  public static final String KEY_AUTH = "auth";
  /* Google account */
  public static final String KEY_ACCOUNT = "account";
  /* non-Aggregate ODK1 server URL for form list */
  public static final String KEY_FORMLIST_URL = "formlist_url";
  /* non-Aggregate ODK1 server URL for submissions */
  public static final String KEY_SUBMISSION_URL = "submission_url";
  /* whether to show the splash screen or not */
  public static final String KEY_SHOW_SPLASH = "showSplash";
  /* path to the splash screen that should be shown */
  public static final String KEY_SPLASH_PATH = "splashPath";

  /**
   * Get the create sql for the forms table (ODK Survey only).
   *
   * @return
   */
  public static String getTableCreateSql(String tableName) {
    //@formatter:off
    return "CREATE TABLE IF NOT EXISTS " + tableName + " ("
        + _ID + " integer primary key, "
        + KEY + " text, "
        + VALUE + " text)";
    //@formatter:on
  }

}