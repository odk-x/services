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

import java.util.List;

import android.net.Uri;
import android.provider.BaseColumns;

/**
 * ODK Survey (only)
 *
 * Tracks what forms are available in the ODK Survey forms directory.
 */
public final class FormsColumns implements BaseColumns {
  // This class cannot be instantiated
  private FormsColumns() {
  }

  public static final String CONTENT_TYPE = "vnd.android.cursor.dir/vnd.opendatakit.form";
  public static final String CONTENT_ITEM_TYPE = "vnd.android.cursor.item/vnd.opendatakit.form";

  /** The form_id that holds the common javascript files for Survey */
  public static final String COMMON_BASE_FORM_ID = "framework";

  // These are the only things needed for an insert
  public static final String TABLE_ID = "tableId"; // for Tables linkage
  public static final String FORM_ID = "formId";

  // these are extracted from the formDef.json for you
  /** the entire JSON settings portion of the formDef.json file */
  public static final String SETTINGS = "settings";
  /** form_version | value from the settings sheet */
  public static final String FORM_VERSION = "formVersion"; // can be null
  /** survey | display.title from the settings sheet */
  public static final String DISPLAY_NAME = "displayName";
  /** locale that the form should start in (extracted from settings) */
  public static final String DEFAULT_FORM_LOCALE = "defaultFormLocale";
  /**
   * column name for the 'instance_name' (display name) of a submission
   * (extracted from settings)
   */
  public static final String INSTANCE_NAME = "instanceName";

  // these are generated for you
  public static final String JSON_MD5_HASH = "jsonMd5Hash";
  public static final String DATE = "date"; // last modification date on the file
  public static final String FILE_LENGTH = "fileLength"; // bytes in formDef.json

  // this is generated for you but you can override if you want
  public static final String DISPLAY_SUBTEXT = "displaySubtext";

  // NOTE: this omits _ID (the primary key)
  public static final String[] formsDataColumnNames = { TABLE_ID, FORM_ID, SETTINGS, FORM_VERSION,
      DISPLAY_NAME, DEFAULT_FORM_LOCALE, INSTANCE_NAME, JSON_MD5_HASH, FILE_LENGTH, DISPLAY_SUBTEXT, DATE };

  /**
   * Get the create sql for the forms table (ODK Survey only).
   *
   * @return
   */
  public static String getTableCreateSql(String tableName) {
    //@formatter:off
    return "CREATE TABLE IF NOT EXISTS " + tableName + " ("
           + _ID + " integer not null primary key, " // for Google...
           + TABLE_ID + " text not null, " // PK part 1
           + FORM_ID + " text not null, "  // PK part 2
           + SETTINGS + " text not null, "
           + FORM_VERSION + " text, "
           + DISPLAY_NAME + " text not null, "
           + DEFAULT_FORM_LOCALE + " text null, "
           + INSTANCE_NAME + " text null, " 
           + JSON_MD5_HASH + " text not null, "
           + DISPLAY_SUBTEXT + " text not null, "
           + FILE_LENGTH + " integer not null, "
           + DATE + " integer not null " // milliseconds
           + ")";
    //@formatter:on
  }

  public static String extractAppNameFromFormsUri(Uri uri) {
    List<String> segments = uri.getPathSegments();

    if (segments.size() < 1) {
      throw new IllegalArgumentException("Unknown URI (incorrect number of segments!) " + uri);
    }

    String appName = segments.get(0);
    return appName;
  }
}