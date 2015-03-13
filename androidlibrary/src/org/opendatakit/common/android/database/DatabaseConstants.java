/*
 * Copyright (C) 2014 University of Washington
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

package org.opendatakit.common.android.database;

public class DatabaseConstants {

  /**
   * key-value store table
   */

  // tablenames for the various key value stores
  public static final String KEY_VALUE_STORE_ACTIVE_TABLE_NAME = "_key_value_store_active";
  public static final String KEY_VALULE_STORE_SYNC_TABLE_NAME = "_key_value_store_sync";

  /**
   * table definitions table
   */

  // only one of these...
  public static final String TABLE_DEFS_TABLE_NAME = "_table_definitions";
  /**
   * column definitions table
   */

  // only one of these...
  public static final String COLUMN_DEFINITIONS_TABLE_NAME = "_column_definitions";

  /**
   * For ODK Survey (only)
   *
   * Tracks all the forms present in the forms directory.
   */
  public static final String SURVEY_CONFIGURATION_TABLE_NAME = "_survey_configuration";

  /**
   * For ODK Survey (only)
   *
   * Tracks which rows have been sent to the server. TODO: rework to accommodate
   * publishing to multiple formids for a given table row
   */

  public static final String UPLOADS_TABLE_NAME = "_uploads";

  /**
   * For ODK Survey (only)
   *
   * Tracks all the forms present in the forms directory.
   */

  public static final String FORMS_TABLE_NAME = "_formDefs";

  /**
   * For ODK Sync (only)
   * 
   * Tracks the ETag values for manifests and files.
   */
  
  public static final String SYNC_ETAGS_TABLE_NAME = "_sync_etags";
}
