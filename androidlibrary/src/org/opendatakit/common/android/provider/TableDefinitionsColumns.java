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

public class TableDefinitionsColumns implements BaseColumns {

  public static final String CONTENT_TYPE = "vnd.android.cursor.dir/vnd.opendatakit.table";
  public static final String CONTENT_ITEM_TYPE = "vnd.android.cursor.item/vnd.opendatakit.table";

  public static final String TABLE_ID = "_table_id";
  public static final String SCHEMA_ETAG = "_schema_etag";
  public static final String LAST_DATA_ETAG = "_last_data_etag";
  public static final String LAST_SYNC_TIME = "_last_sync_time";

  // This class cannot be instantiated
  private TableDefinitionsColumns() {
  }

  public static String getTableCreateSql(String tableName) {
    // @formatter:off
    String create = "CREATE TABLE IF NOT EXISTS " + tableName + "("
      + TABLE_ID + " TEXT NOT NULL PRIMARY KEY, " 
      + SCHEMA_ETAG + " TEXT NULL,"
      + LAST_DATA_ETAG + " TEXT NULL,"
      + LAST_SYNC_TIME + " TEXT NOT NULL )";
    // @formatter:on
    return create;
  }

}
