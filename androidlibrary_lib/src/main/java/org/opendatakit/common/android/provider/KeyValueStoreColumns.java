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

public class KeyValueStoreColumns implements BaseColumns {

  // Names of the columns in the key value store.
  public static final String TABLE_ID = "_table_id";
  public static final String PARTITION = "_partition";
  public static final String ASPECT = "_aspect";
  public static final String KEY = "_key";
  public static final String VALUE_TYPE = "_type";
  public static final String VALUE = "_value";

  /**
   * The table creation SQL statement for a KeyValueStore table.
   *
   * @param tableName
   *          -- the name of the KVS table to be created
   * @return well-formed SQL create-table statement.
   */
  public static String getTableCreateSql(String tableName) {
    //@formatter:off
    return "CREATE TABLE IF NOT EXISTS " + tableName + " ("
        + TABLE_ID + " TEXT NOT NULL, "
        + PARTITION + " TEXT NOT NULL, "
        + ASPECT + " TEXT NOT NULL, "
        + KEY + " TEXT NOT NULL, "
        + VALUE_TYPE + " TEXT NOT NULL, "
        + VALUE + " TEXT NOT NULL, "
        + "PRIMARY KEY ( " + TABLE_ID + ", " + PARTITION + ", " + ASPECT + ", " + KEY + ") )";
    //@formatter:on
  }
}