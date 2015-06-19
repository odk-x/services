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

public class ColumnDefinitionsColumns implements BaseColumns {

  // table_id cannot be null
  public static final String TABLE_ID = "_table_id";
  // element_key cannot be null
  public static final String ELEMENT_KEY = "_element_key";
  // element_name cannot be null
  public static final String ELEMENT_NAME = "_element_name";
  // element_type cannot be null
  public static final String ELEMENT_TYPE = "_element_type";
  // list_child_element_keys can be null
  // json array of [element_key] entries
  public static final String LIST_CHILD_ELEMENT_KEYS = "_list_child_element_keys";

  // This class cannot be instantiated
  private ColumnDefinitionsColumns() {
  }

  /**
   * Get the create sql for the column_definitions table.
   *
   * @return
   */
  public static String getTableCreateSql(String tableName) {
    //@formatter:off
		return "CREATE TABLE IF NOT EXISTS " + tableName + "("
				+ TABLE_ID + " TEXT NOT NULL, "
				+ ELEMENT_KEY + " TEXT NOT NULL, "
				+ ELEMENT_NAME + " TEXT NOT NULL, "
				+ ELEMENT_TYPE + " TEXT NOT NULL, "
				+ LIST_CHILD_ELEMENT_KEYS + " TEXT NULL, "
				+ "PRIMARY KEY ( " + TABLE_ID + ", " + ELEMENT_KEY + ") )";
		//@formatter:on
  }
}
