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

package org.opendatakit.common.android.provider;

import android.provider.BaseColumns;

/**
 * ODK Sync (only)
 *
 * <p>Performs two functions</p>
 * <ul><li>Tracks the md5 hash of local files and their modification
 * times.  Used to optimize the sync time by not re-computing
 * the md5 hash of the local files if their modification times 
 * have not changed.</li>
 * <li>Tracks the md5 hash of the last manifest returned by the
 * server. Subsequent requests will send this in an if-modified
 * header to minimize the size of data transmitted across the wire.
 * </li></ul>
 */
public final class SyncETagColumns implements BaseColumns {

  // This class cannot be instantiated
  private SyncETagColumns() {
  }

  public static final String TABLE_ID = "_table_id";
  public static final String IS_MANIFEST = "_is_manifest";
  public static final String URL = "_url";
  public static final String LAST_MODIFIED_TIMESTAMP = "_last_modified";
  public static final String ETAG_MD5_HASH = "_etag_md5_hash";

  /**
   * Get the create sql for the syncETags table (ODK Sync only).
   *
   * @return
   */
  public static String getTableCreateSql(String tableName) {
    //@formatter:off
    return "CREATE TABLE IF NOT EXISTS " + tableName + " ("
        + _ID + " integer primary key, "
        + TABLE_ID + " TEXT NULL, "
        + IS_MANIFEST + " INTEGER, "
        + URL + " TEXT NOT NULL, "
        + LAST_MODIFIED_TIMESTAMP + " TEXT NOT NULL, "
        + ETAG_MD5_HASH + " TEXT NOT NULL)";
    //@formatter:on
  }

}