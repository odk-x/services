/*
 * Copyright (C) 2014 University of Washington
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
package org.opendatakit.services.database.utilities;

import android.database.Cursor;

import org.opendatakit.aggregate.odktables.rest.TableConstants;
import org.opendatakit.database.DatabaseConstants;
import org.opendatakit.provider.SyncETagColumns;
import org.opendatakit.services.database.OdkConnectionInterface;

import java.net.URI;
import java.util.ArrayList;

public final class SyncETagsUtils {

  /**
   * Methods are all static...
   */
  private SyncETagsUtils() {
  }

  /**
   * Remove all ETags for the given table. Invoked when we delete a table...
   * 
   * @param db
   * @param tableId
   */
  public static final void deleteAllSyncETagsForTableId(OdkConnectionInterface db, String tableId) {

    ArrayList<String> bindArgs = new ArrayList<String>();
    StringBuilder b = new StringBuilder();
    //@formatter:off
    b.append("DELETE FROM ")
     .append(DatabaseConstants.SYNC_ETAGS_TABLE_NAME).append(" WHERE ")
     .append(SyncETagColumns.TABLE_ID);
    //@formatter:on
    if ( tableId == null ) {
      b.append(" IS NULL");
    } else {
      b.append("=?");
      bindArgs.add(tableId);
    }

    db.execSQL(b.toString(), bindArgs.toArray(new String[bindArgs.size()]));
  }

  /**
   * Remove app and table level manifests. Invoked when we select reset configuration
   * and the initialization task is executed.
   *
   * @param db
   */
  public static final void deleteAppAndTableLevelManifestSyncETags(OdkConnectionInterface db) {

    // TODO: Change this to only delete app and table level manifest sync etags
    // for now we will delete all rows that have _is_manifest = 1

    ArrayList<String> bindArgs = new ArrayList<String>();
    StringBuilder b = new StringBuilder();

    b.append("DELETE FROM ")
            .append(DatabaseConstants.SYNC_ETAGS_TABLE_NAME).append(" WHERE ")
            .append(SyncETagColumns.IS_MANIFEST);

    b.append("=?");
    bindArgs.add(DatabaseConstants.INT_TRUE_STRING);

    db.execSQL(b.toString(), bindArgs.toArray(new String[bindArgs.size()]));
  }

  /**
   * Remove all ETags for anything other than the given server. 
   * Invoked when we change the target sync server...
   *
   * Because the server may return urls that include the port specification, the uri
   * needs to be truncated to the scheme and hostname (e.g., https://hostname or http://hostname
   * 
   * @param db
   * @param serverUriPrefix
   */
  public static final void deleteAllSyncETagsExceptForServer(OdkConnectionInterface db, String serverUriPrefix) {

    String uriPrefix = null;
    if ( serverUriPrefix != null ) {
      URI uriBase = URI.create(serverUriPrefix).normalize();
      uriPrefix = uriBase.getScheme() + "://" + uriBase.getHost();
    }

    ArrayList<String> bindArgs = new ArrayList<String>();
    StringBuilder b = new StringBuilder();
    //@formatter:off
    b.append("DELETE FROM ")
     .append(DatabaseConstants.SYNC_ETAGS_TABLE_NAME).append(" WHERE ")
     .append(SyncETagColumns.URL);
    //@formatter:on
    if ( uriPrefix == null ) {
      // i.e., delete everything
      b.append(" IS NOT NULL");
    } else {
      // delete anything not beginning with this prefix
      b.append(" IS NULL")
       .append(" OR length(").append(SyncETagColumns.URL).append(") < abs(?)");
      // wrap with abs() to convert the string to a numeric value
      bindArgs.add(Integer.toString(uriPrefix.length()));

      String likePrefix = uriPrefix;
      likePrefix = likePrefix.replace("\\", "\\\\");
      likePrefix = likePrefix.replace("%", "\\%");
      likePrefix = likePrefix.replace("_", "\\_");

      likePrefix = likePrefix + "%";

      b.append(" OR ").append(SyncETagColumns.URL).append(" NOT LIKE ? ESCAPE ?");
      bindArgs.add(likePrefix);
      bindArgs.add("\\");
    }

     boolean inTransaction = db.inTransaction();
     try {
        if (!inTransaction) {
           db.beginTransactionNonExclusive();
        }

        db.execSQL(b.toString(), bindArgs.toArray(new String[bindArgs.size()]));

        if ( !inTransaction ) {
           db.setTransactionSuccessful();
        }
     } finally {
        if ( !inTransaction ) {
           db.endTransaction();
        }
     }
  }

  /**
   * Remove all ETags for the given server.
   * Invoked when we are resetting the app server (to ensure
   * everything we have locally is pushed to the server).
   *
   * Because the server may return urls that include the port specification, the uri
   * needs to be truncated to the scheme and hostname (e.g., https://hostname or http://hostname
   *
   * @param db
   * @param serverUriPrefix
   */
  public static final void deleteAllSyncETagsUnderServer(OdkConnectionInterface db, String serverUriPrefix) {

    if ( serverUriPrefix == null ) {
      throw new IllegalArgumentException("must specify a serverUriPrefix");
    }
    URI uriBase = URI.create(serverUriPrefix).normalize();
    String uriPrefix = uriBase.getScheme() + "://" + uriBase.getHost();

    ArrayList<String> bindArgs = new ArrayList<String>();
    StringBuilder b = new StringBuilder();
    //@formatter:off
    b.append("DELETE FROM ")
     .append(DatabaseConstants.SYNC_ETAGS_TABLE_NAME).append(" WHERE ")
     .append(SyncETagColumns.URL).append(" IS NOT NULL");
    // delete anything not beginning with this prefix...
    // ...long enough
    b.append(" AND length(").append(SyncETagColumns.URL).append(") >= abs(?)");
    // wrap with abs() to convert the string to a numeric value
    bindArgs.add(Integer.toString(uriPrefix.length()));
    // ...shares prefix

    String likePrefix = uriPrefix;
    likePrefix = likePrefix.replace("\\", "\\\\");
    likePrefix = likePrefix.replace("%", "\\%");
    likePrefix = likePrefix.replace("_", "\\_");

    likePrefix = likePrefix + "%";

    b.append(" AND ").append(SyncETagColumns.URL).append(" LIKE ? ESCAPE ?");
    bindArgs.add(likePrefix);
    bindArgs.add("\\");
    //@formatter:on

    boolean inTransaction = db.inTransaction();
    try {
      if (!inTransaction) {
        db.beginTransactionNonExclusive();
      }

      db.execSQL(b.toString(), bindArgs.toArray(new String[bindArgs.size()]));

      if ( !inTransaction ) {
        db.setTransactionSuccessful();
      }
    } finally {
      if ( !inTransaction ) {
        db.endTransaction();
      }
    }
  }
  
  public static final String getManifestSyncETag(OdkConnectionInterface db, String url, String tableId) {

    ArrayList<String> bindArgs = new ArrayList<String>();
    StringBuilder b = new StringBuilder();
    //@formatter:off
    b.append("SELECT * FROM ")
     .append(DatabaseConstants.SYNC_ETAGS_TABLE_NAME).append(" WHERE ")
     .append(SyncETagColumns.TABLE_ID);
    //@formatter:on
    if ( tableId == null ) {
      b.append(" IS NULL");
    } else {
      b.append("=?");
      bindArgs.add(tableId);
    }
    b.append(" AND ").append(SyncETagColumns.IS_MANIFEST).append("=?");
    bindArgs.add(DatabaseConstants.INT_TRUE_STRING);
    b.append(" AND ").append(SyncETagColumns.URL).append("=?");
    bindArgs.add(url);
    b.append(" ORDER BY ").append(SyncETagColumns.LAST_MODIFIED_TIMESTAMP).append(" DESC");

    Cursor c = null;
    try {
      c = db.rawQuery(b.toString(), bindArgs.toArray(new String[bindArgs.size()]));
      c.moveToFirst();

      if ( c.getCount() == 0 ) {
        // unknown...
        return null;
      }
      
      if ( c.getCount() > 1 ) {
        // TODO: log something
      }
      
      int idx = c.getColumnIndex(SyncETagColumns.ETAG_MD5_HASH);
      if ( c.isNull(idx) ) {
        // shouldn't happen...
        return null;
      }
      String value = c.getString(idx);
      return value;
    } finally {
      if ( c != null && !c.isClosed() ) {
        c.close();
      }
    }
  }

  public static final void updateManifestSyncETag(OdkConnectionInterface db, String url, String tableId, String etag) {

    ArrayList<String> bindArgs = new ArrayList<String>();
    StringBuilder b = new StringBuilder();
    //@formatter:off
    b.append("DELETE FROM ")
     .append(DatabaseConstants.SYNC_ETAGS_TABLE_NAME).append(" WHERE ")
     .append(SyncETagColumns.TABLE_ID);
    //@formatter:on
    if ( tableId == null ) {
      b.append(" IS NULL");
    } else {
      b.append("=?");
      bindArgs.add(tableId);
    }
    b.append(" AND ").append(SyncETagColumns.IS_MANIFEST).append("=?");
    bindArgs.add(DatabaseConstants.INT_TRUE_STRING);
    b.append(" AND ").append(SyncETagColumns.URL).append("=?");
    bindArgs.add(url);

    boolean inTransaction = db.inTransaction();
    try {
      if ( !inTransaction ) {
        db.beginTransactionNonExclusive();
      }
      db.execSQL(b.toString(), bindArgs.toArray(new String[bindArgs.size()]));

      if ( etag != null ) {
        b.setLength(0);
        bindArgs.clear();
        //@formatter:off
        b.append("INSERT INTO ").append(DatabaseConstants.SYNC_ETAGS_TABLE_NAME).append(" (")
         .append(SyncETagColumns.TABLE_ID).append(",")
         .append(SyncETagColumns.IS_MANIFEST).append(",")
         .append(SyncETagColumns.URL).append(",")
         .append(SyncETagColumns.LAST_MODIFIED_TIMESTAMP).append(",")
         .append(SyncETagColumns.ETAG_MD5_HASH).append(") VALUES (");
        //@formatter:on
        if ( tableId == null ) {
          b.append("NULL,");
        } else {
          b.append("?,");
          bindArgs.add(tableId);
        }
        b.append("?,?,?,?)");
        bindArgs.add(DatabaseConstants.INT_TRUE_STRING);
        bindArgs.add(url);
        bindArgs.add(TableConstants.nanoSecondsFromMillis(
            System.currentTimeMillis(), TableConstants.TIMESTAMP_LOCALE));
        bindArgs.add(etag);
  
        db.execSQL(b.toString(), bindArgs.toArray(new String[bindArgs.size()]));
      }
      if ( !inTransaction ) {
        db.setTransactionSuccessful();
      }
    } finally {
      if ( !inTransaction ) {
        db.endTransaction();
      }
    }
  }

  
  /**
   * @param db
   * @param url
   * @param tableId
   * @param modified
   * @return
   */
  public static final String getFileSyncETag(OdkConnectionInterface db, String url, String tableId, long modified) {

    ArrayList<String> bindArgs = new ArrayList<String>();
    StringBuilder b = new StringBuilder();
    //@formatter:off
    b.append("SELECT * FROM ")
     .append(DatabaseConstants.SYNC_ETAGS_TABLE_NAME).append(" WHERE ")
     .append(SyncETagColumns.TABLE_ID);
    //@formatter:on
    if ( tableId == null ) {
      b.append(" IS NULL");
    } else {
      b.append("=?");
      bindArgs.add(tableId);
    }
    b.append(" AND ").append(SyncETagColumns.IS_MANIFEST).append("=?");
    bindArgs.add(DatabaseConstants.INT_FALSE_STRING);
    b.append(" AND ").append(SyncETagColumns.URL).append("=?");
    bindArgs.add(url);
    b.append(" ORDER BY ").append(SyncETagColumns.LAST_MODIFIED_TIMESTAMP).append(" DESC");

    Cursor c = null;
    try {
      c = db.rawQuery(b.toString(), bindArgs.toArray(new String[bindArgs.size()]));
      c.moveToFirst();

      if (c.getCount() == 0) {
        // unknown...
        return null;
      }

      if (c.getCount() > 1) {
        // TODO: log something
      }

      int idx = c.getColumnIndex(SyncETagColumns.ETAG_MD5_HASH);
      int idxLMT = c.getColumnIndex(SyncETagColumns.LAST_MODIFIED_TIMESTAMP);
      if (c.isNull(idx)) {
        // shouldn't happen...
        return null;
      }
      String value = c.getString(idx);
      String lmtValue = c.getString(idxLMT);
      Long modifiedTime = TableConstants.milliSecondsFromNanos(
          lmtValue, TableConstants.TIMESTAMP_LOCALE);
      if (modifiedTime.equals(modified)) {
        return value;
      }
    } finally {
      if ( c != null && !c.isClosed()) {
        c.close();
      }
    }
    return null;
  }

  public static final void updateFileSyncETag(OdkConnectionInterface db, String url, String tableId, long modified, String etag) {

    ArrayList<String> bindArgs = new ArrayList<String>();
    StringBuilder b = new StringBuilder();
    //@formatter:off
    b.append("DELETE FROM ")
     .append(DatabaseConstants.SYNC_ETAGS_TABLE_NAME).append(" WHERE ")
     .append(SyncETagColumns.TABLE_ID);
    //@formatter:on
    if ( tableId == null ) {
      b.append(" IS NULL");
    } else {
      b.append("=?");
      bindArgs.add(tableId);
    }
    b.append(" AND ").append(SyncETagColumns.IS_MANIFEST).append("=?");
    bindArgs.add(DatabaseConstants.INT_FALSE_STRING);
    b.append(" AND ").append(SyncETagColumns.URL).append("=?");
    bindArgs.add(url);

    boolean inTransaction = db.inTransaction();
    try {
      if ( !inTransaction ) {
        db.beginTransactionNonExclusive();
      }
      db.execSQL(b.toString(), bindArgs.toArray(new String[bindArgs.size()]));

      if ( etag != null ) {
        b.setLength(0);
        bindArgs.clear();
        //@formatter:off
        b.append("INSERT INTO ").append(DatabaseConstants.SYNC_ETAGS_TABLE_NAME).append(" (")
         .append(SyncETagColumns.TABLE_ID).append(",")
         .append(SyncETagColumns.IS_MANIFEST).append(",")
         .append(SyncETagColumns.URL).append(",")
         .append(SyncETagColumns.LAST_MODIFIED_TIMESTAMP).append(",")
         .append(SyncETagColumns.ETAG_MD5_HASH).append(") VALUES (");
        //@formatter:on
        if ( tableId == null ) {
          b.append("NULL,");
        } else {
          b.append("?,");
          bindArgs.add(tableId);
        }
        b.append("?,?,?,?)");
        bindArgs.add(DatabaseConstants.INT_FALSE_STRING);
        bindArgs.add(url);
        bindArgs.add(TableConstants.nanoSecondsFromMillis(modified, TableConstants.TIMESTAMP_LOCALE));
        bindArgs.add(etag);
  
        db.execSQL(b.toString(), bindArgs.toArray(new String[bindArgs.size()]));
      }
      if ( !inTransaction ) {
        db.setTransactionSuccessful();
      }
    } finally {
      if ( !inTransaction ) {
        db.endTransaction();
      }
    }
  }

}
