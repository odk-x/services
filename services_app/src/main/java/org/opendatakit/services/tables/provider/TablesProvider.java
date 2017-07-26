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
package org.opendatakit.services.tables.provider;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.database.DataSetObserver;
import android.net.Uri;
import android.support.annotation.NonNull;
import android.text.TextUtils;
import android.util.Log;

import org.opendatakit.services.database.AndroidConnectFactory;
import org.opendatakit.database.DatabaseConstants;
import org.opendatakit.services.database.OdkConnectionFactorySingleton;
import org.opendatakit.services.database.OdkConnectionInterface;
import org.opendatakit.provider.TableDefinitionsColumns;
import org.opendatakit.provider.TablesProviderAPI;
import org.opendatakit.services.database.utlities.ODKDatabaseImplUtils;
import org.opendatakit.utilities.ODKFileUtils;
import org.opendatakit.logging.WebLogger;
import org.opendatakit.logging.WebLoggerIf;
import org.opendatakit.database.service.DbHandle;

import java.io.File;
import java.util.HashSet;
import java.util.List;

public class TablesProvider extends ContentProvider {
  private static final String t = "TablesProvider";

  /**
   * change to true expression if you want to debug this content provider
   */
  private static void possiblyWaitForContentProviderDebugger() {
    if ( false ) {
      android.os.Debug.waitForDebugger();
      int len = "for setting breakpoint".length();
    }
  }

  private class InvalidateMonitor extends DataSetObserver {
    final String appName;
    final DbHandle dbHandleName;

    InvalidateMonitor(String appName, DbHandle dbHandleName) {
      this.appName = appName;
      this.dbHandleName = dbHandleName;
    }

    @Override
    public void onInvalidated() {
      super.onInvalidated();
      // this releases the connection
      OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().removeConnection(appName,
          dbHandleName);
    }
  }

  public String getTablesAuthority() {
    return TablesProviderAPI.AUTHORITY;
  }

  @Override
  public boolean onCreate() {

    // IMPORTANT NOTE: the Application object is not yet created!
    AndroidConnectFactory.configure();

    try {
      ODKFileUtils.verifyExternalStorageAvailability();
      File f = new File(ODKFileUtils.getOdkFolder());
      if (!f.exists()) {
        f.mkdir();
      } else if (!f.isDirectory()) {
        Log.e(t, f.getAbsolutePath() + " is not a directory!");
        return false;
      }
    } catch (Exception e) {
      Log.e(t, "External storage not available");
      return false;
    }

    return true;
  }

  @Override
  public Cursor query(@NonNull Uri uri, String[] projection, String where, String[] whereArgs,
      String sortOrder) {
    possiblyWaitForContentProviderDebugger();

    List<String> segments = uri.getPathSegments();

    if (segments.size() < 1 || segments.size() > 2) {
      throw new IllegalArgumentException("Unknown URI (incorrect number of segments!) " + uri);
    }

    String appName = segments.get(0);
    ODKFileUtils.verifyExternalStorageAvailability();
    ODKFileUtils.assertDirectoryStructure(appName);
    WebLoggerIf logger = WebLogger.getLogger(appName);

    String uriTableId = ((segments.size() == 2) ? segments.get(1) : null);

    // Modify the where clause to account for the presence of a tableId
    String whereId;
    String[] whereIdArgs;

    if (uriTableId == null) {
      whereId = where;
      whereIdArgs = whereArgs;
    } else {
      if (TextUtils.isEmpty(where)) {
        whereId = TableDefinitionsColumns.TABLE_ID + "=?";
        whereIdArgs = new String[1];
        whereIdArgs[0] = uriTableId;
      } else {
        whereId = TableDefinitionsColumns.TABLE_ID + "=? AND (" + where
            + ")";
        whereIdArgs = new String[whereArgs.length + 1];
        whereIdArgs[0] = uriTableId;
        System.arraycopy(whereArgs, 0, whereIdArgs, 1, whereArgs.length);
      }
    }

    // Get the database and run the query
    DbHandle dbHandleName = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().generateInternalUseDbHandle();
    OdkConnectionInterface db = null;
    boolean success = false;
    Cursor c = null;
    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().getConnection(appName, dbHandleName);
      c = db.query(DatabaseConstants.TABLE_DEFS_TABLE_NAME, projection, whereId, whereIdArgs,
          null, null, sortOrder, null);

      if (c == null) {
        logger.w(t, "Unable to query database for appName: " + appName);
        return null;
      }
      // Tell the cursor what uri to watch, so it knows when its source data changes
      c.setNotificationUri(getContext().getContentResolver(), uri);
      c.registerDataSetObserver(new InvalidateMonitor(appName, dbHandleName));
      success = true;
      return c;
    } catch (Exception e) {
      logger.w(t, "Exception while querying database for appName: " + appName);
      logger.printStackTrace(e);
      return null;
    } finally {
      if ( db != null ) {
        try {
          db.releaseReference();
        } finally {
          if ( !success ) {
            // this closes the connection
            // if it was successful, then the InvalidateMonitor will close the connection
            OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().removeConnection(
                appName, dbHandleName);
          }
        }
      }
    }
  }

  @Override
  public String getType(@NonNull Uri uri) {
    List<String> segments = uri.getPathSegments();

    if (segments.size() < 1 || segments.size() > 2) {
      throw new IllegalArgumentException("Unknown URI (incorrect number of segments!) " + uri);
    }
    String uriTableId = ((segments.size() == 2) ? segments.get(1) : null);

    if (uriTableId == null) {
      return TableDefinitionsColumns.CONTENT_TYPE;
    } else {
      return TableDefinitionsColumns.CONTENT_ITEM_TYPE;
    }
  }

  @Override
  public Uri insert(@NonNull Uri uri, ContentValues values) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public synchronized int delete(@NonNull Uri uri, String selection, String[] selectionArgs) {
    possiblyWaitForContentProviderDebugger();

    List<String> segments = uri.getPathSegments();

    if (segments.size() < 1 || segments.size() > 2) {
      throw new IllegalArgumentException("Unknown URI (incorrect number of segments!) " + uri);
    }

    String appName = segments.get(0);
    ODKFileUtils.verifyExternalStorageAvailability();
    ODKFileUtils.assertDirectoryStructure(appName);
    WebLoggerIf log = WebLogger.getLogger(appName);

    String uriTableId = ((segments.size() == 2) ? segments.get(1) : null);

    // Modify the where clause to account for the presence of a tableId
    String whereId;
    String[] whereIdArgs;

    if (uriTableId == null) {
      whereId = selection;
      whereIdArgs = selectionArgs;
    } else {
      if (TextUtils.isEmpty(selection)) {
        whereId = TableDefinitionsColumns.TABLE_ID + "=?";
        whereIdArgs = new String[1];
        whereIdArgs[0] = uriTableId;
      } else {
        whereId = TableDefinitionsColumns.TABLE_ID + "=? AND (" + selection
            + ")";
        whereIdArgs = new String[selectionArgs.length + 1];
        whereIdArgs[0] = uriTableId;
        System.arraycopy(selectionArgs, 0, whereIdArgs, 1, selectionArgs.length);
      }
    }

    int deleteCount = 0;

    // Get the database and run the query
    DbHandle dbHandleName = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().generateInternalUseDbHandle();
    OdkConnectionInterface db = null;
    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().getConnection(appName, dbHandleName);
      db.beginTransactionNonExclusive();
      HashSet<String> tableIds = new HashSet<String>();
      Cursor c = null;
      try {
        c = db.query(DatabaseConstants.TABLE_DEFS_TABLE_NAME,
            new String[] { TableDefinitionsColumns.TABLE_ID }, whereId, whereIdArgs,
            null, null, null, null);
        if ( c != null && c.moveToFirst() ) {
          int idxTableId = c.getColumnIndex(TableDefinitionsColumns.TABLE_ID);
          do {
            String tableId = c.getString(idxTableId);
            tableIds.add(tableId);
          } while ( c.moveToNext());
        }
      } finally {
        if ( c != null && !c.isClosed() ) {
          c.close();
        }
      }

      for ( String tableId : tableIds ) {
        ODKDatabaseImplUtils.get().deleteTableAndAllData(db, tableId);
        deleteCount++;
      }
      db.setTransactionSuccessful();
    } finally {
      if ( db != null ) {
        try {
          if (db.inTransaction()) {
            db.endTransaction();
          }
        } finally {
          try {
            db.releaseReference();
          } finally {
            // this closes the connection
            OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().removeConnection(
                appName, dbHandleName);
          }
        }
      }
    }

    return deleteCount;
  }

  @Override
  public int update(@NonNull Uri uri, ContentValues values, String selection, String[] selectionArgs) {
    throw new UnsupportedOperationException("Not implemented");
  }

}
