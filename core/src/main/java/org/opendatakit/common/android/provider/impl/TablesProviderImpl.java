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
package org.opendatakit.common.android.provider.impl;

import java.io.File;
import java.util.HashSet;
import java.util.List;

import org.opendatakit.common.android.database.DatabaseConstants;
import org.opendatakit.common.android.database.DatabaseFactory;
import org.opendatakit.common.android.database.OdkDatabase;
import org.opendatakit.common.android.provider.TableDefinitionsColumns;
import org.opendatakit.common.android.utilities.ODKDatabaseImplUtils;
import org.opendatakit.common.android.utilities.ODKFileUtils;
import org.opendatakit.common.android.utilities.WebLogger;
import org.opendatakit.core.application.Core;
import org.opendatakit.database.service.OdkDbHandle;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.database.DataSetObserver;
import android.net.Uri;
import android.text.TextUtils;
import android.util.Log;

public abstract class TablesProviderImpl extends ContentProvider {
  private static final String t = "TablesProviderImpl";

  private class InvalidateMonitor extends DataSetObserver {
    String appName;
    OdkDbHandle dbHandleName;
    
    InvalidateMonitor(String appName, OdkDbHandle dbHandleName) {
      this.appName = appName;
      this.dbHandleName = dbHandleName;
    }
    
    @Override
    public void onInvalidated() {
      super.onInvalidated();
      DatabaseFactory.get().releaseDatabase(getContext(), appName, dbHandleName);
    }
  }

  public abstract String getTablesAuthority();

  @Override
  public boolean onCreate() {

    // IMPORTANT NOTE: the Application object is not yet created!

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
  public Cursor query(Uri uri, String[] projection, String where, String[] whereArgs,
      String sortOrder) {
    if ( Core.getInstance().shouldWaitForDebugger() ) {
      android.os.Debug.waitForDebugger();
    }

    List<String> segments = uri.getPathSegments();

    if (segments.size() < 1 || segments.size() > 2) {
      throw new IllegalArgumentException("Unknown URI (incorrect number of segments!) " + uri);
    }

    String appName = segments.get(0);
    ODKFileUtils.verifyExternalStorageAvailability();
    ODKFileUtils.assertDirectoryStructure(appName);
    WebLogger log = WebLogger.getLogger(appName);

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
        for (int i = 0; i < whereArgs.length; ++i) {
          whereIdArgs[i + 1] = whereArgs[i];
        }
      }
    }

    // Get the database and run the query
    OdkDbHandle dbHandleName = DatabaseFactory.get().generateInternalUseDbHandle();
    OdkDatabase db = null;
    boolean success = false;
    Cursor c = null;
    try {
      db = DatabaseFactory.get().getDatabase(getContext(), appName, dbHandleName);
      c = db.query(DatabaseConstants.TABLE_DEFS_TABLE_NAME, projection, whereId, whereIdArgs,
          null, null, sortOrder, null);
      success = true;
    } catch (Exception e) {
      log.w(t, "Unable to query database for appName: " + appName);
      return null;
    } finally {
      if ( !success && db != null ) {
        db.close();
        DatabaseFactory.get().releaseDatabase(getContext(), appName, dbHandleName);
      }
    }

    if (c == null) {
      log.w(t, "Unable to query database for appName: " + appName);
      return null;
    }
    // Tell the cursor what uri to watch, so it knows when its source data
    // changes
    c.setNotificationUri(getContext().getContentResolver(), uri);
    c.registerDataSetObserver(new InvalidateMonitor(appName, dbHandleName));
    return c;
  }

  @Override
  public String getType(Uri uri) {
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
  public Uri insert(Uri uri, ContentValues values) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public synchronized int delete(Uri uri, String selection, String[] selectionArgs) {
    if ( Core.getInstance().shouldWaitForDebugger() ) {
      android.os.Debug.waitForDebugger();
    }

    List<String> segments = uri.getPathSegments();

    if (segments.size() < 1 || segments.size() > 2) {
      throw new IllegalArgumentException("Unknown URI (incorrect number of segments!) " + uri);
    }

    String appName = segments.get(0);
    ODKFileUtils.verifyExternalStorageAvailability();
    ODKFileUtils.assertDirectoryStructure(appName);
    WebLogger log = WebLogger.getLogger(appName);

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
        for (int i = 0; i < selectionArgs.length; ++i) {
          whereIdArgs[i + 1] = selectionArgs[i];
        }
      }
    }

    int deleteCount = 0;
    
    // Get the database and run the query
    OdkDbHandle dbHandleName = DatabaseFactory.get().generateInternalUseDbHandle();
    OdkDatabase db = null;
    try {
      db = DatabaseFactory.get().getDatabase(getContext(), appName, dbHandleName);
      ODKDatabaseImplUtils.get().beginTransactionNonExclusive(db);
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
        ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, appName, tableId);
        deleteCount++;
      }
      db.setTransactionSuccessful();
    } finally {
      if ( db != null ) {
        db.endTransaction();
        db.close();
        DatabaseFactory.get().releaseDatabase(getContext(), appName, dbHandleName);
      }
    }
    
    return deleteCount;
  }

  @Override
  public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
    throw new UnsupportedOperationException("Not implemented");
  }

}
