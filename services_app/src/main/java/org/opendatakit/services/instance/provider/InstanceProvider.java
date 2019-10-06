/*
 * Copyright (C) 2007 The Android Open Source Project
 * Copyright (C) 2011-2013 University of Washington
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

package org.opendatakit.services.instance.provider;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.database.DataSetObserver;
import android.database.SQLException;
import android.net.Uri;
import android.util.Log;

import androidx.annotation.NonNull;

import org.opendatakit.aggregate.odktables.rest.KeyValueStoreConstants;
import org.opendatakit.aggregate.odktables.rest.TableConstants;
import org.opendatakit.androidlibrary.R;
import org.opendatakit.database.DatabaseConstants;
import org.opendatakit.database.data.ColumnDefinition;
import org.opendatakit.database.data.OrderedColumns;
import org.opendatakit.database.service.DbHandle;
import org.opendatakit.database.utilities.CursorUtils;
import org.opendatakit.logging.WebLogger;
import org.opendatakit.provider.DataTableColumns;
import org.opendatakit.provider.InstanceColumns;
import org.opendatakit.provider.InstanceProviderAPI;
import org.opendatakit.provider.KeyValueStoreColumns;
import org.opendatakit.services.database.AndroidConnectFactory;
import org.opendatakit.services.database.OdkConnectionFactorySingleton;
import org.opendatakit.services.database.OdkConnectionInterface;
import org.opendatakit.services.database.utilities.ODKDatabaseImplUtils;
import org.opendatakit.services.utilities.ActiveUserAndLocale;
import org.opendatakit.utilities.ODKFileUtils;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class InstanceProvider extends ContentProvider {

  private static final String t = "InstanceProvider";

  /**
   * change to true expression if you want to debug this content provider
   */
  public static void possiblyWaitForContentProviderDebugger() {
    if ( false ) {
      android.os.Debug.waitForDebugger();
      int len = "for setting breakpoint".length();
    }
  }

  private static final String DATA_TABLE_ID_COLUMN = DataTableColumns.ID;
  private static final String DATA_TABLE_SAVEPOINT_TIMESTAMP_COLUMN = DataTableColumns.SAVEPOINT_TIMESTAMP;
  private static final String DATA_TABLE_SAVEPOINT_TYPE_COLUMN = DataTableColumns.SAVEPOINT_TYPE;

  private static final HashMap<String, String> sInstancesProjectionMap;

  private class InvalidateMonitor extends DataSetObserver {
    String appName;
    DbHandle dbHandleName;

    InvalidateMonitor(String appName, DbHandle dbHandleName) {
      this.appName = appName;
      this.dbHandleName = dbHandleName;
    }

    @Override
    public void onInvalidated() {
      super.onInvalidated();

      OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().removeConnection(appName,
          dbHandleName);
    }
  }


  public String getInstanceAuthority() {
    return InstanceProviderAPI.AUTHORITY;
  }

  private static class IdStruct {
    public final String idUploadsTable;
    public final String idDataTable;

    IdStruct(String idUploadsTable, String idDataTable) {
      this.idUploadsTable = idUploadsTable;
      this.idDataTable = idDataTable;
    }
  }

  @Override
  public boolean onCreate() {

    // IMPORTANT NOTE: the Application object is not yet created!

    // Used to ensure that the singleton has been initialized properly
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
  public Cursor query(@NonNull Uri uri, String[] projection, String selection, String[] selectionArgs,
      String sortOrder) {
    possiblyWaitForContentProviderDebugger();

    List<String> segments = uri.getPathSegments();

    if (segments.size() < 2 || segments.size() > 3) {
      throw new SQLException("Unknown URI (too many segments!) " + uri);
    }

    String appName = segments.get(0);
    ODKFileUtils.verifyExternalStorageAvailability();
    ODKFileUtils.assertDirectoryStructure(appName);

    String tableId = segments.get(1);
    // _ID in UPLOADS_TABLE_NAME
    String instanceId = (segments.size() == 3 ? segments.get(2) : null);

    DbHandle dbHandleName = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().generateInternalUseDbHandle();

    boolean success = false;
    OdkConnectionInterface db = null;
    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().getConnection(appName, dbHandleName);

      internalUpdate(db, uri, appName, tableId);

      Cursor c = internalQuery(db, uri,
          appName, tableId, instanceId,
          projection, selection, selectionArgs, sortOrder);

      if (c == null) {
        return null;
      }

      // Tell the cursor what uri to watch, so it knows when its source data
      // changes
      c.setNotificationUri(getContext().getContentResolver(), uri);
      c.registerDataSetObserver(new InvalidateMonitor(appName, dbHandleName));
      success = true;
      return c;
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

  /**
   * Update the status columns for the instance upload table
   *
   * @param db
   * @param uri
   * @param appName
   * @param tableId
   */
  void internalUpdate(OdkConnectionInterface db,
      Uri uri,
      String appName, String tableId ) {

    String instanceName = null;
    Cursor c = null;

    StringBuilder b = new StringBuilder();

    try {
      db.beginTransactionNonExclusive();

      boolean success = false;
      try {
        success = ODKDatabaseImplUtils.get().hasTableId(db, tableId);
      } catch (Exception e) {
        WebLogger.getLogger(appName).printStackTrace(e);
        throw new SQLException("Unknown URI (exception testing for tableId) " + uri);
      }
      if (!success) {
        throw new SQLException("Unknown URI (missing data table for tableId) " + uri);
      }

      try {
        c = db.query(DatabaseConstants.KEY_VALUE_STORE_ACTIVE_TABLE_NAME,
            new String[] { KeyValueStoreColumns.VALUE }, KeyValueStoreColumns.TABLE_ID + "=? AND "
                + KeyValueStoreColumns.PARTITION + "=? AND " + KeyValueStoreColumns.ASPECT
                + "=? AND " + KeyValueStoreColumns.KEY + "=?", new String[] { tableId,
                KeyValueStoreConstants.PARTITION_TABLE, KeyValueStoreConstants.ASPECT_DEFAULT,
                KeyValueStoreConstants.XML_INSTANCE_NAME }, null, null, null, null);
        if ( c != null ) {
          c.moveToFirst();
          if (c.getCount() == 1) {
            int idxInstanceName = c.getColumnIndex(KeyValueStoreColumns.VALUE);
            instanceName = c.getString(idxInstanceName);
          }
        }
      } finally {
        if ( c != null ) {
          c.close();
        }
      }

      // ARGH! we must ensure that we have records in our UPLOADS_TABLE_NAME
      // for every distinct instance in the data table.
      b.setLength(0);
      //@formatter:off
      b.append("INSERT INTO ").append(DatabaseConstants.UPLOADS_TABLE_NAME).append("(")
          .append(InstanceColumns.DATA_INSTANCE_ID).append(",")
          .append(InstanceColumns.DATA_TABLE_TABLE_ID).append(",").append("SELECT ")
          .append(InstanceColumns.DATA_INSTANCE_ID).append(",")
          .append(InstanceColumns.DATA_TABLE_TABLE_ID).append(",").append(" FROM (")
            .append("SELECT DISTINCT ").append(DATA_TABLE_ID_COLUMN).append(" as ")
            .append(InstanceColumns.DATA_INSTANCE_ID).append(",").append("? as ")
            .append(InstanceColumns.DATA_TABLE_TABLE_ID).append(" FROM ")
            .append(tableId).append(" EXCEPT SELECT DISTINCT ")
            .append(InstanceColumns.DATA_INSTANCE_ID).append(",")
            .append(InstanceColumns.DATA_TABLE_TABLE_ID).append(" FROM ")
            .append(DatabaseConstants.UPLOADS_TABLE_NAME).append(")");
      //@formatter:on

      String[] args = { tableId };
      db.execSQL(b.toString(), args);

      //@formatter:off
      b.append("WITH idname AS (SELECT t.")
              .append(DATA_TABLE_ID_COLUMN).append(" as ").append(InstanceColumns.DATA_INSTANCE_ID).append(", t.");
      if ( instanceName == null ) {
        b.append(DataTableColumns.SAVEPOINT_TIMESTAMP);
      } else {
        b.append(instanceName);
      }
      b.append(" as ").append(InstanceColumns.DATA_INSTANCE_NAME).append(" FROM ")
              .append(tableId).append(" t WHERE t.").append(DataTableColumns.SAVEPOINT_TIMESTAMP).append("=")
              .append("(select max(v.").append(DataTableColumns.SAVEPOINT_TIMESTAMP)
                  .append(") from ").append(tableId).append(" as v where v._id=t._id)")
              .append(") UPDATE ").append(DatabaseConstants.UPLOADS_TABLE_NAME).append(" SET ")
                .append(InstanceColumns.DATA_INSTANCE_NAME).append("=idname.").append(InstanceColumns.DATA_INSTANCE_NAME)
              .append(" WHERE ")
          .append(InstanceColumns.DATA_TABLE_TABLE_ID).append("=? AND ")
          .append(InstanceColumns.DATA_INSTANCE_ID).append("=idname.").append(InstanceColumns.DATA_INSTANCE_ID);
      //@formatter:on
      db.execSQL(b.toString(), args);
      db.setTransactionSuccessful();
    } finally {
      db.endTransaction();
    }
  }


  Cursor internalQuery(OdkConnectionInterface db,
      Uri uri,
      String appName, String tableId, String instanceId,
      String[] projection, String selection, String[] selectionArgs,
      String sortOrder ) {

    ActiveUserAndLocale aul =
        ActiveUserAndLocale.getActiveUserAndLocale(getContext(), appName);

    String fullQuery;
    String filterArgs[];
    Cursor c = null;

    OrderedColumns orderedDefns;

    StringBuilder b = new StringBuilder();

    boolean success = false;
    try {
      success = ODKDatabaseImplUtils.get().hasTableId(db, tableId);
    } catch (Exception e) {
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new SQLException("Unknown URI (exception testing for tableId) " + uri);
    }
    if (!success) {
      throw new SQLException("Unknown URI (missing data table for tableId) " + uri);
    }

    // Can't get away with dataTable.* because of collision with _ID column
    // get map of (elementKey -> ColumnDefinition)
    try {
      orderedDefns = ODKDatabaseImplUtils.get().getUserDefinedColumns(db, tableId);
    } catch (IllegalArgumentException e) {
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new SQLException("Unable to retrieve column definitions for tableId " + tableId);
    }

    // //////////////////////////////////////////////////////////////
    // OK we have the info we need -- now build the query we want...

    // We can now join through and access the data table rows

    b.setLength(0);
    // @formatter:off
    b.append("SELECT ");
    b.append(DatabaseConstants.UPLOADS_TABLE_NAME)
       .append(".").append(InstanceColumns._ID)
         .append(" as ").append(InstanceColumns._ID).append(",")
     .append(DatabaseConstants.UPLOADS_TABLE_NAME)
       .append(".").append(InstanceColumns.DATA_INSTANCE_ID)
         .append(" as ").append(InstanceColumns.DATA_INSTANCE_ID).append(",")
     .append(DatabaseConstants.UPLOADS_TABLE_NAME)
       .append(".").append(InstanceColumns.SUBMISSION_INSTANCE_ID)
         .append(" as ").append(InstanceColumns.SUBMISSION_INSTANCE_ID).append(",");
    // add the dataTable metadata except for _ID (which conflicts with InstanceColumns._ID)
    b.append(tableId).append(".").append(DataTableColumns.ROW_ETAG)
         .append(" as ").append(DataTableColumns.ROW_ETAG).append(",")
     .append(tableId).append(".").append(DataTableColumns.SYNC_STATE)
         .append(" as ").append(DataTableColumns.SYNC_STATE).append(",")
     .append(tableId).append(".").append(DataTableColumns.CONFLICT_TYPE)
         .append(" as ").append(DataTableColumns.CONFLICT_TYPE).append(",")
     .append(tableId).append(".").append(DataTableColumns.DEFAULT_ACCESS)
         .append(" as ").append(DataTableColumns.DEFAULT_ACCESS).append(",")
     .append(tableId).append(".").append(DataTableColumns.ROW_OWNER)
         .append(" as ").append(DataTableColumns.ROW_OWNER).append(",")
     .append(tableId).append(".").append(DataTableColumns.GROUP_READ_ONLY)
         .append(" as ").append(DataTableColumns.GROUP_READ_ONLY).append(",")
     .append(tableId).append(".").append(DataTableColumns.GROUP_MODIFY)
         .append(" as ").append(DataTableColumns.GROUP_MODIFY).append(",")
     .append(tableId).append(".").append(DataTableColumns.GROUP_PRIVILEGED)
         .append(" as ").append(DataTableColumns.GROUP_PRIVILEGED).append(",")
     .append(tableId).append(".").append(DataTableColumns.FORM_ID)
         .append(" as ").append(DataTableColumns.FORM_ID).append(",")
     .append(tableId).append(".").append(DataTableColumns.LOCALE)
         .append(" as ").append(DataTableColumns.LOCALE).append(",")
     .append(tableId).append(".").append(DataTableColumns.SAVEPOINT_TYPE)
         .append(" as ").append(DataTableColumns.SAVEPOINT_TYPE).append(",")
     .append(tableId).append(".").append(DataTableColumns.SAVEPOINT_TIMESTAMP)
         .append(" as ").append(DataTableColumns.SAVEPOINT_TIMESTAMP).append(",")
     .append(tableId).append(".").append(DataTableColumns.SAVEPOINT_CREATOR)
         .append(" as ").append(DataTableColumns.SAVEPOINT_CREATOR).append(",");
    // add the user-specified data fields in this dataTable
    for ( ColumnDefinition cd : orderedDefns.getColumnDefinitions() ) {
      if ( cd.isUnitOfRetention() ) {
        b.append(tableId).append(".").append(cd.getElementKey())
            .append(" as ").append(cd.getElementKey()).append(",");
      }
    }
    b.append("CASE WHEN ").append(DATA_TABLE_SAVEPOINT_TIMESTAMP_COLUMN).append(" IS NULL THEN null")
        .append(" WHEN ").append(InstanceColumns.XML_PUBLISH_TIMESTAMP)
        .append(" IS NULL THEN null").append(" WHEN ").append(DATA_TABLE_SAVEPOINT_TIMESTAMP_COLUMN)
        .append(" > ").append(InstanceColumns.XML_PUBLISH_TIMESTAMP).append(" THEN null")
        .append(" ELSE ").append(InstanceColumns.XML_PUBLISH_TIMESTAMP).append(" END as ")
            .append(InstanceColumns.XML_PUBLISH_TIMESTAMP).append(",");
    b.append("CASE WHEN ").append(DATA_TABLE_SAVEPOINT_TIMESTAMP_COLUMN).append(" IS NULL THEN null")
        .append(" WHEN ").append(InstanceColumns.XML_PUBLISH_TIMESTAMP)
        .append(" IS NULL THEN null").append(" WHEN ").append(DATA_TABLE_SAVEPOINT_TIMESTAMP_COLUMN)
        .append(" > ").append(InstanceColumns.XML_PUBLISH_TIMESTAMP).append(" THEN null")
        .append(" ELSE ").append(InstanceColumns.XML_PUBLISH_STATUS).append(" END as ")
            .append(InstanceColumns.XML_PUBLISH_STATUS).append(",");
    b.append("CASE WHEN ").append(DATA_TABLE_SAVEPOINT_TIMESTAMP_COLUMN).append(" IS NULL THEN null")
        .append(" WHEN ").append(InstanceColumns.XML_PUBLISH_TIMESTAMP)
        .append(" IS NULL THEN null").append(" WHEN ").append(DATA_TABLE_SAVEPOINT_TIMESTAMP_COLUMN)
        .append(" > ").append(InstanceColumns.XML_PUBLISH_TIMESTAMP).append(" THEN null")
        .append(" ELSE ").append(InstanceColumns.DISPLAY_SUBTEXT).append(" END as ")
            .append(InstanceColumns.DISPLAY_SUBTEXT).append(",");
    b.append(InstanceColumns.DATA_INSTANCE_NAME);
    b.append(" as ").append(InstanceColumns.DISPLAY_NAME);
    b.append(" FROM ");
    b.append("( SELECT * FROM ").append(tableId).append(" AS T WHERE T.")
     .append(DATA_TABLE_SAVEPOINT_TIMESTAMP_COLUMN).append("=(SELECT MAX(V.")
     .append(DATA_TABLE_SAVEPOINT_TIMESTAMP_COLUMN).append(") FROM ")
       .append(tableId).append(" AS V WHERE V.")
       .append(DATA_TABLE_ID_COLUMN).append("=T.").append(DATA_TABLE_ID_COLUMN)
     .append(" AND V.").append(DATA_TABLE_SAVEPOINT_TYPE_COLUMN).append(" IS NOT NULL").append(")")
     .append(") as ").append(tableId);
    b.append(" JOIN ").append(DatabaseConstants.UPLOADS_TABLE_NAME).append(" ON ")
        .append(tableId).append(".").append(DATA_TABLE_ID_COLUMN).append("=")
        .append(DatabaseConstants.UPLOADS_TABLE_NAME).append(".")
        .append(InstanceColumns.DATA_INSTANCE_ID).append(" AND ").append("? =")
        .append(DatabaseConstants.UPLOADS_TABLE_NAME).append(".")
        .append(InstanceColumns.DATA_TABLE_TABLE_ID);
    b.append(" WHERE ").append(DATA_TABLE_SAVEPOINT_TYPE_COLUMN).append("=?");
    // @formatter:on

    if (instanceId != null) {
      b.append(" AND ").append(DatabaseConstants.UPLOADS_TABLE_NAME).append(".")
          .append(InstanceColumns._ID).append("=?");
      String tempArgs[] = { tableId, InstanceColumns.STATUS_COMPLETE, instanceId };
      filterArgs = tempArgs;
    } else {
      String tempArgs[] = { tableId, InstanceColumns.STATUS_COMPLETE };
      filterArgs = tempArgs;
    }

    if (selection != null) {
      b.append(" AND (").append(selection).append(")");
    }

    if (selectionArgs != null) {
      String[] tempArgs = new String[filterArgs.length + selectionArgs.length];
      System.arraycopy(filterArgs, 0, tempArgs, 0, filterArgs.length);
      System.arraycopy(selectionArgs, 0, tempArgs, filterArgs.length + 0, selectionArgs.length);
      filterArgs = tempArgs;
    }

    if (sortOrder != null) {
      b.append(" ORDER BY ").append(sortOrder);
    }

    fullQuery = b.toString();


    ODKDatabaseImplUtils.AccessContext accessContext =
        ODKDatabaseImplUtils.get().getAccessContext(db, tableId, aul.activeUser, aul.rolesList);

    c = ODKDatabaseImplUtils.get().rawQuery(db, fullQuery, filterArgs, null,
        accessContext);
    return c;
  }

  @Override
  public String getType(@NonNull Uri uri) {
    // don't see the point of trying to implement this call...
    return null;
    // switch (sUriMatcher.match(uri)) {
    // case INSTANCES:
    // return InstanceColumns.CONTENT_TYPE;
    //
    // case INSTANCE_ID:
    // return InstanceColumns.CONTENT_ITEM_TYPE;
    //
    // default:
    // throw new IllegalArgumentException("Unknown URI " + uri);
    // }
  }

  @Override
  public Uri insert(@NonNull Uri uri, ContentValues initialValues) {
    throw new IllegalArgumentException("Insert not implemented!");
  }

  private String getDisplaySubtext(String xmlPublishStatus, Date xmlPublishDate) {
    if (xmlPublishDate == null) {
      return getContext().getString(R.string.not_yet_sent);
    } else if (InstanceColumns.STATUS_SUBMITTED.equalsIgnoreCase(xmlPublishStatus)) {
      return new SimpleDateFormat(getContext().getString(R.string.sent_on_date_at_time),
          Locale.getDefault()).format(xmlPublishDate);
    } else if (InstanceColumns.STATUS_SUBMISSION_FAILED.equalsIgnoreCase(xmlPublishStatus)) {
      return new SimpleDateFormat(getContext().getString(R.string.sending_failed_on_date_at_time),
          Locale.getDefault()).format(xmlPublishDate);
    } else {
      throw new IllegalStateException("Unrecognized xmlPublishStatus: " + xmlPublishStatus);
    }
  }

  /**
   * This method removes the entry from the content provider, and also removes
   * any associated files. files: form.xml, [formmd5].formdef, formname
   * {directory}
   */
  @Override
  public synchronized int delete(@NonNull Uri uri, String where, String[] whereArgs) {
    possiblyWaitForContentProviderDebugger();

    List<String> segments = uri.getPathSegments();

    if (segments.size() < 2 || segments.size() > 3) {
      throw new SQLException("Unknown URI (too many segments!) " + uri);
    }

    String appName = segments.get(0);
    ODKFileUtils.verifyExternalStorageAvailability();
    ODKFileUtils.assertDirectoryStructure(appName);

    String tableId = segments.get(1);
    // _ID in UPLOADS_TABLE_NAME
    String instanceId = (segments.size() == 3 ? segments.get(2) : null);

    DbHandle dbHandleName = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().generateInternalUseDbHandle();
    OdkConnectionInterface db = null;
    List<IdStruct> idStructs = new ArrayList<IdStruct>();
    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().getConnection(appName, dbHandleName);
      db.beginTransactionNonExclusive();

      boolean success = false;
      try {
        success = ODKDatabaseImplUtils.get().hasTableId(db, tableId);
      } catch (Exception e) {
        WebLogger.getLogger(appName).printStackTrace(e);
        throw new SQLException("Unknown URI (exception testing for tableId) " + uri);
      }

      if (success) {
        // delete the entries matching the filter criteria
        if (segments.size() == 2) {
          where = "(" + where + ") AND (" + InstanceColumns.DATA_INSTANCE_ID + "=? )";
          if (whereArgs != null) {
            String[] args = new String[whereArgs.length + 1];
            System.arraycopy(whereArgs, 0, args, 0, whereArgs.length);
            args[whereArgs.length] = instanceId;
            whereArgs = args;
          } else {
            whereArgs = new String[] { instanceId };
          }
        }

        internalUpdate(db, uri, appName, tableId );

        Cursor del = null;
        try {
          del = internalQuery(db,
              uri,
              appName, tableId, instanceId,
              null, where, whereArgs, null);
          del.moveToPosition(-1);
          while (del.moveToNext()) {
            String iId = CursorUtils.getIndexAsString(del,
                del.getColumnIndex(InstanceColumns._ID));
            String iIdDataTable = CursorUtils.getIndexAsString(del,
                del.getColumnIndex(InstanceColumns.DATA_INSTANCE_ID));
            idStructs.add(new IdStruct(iId, iIdDataTable));
            String path = ODKFileUtils.getInstanceFolder(appName, tableId, iIdDataTable);
            File f = new File(path);
            if (f.exists()) {
              if (f.isDirectory()) {
                ODKFileUtils.deleteDirectory(f);
              } else {
                f.delete();
              }
            }

          }
        } catch (IOException e) {
          WebLogger.getLogger(appName).printStackTrace(e);
          throw new IllegalArgumentException("Unable to delete instance directory: " + e.toString());
        } finally {
          if (del != null) {
            del.close();
          }
        }
      } else {
        // delete anything we find, since the table doesn't exist
        Cursor del = null;
        try {
          where = InstanceColumns.DATA_TABLE_TABLE_ID + "=?";
          whereArgs = new String[] { tableId };
          del = db.query(DatabaseConstants.UPLOADS_TABLE_NAME, null,
              where, whereArgs, null, null, null, null);
          del.moveToPosition(-1);
          while (del.moveToNext()) {
            String iId = CursorUtils.getIndexAsString(del,
                del.getColumnIndex(InstanceColumns._ID));
            String iIdDataTable = CursorUtils.getIndexAsString(del,
                del.getColumnIndex(InstanceColumns.DATA_INSTANCE_ID));
            idStructs.add(new IdStruct(iId, iIdDataTable));
            String path = ODKFileUtils.getInstanceFolder(appName, tableId, iIdDataTable);
            File f = new File(path);
            if (f.exists()) {
              if (f.isDirectory()) {
                ODKFileUtils.deleteDirectory(f);
              } else {
                f.delete();
              }
            }
          }
        } catch (IOException e) {
          WebLogger.getLogger(appName).printStackTrace(e);
          throw new IllegalArgumentException("Unable to delete instance directory: " + e.toString());
        } finally {
          if (del != null) {
            del.close();
          }
        }
      }

      for (IdStruct idStruct : idStructs) {
        db.delete(DatabaseConstants.UPLOADS_TABLE_NAME, InstanceColumns.DATA_INSTANCE_ID + "=?",
            new String[] { idStruct.idUploadsTable });
        db.delete(tableId, DATA_TABLE_ID_COLUMN + "=?", new String[] { idStruct.idDataTable });
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
    getContext().getContentResolver().notifyChange(uri, null);
    return idStructs.size();
  }

  @Override
  public synchronized int update(@NonNull Uri uri, ContentValues cv, String where, String[] whereArgs) {
    possiblyWaitForContentProviderDebugger();

    List<String> segments = uri.getPathSegments();

    if (segments.size() != 3) {
      throw new SQLException("Unknown URI (does not specify instance!) " + uri);
    }

    String appName = segments.get(0);
    ODKFileUtils.verifyExternalStorageAvailability();
    ODKFileUtils.assertDirectoryStructure(appName);

    String tableId = segments.get(1);
    // _ID in UPLOADS_TABLE_NAME
    String instanceId = segments.get(2);

    DbHandle dbHandleName = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().generateInternalUseDbHandle();
    OdkConnectionInterface db = null;
    int count = 0;
    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().getConnection(appName, dbHandleName);
      db.beginTransactionNonExclusive();

      boolean success = false;
      try {
        success = ODKDatabaseImplUtils.get().hasTableId(db, tableId);
      } catch (Exception e) {
        WebLogger.getLogger(appName).printStackTrace(e);
        throw new SQLException("Unknown URI (exception testing for tableId) " + uri);
      }
      if (!success) {
        throw new SQLException("Unknown URI (missing data table for tableId) " + uri);
      }

      internalUpdate(db, uri, appName, tableId );

      // run the query to get all the ids...
      List<IdStruct> idStructs = new ArrayList<IdStruct>();
      Cursor ref = null;
      try {
        // use this provider's query interface to get the set of ids that
        // match (if any)
        ref = internalQuery(db,
            uri,
            appName, tableId, instanceId,
            null, where, whereArgs, null);
        ref.moveToFirst();
        if (ref.getCount() != 0) {
          do {
            String iId = CursorUtils.getIndexAsString(ref,
                ref.getColumnIndex(InstanceColumns._ID));
            String iIdDataTable = CursorUtils.getIndexAsString(ref,
                ref.getColumnIndex(InstanceColumns.DATA_INSTANCE_ID));
            idStructs.add(new IdStruct(iId, iIdDataTable));
          } while (ref.moveToNext());
        }
      } finally {
        if (ref != null) {
          ref.close();
        }
      }

      // update the values string...
      if (cv.containsKey(InstanceColumns.XML_PUBLISH_STATUS)) {
        Date xmlPublishDate = new Date();
        cv.put(InstanceColumns.XML_PUBLISH_TIMESTAMP,
            TableConstants.nanoSecondsFromMillis(xmlPublishDate.getTime(), Locale.ROOT));
        String xmlPublishStatus = cv.getAsString(InstanceColumns.XML_PUBLISH_STATUS);
        if (!cv.containsKey(InstanceColumns.DISPLAY_SUBTEXT)) {
          String text = getDisplaySubtext(xmlPublishStatus, xmlPublishDate);
          cv.put(InstanceColumns.DISPLAY_SUBTEXT, text);
        }
      }

      Map<String,Object> values = new HashMap<String,Object>();
      for ( String key : cv.keySet()) {
        values.put(key, cv.get(key));
      }

      Object[] args = new String[1];
      for (IdStruct idStruct : idStructs) {
        args[0] = idStruct.idUploadsTable;
        count += db.update(DatabaseConstants.UPLOADS_TABLE_NAME, values,
            InstanceColumns._ID + "=?", args);
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
    getContext().getContentResolver().notifyChange(uri, null);
    return count;
  }

  static {

    sInstancesProjectionMap = new HashMap<String, String>();
    sInstancesProjectionMap.put(InstanceColumns._ID, InstanceColumns._ID);
    sInstancesProjectionMap.put(InstanceColumns.DATA_INSTANCE_ID, InstanceColumns.DATA_INSTANCE_ID);
    sInstancesProjectionMap.put(InstanceColumns.XML_PUBLISH_TIMESTAMP,
        InstanceColumns.XML_PUBLISH_TIMESTAMP);
    sInstancesProjectionMap.put(InstanceColumns.XML_PUBLISH_STATUS,
        InstanceColumns.XML_PUBLISH_STATUS);
  }

}
