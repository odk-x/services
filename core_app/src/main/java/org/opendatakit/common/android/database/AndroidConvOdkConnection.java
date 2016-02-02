/*
 * Copyright (C) 2015 University of Washington
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

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;

import org.opendatakit.common.android.utilities.ODKFileUtils;
import org.opendatakit.common.android.utilities.WebLogger;
import org.opendatakit.database.service.OdkDatabaseService;

import android.database.SQLException;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteException;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class AndroidConvOdkConnection implements OdkConnectionInterface{
  final Object mutex = new Object();
  /**
   * Reference count is pre-incremented to account for:
   *
   * One reference immediately held on stack after creation.
   * One reference will be added when we put this into the OdkConnectionFactoryInterface session map
   */

  final String appName;
  final SQLiteDatabase db;
  final String sessionQualifier = "dummyValue";
  Object initializationMutex = new Object();
  boolean initializationComplete = false;
  boolean initializationStatus = false;
  AndroidConvDBHelper dbHelper = null;

  // array of the underlying database handles used by all the content provider
  // instances
  private final Map<String, AndroidConvDBHelper> dbHelpers = new HashMap<String, AndroidConvDBHelper>();

  /**
   * Shared accessor to get a database handle.
   *
   * @param appName
   * @return an entry in dbHelpers
   */
  private synchronized AndroidConvDBHelper getDbHelper(Context context, String appName, String dbFilePath) {

    try {
      ODKFileUtils.verifyExternalStorageAvailability();
      ODKFileUtils.assertDirectoryStructure(appName);
    } catch ( Exception e ) {
      WebLogger.getLogger(appName).i("AndroidOdkConnection", "External storage not available -- purging dbHelpers");
      dbHelpers.clear();
      return null;
    }

    String path = ODKFileUtils.getWebDbFolder(appName);
    File webDb = new File(path);
    if ( !webDb.exists() || !webDb.isDirectory()) {
      ODKFileUtils.assertDirectoryStructure(appName);
    }

    // the assert above should have created it...
    if ( !webDb.exists() || !webDb.isDirectory()) {
      WebLogger.getLogger(appName).i("AndroidOdkConnection", "webDb directory not available -- purging dbHelpers");
      dbHelpers.clear();
      return null;
    }

    AndroidConvDBHelper dbHelper = dbHelpers.get(appName);
    if (dbHelper == null) {
      AndroidConvDBHelper h = new AndroidConvDBHelper(context, dbFilePath);
      dbHelper = h;
      dbHelpers.put(appName, h);
      // CAL: Do I need to do this?
      File targetFile = new File(dbFilePath);
      File parent = targetFile.getParentFile();
      if(!parent.exists() && !parent.mkdirs()){
        throw new IllegalStateException("Couldn't create dir: " + parent);
      }
    }
    return dbHelper;
  }


  private static String getDbFilePath(String appName) {
    File dbFile = new File(ODKFileUtils.getWebDbFolder(appName),
    ODKFileUtils.getNameOfSQLiteDatabase());
    String dbFilePath = dbFile.getAbsolutePath();
    return dbFilePath;
  }

  public static AndroidConvOdkConnection openDatabase(AppNameSharedStateContainer
    appNameSharedStateContainer, String sessionQualifier, Context context) {

    AndroidConvOdkConnection connection = new AndroidConvOdkConnection(
            appNameSharedStateContainer.getAppName(), context);
    return connection;
  }

  public AndroidConvOdkConnection(String appName, Context context) {
    // Commenting out these lines for now
    //this.mutex = mutex;
    //this.sessionQualifier = sessionQualifier;

    this.appName = appName;

    String dbFilePath = getDbFilePath(appName);
    this.dbHelper = getDbHelper(context, appName, dbFilePath);
    this.db = dbHelper.getWritableDatabase();
  }

  public boolean waitForInitializationComplete() {
    for(;;) {
      try {
        synchronized (initializationMutex) {
          if ( initializationComplete ) {
            return initializationStatus;
          }
          initializationMutex.wait(100L);
          if ( initializationComplete ) {
            return initializationStatus;
          }
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      WebLogger.getLogger(appName).i("AndroidOdkConnection", "waitForInitializationComplete - spin waiting " + Thread.currentThread().getId());
    }
  }

  public void signalInitializationComplete(boolean outcome) {
    synchronized (initializationMutex) {
      initializationStatus = outcome;
      initializationComplete = true;
      initializationMutex.notifyAll();
    }
  }

  public String getAppName() {
    return appName;
  }

  public String getSessionQualifier() {
    return sessionQualifier;
  }

  public void dumpDetail(StringBuilder b) {
    WebLogger.getLogger(appName).e(getLogTag(), "dumpDetail: This has not been implemented");
  }

  private String getLogTag() {
    return "AndroidOdkConnection:" + appName + ":" + sessionQualifier;
  }

  public void acquireReference() {
    WebLogger.getLogger(appName).e(getLogTag(), "acquireReference: This has not been implemented");
  }

  public void releaseReference() {
    WebLogger.getLogger(appName).e(getLogTag(), "releaseReference: Check implementation");

//    try {
//      commonWrapUpConnection("releaseReference");
//    } catch ( Throwable t) {
//      WebLogger.getLogger(appName).e(getLogTag(), "ReleaseReference tried to throw an exception!");
//      WebLogger.getLogger(appName).printStackTrace(t);
//    }
  }

  public int getReferenceCount() {
    return 0;
  }

  public boolean isOpen() {
    try {
      synchronized (mutex) {
        return db.isOpen();
      }
    } catch ( Throwable t ) {
      if ( t instanceof SQLiteException ) {
        throw t;
      } else {
        throw new SQLiteException("unexpected", t);
      }
    }
  }

  protected void finalize() throws Throwable {
    super.finalize();
  }

  public void close() {
    throw new IllegalStateException("this method should not be called");
  }

  private void commonWrapUpConnection(String action) throws Throwable {
    try {
      if ( isOpen() ) {
        try {
          while (inTransaction()) {
            endTransaction();
          }
        } finally {
          try {
            synchronized (mutex) {
            db.close();
            }
          } catch ( Throwable t ) {
            if ( t instanceof SQLiteException ) {
              throw t;
            } else {
              throw new SQLiteException("unexpected", t);
            }
          }
        }
      }
    } catch ( Throwable t ) {
      if ( t instanceof SQLiteException ) {
        throw t;
      } else {
        throw new SQLiteException("unexpected", t);
      }
    }
  }

  public int getVersion() throws SQLiteException {
    try {
      synchronized (mutex) {
        return db.getVersion();
      }
    } catch ( Throwable t ) {
      if ( t instanceof SQLiteException ) {
        throw t;
      } else {
        throw new SQLiteException("unexpected", t);
      }
    }
  }

  public void setVersion(int version) throws SQLiteException {
    try {
      synchronized (mutex) {
        db.setVersion(version);
      }
    } catch ( Throwable t ) {
      if ( t instanceof SQLiteException ) {
        throw t;
      } else {
        throw new SQLiteException("unexpected", t);
      }
    }
  }

  public void beginTransactionExclusive() throws SQLException {
    boolean success = false;
    try {
      synchronized (mutex) {
        // is Transaction_Mode_Immediate needed?
        // db.beginTransaction(SQLiteDatabase.TRANSACTION_MODE_IMMEDIATE, null);
        db.beginTransaction();
      }
      success = true;
    } catch ( Throwable t ) {
      if ( t instanceof SQLiteException ) {
        throw t;
      } else {
        throw new SQLiteException("unexpected", t);
      }
    }
    if ( !success ) {
      WebLogger.getLogger(appName).e("AndroidOdkConnection", "Attempting dump of all database connections");
      OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().dumpInfo(true);
    }
  }

  public void beginTransactionNonExclusive() throws SQLException {
    boolean success = false;
    try {
      synchronized (mutex) {
        db.beginTransactionNonExclusive();
      }
      success = true;
    } catch ( Throwable t ) {
      if ( t instanceof SQLiteException ) {
        throw t;
      } else {
        throw new SQLiteException("unexpected", t);
      }
    }
    if ( !success ) {
      WebLogger.getLogger(appName).e("AndroidOdkConnection", "Attempting dump of all database connections");
      OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().dumpInfo(true);
    }
  }

  public boolean inTransaction() {
    try {
      synchronized (mutex) {
        return db.inTransaction();
      }
    } catch ( Throwable t ) {
      if ( t instanceof SQLiteException ) {
        throw t;
      } else {
        throw new SQLiteException("unexpected", t);
      }
    }
  }

  public void setTransactionSuccessful() {
    try {
      synchronized (mutex) {
        db.setTransactionSuccessful();
      }
    } catch ( Throwable t ) {
      if ( t instanceof SQLiteException ) {
        throw t;
      } else {
        throw new SQLiteException("unexpected", t);
      }
    }
  }

  public void endTransaction() {
    try {
      synchronized (mutex) {
        if (db.inTransaction()) {
          db.endTransaction();
        }
      }
    } catch ( Throwable t ) {
      if ( t instanceof SQLiteException ) {
        throw t;
      } else {
        throw new SQLiteException("unexpected", t);
      }
    }
  }

  public int update(String table, ContentValues values, String whereClause, String[] whereArgs) {
    StringBuilder b = new StringBuilder();
    b.append("delete(\"").append(table).append("\",...,");
    if ( whereClause == null ) {
      b.append("null,");
    } else {
      b.append("\"").append(whereClause).append("\",");
    }
    if ( whereArgs == null ) {
      b.append("null)");
    } else {
      b.append("...)");
    }
    try {
      synchronized (mutex) {
        return db.update(table, values, whereClause, whereArgs);
      }
    } catch ( Throwable t ) {
      if ( t instanceof SQLiteException ) {
        throw t;
      } else {
        throw new SQLiteException("unexpected", t);
      }
    }
  }

  public int delete(String table, String whereClause, String[] whereArgs) {
    StringBuilder b = new StringBuilder();
    b.append("delete(\"").append(table).append("\",");
    if ( whereClause == null ) {
      b.append("null,");
    } else {
      b.append("\"").append(whereClause).append("\",");
    }
    if ( whereArgs == null ) {
      b.append("null)");
    } else {
      b.append("...)");
    }
     try {
       synchronized (mutex) {
         return db.delete(table, whereClause, whereArgs);
       }
     } catch ( Throwable t ) {
        if ( t instanceof SQLiteException ) {
          throw t;
        } else {
          throw new SQLiteException("unexpected", t);
        }
     }
  }

  public long replaceOrThrow(String table, String nullColumnHack, ContentValues initialValues)
      throws SQLException {
     StringBuilder b = new StringBuilder();
     b.append("replaceOrThrow(\"").append(table).append("\",");
     if ( nullColumnHack == null ) {
        b.append("null,...)");
     } else {
        b.append("\"").append(nullColumnHack).append("\",...)");
     }
     try {
       synchronized (mutex) {
         return db.replaceOrThrow(table, nullColumnHack, initialValues);
       }
     } catch ( Throwable t ) {
       if ( t instanceof SQLiteException ) {
         throw t;
       } else {
         throw new SQLiteException("unexpected", t);
       }
     }
  }

  public long insertOrThrow(String table, String nullColumnHack, ContentValues values)
      throws SQLException {
    StringBuilder b = new StringBuilder();
    b.append("insertOrThrow(\"").append(table).append("\",");
    if ( nullColumnHack == null ) {
      b.append("null,...)");
    } else {
      b.append("\"").append(nullColumnHack).append("\",...)");
    }
    try {
      synchronized (mutex) {
        return db.insertOrThrow(table, nullColumnHack, values);
      }
    } catch ( Throwable t ) {
      if ( t instanceof SQLiteException ) {
        throw t;
      } else {
        throw new SQLiteException("unexpected", t);
      }
    }
  }

  // Added for execSQL with null arguments
  public void execSQL(String sql) throws SQLException {
    StringBuilder b = new StringBuilder();
    b.append("execSQL(\"").append(sql);
    try {
      synchronized (mutex) {
        OdkDatabaseService.possiblyWaitForDatabaseServiceDebugger();
        db.execSQL(sql);
      }
    } catch ( Throwable t ) {
      if ( t instanceof SQLiteException ) {
        throw t;
      } else {
        throw new SQLiteException("unexpected", t);
      }
    }
  }

  public void execSQL(String sql, Object[] bindArgs) throws SQLException {
    StringBuilder b = new StringBuilder();
    b.append("execSQL(\"").append(sql).append("\",");
    if ( bindArgs == null ) {
      b.append("null)");
    } else {
      b.append("...)");
    }
    try {
      synchronized (mutex) {
        OdkDatabaseService.possiblyWaitForDatabaseServiceDebugger();
        db.execSQL(sql, bindArgs);
      }
    } catch ( Throwable t ) {
      if ( t instanceof SQLiteException ) {
        throw t;
      } else {
        throw new SQLiteException("unexpected", t);
      }
    }
  }

  public Cursor rawQuery(String sql, String[] selectionArgs) {
    StringBuilder b = new StringBuilder();
    b.append("rawQuery(\"").append(sql).append("\",");
    if ( selectionArgs == null ) {
      b.append("null)");
    } else {
      b.append("...)");
    }
    try {
      synchronized (mutex) {
        return db.rawQuery(sql, selectionArgs);
      }
    } catch ( Throwable t ) {
      if ( t instanceof SQLiteException ) {
        throw t;
      } else {
        throw new SQLiteException("unexpected", t);
      }
    }
  }

  public Cursor query(String table, String[] columns, String selection, String[] selectionArgs,
    String groupBy, String having, String orderBy, String limit) {
    StringBuilder b = new StringBuilder();
    b.append("query(\"").append(table).append("\",");
    if ( columns == null ) {
      b.append("null,");
    } else {
      b.append("...,");
    }
    if ( selection == null ) {
      b.append("null,");
    } else {
      b.append("\"").append(selection).append("\",");
    }
    if ( selectionArgs == null ) {
      b.append("null,");
    } else {
      b.append("...,");
    }
    if ( groupBy == null ) {
      b.append("null,");
    } else {
      b.append("\"").append(groupBy).append("\",");
    }
    if ( having == null ) {
      b.append("null,");
    } else {
      b.append("\"").append(having).append("\",");
    }
    if ( orderBy == null ) {
      b.append("null,");
    } else {
      b.append("\"").append(orderBy).append("\",");
    }
    if ( limit == null ) {
      b.append("null)");
    } else {
      b.append("\"").append(limit).append("\")");
    }
    try {
      synchronized (mutex) {
        return db.query(table, columns, selection, selectionArgs, groupBy, having, orderBy, limit);
      }
    } catch ( Throwable t ) {
      if ( t instanceof SQLiteException ) {
        throw t;
      } else {
        throw new SQLiteException("unexpected", t);
      }
    }
  }

  public Cursor queryDistinct(String table, String[] columns, String selection,
      String[] selectionArgs, String groupBy, String having, String orderBy, String limit) {
    StringBuilder b = new StringBuilder();
    b.append("queryDistinct(\"").append(table).append("\",");
    if ( columns == null ) {
      b.append("null,");
    } else {
      b.append("...,");
    }
    if ( selection == null ) {
      b.append("null,");
    } else {
      b.append("\"").append(selection).append("\",");
    }
    if ( selectionArgs == null ) {
      b.append("null,");
    } else {
      b.append("...,");
    }
    if ( groupBy == null ) {
      b.append("null,");
    } else {
      b.append("\"").append(groupBy).append("\",");
    }
    if ( having == null ) {
      b.append("null,");
    } else {
      b.append("\"").append(having).append("\",");
    }
    if ( orderBy == null ) {
      b.append("null,");
    } else {
      b.append("\"").append(orderBy).append("\",");
    }
    if ( limit == null ) {
      b.append("null)");
    } else {
      b.append("\"").append(limit).append("\")");
    }

    try {
      synchronized (mutex) {
        return db.query(true, table, columns, selection, selectionArgs, groupBy, having, orderBy,
                 limit);
      }
    } catch ( Throwable t ) {
      if ( t instanceof SQLiteException ) {
        throw t;
      } else {
        throw new SQLiteException("unexpected", t);
      }
    }
  }
}
