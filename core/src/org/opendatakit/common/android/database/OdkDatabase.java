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

import org.opendatakit.common.android.utilities.WebLogger;
import org.sqlite.database.SQLException;
import org.sqlite.database.sqlite.SQLiteDatabase;

import android.content.ContentValues;
import android.database.Cursor;
import android.util.Log;

public class OdkDatabase {
  final SQLiteDatabase db;
  String sessionQualifier;
  String lastAction = "<new>";
  
  
  public static OdkDatabase openDatabase(String appName, String dbFilePath, String sessionQualifier) {
    SQLiteDatabase db = SQLiteDatabase.openDatabase(dbFilePath, null, 
        SQLiteDatabase.ENABLE_WRITE_AHEAD_LOGGING
        | SQLiteDatabase.OPEN_READWRITE | SQLiteDatabase.CREATE_IF_NECESSARY);
    
    if (!db.isWriteAheadLoggingEnabled()) {
      WebLogger.getLogger(appName).i("OdkDatabase",
          "getWritableDatabase -- writeAheadLogging was disabled!");
      db.enableWriteAheadLogging();
    }
    return new OdkDatabase(db, sessionQualifier);
  }

  private OdkDatabase(SQLiteDatabase db, String sessionQualifier) {
    this.db = db;
    this.sessionQualifier = sessionQualifier;
  }

  private final void log() {
    Log.i("Db", "ref(" + db.getReferenceCount() + ") sq: " + sessionQualifier + " action: " + lastAction);
  }
  
  public String getLastAction() {
    return lastAction;
  }
  
  public void acquireReference() {
    db.acquireReference();
  }

  public void releaseReference() {
    db.releaseReference();
  }

  public boolean isWriteAheadLoggingEnabled() {
    return db.isWriteAheadLoggingEnabled();
  }

  public boolean enableWriteAheadLogging() {
    return db.enableWriteAheadLogging();
  }
  
  public int getVersion() {
    return db.getVersion();
  }
  
  public void setVersion(int version) {
    db.setVersion(version);
  }

  public boolean isOpen() {
    return db.isOpen();
  }

  public void close() {
    lastAction = "close(finalAction: " + lastAction + ")";
    log();
    db.close();
  }

  public void beginTransactionNonExclusive() {
    lastAction = "beginTransactionNonExclusive";
    log();
    db.beginTransactionNonExclusive();
  }

  public boolean inTransaction() {
    return db.inTransaction();
  }

  public void setTransactionSuccessful() {
    lastAction = "setTransactionSuccessful(finalAction: " + lastAction + ")";
    log();
    db.setTransactionSuccessful();
  }

  public void endTransaction() {
    lastAction = "endTransaction(finalAction: " + lastAction + ")";
    log();
    db.endTransaction();
  }

  public int update(String table, ContentValues values, String whereClause, String[] whereArgs) {
    lastAction = "update=" + table + " WHERE " + whereClause;
    log();
    return db.update(table, values, whereClause, whereArgs);
  }

  public int delete(String table, String whereClause, String[] whereArgs) {
    lastAction = "delete=" + table + " WHERE " + whereClause;
    log();
    return db.delete(table, whereClause, whereArgs);
  }

  public long replaceOrThrow(String table, String nullColumnHack, ContentValues initialValues)
      throws SQLException {
    lastAction = "replaceOrThrow=" + table;
    log();
    return db.replaceOrThrow(table, nullColumnHack, initialValues);
  }

  public long insertOrThrow(String table, String nullColumnHack, ContentValues values)
      throws SQLException {
    lastAction = "insertOrThrow=" + table;
    log();
    return db.insertOrThrow(table, nullColumnHack, values);
  }

  public void execSQL(String sql, Object[] bindArgs) throws SQLException {
    lastAction = "execSQL=" + sql;
    log();
    db.execSQL(sql, bindArgs);
  }

  public void execSQL(String sql) throws SQLException {
    lastAction = "execSQL=" + sql;
    log();
    db.execSQL(sql);
  }

  public Cursor rawQuery(String sql, String[] selectionArgs) {
    lastAction = "rawQuery=" + sql;
    log();
    return db.rawQuery(sql, selectionArgs);
  }

  public Cursor query(String table, String[] columns, String selection, String[] selectionArgs,
      String groupBy, String having, String orderBy, String limit) {
    lastAction = "query=" + table + " WHERE " + selection;
    log();
    return db.query(table, columns, selection, selectionArgs, groupBy, having, orderBy, limit);
  }

  public Cursor queryDistinct(String table, String[] columns, String selection,
      String[] selectionArgs, String groupBy, String having, String orderBy, String limit) {
    lastAction = "queryDistinct=" + table + " WHERE " + selection;
    log();
    return db.query(true, table, columns, selection, selectionArgs, groupBy, having, orderBy,
        limit);
  }

}
