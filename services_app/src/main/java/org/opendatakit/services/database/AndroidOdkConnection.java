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

package org.opendatakit.services.database;

import android.database.Cursor;

import org.opendatakit.logging.WebLogger;
import org.opendatakit.utilities.ODKFileUtils;
import org.sqlite.database.SQLException;
import org.sqlite.database.sqlite.SQLiteConnectionBase;
import org.sqlite.database.sqlite.SQLiteDatabaseConfiguration;
import org.sqlite.database.sqlite.SQLiteException;

import java.io.File;
import java.util.Map;

public final class AndroidOdkConnection implements OdkConnectionInterface {
  final Object mutex;
  /**
   * Reference count is pre-incremented to account for:
   * <p/>
   * One reference immediately held on stack after creation.
   * One reference will be added when we put this into the OdkConnectionFactoryInterface session map
   */
  final OperationLog operationLog;
  final String appName;
  final SQLiteConnectionBase db;
  final String sessionQualifier;
  int referenceCount = 1;
  final Object initializationMutex = new Object();
  boolean initializationComplete = false;
  boolean initializationStatus = false;

  private static String getDbFilePath(String appName) {
    File dbFile = new File(ODKFileUtils.getWebDbFolder(appName),
        ODKFileUtils.getNameOfSQLiteDatabase());
    String dbFilePath = dbFile.getAbsolutePath();
    return dbFilePath;
  }

  public static AndroidOdkConnection openDatabase(
      AppNameSharedStateContainer appNameSharedStateContainer, String sessionQualifier) {

    String appName = appNameSharedStateContainer.getAppName();
    String dbFilePath = getDbFilePath(appName);

    SQLiteDatabaseConfiguration configuration = new SQLiteDatabaseConfiguration(appName, dbFilePath,
        SQLiteConnectionBase.ENABLE_WRITE_AHEAD_LOGGING |
            SQLiteConnectionBase.OPEN_READWRITE | SQLiteConnectionBase.CREATE_IF_NECESSARY |
            SQLiteConnectionBase.NO_LOCALIZED_COLLATORS, sessionQualifier);

    boolean success = false;
    SQLiteConnectionBase db = null;
    try {
      db = SQLConnectionFactory.get(configuration, appNameSharedStateContainer.getOperationLog(), null,
          sessionQualifier);

      // this might throw an exception
      db.open();

      // this isn't going to throw an exception
      AndroidOdkConnection connection = new AndroidOdkConnection(
          appNameSharedStateContainer.getSessionMutex(), appName,
          appNameSharedStateContainer.getOperationLog(), db, sessionQualifier);
      success = true;
      return connection;
    } finally {
      if (!success) {
        if ( db != null ) {
          db.releaseReference();
        }
      }
    }
  }

  private AndroidOdkConnection(Object mutex, String appName, OperationLog operationLog,
                               SQLiteConnectionBase db, String sessionQualifier) {
    this.mutex = mutex;
    this.appName = appName;
    this.operationLog = operationLog;
    this.db = db;
    this.sessionQualifier = sessionQualifier;
  }

  /**
   * Work-around for jacoco ART issue https://code.google.com/p/android/issues/detail?id=80961
   *
   * @return
   */
  private Boolean internalWaitForInitializationComplete() throws InterruptedException {
    synchronized (initializationMutex) {
      if (initializationComplete) {
        return initializationStatus;
      }

      initializationMutex.wait(100L);

      if (initializationComplete) {
        return initializationStatus;
      }
    }
    // not yet completed.
    return null;
  }

  public boolean waitForInitializationComplete() {
    for (; ; ) {
      try {
        // invoke method
        // Work-around for jacoco ART issue https://code.google.com/p/android/issues/detail?id=80961
        Boolean outcome = internalWaitForInitializationComplete();
        if ( outcome != null ) {
          return outcome;
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      WebLogger.getLogger(appName).i("AndroidOdkConnection",
          "waitForInitializationComplete - spin waiting " + Thread.currentThread().getId());
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
    db.dump(b, true);
  }

  private String getLogTag() {
    return "AndroidOdkConnection:" + appName + ":" + sessionQualifier;
  }

  public void acquireReference() {
    synchronized (mutex) {
      ++referenceCount;
    }
  }

  public void releaseReference() {
    boolean refCountIsZero = false;
    boolean refCountIsNegative = false;
    synchronized (mutex) {
      --referenceCount;
      refCountIsZero = (referenceCount == 0);
      refCountIsNegative = (referenceCount < 0);
    }
    try {
      if (refCountIsZero) {
        commonWrapUpConnection("releaseReferenceIsZero");
      } else if (refCountIsNegative) {
        commonWrapUpConnection("releaseReferenceIsNegative");
      }
    } catch (Throwable t) {
      WebLogger.getLogger(appName).e(getLogTag(), "ReleaseReference tried to throw an exception!");
      WebLogger.getLogger(appName).printStackTrace(t);
    }
  }

  public int getReferenceCount() {
    // don't log this -- used by dump()
    synchronized (mutex) {
      return referenceCount;
    }
  }

  /**
   * Work-around for jacoco ART issue https://code.google.com/p/android/issues/detail?id=80961
   * @return true if database connection is open
   */
  private boolean internalIsOpen() {
    synchronized (mutex) {
      return db.isOpen();
    }
  }

  public boolean isOpen() {
    final int cookie = operationLog.beginOperation(sessionQualifier, "isOpen()", null, null);

    try {
      // invoke method
      // Work-around for jacoco ART issue https://code.google.com/p/android/issues/detail?id=80961
      return internalIsOpen();
    } catch (Throwable t) {
      operationLog.failOperation(cookie, t);
      if (t instanceof SQLiteException) {
        throw t;
      } else {
        throw new SQLiteException("unexpected", t);
      }
    } finally {
      operationLog.endOperation(cookie);
    }
  }

  protected void finalize() throws Throwable {

    try {
      int refCount = getReferenceCount();
      if (refCount != 0) {
        WebLogger.getLogger(appName).e(getLogTag(),
            "An AndroidOdkConnection object for database '" + appName + "' sessionQualifier '"
                + sessionQualifier + "' was leaked!  Please fix your "
                + "application to end transactions in progress properly and to close the "
                + "database when it is no longer needed.");
      }
    } finally {
      super.finalize();
    }
  }

  public void close() {
    throw new IllegalStateException("this method should not be called");
  }

  /**
   * Work-around for jacoco ART issue https://code.google.com/p/android/issues/detail?id=80961
   * @return
   */
  private void internalCommonWrapUpConnection_Close() {
    synchronized (mutex) {
      db.close();
    }
  }

  private void commonWrapUpConnection(String action) throws Throwable {
    final int cookie = operationLog
        .beginOperation(sessionQualifier, "commonWrapUpConnection(\"" + action + "\")", null, null);

    try {
      if (isOpen()) {
        try {
          while (inTransaction()) {
            endTransaction();
          }
        } finally {
          final int innerCookie = operationLog.beginOperation(sessionQualifier,
              "commonWrapUpConnection(\"" + action + "\") -- close", null, null);

          try {
            // invoke method
            // Work-around for jacoco ART issue https://code.google.com/p/android/issues/detail?id=80961
            internalCommonWrapUpConnection_Close();
          } catch (Throwable t) {
            operationLog.failOperation(innerCookie, t);
            if (t instanceof SQLiteException) {
              throw t;
            } else {
              throw new SQLiteException("unexpected", t);
            }
          } finally {
            operationLog.endOperation(innerCookie);
          }
        }
      }
    } catch (Throwable t) {
      operationLog.failOperation(cookie, t);
      if (t instanceof SQLiteException) {
        throw t;
      } else {
        throw new SQLiteException("unexpected", t);
      }
    } finally {
      operationLog.endOperation(cookie);
    }
  }

  /**
   * Work-around for jacoco ART issue https://code.google.com/p/android/issues/detail?id=80961
   * @return
   */
  private int internalGetVersion() {
    synchronized (mutex) {
      return db.getVersion();
    }
  }

  public int getVersion() throws SQLiteException {
    final int cookie = operationLog.beginOperation(sessionQualifier, "getVersion()", null, null);

    try {
      // invoke method
      // Work-around for jacoco ART issue https://code.google.com/p/android/issues/detail?id=80961
      return internalGetVersion();
    } catch (Throwable t) {
      operationLog.failOperation(cookie, t);
      if (t instanceof SQLiteException) {
        throw t;
      } else {
        throw new SQLiteException("unexpected", t);
      }
    } finally {
      operationLog.endOperation(cookie);
    }
  }

  /**
   * Work-around for jacoco ART issue https://code.google.com/p/android/issues/detail?id=80961
   *
   * @return
   */
  private void internalSetVersion(int version) {
    synchronized (mutex) {
      db.setVersion(version);
    }
  }

  public void setVersion(int version) throws SQLiteException {
    final int cookie = operationLog
        .beginOperation(sessionQualifier, "setVersion(" + version + ")", null, null);

    try {
      // invoke method
      // Work-around for jacoco ART issue https://code.google.com/p/android/issues/detail?id=80961
      internalSetVersion(version);
    } catch (Throwable t) {
      operationLog.failOperation(cookie, t);
      if (t instanceof SQLiteException) {
        throw t;
      } else {
        throw new SQLiteException("unexpected", t);
      }
    } finally {
      operationLog.endOperation(cookie);
    }
  }

  /**
   * Work-around for jacoco ART issue https://code.google.com/p/android/issues/detail?id=80961
   * @return
   */
  private void internalBeginTransactionExclusive() {
    synchronized (mutex) {
      db.beginTransaction(SQLiteConnectionBase.TRANSACTION_MODE_IMMEDIATE, null);
    }
  }

  public void beginTransactionExclusive() throws SQLException {
    boolean success = false;
    final int cookie = operationLog
        .beginOperation(sessionQualifier, "beginTransactionExclusive()", null, null);

    try {
      // invoke method
      // Work-around for jacoco ART issue https://code.google.com/p/android/issues/detail?id=80961
      internalBeginTransactionExclusive();
      success = true;
    } catch (Throwable t) {
      operationLog.failOperation(cookie, t);
      if (t instanceof SQLiteException) {
        throw t;
      } else {
        throw new SQLiteException("unexpected", t);
      }
    } finally {
      operationLog.endOperation(cookie);
    }
    if (!success) {
      WebLogger.getLogger(appName)
          .e("AndroidOdkConnection", "Attempting dump of all database connections");
      OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().dumpInfo(true);
    }
  }

  /**
   * Work-around for jacoco ART issue https://code.google.com/p/android/issues/detail?id=80961
   * @return
   */
  private void internalBeginTransactionNonExclusive() {
    synchronized (mutex) {
      db.beginTransaction(SQLiteConnectionBase.TRANSACTION_MODE_DEFERRED, null);
    }
  }

  public void beginTransactionNonExclusive() throws SQLException {
    boolean success = false;
    final int cookie = operationLog
        .beginOperation(sessionQualifier, "beginTransactionNonExclusive()", null, null);

    try {
      // invoke method
      // Work-around for jacoco ART issue https://code.google.com/p/android/issues/detail?id=80961
      internalBeginTransactionNonExclusive();
      success = true;
    } catch (Throwable t) {
      operationLog.failOperation(cookie, t);
      if (t instanceof SQLiteException) {
        throw t;
      } else {
        throw new SQLiteException("unexpected", t);
      }
    } finally {
      operationLog.endOperation(cookie);
    }
    if (!success) {
      WebLogger.getLogger(appName)
          .e("AndroidOdkConnection", "Attempting dump of all database connections");
      OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().dumpInfo(true);
    }
  }

  /**
   * Work-around for jacoco ART issue https://code.google.com/p/android/issues/detail?id=80961
   * @return
   */
  private boolean internalInTransaction() {
    synchronized (mutex) {
      return db.inTransaction();
    }
  }

  public boolean inTransaction() {
    final int cookie = operationLog.beginOperation(sessionQualifier, "inTransaction()", null, null);

    try {
      // invoke method
      // Work-around for jacoco ART issue https://code.google.com/p/android/issues/detail?id=80961
      return internalInTransaction();
    } catch (Throwable t) {
      operationLog.failOperation(cookie, t);
      if (t instanceof SQLiteException) {
        throw t;
      } else {
        throw new SQLiteException("unexpected", t);
      }
    } finally {
      operationLog.endOperation(cookie);
    }
  }

  /**
   * Work-around for jacoco ART issue https://code.google.com/p/android/issues/detail?id=80961
   * @return
   */
  private void internalSetTransactionSuccessful() {
    synchronized (mutex) {
      db.setTransactionSuccessful();
    }
  }

  public void setTransactionSuccessful() {
    final int cookie = operationLog
        .beginOperation(sessionQualifier, "setTransactionSuccessful()", null, null);

    try {
      // invoke method
      // Work-around for jacoco ART issue https://code.google.com/p/android/issues/detail?id=80961
      internalSetTransactionSuccessful();
    } catch (Throwable t) {
      operationLog.failOperation(cookie, t);
      if (t instanceof SQLiteException) {
        throw t;
      } else {
        throw new SQLiteException("unexpected", t);
      }
    } finally {
      operationLog.endOperation(cookie);
    }
  }

  /**
   * Work-around for jacoco ART issue https://code.google.com/p/android/issues/detail?id=80961
   * @return
   */
  private void internalEndTransaction() {
    synchronized (mutex) {
      db.endTransaction();
    }
  }

  public void endTransaction() {
    final int cookie = operationLog
        .beginOperation(sessionQualifier, "endTransaction()", null, null);
    try {
      // invoke method
      // Work-around for jacoco ART issue https://code.google.com/p/android/issues/detail?id=80961
      internalEndTransaction();
    } catch (Throwable t) {
      operationLog.failOperation(cookie, t);
      if (t instanceof SQLiteException) {
        throw t;
      } else {
        throw new SQLiteException("unexpected", t);
      }
    } finally {
      operationLog.endOperation(cookie);
    }
  }

  /**
   * Work-around for jacoco ART issue https://code.google.com/p/android/issues/detail?id=80961
   *
   * @return number of rows updated
   */
  private int internalUpdate(String table, Map<String, Object> values, String whereClause,
      Object[] whereArgs) {
    synchronized (mutex) {
      return db.update(table, values, whereClause, whereArgs);
    }
  }

  public int update(String table, Map<String, Object> values, String whereClause,
      Object[] whereArgs) {
    StringBuilder b = new StringBuilder();
    b.append("delete(\"").append(table).append("\",...,");
    if (whereClause == null) {
      b.append("null,");
    } else {
      b.append("\"").append(whereClause).append("\",");
    }
    if (whereArgs == null) {
      b.append("null)");
    } else {
      b.append("...)");
    }
    final int cookie = operationLog.beginOperation(sessionQualifier, b.toString(), null, null);
    try {
      // invoke method
      // Work-around for jacoco ART issue https://code.google.com/p/android/issues/detail?id=80961
      return internalUpdate(table, values, whereClause, whereArgs);
    } catch (Throwable t) {
      operationLog.failOperation(cookie, t);
      if (t instanceof SQLiteException) {
        throw t;
      } else {
        throw new SQLiteException("unexpected", t);
      }
    } finally {
      operationLog.endOperation(cookie);
    }
  }

  /**
   * Work-around for jacoco ART issue https://code.google.com/p/android/issues/detail?id=80961
   *
   * @return number of rows updated
   */
  private int internalDelete(String table, String whereClause, Object[] whereArgs) {
    synchronized (mutex) {
      return db.delete(table, whereClause, whereArgs);
    }
  }

  public int delete(String table, String whereClause, Object[] whereArgs) {
    StringBuilder b = new StringBuilder();
    b.append("delete(\"").append(table).append("\",");
    if (whereClause == null) {
      b.append("null,");
    } else {
      b.append("\"").append(whereClause).append("\",");
    }
    if (whereArgs == null) {
      b.append("null)");
    } else {
      b.append("...)");
    }
    final int cookie = operationLog.beginOperation(sessionQualifier, b.toString(), null, null);

    try {
      // invoke method
      // Work-around for jacoco ART issue https://code.google.com/p/android/issues/detail?id=80961
      return internalDelete(table, whereClause, whereArgs);
    } catch (Throwable t) {
      operationLog.failOperation(cookie, t);
      if (t instanceof SQLiteException) {
        throw t;
      } else {
        throw new SQLiteException("unexpected", t);
      }
    } finally {
      operationLog.endOperation(cookie);
    }
  }

  /**
   * Work-around for jacoco ART issue https://code.google.com/p/android/issues/detail?id=80961
   * @return
   */
  private void internalReplaceOrThrow(String table, String nullColumnHack, Map<String, Object> initialValues) {
    synchronized (mutex) {
      db.replaceOrThrow(table, nullColumnHack, initialValues);
    }
  }

  public void replaceOrThrow(String table, String nullColumnHack, Map<String, Object> initialValues)
      throws SQLException {
    StringBuilder b = new StringBuilder();
    b.append("replaceOrThrow(\"").append(table).append("\",");
    if (nullColumnHack == null) {
      b.append("null,...)");
    } else {
      b.append("\"").append(nullColumnHack).append("\",...)");
    }
    final int cookie = operationLog.beginOperation(sessionQualifier, b.toString(), null, null);

    try {
      // invoke method
      // Work-around for jacoco ART issue https://code.google.com/p/android/issues/detail?id=80961
      internalReplaceOrThrow(table, nullColumnHack, initialValues);
    } catch (Throwable t) {
      operationLog.failOperation(cookie, t);
      if (t instanceof SQLiteException) {
        throw t;
      } else {
        throw new SQLiteException("unexpected", t);
      }
    } finally {
      operationLog.endOperation(cookie);
    }
  }

  /**
   * Work-around for jacoco ART issue https://code.google.com/p/android/issues/detail?id=80961
   * @return
   */
  private void internalInsertOrThrow(String table, String nullColumnHack, Map<String, Object> values) {
    synchronized (mutex) {
      db.insertOrThrow(table, nullColumnHack, values);
    }
  }

  public void insertOrThrow(String table, String nullColumnHack, Map<String, Object> values)
      throws SQLException {
    StringBuilder b = new StringBuilder();
    b.append("insertOrThrow(\"").append(table).append("\",");
    if (nullColumnHack == null) {
      b.append("null,...)");
    } else {
      b.append("\"").append(nullColumnHack).append("\",...)");
    }
    final int cookie = operationLog.beginOperation(sessionQualifier, b.toString(), null, null);

    try {
      // invoke method
      // Work-around for jacoco ART issue https://code.google.com/p/android/issues/detail?id=80961
      internalInsertOrThrow(table, nullColumnHack, values);
      return;
    } catch (Throwable t) {
      operationLog.failOperation(cookie, t);
      if (t instanceof SQLiteException) {
        throw t;
      } else {
        throw new SQLiteException("unexpected", t);
      }
    } finally {
      operationLog.endOperation(cookie);
    }
  }

  /**
   * Work-around for jacoco ART issue https://code.google.com/p/android/issues/detail?id=80961
   * @return
   */
  private void internalExecSQL(String sql, Object[] bindArgs) {
    synchronized (mutex) {
      db.execSQL(sql, bindArgs);
    }
  }

  public void execSQL(String sql, Object[] bindArgs) throws SQLException {
    StringBuilder b = new StringBuilder();
    b.append("execSQL(\"").append(sql).append("\",");
    if (bindArgs == null) {
      b.append("null)");
    } else {
      b.append("...)");
    }
    final int cookie = operationLog.beginOperation(sessionQualifier, b.toString(), null, null);

    try {
      // invoke method
      // Work-around for jacoco ART issue https://code.google.com/p/android/issues/detail?id=80961
      internalExecSQL(sql, bindArgs);
    } catch (Throwable t) {
      operationLog.failOperation(cookie, t);
      if (t instanceof SQLiteException) {
        throw t;
      } else {
        throw new SQLiteException("unexpected", t);
      }
    } finally {
      operationLog.endOperation(cookie);
    }
  }

  /**
   * Work-around for jacoco ART issue https://code.google.com/p/android/issues/detail?id=80961
   * @return
   */
  private Cursor internalRawQuery(String sql, Object[] selectionArgs) {
    synchronized (mutex) {
      return db.rawQuery(sql, selectionArgs, null);
    }
  }

  public Cursor rawQuery(String sql, Object[] selectionArgs) {
    StringBuilder b = new StringBuilder();
    b.append("rawQuery(\"").append(sql).append("\",");
    if (selectionArgs == null) {
      b.append("null)");
    } else {
      b.append("...)");
    }
    final int cookie = operationLog.beginOperation(sessionQualifier, b.toString(), null, null);

    try {
      // invoke method
      // Work-around for jacoco ART issue https://code.google.com/p/android/issues/detail?id=80961
      return internalRawQuery(sql, selectionArgs);
    } catch (Throwable t) {
      operationLog.failOperation(cookie, t);
      if (t instanceof SQLiteException) {
        throw t;
      } else {
        throw new SQLiteException("unexpected", t);
      }
    } finally {
      operationLog.endOperation(cookie);
    }
  }

  /**
   * Work-around for jacoco ART issue https://code.google.com/p/android/issues/detail?id=80961
   * @return
   */
  private Cursor internalQuery(String table, String[] columns, String selection, Object[] selectionArgs,
      String groupBy, String having, String orderBy, String limit) {
    synchronized (mutex) {
      return db.query(table, columns, selection, selectionArgs, groupBy, having, orderBy, limit);
    }
  }

  public Cursor query(String table, String[] columns, String selection, Object[] selectionArgs,
      String groupBy, String having, String orderBy, String limit) {
    StringBuilder b = new StringBuilder();
    b.append("query(\"").append(table).append("\",");
    if (columns == null) {
      b.append("null,");
    } else {
      b.append("...,");
    }
    if (selection == null) {
      b.append("null,");
    } else {
      b.append("\"").append(selection).append("\",");
    }
    if (selectionArgs == null) {
      b.append("null,");
    } else {
      b.append("...,");
    }
    if (groupBy == null) {
      b.append("null,");
    } else {
      b.append("\"").append(groupBy).append("\",");
    }
    if (having == null) {
      b.append("null,");
    } else {
      b.append("\"").append(having).append("\",");
    }
    if (orderBy == null) {
      b.append("null,");
    } else {
      b.append("\"").append(orderBy).append("\",");
    }
    if (limit == null) {
      b.append("null)");
    } else {
      b.append("\"").append(limit).append("\")");
    }
    final int cookie = operationLog.beginOperation(sessionQualifier, b.toString(), null, null);

    try {
      // invoke method
      // Work-around for jacoco ART issue https://code.google.com/p/android/issues/detail?id=80961
      return internalQuery(table, columns, selection, selectionArgs, groupBy, having, orderBy,
          limit);
    } catch (Throwable t) {
      operationLog.failOperation(cookie, t);
      if (t instanceof SQLiteException) {
        throw t;
      } else {
        throw new SQLiteException("unexpected", t);
      }
    } finally {
      operationLog.endOperation(cookie);
    }
  }

  /**
   * Work-around for jacoco ART issue https://code.google.com/p/android/issues/detail?id=80961
   * @return
   */
  private Cursor internalQueryDistinct(String table, String[] columns, String selection,
      Object[] selectionArgs, String groupBy, String having, String orderBy, String limit) {
    synchronized (mutex) {
      return db.query(true, table, columns, selection, selectionArgs, groupBy, having, orderBy, limit,
              null);
    }
  }

  public Cursor queryDistinct(String table, String[] columns, String selection,
      Object[] selectionArgs, String groupBy, String having, String orderBy, String limit) {
    StringBuilder b = new StringBuilder();
    b.append("queryDistinct(\"").append(table).append("\",");
    if (columns == null) {
      b.append("null,");
    } else {
      b.append("...,");
    }
    if (selection == null) {
      b.append("null,");
    } else {
      b.append("\"").append(selection).append("\",");
    }
    if (selectionArgs == null) {
      b.append("null,");
    } else {
      b.append("...,");
    }
    if (groupBy == null) {
      b.append("null,");
    } else {
      b.append("\"").append(groupBy).append("\",");
    }
    if (having == null) {
      b.append("null,");
    } else {
      b.append("\"").append(having).append("\",");
    }
    if (orderBy == null) {
      b.append("null,");
    } else {
      b.append("\"").append(orderBy).append("\",");
    }
    if (limit == null) {
      b.append("null)");
    } else {
      b.append("\"").append(limit).append("\")");
    }
    final int cookie = operationLog.beginOperation(sessionQualifier, b.toString(), null, null);

    try {
      // invoke method
      // Work-around for jacoco ART issue https://code.google.com/p/android/issues/detail?id=80961
      return internalQueryDistinct(table, columns, selection, selectionArgs, groupBy, having,
          orderBy, limit);
    } catch (Throwable t) {
      operationLog.failOperation(cookie, t);
      if (t instanceof SQLiteException) {
        throw t;
      } else {
        throw new SQLiteException("unexpected", t);
      }
    } finally {
      operationLog.endOperation(cookie);
    }
  }

}
