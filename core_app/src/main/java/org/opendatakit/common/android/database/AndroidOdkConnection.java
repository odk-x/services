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
import android.database.Cursor;

import android.util.Printer;
import android.util.StringBuilderPrinter;
import org.opendatakit.common.android.utilities.WebLogger;
import org.sqlite.database.SQLException;
import org.sqlite.database.sqlite.SQLiteDatabase;
import org.sqlite.database.sqlite.SQLiteDatabaseConfiguration;
import org.sqlite.database.sqlite.SQLiteDatabaseLockedException;
import org.sqlite.database.sqlite.SQLiteException;

public class AndroidOdkConnection implements OdkConnectionInterface{
  final Object mutex;
  /**
   * Reference count is pre-incremented to account for:
   *
   * One reference immediately held on stack after creation.
   * One reference will be added when we put this into the OdkConnectionFactoryInterface session map
   */
  int referenceCount = 1;
  final String appName;
  final SQLiteDatabase db;
  final String sessionQualifier;
  String lastAction = "<new>";
  long threadId = -1L;
  Object initializationMutex = new Object();
  boolean initializationComplete = false;
  boolean initializationStatus = false;

  
  public static AndroidOdkConnection openDatabase(Object mutex, String appName, String dbFilePath, String sessionQualifier) {
    SQLiteDatabaseConfiguration configuration = new SQLiteDatabaseConfiguration(appName, dbFilePath,
            SQLiteDatabase.ENABLE_WRITE_AHEAD_LOGGING |
                    SQLiteDatabase.OPEN_READWRITE | SQLiteDatabase.CREATE_IF_NECESSARY |
                    SQLiteDatabase.NO_LOCALIZED_COLLATORS );

    // this might throw an exception
    SQLiteDatabase db = SQLiteDatabase.openDatabase(configuration, null, null, sessionQualifier);

    // this isn't going to throw an exception
    return new AndroidOdkConnection(mutex, appName, db, sessionQualifier);
  }

  private AndroidOdkConnection(Object mutex, String appName, SQLiteDatabase db, String sessionQualifier) {
    this.mutex = mutex;
    this.appName = appName;
    this.db = db;
    this.sessionQualifier = sessionQualifier;
    this.threadId = Thread.currentThread().getId();
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

  public long getLastThreadId() {
    return threadId;
  }

  public String getSessionQualifier() {
    return sessionQualifier;
  }

  public void dumpDetail(Printer printer) {
    db.dump(printer, true);
  }

  private String getLogTag() {
    return "AndroidOdkConnection:" + appName + ":" + sessionQualifier;
  }

  private final void log() {
    WebLogger.getLogger(appName).i(getLogTag(), "ref(" + getReferenceCount() + ") sq: " + sessionQualifier + " action: " + lastAction);
  }
  
  public String getLastAction() {
    return lastAction;
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
    } catch ( Throwable t) {
      WebLogger.getLogger(appName).e(getLogTag(), "ReleaseReference tried to throw an exception!");
      WebLogger.getLogger(appName).printStackTrace(t);
    }
  }

  public int getReferenceCount() {
    synchronized (mutex) {
      return referenceCount;
    }
  }

  public boolean isOpen() {
    synchronized (mutex) {
      this.threadId = Thread.currentThread().getId();
      return db.isOpen();
    }
  }

  @Override
  protected void finalize() throws Throwable {

    try {
      int refCount = getReferenceCount();
      if (refCount != 0) {
        WebLogger.getLogger(appName).w(getLogTag(), "finalize: expected no references -- has " + refCount);
      }
      commonWrapUpConnection("finalize");
    } finally {
      super.finalize();
    }
  }

  public void close() {
    throw new IllegalStateException("this method should not be called");
  }

  private void commonWrapUpConnection(String action) {
    if ( isOpen() ) {
      try {
        boolean first = true;
        threadId = Thread.currentThread().getId();
        while (inTransaction()) {
          if (!first) {
            lastAction = action + "...end transaction[unknown] (finalAction: " + lastAction + ")";
          } else {
            lastAction = action + "...end transaction[rollback] (finalAction: " + lastAction + ")";
          }
          first = false;
          log();
          endTransaction();
        }
      } catch ( Throwable t ) {
        WebLogger.getLogger(appName).e("commonWrapUpConnection", "Yikes! threw an exception within connection cleanup routine");
        WebLogger.getLogger(appName).printStackTrace(t);
        throw t;
      } finally {
        threadId = Thread.currentThread().getId();
        lastAction = action + "...close(finalAction: " + lastAction + ")";
        log();
        synchronized (mutex) {
          db.close();
        }
      }
    }
  }

  public int getVersion() {
    threadId = Thread.currentThread().getId();
    lastAction = "getVersion(finalAction: " + lastAction + ")";
    log();
    synchronized (mutex) {
      return db.getVersion();
    }
  }

  public void setVersion(int version) {
    threadId = Thread.currentThread().getId();
    lastAction = "setVersion=" + version + "(finalAction: " + lastAction + ")";
    log();
    synchronized (mutex) {
      db.setVersion(version);
    }
  }

  public void beginTransactionNonExclusive() {
    threadId = Thread.currentThread().getId();
    lastAction = "beginTransactionNonExclusive(priorAction: " + lastAction + ")";
    log();
    try {
      synchronized (mutex) {
        db.beginTransactionNonExclusive();
      }
    } catch (Throwable t) {
      WebLogger.getLogger(appName).printStackTrace(t);
      WebLogger.getLogger(appName).e("AndroidOdkConnection", "Attempting dump of all database connections");
      StringBuilder b = new StringBuilder();
      StringBuilderPrinter p = new StringBuilderPrinter(b);
      SQLiteDatabase.dumpAll(p, true);
      p.println("\n");
      WebLogger.getLogger(appName).e("AndroidOdkConnection", b.toString());
      throw t;
    }
  }

  public boolean inTransaction() {
    threadId = Thread.currentThread().getId();
    lastAction = "inTransaction(finalAction: " + lastAction + ")";
    log();
    synchronized (mutex) {
      return db.inTransaction();
    }
  }

  public void setTransactionSuccessful() {
    threadId = Thread.currentThread().getId();
    lastAction = "setTransactionSuccessful(finalAction: " + lastAction + ")";
    log();
    synchronized (mutex) {
      db.setTransactionSuccessful();
    }
  }

  public void endTransaction() {
    threadId = Thread.currentThread().getId();
    lastAction = "endTransaction(finalAction: " + lastAction + ")";
    log();
    synchronized (mutex) {
      db.endTransaction();
    }
  }

  public int update(String table, ContentValues values, String whereClause, String[] whereArgs) {
    threadId = Thread.currentThread().getId();
    lastAction = "update=" + table + " WHERE " + whereClause;
    log();
    synchronized (mutex) {
      return db.update(table, values, whereClause, whereArgs);
    }
  }

  public int delete(String table, String whereClause, String[] whereArgs) {
    threadId = Thread.currentThread().getId();
    lastAction = "delete=" + table + " WHERE " + whereClause;
    log();
    synchronized (mutex) {
      return db.delete(table, whereClause, whereArgs);
    }
  }

  public long replaceOrThrow(String table, String nullColumnHack, ContentValues initialValues)
      throws SQLException {
    threadId = Thread.currentThread().getId();
    lastAction = "replaceOrThrow=" + table;
    log();
    synchronized (mutex) {
      return db.replaceOrThrow(table, nullColumnHack, initialValues);
    }
  }

  public long insertOrThrow(String table, String nullColumnHack, ContentValues values)
      throws SQLException {
    threadId = Thread.currentThread().getId();
    lastAction = "insertOrThrow=" + table;
    log();
    synchronized (mutex) {
      return db.insertOrThrow(table, nullColumnHack, values);
    }
  }

  public void execSQL(String sql, Object[] bindArgs) throws SQLException {
    threadId = Thread.currentThread().getId();
    lastAction = "execSQL=" + sql;
    log();
    synchronized (mutex) {
      db.execSQL(sql, bindArgs);
    }
  }

  public void execSQL(String sql) throws SQLException {
    threadId = Thread.currentThread().getId();
    lastAction = "execSQL=" + sql;
    log();
    synchronized (mutex) {
      db.execSQL(sql);
    }
  }

  public Cursor rawQuery(String sql, String[] selectionArgs) {
    threadId = Thread.currentThread().getId();
    lastAction = "rawQuery=" + sql;
    log();
    synchronized (mutex) {
      return db.rawQuery(sql, selectionArgs);
    }
  }

  public Cursor query(String table, String[] columns, String selection, String[] selectionArgs,
      String groupBy, String having, String orderBy, String limit) {
    threadId = Thread.currentThread().getId();
    lastAction = "query=" + table + " WHERE " + selection;
    log();
    synchronized (mutex) {
      return db.query(table, columns, selection, selectionArgs, groupBy, having, orderBy, limit);
    }
  }

  public Cursor queryDistinct(String table, String[] columns, String selection,
      String[] selectionArgs, String groupBy, String having, String orderBy, String limit) {
    threadId = Thread.currentThread().getId();
    lastAction = "queryDistinct=" + table + " WHERE " + selection;
    log();
    synchronized (mutex) {
      return db.query(true, table, columns, selection, selectionArgs, groupBy, having, orderBy,
              limit);
    }
  }

}
