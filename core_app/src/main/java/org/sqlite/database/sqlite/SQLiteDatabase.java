/*
 * Copyright (C) 2006 The Android Open Source Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
** Modified to support SQLite extensions by the SQLite developers: 
** sqlite-dev@sqlite.org.
*/

package org.sqlite.database.sqlite;

import android.content.ContentValues;
import android.database.Cursor;
import android.database.CursorWindow;
import android.database.DatabaseUtils;
import android.os.ParcelFileDescriptor;
import android.os.CancellationSignal;
import android.os.OperationCanceledException;
import android.text.TextUtils;

import org.opendatakit.common.android.database.OperationLog;
import org.opendatakit.common.android.utilities.WebLogger;

import org.sqlite.database.DatabaseErrorHandler;
import org.sqlite.database.DefaultDatabaseErrorHandler;
import org.sqlite.database.SQLException;

import java.util.regex.Pattern;

/**
 * Exposes methods to manage a SQLite database.
 *
 * <p>
 * SQLiteDatabase has methods to create, delete, execute SQL commands, and
 * perform other common database management tasks.
 * </p><p>
 * See the Notepad sample application in the SDK for an example of creating
 * and managing a database.
 * </p><p>
 * Database names must be unique within an application, not across all applications.
 * </p>
 *
 * <h3>Localized Collation - ORDER BY</h3>
 * <p>
 * In addition to SQLite's default <code>BINARY</code> collator, Android supplies
 * two more, <code>LOCALIZED</code>, which changes with the system's current locale,
 * and <code>UNICODE</code>, which is the Unicode Collation Algorithm and not tailored
 * to the current locale.
 * </p>
 */
public final class SQLiteDatabase extends SQLiteClosable {
    private static final String TAG = "SQLiteDatabase";

    private static final int EVENT_DB_CORRUPT = 75004;

   /**
    * Transaction mode: Deferred.
    * <p>
    * In a deferred transaction, no locks are acquired on the database
    * until the first operation is performed.  If the first operation is
    * read-only, then a <code>SHARED</code> lock is acquired, otherwise
    * a <code>RESERVED</code> lock is acquired.
    * </p><p>
    * While holding a <code>SHARED</code> lock, this session is only allowed to
    * read but other sessions are allowed to read or write.
    * While holding a <code>RESERVED</code> lock, this session is allowed to read
    * or write but other sessions are only allowed to read.
    * </p><p>
    * Because the lock is only acquired when needed in a deferred transaction,
    * it is possible for another session to write to the database first before
    * this session has a chance to do anything.
    * </p><p>
    * Corresponds to the SQLite <code>BEGIN DEFERRED</code> transaction mode.
    * </p>
    */
   public static final int TRANSACTION_MODE_DEFERRED = 0;

   /**
    * Transaction mode: Immediate.
    * <p>
    * When an immediate transaction begins, the session acquires a
    * <code>RESERVED</code> lock.
    * </p><p>
    * While holding a <code>RESERVED</code> lock, this session is allowed to read
    * or write but other sessions are only allowed to read.
    * </p><p>
    * Corresponds to the SQLite <code>BEGIN IMMEDIATE</code> transaction mode.
    * </p>
    */
   public static final int TRANSACTION_MODE_IMMEDIATE = 1;

   /**
    * Transaction mode: Exclusive.
    * <p>
    * When an exclusive transaction begins, the session acquires an
    * <code>EXCLUSIVE</code> lock.
    * </p><p>
    * While holding an <code>EXCLUSIVE</code> lock, this session is allowed to read
    * or write but no other sessions are allowed to access the database.
    * </p><p>
    * Corresponds to the SQLite <code>BEGIN EXCLUSIVE</code> transaction mode.
    * </p>
    */
   public static final int TRANSACTION_MODE_EXCLUSIVE = 2;

   /**
    * When a constraint violation occurs, an immediate ROLLBACK occurs,
    * thus ending the current transaction, and the command aborts with a
    * return code of SQLITE_CONSTRAINT. If no transaction is active
    * (other than the implied transaction that is created on every command)
    * then this algorithm works the same as ABORT.
    */
   public static final int CONFLICT_ROLLBACK = 1;

   /**
    * When a constraint violation occurs,no ROLLBACK is executed
    * so changes from prior commands within the same transaction
    * are preserved. This is the default behavior.
    */
   public static final int CONFLICT_ABORT = 2;

   /**
    * When a constraint violation occurs, the command aborts with a return
    * code SQLITE_CONSTRAINT. But any changes to the database that
    * the command made prior to encountering the constraint violation
    * are preserved and are not backed out.
    */
   public static final int CONFLICT_FAIL = 3;

   /**
    * When a constraint violation occurs, the one row that contains
    * the constraint violation is not inserted or changed.
    * But the command continues executing normally. Other rows before and
    * after the row that contained the constraint violation continue to be
    * inserted or updated normally. No error is returned.
    */
   public static final int CONFLICT_IGNORE = 4;

   /**
    * When a UNIQUE constraint violation occurs, the pre-existing rows that
    * are causing the constraint violation are removed prior to inserting
    * or updating the current row. Thus the insert or update always occurs.
    * The command continues executing normally. No error is returned.
    * If a NOT NULL constraint violation occurs, the NULL value is replaced
    * by the default value for that column. If the column has no default
    * value, then the ABORT algorithm is used. If a CHECK constraint
    * violation occurs then the IGNORE algorithm is used. When this conflict
    * resolution strategy deletes rows in order to satisfy a constraint,
    * it does not invoke delete triggers on those rows.
    * This behavior might change in a future release.
    */
   public static final int CONFLICT_REPLACE = 5;

   /**
    * Use the following when no conflict action is specified.
    */
   public static final int CONFLICT_NONE = 0;

   private static final String[] CONFLICT_VALUES = new String[]
       {"", " OR ROLLBACK ", " OR ABORT ", " OR FAIL ", " OR IGNORE ", " OR REPLACE "};

   /**
    * Maximum Length Of A LIKE Or GLOB Pattern
    * The pattern matching algorithm used in the default LIKE and GLOB implementation
    * of SQLite can exhibit O(N^2) performance (where N is the number of characters in
    * the pattern) for certain pathological cases. To avoid denial-of-service attacks
    * the length of the LIKE or GLOB pattern is limited to SQLITE_MAX_LIKE_PATTERN_LENGTH bytes.
    * The default value of this limit is 50000. A modern workstation can evaluate
    * even a pathological LIKE or GLOB pattern of 50000 bytes relatively quickly.
    * The denial of service problem only comes into play when the pattern length gets
    * into millions of bytes. Nevertheless, since most useful LIKE or GLOB patterns
    * are at most a few dozen bytes in length, paranoid application developers may
    * want to reduce this parameter to something in the range of a few hundred
    * if they know that external users are able to generate arbitrary patterns.
    */
   public static final int SQLITE_MAX_LIKE_PATTERN_LENGTH = 50000;

   /**
    * Open flag: Flag for {@link SQLiteDatabaseConfiguration} to open the database for reading and writing.
    * If the disk is full, this may fail even before you actually write anything.
    *
    * {@more} Note that the value of this flag is 0, so it is the default.
    */
   public static final int OPEN_READWRITE = 0x00000000;          // update native code if changing

   /**
    * Open flag: Flag for {@link #openDatabase} to open the database for reading only.
    * This is the only reliable way to open a database if the disk may be full.
    */
   // public static final int OPEN_READONLY = 0x00000001;           // update native code if changing

   // private static final int OPEN_READ_MASK = 0x00000001;         // update native code if changing

   /**
    * Open flag: Flag for {@link SQLiteDatabaseConfiguration} to open the database without support for
    * localized collators.
    *
    * {@more} This causes the collator <code>LOCALIZED</code> not to be created.
    * You must be consistent when using this flag to use the setting the database was
    * created with.  If this is set, setLocale will do nothing.
    */
   public static final int NO_LOCALIZED_COLLATORS = 0x00000010;  // update native code if changing

   /**
    * Open flag: Flag for {@link SQLiteDatabaseConfiguration} to create the database file if it does not
    * already exist.
    */
   public static final int CREATE_IF_NECESSARY = 0x10000000;     // update native code if changing

   /**
    * Open flag: Flag for {@link SQLiteDatabaseConfiguration} to open the database file with
    * write-ahead logging enabled by default.
    */
   public static final int ENABLE_WRITE_AHEAD_LOGGING = 0x20000000;

   /*****************************************************************************************
    * Member variables
    * ***************************************************************************************/

    // Error handler to be used when SQLite returns corruption errors.
    // This can be accessed outside of locks
    private final DatabaseErrorHandler mErrorHandler;

    // The sessionQualifier used to identify this session
    // This can be accessed outside of locks
    private final String mSessionQualifier;

    // The logger to use to log errors
    // This can be accessed outside of locks
    private final WebLogger mWebLogger;

   // The database configuration.
   // This is an immutable copy of the constructor argument.
   // This can be accessed outside of locks
   private final SQLiteDatabaseConfiguration mConfiguration;

   // The activity logger across all accesses to the appName database
   // This can be accessed outside of locks
   private final OperationLog mOperationLog;

   // Manages the nesting of transactions on this connection
   // This can be accessed outside of locks
   private final SQLiteTransactionManager mTransactionManager;

   // Shared database state lock.
    // This lock guards all of the shared state of the database, such as its
    // configuration, whether it is open or closed, and so on.  This lock should
    // be held for as little time as possible.
    //
    // The lock MUST NOT be held while attempting to acquire database connections or
    // while executing SQL statements on behalf of the client as it can lead to deadlock.
    //
    // It is ok to hold the lock while reconfiguring the connection pool or dumping
    // statistics because those operations are non-reentrant and do not try to acquire
    // connections that might be held by other threads.
    //
    // Basic rule: grab the lock, access or modify global state, release the lock, then
    // do the required SQL work.
    private final Object mLock = new Object();

    // Warns if the database is finalized without being closed properly.
    // INVARIANT: Guarded by mLock.
    private final CloseGuard mCloseGuardLocked;

   // actual JNI/C++ connection to the database
   // INVARIANT: Guarded by mLock.
   private SQLiteConnection mConnection = null;

    public SQLiteDatabase(SQLiteDatabaseConfiguration configuration,
        OperationLog operationLog,
            DatabaseErrorHandler errorHandler, String sessionQualifier) {
       if (configuration == null) {
          throw new IllegalArgumentException("configuration must not be null.");
       }
       mWebLogger = WebLogger.getLogger(configuration.appName);
       mCloseGuardLocked = CloseGuard.get(WebLogger.getLogger(configuration.appName));
       mErrorHandler = errorHandler != null ? errorHandler : new DefaultDatabaseErrorHandler();
       mConfiguration = new SQLiteDatabaseConfiguration(configuration);
       mSessionQualifier = sessionQualifier;
       mOperationLog = operationLog;
       mTransactionManager = new SQLiteTransactionManager();
    }

    @Override
    protected void finalize() throws Throwable {
        boolean refCountBelowZero = getReferenceCount() < 0;
        try {
            dispose(true, refCountBelowZero);
        } finally {
            super.finalize();
        }
    }

    @Override
    protected void onAllReferencesReleased(boolean refCountBelowZero) {
        dispose(false, refCountBelowZero);
    }

    private void dispose(boolean finalized, boolean refCountBelowZero) {
       SQLiteConnection connection = null;

      synchronized (mLock) {
          if (mCloseGuardLocked != null) {
              if (finalized) {
                mCloseGuardLocked.warnIfOpen();
              }
              mCloseGuardLocked.close();
          }
         connection = mConnection;
         mConnection = null;
      }

       String context = (finalized) ? "finalize: " : "onAllReferencesReleased: ";


       if ( refCountBelowZero ) {
          mWebLogger.e("SQLiteDatabase", context + getAppName() + " " + mSessionQualifier
              + " referenceCount dropped below zero!");
       }

       if ( connection != null ) {

          // log an error if we are closing the connection in the finalizer
          if ( finalized ) {
             mWebLogger.e("SQLiteDatabase", "finalize: " + getAppName() + " " + mSessionQualifier
                 + " is not closed!");
          }

          try {
             connection.close(); // might throw
             mWebLogger.i("SQLiteDatabase", context + getAppName() + " " + mSessionQualifier
                 + " successful connection.close()");
          } catch (Throwable t) {
             mWebLogger.e("SQLiteDatabase", context + getAppName() + " " + mSessionQualifier
                 + " exception during connection.close()");
             mWebLogger.printStackTrace(t);
             throw t;
          }
       }
    }

  /**
   *
   * @return the logger for logging errors, etc.
   */
  public WebLogger getLogger() {
    return mWebLogger;
  }

   /**
    * @return The path to the database file.
    */
   public final String getPath() {
      return mConfiguration.path;
   }

   /**
    *
    * @return the appName for this database.
    */
   public final String getAppName() {
      return mConfiguration.appName;
   }
   /**
    * Returns true if write-ahead logging has been enabled for this database.
    *
    * @return True if write-ahead logging has been enabled for this database.
    *
    * @see #ENABLE_WRITE_AHEAD_LOGGING
    */
   public boolean isWriteAheadLoggingEnabled() {
      return (mConfiguration.openFlags & ENABLE_WRITE_AHEAD_LOGGING) != 0;
   }

   @Override
   public String toString() {
      return "SQLiteDatabase: " + getPath();
   }

   /**
    * A callback interface for a custom sqlite3 function.
    * This can be used to create a function that can be called from
    * sqlite3 database triggers.
    * @hide
    */
   public interface CustomFunction {
      public void callback(String[] args);
   }

    /**
     * Sends a corruption message to the database error handler.
     */
    void onCorruption() {
        mErrorHandler.onCorruption(this);
    }

   /**
    * Executes a statement that does not return a result.
    *
    * @param sql The SQL statement to execute.
    * @param bindArgs The arguments to bind, or null if none.
    * @param cancellationSignal A signal to cancel the operation in progress, or null if none.
    *
    * @throws SQLiteException if an error occurs, such as a syntax error
    * or invalid number of bind arguments.
    * @throws OperationCanceledException if the operation was canceled.
    */
   public void execute(String sql, Object[] bindArgs,
       CancellationSignal cancellationSignal) {
      if (sql == null) {
         throw new IllegalArgumentException("sql must not be null.");
      }

      if (executeSpecial(sql, bindArgs, cancellationSignal)) {
         return;
      }

      mConnection.executeImpl(sql, bindArgs, cancellationSignal); // might throw
   }

   /**
    * Executes a statement that returns a single <code>long</code> result.
    *
    * @param sql The SQL statement to execute.
    * @param bindArgs The arguments to bind, or null if none.
    * @param cancellationSignal A signal to cancel the operation in progress, or null if none.
    * @return The value of the first column in the first row of the result set
    * as a <code>long</code>, or zero if none.
    *
    * @throws SQLiteException if an error occurs, such as a syntax error
    * or invalid number of bind arguments.
    * @throws OperationCanceledException if the operation was canceled.
    */
   public long executeForLong(String sql, Object[] bindArgs,
       CancellationSignal cancellationSignal) {
      if (sql == null) {
         throw new IllegalArgumentException("sql must not be null.");
      }

      if (executeSpecial(sql, bindArgs, cancellationSignal)) {
         return 0L;
      }

      return mConnection.executeForLongImpl(sql, bindArgs, cancellationSignal); // might throw
   }

   /**
    * Executes a statement that returns a single {@link String} result.
    *
    * @param sql The SQL statement to execute.
    * @param bindArgs The arguments to bind, or null if none.
    * @param cancellationSignal A signal to cancel the operation in progress, or null if none.
    * @return The value of the first column in the first row of the result set
    * as a <code>String</code>, or null if none.
    *
    * @throws SQLiteException if an error occurs, such as a syntax error
    * or invalid number of bind arguments.
    * @throws OperationCanceledException if the operation was canceled.
    */
   public String executeForString(String sql, Object[] bindArgs,
       CancellationSignal cancellationSignal) {
      if (sql == null) {
         throw new IllegalArgumentException("sql must not be null.");
      }

      if (executeSpecial(sql, bindArgs, cancellationSignal)) {
         return null;
      }

      return mConnection.executeForStringImpl(sql, bindArgs, cancellationSignal); // might throw
   }

   /**
    * Executes a statement that returns a single BLOB result as a
    * file descriptor to a shared memory region.
    *
    * @param sql The SQL statement to execute.
    * @param bindArgs The arguments to bind, or null if none.
    * @param cancellationSignal A signal to cancel the operation in progress, or null if none.
    * @return The file descriptor for a shared memory region that contains
    * the value of the first column in the first row of the result set as a BLOB,
    * or null if none.
    *
    * @throws SQLiteException if an error occurs, such as a syntax error
    * or invalid number of bind arguments.
    * @throws OperationCanceledException if the operation was canceled.
    */
   public ParcelFileDescriptor executeForBlobFileDescriptor(String sql, Object[] bindArgs,
       CancellationSignal cancellationSignal) {
      if (sql == null) {
         throw new IllegalArgumentException("sql must not be null.");
      }

      if (executeSpecial(sql, bindArgs, cancellationSignal)) {
         return null;
      }

      return mConnection.executeForBlobFileDescriptor(sql, bindArgs,
          cancellationSignal); // might throw
   }

   /**
    * Executes a statement that returns a count of the number of rows
    * that were changed.  Use for UPDATE or DELETE SQL statements.
    *
    * @param sql The SQL statement to execute.
    * @param bindArgs The arguments to bind, or null if none.
    * @param cancellationSignal A signal to cancel the operation in progress, or null if none.
    * @return The number of rows that were changed.
    *
    * @throws SQLiteException if an error occurs, such as a syntax error
    * or invalid number of bind arguments.
    * @throws OperationCanceledException if the operation was canceled.
    */
   public int executeForChangedRowCount(String sql, Object[] bindArgs,
       CancellationSignal cancellationSignal) {
      if (sql == null) {
         throw new IllegalArgumentException("sql must not be null.");
      }

      if (executeSpecial(sql, bindArgs, cancellationSignal)) {
         return 0;
      }

      return mConnection.executeForChangedRowCount(sql, bindArgs,
          cancellationSignal); // might throw
   }

   /**
    * Executes a statement that returns the row id of the last row inserted
    * by the statement.  Use for INSERT SQL statements.
    *
    * @param sql The SQL statement to execute.
    * @param bindArgs The arguments to bind, or null if none.
    * @param cancellationSignal A signal to cancel the operation in progress, or null if none.
    * @return The row id of the last row that was inserted, or 0 if none.
    *
    * @throws SQLiteException if an error occurs, such as a syntax error
    * or invalid number of bind arguments.
    * @throws OperationCanceledException if the operation was canceled.
    */
   public long executeForLastInsertedRowId(String sql, Object[] bindArgs,
       CancellationSignal cancellationSignal) {
      if (sql == null) {
         throw new IllegalArgumentException("sql must not be null.");
      }

      if (executeSpecial(sql, bindArgs, cancellationSignal)) {
         return 0;
      }

      return mConnection.executeForLastInsertedRowId(sql, bindArgs,
          cancellationSignal); // might throw
   }

   /**
    * Executes a statement and populates the specified {@link CursorWindow}
    * with a range of results.  Returns the number of rows that were counted
    * during query execution.
    *
    * @param sql The SQL statement to execute.
    * @param bindArgs The arguments to bind, or null if none.
    * @param window The cursor window to clear and fill.
    * @param startPos The start position for filling the window.
    * @param requiredPos The position of a row that MUST be in the window.
    * If it won't fit, then the query should discard part of what it filled
    * so that it does.  Must be greater than or equal to <code>startPos</code>.
    * @param countAllRows True to count all rows that the query would return
    * regagless of whether they fit in the window.
    * @param cancellationSignal A signal to cancel the operation in progress, or null if none.
    * @return The number of rows that were counted during query execution.  Might
    * not be all rows in the result set unless <code>countAllRows</code> is true.
    *
    * @throws SQLiteException if an error occurs, such as a syntax error
    * or invalid number of bind arguments.
    * @throws OperationCanceledException if the operation was canceled.
    */
   public int executeForCursorWindow(String sql, Object[] bindArgs,
       CursorWindow window, int startPos, int requiredPos, boolean countAllRows,
       CancellationSignal cancellationSignal) {
      if (sql == null) {
         throw new IllegalArgumentException("sql must not be null.");
      }
      if (window == null) {
         throw new IllegalArgumentException("window must not be null.");
      }

      if (executeSpecial(sql, bindArgs, cancellationSignal)) {
         window.clear();
         return 0;
      }

      return mConnection.executeForCursorWindow(sql, bindArgs,
          window, startPos, requiredPos, countAllRows,
          cancellationSignal); // might throw
   }

   /**
    * Performs special reinterpretation of certain SQL statements such as "BEGIN",
    * "COMMIT" and "ROLLBACK" to ensure that transaction state invariants are
    * maintained.
    *
    * This function is mainly used to support legacy apps that perform their
    * own transactions by executing raw SQL rather than calling {@link #beginTransactionNonExclusive}
    * and the like.
    *
    * @param sql The SQL statement to execute.
    * @param bindArgs The arguments to bind, or null if none.
    * @param cancellationSignal A signal to cancel the operation in progress, or null if none.
    * @return True if the statement was of a special form that was handled here,
    * false otherwise.
    *
    * @throws SQLiteException if an error occurs, such as a syntax error
    * or invalid number of bind arguments.
    * @throws OperationCanceledException if the operation was canceled.
    */
   private boolean executeSpecial(String sql, Object[] bindArgs,
       CancellationSignal cancellationSignal) {
      if (sql == null) {
         throw new IllegalArgumentException("sql must not be null.");
      }

      if (cancellationSignal != null) {
         cancellationSignal.throwIfCanceled();
      }

      final int type = DatabaseUtils.getSqlStatementType(sql);
      switch (type) {
      case DatabaseUtils.STATEMENT_BEGIN:
         beginTransactionNonExclusive(cancellationSignal);
         return true;

      case DatabaseUtils.STATEMENT_COMMIT:
         setTransactionSuccessful();
         endTransaction(cancellationSignal);
         return true;

      case DatabaseUtils.STATEMENT_ABORT:
         endTransaction(cancellationSignal);
         return true;
      }
      return false;
   }

    /**
     * Begins a transaction in IMMEDIATE mode. Transactions can be nested. When
     * the outer transaction is ended all of the work done in that transaction
     * and all of the nested transactions will be committed or rolled back. The
     * changes will be rolled back if any transaction is ended without being
     * marked as clean (by calling setTransactionSuccessful). Otherwise they
     * will be committed.
     * <p>
     * Here is the standard idiom for transactions:
     *
     * <pre>
     *   db.beginTransactionNonExclusive();
     *   try {
     *     ...
     *     db.setTransactionSuccessful();
     *   } finally {
     *     db.endTransaction();
     *   }
     * </pre>
     */
    public void beginTransactionNonExclusive() {
       beginTransactionNonExclusive(null);
    }

    public void beginTransactionNonExclusive(CancellationSignal cancellationSignal) {
       if (cancellationSignal != null) {
          cancellationSignal.throwIfCanceled();
       }
       // TODO: change this to Immediate?
       int transactionMode = TRANSACTION_MODE_DEFERRED;

       if (mTransactionManager.beginTransaction(transactionMode) ) {
          boolean success = false;
          try {
             // Execute SQL might throw a runtime exception.
             switch (transactionMode) {
             case SQLiteDatabase.TRANSACTION_MODE_IMMEDIATE:
                mConnection.executeImpl("BEGIN IMMEDIATE;", null, cancellationSignal); // might throw
                break;
             case SQLiteDatabase.TRANSACTION_MODE_EXCLUSIVE:
                mConnection.executeImpl("BEGIN EXCLUSIVE;", null, cancellationSignal); // might throw
                break;
             default:
                mConnection.executeImpl("BEGIN;", null, cancellationSignal); // might throw
                break;
             }
             success = true;
          } finally {
             if ( !success ) {
                mTransactionManager.cancelTransaction();
             }
          }
       }
    }

    /**
     * Returns true if the current thread has a transaction pending.
     *
     * @return True if the current thread is in a transaction.
     */
    public boolean inTransaction() {
      return mTransactionManager.hasTransaction();
    }

    public void open() {
      try {
          openInner();
      } catch (SQLiteDatabaseCorruptException ex) {
          onCorruption();
          openInner();
      }
    }

  private void openInner() {
    synchronized (mLock) {

      if (mConnection != null) {
        throw new IllegalStateException("The database '"
            + getAppName() + "' is already open");
      }

      // might throw
       SQLiteConnection connection = new SQLiteConnection(mConfiguration,
                                          mOperationLog, mSessionQualifier);
       try {
          connection.open();
          mConnection = connection;
       } catch (SQLiteException ex) {
          connection.dispose(false);
          throw ex;
       } finally {
          mCloseGuardLocked.open("close");
          if (mConnection == null) {
             throw new SQLiteCantOpenDatabaseException("The database '"
                 + getAppName() + "' is not open");
          }
       }
    }
  }

    /**
     * Gets the database version.
     *
     * @return the database version
     */
    public int getVersion() {
       long version = executeForLong("PRAGMA user_version;", null, null);
       return ((Long) version).intValue();
    }

    /**
     * Sets the database version.
     *
     * @param version the new database version
     */
    public void setVersion(int version) {
       executeForChangedRowCount("PRAGMA user_version = " + version, null, null);
    }

    /**
     * Query the given URL, returning a {@link Cursor} over the result set.
     *
     * @param distinct true if you want each row to be unique, false otherwise.
     * @param table The table name to compile the query against.
     * @param columns A list of which columns to return. Passing null will
     *            return all columns, which is discouraged to prevent reading
     *            data from storage that isn't going to be used.
     * @param selection A filter declaring which rows to return, formatted as an
     *            SQL WHERE clause (excluding the WHERE itself). Passing null
     *            will return all rows for the given table.
     * @param selectionArgs You may include ?s in selection, which will be
     *         replaced by the values from selectionArgs, in order that they
     *         appear in the selection. The values will be bound as Strings.
     * @param groupBy A filter declaring how to group rows, formatted as an SQL
     *            GROUP BY clause (excluding the GROUP BY itself). Passing null
     *            will cause the rows to not be grouped.
     * @param having A filter declare which row groups to include in the cursor,
     *            if row grouping is being used, formatted as an SQL HAVING
     *            clause (excluding the HAVING itself). Passing null will cause
     *            all row groups to be included, and is required when row
     *            grouping is not being used.
     * @param orderBy How to order the rows, formatted as an SQL ORDER BY clause
     *            (excluding the ORDER BY itself). Passing null will use the
     *            default sort order, which may be unordered.
     * @param limit Limits the number of rows returned by the query,
     *            formatted as LIMIT clause. Passing null denotes no LIMIT clause.
     * @return A {@link Cursor} object, which is positioned before the first entry. Note that
     * {@link Cursor}s are not synchronized, see the documentation for more details.
     * @see Cursor
     */
    public Cursor query(boolean distinct, String table, String[] columns,
            String selection, String[] selectionArgs, String groupBy,
            String having, String orderBy, String limit) {
        return query(distinct, table, columns, selection, selectionArgs, groupBy, having, orderBy,
            limit, null);
    }

    /**
     * Query the given URL, returning a {@link Cursor} over the result set.
     *
     * @param distinct true if you want each row to be unique, false otherwise.
     * @param table The table name to compile the query against.
     * @param columns A list of which columns to return. Passing null will
     *            return all columns, which is discouraged to prevent reading
     *            data from storage that isn't going to be used.
     * @param selection A filter declaring which rows to return, formatted as an
     *            SQL WHERE clause (excluding the WHERE itself). Passing null
     *            will return all rows for the given table.
     * @param selectionArgs You may include ?s in selection, which will be
     *         replaced by the values from selectionArgs, in order that they
     *         appear in the selection. The values will be bound as Strings.
     * @param groupBy A filter declaring how to group rows, formatted as an SQL
     *            GROUP BY clause (excluding the GROUP BY itself). Passing null
     *            will cause the rows to not be grouped.
     * @param having A filter declare which row groups to include in the cursor,
     *            if row grouping is being used, formatted as an SQL HAVING
     *            clause (excluding the HAVING itself). Passing null will cause
     *            all row groups to be included, and is required when row
     *            grouping is not being used.
     * @param orderBy How to order the rows, formatted as an SQL ORDER BY clause
     *            (excluding the ORDER BY itself). Passing null will use the
     *            default sort order, which may be unordered.
     * @param limit Limits the number of rows returned by the query,
     *            formatted as LIMIT clause. Passing null denotes no LIMIT clause.
     * @param cancellationSignal A signal to cancel the operation in progress, or null if none.
     * If the operation is canceled, then {@link OperationCanceledException} will be thrown
     * when the query is executed.
     * @return A {@link Cursor} object, which is positioned before the first entry. Note that
     * {@link Cursor}s are not synchronized, see the documentation for more details.
     * @see Cursor
     */
    public Cursor query(boolean distinct, String table, String[] columns,
            String selection, String[] selectionArgs, String groupBy,
            String having, String orderBy, String limit, CancellationSignal cancellationSignal) {

      String sql = buildQueryString(
              distinct, table, columns, selection, groupBy, having, orderBy, limit);

      return rawQuery(sql, selectionArgs, cancellationSignal);
    }

   private static final Pattern sLimitPattern =
       Pattern.compile("\\s*\\d+\\s*(,\\s*\\d+\\s*)?");

   /**
    * Build an SQL query string from the given clauses.
    *
    * @param distinct true if you want each row to be unique, false otherwise.
    * @param tables The table names to compile the query against.
    * @param columns A list of which columns to return. Passing null will
    *            return all columns, which is discouraged to prevent reading
    *            data from storage that isn't going to be used.
    * @param where A filter declaring which rows to return, formatted as an SQL
    *            WHERE clause (excluding the WHERE itself). Passing null will
    *            return all rows for the given URL.
    * @param groupBy A filter declaring how to group rows, formatted as an SQL
    *            GROUP BY clause (excluding the GROUP BY itself). Passing null
    *            will cause the rows to not be grouped.
    * @param having A filter declare which row groups to include in the cursor,
    *            if row grouping is being used, formatted as an SQL HAVING
    *            clause (excluding the HAVING itself). Passing null will cause
    *            all row groups to be included, and is required when row
    *            grouping is not being used.
    * @param orderBy How to order the rows, formatted as an SQL ORDER BY clause
    *            (excluding the ORDER BY itself). Passing null will use the
    *            default sort order, which may be unordered.
    * @param limit Limits the number of rows returned by the query,
    *            formatted as LIMIT clause. Passing null denotes no LIMIT clause.
    * @return the SQL query string
    */
   private static String buildQueryString(
       boolean distinct, String tables, String[] columns, String where,
       String groupBy, String having, String orderBy, String limit) {
      if (TextUtils.isEmpty(groupBy) && !TextUtils.isEmpty(having)) {
         throw new IllegalArgumentException(
             "HAVING clauses are only permitted when using a groupBy clause");
      }
      if (!TextUtils.isEmpty(limit) && !sLimitPattern.matcher(limit).matches()) {
         throw new IllegalArgumentException("invalid LIMIT clauses:" + limit);
      }

      StringBuilder query = new StringBuilder(120);

      query.append("SELECT ");
      if (distinct) {
         query.append("DISTINCT ");
      }
      if (columns != null && columns.length != 0) {
         appendColumns(query, columns);
      } else {
         query.append("* ");
      }
      query.append("FROM ");
      query.append(tables);
      appendClause(query, " WHERE ", where);
      appendClause(query, " GROUP BY ", groupBy);
      appendClause(query, " HAVING ", having);
      appendClause(query, " ORDER BY ", orderBy);
      appendClause(query, " LIMIT ", limit);

      return query.toString();
   }

   private static void appendClause(StringBuilder s, String name, String clause) {
      if (!TextUtils.isEmpty(clause)) {
         s.append(name);
         s.append(clause);
      }
   }

   /**
    * Add the names that are non-null in columns to s, separating
    * them with commas.
    */
   private static void appendColumns(StringBuilder s, String[] columns) {
      int n = columns.length;

      for (int i = 0; i < n; i++) {
         String column = columns[i];

         if (column != null) {
            if (i > 0) {
               s.append(", ");
            }
            s.append(column);
         }
      }
      s.append(' ');
   }

    /**
     * Query the given table, returning a {@link Cursor} over the result set.
     *
     * @param table The table name to compile the query against.
     * @param columns A list of which columns to return. Passing null will
     *            return all columns, which is discouraged to prevent reading
     *            data from storage that isn't going to be used.
     * @param selection A filter declaring which rows to return, formatted as an
     *            SQL WHERE clause (excluding the WHERE itself). Passing null
     *            will return all rows for the given table.
     * @param selectionArgs You may include ?s in selection, which will be
     *         replaced by the values from selectionArgs, in order that they
     *         appear in the selection. The values will be bound as Strings.
     * @param groupBy A filter declaring how to group rows, formatted as an SQL
     *            GROUP BY clause (excluding the GROUP BY itself). Passing null
     *            will cause the rows to not be grouped.
     * @param having A filter declare which row groups to include in the cursor,
     *            if row grouping is being used, formatted as an SQL HAVING
     *            clause (excluding the HAVING itself). Passing null will cause
     *            all row groups to be included, and is required when row
     *            grouping is not being used.
     * @param orderBy How to order the rows, formatted as an SQL ORDER BY clause
     *            (excluding the ORDER BY itself). Passing null will use the
     *            default sort order, which may be unordered.
     * @return A {@link Cursor} object, which is positioned before the first entry. Note that
     * {@link Cursor}s are not synchronized, see the documentation for more details.
     * @see Cursor
     */
    public Cursor query(String table, String[] columns, String selection,
            String[] selectionArgs, String groupBy, String having,
                        String orderBy) {

      return query(false, table, columns, selection, selectionArgs, groupBy,
              having, orderBy, null /* limit */);
    }

    /**
     * Query the given table, returning a {@link Cursor} over the result set.
     *
     * @param table The table name to compile the query against.
     * @param columns A list of which columns to return. Passing null will
     *            return all columns, which is discouraged to prevent reading
     *            data from storage that isn't going to be used.
     * @param selection A filter declaring which rows to return, formatted as an
     *            SQL WHERE clause (excluding the WHERE itself). Passing null
     *            will return all rows for the given table.
     * @param selectionArgs You may include ?s in selection, which will be
     *         replaced by the values from selectionArgs, in order that they
     *         appear in the selection. The values will be bound as Strings.
     * @param groupBy A filter declaring how to group rows, formatted as an SQL
     *            GROUP BY clause (excluding the GROUP BY itself). Passing null
     *            will cause the rows to not be grouped.
     * @param having A filter declare which row groups to include in the cursor,
     *            if row grouping is being used, formatted as an SQL HAVING
     *            clause (excluding the HAVING itself). Passing null will cause
     *            all row groups to be included, and is required when row
     *            grouping is not being used.
     * @param orderBy How to order the rows, formatted as an SQL ORDER BY clause
     *            (excluding the ORDER BY itself). Passing null will use the
     *            default sort order, which may be unordered.
     * @param limit Limits the number of rows returned by the query,
     *            formatted as LIMIT clause. Passing null denotes no LIMIT clause.
     * @return A {@link Cursor} object, which is positioned before the first entry. Note that
     * {@link Cursor}s are not synchronized, see the documentation for more details.
     * @see Cursor
     */
    public Cursor query(String table, String[] columns, String selection,
            String[] selectionArgs, String groupBy, String having,
            String orderBy, String limit) {

        return query(false, table, columns, selection, selectionArgs, groupBy,
                having, orderBy, limit);
    }

    /**
     * Runs the provided SQL and returns a {@link Cursor} over the result set.
     *
     * @param sql the SQL query. The SQL string must not be ; terminated
     * @param selectionArgs You may include ?s in where clause in the query,
     *     which will be replaced by the values from selectionArgs. The
     *     values will be bound as Strings.
     * @return A {@link Cursor} object, which is positioned before the first entry. Note that
     * {@link Cursor}s are not synchronized, see the documentation for more details.
     */
    public Cursor rawQuery(String sql, String[] selectionArgs) {
      return rawQuery(sql, selectionArgs, null);
    }

   /**
     * Runs the provided SQL and returns a cursor over the result set.
     *
     * @param sql the SQL query. The SQL string must not be ; terminated
     * @param selectionArgs You may include ?s in where clause in the query,
     *     which will be replaced by the values from selectionArgs. The
     *     values will be bound as Strings.
     * @param cancellationSignal A signal to cancel the operation in progress, or null if none.
     * If the operation is canceled, then {@link OperationCanceledException} will be thrown
     * when the query is executed.
     * @return A {@link Cursor} object, which is positioned before the first entry. Note that
     * {@link Cursor}s are not synchronized, see the documentation for more details.
     */
    public Cursor rawQuery(
            String sql, String[] selectionArgs,
            CancellationSignal cancellationSignal) {

       if (sql == null) {
          throw new IllegalArgumentException("sql must not be null.");
       }

       if (cancellationSignal != null) {
          cancellationSignal.throwIfCanceled();
       }

       SQLiteStatementInfo info = new SQLiteStatementInfo();

       mConnection.prepare(sql, info); // might throw

       int selectionArgLength = (selectionArgs == null) ? 0 : selectionArgs.length;
       if (selectionArgLength != info.numParameters) {
          throw new IllegalArgumentException(
           "Incorrect number of bind arguments supplied.  "
               + selectionArgLength + " "
               + "arguments "
               + "were provided but the statement needs "
               + info.numParameters + " arguments.");
       }
        try {
           Cursor cursor = new SQLiteCursor(this,
               info.columnNames, sql, selectionArgs, cancellationSignal);
           return cursor;
        } catch (RuntimeException ex) {
           throw ex;
        }
    }

    /**
     * Convenience method for inserting a row into the database.
     *
     * @param table the table to insert the row into
     * @param nullColumnHack optional; may be <code>null</code>.
     *            SQL doesn't allow inserting a completely empty row without
     *            naming at least one column name.  If your provided <code>values</code> is
     *            empty, no column names are known and an empty row can't be inserted.
     *            If not set to null, the <code>nullColumnHack</code> parameter
     *            provides the name of nullable column name to explicitly insert a NULL into
     *            in the case where your <code>values</code> is empty.
     * @param values this map contains the initial column values for the
     *            row. The keys should be the column names and the values the
     *            column values
     * @return the row ID of the newly inserted row, or -1 if an error occurred
     */
    public long insert(String table, String nullColumnHack, ContentValues values) {
        try {
          return insertWithOnConflict(table, nullColumnHack, values, CONFLICT_NONE);
        } catch (SQLException e) {
          getLogger().printStackTrace(e);
          return -1;
        }
    }

    /**
     * Convenience method for inserting a row into the database.
     *
     * @param table the table to insert the row into
     * @param nullColumnHack optional; may be <code>null</code>.
     *            SQL doesn't allow inserting a completely empty row without
     *            naming at least one column name.  If your provided <code>values</code> is
     *            empty, no column names are known and an empty row can't be inserted.
     *            If not set to null, the <code>nullColumnHack</code> parameter
     *            provides the name of nullable column name to explicitly insert a NULL into
     *            in the case where your <code>values</code> is empty.
     * @param values this map contains the initial column values for the
     *            row. The keys should be the column names and the values the
     *            column values
     * @throws SQLException
     * @return the row ID of the newly inserted row, or -1 if an error occurred
     */
    public long insertOrThrow(String table, String nullColumnHack, ContentValues values)
            throws SQLException {
        return insertWithOnConflict(table, nullColumnHack, values, CONFLICT_NONE);
    }

    /**
     * Convenience method for replacing a row in the database.
     *
     * @param table the table in which to replace the row
     * @param nullColumnHack optional; may be <code>null</code>.
     *            SQL doesn't allow inserting a completely empty row without
     *            naming at least one column name.  If your provided <code>initialValues</code> is
     *            empty, no column names are known and an empty row can't be inserted.
     *            If not set to null, the <code>nullColumnHack</code> parameter
     *            provides the name of nullable column name to explicitly insert a NULL into
     *            in the case where your <code>initialValues</code> is empty.
     * @param initialValues this map contains the initial column values for
     *   the row. The key
     * @throws SQLException
     * @return the row ID of the newly inserted row, or -1 if an error occurred
     */
    public long replaceOrThrow(String table, String nullColumnHack,
            ContentValues initialValues) throws SQLException {
        return insertWithOnConflict(table, nullColumnHack, initialValues,
                CONFLICT_REPLACE);
    }

    /**
     * General method for inserting a row into the database.
     *
     * @param table the table to insert the row into
     * @param nullColumnHack optional; may be <code>null</code>.
     *            SQL doesn't allow inserting a completely empty row without
     *            naming at least one column name.  If your provided <code>initialValues</code> is
     *            empty, no column names are known and an empty row can't be inserted.
     *            If not set to null, the <code>nullColumnHack</code> parameter
     *            provides the name of nullable column name to explicitly insert a NULL into
     *            in the case where your <code>initialValues</code> is empty.
     * @param initialValues this map contains the initial column values for the
     *            row. The keys should be the column names and the values the
     *            column values
     * @param conflictAlgorithm for insert conflict resolver
     * @return the row ID of the newly inserted row
     * OR the primary key of the existing row if the input param 'conflictAlgorithm' =
     * {@link #CONFLICT_IGNORE}
     * OR -1 if any error
     */
    public long insertWithOnConflict(String table, String nullColumnHack,
            ContentValues initialValues, int conflictAlgorithm) {
      StringBuilder sql = new StringBuilder();
      sql.append("INSERT");
      sql.append(CONFLICT_VALUES[conflictAlgorithm]);
      sql.append(" INTO ");
      sql.append(table);
      sql.append('(');

      Object[] bindArgs = null;
      int size = (initialValues != null && initialValues.size() > 0)
              ? initialValues.size() : 0;
      if (size > 0) {
          bindArgs = new Object[size];
          int i = 0;
          for (String colName : initialValues.keySet()) {
              sql.append((i > 0) ? "," : "");
              sql.append(colName);
              bindArgs[i++] = initialValues.get(colName);
          }
          sql.append(')');
          sql.append(" VALUES (");
          for (i = 0; i < size; i++) {
              sql.append((i > 0) ? ",?" : "?");
          }
      } else {
          sql.append(nullColumnHack + ") VALUES (NULL");
      }
      sql.append(')');

     return executeForLastInsertedRowId(sql.toString(), bindArgs, null);
    }

    /**
     * Convenience method for deleting rows in the database.
     *
     * @param table the table to delete from
     * @param whereClause the optional WHERE clause to apply when deleting.
     *            Passing null will delete all rows.
     * @param whereArgs You may include ?s in the where clause, which
     *            will be replaced by the values from whereArgs. The values
     *            will be bound as Strings.
     * @return the number of rows affected if a whereClause is passed in, 0
     *         otherwise. To remove all rows and get a count pass "1" as the
     *         whereClause.
     */
    public int delete(String table, String whereClause, String[] whereArgs) {
      return executeForChangedRowCount("DELETE FROM " + table +
         (!TextUtils.isEmpty(whereClause) ? " WHERE " + whereClause : ""), whereArgs, null);
    }

    /**
     * Convenience method for updating rows in the database.
     *
     * @param table the table to update in
     * @param values a map from column names to new column values. null is a
     *            valid value that will be translated to NULL.
     * @param whereClause the optional WHERE clause to apply when updating.
     *            Passing null will update all rows.
     * @param whereArgs You may include ?s in the where clause, which
     *            will be replaced by the values from whereArgs. The values
     *            will be bound as Strings.
     * @return the number of rows affected
     */
    public int update(String table, ContentValues values, String whereClause, String[] whereArgs) {
        return updateWithOnConflict(table, values, whereClause, whereArgs, CONFLICT_NONE);
    }

    /**
     * Convenience method for updating rows in the database.
     *
     * @param table the table to update in
     * @param values a map from column names to new column values. null is a
     *            valid value that will be translated to NULL.
     * @param whereClause the optional WHERE clause to apply when updating.
     *            Passing null will update all rows.
     * @param whereArgs You may include ?s in the where clause, which
     *            will be replaced by the values from whereArgs. The values
     *            will be bound as Strings.
     * @param conflictAlgorithm for update conflict resolver
     * @return the number of rows affected
     */
    public int updateWithOnConflict(String table, ContentValues values,
            String whereClause, String[] whereArgs, int conflictAlgorithm) {
        if (values == null || values.size() == 0) {
            throw new IllegalArgumentException("Empty values");
        }

         StringBuilder sql = new StringBuilder(120);
         sql.append("UPDATE ");
         sql.append(CONFLICT_VALUES[conflictAlgorithm]);
         sql.append(table);
         sql.append(" SET ");

         // move all bind args to one array
         int setValuesSize = values.size();
         int bindArgsSize = (whereArgs == null) ? setValuesSize : (setValuesSize + whereArgs.length);
         Object[] bindArgs = new Object[bindArgsSize];
         int i = 0;
         for (String colName : values.keySet()) {
             sql.append((i > 0) ? "," : "");
             sql.append(colName);
             bindArgs[i++] = values.get(colName);
             sql.append("=?");
         }
         if (whereArgs != null) {
             for (i = setValuesSize; i < bindArgsSize; i++) {
                 bindArgs[i] = whereArgs[i - setValuesSize];
             }
         }
         if (!TextUtils.isEmpty(whereClause)) {
             sql.append(" WHERE ");
             sql.append(whereClause);
         }

        return executeForChangedRowCount(sql.toString(), bindArgs, null);
    }

    /**
     * Execute a single SQL statement that is NOT a SELECT/INSERT/UPDATE/DELETE.
     * <p>
     * For INSERT statements, use any of the following instead.
     * <ul>
     *   <li>{@link #insert(String, String, ContentValues)}</li>
     *   <li>{@link #insertOrThrow(String, String, ContentValues)}</li>
     *   <li>{@link #insertWithOnConflict(String, String, ContentValues, int)}</li>
     * </ul>
     * <p>
     * For UPDATE statements, use any of the following instead.
     * <ul>
     *   <li>{@link #update(String, ContentValues, String, String[])}</li>
     *   <li>{@link #updateWithOnConflict(String, ContentValues, String, String[], int)}</li>
     * </ul>
     * <p>
     * For DELETE statements, use any of the following instead.
     * <ul>
     *   <li>{@link #delete(String, String, String[])}</li>
     * </ul>
     * <p>
     * For example, the following are good candidates for using this method:
     * <ul>
     *   <li>ALTER TABLE</li>
     *   <li>CREATE or DROP table / trigger / view / index / virtual table</li>
     *   <li>REINDEX</li>
     *   <li>RELEASE</li>
     *   <li>SAVEPOINT</li>
     *   <li>PRAGMA that returns no data</li>
     * </ul>
     * </p>
     * <p>
     * When using {@link #ENABLE_WRITE_AHEAD_LOGGING}, journal_mode is
     * automatically managed by this class. So, do not set journal_mode
     * using "PRAGMA journal_mode'<value>" statement if your app is using
     * {@link #ENABLE_WRITE_AHEAD_LOGGING}
     * </p>
     *
     * @param sql the SQL statement to be executed. Multiple statements separated by semicolons are
     * not supported.
     * @param bindArgs only byte[], String, Long and Double are supported in bindArgs.
     * @throws SQLException if the SQL string is invalid
     */
    public void execSQL(String sql, Object[] bindArgs) throws SQLException {
        execute(sql, bindArgs, null);
    }

    /**
     * Returns true if the database is currently open.
     *
     * @return True if the database is currently open (has not been closed).
     */
    public boolean isOpen() {
        synchronized (mLock) {
          return (mConnection != null);
        }
    }

    public void dump(StringBuilder b, boolean verbose) {
        synchronized (mLock) {
           if (mConnection != null) {
              mConnection.dump(b, verbose);
           }
        }
    }

    /**
     * Runs 'pragma integrity_check' on the given database (and all the attached databases)
     * and returns true if the given database (and all its attached databases) pass integrity_check,
     * false otherwise.
     *<p>
     * If the result is false, then this method logs the errors reported by the integrity_check
     * command execution.
     *<p>
     * Note that 'pragma integrity_check' on a database can take a long time.
     *
     * @return true if the given database (and all its attached databases) pass integrity_check,
     * false otherwise.
     */
    public boolean isDatabaseIntegrityOk() {
       String rslt = null;
        acquireReference();
        try {
           rslt = executeForString("PRAGMA main.integrity_check(1);", null, null);
        } finally {
            releaseReference();
        }

       if (!rslt.equalsIgnoreCase("ok")) {
          // integrity_checker failed on main database
          getLogger().e(TAG,  getAppName() + " " + this.mSessionQualifier + " PRAGMA "
              + "integrity_check returned: " + rslt);
          return false;
       }
       return true;
    }

   /**
    * Marks the current transaction as successful. Do not do any more database work between
    * calling this and calling endTransaction. Do as little non-database work as possible in that
    * situation too. If any errors are encountered between this and endTransaction the transaction
    * will still be committed.
    *
    * @throws IllegalStateException if the current thread is not in a transaction or the
    * transaction is already marked as successful.
    *
    * Marks the current transaction as having completed successfully.
    * <p>
    * This method can be called at most once between {@link #beginTransactionNonExclusive} and
    * {@link #endTransaction} to indicate that the changes made by the transaction should be
    * committed.  If this method is not called, the changes will be rolled back
    * when the transaction is ended.
    * </p>
    *
    * @throws IllegalStateException if there is no current transaction, or if
    * {@link #setTransactionSuccessful} has already been called for the current transaction.
    *
    * @see #beginTransactionNonExclusive
    * @see #endTransaction
    */
   public void setTransactionSuccessful() {
      mTransactionManager.setTransactionSuccessful();
   }

   /**
    * End a transaction. See beginTransaction for notes about how to use this and when transactions
    * are committed and rolled back.
    *
    * Ends the current transaction and commits or rolls back changes.
    * <p>
    * If this is the outermost transaction (not nested within any other
    * transaction), then the changes are committed if {@link #setTransactionSuccessful}
    * was called or rolled back otherwise.
    * </p><p>
    * This method must be called exactly once for each call to {@link #beginTransactionNonExclusive}.
    * </p>
    *
    * @param cancellationSignal A signal to cancel the operation in progress, or null if none.
    *
    * @throws IllegalStateException if there is no current transaction.
    * @throws SQLiteException if an error occurs.
    * @throws OperationCanceledException if the operation was canceled.
    *
    * @see #beginTransactionNonExclusive
    * @see #setTransactionSuccessful
    */
   public void endTransaction(CancellationSignal cancellationSignal) {
      acquireReference();
      if (cancellationSignal != null) {
         cancellationSignal.throwIfCanceled();
      }
      try {
         SQLiteTransactionManager.TransactionOutcome outcome = mTransactionManager.endTransaction();
         // do nothing on the no-action outcome
         if ( outcome == SQLiteTransactionManager.TransactionOutcome.COMMIT_ACTION ) {
            mConnection.executeImpl("COMMIT;", null, cancellationSignal); // might throw
         } else if ( outcome == SQLiteTransactionManager.TransactionOutcome.ROLLBACK_ACTION ) {
            mConnection.executeImpl("ROLLBACK;", null, cancellationSignal); // might throw
         }
      } finally {
         releaseReference();
      }
   }

   /**
    */
   public void endTransaction() {
      endTransaction(null);
   }

}
