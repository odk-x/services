/*
 * Copyright (C) 2011 The Android Open Source Project
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

/* import dalvik.system.BlockGuard; */
import org.opendatakit.common.android.database.AppNameSharedStateContainer;
import org.opendatakit.common.android.database.OperationLog;
import org.opendatakit.common.android.utilities.WebLogger;

import android.database.Cursor;
import android.database.CursorWindow;
import android.database.DatabaseUtils;
import org.sqlite.database.ExtraUtils;
import android.os.CancellationSignal;
import android.os.OperationCanceledException;
import android.os.ParcelFileDescriptor;
import android.util.LruCache;

import java.util.Map;

/**
 * Represents a SQLite database connection.
 * Each connection wraps an instance of a native <code>sqlite3</code> object.
 * <p>
 * When database connection pooling is enabled, there can be multiple active
 * connections to the same database.  Otherwise there is typically only one
 * connection per database.
 * </p><p>
 * When the SQLite WAL feature is enabled, multiple readers and one writer
 * can concurrently access the database.  Without WAL, readers and writers
 * are mutually exclusive.
 * </p>
 *
 * <h2>Ownership and concurrency guarantees</h2>
 * <p>
 * Connection objects are not thread-safe. Classes using connections are
 * responsible for serializing operations to guard against concurrent
 * use of a connection.
 * </p><p>
 * The guarantee of having a single owner allows this class to be implemented
 * without locks and greatly simplifies resource management.
 * </p>
 *
 * <h2>Encapsulation guarantees</h2>
 * <p>
 * The connection object object owns *all* of the SQLite related native
 * objects that are associated with the connection.  What's more, there are
 * no other objects in the system that are capable of obtaining handles to
 * those native objects.  Consequently, when the connection is closed, we do
 * not have to worry about what other components might have references to
 * its associated SQLite state -- there are none.
 * </p><p>
 * Encapsulation is what ensures that the connection object's
 * lifecycle does not become a tortured mess of finalizers and reference
 * queues.
 * </p>
 *
 * <h2>Reentrance</h2>
 * <p>
 * This class must tolerate reentrant execution of SQLite operations because
 * triggers may call custom SQLite functions that perform additional queries.
 * </p>
 *
 * @hide
 */
public final class SQLiteConnection implements CancellationSignal.OnCancelListener {
    private static final String TAG = "SQLiteConnection";
    private static final boolean DEBUG = false;

    private static final String[] EMPTY_STRING_ARRAY = new String[0];

    private final CloseGuard mCloseGuard;

    private final SQLiteDatabaseConfiguration mConfiguration;
    private final String mSessionQualifier;

    // The recent operations log.
    private final OperationLog mRecentOperations;

   /**
    * Mutex used when accessing or using mConnectionPtr
    * i.e., when calling native methods that use it.
    */
   private final Object mConnectionPtrMutex = new Object();

   /**
    * The native (C++) SQLiteConnection pointer.
    * This should only be accessed within the
    * mNativeMutex.
    */
    private long mConnectionPtr = 0L;

    // The number of times attachCancellationSignal has been called.
    // Because SQLite statement execution can be reentrant, we keep track of how many
    // times we have attempted to attach a cancellation signal to the connection so that
    // we can ensure that we detach the signal at the right time.
    private int mCancellationSignalAttachCount;

    private static native long nativeOpen(String path, int openFlags, String label,
            boolean enableTrace, boolean enableProfile);
    private static native void nativeClose(long connectionPtr);
    private static native void nativeRegisterCustomFunction(long connectionPtr,
            SQLiteCustomFunction function);
    private static native void nativeRegisterLocalizedCollators(long connectionPtr, String locale);
    private static native long nativePrepareStatement(long connectionPtr, String sql);
    private static native void nativeFinalizeStatement(long connectionPtr, long statementPtr);
    private static native int nativeGetParameterCount(long connectionPtr, long statementPtr);
    private static native boolean nativeIsReadOnly(long connectionPtr, long statementPtr);
    private static native int nativeGetColumnCount(long connectionPtr, long statementPtr);
    private static native String nativeGetColumnName(long connectionPtr, long statementPtr,
            int index);
    private static native void nativeBindNull(long connectionPtr, long statementPtr,
            int index);
    private static native void nativeBindLong(long connectionPtr, long statementPtr,
            int index, long value);
    private static native void nativeBindDouble(long connectionPtr, long statementPtr,
            int index, double value);
    private static native void nativeBindString(long connectionPtr, long statementPtr,
            int index, String value);
    private static native void nativeBindBlob(long connectionPtr, long statementPtr,
            int index, byte[] value);
    private static native void nativeResetStatementAndClearBindings(
            long connectionPtr, long statementPtr);
    private static native void nativeExecute(long connectionPtr, long statementPtr);
    private static native long nativeExecuteForLong(long connectionPtr, long statementPtr);
    private static native String nativeExecuteForString(long connectionPtr, long statementPtr);
    private static native int nativeExecuteForBlobFileDescriptor(
            long connectionPtr, long statementPtr);
    private static native int nativeExecuteForChangedRowCount(long connectionPtr, long statementPtr);
    private static native long nativeExecuteForLastInsertedRowId(
            long connectionPtr, long statementPtr);
    private static native long nativeExecuteForCursorWindow(
            long connectionPtr, long statementPtr, CursorWindow win,
            int startPos, int requiredPos, boolean countAllRows);
    private static native int nativeGetDbLookaside(long connectionPtr);
    private static native void nativeCancel(long connectionPtr);
    private static native void nativeResetCancel(long connectionPtr, boolean cancelable);

    private static native boolean nativeHasCodec();
    public static boolean hasCodec(){ return nativeHasCodec(); }

    public SQLiteConnection(SQLiteDatabaseConfiguration configuration,
                             OperationLog recentOperations,
                             String connectionId) {
        mConfiguration = new SQLiteDatabaseConfiguration(configuration);
        mCloseGuard = CloseGuard.get(getLogger());
        mRecentOperations = recentOperations;
        mSessionQualifier = connectionId;
        mPreparedStatementCache = new PreparedStatementCache(
                mConfiguration.maxSqlCacheSize);
        mCloseGuard.open("close");
    }

    private WebLogger getLogger() {
    return WebLogger.getLogger(mConfiguration.appName);
  }

    @Override
    protected void finalize() throws Throwable {
        try {
           boolean isNotClosed;
           synchronized (mConnectionPtrMutex) {
              isNotClosed = (mConnectionPtr != 0L);
           }
          if (isNotClosed) {
             // This code is running inside of the SQLiteConnection finalizer.
             //
             // We don't know whether it is just the connection that has been finalized (and leaked)
             // or whether the connection pool has also been or is about to be finalized.
             // Consequently, it would be a bad idea to try to grab any locks or to
             // do any significant work here.  So we do the simplest possible thing and
             // set a flag.  waitForConnection() periodically checks this flag (when it
             // times out) so that it can recover from leaked connections and wake
             // itself or other threads up if necessary.
             //
             // You might still wonder why we don't try to do more to wake up the waiters
             // immediately.  First, as explained above, it would be hard to do safely
             // unless we started an extra Thread to function as a reference queue.  Second,
             // this is never supposed to happen in normal operation.  Third, there is no
             // guarantee that the GC will actually detect the leak in a timely manner so
             // it's not all that important that we recover from the leak in a timely manner
             // either.  Fourth, if a badly behaved application finds itself hung waiting for
             // several seconds while waiting for a leaked connection to be detected and recreated,
             // then perhaps its authors will have added incentive to fix the problem!

             getLogger().w(TAG, "A SQLiteConnection object for database '" + mConfiguration.appName
                 + "' sessionQualifier '" + mSessionQualifier + "' was leaked!  Please fix your "
                 + "application to end transactions in progress properly and to close the "
                 + "database when it is no longer needed.");
          }

          dispose(true);
        } finally {
          super.finalize();
        }
    }

    // Called by SQLiteConnectionPool only.
    // Closes the database closes and releases all of its associated resources.
    // Do not call methods on the connection after it is closed.  It will probably crash.
    void close() {
        dispose(false);
    }

    void open() {
      if ((mConfiguration.openFlags & SQLiteDatabase.ENABLE_WRITE_AHEAD_LOGGING) == 0) {
        throw new IllegalStateException("Only WAL mode is allowed");
      }

       synchronized (mConnectionPtrMutex) {
          mConnectionPtr = nativeOpen(mConfiguration.path, mConfiguration.openFlags, mConfiguration.label,
              SQLiteDebug.DEBUG_SQL_STATEMENTS, SQLiteDebug.DEBUG_SQL_TIME);

          // Register custom functions.
          final int functionCount = mConfiguration.customFunctions.size();
          for (int i = 0; i < functionCount; i++) {
             SQLiteCustomFunction function = mConfiguration.customFunctions.get(i);
             nativeRegisterCustomFunction(mConnectionPtr, function);
          }

          {
             final long newValue = SQLiteGlobal.getDefaultPageSize();
             long value = executeForLong("PRAGMA page_size", null, null);
             if (value != newValue) {
                executeImpl("PRAGMA page_size=" + newValue, null, null);
             }
          }

          {
             final long newValue = mConfiguration.foreignKeyConstraintsEnabled ? 1 : 0;
             long value = executeForLong("PRAGMA foreign_keys", null, null);
             if (value != newValue) {
                executeImpl("PRAGMA foreign_keys=" + newValue, null, null);
             }
          }

          {
             final long newValue = SQLiteGlobal.getJournalSizeLimit();
             long value = executeForLong("PRAGMA journal_size_limit", null, null);
             if (value != newValue) {
                executeForLong("PRAGMA journal_size_limit=" + newValue, null, null);
             }
          }

          {
             final long newValue = SQLiteGlobal.getWALAutoCheckpoint();
             long value = executeForLong("PRAGMA wal_autocheckpoint", null, null);
             if (value != newValue) {
                executeForLong("PRAGMA wal_autocheckpoint=" + newValue, null, null);
             }
          }

          setLockingMode("NORMAL");
          setJournalMode("WAL");
          setSyncMode(SQLiteGlobal.getWALSyncMode());
          setBusyTimeout();
       }
    }

   private void setBusyTimeout() {
      final long newValue = 5000L;
      long value = executeForLong("PRAGMA busy_timeout", null, null);
      if (value != newValue) {
         getLogger().w(TAG,"busy_timeout is not " + newValue + " but " + value);
         // TODO: fix this when C++ code is updated.
         // sqlite code does a strcmp that only recognizes 'busy_timeout', making it
         // impossible to update this value.
         // executeImpl("PRAGMA busy_timeout=" + newValue, null, null);
      }
   }

   private void setLockingMode(String newValue) {
      String value = executeForString("PRAGMA locking_mode", null, null);
      if (!value.equalsIgnoreCase(newValue)) {
         String result = executeForString("PRAGMA locking_mode=" + newValue, null, null);
         if (result.equalsIgnoreCase(newValue)) {
            return;
         }
         // PRAGMA locking_mode silently fails and returns the original journal
         // mode in some cases if the locking mode could not be changed.
         getLogger().e(TAG, "Could not change the database locking mode of '"
             + mConfiguration.label + "' from '" + value + "' to '" + newValue);
         throw new IllegalStateException("Unable to change the locking mode");
      }
   }

   private void setJournalMode(String newValue) {
      String value = executeForString("PRAGMA journal_mode", null, null);
      if (!value.equalsIgnoreCase(newValue)) {
         String result = executeForString("PRAGMA journal_mode=" + newValue, null, null);
         if (result.equalsIgnoreCase(newValue)) {
            return;
         }
         // PRAGMA journal_mode silently fails and returns the original journal
         // mode in some cases if the journal mode could not be changed.
         getLogger().e(TAG, "Could not change the database journal mode of '"
             + mConfiguration.label + "' from '" + value + "' to '" + newValue
             + "' because the database is locked.  This usually means that "
             + "there are other open connections to the database which prevents "
             + "the database from enabling or disabling write-ahead logging mode.  "
             + "Proceeding without changing the journal mode.");
         throw new IllegalStateException("Unable to change the journal mode");
      }
   }

   private void setSyncMode(String newValue) {
      String value = executeForString("PRAGMA synchronous", null, null);
      if (!canonicalizeSyncMode(value).equalsIgnoreCase(
          canonicalizeSyncMode(newValue))) {
         executeImpl("PRAGMA synchronous=" + newValue, null, null);
      }
   }

   private static String canonicalizeSyncMode(String value) {
      if (value.equals("0")) {
         return "OFF";
      } else if (value.equals("1")) {
         return "NORMAL";
      } else if (value.equals("2")) {
         return "FULL";
      }
      return value;
   }

   synchronized void dispose(boolean finalized) {
        if (mCloseGuard != null) {
            if (finalized) {
                mCloseGuard.warnIfOpen();
            }
            mCloseGuard.close();
        }

         synchronized (mConnectionPtrMutex) {
           if (mConnectionPtr != 0L) {
               final int cookie = mRecentOperations.beginOperation(mSessionQualifier, "close", null,
                   null);
               try {
                 mPreparedStatementCache.evictAll();
                 nativeClose(mConnectionPtr);
               } catch ( Throwable t) {
                  mRecentOperations.failOperation(cookie, t);
                 throw t;
               } finally {
                 mConnectionPtr = 0L;
                 mRecentOperations.endOperation(cookie);
               }
           }
        }
    }

    /**
     * Prepares a statement for execution but does not bind its parameters or execute it.
     * <p>
     * This method can be used to check for syntax errors during compilation
     * prior to execution of the statement.  If the {@code outStatementInfo} argument
     * is not null, the provided {@link SQLiteStatementInfo} object is populated
     * with information about the statement.
     * </p><p>
     * A prepared statement makes no reference to the arguments that may eventually
     * be bound to it, consequently it it possible to cache certain prepared statements
     * such as SELECT or INSERT/UPDATE statements.  If the statement is cacheable,
     * then it will be stored in the cache for later.
     * </p><p>
     * To take advantage of this behavior as an optimization, the connection pool
     * provides a method to acquire a connection that already has a given SQL statement
     * in its prepared statement cache so that it is ready for execution.
     * </p>
     *
     * @param sql The SQL statement to prepare.
     * @param outStatementInfo The {@link SQLiteStatementInfo} object to populate
     * with information about the statement, or null if none.
     *
     * @throws SQLiteException if an error occurs, such as a syntax error.
     */
    public void prepare(String sql, SQLiteStatementInfo outStatementInfo) {
        if (sql == null) {
            throw new IllegalArgumentException("sql must not be null.");
        }

       synchronized (mConnectionPtrMutex) {
          if (mConnectionPtr == 0L) {
             throw new SQLiteException("connection closed");
          }
          final int cookie = mRecentOperations
              .beginOperation(mSessionQualifier, "prepare", sql, null);
          try {
             final PreparedStatement statement = mPreparedStatementCache.acquirePreparedStatement(
                 sql);
             try {
                if (outStatementInfo != null) {
                   outStatementInfo.numParameters = statement.mNumParameters;
                   outStatementInfo.readOnly = statement.mReadOnly;

                   final int columnCount = nativeGetColumnCount(mConnectionPtr, statement.mStatementPtr);
                   if (columnCount == 0) {
                      outStatementInfo.columnNames = EMPTY_STRING_ARRAY;
                   } else {
                      outStatementInfo.columnNames = new String[columnCount];
                      for (int i = 0; i < columnCount; i++) {
                         outStatementInfo.columnNames[i] = nativeGetColumnName(mConnectionPtr,
                             statement.mStatementPtr, i);
                      }
                   }
                }
             } finally {
                mPreparedStatementCache.releasePreparedStatement(statement);
             }
          } catch (Throwable t) {
             mRecentOperations.failOperation(cookie, t);
             throw t;
          } finally {
             mRecentOperations.endOperation(cookie);
          }
       }
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
    public void executeImpl(String sql, Object[] bindArgs, CancellationSignal cancellationSignal) {
        if (sql == null) {
            throw new IllegalArgumentException("sql must not be null.");
        }

       synchronized (mConnectionPtrMutex) {
          if (mConnectionPtr == 0L) {
             throw new SQLiteException("connection closed");
          }
          final int cookie = mRecentOperations
              .beginOperation(mSessionQualifier, "executeImpl", sql, bindArgs);
          try {
             final PreparedStatement statement = mPreparedStatementCache.acquirePreparedStatement(sql);
             try {
                bindArguments(statement, bindArgs);
                attachCancellationSignal(cancellationSignal);
                try {
                   nativeExecute(mConnectionPtr, statement.mStatementPtr);
                } finally {
                   detachCancellationSignal(cancellationSignal);
                }
             } finally {
                mPreparedStatementCache.releasePreparedStatement(statement);
             }
          } catch (Throwable t) {
             mRecentOperations.failOperation(cookie, t);
             throw t;
          } finally {
             mRecentOperations.endOperation(cookie);
          }
       }
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

       synchronized (mConnectionPtrMutex) {
          if (mConnectionPtr == 0L) {
             throw new SQLiteException("connection closed");
          }
          final int cookie = mRecentOperations
              .beginOperation(mSessionQualifier, "executeForLong", sql, bindArgs);
          try {
             final PreparedStatement statement = mPreparedStatementCache.acquirePreparedStatement(sql);
             try {
                bindArguments(statement, bindArgs);
                attachCancellationSignal(cancellationSignal);
                try {
                   return nativeExecuteForLong(mConnectionPtr, statement.mStatementPtr);
                } finally {
                   detachCancellationSignal(cancellationSignal);
                }
             } finally {
                mPreparedStatementCache.releasePreparedStatement(statement);
             }
          } catch (Throwable t) {
             mRecentOperations.failOperation(cookie, t);
             throw t;
          } finally {
             mRecentOperations.endOperation(cookie);
          }
       }
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

       synchronized (mConnectionPtrMutex) {
          if (mConnectionPtr == 0L) {
             throw new SQLiteException("connection closed");
          }
          final int cookie = mRecentOperations
              .beginOperation(mSessionQualifier, "executeForString", sql, bindArgs);
          try {
             final PreparedStatement statement = mPreparedStatementCache.acquirePreparedStatement(sql);
             try {
                bindArguments(statement, bindArgs);
                attachCancellationSignal(cancellationSignal);
                try {
                   return nativeExecuteForString(mConnectionPtr, statement.mStatementPtr);
                } finally {
                   detachCancellationSignal(cancellationSignal);
                }
             } finally {
                mPreparedStatementCache.releasePreparedStatement(statement);
             }
          } catch (Throwable t) {
             mRecentOperations.failOperation(cookie, t);
             throw t;
          } finally {
             mRecentOperations.endOperation(cookie);
          }
       }
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

       synchronized (mConnectionPtrMutex) {
          if (mConnectionPtr == 0L) {
             throw new SQLiteException("connection closed");
          }
          final int cookie = mRecentOperations
              .beginOperation(mSessionQualifier, "executeForBlobFileDescriptor", sql, bindArgs);
          try {
             final PreparedStatement statement = mPreparedStatementCache.acquirePreparedStatement(sql);
             try {
                bindArguments(statement, bindArgs);
                attachCancellationSignal(cancellationSignal);
                try {
                   int fd = nativeExecuteForBlobFileDescriptor(mConnectionPtr, statement.mStatementPtr);
                   return fd >= 0 ? ParcelFileDescriptor.adoptFd(fd) : null;
                } finally {
                   detachCancellationSignal(cancellationSignal);
                }
             } finally {
                mPreparedStatementCache.releasePreparedStatement(statement);
             }
          } catch (Throwable t) {
             mRecentOperations.failOperation(cookie, t);
             throw t;
          } finally {
             mRecentOperations.endOperation(cookie);
          }
       }
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
       synchronized (mConnectionPtrMutex) {
          if (mConnectionPtr == 0L) {
             throw new SQLiteException("connection closed");
          }

          int changedRows = 0;
          final int cookie = mRecentOperations
              .beginOperation(mSessionQualifier, "executeForChangedRowCount", sql, bindArgs);
          try {
             final PreparedStatement statement = mPreparedStatementCache.acquirePreparedStatement(sql);
             try {
                bindArguments(statement, bindArgs);
                attachCancellationSignal(cancellationSignal);
                try {
                   changedRows = nativeExecuteForChangedRowCount(mConnectionPtr, statement.mStatementPtr);
                   return changedRows;
                } finally {
                   detachCancellationSignal(cancellationSignal);
                }
             } finally {
                mPreparedStatementCache.releasePreparedStatement(statement);
             }
          } catch (Throwable t) {
             mRecentOperations.failOperation(cookie, t);
             throw t;
          } finally {
             mRecentOperations.endOperationDeferLogAdditional(cookie, "changedRows=" + changedRows);
          }
       }
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

       synchronized (mConnectionPtrMutex) {
          if (mConnectionPtr == 0L) {
             throw new SQLiteException("connection closed");
          }

          final int cookie = mRecentOperations
              .beginOperation(mSessionQualifier, "executeForLastInsertedRowId", sql, bindArgs);
          try {
             final PreparedStatement statement = mPreparedStatementCache.acquirePreparedStatement(sql);
             try {
                bindArguments(statement, bindArgs);
                attachCancellationSignal(cancellationSignal);
                try {
                   return nativeExecuteForLastInsertedRowId(mConnectionPtr, statement.mStatementPtr);
                } finally {
                   detachCancellationSignal(cancellationSignal);
                }
             } finally {
                mPreparedStatementCache.releasePreparedStatement(statement);
             }
          } catch (Throwable t) {
             mRecentOperations.failOperation(cookie, t);
             throw t;
          } finally {
             mRecentOperations.endOperation(cookie);
          }
       }
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

        window.acquireReference();
        try {
            int actualPos = -1;
            int countedRows = -1;
            int filledRows = -1;
           synchronized (mConnectionPtrMutex) {
              if (mConnectionPtr == 0L) {
                 throw new SQLiteException("connection closed");
              }
              final int cookie = mRecentOperations
                  .beginOperation(mSessionQualifier, "executeForCursorWindow", sql, bindArgs);
              try {
                 final PreparedStatement statement = mPreparedStatementCache.acquirePreparedStatement(sql);
                 try {
                    bindArguments(statement, bindArgs);
                    attachCancellationSignal(cancellationSignal);
                    try {
                       final long result = nativeExecuteForCursorWindow(mConnectionPtr, statement.mStatementPtr, window,
                           startPos, requiredPos, countAllRows);
                       actualPos = (int) (result >> 32);
                       countedRows = (int) result;
                       filledRows = window.getNumRows();
                       window.setStartPosition(actualPos);
                       return countedRows;
                    } finally {
                       detachCancellationSignal(cancellationSignal);
                    }
                 } finally {
                    mPreparedStatementCache.releasePreparedStatement(statement);
                 }
              } catch (Throwable t) {
                 mRecentOperations.failOperation(cookie, t);
                 throw t;
              } finally {
                 mRecentOperations.endOperationDeferLogAdditional(cookie,
                     "window='" + window + "', startPos=" + startPos + ", actualPos=" + actualPos + ", filledRows=" + filledRows
                         + ", countedRows=" + countedRows);
              }
           }
        } finally {
            window.releaseReference();
        }
    }

    private void attachCancellationSignal(CancellationSignal cancellationSignal) {
        if (cancellationSignal != null) {
            cancellationSignal.throwIfCanceled();

            mCancellationSignalAttachCount += 1;
            if (mCancellationSignalAttachCount == 1) {
                // Reset cancellation flag before executing the statement.
                nativeResetCancel(mConnectionPtr, true /*cancelable*/);

                // After this point, onCancel() may be called concurrently.
                cancellationSignal.setOnCancelListener(this);
            }
        }
    }

    private void detachCancellationSignal(CancellationSignal cancellationSignal) {
        if (cancellationSignal != null) {
            assert mCancellationSignalAttachCount > 0;

            mCancellationSignalAttachCount -= 1;
            if (mCancellationSignalAttachCount == 0) {
                // After this point, onCancel() cannot be called concurrently.
                cancellationSignal.setOnCancelListener(null);

                // Reset cancellation flag after executing the statement.
                nativeResetCancel(mConnectionPtr, false /*cancelable*/);
            }
        }
    }

    // CancellationSignal.OnCancelListener callback.
    // This method may be called on a different thread than the executing statement.
    // However, it will only be called between calls to attachCancellationSignal and
    // detachCancellationSignal, while a statement is executing.  We can safely assume
    // that the SQLite connection is still alive.
    @Override
    public void onCancel() {
       synchronized (mConnectionPtrMutex) {
          if (mConnectionPtr == 0L) {
             throw new SQLiteException("connection closed");
          }
          nativeCancel(mConnectionPtr);
       }
    }

    private void bindArguments(PreparedStatement statement, Object[] bindArgs) {
        final int count = bindArgs != null ? bindArgs.length : 0;
        if (count != statement.mNumParameters) {
            throw new SQLiteBindOrColumnIndexOutOfRangeException(
                    "Expected " + statement.mNumParameters + " bind arguments but "
                    + count + " were provided.");
        }
        if (count == 0) {
            return;
        }

        final long statementPtr = statement.mStatementPtr;
        for (int i = 0; i < count; i++) {
            final Object arg = bindArgs[i];
            switch (ExtraUtils.getTypeOfObject(arg)) {
                case Cursor.FIELD_TYPE_NULL:
                    nativeBindNull(mConnectionPtr, statementPtr, i + 1);
                    break;
                case Cursor.FIELD_TYPE_INTEGER:
                    nativeBindLong(mConnectionPtr, statementPtr, i + 1,
                            ((Number)arg).longValue());
                    break;
                case Cursor.FIELD_TYPE_FLOAT:
                    nativeBindDouble(mConnectionPtr, statementPtr, i + 1,
                            ((Number)arg).doubleValue());
                    break;
                case Cursor.FIELD_TYPE_BLOB:
                    nativeBindBlob(mConnectionPtr, statementPtr, i + 1, (byte[])arg);
                    break;
                case Cursor.FIELD_TYPE_STRING:
                default:
                    if (arg instanceof Boolean) {
                        // Provide compatibility with legacy applications which may pass
                        // Boolean values in bind args.
                        nativeBindLong(mConnectionPtr, statementPtr, i + 1,
                                ((Boolean)arg).booleanValue() ? 1 : 0);
                    } else {
                        nativeBindString(mConnectionPtr, statementPtr, i + 1, arg.toString());
                    }
                    break;
            }
        }
    }

    /**
     * Dumps debugging information about this connection, in the case where the
     * caller might not actually own the connection.
     *
     * This function is written so that it may be called by a thread that does not
     * own the connection.  We need to be very careful because the connection state is
     * not synchronized.
     *
     * At worst, the method may return stale or slightly wrong data, however
     * it should not crash.  This is ok as it is only used for diagnostic purposes.
     *
     * @param b The StringBuilder to receive the dump, not null.
     * @param verbose True to dump more verbose information.
     */
    public void dump(StringBuilder b, boolean verbose) {
        b.append("SessionQualifier: ").append(mSessionQualifier).append("\n");
        if (verbose) {
           synchronized (mConnectionPtrMutex) {
              b.append("connectionPtr: 0x").append(Long.toHexString(mConnectionPtr)).append("\n");
           }
           b.append("lookaside: ").append(getLookaside()).append("\n");
           b.append("preparedStatementCache hitCount: ")
               .append(getPreparedStatementCacheHitCount()).append(" missCount: ")
               .append(getPreparedStatementCacheMissCount()).append(" size: ")
               .append(getPreparedStatementCacheSize()).append("\n");
           mPreparedStatementCache.dump(b);
        }
    }

   public int getLookaside() {
      synchronized (mConnectionPtrMutex) {
         if ( mConnectionPtr != 0L ) {
            return nativeGetDbLookaside(mConnectionPtr);
         } else {
            return 0;
         }
      }
   }

   public int getPreparedStatementCacheHitCount() {
      return mPreparedStatementCache.hitCount();
   }

   public int getPreparedStatementCacheMissCount() {
      return mPreparedStatementCache.missCount();
   }

   public int getPreparedStatementCacheSize() {
      return mPreparedStatementCache.size();
   }

    @Override
    public String toString() {
        return "SQLiteConnection: " + mConfiguration.path + " (" + mSessionQualifier + ")";
    }

    /**
     * Holder type for a prepared statement.
     *
     * Although this object holds a pointer to a native statement object, it
     * does not have a finalizer.  This is deliberate.  The {@link SQLiteConnection}
     * owns the statement object and will take care of freeing it when needed.
     * In particular, closing the connection requires a guarantee of deterministic
     * resource disposal because all native statement objects must be freed before
     * the native database object can be closed.  So no finalizers here.
     */
    private static final class PreparedStatement {
        // Next item in pool.
        public PreparedStatement mPoolNext;

        // The SQL from which the statement was prepared.
        public String mSql;

        // The native sqlite3_stmt object pointer.
        // Lifetime is managed explicitly by the connection.
        public long mStatementPtr;

        // The number of parameters that the prepared statement has.
        public int mNumParameters;

        // The statement type.
        public int mType;

        // True if the statement is read-only.
        public boolean mReadOnly;

        // True if the statement is in use (currently executing).
        // We need this flag because due to the use of custom functions in triggers, it's
        // possible for SQLite calls to be re-entrant.  Consequently we need to prevent
        // in use statements from being finalized until they are no longer in use.
        public boolean mInUse;
    }

   private final PreparedStatementCache mPreparedStatementCache;


   private final class PreparedStatementCache {

       private final class PreparedStatementCacheImpl
            extends LruCache<String, PreparedStatement> {
          public PreparedStatementCacheImpl(int size) {
             super(size);
          }

          @Override protected void entryRemoved(boolean evicted, String key, PreparedStatement oldValue, PreparedStatement newValue) {
             if (!oldValue.mInUse) {
                releasePreparedStatement(oldValue);
             }
          }

          public void dump(StringBuilder b) {
             b.append("  Prepared statement cache:\n");
             Map<String, PreparedStatement> cache = snapshot();
             if (!cache.isEmpty()) {
                int i = 0;
                for (Map.Entry<String, PreparedStatement> entry : cache.entrySet()) {
                   PreparedStatement statement = entry.getValue();
                   String sql = entry.getKey();
                   b.append("    " + i
                       + ": statementPtr=0x" + Long.toHexString(statement.mStatementPtr)
                       + ", numParameters=" + statement.mNumParameters
                       + ", type=" + statement.mType + ", readOnly="
                       + statement.mReadOnly + ", sql=\""
                       + AppNameSharedStateContainer.trimSqlForDisplay(sql) + "\"\n");
                   i += 1;
                }
             } else {
                b.append("    <none>\n");
             }
          }
       }

       private final PreparedStatementCacheImpl impl;

       private PreparedStatement mPreparedStatementPool;

      PreparedStatementCache(int size) {
         impl = new PreparedStatementCacheImpl(size);
      }

      public void dump(StringBuilder b) {
         synchronized (impl) {
            impl.dump(b);
         }
      }

      /**
       * Caller must hold mConnectionPtrMutex before
       * calling this method.
       */
      void evictAll() {
         synchronized (impl) {
            // may trigger releasePreparedStatement()
            impl.evictAll();
         }
      }

      int hitCount() {
         synchronized (impl) {
            return impl.hitCount();
         }
      }

      int missCount() {
         synchronized (impl) {
            return impl.missCount();
         }
      }

      int size() {
         synchronized (impl) {
            return impl.size();
         }
      }

      /**
       * Caller must hold mConnectionPtrMutex before calling this method.
       *
       * @param sql
       * @return
       */
      PreparedStatement acquirePreparedStatement(String sql) {
         synchronized (impl) {
            // see if we have a not-in-use one already in the cache...
            PreparedStatement existing = impl.get(sql);

            if (existing != null && !existing.mInUse) {
               // we found one - mark it as in-use and return it
               existing.mInUse = true;
               return existing;
            }

            PreparedStatement statement = null;
            // Either there is an in-use statement in the cache or the
            // statement is not yet in the cache. Create a new statement.
            final long statementPtr = nativePrepareStatement(mConnectionPtr, sql);
            try {
               final int numParameters = nativeGetParameterCount(mConnectionPtr, statementPtr);
               final int type = DatabaseUtils.getSqlStatementType(sql);
               final boolean readOnly = nativeIsReadOnly(mConnectionPtr, statementPtr);
               // and build up the statement.
               statement = obtainPreparedStatement(sql, statementPtr, numParameters, type, readOnly);

               if ((existing == null) && isCacheable(type)) {
                  // put may trigger releasePreparedStatement()
                  impl.put(sql, statement);
               }
            } catch (RuntimeException ex) {
               // Finalize the statement if an exception occurred and we did not add
               // it to the cache.  If it is already in the cache, then leave it there.
               PreparedStatement ref = impl.get(sql);
               if ( ref == statement ) {
                  // remove will trigger releasePreparedStatement()
                  impl.remove(sql);
               } else {
                  nativeFinalizeStatement(mConnectionPtr, statementPtr);
               }
               throw ex;
            }
            return statement;
         }
      }

      /**
       * Caller MUST hold the mConnectionPtrMutex before calling this method
       *
       * @param statement
       */
      void releasePreparedStatement(PreparedStatement statement) {
         synchronized (impl) {
            PreparedStatement existing = impl.get(statement.mSql);
            if (existing == statement) {
               try {
                  if ( mConnectionPtr != 0L && statement.mStatementPtr != 0L ) {
                     nativeResetStatementAndClearBindings(mConnectionPtr, statement.mStatementPtr);
                  }
                  statement.mInUse = false;
               } catch (Throwable t) {
                  // The statement could not be reset due to an error.  Remove it from the cache.
                  // When remove() is called, the cache will invoke its entryRemoved() callback,
                  // which will in turn call finalizePreparedStatement() to finalize and
                  // recycle the statement.
                  getLogger().d(TAG, "Could not reset prepared statement due to an exception.  "
                      + "Removing it from the cache.  SQL: "
                      + AppNameSharedStateContainer.trimSqlForDisplay(statement.mSql));
                  getLogger().printStackTrace(t);
                  // remove will trigger releasePreparedStatement() [again]
                  impl.remove(statement.mSql);
               }
            } else {
               try {
                  if ( mConnectionPtr != 0L && statement.mStatementPtr != 0L ) {
                     nativeFinalizeStatement(mConnectionPtr, statement.mStatementPtr);
                  }
               } catch (Throwable t) {
                  // If we are finalizing, we really don't care what the error is.
                  getLogger().d(TAG, "Could not finalize prepared statement due to an exception.  "
                      + "SQL: " + AppNameSharedStateContainer.trimSqlForDisplay(statement.mSql));
                  getLogger().printStackTrace(t);
               } finally {
                  recyclePreparedStatement(statement);
               }
            }
         }
      }

      /**
       * Caller must hold mConnectionPtrMutex before calling this method.
       * Constructs a statement from scratch or from the re-use pool.
       * No interaction with LRU cache.
       *
       * @param sql
       * @param statementPtr
       * @param numParameters
       * @param type
       * @param readOnly
       * @return
       */
      private PreparedStatement obtainPreparedStatement(String sql, long statementPtr,
          int numParameters, int type, boolean readOnly) {
         synchronized (impl) {
            PreparedStatement statement = mPreparedStatementPool;
            if (statement != null) {
               mPreparedStatementPool = statement.mPoolNext;
               statement.mPoolNext = null;
            } else {
               statement = new PreparedStatement();
            }
            statement.mSql = sql;
            statement.mStatementPtr = statementPtr;
            statement.mNumParameters = numParameters;
            statement.mType = type;
            statement.mReadOnly = readOnly;
            statement.mInUse = true;
            return statement;
         }
      }

      /**
       * Puts the returned statement on the re-use pool.
       * No interaction with LRU cache.
       *
       * @param statement
       */
      private void recyclePreparedStatement(PreparedStatement statement) {
         synchronized (impl) {
            statement.mSql = null;
            statement.mPoolNext = mPreparedStatementPool;
            mPreparedStatementPool = statement;
         }
      }

      private boolean isCacheable(int statementType) {
         if (statementType == DatabaseUtils.STATEMENT_UPDATE || statementType == DatabaseUtils.STATEMENT_SELECT) {
            return true;
         }
         return false;
      }
   }
}
