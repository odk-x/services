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

import android.os.CancellationSignal;
import android.os.OperationCanceledException;

import android.database.AbstractWindowedCursor;
import android.database.CursorWindow;
import android.util.Log;
import org.opendatakit.common.android.utilities.ODKFileUtils;
import org.opendatakit.common.android.logging.WebLogger;
import org.opendatakit.common.android.logging.WebLoggerIf;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * A Cursor implementation that exposes results from a query on a
 * {@link SQLiteConnection}.
 *
 * SQLiteUnsafeCursor and its base classes are not internally synchronized.
 * This is wrapped in SQLiteCursor to provide thread safety.
 */
class SQLiteUnsafeCursor extends AbstractWindowedCursor {
    static final String TAG = "SQLiteUnsafeCursor";
    static final int NO_COUNT = -1;

   /**
    * Picks a start position for SQLiteQuery.fillWindow(CursorWindow,int,int,boolean) such that the
    * window will contain the requested row and a useful range of rows
    * around it.
    *
    * When the data set is too large to fit in a cursor window, seeking the
    * cursor can become a very expensive operation since we have to run the
    * query again when we move outside the bounds of the current window.
    *
    * We try to choose a start position for the cursor window such that
    * 1/3 of the window's capacity is used to hold rows before the requested
    * position and 2/3 of the window's capacity is used to hold rows after the
    * requested position.
    *
    * @param cursorPosition The row index of the row we want to get.
    * @param cursorWindowCapacity The estimated number of rows that can fit in
    * a cursor window, or 0 if unknown.
    * @return The recommended start position, always less than or equal to
    * the requested row.
    * @hide
    */
   public static int cursorPickFillWindowStartPosition(
       int cursorPosition, int cursorWindowCapacity) {
      return Math.max(cursorPosition - cursorWindowCapacity / 3, 0);
   }

    /**
     * The names of the columns in the rows
     * Thread-safe.
     */
    private final String[] mColumns;

   /**
    * the sqlQuery to execute
    * Thread-safe.
    */
   private final String mSqlQuery;

   /**
    * the bindArgs for that query
    * Thread-safe.
    */
   private final Object[] mBindArgs;

   /**
    * the cancellation signal when managing cursor window
    * Thread-safe.
    */
   private final CancellationSignal mCancellationSignal;

   /**
    * A mapping of column names to column indices, to speed up lookups
    * Thread-safe.
    */
   private final Map<String, Integer> mColumnNameMap;

   /**
    * The logger for this cursor.
    * Thread-safe.
    */
   private final WebLoggerIf mWebLogger;

   /**
    * The appName.
    * Thread-safe.
    */
   private final String mAppName;

   /**
    * The session qualifier.
    * Thread-safe.
    */
   private final String mSessionQualifier;

   /**
    * The enclosing SQLiteCursor.
    * Thread-safe.
    */
   private final SQLiteCursor mEnclosingCursor;

   /************************************************************************************
    * These fields ARE NOT THREAD SAFE.
    * synchronize on this object before accessing/manipulating them.
    ************************************************************************************/

    /** The number of rows in the cursor */
    private int mCount = NO_COUNT;

    /** The number of rows that can fit in the cursor window, 0 if unknown */
    private int mCursorWindowCapacity;

    private SQLiteConnection mConnection;

   /**
    * Execute a query and provide access to its result set through a Cursor
    * interface. For a query such as: {@code SELECT name, birth, phone FROM
    * myTable WHERE ... LIMIT 1,20 ORDER BY...} the column names (name, birth,
    * phone) would be in the projection argument and everything from
    * {@code FROM} onward would be in the params argument.
    *
    *
    * @param connection
    * @param columnNames caller should NOT modify this array upon return.
    * @param sqlQuery
    * @param bindArgs caller should NOT modify this array upon return.
    * @param cancellationSignal
    */
    public SQLiteUnsafeCursor(SQLiteCursor enclosingCursor,
        SQLiteConnection connection, String[]
        columnNames, String sqlQuery,
        Object[] bindArgs, CancellationSignal cancellationSignal) {
        if (sqlQuery == null) {
            throw new IllegalArgumentException("sqlQuery cannot be null");
        }

       // remember our enclosing SQLiteCursor so we can release it from the connection.
       mEnclosingCursor = enclosingCursor;

       // we are going to hold onto the open connection...
        connection.acquireReference();
        mConnection = connection;

       //
       // The initialized values that follow are all final and thread-safe.
       //

        // mConnection will go away when we are closed, so save the logger now...
        mWebLogger = mConnection.getLogger();
        mColumns = columnNames;
        int columnCount = mColumns.length;
        HashMap<String, Integer> map = new HashMap<String, Integer>(columnCount, 1);
        for (int i = 0; i < columnCount; i++) {
           map.put(mColumns[i], i);
        }
        mColumnNameMap = map;
        mSqlQuery = sqlQuery;
        mBindArgs = bindArgs;
        mCancellationSignal = cancellationSignal;
        mAppName = connection.getAppName();
        mSessionQualifier = connection.getSessionQualifier();
    }

    public String getSql() {
      return mSqlQuery;
    }

    public void throwIfClosed() {
       if (isClosed()) {
          throw new SQLiteException("cursor is closed");
       }

       if (mConnection == null || !mConnection.isOpen()) {
          throw new SQLiteException("cursor's connection (" + mSessionQualifier + ") is closed");
       }
    }

    @Override
    public boolean onMove(int oldPosition, int newPosition) {
        // Make sure the row at newPosition is present in the window
        if (mWindow == null || newPosition < mWindow.getStartPosition() ||
                newPosition >= (mWindow.getStartPosition() + mWindow.getNumRows())) {
            fillWindow(newPosition);
        }

        return true;
    }

    @Override
    public int getCount() {
        if (mCount == NO_COUNT) {
            fillWindow(0);
        }
        return mCount;
    }

    /* 
    ** The AbstractWindowClass contains protected methods clearOrCreateWindow() and
    ** closeWindow(), which are used by the android.database.sqlite.* version of this
    ** class. But, since they are marked with "@hide", the following replacement 
    ** versions are required.
    */
    private void awc_clearOrCreateWindow(String name){
      CursorWindow win = getWindow();
      if( win==null ){
        win = new CursorWindow(name);
        setWindow(win);
      }else{
        win.clear();
      }
    }

    private void awc_closeWindow(){
      setWindow(null);
    }

    private void fillWindow(int requiredPos) {
       if (isClosed()) {
          throw new SQLiteException("cursor is closed");
       }

       if (mConnection == null || !mConnection.isOpen()) {
          throw new SQLiteException("cursor's connection (" + mSessionQualifier + ") is closed");
       }

        awc_clearOrCreateWindow(mConnection.getPath());

        try {
            if (mCount == NO_COUNT) {
                int startPos = cursorPickFillWindowStartPosition(requiredPos, 0);
                mCount = fillWindow(mWindow, startPos, requiredPos, true);
                mCursorWindowCapacity = mWindow.getNumRows();
                // mWebLogger.d(TAG, "connection:" + mSessionQualifier + " received count(*) "
                //     + "from native_fill_window: " + mCount);
            } else {
                int startPos = cursorPickFillWindowStartPosition(requiredPos,
                        mCursorWindowCapacity);
                fillWindow(mWindow, startPos, requiredPos, false);
            }
        } catch (RuntimeException ex) {
            // Close the cursor window if the query failed and therefore will
            // not produce any results.  This helps to avoid accidentally leaking
            // the cursor window if the client does not correctly handle exceptions
            // and fails to close the cursor.
            awc_closeWindow();
            throw ex;
        }
    }

   /**
    * Reads rows into a buffer.
    *
    * @param window The window to fill into
    * @param startPos The start position for filling the window.
    * @param requiredPos The position of a row that MUST be in the window.
    * If it won't fit, then the query should discard part of what it filled.
    * @param countAllRows True to count all rows that the query would
    * return regardless of whether they fit in the window.
    * @return Number of rows that were enumerated.  Might not be all rows
    * unless countAllRows is true.
    *
    * @throws SQLiteException if an error occurs.
    * @throws OperationCanceledException if the operation was canceled.
    */
   private int fillWindow(CursorWindow window, int startPos, int requiredPos, boolean
       countAllRows) {
      if ( window == null ) {
         throw new SQLiteException("window is closed");
      }

      if (isClosed()) {
         throw new SQLiteException("cursor is closed");
      }

      if (mConnection == null || !mConnection.isOpen()) {
         throw new SQLiteException("cursor's connection (" + mSessionQualifier + ") is closed");
      }

      window.acquireReference();
      try {
         int numRows = mConnection.executeForCursorWindow(mSqlQuery, mBindArgs, window, startPos,
             requiredPos, countAllRows, mCancellationSignal);
         return numRows;
      } catch (SQLiteException ex) {
         mWebLogger.e(TAG, "exception: " + ex.getMessage() + "; connection: " + mSessionQualifier
             + " query: " + mSqlQuery);
         throw ex;
      } finally {
         window.releaseReference();
      }
   }

   /**
    * This is thread-safe. It only accesses immutable (final) data.
    *
    * @param columnName
    * @return
    */
    @Override
    public int getColumnIndex(String columnName) {

        // Hack according to bug 903852
        final int periodIndex = columnName.lastIndexOf('.');
        if (periodIndex != -1) {
            Exception e = new Exception();
           mWebLogger.e(TAG, "connection:" + mSessionQualifier
               + " requesting column name with table name -- " + columnName);
           mWebLogger.printStackTrace(e);
            columnName = columnName.substring(periodIndex + 1);
        }

        Integer i = mColumnNameMap.get(columnName);
        if (i != null) {
            return i.intValue();
        } else {
            return -1;
        }
    }

   /**
    * This is thread-safe. If the caller alters the array, we will be
    * in trouble...
    *
    * @return
    */
    @Override
    public String[] getColumnNames() {
        return mColumns;
    }

    @Override
    public void deactivate() {
        super.deactivate();
    }

    @Override
    public void close() {
       super.close();

       try {
          // clear and release the CursorWindow, if any.
          setWindow(null);
       } finally {
          // clear and release the connection
          SQLiteConnection connection = null;
          connection = mConnection;
          mConnection = null;

          if (connection != null) {
             // release the cursor
             connection.releaseCursor(mEnclosingCursor);
             // release the reference to it
             connection.releaseReference();
          }
       }
    }

    @Override
    public boolean requery() {
        if (isClosed()) {
            return false;
        }

         if (mConnection == null || !mConnection.isOpen()) {
            return false;
         }

         if (mWindow != null) {
             mWindow.clear();
         }

         mPos = -1;
         mCount = NO_COUNT;

        try {
            return super.requery();
        } catch (IllegalStateException e) {
            // for backwards compatibility, just return false
           mWebLogger.w(TAG, "connection:" + mSessionQualifier + " requery() failed " + e.getMessage());
           mWebLogger.printStackTrace(e);
            return false;
        }
    }

    @Override
    public void setWindow(CursorWindow window) {
        super.setWindow(window);
        mCount = NO_COUNT;
    }

    /**
     * Release the native resources, if they haven't been released yet.
     */
    @Override
    protected void finalize() {
        try {
            // if the cursor hasn't been closed yet, close it first
           boolean shouldClose = false;
           String outstandingWork = "";
            if (mWindow != null ) {
               outstandingWork += " mWindow;";
               shouldClose = true;
            }
           if (mConnection != null) {
              outstandingWork += " mConnection";
              shouldClose = true;
           }
           if ( shouldClose ) {
              // during AndroidUnitTest testing, the directory might be
              // torn down before the finalize has completed.
              File f = new File(ODKFileUtils.getLoggingFolder(mAppName));
              if ( f.exists() && f.isDirectory() ) {
                 mWebLogger.w(TAG, "connection:" + mSessionQualifier
                      + " finalize: cursor:" + mSqlQuery + " not closed: " + outstandingWork);
              } else {
                 Log.e(TAG, "connection:" + mSessionQualifier
                     + " finalize: cursor:" + mSqlQuery + " not closed: " + outstandingWork);
              }
              close();
            }
        } finally {
            super.finalize();
        }
    }
}
