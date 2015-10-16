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
import org.sqlite.database.ExtraUtils;

import android.database.AbstractWindowedCursor;
import android.database.CursorWindow;

import java.util.HashMap;
import java.util.Map;

/**
 * A Cursor implementation that exposes results from a query on a
 * {@link SQLiteDatabase}.
 *
 * SQLiteCursor is not internally synchronized so code using a SQLiteCursor from multiple
 * threads should perform its own synchronization when using the SQLiteCursor.
 */
public class SQLiteCursor extends AbstractWindowedCursor {
    static final String TAG = "SQLiteCursor";
    static final int NO_COUNT = -1;

    /** The names of the columns in the rows */
    private final String[] mColumns;

   private SQLiteDatabase mDatabase;

   /** the sqlQuery to execute */
   private final String mSqlQuery;

   /** the bindArgs for that query */
   private final Object[] mBindArgs;

   /** the cancellation signal when managing cursor window */
   private final CancellationSignal mCancellationSignal;

    /** The number of rows in the cursor */
    private int mCount = NO_COUNT;

    /** The number of rows that can fit in the cursor window, 0 if unknown */
    private int mCursorWindowCapacity;

    /** A mapping of column names to column indices, to speed up lookups */
    private Map<String, Integer> mColumnNameMap;

    /** Used to find out where a cursor was allocated in case it never got released. */
    private final Throwable mStackTrace;

   /**
    * Execute a query and provide access to its result set through a Cursor
    * interface. For a query such as: {@code SELECT name, birth, phone FROM
    * myTable WHERE ... LIMIT 1,20 ORDER BY...} the column names (name, birth,
    * phone) would be in the projection argument and everything from
    * {@code FROM} onward would be in the params argument.
    *
    *
    * @param database
    * @param columnNames
    * @param sqlQuery
    * @param bindArgs
    * @param cancellationSignal
    */
    public SQLiteCursor(
        SQLiteDatabase database, String[] columnNames,
        String sqlQuery, Object[] bindArgs, CancellationSignal cancellationSignal) {
        if (sqlQuery == null) {
            throw new IllegalArgumentException("sqlQuery cannot be null");
        }
        if (/* StrictMode.vmSqliteObjectLeaksEnabled() */ false ) {
            mStackTrace = new DatabaseObjectNotClosedException().fillInStackTrace();
        } else {
            mStackTrace = null;
        }

       // we are going to hold onto the open database...
       database.acquireReference();
        mDatabase = database;
        mColumnNameMap = null;
        mSqlQuery = sqlQuery;
        mBindArgs = bindArgs;
        mCancellationSignal = cancellationSignal;

        mColumns = columnNames;
    }

    /**
     * Get the database that this cursor is associated with.
     * @return the SQLiteDatabase that this cursor is associated with.
     */
    public SQLiteDatabase getDatabase() {
        return mDatabase;
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
        awc_clearOrCreateWindow(getDatabase().getPath());

        try {
            if (mCount == NO_COUNT) {
                int startPos = ExtraUtils.cursorPickFillWindowStartPosition(requiredPos, 0);
                mCount = fillWindow(mWindow, startPos, requiredPos, true);
                mCursorWindowCapacity = mWindow.getNumRows();
                getDatabase().getLogger().d(TAG, "received count(*) from native_fill_window: " + mCount);
            } else {
                int startPos = ExtraUtils.cursorPickFillWindowStartPosition(requiredPos,
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
   int fillWindow(CursorWindow window, int startPos, int requiredPos, boolean countAllRows) {
      window.acquireReference();
      try {
         int numRows = getDatabase().executeForCursorWindow(mSqlQuery, mBindArgs,
             window, startPos, requiredPos, countAllRows,
             mCancellationSignal);
         return numRows;
      } catch (SQLiteException ex) {
         getDatabase().getLogger().e(TAG, "exception: " + ex.getMessage() + "; query: " + mSqlQuery);
         throw ex;
      } finally {
         window.releaseReference();
      }
   }

    @Override
    public int getColumnIndex(String columnName) {
        // Create mColumnNameMap on demand
        if (mColumnNameMap == null) {
            String[] columns = mColumns;
            int columnCount = columns.length;
            HashMap<String, Integer> map = new HashMap<String, Integer>(columnCount, 1);
            for (int i = 0; i < columnCount; i++) {
                map.put(columns[i], i);
            }
            mColumnNameMap = map;
        }

        // Hack according to bug 903852
        final int periodIndex = columnName.lastIndexOf('.');
        if (periodIndex != -1) {
            Exception e = new Exception();
            getDatabase().getLogger().e(TAG, "requesting column name with table name -- " + columnName);
            getDatabase().getLogger().printStackTrace(e);
            columnName = columnName.substring(periodIndex + 1);
        }

        Integer i = mColumnNameMap.get(columnName);
        if (i != null) {
            return i.intValue();
        } else {
            return -1;
        }
    }

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

       SQLiteDatabase db = null;
       synchronized (this) {
          db = mDatabase;
          mDatabase = null;
       }
       if ( db != null ) {
          db.releaseReference();
       }
    }

    @Override
    public boolean requery() {
        if (isClosed()) {
            return false;
        }

        synchronized (this) {
            if (!mDatabase.isOpen()) {
               return false;
            }

            if (mWindow != null) {
                mWindow.clear();
            }
            mPos = -1;
            mCount = NO_COUNT;
        }

        try {
            return super.requery();
        } catch (IllegalStateException e) {
            // for backwards compatibility, just return false
            getDatabase().getLogger().w(TAG, "requery() failed " + e.getMessage());
            getDatabase().getLogger().printStackTrace(e);
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
            if (mWindow != null) {
                    /*
                if (mStackTrace != null) {
                    String sql = mQuery.getSql();
                    int len = sql.length();
                    StrictMode.onSqliteObjectLeaked(
                        "Finalizing a Cursor that has not been deactivated or closed. " +
                        "database = " + mQuery.getConnection().getLabel() +
                        ", table = " + mEditTable +
                        ", query = " + sql.substring(0, (len > 1000) ? 1000 : len),
                        mStackTrace);
                }
                */
                close();
            }
        } finally {
            super.finalize();
        }
    }
}
