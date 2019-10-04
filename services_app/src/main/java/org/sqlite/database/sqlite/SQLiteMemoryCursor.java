/*
 * Copyright (C) 2016 University of Washington
 *
 * Extensively modified MatrixCursor that efficiently ties into sqlite C++ api.
 *
 * Copyright (C) 2007 The Android Open Source Project
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

package org.sqlite.database.sqlite;

import android.content.ContentResolver;
import android.database.CharArrayBuffer;
import android.database.ContentObservable;
import android.database.ContentObserver;
import android.database.Cursor;
import android.database.CursorIndexOutOfBoundsException;
import android.database.DataSetObservable;
import android.database.DataSetObserver;
import android.net.Uri;
import android.os.Bundle;

import org.opendatakit.logging.WebLogger;

import java.lang.ref.WeakReference;

/**
 * An immutable cursor implementation backed by an array of {@code Object}s.
 */
public class SQLiteMemoryCursor implements Cursor {

  private static final String TAG = "SQLiteMemoryCursor";
  
    static final char NULL_TYPE = 'n';
    static final char STRING_TYPE = 's';
    static final char LONG_TYPE = 'l';
    static final char DOUBLE_TYPE = 'd';
    static final char BYTEARRAY_TYPE = 'b';
    static final char OBJECT_TYPE = 'o';
    private static final String[] NO_COLUMNS = new String[0];

    private Object[] sqliteContent;
    // first row of sqliteContent
    private String[] columnNames;
    // second row of sqliteContent
    private char[] dataTypes;
    // data rows are remaining rows of sqliteContent
    private int rowCount;

    //////////////////////////////////////////////////////

    /**
     * Use {@link #getPosition()} to access this value.
     */
    private int mPos;

    /**
     * Use {@link #isClosed()} to access this value.
     */
    private boolean mClosed;

    /**
     * Do not use outside of this class.
     */
    private ContentResolver mContentResolver;

    private Uri mNotifyUri;

    private final Object mSelfObserverLock = new Object();
    private ContentObserver mSelfObserver;
    private boolean mSelfObserverRegistered;

    private final DataSetObservable mDataSetObservable = new DataSetObservable();
    private final ContentObservable mContentObservable = new ContentObservable();

    private Bundle mExtras = Bundle.EMPTY;

    /* Methods that may optionally be implemented by subclasses */

    @Override
    public int getColumnCount() {
        return getColumnNames().length;
    }

    @Override
    public void deactivate() {
        onDeactivateOrClose();
    }

    /** @hide */
    protected void onDeactivateOrClose() {
        if (mSelfObserver != null) {
            mContentResolver.unregisterContentObserver(mSelfObserver);
            mSelfObserverRegistered = false;
        }
        mDataSetObservable.notifyInvalidated();
    }

    @Override
    public boolean requery() {
        if (mSelfObserver != null && mSelfObserverRegistered == false) {
            mContentResolver.registerContentObserver(mNotifyUri, true, mSelfObserver);
            mSelfObserverRegistered = true;
        }
        mDataSetObservable.notifyChanged();
        return true;
    }

    @Override
    public boolean isClosed() {
        return mClosed;
    }

    @Override
    public void close() {
      boolean notYetClosed = !isClosed();
      
      mClosed = true;
      mContentObservable.unregisterAll();
        // release the held memory now.
        sqliteContent = null;
        columnNames = NO_COLUMNS;
        dataTypes = null;
        rowCount = 0;
        
        if ( notYetClosed ) {
          onDeactivateOrClose();
        }
    }

    @Override
    public void copyStringToBuffer(int columnIndex, CharArrayBuffer buffer) {
        // Default implementation, uses getString
        String result = getString(columnIndex);
        if (result != null) {
            char[] data = buffer.data;
            if (data == null || data.length < result.length()) {
                buffer.data = result.toCharArray();
            } else {
                result.getChars(0, result.length(), data, 0);
            }
            buffer.sizeCopied = result.length();
        } else {
            buffer.sizeCopied = 0;
        }
    }

    /* -------------------------------------------------------- */
    /* Implementation */

    @Override
    public final int getPosition() {
        return mPos;
    }

    @Override
    public final boolean moveToPosition(int position) {
        // Make sure position isn't past the end of the cursor
        final int count = getCount();
        if (position >= count) {
            mPos = count;
            return false;
        }

        // Make sure position isn't before the beginning of the cursor
        if (position < 0) {
            mPos = -1;
            return false;
        }

        // Check for no-op moves, and skip the rest of the work for them
        if (position == mPos) {
            return true;
        }

        mPos = position;
        return true;
    }

    @Override
    public final boolean move(int offset) {
        return moveToPosition(mPos + offset);
    }

    @Override
    public final boolean moveToFirst() {
        return moveToPosition(0);
    }

    @Override
    public final boolean moveToLast() {
        return moveToPosition(getCount() - 1);
    }

    @Override
    public final boolean moveToNext() {
        return moveToPosition(mPos + 1);
    }

    @Override
    public final boolean moveToPrevious() {
        return moveToPosition(mPos - 1);
    }

    @Override
    public final boolean isFirst() {
        return mPos == 0 && getCount() != 0;
    }

    @Override
    public final boolean isLast() {
        int cnt = getCount();
        return mPos == (cnt - 1) && cnt != 0;
    }

    @Override
    public final boolean isBeforeFirst() {
        if (getCount() == 0) {
            return true;
        }
        return mPos == -1;
    }

    @Override
    public final boolean isAfterLast() {
        if (getCount() == 0) {
            return true;
        }
        return mPos == getCount();
    }

    @Override
    public int getColumnIndex(String columnName) {
        // Hack according to bug 903852
        final int periodIndex = columnName.lastIndexOf('.');
        if (periodIndex != -1) {
            Exception e = new Exception();
            WebLogger.getContextLogger().e(TAG, 
                "requesting column name with table name -- " + columnName);
            WebLogger.getContextLogger().printStackTrace(e);
            columnName = columnName.substring(periodIndex + 1);
        }

        String columnNames[] = getColumnNames();
        int length = columnNames.length;
        for (int i = 0; i < length; i++) {
            if (columnNames[i].equalsIgnoreCase(columnName)) {
                return i;
            }
        }
        return -1;
    }

    @Override
    public int getColumnIndexOrThrow(String columnName) {
        final int index = getColumnIndex(columnName);
        if (index < 0) {
            throw new IllegalArgumentException("column '" + columnName + "' does not exist");
        }
        return index;
    }

    @Override
    public String getColumnName(int columnIndex) {
        return getColumnNames()[columnIndex];
    }

    @Override
    public void registerContentObserver(ContentObserver observer) {
        mContentObservable.registerObserver(observer);
    }

    @Override
    public void unregisterContentObserver(ContentObserver observer) {
        // cursor will unregister all observers when it close
        if (!mClosed) {
            mContentObservable.unregisterObserver(observer);
        }
    }

    @Override
    public void registerDataSetObserver(DataSetObserver observer) {
        mDataSetObservable.registerObserver(observer);
    }

    @Override
    public void unregisterDataSetObserver(DataSetObserver observer) {
        mDataSetObservable.unregisterObserver(observer);
    }

    /**
     * Subclasses must call this method when they finish committing updates to notify all
     * observers.
     *
     * @param selfChange
     */
    protected void onChange(boolean selfChange) {
        synchronized (mSelfObserverLock) {
            mContentObservable.dispatchChange(selfChange, null);
            if (mNotifyUri != null && selfChange) {
                mContentResolver.notifyChange(mNotifyUri, mSelfObserver);
            }
        }
    }

    /**
     * Specifies a content URI to watch for changes.
     *
     * @param cr The content resolver from the caller's context.
     * @param notifyUri The URI to watch for changes. This can be a
     * specific row URI, or a base URI for a whole class of content.
     */
    @Override
    public void setNotificationUri(ContentResolver cr, Uri notifyUri) {
        synchronized (mSelfObserverLock) {
            mNotifyUri = notifyUri;
            mContentResolver = cr;
            if (mSelfObserver != null) {
                mContentResolver.unregisterContentObserver(mSelfObserver);
            }
            mSelfObserver = new SelfContentObserver(this);
            mContentResolver.registerContentObserver(mNotifyUri, true, mSelfObserver);
            mSelfObserverRegistered = true;
        }
    }

    @Override
    public Uri getNotificationUri() {
        synchronized (mSelfObserverLock) {
            return mNotifyUri;
        }
    }

    @Override
    public boolean getWantsAllOnMoveCalls() {
        return false;
    }

    @Override
    public void setExtras(Bundle extras) {
        mExtras = (extras == null) ? Bundle.EMPTY : extras;
    }

    @Override
    public Bundle getExtras() {
        return mExtras;
    }

    @Override
    public Bundle respond(Bundle extras) {
        return Bundle.EMPTY;
    }

    /**
     * This function throws CursorIndexOutOfBoundsException if
     * the cursor position is out of bounds. Subclass implementations of
     * the get functions should call this before attempting
     * to retrieve data.
     *
     * @throws CursorIndexOutOfBoundsException
     */
    protected void checkPosition() {
        if (-1 == mPos || getCount() == mPos) {
            throw new CursorIndexOutOfBoundsException(mPos, getCount());
        }
    }

    @Override
    protected void finalize() {
        if (mSelfObserver != null && mSelfObserverRegistered == true) {
            mContentResolver.unregisterContentObserver(mSelfObserver);
        }
        try {
            if (!mClosed) close();
        } catch(Exception e) { }
    }

    /**
     * Cursors use this class to track changes others make to their URI.
     */
    protected static class SelfContentObserver extends ContentObserver {
        WeakReference<SQLiteMemoryCursor> mCursor;

        public SelfContentObserver(SQLiteMemoryCursor cursor) {
            super(null);
            mCursor = new WeakReference<SQLiteMemoryCursor>(cursor);
        }

        @Override
        public boolean deliverSelfNotifications() {
            return false;
        }

        @Override
        public void onChange(boolean selfChange) {
          SQLiteMemoryCursor cursor = mCursor.get();
            if (cursor != null) {
                cursor.onChange(false);
            }
        }
    }
    //////////////////////////////////////////////////////
    /**
     * Constructs a new cursor with the given sqliteContent array
     *
     * @param sqliteContent an array of object arrays. First row
     *                      contains column names. Second row
     *                      contains data types in column. Remaining
     *                      rows contain the data.
     */
    public SQLiteMemoryCursor(Object[] sqliteContent) {
        this.mPos = -1;
        this.sqliteContent = sqliteContent;
        this.columnNames = (String[]) sqliteContent[0];
        this.dataTypes = (char[]) sqliteContent[1];
        this.rowCount = sqliteContent.length-2;
    }

    /**
     * Gets value at the given column for the current row.
     */
    private Object get(int column) {
        if (column < 0 || column >= columnNames.length) {
            throw new CursorIndexOutOfBoundsException("Requested column: "
                    + column + ", # of columns: " +  columnNames.length);
        }
        if (getPosition() < 0) {
            throw new CursorIndexOutOfBoundsException("Before first row.");
        }
        if (getPosition() >= rowCount) {
            throw new CursorIndexOutOfBoundsException("After last row.");
        }
        Object[] row = (Object[]) sqliteContent[2+getPosition()];
        return row[column];
    }

    // AbstractCursor implementation.

    @Override
    public int getCount() {
        return rowCount;
    }

    @Override
    public String[] getColumnNames() {
        return columnNames;
    }

    @Override
    public String getString(int column) {
        Object value = get(column);
        if (value == null) return null;
        if (dataTypes[column] == STRING_TYPE) {
            return (String) value;
        }
        return value.toString();
    }

    @Override
    public short getShort(int column) {
        Object value = get(column);
        if (value == null) return 0;
        if (value instanceof Number) return ((Number) value).shortValue();
        if (dataTypes[column] == LONG_TYPE) {
            return ((Long) value).shortValue();
        } else if (dataTypes[column] == DOUBLE_TYPE) {
            return ((Double) value).shortValue();
        }
        return Short.parseShort(value.toString());
    }

    @Override
    public int getInt(int column) {
        Object value = get(column);
        if (value == null) return 0;
        if (dataTypes[column] == LONG_TYPE) {
            return ((Long) value).intValue();
        } else if (dataTypes[column] == DOUBLE_TYPE) {
            return ((Double) value).intValue();
        }
        return Integer.parseInt(value.toString());
    }

    @Override
    public long getLong(int column) {
        Object value = get(column);
        if (value == null) return 0;
        if (dataTypes[column] == LONG_TYPE) {
            return (Long) value;
        } else if (dataTypes[column] == DOUBLE_TYPE) {
            return ((Double) value).longValue();
        }
        return Long.parseLong(value.toString());
    }

    @Override
    public float getFloat(int column) {
        Object value = get(column);
        if (value == null) return 0.0f;
        if (dataTypes[column] == LONG_TYPE) {
            return ((Long) value).floatValue();
        } else if (dataTypes[column] == DOUBLE_TYPE) {
            return ((Double) value).floatValue();
        }
        return Float.parseFloat(value.toString());
    }

    @Override
    public double getDouble(int column) {
        Object value = get(column);
        if (value == null) return 0.0d;
        if (dataTypes[column] == LONG_TYPE) {
            return ((Long) value).doubleValue();
        } else if (dataTypes[column] == DOUBLE_TYPE) {
            return (Double) value;
        }
        return Double.parseDouble(value.toString());
    }

    @Override
    public byte[] getBlob(int column) {
        Object value = get(column);
        if (dataTypes[column] == BYTEARRAY_TYPE) {
            return (byte[]) value;
        }
        throw new IllegalStateException("Requesting blob when data type is not blob");
    }

    @Override
    public int getType(int column) {

        if (column < 0 || column >= columnNames.length) {
            throw new CursorIndexOutOfBoundsException("Requested column: "
                    + column + ", # of columns: " +  columnNames.length);
        }

        char type = dataTypes[column];
        if ((rowCount == 0) || (type == NULL_TYPE) ||
                (getPosition() >= 0 && getPosition() < rowCount && isNull(column)) ) {
            return Cursor.FIELD_TYPE_NULL;
        } else if ( type == STRING_TYPE ) {
            return Cursor.FIELD_TYPE_STRING;
        } else if ( type == LONG_TYPE ) {
            return Cursor.FIELD_TYPE_INTEGER;
        } else if ( type == DOUBLE_TYPE ) {
            return Cursor.FIELD_TYPE_FLOAT;
        } else if ( type == BYTEARRAY_TYPE ) {
            return Cursor.FIELD_TYPE_BLOB;
        }
        throw new IllegalStateException("Requested column: "
                + column + " has two or more data types in it");
    }

    @Override
    public boolean isNull(int column) {
        return get(column) == null;
    }
}
