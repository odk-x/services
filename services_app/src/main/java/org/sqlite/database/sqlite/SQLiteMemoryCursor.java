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

import android.database.AbstractCursor;
import android.database.Cursor;
import android.database.CursorIndexOutOfBoundsException;

/**
 * An immutable cursor implementation backed by an array of {@code Object}s.
 */
public class SQLiteMemoryCursor extends AbstractCursor {

    private static final char NULL_TYPE = 'n';
    private static final char STRING_TYPE = 's';
    private static final char LONG_TYPE = 'l';
    private static final char DOUBLE_TYPE = 'd';
    private static final char BYTEARRAY_TYPE = 'b';
    private static final char OBJECT_TYPE = 'o';
    private static final String[] NO_COLUMNS = new String[0];

    private Object[] sqliteContent;
    // first row of sqliteContent
    private String[] columnNames;
    // second row of sqliteContent
    private char[] dataTypes;
    // data rows are remaining rows of sqliteContent
    private int rowCount;

    /**
     * Constructs a new cursor with the given sqliteContent array
     *
     * @param sqliteContent an array of object arrays. First row
     *                      contains column names. Second row
     *                      contains data types in column. Remaining
     *                      rows contain the data.
     */
    public SQLiteMemoryCursor(Object[] sqliteContent) {
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
        if (mPos < 0) {
            throw new CursorIndexOutOfBoundsException("Before first row.");
        }
        if (mPos >= rowCount) {
            throw new CursorIndexOutOfBoundsException("After last row.");
        }
        Object[] row = (Object[]) sqliteContent[2+mPos];
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
                (mPos >= 0 && mPos < rowCount && isNull(column)) ) {
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

    @Override
    public void close() {
        if ( !isClosed() ) {
            super.close();
        }
        // release the held memory now.
        sqliteContent = null;
        columnNames = NO_COLUMNS;
        dataTypes = null;
        rowCount = 0;
    }
}
