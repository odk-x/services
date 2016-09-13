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

package org.sqlite.database.sqlite;

import android.annotation.SuppressLint;
import android.content.ContentResolver;
import android.database.*;
import android.net.Uri;
import android.os.Bundle;
import android.os.CancellationSignal;

/**
 * A wrapper class that provides synchronized access to a #SQLiteUnsafeCursor
 *
 * @author mitchellsundt@gmail.com
 */
public class SQLiteCursor implements CrossProcessCursor {
   private static final String TAG = "SQLiteCursor";

   private final SQLiteUnsafeCursor impl;

   public SQLiteCursor(SQLiteConnection connection, String[] columnNames, String sqlQuery,
       Object[] bindArgs, CancellationSignal cancellationSignal) {
      impl = new SQLiteUnsafeCursor(this, connection, columnNames, sqlQuery, bindArgs,
                                    cancellationSignal);
   }

   @Override public CursorWindow getWindow() {
      synchronized (impl) {
         impl.throwIfClosed();
         // verified that CursorWindow does not contain
         // any information about the sourcing impl.
         return impl.getWindow();
      }
   }

   @Override public void fillWindow(int position, CursorWindow window) {
      synchronized (impl) {
         impl.throwIfClosed();
         // verified that CursorWindow does not contain
         // any information about the sourcing impl.
         impl.fillWindow(position, window);
      }
   }

   @Override public boolean onMove(int oldPosition, int newPosition) {
      synchronized (impl) {
         impl.throwIfClosed();
         return impl.onMove(oldPosition, newPosition);
      }
   }

   @Override public int getCount() {
      synchronized (impl) {
         impl.throwIfClosed();
         return impl.getCount();
      }
   }

   @Override public int getPosition() {
      synchronized (impl) {
         impl.throwIfClosed();
         return impl.getPosition();
      }
   }

   @Override public boolean move(int offset) {
      synchronized (impl) {
         impl.throwIfClosed();
         return impl.move(offset);
      }
   }

   @Override public boolean moveToPosition(int position) {
      synchronized (impl) {
         impl.throwIfClosed();
         return impl.moveToPosition(position);
      }
   }

   @Override public boolean moveToFirst() {
      synchronized (impl) {
         impl.throwIfClosed();
         return impl.moveToFirst();
      }
   }

   @Override public boolean moveToLast() {
      synchronized (impl) {
         impl.throwIfClosed();
         return impl.moveToLast();
      }
   }

   @Override public boolean moveToNext() {
      synchronized (impl) {
         impl.throwIfClosed();
         return impl.moveToNext();
      }
   }

   @Override public boolean moveToPrevious() {
      synchronized (impl) {
         impl.throwIfClosed();
         return impl.moveToPrevious();
      }
   }

   @Override public boolean isFirst() {
      synchronized (impl) {
         impl.throwIfClosed();
         return impl.isFirst();
      }
   }

   @Override public boolean isLast() {
      synchronized (impl) {
         impl.throwIfClosed();
         return impl.isLast();
      }
   }

   @Override public boolean isBeforeFirst() {
      synchronized (impl) {
         impl.throwIfClosed();
         return impl.isBeforeFirst();
      }
   }

   @Override public boolean isAfterLast() {
      synchronized (impl) {
         impl.throwIfClosed();
         return impl.isAfterLast();
      }
   }

   public String getSql() {
      return impl.getSql();
   }

   @Override public int getColumnIndex(String columnName) {
      return impl.getColumnIndex(columnName);
   }

   @Override public int getColumnIndexOrThrow(String columnName) throws IllegalArgumentException {
      return impl.getColumnIndexOrThrow(columnName);
   }

   @Override public String getColumnName(int columnIndex) {
      return impl.getColumnName(columnIndex);
   }

   @Override public String[] getColumnNames() {
      return impl.getColumnNames();
   }

   @Override public int getColumnCount() {
      return impl.getColumnCount();
   }

   @Override public byte[] getBlob(int columnIndex) {
      synchronized (impl) {
         impl.throwIfClosed();
         return impl.getBlob(columnIndex);
      }
   }

   @Override public String getString(int columnIndex) {
      synchronized (impl) {
         impl.throwIfClosed();
         return impl.getString(columnIndex);
      }
   }

   @Override public void copyStringToBuffer(int columnIndex, CharArrayBuffer buffer) {
      synchronized (impl) {
         impl.throwIfClosed();
         impl.copyStringToBuffer(columnIndex, buffer);
      }
   }

   @Override public short getShort(int columnIndex) {
      synchronized (impl) {
         impl.throwIfClosed();
         return impl.getShort(columnIndex);
      }
   }

   @Override public int getInt(int columnIndex) {
      synchronized (impl) {
         impl.throwIfClosed();
         return impl.getInt(columnIndex);
      }
   }

   @Override public long getLong(int columnIndex) {
      synchronized (impl) {
         impl.throwIfClosed();
         return impl.getLong(columnIndex);
      }
   }

   @Override public float getFloat(int columnIndex) {
      synchronized (impl) {
         impl.throwIfClosed();
         return impl.getFloat(columnIndex);
      }
   }

   @Override public double getDouble(int columnIndex) {
      synchronized (impl) {
         impl.throwIfClosed();
         return impl.getDouble(columnIndex);
      }
   }

   @Override public int getType(int columnIndex) {
      synchronized (impl) {
         impl.throwIfClosed();
         return impl.getType(columnIndex);
      }
   }

   @Override public boolean isNull(int columnIndex) {
      synchronized (impl) {
         impl.throwIfClosed();
         return impl.isNull(columnIndex);
      }
   }

   @Override
   @SuppressLint("Deprecation")
   public void deactivate() {
      synchronized (impl) {
         impl.deactivate();
      }
   }

   @Override
   @SuppressLint("Deprecation")
   public boolean requery() {
      synchronized (impl) {
         return impl.requery();
      }
   }

   @Override public void close() {
      synchronized (impl) {
         impl.close();
      }
   }

   @Override public boolean isClosed() {
      synchronized (impl) {
         return impl.isClosed();
      }
   }

   @Override public void registerContentObserver(ContentObserver observer) {
      synchronized (impl) {
         // TODO: wrap this to protect against implementation changes?
         // TODO: or verify that this is safe across several API levels?
         // ok at API 19
         impl.registerContentObserver(observer);
      }
   }

   @Override public void unregisterContentObserver(ContentObserver observer) {
      synchronized (impl) {
         // TODO: wrap this to protect against implementation changes?
         // TODO: or verify that this is safe across several API levels?
         // ok at API 19
         impl.unregisterContentObserver(observer);
      }
   }

   @Override public void registerDataSetObserver(DataSetObserver observer) {
      synchronized (impl) {
         // these are OK -- the impl invokes them, but doesn't pass any identifying info.
         impl.registerDataSetObserver(observer);
      }
   }

   @Override public void unregisterDataSetObserver(DataSetObserver observer) {
      synchronized (impl) {
         // these are OK -- the impl invokes them, but doesn't pass any identifying info.
         impl.unregisterDataSetObserver(observer);
      }
   }

   @Override public void setNotificationUri(ContentResolver cr, Uri uri) {
      synchronized (impl) {
         impl.setNotificationUri(cr, uri);
      }
   }

   @Override public Uri getNotificationUri() {
      synchronized (impl) {
         return impl.getNotificationUri();
      }
   }

   @Override public boolean getWantsAllOnMoveCalls() {
      synchronized (impl) {
         return impl.getWantsAllOnMoveCalls();
      }
   }

   @Override public Bundle getExtras() {
      synchronized (impl) {
         return impl.getExtras();
      }
   }

   @Override public Bundle respond(Bundle extras) {
      synchronized (impl) {
         return impl.respond(extras);
      }
   }
}
