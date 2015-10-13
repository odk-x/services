/*
 * Copyright (C) 2014 University of Washington
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.opendatakit.common.android.utilities;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import android.annotation.SuppressLint;
import android.database.Cursor;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;

public class ODKCursorUtils {

  private static final String t = "ODKCursorUtils";

  /**
   * values that can be returned from getTableHealth()
   */
  public static final int TABLE_HEALTH_IS_CLEAN = 0;
  public static final int TABLE_HEALTH_HAS_CONFLICTS = 1;
  public static final int TABLE_HEALTH_HAS_CHECKPOINTS = 2;
  public static final int TABLE_HEALTH_HAS_CHECKPOINTS_AND_CONFLICTS = 3;

  public static final String DEFAULT_LOCALE = "default";
  public static final String DEFAULT_CREATOR = "anonymous";

  /**
   * Return the data stored in the cursor at the given index and given position
   * (ie the given row which the cursor is currently on) as null OR a String.
   * <p>
   * NB: Currently only checks for Strings, long, int, and double.
   *
   * @param c
   * @param i
   * @return
   */
  @SuppressLint("NewApi")
  public static String getIndexAsString(Cursor c, int i) {
    // If you add additional return types here be sure to modify the javadoc.
    if (i == -1)
      return null;
    if (c.isNull(i)) {
      return null;
    }
    switch (c.getType(i)) {
    case Cursor.FIELD_TYPE_STRING:
      return c.getString(i);
    case Cursor.FIELD_TYPE_FLOAT: {
       // the static version of this seems to have problems...
       Double d = c.getDouble(i);
       String v = d.toString();
       return v;
    }
    case Cursor.FIELD_TYPE_INTEGER: {
       // the static version of this seems to have problems...
       Long l = c.getLong(i);
       String v = l.toString();
       return v;
    }
    case Cursor.FIELD_TYPE_NULL:
      return c.getString(i);
    default:
    case Cursor.FIELD_TYPE_BLOB:
      throw new IllegalStateException("Unexpected data type in SQLite table");
    }
  }

  /**
   * Retrieve the data type of the [i] field in the Cursor.
   * 
   * @param c
   * @param i
   * @return
   */
  public static final Class<?> getIndexDataType(Cursor c, int i) {
    switch (c.getType(i)) {
    case Cursor.FIELD_TYPE_STRING:
      return String.class;
    case Cursor.FIELD_TYPE_FLOAT:
      return Double.class;
    case Cursor.FIELD_TYPE_INTEGER:
      return Long.class;
    case Cursor.FIELD_TYPE_NULL:
      return String.class;
    default:
    case Cursor.FIELD_TYPE_BLOB:
      throw new IllegalStateException("Unexpected data type in SQLite table");
    }
  }

  /**
   * Return the data stored in the cursor at the given index and given position
   * (ie the given row which the cursor is currently on) as null OR whatever
   * data type it is.
   * <p>
   * This does not actually convert data types from one type to the other.
   * Instead, it safely preserves null values and returns boxed data values. If
   * you specify ArrayList or HashMap, it JSON deserializes the value into one
   * of those.
   *
   * @param c
   * @param clazz
   * @param i
   * @return
   */
  @SuppressLint("NewApi")
  public static final <T> T getIndexAsType(Cursor c, Class<T> clazz, int i) {
    // If you add additional return types here be sure to modify the javadoc.
    try {
      if (i == -1)
        return null;
      if (c.isNull(i)) {
        return null;
      }
      if (clazz == Long.class) {
        Long l = c.getLong(i);
        return (T) l;
      } else if (clazz == Integer.class) {
        Integer l = c.getInt(i);
        return (T) l;
      } else if (clazz == Double.class) {
        Double d = c.getDouble(i);
        return (T) d;
      } else if (clazz == String.class) {
        String str = c.getString(i);
        return (T) str;
      } else if (clazz == Boolean.class) {
        // stored as integers
        Integer l = c.getInt(i);
        return (T) Boolean.valueOf(l != 0);
      } else if (clazz == ArrayList.class) {
        // json deserialization of an array
        String str = c.getString(i);
        return (T) ODKFileUtils.mapper.readValue(str, ArrayList.class);
      } else if (clazz == HashMap.class) {
        // json deserialization of an object
        String str = c.getString(i);
        return (T) ODKFileUtils.mapper.readValue(str, HashMap.class);
      } else {
        throw new IllegalStateException("Unexpected data type in SQLite table");
      }
    } catch (ClassCastException e) {
      e.printStackTrace();
      throw new IllegalStateException("Unexpected data type conversion failure " + e.toString()
          + " in SQLite table ");
    } catch (JsonParseException e) {
      e.printStackTrace();
      throw new IllegalStateException("Unexpected data type conversion failure " + e.toString()
          + " on SQLite table");
    } catch (JsonMappingException e) {
      e.printStackTrace();
      throw new IllegalStateException("Unexpected data type conversion failure " + e.toString()
          + " on SQLite table");
    } catch (IOException e) {
      e.printStackTrace();
      throw new IllegalStateException("Unexpected data type conversion failure " + e.toString()
          + " on SQLite table");
    }
  }
}
