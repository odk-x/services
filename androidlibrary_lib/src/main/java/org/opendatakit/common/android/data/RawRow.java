/*
 * Copyright (C) 2015 University of Washington
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
package org.opendatakit.common.android.data;

import android.os.Parcel;
import android.os.Parcelable;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import org.opendatakit.common.android.utilities.ODKFileUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;

/**
 * This represents a single row of data in a table.
 *
 * @author sudar.sam@gmail.com
 *
 */
/*
 * This class is final to try and reduce overhead. As final there is no
 * extended-class pointer. Not positive this is really a thing, need to
 * investigate. Nothing harmed by finalizing, though.
 */
public final class RawRow implements Parcelable {

  private final RawUserTable mUserTable;
  /**
   * The id of the row.
   */
  private final String mRowId;

  /**
   * Holds a mix of user-data and meta-data of the row
   */
  private final String[] mRowData;

  /**
   * Construct the row.
   *
   * @param rowId
   * @param rowData
   *          the combined set of data and metadata in the row.
   */
  public RawRow(RawUserTable userTable, String rowId, String[] rowData) {
    this.mUserTable = userTable;
    this.mRowId = rowId;
    this.mRowData = rowData;
  }

  /**
   * Return the id of this row.
   *
   * @return
   */
  public String getRowId() {
    return this.mRowId;
  }

  /**
   * Return the String representing the contents of the cellIndex'th column
   * <p>
   * Null values are returned as nulls.
   *
   * @param cellIndex
   *          cellIndex of data or metadata column (0..nCol-1)
   * @return String representation of contents of column. Null values are
   *         returned as null. Note that boolean values are reported as "1" or "0"
   */
  public String getRawDataOrMetadataByIndex(int cellIndex) {
    String result;
    result = this.mRowData[cellIndex];
    if (result == null) {
      return null;
    }
    return result;
  }

  /**
   * Return the data stored in the cursor at the given cellIndex column position
   * as null OR whatever data type it is.
   * <p>
   * This does not actually convert data types from one type to the other.
   * Instead, it safely preserves null values and returns boxed data values.
   * If you specify ArrayList or HashMap, it JSON deserializes the value into
   * one of those.
   *
   * @param cellIndex
   *          cellIndex of data or metadata column (0..nCol-1)
   * @param clazz
   * @return
   */
  @SuppressWarnings("unchecked")
  public final <T> T getRawDataType(int cellIndex, Class<T> clazz) {
    // If you add additional return types here be sure to modify the javadoc.
    try {
      String value = getRawDataOrMetadataByIndex(cellIndex);
      if (value == null) {
        return null;
      }
      if (clazz == Long.class) {
        Long l = Long.parseLong(value);
        return (T) (Long) l;
      } else if (clazz == Integer.class) {
        Integer l = Integer.parseInt(value);
        return (T) (Integer) l;
      } else if (clazz == Double.class) {
        Double d = Double.parseDouble(value);
        return (T) (Double) d;
      } else if (clazz == String.class) {
        return (T) (String) value;
      } else if (clazz == Boolean.class) {
        // booleans are stored as integer 1 or 0 in user tables.
        return (T) (Boolean) Boolean.valueOf(!value.equals("0"));
      } else if (clazz == ArrayList.class) {
        // json deserialization of an array
        return (T) ODKFileUtils.mapper.readValue(value, ArrayList.class);
      } else if (clazz == HashMap.class) {
        // json deserialization of an object
        return (T) ODKFileUtils.mapper.readValue(value, HashMap.class);
      } else if (clazz == TreeMap.class) {
        // json deserialization of an object
        return (T) ODKFileUtils.mapper.readValue(value, TreeMap.class);
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

  @Override
  public int hashCode() {
    final int PRIME = 31;
    int result = 1;
    result = result * PRIME + this.mRowId.hashCode();
    result = result * PRIME + this.mRowData.hashCode();
    return result;
  }

  @Override
  public int describeContents() {
    return 0;
  }

  @Override
  public void writeToParcel(Parcel out, int flags) {
    out.writeString(mRowId);
    if (mRowData == null) {
      out.writeInt(0);
      String[] emptyString = {};
      out.writeStringArray(emptyString);
    } else {
      out.writeInt(mRowData.length);
      out.writeStringArray(mRowData);
    }
  }

  public RawRow(RawUserTable userTable, Parcel in) {
    this.mUserTable = userTable;
    this.mRowId = in.readString();
    int count;
    count = in.readInt();
    this.mRowData = new String[count];
    in.readStringArray(mRowData);
  }

  /**
   * The CREATOR and this constructor are not used.
   * Row is constructed within the UserTable parcel context.
   *
   * @param in
   */
  public RawRow(Parcel in) {
    throw new IllegalStateException("never used");
  }

  /**
   * Declared to appease lint
   */
  public static final Creator<RawRow> CREATOR = new Creator<RawRow>() {
    public RawRow createFromParcel(Parcel in) {
      return new RawRow(in);
    }

    public RawRow[] newArray(int size) {
      return new RawRow[size];
    }
  };
}