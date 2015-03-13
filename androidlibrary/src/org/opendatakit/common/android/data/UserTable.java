/*
 * Copyright (C) 2012 University of Washington
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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

import org.opendatakit.aggregate.odktables.rest.ElementDataType;
import org.opendatakit.aggregate.odktables.rest.ElementType;
import org.opendatakit.common.android.provider.DataTableColumns;
import org.opendatakit.common.android.utilities.DataUtil;
import org.opendatakit.common.android.utilities.ODKFileUtils;
import org.opendatakit.common.android.utilities.WebLogger;

import android.content.Context;
import android.os.Parcel;
import android.os.Parcelable;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;

/**
 * This class represents a table. This can be conceptualized as a list of rows.
 * Each row comprises the user-defined columns, or data, as well as the
 * ODKTables-specified metadata.
 * <p>
 * This should be considered an immutable class.
 *
 * @author unknown
 * @author sudar.sam@gmail.com
 *
 */
public class UserTable implements Parcelable {

  private static final String TAG = UserTable.class.getSimpleName();

  private final OrderedColumns mColumnDefns;
  private final ArrayList<Row> mRows;
  /**
   * The {@link TableProperties} associated with this table. Included so that
   * more intelligent things can be done with regards to interpretation of type.
   */
//  private final TableProperties mTp;
  private final String mSqlWhereClause;
  private final String[] mSqlSelectionArgs;
  private final String[] mSqlGroupByArgs;
  private final String mSqlHavingClause;
  private final String mSqlOrderByElementKey;
  private final String mSqlOrderByDirection;
  
  private final String[] mAdminColumnOrder;

  private final Map<String,Integer> mElementKeyToIndex;
  private final String[] mElementKeyForIndex;

  private final DataUtil du;

  public UserTable(UserTable table, List<Integer> indexes) {
    this.mColumnDefns = table.mColumnDefns;
    du = table.du;
    mRows = new ArrayList<Row>(indexes.size());
    for (int i = 0 ; i < indexes.size(); ++i) {
      Row r = table.getRowAtIndex(indexes.get(i));
      mRows.add(r);
    }
    this.mSqlWhereClause = table.mSqlWhereClause;
    this.mSqlSelectionArgs = table.mSqlSelectionArgs;
    this.mSqlGroupByArgs = table.mSqlGroupByArgs;
    this.mSqlHavingClause = table.mSqlHavingClause;
    this.mSqlOrderByElementKey = table.mSqlOrderByElementKey;
    this.mSqlOrderByDirection = table.mSqlOrderByDirection;
    this.mAdminColumnOrder = table.mAdminColumnOrder;
    this.mElementKeyToIndex = table.mElementKeyToIndex;
    this.mElementKeyForIndex = table.mElementKeyForIndex;
  }

  public UserTable(OrderedColumns columnDefns,
      String sqlWhereClause, String[] sqlSelectionArgs,
      String[] sqlGroupByArgs, String sqlHavingClause,
      String sqlOrderByElementKey, String sqlOrderByDirection,
      String[] adminColumnOrder, HashMap<String, Integer> elementKeyToIndex,
      String[] elementKeyForIndex, Integer rowCount ) {
    this.mColumnDefns = columnDefns;
    du = new DataUtil(Locale.ENGLISH, TimeZone.getDefault());

    this.mSqlWhereClause = sqlWhereClause;
    this.mSqlSelectionArgs = sqlSelectionArgs;
    this.mSqlGroupByArgs = sqlGroupByArgs;
    this.mSqlHavingClause = sqlHavingClause;
    this.mSqlOrderByElementKey = sqlOrderByElementKey;
    this.mSqlOrderByDirection = sqlOrderByDirection;
    this.mAdminColumnOrder = adminColumnOrder;
    this.mElementKeyToIndex = elementKeyToIndex;
    this.mElementKeyForIndex = elementKeyForIndex;
    if ( rowCount != null ) {
      mRows = new ArrayList<Row>(rowCount);
    } else {
      mRows = new ArrayList<Row>();
    }
  }
  
  public void addRow(Row row) {
    mRows.add(row);
  }

  public String getAppName() {
    return mColumnDefns.getAppName();
  }
  
  public String getTableId() {
    return mColumnDefns.getTableId();
  }
  
  public OrderedColumns getColumnDefinitions() {
    return mColumnDefns;
  }
  
  public Row getRowAtIndex(int index) {
    return this.mRows.get(index);
  }

  public Integer getColumnIndexOfElementKey(String elementKey) {
    return this.mElementKeyToIndex.get(elementKey);
  }

  public String getElementKey(int colNum) {
    return this.mElementKeyForIndex[colNum];
  }

  public String getWhereClause() {
    return mSqlWhereClause;
  }

  public String[] getSelectionArgs() {
    return mSqlSelectionArgs.clone();
  }

  /**
   * True if the table has a group-by clause in its query
   *
   * @return
   */
  public boolean isGroupedBy() {
    return mSqlGroupByArgs != null && mSqlGroupByArgs.length != 0;
  }

  public String[] getGroupByArgs() {
    return mSqlGroupByArgs.clone();
  }

  public String getHavingClause() {
    return mSqlHavingClause;
  }

  public String getOrderByElementKey() {
    return mSqlOrderByElementKey;
  }

  public String getOrderByDirection() {
    return mSqlOrderByDirection;
  }

  public int getWidth() {
    return mElementKeyForIndex.length;
  }

  public int getNumberOfRows() {
    return this.mRows.size();
  }

  public boolean hasCheckpointRows() {
    for ( Row row : mRows ) {
      String type = row.getRawDataOrMetadataByElementKey(DataTableColumns.SAVEPOINT_TYPE);
      if ( type == null || type.length() == 0 ) {
        return true;
      }
    }
    return false;
  }

  public boolean hasConflictRows() {
    for ( Row row : mRows ) {
      String conflictType = row.getRawDataOrMetadataByElementKey(DataTableColumns.CONFLICT_TYPE);
      if ( conflictType != null && conflictType.length() != 0 ) {
        return true;
      }
    }
    return false;
  }
  /**
   * Scan the rowIds to get the row number. As the rowIds are not sorted, this
   * is a potentially expensive operation, scanning the entire array, as well as
   * the cost of checking String equality. Should be used only when necessary.
   * <p>
   * Return -1 if the row Id is not found.
   *
   * @param rowId
   * @return
   */
  public int getRowNumFromId(String rowId) {
    for (int i = 0; i < this.mRows.size(); i++) {
      if (this.mRows.get(i).mRowId.equals(rowId)) {
        return i;
      }
    }
    return -1;
  }

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
  public static final class Row implements Parcelable {

    private final UserTable mUserTable;
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
    public Row(UserTable userTable, String rowId, String[] rowData) {
      this.mUserTable = userTable;
      this.mRowId = rowId;
      this.mRowData = rowData;
    }

    /**
     * Return the id of this row.
     * @return
     */
    public String getRowId() {
      return this.mRowId;
    }

    /**
     * Return the String representing the contents of the column represented by
     * the passed in elementKey. This can be either the element key of a
     * user-defined column or a ODKTabes-specified metadata column.
     * <p>
     * Null values are returned as nulls.
     *
     * @param elementKey
     *          elementKey of data or metadata column
     * @return String representation of contents of column. Null values are
     *         returned as null.
     */
    public String getRawDataOrMetadataByElementKey(String elementKey) {
      String result;
      Integer cell = mUserTable.mElementKeyToIndex.get(elementKey);
      if ( cell == null ) {
        WebLogger.getLogger(mUserTable.mColumnDefns.getAppName()).e(TAG, "elementKey [" + elementKey + "] was not found in table");
        return null;
      }
      result = this.mRowData[cell];
      if (result == null) {
        return null;
      }
      return result;
    }

    /**
     * Return the data stored in the cursor at the given index and given position
     * (ie the given row which the cursor is currently on) as null OR whatever
     * data type it is.
     * <p>This does not actually convert data types from one type
     * to the other. Instead, it safely preserves null values and returns boxed
     * data values. If you specify ArrayList or HashMap, it JSON deserializes
     * the value into one of those.
     *
     * @param c
     * @param clazz
     * @param i
     * @return
     */
    @SuppressWarnings("unchecked")
    public final <T> T getRawDataType(String elementKey, Class<T> clazz) {
      // If you add additional return types here be sure to modify the javadoc.
      try {
        String value = getRawDataOrMetadataByElementKey(elementKey);
        if (value == null) {
          return null;
        }
        if ( clazz == Long.class ) {
          Long l = Long.parseLong(value);
          return (T) l;
        } else if ( clazz == Integer.class ) {
          Integer l = Integer.parseInt(value);
          return (T) l;
        } else if ( clazz == Double.class ) {
          Double d = Double.parseDouble(value);
          return (T) d;
        } else if ( clazz == String.class ) {
          return (T) value;
        } else if ( clazz == Boolean.class ) {
          return (T) Boolean.valueOf(value);
        } else if ( clazz == ArrayList.class ) {
          // json deserialization of an array
          return (T) ODKFileUtils.mapper.readValue(value, ArrayList.class);
        } else if ( clazz == HashMap.class ) {
          // json deserialization of an object
          return (T) ODKFileUtils.mapper.readValue(value, HashMap.class);
        } else {
          throw new IllegalStateException("Unexpected data type in SQLite table");
        }
      } catch (ClassCastException e) {
        e.printStackTrace();
        throw new IllegalStateException("Unexpected data type conversion failure " + e.toString() + " in SQLite table ");
      } catch (JsonParseException e) {
        e.printStackTrace();
        throw new IllegalStateException("Unexpected data type conversion failure " + e.toString() + " on SQLite table");
      } catch (JsonMappingException e) {
        e.printStackTrace();
        throw new IllegalStateException("Unexpected data type conversion failure " + e.toString() + " on SQLite table");
      } catch (IOException e) {
        e.printStackTrace();
        throw new IllegalStateException("Unexpected data type conversion failure " + e.toString() + " on SQLite table");
      }
    }

    public String getDisplayTextOfData(Context context, ElementType type, String elementKey, boolean showErrorText) {
      // TODO: share processing with CollectUtil.writeRowDataToBeEdited(...)
      String raw = getRawDataOrMetadataByElementKey(elementKey);
      if ( raw == null ) {
        return null;
      }
      if ( raw.length() == 0 ) {
        throw new IllegalArgumentException("unexpected zero-length string in database! " + elementKey);
      }
      
      if ( type == null ) {
        return raw;
      }
      if ( type.getDataType() == ElementDataType.rowpath ) {
        File f = ODKFileUtils.getAsFile(mUserTable.getAppName(), raw);
        return f.getName();
      } else if ( type.getDataType() == ElementDataType.configpath ) {
        return raw;
      } else if ( type.getDataType() == ElementDataType.number &&
                  raw.indexOf('.') != -1 ) {
        // trim trailing zeros on numbers (leaving the last one)
        int lnz = raw.length()-1;
        while ( lnz > 0 && raw.charAt(lnz) == '0' ) {
          lnz--;
        }
        if ( lnz >= raw.length()-2 ) {
          // ended in non-zero or x0
          return raw;
        } else {
          // ended in 0...0
          return raw.substring(0, lnz+2);
        }
      } else {
        return raw;
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
      if ( mRowData == null ) {
        out.writeInt(0);
        String[] emptyString = {};
        out.writeStringArray(emptyString);
      } else {
        out.writeInt(mRowData.length);
        out.writeStringArray(mRowData);
      }
    }
    
    public Row(UserTable userTable, Parcel in) {
      this.mUserTable = userTable;
      this.mRowId = in.readString();
      int count;
      count = in.readInt();
      this.mRowData = new String[count];
      in.readStringArray(mRowData);
    }
  }
  

  @Override
  public int describeContents() {
    return 0;
  }

  @Override
  public void writeToParcel(Parcel out, int flags) {
    String[] emptyString = {};
    out.writeString(mSqlWhereClause);
    if ( mSqlSelectionArgs == null ) {
      out.writeInt(0);
      out.writeStringArray(emptyString);
    } else {
      out.writeInt(mSqlSelectionArgs.length);
      out.writeStringArray(mSqlSelectionArgs);
    }
    if ( mSqlGroupByArgs == null ) {
      out.writeInt(0);
      out.writeStringArray(emptyString);
    } else {
      out.writeInt(mSqlGroupByArgs.length);
      out.writeStringArray(mSqlGroupByArgs);
    }
    out.writeString(mSqlHavingClause);
    out.writeString(mSqlOrderByElementKey);
    out.writeString(mSqlOrderByDirection);
    this.mColumnDefns.writeToParcel(out, flags);
    out.writeInt(mAdminColumnOrder.length);
    out.writeStringArray(mAdminColumnOrder);
    out.writeInt(mRows.size());
    for ( Row r : mRows ) {
      r.writeToParcel(out, flags);
    }
  }
  
  public UserTable(Parcel in) {
    this.mSqlWhereClause = in.readString();
    int count;
    count = in.readInt();
    this.mSqlSelectionArgs = new String[count];
    in.readStringArray(mSqlSelectionArgs);
    count = in.readInt();
    this.mSqlGroupByArgs = new String[count];
    in.readStringArray(mSqlGroupByArgs);
    this.mSqlHavingClause = in.readString();
    this.mSqlOrderByElementKey = in.readString();
    this.mSqlOrderByDirection = in.readString();
    this.mColumnDefns = new OrderedColumns(in);
    count = in.readInt();
    this.mAdminColumnOrder = new String[count];
    in.readStringArray(mAdminColumnOrder);
    // count of rows...
    count = in.readInt();
    mRows = new ArrayList<Row>(count);
    for ( int j = 0 ; j < count ; ++j ) {
      Row r = new Row(this, in);
      mRows.add(r);
    }
    
    du = new DataUtil(Locale.ENGLISH, TimeZone.getDefault());

    // These maps will map the element key to the corresponding index in
    // either data or metadata. If the user has defined a column with the
    // element key _my_data, and this column is at index 5 in the data
    // array, dataKeyToIndex would then have a mapping of _my_data:5.
    // The sync_state column, if present at index 7, would have a mapping
    // in metadataKeyToIndex of sync_state:7.
    mElementKeyToIndex = new HashMap<String, Integer>();
    List<String> userColumnOrder = mColumnDefns.getRetentionColumnNames();
    mElementKeyForIndex = new String[userColumnOrder.size()+mAdminColumnOrder.length];
    int i = 0;
    for (i = 0; i < userColumnOrder.size(); i++) {
      String elementKey = userColumnOrder.get(i);
      mElementKeyForIndex[i] = elementKey;
      mElementKeyToIndex.put(elementKey, i);
    }

    for (int j = 0; j < mAdminColumnOrder.length; j++) {
      // TODO: problem is here. unclear how to best get just the
      // metadata in here. hmm.
      String elementKey = mAdminColumnOrder[j];
      mElementKeyForIndex[i+j] = elementKey;
      mElementKeyToIndex.put(elementKey, i+j);
    }
  }

  public static final Parcelable.Creator<UserTable> CREATOR
          = new Parcelable.Creator<UserTable>() {
      public UserTable createFromParcel(Parcel in) {
          return new UserTable(in);
      }

      public UserTable[] newArray(int size) {
          return new UserTable[size];
      }
  };


}
