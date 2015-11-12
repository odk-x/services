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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.opendatakit.common.android.provider.DataTableColumns;

import android.os.Parcel;
import android.os.Parcelable;

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

  static final String TAG = UserTable.class.getSimpleName();

  private final OrderedColumns mColumnDefns;
  private final ArrayList<Row> mRows;

  private final String mSqlWhereClause;
  private final String[] mSqlSelectionArgs;
  private final String[] mSqlGroupByArgs;
  private final String mSqlHavingClause;
  private final String mSqlOrderByElementKey;
  private final String mSqlOrderByDirection;

  private final String[] mAdminColumnOrder;

  private final Map<String, Integer> mElementKeyToIndex;
  private final String[] mElementKeyForIndex;

  public UserTable(UserTable table, List<Integer> indexes) {
    this.mColumnDefns = table.mColumnDefns;
    mRows = new ArrayList<Row>(indexes.size());
    for (int i = 0; i < indexes.size(); ++i) {
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

  public UserTable(OrderedColumns columnDefns, String sqlWhereClause, String[] sqlSelectionArgs,
      String[] sqlGroupByArgs, String sqlHavingClause, String sqlOrderByElementKey,
      String sqlOrderByDirection, String[] adminColumnOrder,
      HashMap<String, Integer> elementKeyToIndex, String[] elementKeyForIndex, Integer rowCount) {
    this.mColumnDefns = columnDefns;

    this.mSqlWhereClause = sqlWhereClause;
    this.mSqlSelectionArgs = sqlSelectionArgs;
    this.mSqlGroupByArgs = sqlGroupByArgs;
    this.mSqlHavingClause = sqlHavingClause;
    this.mSqlOrderByElementKey = sqlOrderByElementKey;
    this.mSqlOrderByDirection = sqlOrderByDirection;
    this.mAdminColumnOrder = adminColumnOrder;
    this.mElementKeyToIndex = elementKeyToIndex;
    this.mElementKeyForIndex = elementKeyForIndex;
    if (rowCount != null) {
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

  /**
   * This is EXPENSIVE!!!  Used only for JS return value
   * Do not use for anything else!!!!
   *
   * @return copy of the map. Used for JS return value
   */
  public Map<String, Integer> getElementKeyMap() {
    HashMap<String, Integer> copyMap = new HashMap<String, Integer>(this.mElementKeyToIndex);
    return copyMap;
  }

  public String getElementKey(int colNum) {
    return this.mElementKeyForIndex[colNum];
  }

  public String getWhereClause() {
    return mSqlWhereClause;
  }

  public String[] getSelectionArgs() {
    if ( mSqlSelectionArgs == null ) {
      return null;
    }
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
    if (mSqlGroupByArgs == null) {
      return null;
    }
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
    for (Row row : mRows) {
      String type = row.getRawDataOrMetadataByElementKey(DataTableColumns.SAVEPOINT_TYPE);
      if (type == null || type.length() == 0) {
        return true;
      }
    }
    return false;
  }

  public boolean hasConflictRows() {
    for (Row row : mRows) {
      String conflictType = row.getRawDataOrMetadataByElementKey(DataTableColumns.CONFLICT_TYPE);
      if (conflictType != null && conflictType.length() != 0) {
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
      if (this.mRows.get(i).getRowId().equals(rowId)) {
        return i;
      }
    }
    return -1;
  }

  @Override
  public int describeContents() {
    return 0;
  }

  @Override
  public void writeToParcel(Parcel out, int flags) {
    String[] emptyString = {};
    out.writeString(mSqlWhereClause);
    if (mSqlSelectionArgs == null) {
      out.writeInt(-1);
    } else {
      out.writeInt(mSqlSelectionArgs.length);
      out.writeStringArray(mSqlSelectionArgs);
    }
    if (mSqlGroupByArgs == null) {
      out.writeInt(-1);
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
    for (Row r : mRows) {
      r.writeToParcel(out, flags);
    }
  }

  public UserTable(Parcel in) {
    this.mSqlWhereClause = in.readString();
    int n = in.readInt();
    if ( n == -1 ) {
      this.mSqlSelectionArgs = null;
    } else {
      this.mSqlSelectionArgs = new String[n];
      in.readStringArray(mSqlSelectionArgs);
    }
    n = in.readInt();
    if ( n == -1 ) {
      this.mSqlGroupByArgs = null;
    } else {
      this.mSqlGroupByArgs = new String[n];
      in.readStringArray(mSqlGroupByArgs);
    }
    this.mSqlHavingClause = in.readString();
    this.mSqlOrderByElementKey = in.readString();
    this.mSqlOrderByDirection = in.readString();
    this.mColumnDefns = new OrderedColumns(in);
    this.mAdminColumnOrder = new String[in.readInt()];
    in.readStringArray(mAdminColumnOrder);
    // count of rows...
    int i = in.readInt();
    mRows = new ArrayList<Row>(i);
    for (; i > 0; --i) {
      Row r = new Row(this, in);
      mRows.add(r);
    }

    // These maps will map the element key to the corresponding index in
    // either data or metadata. If the user has defined a column with the
    // element key _my_data, and this column is at index 5 in the data
    // array, dataKeyToIndex would then have a mapping of _my_data:5.
    // The sync_state column, if present at index 7, would have a mapping
    // in metadataKeyToIndex of sync_state:7.
    mElementKeyToIndex = new HashMap<String, Integer>();
    List<String> userColumnOrder = mColumnDefns.getRetentionColumnNames();
    mElementKeyForIndex = new String[userColumnOrder.size() + mAdminColumnOrder.length];
    for (i = 0; i < userColumnOrder.size(); i++) {
      String elementKey = userColumnOrder.get(i);
      mElementKeyForIndex[i] = elementKey;
      mElementKeyToIndex.put(elementKey, i);
    }

    for (int j = 0; j < mAdminColumnOrder.length; j++) {
      // TODO: problem is here. unclear how to best get just the
      // metadata in here. hmm.
      String elementKey = mAdminColumnOrder[j];
      mElementKeyForIndex[i + j] = elementKey;
      mElementKeyToIndex.put(elementKey, i + j);
    }
  }

  public static final Parcelable.Creator<UserTable> CREATOR = new Parcelable.Creator<UserTable>() {
    public UserTable createFromParcel(Parcel in) {
      return new UserTable(in);
    }

    public UserTable[] newArray(int size) {
      return new UserTable[size];
    }
  };

}
