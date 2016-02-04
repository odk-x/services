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

import android.os.Parcel;
import android.os.Parcelable;

import java.util.ArrayList;

/**
 * This class represents a table of results from an arbitrary query.
 *
 * The arbitrary query results are interpreted against a reference table's mColumnDefns.
 *
 * If the arbitrary query result column (without alias) matches a metadata column or
 * one of the reference table's column names, then the data type conversion for that
 * column is applied when generating the result set.
 *
 * Otherwise, the result will be returned as a string value (and the caller will need to apply
 * their own transformations).
 *
 * If the result set lacks an _id (RowId) column, an ordinal number (1..n) will be used.
 *
 * <p>
 * This should be considered an immutable class.
 *
 * @author mitchellsundt@gmail.com
 * @author sudar.sam@gmail.com
 *
 */
public class RawUserTable implements Parcelable {

  static final String TAG = RawUserTable.class.getSimpleName();

  private final ArrayList<RawRow> mRows;

  private final String mSqlCommand;
  private final String[] mSqlBindArgs;

  private final String[] mElementKeyForIndex;

  public RawUserTable(
      String sqlCommand, String[] sqlBindArgs, String[] elementKeyForIndex, Integer rowCount) {

    this.mSqlCommand = sqlCommand;
    this.mSqlBindArgs = sqlBindArgs;
    this.mElementKeyForIndex = elementKeyForIndex;
    if ( mElementKeyForIndex == null ) {
      throw new IllegalStateException("mElementKeyForIndex cannot be null!");
    }
    if (rowCount != null) {
      mRows = new ArrayList<RawRow>(rowCount);
    } else {
      mRows = new ArrayList<RawRow>();
    }
  }

  public void addRow(RawRow row) {
    mRows.add(row);
  }

  public RawRow getRowAtIndex(int index) {
    return this.mRows.get(index);
  }

  public String getElementKey(int colNum) {
    return this.mElementKeyForIndex[colNum];
  }

  public String getSqlCommand() {
    return mSqlCommand;
  }

  public String[] getSqlBindArgs() {
    if ( mSqlBindArgs == null ) {
      return null;
    }
    return mSqlBindArgs.clone();
  }

  public int getWidth() {
    return mElementKeyForIndex.length;
  }

  public int getNumberOfRows() {
    return this.mRows.size();
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
    out.writeString(mSqlCommand);
    if (mSqlBindArgs == null) {
      out.writeInt(-1);
    } else {
      out.writeInt(mSqlBindArgs.length);
      out.writeStringArray(mSqlBindArgs);
    }
    out.writeInt(mElementKeyForIndex.length);
    out.writeStringArray(mElementKeyForIndex);
    out.writeInt(mRows.size());
    for (RawRow r : mRows) {
      r.writeToParcel(out, flags);
    }
  }

  public RawUserTable(Parcel in) {
    this.mSqlCommand = in.readString();
    int n = in.readInt();
    if ( n == -1 ) {
      this.mSqlBindArgs = null;
    } else {
      this.mSqlBindArgs = new String[n];
      in.readStringArray(mSqlBindArgs);
    }
    n = in.readInt();
    this.mElementKeyForIndex = new String[n];
    in.readStringArray(mElementKeyForIndex);
    // count of rows...
    int i = in.readInt();
    mRows = new ArrayList<RawRow>(i);
    for (; i > 0; --i) {
      RawRow r = new RawRow(this, in);
      mRows.add(r);
    }
  }

  public static final Creator<RawUserTable> CREATOR = new Creator<RawUserTable>() {
    public RawUserTable createFromParcel(Parcel in) {
      return new RawUserTable(in);
    }

    public RawUserTable[] newArray(int size) {
      return new RawUserTable[size];
    }
  };

}
