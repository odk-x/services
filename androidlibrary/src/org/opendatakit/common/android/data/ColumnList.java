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

package org.opendatakit.common.android.data;

import java.util.ArrayList;
import java.util.List;

import org.opendatakit.aggregate.odktables.rest.entity.Column;

import android.os.Parcel;
import android.os.Parcelable;

/**
 * Wrapper class for a list of Columns so that it can 
 * be sent to an Android Service.
 * 
 * @author mitchellsundt@gmail.com
 *
 */
public class ColumnList implements Parcelable {

  private List<Column> columns;
  
  public ColumnList(List<Column> columns) {
    if ( columns == null ) {
      throw new IllegalArgumentException("list of Columns cannot be null!");
    }
    this.columns = columns;
  }

  public ColumnList(Parcel in) {
    readFromParcel(in);
  }
  
  public List<Column> getColumns() {
    return columns;
  }

  @Override
  public int describeContents() {
    return 0;
  }

  @Override
  public void writeToParcel(Parcel out, int flags) {
    out.writeInt(columns.size());
    for ( Column column : columns ) {
      out.writeString(column.getElementKey());
      out.writeString(column.getElementName());
      out.writeString(column.getElementType());
      out.writeString(column.getListChildElementKeys());
    }
  }
  
  private void readFromParcel(Parcel in) {
    int count = in.readInt();
    columns = new ArrayList<Column>(count);
    for ( int i = 0 ; i < count ; ++i ) {
      String elementKey = in.readString();
      String elementName = in.readString();
      String elementType = in.readString();
      String listChildElementKeys = in.readString();
      
      Column col = new Column(elementKey, elementName,
                              elementType, listChildElementKeys);
      columns.add(col);
    }
  }

  public static final Parcelable.Creator<ColumnList> CREATOR
          = new Parcelable.Creator<ColumnList>() {
      public ColumnList createFromParcel(Parcel in) {
          return new ColumnList(in);
      }

      public ColumnList[] newArray(int size) {
          return new ColumnList[size];
      }
  };
}
