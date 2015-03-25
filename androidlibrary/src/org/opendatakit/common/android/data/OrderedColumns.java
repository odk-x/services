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
import java.util.TreeMap;

import org.opendatakit.aggregate.odktables.rest.ElementDataType;
import org.opendatakit.aggregate.odktables.rest.ElementType;
import org.opendatakit.aggregate.odktables.rest.entity.Column;
import org.opendatakit.common.android.utilities.GeoColumnUtil;

import android.os.Parcel;
import android.os.Parcelable;

public class OrderedColumns implements Parcelable {

  public interface OrderedColumnsIterator {
    public void doAction( ColumnDefinition cd) throws Exception;
  }
  
  private final String appName;
  private final String tableId;
  private final ArrayList<ColumnDefinition> orderedDefns;
  
  // private static final Map<String, ArrayList<ColumnDefinition> > mapping;
  
  public OrderedColumns(String appName, String tableId, List<Column> columns) {
    this.appName = appName;
    this.tableId = tableId;
    this.orderedDefns = ColumnDefinition.buildColumnDefinitions(appName, tableId, columns);
  }
  
  public ColumnDefinition find(String elementKey) {
    return ColumnDefinition.find(orderedDefns, elementKey);
  }
  
  /**
   * Get the names of the columns that are written into the underlying database table.
   * These are the isUnitOfRetention() columns.
   * 
   * @param orderedDefns
   * @return
   */
  public ArrayList<String> getRetentionColumnNames() {
    ArrayList<String> writtenColumns = new ArrayList<String>();
    for ( ColumnDefinition cd : orderedDefns ) {
      if ( cd.isUnitOfRetention() ) {
        writtenColumns.add(cd.getElementKey());
      }
    }
    return writtenColumns;
  }
  

  public boolean graphViewIsPossible() {
    for (ColumnDefinition cd : orderedDefns) {
      if (!cd.isUnitOfRetention()) {
        continue;
      }
      ElementType elementType = cd.getType();
      ElementDataType type = elementType.getDataType();
      if (type == ElementDataType.number || type == ElementDataType.integer) {
        return true;
      }
    }
    return false;
  }
  
  /**
   * Extract the list of geopoints from the table.
   * 
   * @param orderedDefns
   * @return the list of geopoints.
   */
  public ArrayList<ColumnDefinition> getGeopointColumnDefinitions() {
    ArrayList<ColumnDefinition> cdList = new ArrayList<ColumnDefinition>();

    for (ColumnDefinition cd : orderedDefns) {
      if (cd.getType().getElementType().equals(ElementType.GEOPOINT)) {
        cdList.add(cd);
      }
    }
    return cdList;
  }

  public boolean mapViewIsPossible() {
    List<ColumnDefinition> geoPoints = getGeopointColumnDefinitions();
    if (geoPoints.size() != 0) {
      return true;
    }

    boolean hasLatitude = false;
    boolean hasLongitude = false;
    for (ColumnDefinition cd : orderedDefns) {
      hasLatitude = hasLatitude || GeoColumnUtil.get().isLatitudeColumnDefinition(geoPoints, cd);
      hasLongitude = hasLongitude || GeoColumnUtil.get().isLongitudeColumnDefinition(geoPoints, cd);
    }
    
    return (hasLatitude && hasLongitude);
  }
  
  public ArrayList<ColumnDefinition> getColumnDefinitions() {
    return orderedDefns;
  }

  public String getAppName() {
    return appName;
  }
  
  public String getTableId() {
    return tableId;
  }
  
  public ArrayList<Column> getColumns() {
    return ColumnDefinition.getColumns(orderedDefns);
  }
  
  public TreeMap<String, Object> getDataModel() {
    return ColumnDefinition.getDataModel(orderedDefns);
  }

  @Override
  public int describeContents() {
    return 0;
  }

  @Override
  public void writeToParcel(Parcel out, int flags) {
    out.writeString(appName);
    out.writeString(tableId);
    ColumnList cl = new ColumnList(getColumns());
    cl.writeToParcel(out, flags);
  }
  
  public OrderedColumns(Parcel in) {
    appName = in.readString();
    tableId = in.readString();
    ColumnList cl = new ColumnList(in);
    this.orderedDefns = ColumnDefinition.buildColumnDefinitions(appName, tableId, cl.getColumns());
  }

  public static final Parcelable.Creator<OrderedColumns> CREATOR
          = new Parcelable.Creator<OrderedColumns>() {
      public OrderedColumns createFromParcel(Parcel in) {
          return new OrderedColumns(in);
      }

      public OrderedColumns[] newArray(int size) {
          return new OrderedColumns[size];
      }
  };

}
