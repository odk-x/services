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
package org.opendatakit.common.android.data;

import android.os.Parcel;
import android.os.Parcelable;

/**
 * A simple struct to hold the contents of a table definition entry.
 * 
 * @author mitchellsundt@gmail.com
 *
 */
public class TableDefinitionEntry implements Parcelable {

  public String tableId;

  public String schemaETag;

  public String lastDataETag;

  public String lastSyncTime;

  public TableDefinitionEntry(String tableId) {
    this.tableId = tableId;
  }
  
  public TableDefinitionEntry(Parcel in) {
    readFromParcel(in);
  }

  public String getTableId() {
    return tableId;
  }

  public String getSchemaETag() {
    return schemaETag;
  }

  public void setSchemaETag(String schemaETag) {
    this.schemaETag = schemaETag;
  }

  public String getLastDataETag() {
    return lastDataETag;
  }

  public void setLastDataETag(String lastDataETag) {
    this.lastDataETag = lastDataETag;
  }

  public String getLastSyncTime() {
    return lastSyncTime;
  }

  public void setLastSyncTime(String lastSyncTime) {
    this.lastSyncTime = lastSyncTime;
  }

  @Override
  public int describeContents() {
    return 0;
  }

  @Override
  public void writeToParcel(Parcel out, int flags) {
    out.writeString(tableId);
    out.writeString(schemaETag);
    out.writeString(lastDataETag);
    out.writeString(lastSyncTime);
  }
  
  private void readFromParcel(Parcel in) {
    tableId = in.readString();
    schemaETag = in.readString();
    lastDataETag = in.readString();
    lastSyncTime = in.readString();
  }

  public static final Parcelable.Creator<TableDefinitionEntry> CREATOR
          = new Parcelable.Creator<TableDefinitionEntry>() {
      public TableDefinitionEntry createFromParcel(Parcel in) {
          return new TableDefinitionEntry(in);
      }

      public TableDefinitionEntry[] newArray(int size) {
          return new TableDefinitionEntry[size];
      }
  };
}
