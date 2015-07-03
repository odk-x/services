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
package org.opendatakit.database.service;

import android.os.Parcel;
import android.os.Parcelable;

public enum TableHealthStatus implements Parcelable {
  TABLE_HEALTH_IS_CLEAN, 
  TABLE_HEALTH_HAS_CONFLICTS, 
  TABLE_HEALTH_HAS_CHECKPOINTS,
  TABLE_HEALTH_HAS_CHECKPOINTS_AND_CONFLICTS;
  
  @Override
  public int describeContents() {
    return 0;
  }

  @Override
  public void writeToParcel(Parcel dest, int flags) {
    dest.writeString(this.name());
  }

  public static final Parcelable.Creator<TableHealthStatus> CREATOR = new Parcelable.Creator<TableHealthStatus>() {
    public TableHealthStatus createFromParcel(Parcel in) {
      return TableHealthStatus.valueOf(in.readString());
    }

    public TableHealthStatus[] newArray(int size) {
      return new TableHealthStatus[size];
    }
  };


}
