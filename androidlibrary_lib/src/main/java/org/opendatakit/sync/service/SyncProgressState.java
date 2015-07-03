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
package org.opendatakit.sync.service;

import android.os.Parcel;
import android.os.Parcelable;

public enum SyncProgressState implements Parcelable {
  INIT, STARTING, APP_FILES, TABLE_FILES, ROWS, COMPLETE, ERROR;

  @Override
  public int describeContents() {
    return 0;
  }

  @Override
  public void writeToParcel(Parcel dest, int flags) {
    dest.writeString(this.name());
  }

  public static final Parcelable.Creator<SyncProgressState> CREATOR = new Parcelable.Creator<SyncProgressState>() {
    public SyncProgressState createFromParcel(Parcel in) {
      return SyncProgressState.valueOf(in.readString());
    }

    public SyncProgressState[] newArray(int size) {
      return new SyncProgressState[size];
    }
  };

}