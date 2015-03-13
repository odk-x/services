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

public class OdkDbHandle implements Parcelable {
  private final String databaseHandle;

  public OdkDbHandle(String databaseHandle) {
    this.databaseHandle = databaseHandle;
  }
  
  public OdkDbHandle(Parcel in) {
    this.databaseHandle = in.readString();
  }
  
  public String getDatabaseHandle() {
    return this.databaseHandle;
  }
  
  @Override
  public int describeContents() {
    return 0;
  }

  @Override
  public void writeToParcel(Parcel dest, int flags) {
    dest.writeString(this.getDatabaseHandle());
  }

  public static final Parcelable.Creator<OdkDbHandle> CREATOR = new Parcelable.Creator<OdkDbHandle>() {
    public OdkDbHandle createFromParcel(Parcel in) {
      return new OdkDbHandle(in);
    }

    public OdkDbHandle[] newArray(int size) {
      return new OdkDbHandle[size];
    }
  };


}
