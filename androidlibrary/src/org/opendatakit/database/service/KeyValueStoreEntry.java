/*
 * Copyright (C) 2012-2013 University of Washington
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

/**
 * This holds one row from the key-value store.
 * <p>
 * For a more in-depth explanation of all these fields, see the
 * KeyValueStoreManager.java class in ODK Tables.
 *
 * @author sudar.sam@gmail.com
 *
 */
public class KeyValueStoreEntry implements Parcelable, Comparable<KeyValueStoreEntry> {

  /**
   * The table id of the table to which this entry belongs.
   */
  public String tableId;

  /**
   * The partition in the key value store to which the entry belongs. For an in
   * depth example see KeyValueStoreManager.java in the ODK Tables project.
   * Otherwise, just know that it is essentially the identifier of the class
   * that is responsible for managing the entry. ListView.java would have (by
   * convention) a partition name ListView. TableProperties and ColumnProperties
   * are the exception, belonging simply to the partitions "Table" and "Column".
   */
  public String partition;

  /**
   * The aspect is essentially the scope, or the instance of the partition, to
   * which this key/entry belongs. For instance, a table-wide property would
   * have the aspect "default". A column's aspect would be its element key (ie
   * its unique column identifier for the table). A particular saved graph view
   * might have the display name of that graph.
   */
  public String aspect;

  /**
   * The key of this entry. This is important so that ODKTables knows what to do
   * with this entry. Eg a key of "list" might mean that this entry is important
   * to the list view of the table.
   */
  public String key;

  /**
   * The type of this entry. This is important to taht ODKTables knows how to
   * interpret the value of this entry. Eg type String means that the value
   * holds a string. Type file means that the value is a JSON object holding a
   * FileManifestEntry object with information relating to the version of the
   * file and how to get it.
   */
  public String type;

  /**
   * The actual value of this entry. If the type is String, this is a string. If
   * it is a File, it is a FileManifestEntry JSON object.
   */
  public String value;
  
  public KeyValueStoreEntry() {
  }
  
  public KeyValueStoreEntry(Parcel in) {
    readFromParcel(in);
  }

  @Override
  public String toString() {
    return "[tableId=" + tableId + ", partition=" + partition + ", aspect=" + aspect + ", key="
        + key + ", type=" + type + ", value=" + value + "]";
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((tableId == null) ? 0 : tableId.hashCode());
    result = prime * result + ((partition == null) ? 0 : partition.hashCode());
    result = prime * result + ((aspect == null) ? 0 : aspect.hashCode());
    result = prime * result + ((key == null) ? 0 : key.hashCode());
    result = prime * result + ((type == null) ? 0 : type.hashCode());
    result = prime * result + ((value == null) ? 0 : value.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof KeyValueStoreEntry)) {
      return false;
    }
    KeyValueStoreEntry other = (KeyValueStoreEntry) obj;
    return (tableId == null ? other.tableId == null : tableId.equals(other.tableId))
        && (partition == null ? other.partition == null : partition.equals(other.partition))
        && (aspect == null ? other.aspect == null : aspect.equals(other.aspect))
        && (key == null ? other.key == null : key.equals(other.key))
        && (type == null ? other.type == null : type.equals(other.type))
        && (value == null ? other.value == null : value.equals(other.value));
  }

  @Override
  public int compareTo(KeyValueStoreEntry that) {
    int partitionComparison = this.partition.compareTo(that.partition);
    if (partitionComparison != 0) {
      return partitionComparison;
    }
    int aspectComparison = this.aspect.compareTo(that.aspect);
    if (aspectComparison != 0) {
      return aspectComparison;
    }
    // Otherwise, we'll just return the value of the key, b/c if the key
    // is also the same, we're equal.
    int keyComparison = this.key.compareTo(that.key);
    return keyComparison;
  }

  @Override
  public int describeContents() {
    return 0;
  }

  @Override
  public void writeToParcel(Parcel out, int flags) {
    out.writeString(tableId);
    out.writeString(partition);
    out.writeString(aspect);
    out.writeString(key);
    out.writeString(type);
    out.writeString(value);
  }
  
  private void readFromParcel(Parcel in) {
    tableId = in.readString();
    partition = in.readString();
    aspect = in.readString();
    key = in.readString();
    type = in.readString();
    value = in.readString();
  }

  public static final Parcelable.Creator<KeyValueStoreEntry> CREATOR
          = new Parcelable.Creator<KeyValueStoreEntry>() {
      public KeyValueStoreEntry createFromParcel(Parcel in) {
          return new KeyValueStoreEntry(in);
      }

      public KeyValueStoreEntry[] newArray(int size) {
          return new KeyValueStoreEntry[size];
      }
  };

}
