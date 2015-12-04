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
package org.opendatakit.resolve.conflict;

import org.opendatakit.common.android.data.UserTable;

/**
 * Holds data from both the server and local databases about the rows that are
 * in conflict. Of all the rows in the entire database, these are the subset
 * that are in conflict. There is a notion of a server-side and a local version
 * of each of these rows.
 * 
 * @author sudar.sam@gmail.com
 *
 */
public class ConflictTable {

  private UserTable mLocalTable;
  private UserTable mServerTable;

  /**
   * Construct a conflict table. For the tables to have any sort of ability to
   * relate to one another, their rows must have been sorted on something that
   * will be shared between them, e.g. their UUID. This way the ith row will in
   * each table will be sure to point to the same row, assuming that there is
   * nothing gone wrong.
   * 
   * @param localTable
   * @param serverTable
   */
  public ConflictTable(UserTable localTable, UserTable serverTable) {
    this.mLocalTable = localTable;
    this.mServerTable = serverTable;
  }

  public UserTable getLocalTable() {
    return this.mLocalTable;
  }

  public UserTable getServerTable() {
    return this.mServerTable;
  }

}
