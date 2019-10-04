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
package org.opendatakit.services.database.utilities;

import android.database.Cursor;

import org.opendatakit.database.DatabaseConstants;
import org.opendatakit.provider.ChoiceListColumns;
import org.opendatakit.services.database.OdkConnectionInterface;

import java.util.ArrayList;

/**
 * Manipulator class for setting and getting choiceList definitions.
 */
public final class ChoiceListUtils {

  /**
   * Methods are all static...
   */
  private ChoiceListUtils() {
  }

  /**
   * @param db           a database connection to use
   * @param choiceListId which row of choices to get from the database
   * @return
   */
  public static final String getChoiceList(OdkConnectionInterface db, String choiceListId) {

    if ( choiceListId == null || choiceListId.trim().length() == 0 ) {
      return null;
    }

    ArrayList<String> bindArgs = new ArrayList<String>();
    StringBuilder b = new StringBuilder();
    //@formatter:off
    b.append("SELECT ").append(ChoiceListColumns.CHOICE_LIST_JSON).append(" FROM ")
     .append("\"").append(DatabaseConstants.CHOICE_LIST_TABLE_NAME).append("\" WHERE ")
     .append(ChoiceListColumns.CHOICE_LIST_ID).append("=?");
    //@formatter:on
    bindArgs.add(choiceListId);

    Cursor c = null;
    try {
      c = db.rawQuery(b.toString(), bindArgs.toArray(new String[bindArgs.size()]));

      c.moveToFirst();
      if (c.getCount() == 0) {
        // unknown...
        return null;
      }

      if (c.getCount() > 1) {
        throw new IllegalStateException(
            "getChoiceList: multiple entries for choiceListId " + choiceListId);
      }

      int idx = c.getColumnIndex(ChoiceListColumns.CHOICE_LIST_JSON);
      if (c.isNull(idx)) {
        // shouldn't happen...
        return null;
      }
      String value = c.getString(idx);
      if (value == null || value.trim().length() == 0) {
        // also shouldn't happen...
        return null;
      }

      return value;
    } finally {
      if ( c != null && !c.isClosed()) {
        c.close();
      }
    }
  }

  /**
   * Updates the choice just row with the given id to have the new json
   *
   * @param db             a database connection to use
   * @param choiceListId   the id of the row in the choice list table
   * @param choiceListJSON json representing the new set of choices
   */
  public static final void setChoiceList(OdkConnectionInterface db, String choiceListId,
      String choiceListJSON) {

    if ( choiceListId == null || choiceListId.trim().length() == 0 ) {
      throw new IllegalArgumentException("setChoiceList: choiceListId cannot be null or empty");
    }
    if ( choiceListJSON == null || choiceListJSON.trim().length() == 0 ) {
      throw new IllegalArgumentException("setChoiceList: choiceListJSON cannot be null or empty");
    }
    if ( !choiceListJSON.trim().equals(choiceListJSON) ) {
      throw new IllegalArgumentException(
          "setChoiceList: choiceListJSON cannot have excess white space");
    }

    ArrayList<String> bindArgs = new ArrayList<String>();
    StringBuilder b = new StringBuilder();
    //@formatter:off
    b.append("DELETE FROM ")
     .append("\"").append(DatabaseConstants.CHOICE_LIST_TABLE_NAME).append("\" WHERE ")
     .append(ChoiceListColumns.CHOICE_LIST_ID).append("=?");
    //@formatter:on
    bindArgs.add(choiceListId);

    boolean inTransaction = db.inTransaction();
    try {
      if ( !inTransaction ) {
        db.beginTransactionNonExclusive();
      }
      db.execSQL(b.toString(), bindArgs.toArray(new String[bindArgs.size()]));

      b.setLength(0);
      bindArgs.clear();
      //@formatter:off
      b.append("INSERT INTO \"").append(DatabaseConstants.CHOICE_LIST_TABLE_NAME).append("\" (")
       .append(ChoiceListColumns.CHOICE_LIST_ID).append(",")
       .append(ChoiceListColumns.CHOICE_LIST_JSON).append(") VALUES (?,?)");
      //@formatter:on
      bindArgs.add(choiceListId);
      bindArgs.add(choiceListJSON);

      db.execSQL(b.toString(), bindArgs.toArray(new String[bindArgs.size()]));

      if ( !inTransaction ) {
        db.setTransactionSuccessful();
      }
    } finally {
      if ( !inTransaction ) {
        db.endTransaction();
      }
    }
  }
}
