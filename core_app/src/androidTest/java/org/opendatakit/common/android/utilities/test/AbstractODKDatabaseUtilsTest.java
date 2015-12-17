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

package org.opendatakit.common.android.utilities.test;

import android.content.ContentValues;
import android.database.Cursor;
import android.test.AndroidTestCase;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.lang3.CharEncoding;
import org.opendatakit.aggregate.odktables.rest.*;
import org.opendatakit.aggregate.odktables.rest.entity.Column;
import org.opendatakit.common.android.data.*;
import org.opendatakit.common.android.database.AndroidConnectFactory;
import org.opendatakit.common.android.database.DatabaseConstants;
import org.opendatakit.common.android.database.OdkConnectionFactorySingleton;
import org.opendatakit.common.android.database.OdkConnectionInterface;
import org.opendatakit.common.android.provider.*;
import org.opendatakit.common.android.utilities.*;
import org.opendatakit.database.service.KeyValueStoreEntry;
import org.opendatakit.database.service.OdkDbHandle;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Created by wrb on 9/21/2015.
 *
 * Put the actual unit tests in this Abstract class as there are two setups for running these tests
 *
 * In ODKDatabaseImplUtilsKeepState it keeps the database initalized between tests whereas
 * in ODKDatabaseImplUtilsResetState, it wipes the database from the file system between each test
 */
public abstract class AbstractODKDatabaseUtilsTest extends AndroidTestCase {

  private static final String TAG = "AbstractODKDatabaseUtilsTest";

  private static final String testTable = "testTable";

  private static final String elemKey = "_element_key";
  private static final String elemName = "_element_name";
  private static final String listChildElemKeys = "_list_child_element_keys";

  protected OdkConnectionInterface db;

  protected abstract String getAppName();

  protected void verifyNoTablesExistNCleanAllTables() {
    /* NOTE: if there is a problem it might be the fault of the previous test if the assertion
       failure is happening in the setUp function as opposed to the tearDown function */

    List<String> tableIds = ODKDatabaseImplUtils.get().getAllTableIds(db);

    boolean tablesGone = (tableIds.size() == 0);

    // Drop any leftover table now that the test is done
    for(String id : tableIds) {
      ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), id);
    }

    assertTrue(tablesGone);
  }

  protected void verifyNoTablesExist() {
    List<String> tableIds = ODKDatabaseImplUtils.get().getAllTableIds(db);
    assertTrue(tableIds.size() == 0);
  }
  /*
   * Check that the database is setup
   */
  public void testPreConditions() {
    assertNotNull(db);
  }


  /*
 * Test creation of user defined database table when table doesn't exist
 */
  public void testCreateOrOpenDbTableWhenTableDoesNotExist_ExpectPass() {
    verifyNoTablesExist();

    // Create the database table
    String tableName= testTable;
    String testCol = "testColumn";
    String testColType = ElementDataType.integer.name();
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[]"));
    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
            .createOrOpenDBTableWithColumns(db, getAppName(), tableName, columns);

    // Check that the table exists
    Cursor cursor = db.rawQuery("SELECT name FROM sqlite_master WHERE type='table' AND name='" + tableName + "'", null);
    assertNotNull("Cursor is null", cursor);
    assertEquals("Cursor should only have 1 row", cursor.getCount(), 1);
    cursor.moveToFirst();
    assertEquals("Name of user defined table does not match", cursor.getString(0), tableName);

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableName);
    verifyNoTablesExist();
  }

  /*
   * Test query when there is no data
   */
  public void testQueryWithNoData_ExpectFail() {
    String tableId = testTable;
    boolean thrown = false;

    try {
      // this will not interact with the database if the
      // query string is found in the PreparedStatement cache.
      Cursor c = ODKDatabaseImplUtils.get().query(db, tableId, null, null, null, null, null, null, null);
      // we must get the count of rows to actually interact
      // with the database.
      c.getCount();
    } catch (Exception e) {
      thrown = true;
      e.printStackTrace();
    }
    assertTrue(thrown);
  }

  /*
   * Test query when there is data
   */
  public void testQueryWithData_ExpectPass() {
    String tableId = testTable;
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column("col1", "col1", "string", "[]"));
    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
        .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);

    // Check that the user defined rows are in the table
    Cursor cursor = ODKDatabaseImplUtils.get().query(db, tableId, null, null, null, null, null,
        null, null);
    Cursor refCursor = db.query(tableId, null, null, null, null, null, null, null);

    if (cursor != null && refCursor != null) {
      int index = 0;
      while (cursor.moveToNext() && refCursor.moveToNext()) {
        int testType = cursor.getType(index);
        int refType = refCursor.getType(index);
        assertEquals(testType, refType);

        switch (refType) {
        case Cursor.FIELD_TYPE_BLOB:
          byte[] byteArray = cursor.getBlob(index);
          byte[] refByteArray = refCursor.getBlob(index);
          assertEquals(byteArray, refByteArray);
          break;
        case Cursor.FIELD_TYPE_FLOAT:
          float valueFloat = cursor.getFloat(index);
          float refValueFloat = refCursor.getFloat(index);
          assertEquals(valueFloat, refValueFloat);
          break;
        case Cursor.FIELD_TYPE_INTEGER:
          int valueInt = cursor.getInt(index);
          int refValueInt = refCursor.getInt(index);
          assertEquals(valueInt, refValueInt);
          break;
        case Cursor.FIELD_TYPE_STRING:
          String valueStr = cursor.getString(index);
          String refValueStr = refCursor.getString(index);
          assertEquals(valueStr, refValueStr);
          break;
        case Cursor.FIELD_TYPE_NULL:
        default:
          break;
        }
      }
    }

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test raw query when there is data
   */
  public void testRawQueryWithNoData_ExpectFail() {
    String tableId = testTable;
    String query = "SELECT * FROM " + tableId;
    boolean thrown = false;

    try {
      // this will not interact with the database if the
      // query string is found in the PreparedStatement cache.
      Cursor c = ODKDatabaseImplUtils.get().rawQuery(db, query, null);
      // we must get the count of rows to actually interact
      // with the database.
      c.getCount();
    } catch (Exception e) {
      thrown = true;
      e.printStackTrace();
    }

    assertTrue(thrown);
  }

  /*
   * Test raw query when there is no data
   */
  public void testRawQueryWithData_ExpectPass() {
    String tableId = testTable;
    String query = "SELECT * FROM " + tableId;
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column("col1", "col1", "string", "[]"));
    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
        .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);

    // Check that the user defined rows are in the table
    Cursor cursor = ODKDatabaseImplUtils.get().rawQuery(db, query, null);
    Cursor refCursor = db.rawQuery(query, null);

    if (cursor != null && refCursor != null) {
      int index = 0;
      while (cursor.moveToNext() && refCursor.moveToNext()) {
        int testType = cursor.getType(index);
        int refType = refCursor.getType(index);
        assertEquals(testType, refType);

        switch (refType) {
        case Cursor.FIELD_TYPE_BLOB:
          byte[] byteArray = cursor.getBlob(index);
          byte[] refByteArray = refCursor.getBlob(index);
          assertEquals(byteArray, refByteArray);
          break;
        case Cursor.FIELD_TYPE_FLOAT:
          float valueFloat = cursor.getFloat(index);
          float refValueFloat = refCursor.getFloat(index);
          assertEquals(valueFloat, refValueFloat);
          break;
        case Cursor.FIELD_TYPE_INTEGER:
          int valueInt = cursor.getInt(index);
          int refValueInt = refCursor.getInt(index);
          assertEquals(valueInt, refValueInt);
          break;
        case Cursor.FIELD_TYPE_STRING:
          String valueStr = cursor.getString(index);
          String refValueStr = refCursor.getString(index);
          assertEquals(valueStr, refValueStr);
          break;
        case Cursor.FIELD_TYPE_NULL:
        default:
          break;
        }
      }
    }

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test creation of user defined database table with column when table does
   * not exist
   */
  public void testCreateOrOpenDbTableWithColumnWhenColumnDoesNotExist_ExpectPass() {
    String tableId = testTable;
    String testCol = "testColumn";
    String testColType = ElementDataType.integer.name();
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[]"));
    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
        .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);

    OrderedColumns coldefs = ODKDatabaseImplUtils.get().getUserDefinedColumns(db, getAppName(), tableId);
    assertEquals(coldefs.getColumnDefinitions().size(), 1);
    assertEquals(coldefs.getColumnDefinitions().get(0).getElementKey(), testCol);

    for (ColumnDefinition col : coldefs.getColumnDefinitions()) {
      String key = col.getElementKey();
      String name = col.getElementName();
      String type = col.getElementType();
      assertTrue(key.equals(testCol));
      assertTrue(name.equals(testCol));
      assertTrue(type.equals(testColType));
    }

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test creation of user defined database table with column when table does
   * exist
   */
  public void testCreateOrOpenDbTableWithColumnWhenColumnDoesExist_ExpectPass() {
    String tableId = testTable;
    String testCol = "testColumn";
    String testColType = ElementDataType.integer.name();
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[]"));
    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
        .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);
    OrderedColumns orderedColumns2 = ODKDatabaseImplUtils.get()
        .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);

    OrderedColumns coldefs = ODKDatabaseImplUtils.get().getUserDefinedColumns(db, getAppName(), tableId);
    assertEquals(coldefs.getColumnDefinitions().size(), 1);
    assertEquals(coldefs.getColumnDefinitions().get(0).getElementKey(), testCol);

    for (ColumnDefinition col : coldefs.getColumnDefinitions()) {
      String key = col.getElementKey();
      String name = col.getElementName();
      String type = col.getElementType();
      assertTrue(key.equals(testCol));
      assertTrue(name.equals(testCol));
      assertTrue(type.equals(testColType));
    }

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test creation of user defined database table with column when column is
   * null
   */
  public void testCreateOrOpenDbTableWithColumnWhenColumnIsNull_ExpectFail() {
    String tableId = testTable;
    boolean thrown = false;
    OrderedColumns orderedColumns = null;

    try {
      orderedColumns = ODKDatabaseImplUtils.get().createOrOpenDBTableWithColumns(db, getAppName(), tableId, null);
    } catch (Exception e) {
      thrown = true;
      e.printStackTrace();
    }

    assertTrue(thrown);

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

   /*
    * Test creation of user defined database table with column when column is
    * null
    */
   public void testCreateOrOpenDbTableWithColumnWhenColumnIsEmpty_ExpectPass() {
      String tableId = testTable;
      List<Column> columns = new ArrayList<Column>();

      boolean thrown = false;
      OrderedColumns orderedColumns = null;

      try {
         orderedColumns = ODKDatabaseImplUtils.get().createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);
      } catch (Exception e) {
         thrown = true;
         e.printStackTrace();
      }

      assertFalse(thrown);

      // Drop the table now that the test is done
      ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
   }
  /*
   * Test creation of user defined database table with column when column is int
   */
  public void testCreateOrOpenDbTableWithColumnWhenColumnIsInt_ExpectPass() {
    String tableId = testTable;
    String testCol = "testColumn";
    String testColType = ElementDataType.integer.name();
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[]"));
    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
        .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);

    OrderedColumns coldefs = ODKDatabaseImplUtils.get().getUserDefinedColumns(db, getAppName(), tableId);
    assertEquals(coldefs.getColumnDefinitions().size(), 1);
    assertEquals(coldefs.getColumnDefinitions().get(0).getElementKey(), testCol);

    for (ColumnDefinition col : coldefs.getColumnDefinitions()) {
      String key = col.getElementKey();
      String name = col.getElementName();
      String type = col.getElementType();
      assertTrue(key.equals(testCol));
      assertTrue(name.equals(testCol));
      assertTrue(type.equals(testColType));
    }

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test creation of user defined database table with column when column is
   * array
   */
  public void testCreateOrOpenDbTableWithColumnWhenColumnIsArray_ExpectFail() {
    String tableId = testTable;
    String testCol = "testColumn";
    String itemsStr = "items";
    String testColItems = testCol + "_" + itemsStr;
    String testColType = ElementDataType.array.name();
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[\"" + testColItems + "\"]"));

    boolean success = false;
    OrderedColumns orderedColumns;

    try {
      orderedColumns = ODKDatabaseImplUtils.get().createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);
      success = true;
    } catch (IllegalArgumentException e) {
      // no-op
    }
    assertFalse(success);

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test creation of user defined database table with column when column is
   * array
   */
  public void testCreateOrOpenDbTableWithColumnWhenColumnIsArrayEmpty_ExpectFail() {
    String tableId = testTable;
    String testCol = "testColumn";
    String testColType = ElementDataType.array.name();
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[]"));

    boolean success = false;
    OrderedColumns orderedColumns;

    try {
      orderedColumns = ODKDatabaseImplUtils.get().createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);
      success = true;
    } catch (IllegalArgumentException e) {
      // no-op
    }
    assertFalse(success);

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test creation of user defined database table with column when column is
   * array
   */
  public void testCreateOrOpenDbTableWithColumnWhenColumnIsArray_ExpectPass() {
    String tableId = testTable;
    String testCol = "testColumn";
    String itemsStr = "items";
    String testColItems = testCol + "_" + itemsStr;
    String testColType = ElementDataType.array.name();
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[\"" + testColItems + "\"]"));
    columns.add(new Column(testColItems, itemsStr, ElementDataType.string.name(), "[]"));
    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
        .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);

    OrderedColumns coldefs = ODKDatabaseImplUtils.get().getUserDefinedColumns(db, getAppName(), tableId);
    assertEquals(coldefs.getColumnDefinitions().size(), 2);
    assertEquals(coldefs.getColumnDefinitions().get(0).getElementKey(), testCol);
    assertEquals(coldefs.getColumnDefinitions().get(1).getElementKey(), testColItems);

    for (ColumnDefinition col : coldefs.getColumnDefinitions()) {
      String key = col.getElementKey();
      String name = col.getElementName();
      String type = col.getElementType();
      if (key.equals(testCol)) {
        assertTrue(key.equals(testCol));
        assertTrue(name.equals(testCol));
        assertTrue(type.equals(testColType));
      } else {
        assertTrue(key.equals(testColItems));
        assertTrue(name.equals(itemsStr));
        assertTrue(type.equals(ElementDataType.string.name()));
      }
    }

    // Select everything out of the table
    String sel = "SELECT * FROM " + DatabaseConstants.COLUMN_DEFINITIONS_TABLE_NAME + " WHERE "
        + elemKey + " = ?";
    String[] selArgs = { "" + testCol };
    Cursor cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);

    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(listChildElemKeys);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_STRING);
      String valStr = cursor.getString(ind);
      String testVal = "[\"" + testColItems + "\"]";
      assertEquals(valStr, testVal);
    }

    // Select everything out of the table
    sel = "SELECT * FROM " + DatabaseConstants.COLUMN_DEFINITIONS_TABLE_NAME + " WHERE " + elemKey
        + " = ?";
    String[] selArgs2 = { testColItems };
    cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs2);

    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(elemName);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_STRING);
      String valStr = cursor.getString(ind);
      assertEquals(valStr, itemsStr);
    }

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test creation of user defined database table with column when column is
   * array
   */
  public void testCreateOrOpenDbTableWithColumnWhenColumnIsBoolean_ExpectPass() {
    String tableId = testTable;
    String testCol = "testColumn";
    String testColType = ElementDataType.bool.name();
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[]"));
    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
        .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);

    OrderedColumns coldefs = ODKDatabaseImplUtils.get().getUserDefinedColumns(db, getAppName(), tableId);
    assertEquals(coldefs.getColumnDefinitions().size(), 1);
    assertEquals(coldefs.getColumnDefinitions().get(0).getElementKey(), testCol);

    for (ColumnDefinition col : coldefs.getColumnDefinitions()) {
      String key = col.getElementKey();
      String name = col.getElementName();
      String type = col.getElementType();
      assertTrue(key.equals(testCol));
      assertTrue(name.equals(testCol));
      assertTrue(type.equals(testColType));
    }

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test creation of user defined database table with column when column is
   * string
   */
  public void testCreateOrOpenDbTableWithColumnWhenColumnIsString_ExpectPass() {
    String tableId = testTable;
    String testCol = "testColumn";
    String testColType = ElementDataType.string.name();
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[]"));
    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
        .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);

    OrderedColumns coldefs = ODKDatabaseImplUtils.get().getUserDefinedColumns(db, getAppName(), tableId);
    assertEquals(coldefs.getColumnDefinitions().size(), 1);
    assertEquals(coldefs.getColumnDefinitions().get(0).getElementKey(), testCol);

    for (ColumnDefinition col : coldefs.getColumnDefinitions()) {
      String key = col.getElementKey();
      String name = col.getElementName();
      String type = col.getElementType();
      assertTrue(key.equals(testCol));
      assertTrue(name.equals(testCol));
      assertTrue(type.equals(testColType));
    }

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test creation of user defined database table with column when column is
   * date
   */
  public void testCreateOrOpenDbTableWithColumnWhenColumnIsDate_ExpectPass() {
    String tableId = testTable;
    String testCol = "testColumn";
    String testColType = ElementType.DATE;
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[]"));
    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
        .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);

    OrderedColumns coldefs = ODKDatabaseImplUtils.get().getUserDefinedColumns(db, getAppName(), tableId);
    assertEquals(coldefs.getColumnDefinitions().size(), 1);
    assertEquals(coldefs.getColumnDefinitions().get(0).getElementKey(), testCol);

    for (ColumnDefinition col : coldefs.getColumnDefinitions()) {
      String key = col.getElementKey();
      String name = col.getElementName();
      String type = col.getElementType();
      assertTrue(key.equals(testCol));
      assertTrue(name.equals(testCol));
      assertTrue(type.equals(testColType));
    }

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test creation of user defined database table with column when column is
   * datetime
   */
  public void testCreateOrOpenDbTableWithColumnWhenColumnIsDateTime_ExpectPass() {
    String tableId = testTable;
    String testCol = "testColumn";
    String testColType = ElementType.DATETIME;
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[]"));
    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
        .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);

    OrderedColumns coldefs = ODKDatabaseImplUtils.get().getUserDefinedColumns(db, getAppName(), tableId);
    assertEquals(coldefs.getColumnDefinitions().size(), 1);
    assertEquals(coldefs.getColumnDefinitions().get(0).getElementKey(), testCol);

    for (ColumnDefinition col : coldefs.getColumnDefinitions()) {
      String key = col.getElementKey();
      String name = col.getElementName();
      String type = col.getElementType();
      assertTrue(key.equals(testCol));
      assertTrue(name.equals(testCol));
      assertTrue(type.equals(testColType));
    }

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test creation of user defined database table with column when column is
   * time
   */
  public void testCreateOrOpenDbTableWithColumnWhenColumnIsTime_ExpectPass() {
    String tableId = testTable;
    String testCol = "testColumn";
    String testColType = ElementType.TIME;
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[]"));
    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
        .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);

    OrderedColumns coldefs = ODKDatabaseImplUtils.get().getUserDefinedColumns(db, getAppName(), tableId);
    assertEquals(coldefs.getColumnDefinitions().size(), 1);
    assertEquals(coldefs.getColumnDefinitions().get(0).getElementKey(), testCol);

    for (ColumnDefinition col : coldefs.getColumnDefinitions()) {
      String key = col.getElementKey();
      String name = col.getElementName();
      String type = col.getElementType();
      assertTrue(key.equals(testCol));
      assertTrue(name.equals(testCol));
      assertTrue(type.equals(testColType));
    }

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test creation of user defined database table with column when column is
   * geopoint
   */
  public void testCreateOrOpenDbTableWithColumnWhenColumnIsGeopointLongMissing_ExpectFail() {
    String tableId = testTable;
    String testCol = "testColumn";
    String lat = "latitude";
    String lng = "longitude";
    String alt = "altitude";
    String acc = "accuracy";
    String testColLat = testCol + "_" + lat;
    String testColLng = testCol + "_" + lng;
    String testColAlt = testCol + "_" + alt;
    String testColAcc = testCol + "_" + acc;
    String testColType = ElementType.GEOPOINT;
    String testColResType = ElementDataType.number.name();
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[\"" + testColLat + "\",\"" + testColLng
        + "\",\"" + testColAlt + "\",\"" + testColAcc + "\"]"));
    columns.add(new Column(testColLat, lat, testColResType, "[]"));
    columns.add(new Column(testColAlt, alt, testColResType, "[]"));
    columns.add(new Column(testColAcc, acc, testColResType, "[]"));

    boolean success = false;
    OrderedColumns orderedColumns;

    try {
      orderedColumns = ODKDatabaseImplUtils.get().createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);
      success = true;
    } catch (IllegalArgumentException e) {
      // expected
    }
    assertFalse(success);

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test creation of user defined database table with column when column is
   * geopoint
   */
  public void testCreateOrOpenDbTableWithColumnWhenColumnIsGeopointAltMissing_ExpectFail() {
    String tableId = testTable;
    String testCol = "testColumn";
    String lat = "latitude";
    String lng = "longitude";
    String alt = "altitude";
    String acc = "accuracy";
    String testColLat = testCol + "_" + lat;
    String testColLng = testCol + "_" + lng;
    String testColAlt = testCol + "_" + alt;
    String testColAcc = testCol + "_" + acc;
    String testColType = ElementType.GEOPOINT;
    String testColResType = ElementDataType.number.name();
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[\"" + testColLat + "\",\"" + testColLng
        + "\",\"" + testColAlt + "\",\"" + testColAcc + "\"]"));
    columns.add(new Column(testColLat, lat, testColResType, "[]"));
    columns.add(new Column(testColLng, lng, testColResType, "[]"));
    columns.add(new Column(testColAcc, acc, testColResType, "[]"));

    boolean success = false;
    OrderedColumns orderedColumns;

    try {
      orderedColumns = ODKDatabaseImplUtils.get().createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);
      success = true;
    } catch (IllegalArgumentException e) {
      // expected
    }
    assertFalse(success);

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test creation of user defined database table with column when column is
   * geopoint
   */
  public void testCreateOrOpenDbTableWithColumnWhenColumnIsGeopointAccMissing_ExpectFail() {
    String tableId = testTable;
    String testCol = "testColumn";
    String lat = "latitude";
    String lng = "longitude";
    String alt = "altitude";
    String acc = "accuracy";
    String testColLat = testCol + "_" + lat;
    String testColLng = testCol + "_" + lng;
    String testColAlt = testCol + "_" + alt;
    String testColAcc = testCol + "_" + acc;
    String testColType = ElementType.GEOPOINT;
    String testColResType = ElementDataType.number.name();
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[\"" + testColLat + "\",\"" + testColLng
        + "\",\"" + testColAlt + "\",\"" + testColAcc + "\"]"));
    columns.add(new Column(testColLat, lat, testColResType, "[]"));
    columns.add(new Column(testColLng, lng, testColResType, "[]"));
    columns.add(new Column(testColAlt, alt, testColResType, "[]"));

    boolean success = false;
    OrderedColumns orderedColumns;

    try {
      orderedColumns = ODKDatabaseImplUtils.get().createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);
      success = true;
    } catch (IllegalArgumentException e) {
      // expected
    }
    assertFalse(success);

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test creation of user defined database table with column when column is
   * geopoint and there is no list of children -- this is expected to succeed
   * because we do not have a separate table of data types.
   * 
   * If we registered data types, we could detect the malformedness of the
   * geopoint data type (and we could even create the subelements based off of
   * the known definition of this datatype.
   */
  public void testCreateOrOpenDbTableWithColumnWhenColumnIsGeopointListMissing_ExpectSuccess() {
    String tableId = testTable;
    String testCol = "testColumn";
    String lat = "latitude";
    String lng = "longitude";
    String alt = "altitude";
    String acc = "accuracy";
    String testColLat = testCol + "_" + lat;
    String testColLng = testCol + "_" + lng;
    String testColAlt = testCol + "_" + alt;
    String testColAcc = testCol + "_" + acc;
    String testColType = ElementType.GEOPOINT;
    String testColResType = ElementDataType.number.name();
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[]"));
    columns.add(new Column(testColLat, lat, testColResType, "[]"));
    columns.add(new Column(testColLng, lng, testColResType, "[]"));
    columns.add(new Column(testColAlt, alt, testColResType, "[]"));

    boolean success = false;
    OrderedColumns orderedColumns;

    try {
      orderedColumns = ODKDatabaseImplUtils.get().createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);
      success = true;
    } catch (IllegalArgumentException e) {
      // expected
    }
    assertTrue(success);

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  public void testCreateOrOpenDbTableWithColumnWhenColumnIsGeopointBadChildKey_ExpectFail() {
    String tableId = testTable;
    String testCol = "testColumn";
    String lat = "latitude";
    String lng = "longitude";
    String alt = "altitude";
    String acc = "accuracy";
    String testColLat = testCol + "_" + lat;
    String testColLng = testCol + "d_" + lng;
    String testColAlt = testCol + "_" + alt;
    String testColAcc = testCol + "_" + acc;
    String testColType = ElementType.GEOPOINT;
    String testColResType = ElementDataType.number.name();
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[\"" + testColLat + "\",\"" + testColLng
        + "\",\"" + testColAlt + "\",\"" + testColAcc + "\"]"));
    columns.add(new Column(testColLat, lat, testColResType, "[]"));
    columns.add(new Column(testColLng, lng, testColResType, "[]"));
    columns.add(new Column(testColAlt, alt, testColResType, "[]"));

    boolean success = false;
    OrderedColumns orderedColumns;

    try {
      orderedColumns = ODKDatabaseImplUtils.get().createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);
      success = true;
    } catch (IllegalArgumentException e) {
      // expected
    }
    assertFalse(success);

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test creation of user defined database table with column when column is
   * geopoint
   */
  public void testCreateOrOpenDbTableWithColumnWhenColumnIsGeopointLatMissing_ExpectFail() {
    String tableId = testTable;
    String testCol = "testColumn";
    String lat = "latitude";
    String lng = "longitude";
    String alt = "altitude";
    String acc = "accuracy";
    String testColLat = testCol + "_" + lat;
    String testColLng = testCol + "_" + lng;
    String testColAlt = testCol + "_" + alt;
    String testColAcc = testCol + "_" + acc;
    String testColType = ElementType.GEOPOINT;
    String testColResType = ElementDataType.number.name();
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[\"" + testColLat + "\",\"" + testColLng
        + "\",\"" + testColAlt + "\",\"" + testColAcc + "\"]"));
    columns.add(new Column(testColLng, lng, testColResType, "[]"));
    columns.add(new Column(testColAlt, alt, testColResType, "[]"));
    columns.add(new Column(testColAcc, acc, testColResType, "[]"));

    boolean success = false;
    OrderedColumns orderedColumns;

    try {
      orderedColumns = ODKDatabaseImplUtils.get().createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);
      success = true;
    } catch (IllegalArgumentException e) {
      // expected
    }
    assertFalse(success);

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test creation of user defined database table with column when column is
   * geopoint
   */
  public void testCreateOrOpenDbTableWithColumnWhenColumnIsGeopoint_ExpectPass() {
    String tableId = testTable;
    String testCol = "testColumn";
    String lat = "latitude";
    String lng = "longitude";
    String alt = "altitude";
    String acc = "accuracy";
    String testColLat = testCol + "_" + lat;
    String testColLng = testCol + "_" + lng;
    String testColAlt = testCol + "_" + alt;
    String testColAcc = testCol + "_" + acc;
    String testColType = ElementType.GEOPOINT;
    String testColResType = ElementDataType.number.name();
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[\"" + testColLat + "\",\"" + testColLng
            + "\",\"" + testColAlt + "\",\"" + testColAcc + "\"]"));
    columns.add(new Column(testColLat, lat, testColResType, "[]"));
    columns.add(new Column(testColLng, lng, testColResType, "[]"));
    columns.add(new Column(testColAlt, alt, testColResType, "[]"));
    columns.add(new Column(testColAcc, acc, testColResType, "[]"));
    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
        .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);

    OrderedColumns coldefs = ODKDatabaseImplUtils.get().getUserDefinedColumns(db, getAppName(), tableId);
    assertEquals(coldefs.getColumnDefinitions().size(), 5);
    assertEquals(coldefs.getColumnDefinitions().get(0).getElementKey(), testCol);
    assertEquals(coldefs.getColumnDefinitions().get(1).getElementKey(), testColAcc);
    assertEquals(coldefs.getColumnDefinitions().get(2).getElementKey(), testColAlt);
    assertEquals(coldefs.getColumnDefinitions().get(3).getElementKey(), testColLat);
    assertEquals(coldefs.getColumnDefinitions().get(4).getElementKey(), testColLng);

    List<String> cols = new ArrayList<String>();
    cols.add(lat);
    cols.add(lng);
    cols.add(alt);
    cols.add(acc);

    for (ColumnDefinition col : coldefs.getColumnDefinitions()) {
      String key = col.getElementKey();
      String name = col.getElementName();
      String type = col.getElementType();
      if (key.equals(testCol)) {
        assertTrue(key.equals(testCol));
        assertTrue(name.equals(testCol));
        assertTrue(type.equals(testColType));
      } else {
        assertTrue(key.equals(testCol + "_" + name));
        assertTrue(cols.contains(name));
        assertTrue(type.equals(testColResType));
      }
    }

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test creation of user defined database table with column when column is
   * mimeUri
   */
  public void testCreateOrOpenDbTableWithColumnWhenColumnIsMimeUri_ExpectPass() {
    String tableId = testTable;
    String testCol = "testColumn";
    String uriFrag = "uriFragment";
    String conType = "contentType";
    String testColUriFrag = testCol + "_" + uriFrag;
    String testColContType = testCol + "_" + conType;
    String testColType = DataTypeNamesToRemove.MIMEURI;

    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[\"" + testColUriFrag + "\",\""
        + testColContType + "\"]"));
    columns.add(new Column(testColUriFrag, "uriFragment", ElementDataType.rowpath.name(), "[]"));
    columns.add(new Column(testColContType, "contentType", ElementDataType.string.name(), "[]"));
    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
        .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);

    OrderedColumns coldefs = ODKDatabaseImplUtils.get().getUserDefinedColumns(db, getAppName(), tableId);
    assertEquals(coldefs.getColumnDefinitions().size(), 3);
    assertEquals(coldefs.getColumnDefinitions().get(0).getElementKey(), testCol);
    assertEquals(coldefs.getColumnDefinitions().get(1).getElementKey(), testColContType);
    assertEquals(coldefs.getColumnDefinitions().get(2).getElementKey(), testColUriFrag);

    List<String> cols = new ArrayList<String>();
    cols.add(uriFrag);
    cols.add(conType);

    for (ColumnDefinition col : coldefs.getColumnDefinitions()) {
      String key = col.getElementKey();
      String name = col.getElementName();
      String type = col.getElementType();
      if (key.equals(testCol)) {
        assertTrue(key.equals(testCol));
        assertTrue(name.equals(testCol));
        assertTrue(type.equals(testColType));
      } else {
        assertTrue(key.equals(testCol + "_" + name));
        assertTrue(cols.contains(name));
        if (name.equals(uriFrag)) {
          assertTrue(type.equals(ElementDataType.rowpath.name()));
        } else {
          assertTrue(type.equals(ElementDataType.string.name()));
        }
      }
    }

    // Select everything out of the table for element key
    String sel = "SELECT * FROM " + DatabaseConstants.COLUMN_DEFINITIONS_TABLE_NAME + " WHERE "
        + elemKey + " = ?";
    String[] selArgs = { "" + testCol };
    Cursor cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);

    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(listChildElemKeys);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_STRING);
      String valStr = cursor.getString(ind);
      String testVal = "[\"" + testColUriFrag + "\",\"" + testColContType + "\"]";
      assertEquals(valStr, testVal);
    }

    // Select everything out of the table for uriFragment
    sel = "SELECT * FROM " + DatabaseConstants.COLUMN_DEFINITIONS_TABLE_NAME + " WHERE " + elemKey
        + " = ?";
    String[] selArgs2 = { testColUriFrag };
    cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs2);

    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(elemName);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_STRING);
      String valStr = cursor.getString(ind);
      assertEquals(valStr, uriFrag);
    }

    // Select everything out of the table for contentType
    sel = "SELECT * FROM " + DatabaseConstants.COLUMN_DEFINITIONS_TABLE_NAME + " WHERE " + elemKey
        + " = ?";
    String[] selArgs3 = { testColContType };
    cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs3);

    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(elemName);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_STRING);
      String valStr = cursor.getString(ind);
      assertEquals(valStr, conType);
    }

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test getting all column names when columns exist
   */
  public void testGetAllColumnNamesWhenColumnsExist_ExpectPass() {
    String tableId = testTable;
    List<Column> columns = new ArrayList<Column>();
    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
        .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);

    String[] colNames = ODKDatabaseImplUtils.get().getAllColumnNames(db, tableId);
    boolean colLength = (colNames.length > 0);
    assertTrue(colLength);
    Arrays.sort(colNames);

    List<String> defCols = ODKDatabaseImplUtils.get().getAdminColumns();

    assertEquals(colNames.length, defCols.size());
    for (int i = 0; i < colNames.length; i++) {
      assertEquals(colNames[i], defCols.get(i));
    }

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test getting all column names when table does not exist
   */
  public void testGetAllColumnNamesWhenTableDoesNotExist_ExpectFail() {
    String tableId = testTable;
    boolean thrown = false;

    try {
      String[] results = ODKDatabaseImplUtils.get().getAllColumnNames(db, tableId);
    } catch (Exception e) {
      thrown = true;
      e.printStackTrace();
    }

    assertTrue(thrown);
  }

  /*
   * Test getting user defined column names when columns exist
   */
  public void testGetUserDefinedColumnNamesWhenColumnsExist_ExpectPass() {
    String tableId = testTable;
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column("testCol", "testCol", "string", "[]"));
    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
        .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);

    OrderedColumns defns = ODKDatabaseImplUtils.get().getUserDefinedColumns(db, getAppName(), tableId);

    assertEquals(defns.getColumnDefinitions().size(), 1);
    assertEquals(columns.size(), 1);
    Column cdref = ColumnDefinition.getColumns(defns.getColumnDefinitions()).get(0);
    Column cdalt = columns.get(0);
    assertEquals(cdref, cdalt);

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test getting user defined column names when column does not exist
   */
  public void testGetUserDefinedColumnNamesWhenColumnDoesNotExist_ExpectPass() {
    String tableId = testTable;
    OrderedColumns defns = ODKDatabaseImplUtils.get().getUserDefinedColumns(db, getAppName(), tableId);

    assertTrue(defns.getColumnDefinitions().isEmpty());
  }

  /*
   * Test getting user defined column names when table does not exist
   */
  public void testGetUserDefinedColumnNamesWhenTableDoesNotExist_ExpectPass() {
    String tableId = testTable;
    OrderedColumns defns = ODKDatabaseImplUtils.get().getUserDefinedColumns(db, getAppName(), tableId);
    assertTrue(defns.getColumnDefinitions().isEmpty());
  }

  /*
   * Test writing the data into the existing db table with all null values
   */
  public void testWriteDataIntoExisitingDbTableWithAllNullValues_ExpectFail() {
    String tableId = testTable;
    String testCol = "testColumn";
    String testColType = ElementDataType.integer.name();
    boolean thrown = false;
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[]"));
    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
        .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);

    try {
      ODKDatabaseImplUtils.get().insertRowWithId(db, tableId, orderedColumns, null, null);
    } catch (Exception e) {
      thrown = true;
      e.printStackTrace();
    }

    assertTrue(thrown);

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test writing the data into the existing db table with valid values
   */
  public void testWriteDataIntoExisitingDbTableWithValidValue_ExpectPass() {
    String tableId = testTable;
    String testCol = "testColumn";
    String testColType = ElementDataType.integer.name();
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[]"));
    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
        .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);
    int testVal = 5;

    ContentValues cvValues = new ContentValues();
    cvValues.put(testCol, testVal);
    ODKDatabaseImplUtils.get().insertRowWithId(db, tableId, orderedColumns, cvValues,
        ODKDataUtils.genUUID());

    // Select everything out of the table
    String sel = "SELECT * FROM " + tableId + " WHERE " + testCol + " = ?";
    String[] selArgs = { "" + testVal };
    Cursor cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);

    int val = 0;
    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(testCol);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_INTEGER);
      val = cursor.getInt(ind);
    }

    assertEquals(val, testVal);

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test writing the data into the existing db table with valid values and a
   * certain id
   */
  public void testWriteDataIntoExisitingDbTableWithIdWhenIdDoesNotExist_ExpectPass() {
    String tableId = testTable;
    String testCol = "testColumn";
    String testColType = ElementDataType.integer.name();
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[]"));
    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
        .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);
    int testVal = 5;

    ContentValues cvValues = new ContentValues();
    cvValues.put(testCol, testVal);

    String uuid = UUID.randomUUID().toString();
    ODKDatabaseImplUtils.get().insertRowWithId(db, tableId, orderedColumns, cvValues, uuid);

    // Select everything out of the table
    String sel = "SELECT * FROM " + tableId + " WHERE " + testCol + " = ?";
    String[] selArgs = { "" + testVal };
    Cursor cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);
    assertEquals(cursor.getCount(), 1);

    int val = 0;
    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(testCol);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_INTEGER);
      val = cursor.getInt(ind);
    }

    assertEquals(val, testVal);

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test writing the data into the existing db table with valid values and an
   * existing id
   */
  public void testWriteDataIntoExisitingDbTableWithIdWhenIdAlreadyExists_ExpectPass() {
    String tableId = testTable;
    String testCol = "testColumn";
    String testColType = ElementDataType.integer.name();
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[]"));
    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
        .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);

    int testVal = 5;
    boolean thrown = false;

    ContentValues cvValues = new ContentValues();
    cvValues.put(testCol, testVal);

    String uuid = UUID.randomUUID().toString();
    ODKDatabaseImplUtils.get().insertRowWithId(db, tableId, orderedColumns, cvValues, uuid);

    // Select everything out of the table
    String sel = "SELECT * FROM " + tableId + " WHERE " + testCol + " = ?";
    String[] selArgs = { "" + testVal };
    Cursor cursor = null;
     try {
        cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);
        assertEquals(cursor.getCount(), 1);

        int val = 0;
        while (cursor.moveToNext()) {
           int ind = cursor.getColumnIndex(testCol);
           int type = cursor.getType(ind);
           assertEquals(type, Cursor.FIELD_TYPE_INTEGER);
           val = cursor.getInt(ind);
        }

        assertEquals(val, testVal);
     } finally {
        if ( cursor != null && !cursor.isClosed() ) {
           cursor.close();
        }
     }
    // Try updating that row in the database
    int testVal2 = 25;
    ContentValues cvValues2 = new ContentValues();
    cvValues2.put(testCol, testVal2);

    try {
      ODKDatabaseImplUtils.get().insertRowWithId(db, tableId, orderedColumns, cvValues2, uuid);
    } catch (IllegalArgumentException e) {
      thrown = true;
      e.printStackTrace();
    }

    assertEquals(thrown, true);

     /**
      * NOTE: we expect the log to report a failure to close this cursor.
      * It is GC'd and closed in its finalizer. This is confirming that
      * the finalizer is doing the right thing.
      */

    // Select everything out of the table
    String sel2 = "SELECT * FROM " + tableId;
    String[] selArgs2 = {};
    Cursor cursor2 = ODKDatabaseImplUtils.get().rawQuery(db, sel2, selArgs2);
    assertEquals(cursor2.getCount(), 1);

    int val2 = 0;
    while (cursor2.moveToNext()) {
      int ind = cursor2.getColumnIndex(testCol);
      int type = cursor2.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_INTEGER);
      val2 = cursor2.getInt(ind);
    }

    assertEquals(val2, testVal);

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test updating the data in an existing db table with valid values when the
   * id does not exist
   */
  public void testUpdateDataInExistingDBTableWithIdWhenIdDoesNotExist_ExpectPass() {

    String tableId = testTable;
    String testCol = "testColumn";
    String testColType = ElementDataType.integer.name();
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[]"));
    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
        .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);

    int testVal = 5;

    ContentValues cvValues = new ContentValues();
    cvValues.put(testCol, testVal);

    String uuid = UUID.randomUUID().toString();
    ODKDatabaseImplUtils.get().updateRowWithId(db, tableId, orderedColumns, cvValues, uuid);

    // Select everything out of the table
    String sel = "SELECT * FROM " + tableId + " WHERE " + testCol + " = ?";
    String[] selArgs = { "" + testVal };
    Cursor cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);
    assertEquals(cursor.getCount(), 1);

    int val = 0;
    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(testCol);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_INTEGER);
      val = cursor.getInt(ind);
    }

    assertEquals(val, testVal);

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test updating the data in the existing db table with valid values when the
   * id already exists
   */
  public void testUpdateDataInExistingDBTableWithIdWhenIdAlreadyExists_ExpectPass() {
    String tableId = testTable;
    String testCol = "testColumn";
    String testColType = ElementDataType.integer.name();
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[]"));
    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
        .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);

    int testVal = 5;
    boolean thrown = false;

    ContentValues cvValues = new ContentValues();
    cvValues.put(testCol, testVal);

    String uuid = UUID.randomUUID().toString();
    ODKDatabaseImplUtils.get().updateRowWithId(db, tableId, orderedColumns, cvValues, uuid);

    // Select everything out of the table
    String sel = "SELECT * FROM " + tableId + " WHERE " + testCol + " = ?";
    String[] selArgs = { "" + testVal };
    Cursor cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);
    assertEquals(cursor.getCount(), 1);

    int val = 0;
    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(testCol);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_INTEGER);
      val = cursor.getInt(ind);
    }

    assertEquals(val, testVal);

    // Try updating that row in the database
    int testVal2 = 25;
    ContentValues cvValues2 = new ContentValues();
    cvValues2.put(testCol, testVal2);

    ODKDatabaseImplUtils.get().updateRowWithId(db, tableId, orderedColumns, cvValues2, uuid);

    // Select everything out of the table
    String sel2 = "SELECT * FROM " + tableId;
    String[] selArgs2 = {};
    Cursor cursor2 = ODKDatabaseImplUtils.get().rawQuery(db, sel2, selArgs2);
    assertEquals(cursor2.getCount(), 1);

    int val2 = 0;
    while (cursor2.moveToNext()) {
      int ind = cursor2.getColumnIndex(testCol);
      int type = cursor2.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_INTEGER);
      val2 = cursor2.getInt(ind);
    }

    assertEquals(val2, testVal2);

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test writing the data into the existing db table with valid values and an
   * existing id
   */
  public void testWriteDataIntoExisitingDbTableWithIdWhenIdIsNull_ExpectFail() {
    String tableId = testTable;
    String testCol = "testColumn";
    String testColType = ElementDataType.integer.name();
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[]"));
    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
        .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);

    int testVal = 5;
    boolean thrown = false;

    ContentValues cvValues = new ContentValues();
    cvValues.put(testCol, testVal);

    String uuid = null;
    try {
      ODKDatabaseImplUtils.get().insertRowWithId(db, tableId, orderedColumns, cvValues, uuid);
    } catch (Exception e) {
      thrown = true;
      e.printStackTrace();
    }

    assertTrue(thrown);

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test writing the data and metadata into the existing db table with valid
   * values
   */
  public void testWriteDataAndMetadataIntoExistingDBTableWithValidValue_ExpectPass() {
    String tableId = testTable;
    String nullString = null;
    String testColType = ElementDataType.string.name();
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column("col1", "col1", testColType, "[]"));
    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
        .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);

    String uuid = UUID.randomUUID().toString();
    String timeStamp = TableConstants.nanoSecondsFromMillis(System.currentTimeMillis());

    ContentValues cvValues = new ContentValues();
    cvValues.put(DataTableColumns.ID, uuid);
    cvValues.put(DataTableColumns.ROW_ETAG, nullString);
    cvValues.put(DataTableColumns.SYNC_STATE, SyncState.new_row.name());
    cvValues.put(DataTableColumns.CONFLICT_TYPE, nullString);
    cvValues.put(DataTableColumns.FILTER_TYPE, nullString);
    cvValues.put(DataTableColumns.FILTER_VALUE, nullString);
    cvValues.put(DataTableColumns.FORM_ID, nullString);
    cvValues.put(DataTableColumns.LOCALE, nullString);
    cvValues.put(DataTableColumns.SAVEPOINT_TYPE, nullString);
    cvValues.put(DataTableColumns.SAVEPOINT_TIMESTAMP, timeStamp);
    cvValues.put(DataTableColumns.SAVEPOINT_CREATOR, nullString);

    ODKDatabaseImplUtils.get().insertRowWithId(db, tableId, orderedColumns, cvValues, uuid);

    // Select everything out of the table
    String sel = "SELECT * FROM " + tableId + " WHERE " + DataTableColumns.ID + " = ?";
    String[] selArgs = { uuid };
    Cursor cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);

    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(DataTableColumns.SAVEPOINT_TIMESTAMP);
      String ts = cursor.getString(ind);
      assertEquals(ts, timeStamp);

      ind = cursor.getColumnIndex(DataTableColumns.SYNC_STATE);
      String ss = cursor.getString(ind);
      assertEquals(ss, SyncState.new_row.name());
    }

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test writing writing metadata into an existing table when the rowID is null
   */
  public void testWriteDataAndMetadataIntoExistingDBTableWhenIDIsNull_ExpectFail() {
    String tableId = testTable;
    String nullString = null;
    boolean thrown = false;
    String testColType = ElementDataType.string.name();
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column("col1", "col1", testColType, "[]"));
    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
        .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);

    String timeStamp = TableConstants.nanoSecondsFromMillis(System.currentTimeMillis());

    ContentValues cvValues = new ContentValues();
    cvValues.put(DataTableColumns.ID, nullString);
    cvValues.put(DataTableColumns.ROW_ETAG, nullString);
    cvValues.put(DataTableColumns.SYNC_STATE, SyncState.new_row.name());
    cvValues.put(DataTableColumns.CONFLICT_TYPE, nullString);
    cvValues.put(DataTableColumns.FILTER_TYPE, nullString);
    cvValues.put(DataTableColumns.FILTER_VALUE, nullString);
    cvValues.put(DataTableColumns.FORM_ID, nullString);
    cvValues.put(DataTableColumns.LOCALE, nullString);
    cvValues.put(DataTableColumns.SAVEPOINT_TYPE, nullString);
    cvValues.put(DataTableColumns.SAVEPOINT_TIMESTAMP, timeStamp);
    cvValues.put(DataTableColumns.SAVEPOINT_CREATOR, nullString);

    try {
      ODKDatabaseImplUtils.get().insertRowWithId(db, tableId, orderedColumns, cvValues, nullString);
    } catch (Exception e) {
      thrown = true;
      e.printStackTrace();
    }

    assertTrue(thrown);

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test writing metadata into the existing db table when sync state is null.
   * The sync state and other fields that should not be null will be silently
   * replaced with non-null values.
   */
  public void testWriteDataAndMetadataIntoExistingDBTableWhenSyncStateIsNull_ExpectSuccess() {
    String tableId = testTable;
    String nullString = null;
    boolean thrown = false;
    String testColType = ElementDataType.string.name();
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column("col1", "col1", testColType, "[]"));
    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
        .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);

    String uuid = UUID.randomUUID().toString();
    String timeStamp = TableConstants.nanoSecondsFromMillis(System.currentTimeMillis());

    ContentValues cvValues = new ContentValues();
    cvValues.put(DataTableColumns.ID, uuid);
    cvValues.put(DataTableColumns.ROW_ETAG, nullString);
    cvValues.put(DataTableColumns.SYNC_STATE, nullString);
    cvValues.put(DataTableColumns.CONFLICT_TYPE, nullString);
    cvValues.put(DataTableColumns.FILTER_TYPE, nullString);
    cvValues.put(DataTableColumns.FILTER_VALUE, nullString);
    cvValues.put(DataTableColumns.FORM_ID, nullString);
    cvValues.put(DataTableColumns.LOCALE, nullString);
    cvValues.put(DataTableColumns.SAVEPOINT_TYPE, nullString);
    cvValues.put(DataTableColumns.SAVEPOINT_TIMESTAMP, timeStamp);
    cvValues.put(DataTableColumns.SAVEPOINT_CREATOR, nullString);

    try {
      ODKDatabaseImplUtils.get().insertRowWithId(db, tableId, orderedColumns, cvValues, uuid);
    } catch (Exception e) {
      thrown = true;
      e.printStackTrace();
    }

    assertFalse(thrown);

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test writing metadata into the existing db table when sync state is null
   */
  public void testWriteDataAndMetadataIntoExistingDBTableWhenTimeStampIsNull_ExpectFail() {
    // TODO: should this fail or succeed?
    String tableId = testTable;
    String nullString = null;
    boolean thrown = false;

    String testColType = ElementDataType.string.name();
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column("col1", "col1", testColType, "[]"));
    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
        .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);

    String uuid = UUID.randomUUID().toString();

    ContentValues cvValues = new ContentValues();
    cvValues.put(DataTableColumns.ID, uuid);
    cvValues.put(DataTableColumns.ROW_ETAG, nullString);
    cvValues.put(DataTableColumns.SYNC_STATE, SyncState.new_row.name());
    cvValues.put(DataTableColumns.CONFLICT_TYPE, nullString);
    cvValues.put(DataTableColumns.FILTER_TYPE, nullString);
    cvValues.put(DataTableColumns.FILTER_VALUE, nullString);
    cvValues.put(DataTableColumns.FORM_ID, nullString);
    cvValues.put(DataTableColumns.LOCALE, nullString);
    cvValues.put(DataTableColumns.SAVEPOINT_TYPE, nullString);
    cvValues.put(DataTableColumns.SAVEPOINT_TIMESTAMP, nullString);
    cvValues.put(DataTableColumns.SAVEPOINT_CREATOR, nullString);

    try {
      ODKDatabaseImplUtils.get().insertRowWithId(db, tableId, orderedColumns, cvValues, uuid);
    } catch (Exception e) {
      thrown = true;
      e.printStackTrace();
    }

    // TODO: should this fail or succeed?
    // assertTrue(thrown);
    assertFalse(thrown);

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test writing the data into the existing db table with array value
   */
  public void testWriteDataIntoExisitingDbTableWithArray_ExpectPass() {
    String tableId = testTable;
    String testCol = "testColumn";
    String testColType = ElementDataType.array.name();
    String testVal = "item";

    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[\"" + testCol + "_items\"]"));
    columns.add(new Column(testCol + "_items", "items", ElementDataType.string.name(), "[]"));
    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
        .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);

    ContentValues cvValues = new ContentValues();
    cvValues.put(testCol, testVal);
    ODKDatabaseImplUtils.get().insertRowWithId(db, tableId, orderedColumns, cvValues,
        ODKDataUtils.genUUID());

    // Select everything out of the table
    String sel = "SELECT * FROM " + tableId + " WHERE " + testCol + " = ?";
    String[] selArgs = { "" + testVal };
    Cursor cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);

    String val = null;
    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(testCol);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_STRING);
      val = cursor.getString(ind);
    }

    assertEquals(val, testVal);

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test writing the data into the existing db table with boolean value
   */
  public void testWriteDataIntoExisitingDbTableWithBoolean_ExpectPass() {
    String tableId = testTable;
    String testCol = "testColumn";
    String testColType = ElementDataType.bool.name();
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[]"));
    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
        .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);

    int testVal = 1;

    ContentValues cvValues = new ContentValues();
    cvValues.put(testCol, testVal);
    ODKDatabaseImplUtils.get().insertRowWithId(db, tableId, orderedColumns, cvValues,
        ODKDataUtils.genUUID());

    // Select everything out of the table
    String sel = "SELECT * FROM " + tableId + " WHERE " + testCol + " = ?";
    String[] selArgs = { "" + testVal };
    Cursor cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);

    int val = 0;
    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(testCol);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_INTEGER);
      val = cursor.getInt(ind);
    }

    assertEquals(val, testVal);

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test writing the data into the existing db table with valid values
   */
  public void testWriteDataIntoExisitingDbTableWithDate_ExpectPass() {
    String tableId = testTable;
    String testCol = "testColumn";
    String testColType = ElementType.DATE;
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[]"));
    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
        .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);

    String testVal = TableConstants.nanoSecondsFromMillis(System.currentTimeMillis());

    ContentValues cvValues = new ContentValues();
    cvValues.put(testCol, testVal);
    ODKDatabaseImplUtils.get().insertRowWithId(db, tableId, orderedColumns, cvValues,
        ODKDataUtils.genUUID());

    // Select everything out of the table
    String sel = "SELECT * FROM " + tableId + " WHERE " + testCol + " = ?";
    String[] selArgs = { "" + testVal };
    Cursor cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);

    String val = null;
    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(testCol);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_STRING);
      val = cursor.getString(ind);
    }

    assertEquals(val, testVal);

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test writing the data into the existing db table with datetime
   */
  public void testWriteDataIntoExisitingDbTableWithDatetime_ExpectPass() {
    String tableId = testTable;
    String testCol = "testColumn";
    String testColType = ElementType.DATETIME;
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[]"));
    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
        .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);

    String testVal = TableConstants.nanoSecondsFromMillis(System.currentTimeMillis());

    ContentValues cvValues = new ContentValues();
    cvValues.put(testCol, testVal);
    ODKDatabaseImplUtils.get().insertRowWithId(db, tableId, orderedColumns, cvValues,
        ODKDataUtils.genUUID());

    // Select everything out of the table
    String sel = "SELECT * FROM " + tableId + " WHERE " + testCol + " = ?";
    String[] selArgs = { "" + testVal };
    Cursor cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);

    String val = null;
    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(testCol);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_STRING);
      val = cursor.getString(ind);
    }

    assertEquals(val, testVal);

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test writing the data into the existing db table with geopoint
   */
  public void testWriteDataIntoExisitingDbTableWithGeopoint_ExpectPass() {
    String tableId = testTable;
    String testCol = "testColumn";
    String testColLat = "testColumn_latitude";
    String testColLong = "testColumn_longitude";
    String testColAlt = "testColumn_altitude";
    String testColAcc = "testColumn_accuracy";
    double pos_lat = 5.55;
    double pos_long = 6.6;
    double pos_alt = 7.77;
    double pos_acc = 8.88;
    String testColType = ElementType.GEOPOINT;
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[\"" + testColLat + "\",\""
        + testColLong + "\",\"" + testColAlt + "\",\"" + testColAcc + "\"]"));
    columns.add(new Column(testColLat, "latitude", ElementDataType.number.name(), "[]"));
    columns.add(new Column(testColLong, "longitude", ElementDataType.number.name(), "[]"));
    columns.add(new Column(testColAlt, "altitude", ElementDataType.number.name(), "[]"));
    columns.add(new Column(testColAcc, "accuracy", ElementDataType.number.name(), "[]"));
    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
        .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);

    ContentValues cvValues = new ContentValues();
    cvValues.put(testColLat, pos_lat);
    cvValues.put(testColLong, pos_long);
    cvValues.put(testColAlt, pos_alt);
    cvValues.put(testColAcc, pos_acc);

    ODKDatabaseImplUtils.get().insertRowWithId(db, tableId, orderedColumns, cvValues,
        ODKDataUtils.genUUID());

    // Select everything out of the table
    String sel = "SELECT * FROM " + tableId + " WHERE " + testColLat + " = ?";
    String[] selArgs = { "" + pos_lat };
    Cursor cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);

    double valLat = 0;
    double valLong = 0;
    double valAlt = 0;
    double valAcc = 0;
    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(testColLat);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_FLOAT);
      valLat = cursor.getDouble(ind);

      ind = cursor.getColumnIndex(testColLong);
      type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_FLOAT);
      valLong = cursor.getDouble(ind);

      ind = cursor.getColumnIndex(testColAlt);
      type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_FLOAT);
      valAlt = cursor.getDouble(ind);

      ind = cursor.getColumnIndex(testColAcc);
      type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_FLOAT);
      valAcc = cursor.getDouble(ind);
    }

    assertEquals(valLat, pos_lat);
    assertEquals(valLong, pos_long);
    assertEquals(valAlt, pos_alt);
    assertEquals(valAcc, pos_acc);

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test writing the data into the existing db table with integer
   */
  public void testWriteDataIntoExisitingDbTableWithInteger_ExpectPass() {
    String tableId = testTable;
    String testCol = "testColumn";
    String testColType = ElementDataType.integer.name();
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[]"));
    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
        .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);

    int testVal = 5;

    ContentValues cvValues = new ContentValues();
    cvValues.put(testCol, testVal);
    ODKDatabaseImplUtils.get().insertRowWithId(db, tableId, orderedColumns, cvValues,
        ODKDataUtils.genUUID());

    // Select everything out of the table
    String sel = "SELECT * FROM " + tableId + " WHERE " + testCol + " = ?";
    String[] selArgs = { "" + testVal };
    Cursor cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);

    int val = 0;
    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(testCol);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_INTEGER);
      val = cursor.getInt(ind);
    }

    assertEquals(val, testVal);

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test writing the data into the existing db table with mimeUri
   */
  public void testWriteDataIntoExisitingDbTableWithMimeUri_ExpectPass() {
    String tableId = testTable;
    String testCol = "testColumn";
    String testColUriFragment = "testColumn_uriFragment";
    String testColContentType = "testColumn_contentType";
    String testColType = DataTypeNamesToRemove.MIMEURI;

    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[\"" + testColUriFragment + "\",\""
        + testColContentType + "\"]"));
    columns
        .add(new Column(testColUriFragment, "uriFragment", ElementDataType.rowpath.name(), "[]"));
    columns.add(new Column(testColContentType, "contentType", ElementDataType.string.name(), "[]"));
    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
        .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);

    String uuid = UUID.randomUUID().toString();

    String testUriFragment = "tables/example/instances/" + uuid + "/" + testCol + "-" + uuid
        + ".jpg";
    String testContentType = "image/jpg";

    ContentValues cvValues = new ContentValues();
    cvValues.put(testColUriFragment, testUriFragment);
    cvValues.put(testColContentType, testContentType);
    ODKDatabaseImplUtils.get().insertRowWithId(db, tableId, orderedColumns, cvValues, uuid);

    // Select everything out of the table
    String sel = "SELECT * FROM " + tableId + " WHERE " + testColUriFragment + " = ?";
    String[] selArgs = { "" + testUriFragment };
    Cursor cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);

    String valUriFragment = null;
    String valContentType = null;
    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(testColUriFragment);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_STRING);
      valUriFragment = cursor.getString(ind);

      ind = cursor.getColumnIndex(testColContentType);
      type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_STRING);
      valContentType = cursor.getString(ind);
    }

    assertEquals(valUriFragment, testUriFragment);
    assertEquals(valContentType, testContentType);

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test writing the data into the existing db table with number
   */
  public void testWriteDataIntoExisitingDbTableWithNumber_ExpectPass() {
    String tableId = testTable;
    String testCol = "testColumn";
    String testColType = ElementDataType.number.name();
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[]"));
    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
        .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);

    double testVal = 5.5;

    ContentValues cvValues = new ContentValues();
    cvValues.put(testCol, testVal);
    ODKDatabaseImplUtils.get().insertRowWithId(db, tableId, orderedColumns, cvValues,
        ODKDataUtils.genUUID());

    // Select everything out of the table
    String sel = "SELECT * FROM " + tableId + " WHERE " + testCol + " = ?";
    String[] selArgs = { "" + testVal };
    Cursor cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);

    double val = 0;
    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(testCol);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_FLOAT);
      val = cursor.getDouble(ind);
    }

    assertEquals(val, testVal);

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test writing the data into the existing db table with string
   */
  public void testWriteDataIntoExisitingDbTableWithString_ExpectPass() {
    String tableId = testTable;
    String testCol = "testColumn";
    String testColType = ElementDataType.string.name();
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[]"));
    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
        .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);

    String testVal = "test";

    ContentValues cvValues = new ContentValues();
    cvValues.put(testCol, testVal);
    ODKDatabaseImplUtils.get().insertRowWithId(db, tableId, orderedColumns, cvValues,
        ODKDataUtils.genUUID());

    // Select everything out of the table
    String sel = "SELECT * FROM " + tableId + " WHERE " + testCol + " = ?";
    String[] selArgs = { "" + testVal };
    Cursor cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);

    String val = null;
    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(testCol);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_STRING);
      val = cursor.getString(ind);
    }

    assertEquals(val, testVal);

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test writing the data into the existing db table with time
   */
  public void testWriteDataIntoExisitingDbTableWithTime_ExpectPass() {
    String tableId = testTable;
    String testCol = "testColumn";
    String testColType = ElementType.TIME;
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[]"));
    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
        .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);

    String interMed = TableConstants.nanoSecondsFromMillis(System.currentTimeMillis());
    int pos = interMed.indexOf('T');
    String testVal = null;

    if (pos > -1) {
      testVal = interMed.substring(pos + 1);
    } else {
      fail("The conversion of the date time string to time is incorrect");
     // Log.i(TAG, "Time string is " + interMed);
    }

   // Log.i(TAG, "Time string is " + testVal);

    ContentValues cvValues = new ContentValues();
    cvValues.put(testCol, testVal);
    ODKDatabaseImplUtils.get().insertRowWithId(db, tableId, orderedColumns, cvValues,
        ODKDataUtils.genUUID());

    // Select everything out of the table
    String sel = "SELECT * FROM " + tableId + " WHERE " + testCol + " = ?";
    String[] selArgs = { "" + testVal };
    Cursor cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);

    String val = null;
    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(testCol);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_STRING);
      val = cursor.getString(ind);
    }

    assertEquals(val, testVal);

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);

  }

  /*
   * Test inserting a checkpoint row in the database
   */
  public void testInsertCheckpointRowIntoExistingDBTableWithIdWhenRowAlreadyExists_ExpectPass() {
    String tableId = testTable;
    String testCol = "testColumn";
    String testColType = ElementDataType.string.name();
    String testVal = "test";
    String testVal2 = "test2";
    String rowId = ODKDataUtils.genUUID();
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[]"));
    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
            .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);


    ContentValues cvValues = new ContentValues();
    cvValues.put(testCol, testVal);
    ODKDatabaseImplUtils.get().insertRowWithId(db, tableId, orderedColumns, cvValues, rowId);

    // Select everything out of the table
    String sel = "SELECT * FROM " + tableId + " WHERE " + testCol + " = ?";
    String[] selArgs = { "" + testVal };
    Cursor cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);

    String val = null;
    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(testCol);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_STRING);
      val = cursor.getString(ind);
    }

    assertEquals(val, testVal);

    ContentValues updatedCvValues = new ContentValues();
    updatedCvValues.put(testCol, testVal2);
    ODKDatabaseImplUtils.get().insertCheckpointRowWithId(db, tableId, orderedColumns,
        updatedCvValues, rowId);

    // Select everything out of the table
    sel = "SELECT * FROM " + tableId;
    selArgs = new String[0];
    cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);

    assertEquals(cursor.getCount(), 2);

    // Select everything out of the table
    sel = "SELECT * FROM " + tableId + " WHERE " + testCol + " = ?";
    selArgs = new String[1];
    selArgs[0] =  "" + testVal2;
    cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);

    String val2 = null;
    String saveptType = null;
    while (cursor.moveToNext()) {
      // Get the actual value
      int ind = cursor.getColumnIndex(testCol);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_STRING);
      val2 = cursor.getString(ind);

      // Get the savepoint_type
      ind = cursor.getColumnIndex(DataTableColumns.SAVEPOINT_TYPE);
      assertTrue(cursor.isNull(ind));

      // Get the conflict_type and make sure that it is null
      ind = cursor.getColumnIndex(DataTableColumns.CONFLICT_TYPE);
      assertTrue(cursor.isNull(ind));
    }

    assertEquals(val2, testVal2);

    // Also make sure that the savepoint_type
    // is empty
    assertNotSame(saveptType, SavepointTypeManipulator.incomplete());
    assertNotSame(saveptType, SavepointTypeManipulator.complete());

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test inserting a checkpoint row in the database
   */
  public void testInsertCheckpointRowIntoExistingDBTableWithIdWhenRowDoesNotExist_ExpectPass() {
    String tableId = testTable;
    String testCol = "testColumn";
    String testColType = ElementDataType.string.name();
    String testVal = "test";
    String rowId = ODKDataUtils.genUUID();
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[]"));
    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
            .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);

    ContentValues cvValues = new ContentValues();
    cvValues.put(testCol, testVal);
    ODKDatabaseImplUtils.get().insertCheckpointRowWithId(db, tableId, orderedColumns, cvValues,
        rowId);

    // Select everything out of the table
    String sel = "SELECT * FROM " + tableId + " WHERE " + testCol + " = ?";
    String[] selArgs = { "" + testVal };
    Cursor cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);

    String val = null;
    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(testCol);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_STRING);
      val = cursor.getString(ind);

      ind = cursor.getColumnIndex(DataTableColumns.SAVEPOINT_TYPE);
      assertTrue(cursor.isNull(ind));

      // Get the conflict_type and make sure that it is null
      ind = cursor.getColumnIndex(DataTableColumns.CONFLICT_TYPE);
      assertTrue(cursor.isNull(ind));
    }

    assertEquals(val, testVal);

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test inserting a checkpoint row in the database
   */
  public void testInsertCheckpointRowIntoExistingDBTableWithIdWhenRowIdNotProvided_ExpectPass() {
    String tableId = testTable;
    String testCol = "testColumn";
    String testColType = ElementDataType.string.name();
    String testVal = "test";
    String rowId = null;
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[]"));
    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
            .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);

    ContentValues cvValues = new ContentValues();
    cvValues.put(testCol, testVal);
    ODKDatabaseImplUtils.get().insertCheckpointRowWithId(db, tableId, orderedColumns, cvValues,
        rowId);

    // Select everything out of the table
    String sel = "SELECT * FROM " + tableId + " WHERE " + testCol + " = ?";
    String[] selArgs = { "" + testVal };
    Cursor cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);

    String val = null;
    String saveptType = null;
    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(testCol);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_STRING);
      val = cursor.getString(ind);

      ind = cursor.getColumnIndex(DataTableColumns.SAVEPOINT_TIMESTAMP);
      type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_STRING);
      saveptType = cursor.getString(ind);

      // Get the conflict_type and make sure that it is null
      ind = cursor.getColumnIndex(DataTableColumns.CONFLICT_TYPE);
      assertTrue(cursor.isNull(ind));
    }

    assertEquals(val, testVal);

    // Also make sure that the savepoint_type
    // is empty
    assertNotSame(saveptType, SavepointTypeManipulator.incomplete());
    assertNotSame(saveptType, SavepointTypeManipulator.complete());


    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test inserting a checkpoint row in the database
   */
  public void testInsertCheckpointRowIntoExistingDBTableWithIdWithRowConflictType_ExpectFail() {
    String tableId = testTable;
    String testCol = "testColumn";
    String testColType = ElementDataType.string.name();
    String testVal = "test";
    String rowId = ODKDataUtils.genUUID();
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[]"));
    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
            .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);

    ContentValues cvValues = new ContentValues();
    cvValues.put(testCol, testVal);
    cvValues.putNull(DataTableColumns.CONFLICT_TYPE);

    boolean thrown = true;
    try {
      ODKDatabaseImplUtils.get().insertCheckpointRowWithId(db, tableId, orderedColumns, cvValues,
          rowId);
    } catch (Exception e) {
      thrown = true;
      e.printStackTrace();
    }

    assertTrue(thrown);

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test inserting a checkpoint row in the database
   */
  public void testInsertCheckpointRowIntoExistingDBTableWithIdWithRowSavepointType_ExpectFail() {
    String tableId = testTable;
    String testCol = "testColumn";
    String testColType = ElementDataType.string.name();
    String testVal = "test";
    String rowId = ODKDataUtils.genUUID();
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[]"));
    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
            .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);

    ContentValues cvValues = new ContentValues();
    cvValues.put(testCol, testVal);
    cvValues.putNull(DataTableColumns.SAVEPOINT_TYPE);

    boolean thrown = true;
    try {
      ODKDatabaseImplUtils.get().insertCheckpointRowWithId(db, tableId, orderedColumns, cvValues,
          rowId);
    } catch (Exception e) {
      thrown = true;
      e.printStackTrace();
    }

    assertTrue(thrown);

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test inserting a checkpoint row in the database
   */
  public void testInsertCheckpointRowIntoExistingDBTableWithIdWithRowSavepointTimestamp_ExpectFail() {
    String tableId = testTable;
    String testCol = "testColumn";
    String testColType = ElementDataType.string.name();
    String testVal = "test";
    String rowId = ODKDataUtils.genUUID();
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[]"));
    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
            .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);

    ContentValues cvValues = new ContentValues();
    cvValues.put(testCol, testVal);
    cvValues.putNull(DataTableColumns.SAVEPOINT_TIMESTAMP);

    boolean thrown = true;
    try {
      ODKDatabaseImplUtils.get().insertCheckpointRowWithId(db, tableId, orderedColumns, cvValues,
          rowId);
    } catch (Exception e) {
      thrown = true;
      e.printStackTrace();
    }

    assertTrue(thrown);

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test inserting a checkpoint row in the database
   */
  public void testInsertCheckpointRowIntoExistingDBTableWithIdAndNoData_ExpectFail() {
    String tableId = testTable;
    String testCol = "testColumn";
    String testColType = ElementDataType.string.name();
    String testVal = "test";
    String rowId = ODKDataUtils.genUUID();
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[]"));
    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
            .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);

    ContentValues cvValues = new ContentValues();

    boolean thrown = true;
    try {
      ODKDatabaseImplUtils.get().insertCheckpointRowWithId(db, tableId, orderedColumns, cvValues,
          rowId);
    } catch (Exception e) {
      thrown = true;
      e.printStackTrace();
    }

    assertTrue(thrown);

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test saving a checkpoint row in the database as complete
   */
  public void testSaveAsCompleteMostRecentCheckpointDataInDBTableWithId_ExpectPass() {
    String tableId = testTable;
    String testCol = "testColumn";
    String testColType = ElementDataType.string.name();
    String testVal = "test";
    String testVal2 = "test2";
    String rowId = ODKDataUtils.genUUID();
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[]"));
    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
            .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);


    ContentValues cvValues = new ContentValues();
    cvValues.put(testCol, testVal);
    ODKDatabaseImplUtils.get().insertRowWithId(db, tableId, orderedColumns, cvValues, rowId);

    // Select everything out of the table
    String sel = "SELECT * FROM " + tableId + " WHERE " + testCol + " = ?";
    String[] selArgs = { "" + testVal };
    Cursor cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);

    String val = null;
    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(testCol);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_STRING);
      val = cursor.getString(ind);
    }

    assertEquals(val, testVal);

    ContentValues updatedCvValues = new ContentValues();
    updatedCvValues.put(testCol, testVal2);
    ODKDatabaseImplUtils.get().insertCheckpointRowWithId(db, tableId, orderedColumns,
        updatedCvValues, rowId);

    // Select everything out of the table
    sel = "SELECT * FROM " + tableId;
    selArgs = new String[0];
    cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);

    assertEquals(cursor.getCount(), 2);

    // Select everything out of the table
    sel = "SELECT * FROM " + tableId + " WHERE " + testCol + " = ?";
    selArgs = new String[1];
    selArgs[0] =  "" + testVal2;
    cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);

    String val2 = null;
    String saveptType = null;
    while (cursor.moveToNext()) {
      // Get the actual value
      int ind = cursor.getColumnIndex(testCol);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_STRING);
      val2 = cursor.getString(ind);

      // Get the savepoint_timestamp
      ind = cursor.getColumnIndex(DataTableColumns.SAVEPOINT_TIMESTAMP);
      type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_STRING);
      saveptType = cursor.getString(ind);

      // Get the conflict_type and make sure that it is null
      ind = cursor.getColumnIndex(DataTableColumns.CONFLICT_TYPE);
      assertTrue(cursor.isNull(ind));
    }

    assertEquals(val2, testVal2);

    // Also make sure that the savepoint_type
    // is empty
    assertNotSame(saveptType, SavepointTypeManipulator.incomplete());
    assertNotSame(saveptType, SavepointTypeManipulator.complete());

    ODKDatabaseImplUtils.get().saveAsCompleteMostRecentCheckpointRowWithId(db, tableId, rowId);
    // Select everything out of the table
    sel = "SELECT * FROM " + tableId;
    selArgs = new String[0];
    cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);

    assertEquals(cursor.getCount(), 1);

    val2 = null;
    saveptType = null;
    while (cursor.moveToNext()) {
      // Get the actual value
      int ind = cursor.getColumnIndex(testCol);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_STRING);
      val2 = cursor.getString(ind);

      // Get the savepoint_timestamp
      ind = cursor.getColumnIndex(DataTableColumns.SAVEPOINT_TYPE);
      type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_STRING);
      saveptType = cursor.getString(ind);

      // Get the conflict_type and make sure that it is null
      ind = cursor.getColumnIndex(DataTableColumns.CONFLICT_TYPE);
      assertTrue(cursor.isNull(ind));
    }

    assertEquals(val2, testVal2);
    assertEquals(saveptType, SavepointTypeManipulator.complete());

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test saving a checkpoint row in the database as incomplete
   */
  public void testSaveAsIncompleteMostRecentCheckpointDataInDBTableWithId_ExpectPass() {
    String tableId = testTable;
    String testCol = "testColumn";
    String testColType = ElementDataType.string.name();
    String testVal = "test";
    String testVal2 = "test2";
    String rowId = ODKDataUtils.genUUID();
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[]"));
    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
            .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);


    ContentValues cvValues = new ContentValues();
    cvValues.put(testCol, testVal);
    ODKDatabaseImplUtils.get().insertRowWithId(db, tableId, orderedColumns, cvValues, rowId);

    // Select everything out of the table
    String sel = "SELECT * FROM " + tableId + " WHERE " + testCol + " = ?";
    String[] selArgs = { "" + testVal };
    Cursor cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);

    String val = null;
    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(testCol);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_STRING);
      val = cursor.getString(ind);
    }

    assertEquals(val, testVal);

    ContentValues updatedCvValues = new ContentValues();
    updatedCvValues.put(testCol, testVal2);
    ODKDatabaseImplUtils.get().insertCheckpointRowWithId(db, tableId, orderedColumns,
        updatedCvValues, rowId);

    // Select everything out of the table
    sel = "SELECT * FROM " + tableId;
    selArgs = new String[0];
    cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);

    assertEquals(cursor.getCount(), 2);

    // Select everything out of the table
    sel = "SELECT * FROM " + tableId + " WHERE " + testCol + " = ?";
    selArgs = new String[1];
    selArgs[0] =  "" + testVal2;
    cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);

    String val2 = null;
    String saveptType = null;
    while (cursor.moveToNext()) {
      // Get the actual value
      int ind = cursor.getColumnIndex(testCol);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_STRING);
      val2 = cursor.getString(ind);

      // Get the savepoint_timestamp
      ind = cursor.getColumnIndex(DataTableColumns.SAVEPOINT_TIMESTAMP);
      type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_STRING);
      saveptType = cursor.getString(ind);

      // Get the conflict_type and make sure that it is null
      ind = cursor.getColumnIndex(DataTableColumns.CONFLICT_TYPE);
      assertTrue(cursor.isNull(ind));
    }

    assertEquals(val2, testVal2);

    // Also make sure that the savepoint_type
    // is empty
    assertNotSame(saveptType, SavepointTypeManipulator.incomplete());
    assertNotSame(saveptType, SavepointTypeManipulator.complete());

    ODKDatabaseImplUtils.get().saveAsIncompleteMostRecentCheckpointRowWithId(db, tableId, rowId);

    // Select everything out of the table
    sel = "SELECT * FROM " + tableId;
    selArgs = new String[0];
    cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);

    assertEquals(cursor.getCount(), 1);

    val2 = null;
    saveptType = null;
    while (cursor.moveToNext()) {
      // Get the actual value
      int ind = cursor.getColumnIndex(testCol);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_STRING);
      val2 = cursor.getString(ind);

      // Get the savepoint_timestamp
      ind = cursor.getColumnIndex(DataTableColumns.SAVEPOINT_TYPE);
      type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_STRING);
      saveptType = cursor.getString(ind);

      // Get the conflict_type and make sure that it is null
      ind = cursor.getColumnIndex(DataTableColumns.CONFLICT_TYPE);
      assertTrue(cursor.isNull(ind));
    }

    assertEquals(val2, testVal2);
    assertEquals(saveptType, SavepointTypeManipulator.incomplete());

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test inserting a checkpoint row in the database
   */
  public void testDeleteLastCheckpointRowWithId_ExpectPass() {
    String tableId = testTable;
    String testCol = "testColumn";
    String testColType = ElementDataType.string.name();
    String testVal = "test";
    String rowId = ODKDataUtils.genUUID();
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[]"));
    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
            .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);

    ContentValues cvValues = new ContentValues();
    cvValues.put(testCol, testVal);
    ODKDatabaseImplUtils.get().insertCheckpointRowWithId(db, tableId, orderedColumns, cvValues,
        rowId);

    // Select everything out of the table
    String sel = "SELECT * FROM " + tableId + " WHERE " + testCol + " = ?";
    String[] selArgs = { "" + testVal };
    Cursor cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);

    String val = null;
    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(testCol);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_STRING);
      val = cursor.getString(ind);

      ind = cursor.getColumnIndex(DataTableColumns.SAVEPOINT_TYPE);
      assertTrue(cursor.isNull(ind));

      // Get the conflict_type and make sure that it is null
      ind = cursor.getColumnIndex(DataTableColumns.CONFLICT_TYPE);
      assertTrue(cursor.isNull(ind));
    }

    assertEquals(val, testVal);

    ODKDatabaseImplUtils.get().deleteLastCheckpointRowWithId(db, tableId, rowId);
    // Select everything out of the table
    sel = "SELECT * FROM " + tableId;
    selArgs = new String[0];
    cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);

    assertEquals(cursor.getCount(), 0);

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test change data rows to new row state
   */
  public void testChangeDataRowsToNewRowState_ExpectPass() {
    // Test this after restructuring of the Sync code
  }

  // Added for the test below
  // Not sure if this needs to be public or not?
  private int countUpToLastNonNullElement(String[] row) {
    for (int i = row.length - 1; i >= 0; --i) {
      if (row[i] != null) {
        return (i + 1);
      }
    }
    return 0;
  }

  private void columnPropertiesHelper(String tableId, List<Column> columns,
      List<KeyValueStoreEntry> kvsEntries) {
    // reading data
    File file = null;
    FileInputStream in = null;
    InputStreamReader input = null;
    RFC4180CsvReader cr = null;
    try {
      file = new File(ODKFileUtils.getTableDefinitionCsvFile(getAppName(), tableId));
      in = new FileInputStream(file);
      input = new InputStreamReader(in, CharEncoding.UTF_8);
      cr = new RFC4180CsvReader(input);

      String[] row;

      // Read ColumnDefinitions
      // get the column headers
      String[] colHeaders = cr.readNext();
      int colHeadersLength = countUpToLastNonNullElement(colHeaders);
      // get the first row
      row = cr.readNext();
      while (row != null && countUpToLastNonNullElement(row) != 0) {

        String elementKeyStr = null;
        String elementNameStr = null;
        String elementTypeStr = null;
        String listChildElementKeysStr = null;
        int rowLength = countUpToLastNonNullElement(row);
        for (int i = 0; i < rowLength; ++i) {
          if (i >= colHeadersLength) {
            throw new IllegalStateException("data beyond header row of ColumnDefinitions table");
          }
          if (ColumnDefinitionsColumns.ELEMENT_KEY.equals(colHeaders[i])) {
            elementKeyStr = row[i];
          }
          if (ColumnDefinitionsColumns.ELEMENT_NAME.equals(colHeaders[i])) {
            elementNameStr = row[i];
          }
          if (ColumnDefinitionsColumns.ELEMENT_TYPE.equals(colHeaders[i])) {
            elementTypeStr = row[i];
          }
          if (ColumnDefinitionsColumns.LIST_CHILD_ELEMENT_KEYS.equals(colHeaders[i])) {
            listChildElementKeysStr = row[i];
          }
        }

        if (elementKeyStr == null || elementTypeStr == null) {
          throw new IllegalStateException("ElementKey and ElementType must be specified");
        }

        columns.add(new Column(elementKeyStr, elementNameStr, elementTypeStr,
            listChildElementKeysStr));

        // get next row or blank to end...
        row = cr.readNext();
      }

      cr.close();
      try {
        input.close();
      } catch (IOException e) {
      }
      try {
        in.close();
      } catch (IOException e) {
      }

      file = new File(ODKFileUtils.getTablePropertiesCsvFile(getAppName(), tableId));
      in = new FileInputStream(file);
      input = new InputStreamReader(in, CharEncoding.UTF_8);
      cr = new RFC4180CsvReader(input);
      // Read KeyValueStore
      // read the column headers
      String[] kvsHeaders = cr.readNext();
      // read the first row
      row = cr.readNext();
      while (row != null && countUpToLastNonNullElement(row) != 0) {
        String partition = null;
        String aspect = null;
        String key = null;
        String type = null;
        String value = null;
        int rowLength = countUpToLastNonNullElement(row);
        for (int i = 0; i < rowLength; ++i) {
          if (KeyValueStoreColumns.PARTITION.equals(kvsHeaders[i])) {
            partition = row[i];
          }
          if (KeyValueStoreColumns.ASPECT.equals(kvsHeaders[i])) {
            aspect = row[i];
          }
          if (KeyValueStoreColumns.KEY.equals(kvsHeaders[i])) {
            key = row[i];
          }
          if (KeyValueStoreColumns.VALUE_TYPE.equals(kvsHeaders[i])) {
            type = row[i];
          }
          if (KeyValueStoreColumns.VALUE.equals(kvsHeaders[i])) {
            value = row[i];
          }
        }
        KeyValueStoreEntry kvsEntry = KeyValueStoreUtils.buildEntry(tableId, partition, aspect, key,
            ElementDataType.valueOf(type), value);
        kvsEntries.add(kvsEntry);
        // get next row or blank to end...
        row = cr.readNext();
      }
      cr.close();
      try {
        input.close();
      } catch (IOException e) {
      }
      try {
        in.close();
      } catch (IOException e) {
      }

    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
        if (input != null) {
          input.close();
        }
      } catch (IOException e) {
      }
    }

    // Go through the KVS list and replace all the choiceList entries with their choiceListId
    for ( KeyValueStoreEntry entry : kvsEntries ) {
      if ( entry.partition.equals(KeyValueStoreConstants.PARTITION_COLUMN) &&
          entry.key.equals(KeyValueStoreConstants.COLUMN_DISPLAY_CHOICES_LIST) ) {
        // stored type is a string -- the choiceListId
        entry.type = ElementDataType.string.name();
        if ((entry.value != null) && (entry.value.trim().length() != 0)) {
          String choiceListId = ODKDatabaseImplUtils.get().setChoiceList(db, getAppName(), entry
              .value);
          entry.value = choiceListId;
        } else {
          entry.value = null;
        }
      }
    }

  }
  /*
   * Test create or open table with columns and properties
   */
  public void testCreateOrOpenDBTableWithColumnsAndProperties_ExpectPass() {
    String tableId = testTable;
    String testCol = "testColumn";
    String testColType = ElementDataType.string.name();
    String testVal = "test";
    String rowId = ODKDataUtils.genUUID();
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[]"));

    List<KeyValueStoreEntry> kvsEntries = new ArrayList<KeyValueStoreEntry>();
    KeyValueStoreEntry kvsEntry = KeyValueStoreUtils.buildEntry(tableId, KeyValueStoreConstants.PARTITION_TABLE,
        KeyValueStoreConstants.ASPECT_DEFAULT, KeyValueStoreConstants.COLUMN_DISPLAY_NAME,
        ElementDataType.valueOf(ElementDataType.object.name()), tableId);
    kvsEntries.add(kvsEntry);

    try {
      ODKDatabaseImplUtils.get()
          .createOrOpenDBTableWithColumnsAndProperties(db, getAppName(), tableId, columns,
              kvsEntries, true);
    } catch (Exception e){
      e.printStackTrace();
    }

    // Ensure that at least one of the expected properties is in the KVS table
    String sel = "SELECT * FROM " + DatabaseConstants.KEY_VALUE_STORE_ACTIVE_TABLE_NAME +
        " WHERE " + KeyValueStoreColumns.PARTITION + " = ? AND " + KeyValueStoreColumns.KEY +
        " = ? AND " + KeyValueStoreColumns.VALUE + " = ?";
    String[] selArgs = { KeyValueStoreConstants.PARTITION_TABLE,
        KeyValueStoreConstants.COLUMN_DISPLAY_NAME, tableId };
    Cursor cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);
    assertEquals(cursor.getCount(), 1);

    // Now delete the metadata
    ODKDatabaseImplUtils.get().deleteDBTableMetadata(db, tableId, null, null, null);

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);

  }

  /*
   * Test delete checkpoint rows with id
   */
  public void testDeleteCheckpointRowsWithValidId_ExpectPass() {
    String tableId = testTable;
    String testCol = "testColumn";
    String testColType = ElementDataType.string.name();
    String testVal = "test";
    String rowId = ODKDataUtils.genUUID();
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[]"));
    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
        .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);

    ContentValues cvValues = new ContentValues();
    cvValues.put(testCol, testVal);
    ODKDatabaseImplUtils.get().insertCheckpointRowWithId(db, tableId, orderedColumns, cvValues,
        rowId);

    // Select everything out of the table
    String sel = "SELECT * FROM " + tableId + " WHERE " + testCol + " = ?";
    String[] selArgs = { "" + testVal };
    Cursor cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);

    String val = null;
    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(testCol);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_STRING);
      val = cursor.getString(ind);

      ind = cursor.getColumnIndex(DataTableColumns.SAVEPOINT_TYPE);
      assertTrue(cursor.isNull(ind));

      // Get the conflict_type and make sure that it is null
      ind = cursor.getColumnIndex(DataTableColumns.CONFLICT_TYPE);
      assertTrue(cursor.isNull(ind));
    }

    assertEquals(val, testVal);

    ODKDatabaseImplUtils.get().deleteAllCheckpointRowsWithId(db, getAppName(), tableId, rowId);
    // Select everything out of the table
    sel = "SELECT * FROM " + tableId;
    selArgs = new String[0];
    cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);

    assertEquals(cursor.getCount(), 0);

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
 * Test delete checkpoint rows with id
 */
  public void testDeleteCheckpointRowsWithInvalidId_ExpectFail() {
    String tableId = testTable;
    String testCol = "testColumn";
    String testColType = ElementDataType.string.name();
    String testVal = "test";
    String rowId = ODKDataUtils.genUUID();
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[]"));
    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
        .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);

    ContentValues cvValues = new ContentValues();
    cvValues.put(testCol, testVal);
    ODKDatabaseImplUtils.get().insertCheckpointRowWithId(db, tableId, orderedColumns, cvValues,
        rowId);

    // Select everything out of the table
    String sel = "SELECT * FROM " + tableId + " WHERE " + testCol + " = ?";
    String[] selArgs = { "" + testVal };
    Cursor cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);

    String val = null;
    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(testCol);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_STRING);
      val = cursor.getString(ind);

      ind = cursor.getColumnIndex(DataTableColumns.SAVEPOINT_TYPE);
      assertTrue(cursor.isNull(ind));

      // Get the conflict_type and make sure that it is null
      ind = cursor.getColumnIndex(DataTableColumns.CONFLICT_TYPE);
      assertTrue(cursor.isNull(ind));
    }

    assertEquals(val, testVal);

    String invalidRowId = ODKDataUtils.genUUID();
    boolean thrown = true;
    try {
      ODKDatabaseImplUtils.get().deleteAllCheckpointRowsWithId(db, getAppName(), tableId,
          invalidRowId);
    } catch (Exception e) {
      thrown = true;
      e.printStackTrace();
    }

    assertTrue(thrown);

    // Select everything out of the table
    sel = "SELECT * FROM " + tableId;
    selArgs = new String[0];
    cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);

    assertEquals(cursor.getCount(), 1);

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test delete db table metadata - delete all metadata
   */
  public void testDeleteDBTableMetadata_ExpectPass() {
    String tableId = testTable;
    String testCol = "testColumn";
    String testColType = ElementDataType.string.name();
    String partition = KeyValueStoreConstants.PARTITION_TABLE;
    String aspect = KeyValueStoreConstants.ASPECT_DEFAULT;
    String key = KeyValueStoreConstants.COLUMN_DISPLAY_NAME;
    String type = ElementDataType.object.name();
    String kvsValue = tableId;
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[]"));

    List<KeyValueStoreEntry> kvsEntries = new ArrayList<KeyValueStoreEntry>();
    KeyValueStoreEntry kvsEntry = KeyValueStoreUtils.buildEntry(tableId, partition,
        aspect, key, ElementDataType.valueOf(type), kvsValue);
    kvsEntries.add(kvsEntry);

    try {
      ODKDatabaseImplUtils.get()
          .createOrOpenDBTableWithColumnsAndProperties(db, getAppName(), tableId, columns,
              kvsEntries, true);
    } catch (Exception e){
      e.printStackTrace();
    }

    // Ensure that the expected properties is in the KVS table
    String sel = "SELECT * FROM " + DatabaseConstants.KEY_VALUE_STORE_ACTIVE_TABLE_NAME +
        " WHERE " + KeyValueStoreColumns.PARTITION + " = ? AND " + KeyValueStoreColumns.KEY +
        " = ? AND " + KeyValueStoreColumns.VALUE + " = ?";
    String[] selArgs = { partition, key, kvsValue };
    Cursor cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);
    assertEquals(cursor.getCount(), 1);

    // Now make sure that the returned value is equal to the original value
    ArrayList<KeyValueStoreEntry> retKVSEntries =  ODKDatabaseImplUtils.get().getDBTableMetadata(db, tableId,
        partition, aspect, key);

    assertEquals(retKVSEntries, kvsEntries);

    // Now delete the metadata
    ODKDatabaseImplUtils.get().deleteDBTableMetadata(db, tableId, partition, aspect, key);

    // Ensure that the expected properties is in the KVS table
    cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);
    assertEquals(cursor.getCount(), 0);

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test delete server conflict row with id
   * Place a row in conflict and then delete it
   */
  public void testDeleteServerConflictRowWithIdAndLocDelOldVals_ExpectPass() {
    String tableId = testTable;
    String testCol = "testColumn";
    String testColType = ElementDataType.integer.name();
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[]"));
    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
        .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);
    int testVal = 5;

    // local record that is synced and pending deletion...

    ContentValues cvValues = new ContentValues();
    String rowId = ODKDataUtils.genUUID();
    cvValues.put(testCol, testVal);
    cvValues.put(DataTableColumns.ROW_ETAG, ODKDataUtils.genUUID());
    cvValues.put(DataTableColumns.SYNC_STATE, SyncState.deleted.name());
    ODKDatabaseImplUtils.get().insertRowWithId(db, tableId, orderedColumns, cvValues, rowId);

    // Select everything out of the table
    String sel = "SELECT * FROM " + tableId + " WHERE " + DataTableColumns.ID + " = ? ORDER BY "
        + DataTableColumns.CONFLICT_TYPE + " ASC";
    String[] selArgs = { rowId };
    Cursor cursor = null;

    int val = 0;
    try {
      cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);
      assertEquals(cursor.getCount(), 1);

      while (cursor.moveToNext()) {
        int ind = cursor.getColumnIndex(testCol);
        int type = cursor.getType(ind);
        assertEquals(type, Cursor.FIELD_TYPE_INTEGER);
        val = cursor.getInt(ind);
      }
    } finally {
      cursor.close();
    }

    assertEquals(val, testVal);

    // server has a change...
    ContentValues updates = new ContentValues();
    updates.put(DataTableColumns.CONFLICT_TYPE, ConflictType.SERVER_UPDATED_UPDATED_VALUES);
    updates.put(DataTableColumns.SYNC_STATE, SyncState.in_conflict.name());
    updates.put(DataTableColumns.ROW_ETAG, ODKDataUtils.genUUID());
    updates.put(testCol, testVal + 6);

    // Place row in conflict
    int conflictType = ConflictType.LOCAL_DELETED_OLD_VALUES;
    ODKDatabaseImplUtils.get().placeRowIntoServerConflictWithId(db, tableId, orderedColumns,
          updates, rowId, conflictType);

    // Run the query again and make sure that the place row in conflict worked as expected
    UserTable table = ODKDatabaseImplUtils.get().rawSqlQuery(db, getAppName(), tableId,
        orderedColumns, DataTableColumns.ID + "=?", new String[] { rowId }, null, null,
            DataTableColumns.CONFLICT_TYPE, "ASC" );

    assertEquals(table.getNumberOfRows(), 2);

    Row first = table.getRowAtIndex(0);
    Row second = table.getRowAtIndex(1);

    String v;
    int conflictTypeVal;

    v = first.getRawDataOrMetadataByElementKey(DataTableColumns.CONFLICT_TYPE);
    assertNotNull(v);
    conflictTypeVal = Integer.valueOf(v);
    assertEquals(conflictType, conflictTypeVal);

    v = second.getRawDataOrMetadataByElementKey(DataTableColumns.CONFLICT_TYPE);
    assertNotNull(v);
    conflictTypeVal = Integer.valueOf(v);
    assertEquals(ConflictType.SERVER_UPDATED_UPDATED_VALUES, conflictTypeVal);

    // Now delete the row
    ODKDatabaseImplUtils.get().resolveServerConflictWithDeleteRowWithId(db, getAppName(), tableId,
        rowId);

    // Run the query yet again to make sure that things worked as expected
    table = ODKDatabaseImplUtils.get().rawSqlQuery(db, getAppName(), tableId,
        orderedColumns, DataTableColumns.ID + "=?", new String[] { rowId }, null, null,
        DataTableColumns.CONFLICT_TYPE, "ASC" );

    assertEquals(table.getNumberOfRows(), 0);

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test delete server conflict row with id
   * Place a row in conflict and then delete it
   */
//  public void testDeleteServerConflictRowWithIdAndLocUpdUpdVals_ExpectPass() {
//    String tableId = testTable;
//    String testCol = "testColumn";
//    String testColType = ElementDataType.integer.name();
//    List<Column> columns = new ArrayList<Column>();
//    columns.add(new Column(testCol, testCol, testColType, "[]"));
//    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
//        .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);
//    int testVal = 5;
//
//  ContentValues cvValues = new ContentValues();
//  String rowId = ODKDataUtils.genUUID();
//  cvValues.put(testCol, testVal);
//  ODKDatabaseImplUtils.get().insertDataIntoExistingDBTableWithId(db, tableId, orderedColumns,
//      cvValues, rowId);
//
//  // Select everything out of the table
//  String sel = "SELECT * FROM " + tableId + " WHERE " + testCol + " = ?";
//  String[] selArgs = { "" + testVal };
//  Cursor cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);
//
//  int val = 0;
//  while (cursor.moveToNext()) {
//    int ind = cursor.getColumnIndex(testCol);
//    int type = cursor.getType(ind);
//    assertEquals(type, Cursor.FIELD_TYPE_INTEGER);
//    val = cursor.getInt(ind);
//  }
//
//  assertEquals(val, testVal);
//
//  // Place row in conflict
//  int conflictType = ConflictType.LOCAL_UPDATED_UPDATED_VALUES;
//  ODKDatabaseImplUtils.get().placeRowIntoServerConflictWithId(db, tableId, orderedColumns,
//      cvValues, rowId, conflictType);
//  //ODKDatabaseImplUtils.get().placeRowIntoConflict(db, tableId, rowId, conflictType);
//
//  // Run the query again and make sure that the place row in conflict worked as expected
//  cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);
//  assertEquals(cursor.getCount(), 1);
//
//  int conflictTypeVal = -1;
//  while (cursor.moveToNext()) {
//    int ind = cursor.getColumnIndex(DataTableColumns.CONFLICT_TYPE);
//    int type = cursor.getType(ind);
//    assertEquals(type, Cursor.FIELD_TYPE_INTEGER);
//    conflictTypeVal = cursor.getInt(ind);
//  }
//
//  assertEquals(conflictType, conflictTypeVal);
//
//  // Now delete the row
//  ODKDatabaseImplUtils.get().deleteServerConflictRowWithId(db, tableId, rowId);
//
//  // Run the query yet again to make sure that things worked as expected
//  cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);
//  assertEquals(cursor.getCount(), 1);
//
//  // Drop the table now that the test is done
//  ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
//}

  /*
   * Test delete server conflict row with id
   * Place a row in conflict and then delete it
   */
//  public void testDeleteServerConflictRowWithIdAndSrvDelOldValues_ExpectPass() {
//    String tableId = testTable;
//    String testCol = "testColumn";
//    String testColType = ElementDataType.integer.name();
//    List<Column> columns = new ArrayList<Column>();
//    columns.add(new Column(testCol, testCol, testColType, "[]"));
//    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
//        .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);
//    int testVal = 5;
//
//    ContentValues cvValues = new ContentValues();
//    String rowId = ODKDataUtils.genUUID();
//    cvValues.put(testCol, testVal);
//    ODKDatabaseImplUtils.get().insertDataIntoExistingDBTableWithId(db, tableId, orderedColumns,
//        cvValues, rowId);
//
//    // Select everything out of the table
//    String sel = "SELECT * FROM " + tableId + " WHERE " + testCol + " = ?";
//    String[] selArgs = { "" + testVal };
//    Cursor cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);
//
//    int val = 0;
//    while (cursor.moveToNext()) {
//      int ind = cursor.getColumnIndex(testCol);
//      int type = cursor.getType(ind);
//      assertEquals(type, Cursor.FIELD_TYPE_INTEGER);
//      val = cursor.getInt(ind);
//    }
//
//    assertEquals(val, testVal);
//
//    // Place row in conflict
//    int conflictType = ConflictType.SERVER_DELETED_OLD_VALUES;
//    ODKDatabaseImplUtils.get().placeRowIntoConflict(db, tableId, rowId, conflictType);
//
//    // Run the query again and make sure that the place row in conflict worked as expected
//    cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);
//    assertEquals(cursor.getCount(), 1);
//
//    int conflictTypeVal = -1;
//    while (cursor.moveToNext()) {
//      int ind = cursor.getColumnIndex(DataTableColumns.CONFLICT_TYPE);
//      int type = cursor.getType(ind);
//      assertEquals(type, Cursor.FIELD_TYPE_INTEGER);
//      conflictTypeVal = cursor.getInt(ind);
//    }
//
//    assertEquals(conflictType, conflictTypeVal);
//
//    // Now delete the row
//    ODKDatabaseImplUtils.get().deleteServerConflictRowWithId(db, tableId, rowId);
//
//    // Run the query yet again to make sure that things worked as expected
//    cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);
//    assertEquals(cursor.getCount(), 0);
//
//    // Drop the table now that the test is done
//    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
//  }

  /*
   * Test delete server conflict row with id
   * Place a row in conflict and then delete it
   */
//  public void testDeleteServerConflictRowWithIdAndSrvUpdUpdVals_ExpectPass() {
//    String tableId = testTable;
//    String testCol = "testColumn";
//    String testColType = ElementDataType.integer.name();
//    List<Column> columns = new ArrayList<Column>();
//    columns.add(new Column(testCol, testCol, testColType, "[]"));
//    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
//        .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);
//    int testVal = 5;
//
//    ContentValues cvValues = new ContentValues();
//    String rowId = ODKDataUtils.genUUID();
//    cvValues.put(testCol, testVal);
//    ODKDatabaseImplUtils.get().insertDataIntoExistingDBTableWithId(db, tableId, orderedColumns,
//        cvValues, rowId);
//
//    // Select everything out of the table
//    String sel = "SELECT * FROM " + tableId + " WHERE " + testCol + " = ?";
//    String[] selArgs = { "" + testVal };
//    Cursor cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);
//
//    int val = 0;
//    while (cursor.moveToNext()) {
//      int ind = cursor.getColumnIndex(testCol);
//      int type = cursor.getType(ind);
//      assertEquals(type, Cursor.FIELD_TYPE_INTEGER);
//      val = cursor.getInt(ind);
//    }
//
//    assertEquals(val, testVal);
//
//    // Place row in conflict
//    int conflictType = ConflictType.SERVER_UPDATED_UPDATED_VALUES;
//    ODKDatabaseImplUtils.get().placeRowIntoConflict(db, tableId, rowId, conflictType);
//
//    // Run the query again and make sure that the place row in conflict worked as expected
//    cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);
//    assertEquals(cursor.getCount(), 1);
//
//    int conflictTypeVal = -1;
//    while (cursor.moveToNext()) {
//      int ind = cursor.getColumnIndex(DataTableColumns.CONFLICT_TYPE);
//      int type = cursor.getType(ind);
//      assertEquals(type, Cursor.FIELD_TYPE_INTEGER);
//      conflictTypeVal = cursor.getInt(ind);
//    }
//
//    assertEquals(conflictType, conflictTypeVal);
//
//    // Now delete the row
//    ODKDatabaseImplUtils.get().deleteServerConflictRowWithId(db, tableId, rowId);
//
//    // Run the query yet again to make sure that things worked as expected
//    cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);
//    assertEquals(cursor.getCount(), 0);
//
//    // Drop the table now that the test is done
//    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
//  }

  /*
   * Test enforce types table metadata
   */
  public void testEnforceTypesDBTableMetadata_ExpectPass() {
    String tableId = testTable;
    String testCol = "testColumn";
    String testColType = ElementDataType.string.name();
    String partition = KeyValueStoreConstants.PARTITION_TABLE;
    String aspect = KeyValueStoreConstants.ASPECT_DEFAULT;
    String key = KeyValueStoreConstants.COLUMN_DISPLAY_NAME;
    String type = ElementDataType.integer.name();
    String kvsValue = tableId;
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[]"));

    List<KeyValueStoreEntry> kvsEntries = new ArrayList<KeyValueStoreEntry>();
    KeyValueStoreEntry kvsEntry = KeyValueStoreUtils.buildEntry(tableId, partition,
        aspect, key, ElementDataType.valueOf(type), kvsValue);
    kvsEntries.add(kvsEntry);

    // createOrOpenDBTableWithColumnsAndProperties calls enforceTypesDBTableMetadata
    try {
      ODKDatabaseImplUtils.get()
          .createOrOpenDBTableWithColumnsAndProperties(db, getAppName(), tableId, columns,
              kvsEntries, true);
    } catch (Exception e){
      e.printStackTrace();
    }

    // Ensure that the expected properties is in the KVS table
    String sel = "SELECT * FROM " + DatabaseConstants.KEY_VALUE_STORE_ACTIVE_TABLE_NAME +
        " WHERE " + KeyValueStoreColumns.PARTITION + " = ? AND " + KeyValueStoreColumns.KEY +
        " = ? AND " + KeyValueStoreColumns.VALUE + " = ?";
    String[] selArgs = { partition, key, kvsValue };
    Cursor cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);
    assertEquals(cursor.getCount(), 1);

    // Now make sure that the returned value is equal to the original value
    ArrayList<KeyValueStoreEntry> retKVSEntries =  ODKDatabaseImplUtils.get().getDBTableMetadata(db, tableId,
        partition, aspect, key);

    assertEquals(retKVSEntries.size(), kvsEntries.size());

    assertNotSame(retKVSEntries.get(0).type, kvsEntries.get(0).type);

    assertEquals(retKVSEntries.get(0).type, ElementDataType.object.name());

    // Now make sure that the table has the right value for displayName
    cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);
    assertEquals(cursor.getCount(), 1);

    String val = null;
    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(KeyValueStoreColumns.VALUE_TYPE);
      int resultType = cursor.getType(ind);
      assertEquals(resultType, Cursor.FIELD_TYPE_STRING);
      val = cursor.getString(ind);
    }

    // In this case the type should have been changed to string
    assertEquals(val, ElementDataType.object.name());

    // Now delete the metadata
    ODKDatabaseImplUtils.get().deleteDBTableMetadata(db, tableId, partition, aspect, key);

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test get choice list
   */
  public void testGetChoiceList_ExpectPass() {
    ArrayList<Object> values = new ArrayList<Object>();
    Map<String,Object> myMap = new TreeMap<String,Object>();
    Map<String, Object> displayText = new TreeMap<String, Object>();
    displayText.put("text", "displayText");
    myMap.put("choice_list_name", "test_list");
    myMap.put("data_value", "test");
    myMap.put("display", displayText);
    values.add(myMap);

    String jsonChoiceList  = null;
    try {
      jsonChoiceList = ODKFileUtils.mapper.writeValueAsString(values);
    } catch (JsonProcessingException e) {
      e.printStackTrace();
    }

    // Set the choice list id
    String choiceListId = ODKDatabaseImplUtils.get().setChoiceList(db, getAppName(), jsonChoiceList);

    // Get the choice list
    String retJsonChoiceList = ODKDatabaseImplUtils.get().getChoiceList(db, getAppName(),
        choiceListId);

    assertEquals(jsonChoiceList, retJsonChoiceList);

    // Select the _choice_list_id from the _choice_lists table
    String sel = "SELECT * FROM " + DatabaseConstants.CHOICE_LIST_TABLE_NAME +
        " WHERE " + ChoiceListColumns.CHOICE_LIST_ID + " = ?";
    String[] selArgs = { "" + choiceListId };
    Cursor cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);
    assertEquals(cursor.getCount(), 1);

    String val = null;
    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(ChoiceListColumns.CHOICE_LIST_JSON);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_STRING);
      val = cursor.getString(ind);
    }

    assertEquals(val, retJsonChoiceList);
  }

  /*
   * Test get table metadata
   */
  public void testGetDBTableMetadata_ExpectPass() {
    String tableId = testTable;
    String testCol = "testColumn";
    String testColType = ElementDataType.string.name();
    String partition = KeyValueStoreConstants.PARTITION_TABLE;
    String aspect = KeyValueStoreConstants.ASPECT_DEFAULT;
    String key = KeyValueStoreConstants.COLUMN_DISPLAY_NAME;
    String type = ElementDataType.object.name();
    String kvsValue = tableId;
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[]"));

    List<KeyValueStoreEntry> kvsEntries = new ArrayList<KeyValueStoreEntry>();
    KeyValueStoreEntry kvsEntry = KeyValueStoreUtils.buildEntry(tableId, partition,
        aspect, key, ElementDataType.valueOf(type), kvsValue);
    kvsEntries.add(kvsEntry);

    try {
      ODKDatabaseImplUtils.get()
          .createOrOpenDBTableWithColumnsAndProperties(db, getAppName(), tableId, columns,
              kvsEntries, true);
    } catch (Exception e){
      e.printStackTrace();
    }

    // Ensure that the expected properties is in the KVS table
    String sel = "SELECT * FROM " + DatabaseConstants.KEY_VALUE_STORE_ACTIVE_TABLE_NAME +
        " WHERE " + KeyValueStoreColumns.PARTITION + " = ? AND " + KeyValueStoreColumns.KEY +
        " = ? AND " + KeyValueStoreColumns.VALUE + " = ?";
    String[] selArgs = { partition, key, kvsValue };
    Cursor cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);
    assertEquals(cursor.getCount(), 1);

    // Now make sure that the returned value is equal to the original value
    ArrayList<KeyValueStoreEntry> retKVSEntries =  ODKDatabaseImplUtils.get().getDBTableMetadata(db, tableId,
        partition, aspect, key);

    assertEquals(retKVSEntries.get(0), kvsEntries.get(0));

    // Now delete the metadata
    ODKDatabaseImplUtils.get().deleteDBTableMetadata(db, tableId, partition, aspect, key);

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test get table definition entry
   */
  public void testGetTableDefinitionEntry_ExpectPass() {
    String tableId = testTable;
    String testCol = "testColumn";
    String testColType = ElementDataType.integer.name();
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[]"));
    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
        .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);
    int testVal = 5;

    ContentValues cvValues = new ContentValues();
    String rowId = ODKDataUtils.genUUID();
    cvValues.put(testCol, testVal);
    ODKDatabaseImplUtils.get().insertRowWithId(db, tableId, orderedColumns, cvValues, rowId);

    // Select everything out of the table
    String sel = "SELECT * FROM " + tableId + " WHERE " + testCol + " = ?";
    String[] selArgs = { "" + testVal };
    Cursor cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);
    assertEquals(cursor.getCount(), 1);

    int val = 0;
    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(testCol);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_INTEGER);
      val = cursor.getInt(ind);
    }

    assertEquals(val, testVal);

    // Select everything out of the table
    String sel2 = "SELECT * FROM " + DatabaseConstants.TABLE_DEFS_TABLE_NAME +
        " WHERE " + TableDefinitionsColumns.TABLE_ID + " = ?";
    String [] selArgs2 = { "" + tableId };
    cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel2, selArgs2);
    assertEquals(cursor.getCount(), 1);

    String syncTimeVal = null;
    String schemaETagVal = null;
    String lastDataETagVal = null;
    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(TableDefinitionsColumns.LAST_SYNC_TIME);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_STRING);
      syncTimeVal = cursor.getString(ind);

      ind = cursor.getColumnIndex(TableDefinitionsColumns.SCHEMA_ETAG);
      assertTrue(cursor.isNull(ind));

      ind = cursor.getColumnIndex(TableDefinitionsColumns.LAST_DATA_ETAG);
      assertTrue(cursor.isNull(ind));
    }

    // Now get the table definition entry
    TableDefinitionEntry tde = ODKDatabaseImplUtils.get().getTableDefinitionEntry(db, tableId);

    // Compare the table definition entry and the raw query results
    assertEquals(syncTimeVal, tde.getLastSyncTime());
    assertEquals(schemaETagVal, tde.getSchemaETag());
    assertEquals(lastDataETagVal, tde.getLastDataETag());

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test get table health when table is healthy
   */
  public void testGetTableHealthWhenTableIsClean_ExpectPass() {
    String tableId = testTable;
    String testCol = "testColumn";
    String testColType = ElementDataType.integer.name();
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[]"));
    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
        .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);
    int testVal = 5;

    ContentValues cvValues = new ContentValues();
    String rowId = ODKDataUtils.genUUID();
    cvValues.put(testCol, testVal);
    ODKDatabaseImplUtils.get().insertRowWithId(db, tableId, orderedColumns, cvValues, rowId);

    // Select everything out of the table
    String sel = "SELECT * FROM " + tableId + " WHERE " + testCol + " = ?";
    String[] selArgs = { "" + testVal };
    Cursor cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);

    int val = 0;
    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(testCol);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_INTEGER);
      val = cursor.getInt(ind);
    }

    assertEquals(val, testVal);

    // Test that the health of the table is CLEAN
    int health = ODKDatabaseImplUtils.get().getTableHealth(db, tableId);

    assertEquals(health, ODKCursorUtils.TABLE_HEALTH_IS_CLEAN);

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
 * Test get table health when table has checkpoints
 */
  public void testGetTableHealthWhenTableHasChkpts_ExpectPass() {
    String tableId = testTable;
    String testCol = "testColumn";
    String testColType = ElementDataType.integer.name();
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[]"));
    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
        .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);
    int testVal = 5;

    ContentValues cvValues = new ContentValues();
    String rowId = ODKDataUtils.genUUID();
    cvValues.put(testCol, testVal);
    ODKDatabaseImplUtils.get().insertRowWithId(db, tableId, orderedColumns, cvValues, rowId);

    // Select everything out of the table
    String sel = "SELECT * FROM " + tableId + " WHERE " + testCol + " = ?";
    String[] selArgs = { "" + testVal };
    Cursor cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);

    int val = 0;
    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(testCol);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_INTEGER);
      val = cursor.getInt(ind);
    }

    assertEquals(val, testVal);

    int testVal2 = 200;
    ContentValues updatedCvValues = new ContentValues();
    updatedCvValues.put(testCol, testVal2);
    ODKDatabaseImplUtils.get().insertCheckpointRowWithId(db, tableId, orderedColumns,
        updatedCvValues, rowId);

    // Select everything out of the table
    sel = "SELECT * FROM " + tableId;
    selArgs = new String[0];
    cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);

    assertEquals(cursor.getCount(), 2);

    // Select everything out of the table
    sel = "SELECT * FROM " + tableId + " WHERE " + testCol + " = ?";
    selArgs = new String[1];
    selArgs[0] =  "" + testVal2;
    cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);

    int val2 = 0;
    String saveptType = null;
    while (cursor.moveToNext()) {
      // Get the actual value
      int ind = cursor.getColumnIndex(testCol);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_INTEGER);
      val2 = cursor.getInt(ind);

      // Get the savepoint_type
      ind = cursor.getColumnIndex(DataTableColumns.SAVEPOINT_TYPE);
      assertTrue(cursor.isNull(ind));

      // Get the conflict_type and make sure that it is null
      ind = cursor.getColumnIndex(DataTableColumns.CONFLICT_TYPE);
      assertTrue(cursor.isNull(ind));
    }

    assertEquals(val2, testVal2);

    // Also make sure that the savepoint_type
    // is empty
    assertNotSame(saveptType, SavepointTypeManipulator.incomplete());
    assertNotSame(saveptType, SavepointTypeManipulator.complete());

    // Test that the health of the table is CLEAN
    int health = ODKDatabaseImplUtils.get().getTableHealth(db, tableId);

    assertEquals(health, ODKCursorUtils.TABLE_HEALTH_HAS_CHECKPOINTS);

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test get table health when table is has conflicts
   */
//  public void testGetTableHealthWhenTableHasConflicts_ExpectPass() {
//    String tableId = testTable;
//    String testCol = "testColumn";
//    String testColType = ElementDataType.integer.name();
//    List<Column> columns = new ArrayList<Column>();
//    columns.add(new Column(testCol, testCol, testColType, "[]"));
//    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
//        .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);
//    int testVal = 5;
//
//    ContentValues cvValues = new ContentValues();
//    String rowId = ODKDataUtils.genUUID();
//    cvValues.put(testCol, testVal);
//    ODKDatabaseImplUtils.get().insertDataIntoExistingDBTableWithId(db, tableId, orderedColumns,
//        cvValues, rowId);
//
//    // Select everything out of the table
//    String sel = "SELECT * FROM " + tableId + " WHERE " + testCol + " = ?";
//    String[] selArgs = { "" + testVal };
//    Cursor cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);
//
//    int val = 0;
//    while (cursor.moveToNext()) {
//      int ind = cursor.getColumnIndex(testCol);
//      int type = cursor.getType(ind);
//      assertEquals(type, Cursor.FIELD_TYPE_INTEGER);
//      val = cursor.getInt(ind);
//    }
//
//    assertEquals(val, testVal);
//
//    // Place row in conflict
//    int conflictType = ConflictType.LOCAL_DELETED_OLD_VALUES;
//    ODKDatabaseImplUtils.get().placeRowIntoServerConflictWithId(db, tableId,orderedColumns,
//        cvValues, rowId, conflictType);
//    //ODKDatabaseImplUtils.get().placeRowIntoConflict(db, tableId, rowId, conflictType);
//
//    // Run the query again and make sure that the place row in conflict worked as expected
//    cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);
//
//    int conflictTypeVal = -1;
//    while (cursor.moveToNext()) {
//      int ind = cursor.getColumnIndex(DataTableColumns.CONFLICT_TYPE);
//      int type = cursor.getType(ind);
//      assertEquals(type, Cursor.FIELD_TYPE_INTEGER);
//      conflictTypeVal = cursor.getInt(ind);
//    }
//
//    assertEquals(conflictType, conflictTypeVal);
//
//
//    // Test that the health of the table is CLEAN
//    int health = ODKDatabaseImplUtils.get().getTableHealth(db, tableId);
//
//    assertEquals(health, ODKCursorUtils.TABLE_HEALTH_HAS_CONFLICTS);
//
//    // Drop the table now that the test is done
//    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
//  }

  /*
   * Test get table health when table is has checkpoints and conflicts
   */
//  public void testGetTableHealthWhenTableHasChkptsAndConflicts_ExpectPass() {
//    String tableId = testTable;
//    String testCol = "testColumn";
//    String testColType = ElementDataType.integer.name();
//    List<Column> columns = new ArrayList<Column>();
//    columns.add(new Column(testCol, testCol, testColType, "[]"));
//    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
//        .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);
//    int testVal = 5;
//
//    ContentValues cvValues = new ContentValues();
//    String rowId = ODKDataUtils.genUUID();
//    cvValues.put(testCol, testVal);
//    ODKDatabaseImplUtils.get().insertDataIntoExistingDBTableWithId(db, tableId, orderedColumns,
//        cvValues, rowId);
//
//    // Select everything out of the table
//    String sel = "SELECT * FROM " + tableId + " WHERE " + testCol + " = ?";
//    String[] selArgs = { "" + testVal };
//    Cursor cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);
//
//    int val = 0;
//    while (cursor.moveToNext()) {
//      int ind = cursor.getColumnIndex(testCol);
//      int type = cursor.getType(ind);
//      assertEquals(type, Cursor.FIELD_TYPE_INTEGER);
//      val = cursor.getInt(ind);
//    }
//
//    assertEquals(val, testVal);
//
//    int testVal2 = 200;
//    ContentValues updatedCvValues = new ContentValues();
//    updatedCvValues.put(testCol, testVal2);
//    ODKDatabaseImplUtils.get().insertCheckpointRowWithId(db, tableId, orderedColumns, updatedCvValues, rowId);
//
//    // Select everything out of the table
//    sel = "SELECT * FROM " + tableId;
//    selArgs = new String[0];
//    cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);
//
//    assertEquals(cursor.getCount(), 2);
//
//    // Select everything out of the table
//    sel = "SELECT * FROM " + tableId + " WHERE " + testCol + " = ?";
//    selArgs = new String[1];
//    selArgs[0] =  "" + testVal2;
//    cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);
//
//    int val2 = 0;
//    String saveptType = null;
//    while (cursor.moveToNext()) {
//      // Get the actual value
//      int ind = cursor.getColumnIndex(testCol);
//      int type = cursor.getType(ind);
//      assertEquals(type, Cursor.FIELD_TYPE_INTEGER);
//      val2 = cursor.getInt(ind);
//
//      // Get the savepoint_type
//      ind = cursor.getColumnIndex(DataTableColumns.SAVEPOINT_TYPE);
//      assertTrue(cursor.isNull(ind));
//
//      // Get the conflict_type and make sure that it is null
//      ind = cursor.getColumnIndex(DataTableColumns.CONFLICT_TYPE);
//      assertTrue(cursor.isNull(ind));
//    }
//
//    assertEquals(val2, testVal2);
//
//    // Also make sure that the savepoint_type
//    // is empty
//    assertNotSame(saveptType, SavepointTypeManipulator.incomplete());
//    assertNotSame(saveptType, SavepointTypeManipulator.complete());
//
//    // Place row in conflict
//    int conflictType = ConflictType.LOCAL_DELETED_OLD_VALUES;
//    ODKDatabaseImplUtils.get().placeRowIntoConflict(db, tableId, rowId, conflictType);
//
//    // Run the query again and make sure that the place row in conflict worked as expected
//    cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);
//
//    int conflictTypeVal = -1;
//    while (cursor.moveToNext()) {
//      int ind = cursor.getColumnIndex(DataTableColumns.CONFLICT_TYPE);
//      int type = cursor.getType(ind);
//      assertEquals(type, Cursor.FIELD_TYPE_INTEGER);
//      conflictTypeVal = cursor.getInt(ind);
//    }
//
//    assertEquals(conflictType, conflictTypeVal);
//
//    // Test that the health of the table is CLEAN
//    int health = ODKDatabaseImplUtils.get().getTableHealth(db, tableId);
//
//    assertEquals(health, ODKCursorUtils.TABLE_HEALTH_HAS_CHECKPOINTS_AND_CONFLICTS);
//
//    // Drop the table now that the test is done
//    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
//  }

  /*
   * Test place row into conflict
   */
//  public void testPlaceRowIntoConflict_ExpectPass() {
//    String tableId = testTable;
//    String testCol = "testColumn";
//    String testColType = ElementDataType.integer.name();
//    List<Column> columns = new ArrayList<Column>();
//    columns.add(new Column(testCol, testCol, testColType, "[]"));
//    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
//        .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);
//    int testVal = 5;
//
//    ContentValues cvValues = new ContentValues();
//    String rowId = ODKDataUtils.genUUID();
//    cvValues.put(testCol, testVal);
//    ODKDatabaseImplUtils.get().insertDataIntoExistingDBTableWithId(db, tableId, orderedColumns,
//        cvValues, rowId);
//
//    // Select everything out of the table
//    String sel = "SELECT * FROM " + tableId + " WHERE " + testCol + " = ?";
//    String[] selArgs = { "" + testVal };
//    Cursor cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);
//
//    int val = 0;
//    while (cursor.moveToNext()) {
//      int ind = cursor.getColumnIndex(testCol);
//      int type = cursor.getType(ind);
//      assertEquals(type, Cursor.FIELD_TYPE_INTEGER);
//      val = cursor.getInt(ind);
//    }
//
//    assertEquals(val, testVal);
//
//    // Place row in conflict
//    int conflictType = ConflictType.LOCAL_DELETED_OLD_VALUES;
//    ODKDatabaseImplUtils.get().placeRowIntoConflict(db, tableId, rowId, conflictType);
//
//    // Run the query again and make sure that the place row in conflict worked as expected
//    cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);
//
//    int conflictTypeVal = -1;
//    while (cursor.moveToNext()) {
//      int ind = cursor.getColumnIndex(DataTableColumns.CONFLICT_TYPE);
//      int type = cursor.getType(ind);
//      assertEquals(type, Cursor.FIELD_TYPE_INTEGER);
//      conflictTypeVal = cursor.getInt(ind);
//    }
//
//    assertEquals(conflictType, conflictTypeVal);
//
//    // Drop the table now that the test is done
//    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
//  }

  /*
   * Test query distinct
   * Add two rows with the same data in a column
   * and make sure that only one is returned
   */
  public void testQueryDistinct_ExpectPass() {
    String tableId = testTable;
    String testCol = "testColumn";
    String testColType = ElementDataType.integer.name();
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[]"));
    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
        .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);

    int testVal = 5;
    boolean thrown = false;

    ContentValues cvValues = new ContentValues();
    cvValues.put(testCol, testVal);

    String uuid = UUID.randomUUID().toString();
    ODKDatabaseImplUtils.get().insertRowWithId(db, tableId, orderedColumns, cvValues, uuid);

    // Select everything out of the table
    String sel = "SELECT * FROM " + tableId + " WHERE " + testCol + " = ?";
    String[] selArgs = { "" + testVal };
    Cursor cursor = null;
    try {
      cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);
      assertEquals(cursor.getCount(), 1);

      int val = 0;
      while (cursor.moveToNext()) {
        int ind = cursor.getColumnIndex(testCol);
        int type = cursor.getType(ind);
        assertEquals(type, Cursor.FIELD_TYPE_INTEGER);
        val = cursor.getInt(ind);
      }

      assertEquals(val, testVal);
    } finally {
      if ( cursor != null && !cursor.isClosed() ) {
        cursor.close();
      }
    }
    // Add another row in the database with the same value
    String uuid2 = UUID.randomUUID().toString();
    ContentValues cvValues2 = new ContentValues();
    cvValues2.put(testCol, testVal);

    ODKDatabaseImplUtils.get().insertRowWithId(db, tableId, orderedColumns, cvValues2, uuid2);

    // Select everything out of the table
    String sel2 = "SELECT * FROM " + tableId;
    String[] selArgs2 = {};
    Cursor cursor2 = ODKDatabaseImplUtils.get().rawQuery(db, sel2, selArgs2);
    assertEquals(cursor2.getCount(), 2);

    System.out.println("testQueryDistinct_ExpectPass: after select *  query");
    OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().dumpInfo(false);

    // Make sure the values are correct
    int val2 = 0;
    while (cursor2.moveToNext()) {
      int ind = cursor2.getColumnIndex(testCol);
      int type = cursor2.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_INTEGER);
      val2 = cursor2.getInt(ind);
      assertEquals(val2, testVal);
    }

    // The moment of truth! test the queryDistinct
    // Get all of the rows of the database but only return testCol
    String [] retCols = {testCol};
    Cursor cursor3 = ODKDatabaseImplUtils.get().queryDistinct(db, tableId, retCols, null, null,
        null, null, null, null);
    assertEquals(cursor3.getCount(), 1);

    int val3 = 0;
    while (cursor3.moveToNext()) {
      int ind = cursor3.getColumnIndex(testCol);
      int type = cursor3.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_INTEGER);
      val3 = cursor3.getInt(ind);
    }

    assertEquals(val3, testVal);

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test replace metadata with KVS
   */
  public void testReplaceDBTableMetadataWithKVS_ExpectPass() {
    String tableId = testTable;
    String testCol = "testColumn";
    String testColType = ElementDataType.string.name();
    String partition = KeyValueStoreConstants.PARTITION_TABLE;
    String aspect = KeyValueStoreConstants.ASPECT_DEFAULT;
    String key = KeyValueStoreConstants.COLUMN_DISPLAY_NAME;
    String type = ElementDataType.object.name();
    String kvsValue = tableId;
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[]"));

    List<KeyValueStoreEntry> kvsEntries = new ArrayList<KeyValueStoreEntry>();
    KeyValueStoreEntry kvsEntry = KeyValueStoreUtils.buildEntry(tableId, partition,
        aspect, key, ElementDataType.valueOf(type), kvsValue);
    kvsEntries.add(kvsEntry);

    try {
      ODKDatabaseImplUtils.get()
          .createOrOpenDBTableWithColumnsAndProperties(db, getAppName(), tableId, columns,
              kvsEntries, true);
    } catch (Exception e){
      e.printStackTrace();
    }

    // Ensure that the expected properties is in the KVS table
    String sel = "SELECT * FROM " + DatabaseConstants.KEY_VALUE_STORE_ACTIVE_TABLE_NAME +
        " WHERE " + KeyValueStoreColumns.PARTITION + " = ? AND " + KeyValueStoreColumns.KEY +
        " = ? AND " + KeyValueStoreColumns.VALUE + " = ?";
    String[] selArgs = { partition, key, kvsValue };
    Cursor cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);
    assertEquals(cursor.getCount(), 1);

    // Make sure that the returned value is equal to the original value
    ArrayList<KeyValueStoreEntry> retKVSEntries =  ODKDatabaseImplUtils.get().getDBTableMetadata(db, tableId,
        partition, aspect, key);

    assertEquals(retKVSEntries.get(0), kvsEntries.get(0));

    // Replace the metadata
    String newKVSValue = "newTestTable";
    List<KeyValueStoreEntry> newKVSEntries = new ArrayList<KeyValueStoreEntry>();
    KeyValueStoreEntry newKVSEntry = KeyValueStoreUtils.buildEntry(tableId, partition,
        aspect, key, ElementDataType.valueOf(type), newKVSValue);
    newKVSEntries.add(newKVSEntry);
    ODKDatabaseImplUtils.get().replaceDBTableMetadata(db, tableId, newKVSEntries, true);

    String [] selArgs2 = { partition, key, newKVSValue };
    cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs2);
    assertEquals(cursor.getCount(), 1);

    // Make sure that the returned value is equal to the original value
    ArrayList<KeyValueStoreEntry> newRetKVSEntries =  ODKDatabaseImplUtils.get().getDBTableMetadata
        (db, tableId, partition, aspect, key);

    assertEquals(newRetKVSEntries.get(0), newKVSEntries.get(0));

    // Delete the metadata
    ODKDatabaseImplUtils.get().deleteDBTableMetadata(db, tableId, partition, aspect, key);

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test replace metadata
   */
  public void testReplaceDBTableMetadata_ExpectPass() {
    String tableId = testTable;
    String testCol = "testColumn";
    String testColType = ElementDataType.string.name();
    String partition = KeyValueStoreConstants.PARTITION_TABLE;
    String aspect = KeyValueStoreConstants.ASPECT_DEFAULT;
    String key = KeyValueStoreConstants.COLUMN_DISPLAY_NAME;
    String type = ElementDataType.object.name();
    String kvsValue = tableId;
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[]"));

    List<KeyValueStoreEntry> kvsEntries = new ArrayList<KeyValueStoreEntry>();
    KeyValueStoreEntry kvsEntry = KeyValueStoreUtils.buildEntry(tableId, partition,
        aspect, key, ElementDataType.valueOf(type), kvsValue);
    kvsEntries.add(kvsEntry);

    try {
      ODKDatabaseImplUtils.get()
          .createOrOpenDBTableWithColumnsAndProperties(db, getAppName(), tableId, columns,
              kvsEntries, true);
    } catch (Exception e){
      e.printStackTrace();
    }

    // Ensure that the expected properties is in the KVS table
    String sel = "SELECT * FROM " + DatabaseConstants.KEY_VALUE_STORE_ACTIVE_TABLE_NAME +
        " WHERE " + KeyValueStoreColumns.PARTITION + " = ? AND " + KeyValueStoreColumns.KEY +
        " = ? AND " + KeyValueStoreColumns.VALUE + " = ?";
    String[] selArgs = { partition, key, kvsValue };
    Cursor cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);
    assertEquals(cursor.getCount(), 1);

    // Make sure that the returned value is equal to the original value
    ArrayList<KeyValueStoreEntry> retKVSEntries =  ODKDatabaseImplUtils.get().getDBTableMetadata(db, tableId,
        partition, aspect, key);

    assertEquals(retKVSEntries.get(0), kvsEntries.get(0));

    // Replace the metadata
    String newKVSValue = "newTestTable";
    KeyValueStoreEntry newKVSEntry = KeyValueStoreUtils.buildEntry(tableId, partition,
        aspect, key, ElementDataType.valueOf(type), newKVSValue);
    ODKDatabaseImplUtils.get().replaceDBTableMetadata(db, newKVSEntry);

    String [] selArgs2 = { partition, key, newKVSValue };
    cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs2);
    assertEquals(cursor.getCount(), 1);

    // Make sure that the returned value is equal to the original value
    ArrayList<KeyValueStoreEntry> newRetKVSEntries =  ODKDatabaseImplUtils.get().getDBTableMetadata
        (db, tableId, partition, aspect, key);

    assertEquals(newRetKVSEntries.get(0), newKVSEntry);

    // Delete the metadata
    ODKDatabaseImplUtils.get().deleteDBTableMetadata(db, tableId, partition, aspect, key);

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test replace metadata sub list
   */
  public void testReplaceDBTableMetadataSubList_ExpectPass() {
    String tableId = testTable;
    String testCol = "testColumn";
    String testColType = ElementDataType.string.name();
    String partition = KeyValueStoreConstants.PARTITION_TABLE;
    String aspect = KeyValueStoreConstants.ASPECT_DEFAULT;
    String key = KeyValueStoreConstants.COLUMN_DISPLAY_NAME;
    String type = ElementDataType.object.name();
    String kvsValue = tableId;
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[]"));

    List<KeyValueStoreEntry> kvsEntries = new ArrayList<KeyValueStoreEntry>();
    KeyValueStoreEntry kvsEntry = KeyValueStoreUtils.buildEntry(tableId, partition,
        aspect, key, ElementDataType.valueOf(type), kvsValue);
    kvsEntries.add(kvsEntry);

    try {
      ODKDatabaseImplUtils.get()
          .createOrOpenDBTableWithColumnsAndProperties(db, getAppName(), tableId, columns,
              kvsEntries, true);
    } catch (Exception e){
      e.printStackTrace();
    }

    // Ensure that the expected properties is in the KVS table
    String sel = "SELECT * FROM " + DatabaseConstants.KEY_VALUE_STORE_ACTIVE_TABLE_NAME +
        " WHERE " + KeyValueStoreColumns.PARTITION + " = ? AND " + KeyValueStoreColumns.KEY +
        " = ? AND " + KeyValueStoreColumns.VALUE + " = ?";
    String[] selArgs = { partition, key, kvsValue };
    Cursor cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);
    assertEquals(cursor.getCount(), 1);

    // Make sure that the returned value is equal to the original value
    ArrayList<KeyValueStoreEntry> retKVSEntries =  ODKDatabaseImplUtils.get().getDBTableMetadata(db, tableId,
        partition, aspect, key);

    assertEquals(retKVSEntries.get(0), kvsEntries.get(0));

    // Replace the metadata
    List<KeyValueStoreEntry> newKVSEntries = new ArrayList<KeyValueStoreEntry>();
    String newKVSValue = "newTestTable";
    KeyValueStoreEntry newKVSEntry = KeyValueStoreUtils.buildEntry(tableId, partition,
        aspect, key, ElementDataType.valueOf(type), newKVSValue);
    newKVSEntries.add(newKVSEntry);
    ODKDatabaseImplUtils.get().replaceDBTableMetadataSubList(db, tableId, partition, aspect,
        newKVSEntries);

    String [] selArgs2 = { partition, key, newKVSValue };
    cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs2);
    assertEquals(cursor.getCount(), 1);

    // Make sure that the returned value is equal to the original value
    ArrayList<KeyValueStoreEntry> newRetKVSEntries =  ODKDatabaseImplUtils.get().getDBTableMetadata
        (db, tableId, partition, aspect, key);

    assertEquals(newRetKVSEntries.get(0), newKVSEntry);

    // Delete the metadata
    ODKDatabaseImplUtils.get().deleteDBTableMetadata(db, tableId, partition, aspect, key);

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test resolve server conflict with delete in existing table with id
   */
  public void testResolveServerConflictWithDeleteInExistingDbTableWithId_ExpectPass() {
    // Test this after restructuring of the Sync code
  }

  /*
   * Test resolve server conflict with update in existing table with id
   */
  public void testResolveServerConflictWithUpdateInExistingDbTableWithId_ExpectPass() {
    // Test this after restructuring of the Sync code
  }

  /*
   * Test restore row from conflict
   */
//  public void testRestoreRowFromConflict_ExpectPass() {
//    String tableId = testTable;
//    String testCol = "testColumn";
//    String testColType = ElementDataType.integer.name();
//    List<Column> columns = new ArrayList<Column>();
//    columns.add(new Column(testCol, testCol, testColType, "[]"));
//    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
//        .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);
//    int testVal = 5;
//
//    ContentValues cvValues = new ContentValues();
//    String rowId = ODKDataUtils.genUUID();
//    cvValues.put(testCol, testVal);
//    ODKDatabaseImplUtils.get().insertDataIntoExistingDBTableWithId(db, tableId, orderedColumns,
//        cvValues, rowId);
//
//    // Select everything out of the table
//    String sel = "SELECT * FROM " + tableId + " WHERE " + testCol + " = ?";
//    String[] selArgs = { "" + testVal };
//    Cursor cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);
//
//    int val = 0;
//    while (cursor.moveToNext()) {
//      int ind = cursor.getColumnIndex(testCol);
//      int type = cursor.getType(ind);
//      assertEquals(type, Cursor.FIELD_TYPE_INTEGER);
//      val = cursor.getInt(ind);
//    }
//
//    assertEquals(val, testVal);
//
//    // Place row in conflict
//    int conflictType = ConflictType.LOCAL_DELETED_OLD_VALUES;
//    ODKDatabaseImplUtils.get().placeRowIntoConflict(db, tableId, rowId, conflictType);
//
//    // Run the query again and make sure that the place row in conflict worked as expected
//    cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);
//
//    int conflictTypeVal = -1;
//    while (cursor.moveToNext()) {
//      int ind = cursor.getColumnIndex(DataTableColumns.CONFLICT_TYPE);
//      int type = cursor.getType(ind);
//      assertEquals(type, Cursor.FIELD_TYPE_INTEGER);
//      conflictTypeVal = cursor.getInt(ind);
//    }
//
//    assertEquals(conflictType, conflictTypeVal);
//
//    // Restore row from conflict
//    ODKDatabaseImplUtils.get().restoreRowFromConflict(db, tableId, rowId, SyncState.synced, conflictType);
//
//    // Run the query again and make sure that the restore row
//    // from conflict worked as expected
//    cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);
//    assertEquals(cursor.getCount(), 1);
//
//    while (cursor.moveToNext()) {
//      int ind = cursor.getColumnIndex(DataTableColumns.CONFLICT_TYPE);
//      assertTrue(cursor.isNull(ind));
//
//      int indSyncState = cursor.getColumnIndex(DataTableColumns.SYNC_STATE);
//      int typeSyncState = cursor.getType(indSyncState);
//      assertEquals(typeSyncState, Cursor.FIELD_TYPE_STRING);
//      String syncStateVal = cursor.getString(indSyncState);
//      assertEquals(syncStateVal, SyncState.synced.name());
//    }
//
//    // Drop the table now that the test is done
//    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
//  }

  /*
   * Test server table schema eTag changed
   */
  public void testServerTableSchemaETagChanged_ExpectPass() {
    // Test this after restructuring of the Sync code
  }

  /*
   * Test set choice list
   */
  public void testSetChoiceList() {
    ArrayList<Object> values = new ArrayList<Object>();
    Map<String,Object> myMap = new TreeMap<String,Object>();
    Map<String, Object> displayText = new TreeMap<String, Object>();
    displayText.put("text", "displayText");
    myMap.put("choice_list_name", "test_list");
    myMap.put("data_value", "test");
    myMap.put("display", displayText);
    values.add(myMap);

    String jsonChoiceList  = null;
    try {
      jsonChoiceList = ODKFileUtils.mapper.writeValueAsString(values);
    } catch (JsonProcessingException e) {
      e.printStackTrace();
    }
    String choiceListId = ODKDatabaseImplUtils.get().setChoiceList(db, getAppName(), jsonChoiceList);

    // Select the _choice_list_id from the _choice_lists table
    String sel = "SELECT * FROM " + DatabaseConstants.CHOICE_LIST_TABLE_NAME +
        " WHERE " + ChoiceListColumns.CHOICE_LIST_ID + " = ?";
    String[] selArgs = { "" + choiceListId };
    Cursor cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);
    assertEquals(cursor.getCount(), 1);

    String val = null;
    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(ChoiceListColumns.CHOICE_LIST_JSON);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_STRING);
      val = cursor.getString(ind);
    }

    assertEquals(val, jsonChoiceList);
  }

  /*
   * Test update table eTags
   */
  public void testUpdateDBTableETags() {
    String tableId = testTable;
    String testCol = "testColumn";
    String testColType = ElementDataType.integer.name();
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[]"));
    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
        .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);
    int testVal = 5;

    ContentValues cvValues = new ContentValues();
    String rowId = ODKDataUtils.genUUID();
    cvValues.put(testCol, testVal);
    ODKDatabaseImplUtils.get().insertRowWithId(db, tableId, orderedColumns, cvValues, rowId);

    // Select everything out of the table
    String sel = "SELECT * FROM " + tableId + " WHERE " + testCol + " = ?";
    String[] selArgs = { "" + testVal };
    Cursor cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);
    assertEquals(cursor.getCount(), 1);

    int val = 0;
    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(testCol);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_INTEGER);
      val = cursor.getInt(ind);
    }

    assertEquals(val, testVal);

    // Select everything out of the table
    String sel2 = "SELECT * FROM " + DatabaseConstants.TABLE_DEFS_TABLE_NAME +
        " WHERE " + TableDefinitionsColumns.TABLE_ID + " = ?";
    String [] selArgs2 = { "" + tableId };
    cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel2, selArgs2);
    assertEquals(cursor.getCount(), 1);

    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(TableDefinitionsColumns.SCHEMA_ETAG);
      assertTrue(cursor.isNull(ind));

      int ind2 = cursor.getColumnIndex(TableDefinitionsColumns.LAST_DATA_ETAG);
      assertTrue(cursor.isNull(ind2));
    }

    // update db schema etag and last data etag
    String newSchemaETag = ODKDataUtils.genUUID();
    String newLastDataETag = ODKDataUtils.genUUID();
    ODKDatabaseImplUtils.get().updateDBTableETags(db, tableId, newSchemaETag, newLastDataETag);

    // Select everything out of the table
    cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel2, selArgs2);
    assertEquals(cursor.getCount(), 1);

    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(TableDefinitionsColumns.SCHEMA_ETAG);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_STRING);
      assertEquals(newSchemaETag, cursor.getString(ind));

      int ind2 = cursor.getColumnIndex(TableDefinitionsColumns.LAST_DATA_ETAG);
      int type2 = cursor.getType(ind2);
      assertEquals(type2, Cursor.FIELD_TYPE_STRING);
      assertEquals(newLastDataETag, cursor.getString(ind2));
    }

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test update table last sync time
   */
  public void testUpdateDBTableLastSyncTime_ExpectPass() {
    String tableId = testTable;
    String testCol = "testColumn";
    String testColType = ElementDataType.integer.name();
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[]"));
    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
        .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);
    int testVal = 5;

    ContentValues cvValues = new ContentValues();
    String rowId = ODKDataUtils.genUUID();
    cvValues.put(testCol, testVal);
    ODKDatabaseImplUtils.get().insertRowWithId(db, tableId, orderedColumns, cvValues, rowId);

    // Select everything out of the table
    String sel = "SELECT * FROM " + tableId + " WHERE " + testCol + " = ?";
    String[] selArgs = { "" + testVal };
    Cursor cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);
    assertEquals(cursor.getCount(), 1);

    int val = 0;
    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(testCol);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_INTEGER);
      val = cursor.getInt(ind);
    }

    assertEquals(val, testVal);

    // Select everything out of the table
    String sel2 = "SELECT * FROM " + DatabaseConstants.TABLE_DEFS_TABLE_NAME +
        " WHERE " + TableDefinitionsColumns.TABLE_ID + " = ?";
    String [] selArgs2 = { "" + tableId };
    cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel2, selArgs2);
    assertEquals(cursor.getCount(), 1);

    String defaultSyncTime = "-1";
    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(TableDefinitionsColumns.LAST_SYNC_TIME);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_STRING);
      assertEquals(cursor.getString(ind), defaultSyncTime);
    }

    // udpate db table last sync time
    ODKDatabaseImplUtils.get().updateDBTableLastSyncTime(db, tableId);

    // Select everything out of the table
    cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel2, selArgs2);
    assertEquals(cursor.getCount(), 1);

    String syncTime = null;
    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(TableDefinitionsColumns.LAST_SYNC_TIME);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_STRING);
      syncTime = cursor.getString(ind);
    }

    // CAL: Should there be more checks here?
    assertNotSame(syncTime, defaultSyncTime);

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test update row eTag and sync state
   */
  public void testUpdateRowETagAndSyncState_ExpectPass() {
    String tableId = testTable;
    String testCol = "testColumn";
    String testColType = ElementDataType.integer.name();
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[]"));
    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
        .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);
    int testVal = 5;

    ContentValues cvValues = new ContentValues();
    String rowId = ODKDataUtils.genUUID();
    cvValues.put(testCol, testVal);
    ODKDatabaseImplUtils.get().insertRowWithId(db, tableId, orderedColumns, cvValues, rowId);

    // Select everything out of the table
    String sel = "SELECT * FROM " + tableId + " WHERE " + testCol + " = ?";
    String[] selArgs = { "" + testVal };
    Cursor cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);

    int val = 0;
    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(testCol);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_INTEGER);
      val = cursor.getInt(ind);
    }

    assertEquals(val, testVal);

    // Update the row ETag and sync state
    String rowETag = ODKDataUtils.genUUID();
    ODKDatabaseImplUtils.get().updateRowETagAndSyncState(db, tableId, rowId, rowETag, SyncState.synced);

    // Run the query again and make sure that the place row in conflict worked as expected
    cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);

    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(DataTableColumns.ROW_ETAG);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_STRING);
      String rowETagVal = cursor.getString(ind);
      assertEquals(rowETag, rowETagVal);

      int indSyncState = cursor.getColumnIndex(DataTableColumns.SYNC_STATE);
      int typeSyncState = cursor.getType(indSyncState);
      assertEquals(typeSyncState, Cursor.FIELD_TYPE_STRING);
      String syncStateVal = cursor.getString(indSyncState);
      assertEquals(syncStateVal, SyncState.synced.name());
    }

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /**
   * Generates sequential unique IDs starting with 1, 2, 3, and so on.
   * <p>
   * This class is thread-safe.
   * </p>
   */
  static class UniqueIdGenerator {
    private final AtomicLong counter = new AtomicLong(0);

    public long nextId() {
      return counter.incrementAndGet();
    }
  }

  private List<Long> threadTest(final int threadCount, final String tableId, final String
      rowId, final String colPrefix, final OrderedColumns orderedColumns, final boolean useNewDB,
      final boolean multipleWrites, final int numOfMultiWrites) throws
      InterruptedException, ExecutionException {

    final UniqueIdGenerator domainObject = new UniqueIdGenerator();
    Callable<Long> task = new Callable<Long>() {
      @Override
      public synchronized Long call() {
        Long origVal = domainObject.nextId();
        int testVal = origVal.intValue();
        String testCol = colPrefix + testVal;
        ContentValues cvValues = null;

        OdkConnectionInterface dbToUse = db;
        if (useNewDB) {
          OdkDbHandle uniqueKey = new OdkDbHandle(AbstractODKDatabaseUtilsTest.class
              .getSimpleName() + testVal + AndroidConnectFactory.INTERNAL_TYPE_SUFFIX);
          dbToUse = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().getConnection(getAppName(), uniqueKey);
        }

        if (multipleWrites) {
          for (int i = 1; i <= numOfMultiWrites; i++) {
            cvValues = new ContentValues();
            cvValues.put(testCol, i);
            ODKDatabaseImplUtils.get().updateRowWithId(dbToUse, tableId, orderedColumns, cvValues,
                rowId);
            try {
              Thread.sleep(0);
            } catch (Exception e) {
              e.printStackTrace();
            }
          }
        } else {
          cvValues = new ContentValues();
          cvValues.put(testCol, testVal);
          ODKDatabaseImplUtils.get().updateRowWithId(dbToUse, tableId, orderedColumns, cvValues,
              rowId);
        }


        if (dbToUse != null && useNewDB) {
          dbToUse.releaseReference();
        }
        return origVal;
      }
    };
    List<Callable<Long>> tasks = Collections.nCopies(threadCount, task);
    ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
    List<Future<Long>> futures = executorService.invokeAll(tasks);
    List<Long> resultList = new ArrayList<Long>(futures.size());

    // Check for exceptions
    for (Future<Long> future : futures) {
      // Throws an exception if an exception was thrown by the task.
      resultList.add(future.get());
    }
    // Validate the IDs
    assertEquals(threadCount, futures.size());
    List<Long> expectedList = new ArrayList<Long>(threadCount);
    for (long i = 1; i <= threadCount; i++) {
      expectedList.add(i);
    }
    Collections.sort(resultList);
    assertEquals(expectedList, resultList);

    return resultList;
  }

  /*
   * Test multi-threaded test for inserting data into the database
   */
  public void testMultithreadedDBInsertionWithoutClosingCursor_ExpectPass() {
    int numOfThreads = 5;
    String tableId = testTable;
    String colPrefix = "testColumn";
    String testColType = ElementDataType.integer.name();
    List<Column> columns = new ArrayList<Column>();

    // Create table with the right number of columns
    for (int i = 0; i <= numOfThreads; i++) {
      String testCol = colPrefix + i;
      columns.add(new Column(testCol, testCol, testColType, "[]"));
    }

    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
        .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);

    // Insert data so that the threads can all just update
    int testVal = 0;
    String setupTestCol = colPrefix + 0;
    ContentValues cvValues = new ContentValues();
    String rowId = ODKDataUtils.genUUID();
    cvValues.put(setupTestCol, testVal);
    ODKDatabaseImplUtils.get().insertRowWithId(db, tableId, orderedColumns, cvValues, rowId);

    // Ensure that the row exists
    String sel = "SELECT * FROM " + tableId + " WHERE " + setupTestCol + " = ?";
    String[] selArgs = { "" + testVal };
    Cursor cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);


    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(setupTestCol);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_INTEGER);
      int val = cursor.getInt(ind);
      assertEquals(val, testVal);
    }

    // Have the threads all update the corresponding column in the table
    try {
      threadTest(numOfThreads, tableId, rowId, colPrefix, orderedColumns, false, false, 0);
    } catch (Exception e) {
      e.printStackTrace();
    }

    // Ensure that the row exists
    String sel2 = "SELECT * FROM " + tableId + " WHERE " + DataTableColumns.ID + " = ?";
    String[] selArgs2 = { "" + rowId };
    Cursor cursor2 = ODKDatabaseImplUtils.get().rawQuery(db, sel2, selArgs2);

    assertEquals(cursor2.getCount(), 1);

    System.out.println("testMultithreadedDBInsertionWithoutClosingCursor_ExpectPass: before assert");
    OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().dumpInfo(false);

    while (cursor2.moveToNext()) {
      for (int i = 0; i <= numOfThreads; i++) {
        String columnName = colPrefix + i;
        int ind = cursor2.getColumnIndex(columnName);
        int type = cursor2.getType(ind);
        assertEquals(type, Cursor.FIELD_TYPE_INTEGER);
        int val = cursor2.getInt(ind);
        assertEquals(val, i);
      }
    }

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test multi-threaded test for inserting data into the database
   */
  public void testMultithreadedDBInsertionWithClosingCursor_ExpectPass() {
    int numOfThreads = 5;
    String tableId = testTable;
    String colPrefix = "testColumn";
    String testColType = ElementDataType.integer.name();
    List<Column> columns = new ArrayList<Column>();

    // Create table with the right number of columns
    for (int i = 0; i <= numOfThreads; i++) {
      String testCol = colPrefix + i;
      columns.add(new Column(testCol, testCol, testColType, "[]"));
    }

    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
        .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);

    // Insert data so that the threads can all just update
    int testVal = 0;
    String setupTestCol = colPrefix + 0;
    ContentValues cvValues = new ContentValues();
    String rowId = ODKDataUtils.genUUID();
    cvValues.put(setupTestCol, testVal);
    ODKDatabaseImplUtils.get().insertRowWithId(db, tableId, orderedColumns, cvValues, rowId);

    // Ensure that the row exists
    String sel = "SELECT * FROM " + tableId + " WHERE " + setupTestCol + " = ?";
    String[] selArgs = { "" + testVal };
    Cursor cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);

    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(setupTestCol);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_INTEGER);
      int val = cursor.getInt(ind);
      assertEquals(val, testVal);
    }

    if (cursor != null && !cursor.isClosed()){
      cursor.close();
    }

    List<Long> returnedResults = null;

    // Have the threads all update the corresponding column in the table
    try {
      returnedResults = threadTest(numOfThreads, tableId, rowId, colPrefix, orderedColumns,
          false, false, 0);
    } catch (Exception e) {
      e.printStackTrace();
    }

    // Extra check to make sure that this has finished before
    // anything continues
    List<Long> expectedList = new ArrayList<Long>(numOfThreads);
    for (long i = 1; i <= numOfThreads; i++) {
      expectedList.add(i);
    }
    if (returnedResults != null) {
      Collections.sort(returnedResults);
    }

    assertEquals(expectedList, returnedResults);

    // Ensure that the row exists
    String sel2 = "SELECT * FROM " + tableId;
    String[] selArgs2 = null;
    Cursor cursor2 = ODKDatabaseImplUtils.get().rawQuery(db, sel2, selArgs2);

    assertEquals(cursor2.getCount(), 1);

    System.out.println("testMultithreadedDBInsertionWithClosingCursor_ExpectPass: before assert");
    OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().dumpInfo(false);

    while (cursor2.moveToNext()) {
      assertEquals(cursor2.getColumnIndex(colPrefix), -1);
      for (int i = 0; i <= numOfThreads; i++) {
        String columnName = colPrefix + i;
        int ind = cursor2.getColumnIndex(columnName);
        int type = cursor2.getType(ind);
        assertEquals(type, Cursor.FIELD_TYPE_INTEGER);
        int val = cursor2.getInt(ind);
        assertEquals(val, i);
      }
    }

    if (cursor2 != null && !cursor2.isClosed()) {
      cursor2.close();
    }

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
 * Test multi-threaded test for inserting data into the database
 */
  public void testMultithreadedDBInsertionWithClosingCursorAndOrigConn_ExpectPass() {
    int numOfThreads = 5;
    String tableId = testTable;
    String colPrefix = "testColumn";
    String testColType = ElementDataType.integer.name();
    List<Column> columns = new ArrayList<Column>();

    // Create table with the right number of columns
    for (int i = 0; i <= numOfThreads; i++) {
      String testCol = colPrefix + i;
      columns.add(new Column(testCol, testCol, testColType, "[]"));
    }

    String uniqueUUID = ODKDataUtils.genUUID();
    OdkDbHandle prevUniqueKey = new OdkDbHandle(AbstractODKDatabaseUtilsTest.class
        .getSimpleName() + uniqueUUID + AndroidConnectFactory.INTERNAL_TYPE_SUFFIX);
    OdkConnectionInterface prevDb = OdkConnectionFactorySingleton
        .getOdkConnectionFactoryInterface().getConnection(getAppName(), prevUniqueKey);

    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
        .createOrOpenDBTableWithColumns(prevDb, getAppName(), tableId, columns);

    // Insert data so that the threads can all just update
    int testVal = 0;
    String setupTestCol = colPrefix + 0;
    ContentValues cvValues = new ContentValues();
    String rowId = ODKDataUtils.genUUID();
    cvValues.put(setupTestCol, testVal);
    ODKDatabaseImplUtils.get().insertRowWithId(prevDb, tableId, orderedColumns, cvValues, rowId);

    // Ensure that the row exists
    String sel = "SELECT * FROM " + tableId + " WHERE " + setupTestCol + " = ?";
    String[] selArgs = { "" + testVal };
    Cursor cursor = ODKDatabaseImplUtils.get().rawQuery(prevDb, sel, selArgs);

    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(setupTestCol);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_INTEGER);
      int val = cursor.getInt(ind);
      assertEquals(val, testVal);
    }

    if (cursor != null && !cursor.isClosed()){
      cursor.close();
    }

    List<Long> returnedResults = null;

    // Have the threads all update the corresponding column in the table
    try {
      returnedResults = threadTest(numOfThreads, tableId, rowId, colPrefix, orderedColumns, true,
          false, 0);
    } catch (Exception e) {
      e.printStackTrace();
    }

    // Extra check to make sure that this has finished before
    // anything continues
    List<Long> expectedList = new ArrayList<Long>(numOfThreads);
    for (long i = 1; i <= numOfThreads; i++) {
      expectedList.add(i);
    }
    if (returnedResults != null) {
      Collections.sort(returnedResults);
    }

    assertEquals(expectedList, returnedResults);

    // Ensure that the row exists
    String sel2 = "SELECT * FROM " + tableId;
    String[] selArgs2 = null;
    Cursor cursor2 = ODKDatabaseImplUtils.get().rawQuery(prevDb, sel2, selArgs2);

    assertEquals(cursor2.getCount(), 1);

    System.out.println("testMultithreadedDBInsertionWithClosingCursor_ExpectPass: before assert");
    OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().dumpInfo(false);

    while (cursor2.moveToNext()) {
      assertEquals(cursor2.getColumnIndex(colPrefix), -1);
      for (int i = 0; i <= numOfThreads; i++) {
        String columnName = colPrefix + i;
        int ind = cursor2.getColumnIndex(columnName);
        int type = cursor2.getType(ind);
        assertEquals(type, Cursor.FIELD_TYPE_INTEGER);
        int val = cursor2.getInt(ind);
        assertEquals(val, i);
      }
    }

    if (cursor2 != null && !cursor2.isClosed()) {
      cursor2.close();
    }

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test multi-threaded test for inserting data into the database
   */
  public void testMultithreadedDBInsertionWithDBIntPerThreadAndForQuery_ExpectPass() {
    int numOfThreads = 5;
    String tableId = testTable;
    String colPrefix = "testColumn";
    String testColType = ElementDataType.integer.name();
    List<Column> columns = new ArrayList<Column>();

    // Create table with the right number of columns
    for (int i = 0; i <= numOfThreads; i++) {
      String testCol = colPrefix + i;
      columns.add(new Column(testCol, testCol, testColType, "[]"));
    }

    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
        .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);

    // Insert data so that the threads can all just update
    int testVal = 0;
    String setupTestCol = colPrefix + 0;
    ContentValues cvValues = new ContentValues();
    String rowId = ODKDataUtils.genUUID();
    cvValues.put(setupTestCol, testVal);
    ODKDatabaseImplUtils.get().insertRowWithId(db, tableId, orderedColumns, cvValues, rowId);

    // Ensure that the row exists
    String sel = "SELECT * FROM " + tableId + " WHERE " + setupTestCol + " = ?";
    String[] selArgs = { "" + testVal };
    Cursor cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);

    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(setupTestCol);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_INTEGER);
      int val = cursor.getInt(ind);
      assertEquals(val, testVal);
    }

    if (cursor != null && !cursor.isClosed()){
      cursor.close();
    }

    // Have the threads all update the corresponding column in the table
    try {
      threadTest(numOfThreads, tableId, rowId, colPrefix, orderedColumns, true, false, 0);
    } catch (Exception e) {
      e.printStackTrace();
    }

    // Ensure that the row exists
    String sel2 = "SELECT * FROM " + tableId;
    String[] selArgs2 = null;

    // Query with new connection to see if this gets all recent operations
    OdkDbHandle uniqueKey = new OdkDbHandle(AbstractODKDatabaseUtilsTest.class
        .getSimpleName() + testVal + AndroidConnectFactory.INTERNAL_TYPE_SUFFIX);
    OdkConnectionInterface dbForQuery = OdkConnectionFactorySingleton
        .getOdkConnectionFactoryInterface().getConnection
        (getAppName(), uniqueKey);

    Cursor cursor2 = ODKDatabaseImplUtils.get().rawQuery(dbForQuery, sel2, selArgs2);

    assertEquals(cursor2.getCount(), 1);

    System.out.println("testMultithreadedDBInsertionWithDBIntPerThreadAndForQuery_ExpectPass: before assert");
    OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().dumpInfo(false);

    while (cursor2.moveToNext()) {
      for (int i = 1; i <= numOfThreads; i++) {
        System.out.println("testMultithreadedDBInsertionWithDBIntPerThreadAndForQuery_ExpectPass: assertion "
            + "for thread " + i);
        String columnName = colPrefix + i;
        int ind = cursor2.getColumnIndex(columnName);
        int type = cursor2.getType(ind);
        assertEquals(type, Cursor.FIELD_TYPE_INTEGER);
        int val = cursor2.getInt(ind);
        assertEquals(val, i);
      }
    }

    if (cursor2 != null && !cursor2.isClosed()) {
      cursor2.close();
    }

    System.out.println("testMultithreadedDBInsertionWithDBIntPerThreadAndForQuery_ExpectPass: after assert");
    OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().dumpInfo(false);

    // Release the OdkConnectionInterface used for the query
    dbForQuery.releaseReference();

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test multi-threaded test for inserting data into the database
   */
  public void testMultithreadedDBInsertionWithDBIntPerThreadWithTxn_ExpectPass() {
    int numOfThreads = 5;
    String tableId = testTable;
    String colPrefix = "testColumn";
    String testColType = ElementDataType.integer.name();
    List<Column> columns = new ArrayList<Column>();

    // Create table with the right number of columns
    for (int i = 0; i <= numOfThreads; i++) {
      String testCol = colPrefix + i;
      columns.add(new Column(testCol, testCol, testColType, "[]"));
    }

    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
        .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);

    // Insert data so that the threads can all just update
    int testVal = 0;
    String setupTestCol = colPrefix + 0;
    ContentValues cvValues = new ContentValues();
    String rowId = ODKDataUtils.genUUID();
    cvValues.put(setupTestCol, testVal);
    ODKDatabaseImplUtils.get().insertRowWithId(db, tableId, orderedColumns, cvValues, rowId);

    // Ensure that the row exists
    String sel = "SELECT * FROM " + tableId + " WHERE " + setupTestCol + " = ?";
    String[] selArgs = { "" + testVal };
    Cursor cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);

    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(setupTestCol);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_INTEGER);
      int val = cursor.getInt(ind);
      assertEquals(val, testVal);
    }

    if (cursor != null && !cursor.isClosed()){
      cursor.close();
    }

    // Have the threads all update the corresponding column in the table
    try {
      threadTest(numOfThreads, tableId, rowId, colPrefix, orderedColumns, true, false, 0);
    } catch (Exception e) {
      e.printStackTrace();
    }

    // Ensure that the row exists
    boolean dbWithinTrxn = db.inTransaction();
    if (!dbWithinTrxn) {
      db.beginTransactionExclusive();
      String sel2 = "SELECT * FROM " + tableId;
      String[] selArgs2 = null;
      Cursor cursor2 = ODKDatabaseImplUtils.get().rawQuery(db, sel2, selArgs2);

      assertEquals(cursor2.getCount(), 1);

      System.out.println("testMultithreadedDBInsertionWithDBIntPerThreadWithTxn_ExpectPass: before assert");
      OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().dumpInfo(false);

      while (cursor2.moveToNext()) {
        for (int i = 1; i <= numOfThreads; i++) {
          System.out.println("testMultithreadedDBInsertionWithDBIntPerThreadWithTxn_ExpectPass: assertion "
              + "for thread " + i);
          String columnName = colPrefix + i;
          int ind = cursor2.getColumnIndex(columnName);
          int type = cursor2.getType(ind);
          assertEquals(type, Cursor.FIELD_TYPE_INTEGER);
          int val = cursor2.getInt(ind);
          assertEquals(val, i);
        }
      }

      if (cursor2 != null && !cursor2.isClosed()) {
        cursor2.close();
      }

      db.setTransactionSuccessful();
      db.endTransaction();
    }

    System.out.println("testMultithreadedDBInsertionWithDBIntPerThreadWithTxn_ExpectPass: after assert");
    OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().dumpInfo(false);

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test multi-threaded test for inserting data into the database
   */
  public void testMultithreadedDBInsertionWithDBIntPerThreadWithTxnOnUpdate_ExpectPass() {
    int numOfThreads = 5;
    String tableId = testTable;
    String colPrefix = "testColumn";
    String testColType = ElementDataType.integer.name();
    List<Column> columns = new ArrayList<Column>();

    // Create table with the right number of columns
    for (int i = 0; i <= numOfThreads; i++) {
      String testCol = colPrefix + i;
      columns.add(new Column(testCol, testCol, testColType, "[]"));
    }

    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
        .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);

    // Insert data so that the threads can all just update
    int testVal = 0;
    String setupTestCol = colPrefix + 0;
    ContentValues cvValues = new ContentValues();
    String rowId = ODKDataUtils.genUUID();
    cvValues.put(setupTestCol, testVal);
    ODKDatabaseImplUtils.get().insertRowWithId(db, tableId, orderedColumns, cvValues, rowId);

    // Ensure that the row exists
    String sel = "SELECT * FROM " + tableId + " WHERE " + setupTestCol + " = ?";
    String[] selArgs = { "" + testVal };
    Cursor cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);

    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(setupTestCol);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_INTEGER);
      int val = cursor.getInt(ind);
      assertEquals(val, testVal);
    }

    if (cursor != null && !cursor.isClosed()){
      cursor.close();
    }

    // Have the threads all update the corresponding column in the table
    try {
      threadTest(numOfThreads, tableId, rowId, colPrefix, orderedColumns, true, false, 0);
    } catch (Exception e) {
      e.printStackTrace();
    }

    // Ensure that the row exists
    boolean dbWithinTrxn = db.inTransaction();
    int testValAgain = 100;
    if (!dbWithinTrxn) {
      db.beginTransactionExclusive();
      ContentValues cvValuesAgain = new ContentValues();
      cvValuesAgain.put(setupTestCol, testValAgain);
      ODKDatabaseImplUtils.get().updateRowWithId(db, tableId, orderedColumns, cvValuesAgain, rowId);
      db.setTransactionSuccessful();
      db.endTransaction();
    }

    String sel2 = "SELECT * FROM " + tableId;
    String[] selArgs2 = null;
    Cursor cursor2 = ODKDatabaseImplUtils.get().rawQuery(db, sel2, selArgs2);

    assertEquals(cursor2.getCount(), 1);

    System.out.println("testMultithreadedDBInsertionWithDBIntPerThreadWithTxn_ExpectPass: before assert");
    OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().dumpInfo(false);

    while (cursor2.moveToNext()) {
      int indAgain = cursor2.getColumnIndex(setupTestCol);
      int typeAgain = cursor2.getType(indAgain);
      assertEquals(typeAgain, Cursor.FIELD_TYPE_INTEGER);
      int valAgain = cursor2.getInt(indAgain);
      assertEquals(valAgain, testValAgain);
      for (int i = 1; i <= numOfThreads; i++) {
        System.out.println("testMultithreadedDBInsertionWithDBIntPerThreadWithTxn_ExpectPass: assertion "
            + "for thread " + i);
        String columnName = colPrefix + i;
        int ind = cursor2.getColumnIndex(columnName);
        int type = cursor2.getType(ind);
        assertEquals(type, Cursor.FIELD_TYPE_INTEGER);
        int val = cursor2.getInt(ind);
        assertEquals(val, i);
      }
    }

    if (cursor2 != null && !cursor2.isClosed()) {
      cursor2.close();
    }

    System.out.println("testMultithreadedDBInsertionWithDBIntPerThreadWithTxn_ExpectPass: after assert");
    OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().dumpInfo(false);

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test multi-threaded test for inserting data into the database
   */
  public void testMultithreadedMultipleDBInsertionWithNewDBForQuery_ExpectPass() {
    int numOfThreads = 20;
    String tableId = testTable;
    String colPrefix = "testColumn";
    String testColType = ElementDataType.integer.name();
    List<Column> columns = new ArrayList<Column>();

    // Create table with the right number of columns
    for (int i = 0; i <= numOfThreads; i++) {
      String testCol = colPrefix + i;
      columns.add(new Column(testCol, testCol, testColType, "[]"));
    }

    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
        .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);

    // Insert data so that the threads can all just update
    int testVal = 0;
    String setupTestCol = colPrefix + 0;
    ContentValues cvValues = new ContentValues();
    String rowId = ODKDataUtils.genUUID();
    cvValues.put(setupTestCol, testVal);
    ODKDatabaseImplUtils.get().insertRowWithId(db, tableId, orderedColumns, cvValues, rowId);

    // Ensure that the row exists
    String sel = "SELECT * FROM " + tableId + " WHERE " + setupTestCol + " = ?";
    String[] selArgs = { "" + testVal };
    Cursor cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);

    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(setupTestCol);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_INTEGER);
      int val = cursor.getInt(ind);
      assertEquals(val, testVal);
    }

    if (cursor != null && !cursor.isClosed()){
      cursor.close();
    }

    // Have the threads all update the corresponding column in the table
    int numOfWritesForThreads = 100;
    try {
      threadTest(numOfThreads, tableId, rowId, colPrefix, orderedColumns, true, true,
          numOfWritesForThreads);
    } catch (Exception e) {
      e.printStackTrace();
    }

    // Ensure that the row exists
    Cursor cursor2 = null;

    // Try this to see if it makes a difference
    // Query with new connection to see if this gets all recent operations
    String uuid = ODKDataUtils.genUUID();
    OdkDbHandle uniqueKey = new OdkDbHandle(AbstractODKDatabaseUtilsTest.class
        .getSimpleName() + uuid + AndroidConnectFactory.INTERNAL_TYPE_SUFFIX);
    OdkConnectionInterface dbForQuery = OdkConnectionFactorySingleton
        .getOdkConnectionFactoryInterface().getConnection
            (getAppName(), uniqueKey);

    String sel2 = "SELECT * FROM " + tableId;
    String[] selArgs2 = null;
    cursor2 = ODKDatabaseImplUtils.get().rawQuery(dbForQuery, sel2, selArgs2);

    assertEquals(cursor2.getCount(), 1);

    System.out.println("testMultithreadedMultipleDBInsertion_ExpectPass: before assert");
    OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().dumpInfo(false);

    while (cursor2.moveToNext()) {
      int indAgain = cursor2.getColumnIndex(setupTestCol);
      int typeAgain = cursor2.getType(indAgain);
      assertEquals(typeAgain, Cursor.FIELD_TYPE_INTEGER);
      int valAgain = cursor2.getInt(indAgain);
      assertEquals(valAgain, testVal);
      for (int i = 1; i <= numOfThreads; i++) {
        System.out.println("testMultithreadedMultipleDBInsertion_ExpectPass: assertion "
            + "for thread " + i);
        String columnName = colPrefix + i;
        int ind = cursor2.getColumnIndex(columnName);
        int type = cursor2.getType(ind);
        assertEquals(type, Cursor.FIELD_TYPE_INTEGER);
        int val = cursor2.getInt(ind);
        assertEquals(val, numOfWritesForThreads);
      }
    }

    if (cursor2 != null && !cursor2.isClosed()) {
      cursor2.close();
    }

    System.out.println("testMultithreadedMultipleDBInsertion_ExpectPass: after assert");
    OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().dumpInfo(false);

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
* Test multi-threaded test for inserting data into the database
*/
  public void testMultithreadedMultipleDBInsertionWithSameSelect_ExpectPass() {
    int numOfThreads = 20;
    String tableId = testTable;
    String colPrefix = "testColumn";
    String testColType = ElementDataType.integer.name();
    List<Column> columns = new ArrayList<Column>();

    // Create table with the right number of columns
    for (int i = 0; i <= numOfThreads; i++) {
      String testCol = colPrefix + i;
      columns.add(new Column(testCol, testCol, testColType, "[]"));
    }

    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
        .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);

    // Insert data so that the threads can all just update
    int testVal = 0;
    String setupTestCol = colPrefix + 0;
    ContentValues cvValues = new ContentValues();
    String rowId = ODKDataUtils.genUUID();
    cvValues.put(setupTestCol, testVal);
    ODKDatabaseImplUtils.get().insertRowWithId(db, tableId, orderedColumns, cvValues, rowId);

    // Ensure that the row exists
    String sel = "SELECT * FROM " + tableId;
    String[] selArgs = null;
    Cursor cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);

    while (cursor.moveToNext()) {
      int ind = cursor.getColumnIndex(setupTestCol);
      int type = cursor.getType(ind);
      assertEquals(type, Cursor.FIELD_TYPE_INTEGER);
      int val = cursor.getInt(ind);
      assertEquals(val, testVal);
    }

    if (cursor != null && !cursor.isClosed()){
      cursor.close();
    }

    // Have the threads all update the corresponding column in the table
    int numOfWritesForThreads = 100;
    try {
      threadTest(numOfThreads, tableId, rowId, colPrefix, orderedColumns, true, true,
          numOfWritesForThreads);
    } catch (Exception e) {
      e.printStackTrace();
    }


    Cursor cursor2 = null;
    String sel2 = "SELECT * FROM " + tableId;
    String[] selArgs2 = null;
    cursor2 = ODKDatabaseImplUtils.get().rawQuery(db, sel2, selArgs2);

    assertEquals(cursor2.getCount(), 1);

    System.out.println("testMultithreadedMultipleDBInsertionWithSameSelect_ExpectPass: before assert");
    OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().dumpInfo(false);

    while (cursor2.moveToNext()) {
      int indAgain = cursor2.getColumnIndex(setupTestCol);
      int typeAgain = cursor2.getType(indAgain);
      assertEquals(typeAgain, Cursor.FIELD_TYPE_INTEGER);
      int valAgain = cursor2.getInt(indAgain);
      assertEquals(valAgain, testVal);
      for (int i = 1; i <= numOfThreads; i++) {
        System.out.println("testMultithreadedMultipleDBInsertionWithSameSelect_ExpectPass: assertion "
            + "for thread " + i);
        String columnName = colPrefix + i;
        int ind = cursor2.getColumnIndex(columnName);
        int type = cursor2.getType(ind);
        assertEquals(type, Cursor.FIELD_TYPE_INTEGER);
        int val = cursor2.getInt(ind);
        assertEquals(val, numOfWritesForThreads);
      }
    }

    if (cursor2 != null && !cursor2.isClosed()) {
      cursor2.close();
    }

    System.out.println("testMultithreadedMultipleDBInsertionWithSameSelect_ExpectPass: after assert");
    OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().dumpInfo(false);

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

  /*
   * Test multi-threaded test for inserting data into the database
   */
  public void testMultipleConnectionsWithTableDeletionAndCreation_ExpectPass() {
    String tableId = testTable;
    String testCol = "testColumn";
    String testColType = ElementDataType.integer.name();
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column(testCol, testCol, testColType, "[]"));

    // Create two different db connections
    String uuid1 = ODKDataUtils.genUUID();
    OdkDbHandle uniqueKey1 = new OdkDbHandle(AbstractODKDatabaseUtilsTest.class
        .getSimpleName() + uuid1 + AndroidConnectFactory.INTERNAL_TYPE_SUFFIX);
    OdkConnectionInterface db1 = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
        .getConnection(getAppName(), uniqueKey1);

    String uuid2 = ODKDataUtils.genUUID();
    OdkDbHandle uniqueKey2 = new OdkDbHandle(AbstractODKDatabaseUtilsTest.class.getSimpleName() +
      uuid2 + AndroidConnectFactory.INTERNAL_TYPE_SUFFIX);
    OdkConnectionInterface db2 = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
        .getConnection(getAppName(), uniqueKey2);

    // Create a table on db1
    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
        .createOrOpenDBTableWithColumns(db1, getAppName(), tableId, columns);

    // Insert a row using db1
    int testVal = 5;
    ContentValues cvValues = new ContentValues();
    String rowId = ODKDataUtils.genUUID();
    cvValues.put(testCol, testVal);
    ODKDatabaseImplUtils.get().insertRowWithId(db1, tableId, orderedColumns, cvValues, rowId);

    // Have both query the table
    // Query with db1
    String sel = "SELECT * FROM " + tableId;
    String[] selArgs = null;
    Cursor cursor1 = ODKDatabaseImplUtils.get().rawQuery(db1, sel, selArgs);

    while (cursor1.moveToNext()) {
      int ind1 = cursor1.getColumnIndex(testCol);
      int type1 = cursor1.getType(ind1);
      assertEquals(type1, Cursor.FIELD_TYPE_INTEGER);
      int val1 = cursor1.getInt(ind1);
      assertEquals(val1, testVal);
    }

    // Query with db2
    Cursor cursor2 = ODKDatabaseImplUtils.get().rawQuery(db2, sel, selArgs);

    while (cursor2.moveToNext()) {
      int ind2 = cursor2.getColumnIndex(testCol);
      int type2 = cursor2.getType(ind2);
      assertEquals(type2, Cursor.FIELD_TYPE_INTEGER);
      int val2 = cursor2.getInt(ind2);
      assertEquals(val2, testVal);
    }

    // Delete the table and recreate with a different row
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db1, getAppName(), tableId);

    // Create a table on db1
    String newTestCol = "testColumn0";
    List<Column> newColumns = new ArrayList<Column>();
    newColumns.add(new Column(newTestCol, newTestCol, testColType, "[]"));
    orderedColumns = ODKDatabaseImplUtils.get()
        .createOrOpenDBTableWithColumns(db1, getAppName(), tableId, newColumns);

    // Re-create the same table with different row
    int newTestVal = 200;
    cvValues = new ContentValues();
    rowId = ODKDataUtils.genUUID();
    cvValues.put(newTestCol, newTestVal);
    ODKDatabaseImplUtils.get().insertRowWithId(db1, tableId, orderedColumns, cvValues, rowId);

    // Have both connections re-query the table
    // Query with db1
    cursor1= ODKDatabaseImplUtils.get().rawQuery(db1, sel, selArgs);

    while (cursor1.moveToNext()) {
      int ind3 = cursor1.getColumnIndex(newTestCol);
      int type3 = cursor1.getType(ind3);
      assertEquals(type3, Cursor.FIELD_TYPE_INTEGER);
      int val3 = cursor1.getInt(ind3);
      assertEquals(val3, newTestVal);
    }

    // Query with db2
    cursor2 = ODKDatabaseImplUtils.get().rawQuery(db2, sel, selArgs);

    while (cursor2.moveToNext()) {
      int ind4 = cursor2.getColumnIndex(testCol);
      int type4 = cursor2.getType(ind4);
      assertEquals(type4, Cursor.FIELD_TYPE_INTEGER);
      int val4 = cursor2.getInt(ind4);
      assertEquals(val4, newTestVal);
    }

    // Close the cursor
    if (cursor1 != null && !cursor1.isClosed()){
      cursor1.close();
    }

    if (cursor2 != null && !cursor2.isClosed()){
      cursor2.close();
    }

    System.out.println("testMultipleConnectionsWithTableDeletionAndCreation_ExpectPass: after assert");
    OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().dumpInfo(false);

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);
  }

 /*
  * Test for memory leaks in the SQL interface.
  *
  * The general plan is to do a large loop where we create a table, insert a row,
  * select data from the table, drop the table, and create 2 x ( set of small byte[]
  * allocations ), every iteration, free 1x of the byte[] allocations.
  */
  public void testMemoryLeakCycling_ExpectPass() {

    LinkedList<byte[]> byteQueue = new LinkedList<byte[]>();

    String tableId = "memoryTest";
    int maxBytes = 32;
    int maxIterations = 1000;
    String testColType = ElementDataType.string.name();

    for (int j = 0 ; j < maxIterations ; ++j ) {
      System.out.println("iteration " + j + " of " + maxIterations);

      int maxCols = 10 + (j % 7);
      // construct table
      List<Column> columns = new ArrayList<Column>();
      for (int i = 0; i < maxCols; ++i) {
        String testCol = "testColumn_" + Integer.toString(i);
        columns.add(new Column(testCol, testCol, testColType, "[]"));
      }
      OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
          .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);

      String rowId = ODKDataUtils.genUUID();

      ContentValues cvValues = new ContentValues();
      for (int i = 0; i < maxCols; ++i) {
        String testCol = "testColumn_" + Integer.toString(i);
        String testVal = "testVal_" + Integer.toString(i);
        cvValues.put(testCol, testVal);
      }

      ODKDatabaseImplUtils.get()
          .insertRowWithId(db, tableId, orderedColumns, cvValues, rowId);

      // Select everything out of the table
      String queryCol = "testColumn_" + Integer.toString(j % maxCols);
      String queryVal = "testVal_" + Integer.toString(j % maxCols);
      String sel = "SELECT * FROM " + tableId + " WHERE " + queryCol + " = ?";
      String[] selArgs = { queryVal };
      Cursor cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);

      String val = null;
      while (cursor.moveToNext()) {
        int ind = cursor.getColumnIndex(queryCol);
        int type = cursor.getType(ind);
        assertEquals(type, Cursor.FIELD_TYPE_STRING);
        val = cursor.getString(ind);

        ind = cursor.getColumnIndex(DataTableColumns.SAVEPOINT_TYPE);
        assertFalse(cursor.isNull(ind));

        // Get the conflict_type and make sure that it is null
        ind = cursor.getColumnIndex(DataTableColumns.CONFLICT_TYPE);
        assertTrue(cursor.isNull(ind));
      }

      assertEquals(val, queryVal);
      cursor.close();

      ODKDatabaseImplUtils.get().deleteRowWithId(db, getAppName(), tableId, rowId);

      // Select everything out of the table
      sel = "SELECT * FROM " + tableId;
      selArgs = new String[0];
      cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);

      assertEquals(cursor.getCount(), 0);

      // Drop the table now that the test is done
      ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);

      for ( int len = 1 ; len < maxBytes ; len += 4 ) {
        byte[] bytes = new byte[len];
        for ( int k = 0 ; k < len ; ++k ) {
          bytes[k] = (byte) k;
        }
        byteQueue.add(bytes);
      }
      for ( int len = 1 ; len < maxBytes ; len += 4 ) {
        byte[] bytes = new byte[len];
        for ( int k = 0 ; k < len ; ++k ) {
          bytes[k] = (byte) k;
        }
        byteQueue.add(bytes);
      }
      for ( int len = 1 ; len < maxBytes ; len += 4 ) {
        byte[] bytes = byteQueue.pop();
        for ( int k = 0 ; k < len ; ++k ) {
          assertEquals(bytes[k], (byte) k);
        }
      }
    }
  }
}
