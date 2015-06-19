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

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.opendatakit.aggregate.odktables.rest.ElementDataType;
import org.opendatakit.aggregate.odktables.rest.ElementType;
import org.opendatakit.aggregate.odktables.rest.SyncState;
import org.opendatakit.aggregate.odktables.rest.TableConstants;
import org.opendatakit.aggregate.odktables.rest.entity.Column;
import org.opendatakit.common.android.data.ColumnDefinition;
import org.opendatakit.common.android.data.OrderedColumns;
import org.opendatakit.common.android.database.DatabaseConstants;
import org.opendatakit.common.android.database.DatabaseFactory;
import org.opendatakit.common.android.database.OdkDatabase;
import org.opendatakit.common.android.provider.ColumnDefinitionsColumns;
import org.opendatakit.common.android.provider.DataTableColumns;
import org.opendatakit.common.android.provider.KeyValueStoreColumns;
import org.opendatakit.common.android.provider.TableDefinitionsColumns;
import org.opendatakit.common.android.utilities.ODKDataUtils;
import org.opendatakit.common.android.utilities.ODKDatabaseImplUtils;
import org.opendatakit.common.android.utilities.ODKFileUtils;
import org.opendatakit.database.service.OdkDbHandle;

import android.content.ContentValues;
import android.database.Cursor;
import android.test.AndroidTestCase;
import android.test.RenamingDelegatingContext;
import android.util.Log;

public class ODKDatabaseImplUtilsTest extends AndroidTestCase {

  private static final String TAG = "ODKDatabaseImplUtilsTest";

  private static final String TEST_FILE_PREFIX = "test_";

  private static final String DATABASE_NAME = "test.db";

  private static final String testTable = "testTable";

  private static final String elemKey = "_element_key";
  private static final String elemName = "_element_name";
  private static final String listChildElemKeys = "_list_child_element_keys";

  private OdkDbHandle uniqueKey;
  private OdkDatabase db;

  private static class DatabaseInitializer {

    public static void onCreate(OdkDatabase db) {
      String createColCmd = ColumnDefinitionsColumns
          .getTableCreateSql(DatabaseConstants.COLUMN_DEFINITIONS_TABLE_NAME);

      try {
        db.execSQL(createColCmd);
      } catch (Exception e) {
        Log.e("test", "Error while creating table "
            + DatabaseConstants.COLUMN_DEFINITIONS_TABLE_NAME);
        e.printStackTrace();
      }

      String createTableDefCmd = TableDefinitionsColumns
          .getTableCreateSql(DatabaseConstants.TABLE_DEFS_TABLE_NAME);

      try {
        db.execSQL(createTableDefCmd);
      } catch (Exception e) {
        Log.e("test", "Error while creating table " + DatabaseConstants.TABLE_DEFS_TABLE_NAME);
        e.printStackTrace();
      }

      String createKVSCmd = KeyValueStoreColumns
          .getTableCreateSql(DatabaseConstants.KEY_VALUE_STORE_ACTIVE_TABLE_NAME);

      try {
        db.execSQL(createKVSCmd);
      } catch (Exception e) {
        Log.e("test", "Error while creating table "
            + DatabaseConstants.KEY_VALUE_STORE_ACTIVE_TABLE_NAME);
        e.printStackTrace();
      }
    }
  }

  /*
   * Set up the database for the tests(non-Javadoc)
   * 
   * @see android.test.AndroidTestCase#setUp()
   */
  @Override
  protected synchronized void setUp() throws Exception {
    super.setUp();

    uniqueKey = DatabaseFactory.get().generateInternalUseDbHandle();

    RenamingDelegatingContext context = new RenamingDelegatingContext(getContext(),
        TEST_FILE_PREFIX);

    DatabaseFactory.get().releaseAllDatabases(context);
    FileUtils.deleteDirectory(new File(ODKFileUtils.getAppFolder(getAppName())));
    
    ODKFileUtils.verifyExternalStorageAvailability();

    ODKFileUtils.assertDirectoryStructure(getAppName());
    
    db = DatabaseFactory.get().getDatabase(context, getAppName(), uniqueKey);

    DatabaseInitializer.onCreate(db);
  }

  protected String getAppName() {
    return "test-" + uniqueKey.getDatabaseHandle().substring(6);
  }
  
  /*
   * Destroy all test data once tests are done(non-Javadoc)
   * 
   * @see android.test.AndroidTestCase#tearDown()
   */
  @Override
  protected void tearDown() throws Exception {
    super.tearDown();

    if (db != null) {
      db.close();
    }
      
    RenamingDelegatingContext context = new RenamingDelegatingContext(getContext(),
        TEST_FILE_PREFIX);
    DatabaseFactory.get().releaseDatabase(context, getAppName(), uniqueKey);
    DatabaseFactory.get().releaseAllDatabases(context);
    FileUtils.deleteDirectory(new File(ODKFileUtils.getAppFolder(getAppName())));
  }

  /*
   * Check that the database is setup
   */
  public void testPreConditions() {
    assertNotNull(db);
  }

  /*
   * Test query when there is no data
   */
  public void testQueryWithNoData_ExpectFail() {
    String tableId = testTable;
    boolean thrown = false;

    try {
      ODKDatabaseImplUtils.get().query(db, tableId, null, null, null, null, null, null, null);
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
    db.execSQL("DROP TABLE " + tableId);
  }

  /*
   * Test raw query when there is data
   */
  public void testRawQueryWithNoData_ExpectFail() {
    String tableId = testTable;
    String query = "SELECT * FROM " + tableId;
    boolean thrown = false;

    try {
      ODKDatabaseImplUtils.get().rawQuery(db, query, null);
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
    db.execSQL("DROP TABLE " + tableId);
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
    db.execSQL("DROP TABLE " + tableId);
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
    db.execSQL("DROP TABLE " + tableId);
  }

  /*
   * Test creation of user defined database table with column when column is
   * null
   */
  public void testCreateOrOpenDbTableWithColumnWhenColumnIsNull_ExpectPass() {
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
    db.execSQL("DROP TABLE IF EXISTS " + tableId);
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
    db.execSQL("DROP TABLE IF EXISTS " + tableId);
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
    db.execSQL("DROP TABLE IF EXISTS  " + tableId);
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
    db.execSQL("DROP TABLE IF EXISTS " + tableId);
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
    db.execSQL("DROP TABLE " + tableId);
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
    db.execSQL("DROP TABLE " + tableId);
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
    db.execSQL("DROP TABLE " + tableId);
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
    db.execSQL("DROP TABLE " + tableId);
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
    db.execSQL("DROP TABLE " + tableId);
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
    db.execSQL("DROP TABLE " + tableId);
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
    db.execSQL("DROP TABLE IF EXISTS  " + tableId);
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
    db.execSQL("DROP TABLE IF EXISTS " + tableId);
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
    db.execSQL("DROP TABLE IF EXISTS " + tableId);
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
    db.execSQL("DROP TABLE IF EXISTS " + tableId);
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
    db.execSQL("DROP TABLE IF EXISTS " + tableId);
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
    db.execSQL("DROP TABLE IF EXISTS " + tableId);
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
    db.execSQL("DROP TABLE " + tableId);
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
    db.execSQL("DROP TABLE " + tableId);
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
    db.execSQL("DROP TABLE " + tableId);
  }

  /*
   * Test getting all column names when table does not exist
   */
  public void testGetAllColumnNamesWhenTableDoesNotExist_ExpectFail() {
    String tableId = testTable;
    boolean thrown = false;

    try {
      ODKDatabaseImplUtils.get().getAllColumnNames(db, tableId);
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
    db.execSQL("DROP TABLE " + tableId);
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
      ODKDatabaseImplUtils.get().insertDataIntoExistingDBTableWithId(db, tableId, orderedColumns, null,
          null);
    } catch (Exception e) {
      thrown = true;
      e.printStackTrace();
    }

    assertTrue(thrown);

    // Drop the table now that the test is done
    db.execSQL("DROP TABLE " + tableId);
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
    ODKDatabaseImplUtils.get().insertDataIntoExistingDBTableWithId(db, tableId, orderedColumns,
        cvValues, ODKDataUtils.genUUID());

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
    db.execSQL("DROP TABLE " + tableId);
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
    ODKDatabaseImplUtils.get().insertDataIntoExistingDBTableWithId(db, tableId, orderedColumns,
        cvValues, uuid);

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
    db.execSQL("DROP TABLE " + tableId);
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
    ODKDatabaseImplUtils.get().insertDataIntoExistingDBTableWithId(db, tableId, orderedColumns,
        cvValues, uuid);

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

    try {
      ODKDatabaseImplUtils.get().insertDataIntoExistingDBTableWithId(db, tableId, orderedColumns,
          cvValues2, uuid);
    } catch (IllegalArgumentException e) {
      thrown = true;
      e.printStackTrace();
    }

    assertEquals(thrown, true);

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
    db.execSQL("DROP TABLE " + tableId);
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
    ODKDatabaseImplUtils.get().updateDataInExistingDBTableWithId(db, tableId, orderedColumns, cvValues,
        uuid);

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
    db.execSQL("DROP TABLE " + tableId);
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
    ODKDatabaseImplUtils.get().updateDataInExistingDBTableWithId(db, tableId, orderedColumns, cvValues,
        uuid);

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

    ODKDatabaseImplUtils.get().updateDataInExistingDBTableWithId(db, tableId, orderedColumns,
        cvValues2, uuid);

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
    db.execSQL("DROP TABLE " + tableId);
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
      ODKDatabaseImplUtils.get().insertDataIntoExistingDBTableWithId(db, tableId, orderedColumns,
          cvValues, uuid);
    } catch (Exception e) {
      thrown = true;
      e.printStackTrace();
    }

    assertTrue(thrown);

    // Drop the table now that the test is done
    db.execSQL("DROP TABLE " + tableId);
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

    ODKDatabaseImplUtils.get().insertDataIntoExistingDBTableWithId(db, tableId, orderedColumns,
        cvValues, uuid);

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
    db.execSQL("DROP TABLE " + tableId);
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
      ODKDatabaseImplUtils.get().insertDataIntoExistingDBTableWithId(db, tableId, orderedColumns,
          cvValues, nullString);
    } catch (Exception e) {
      thrown = true;
      e.printStackTrace();
    }

    assertTrue(thrown);

    // Drop the table now that the test is done
    db.execSQL("DROP TABLE " + tableId);
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
      ODKDatabaseImplUtils.get().insertDataIntoExistingDBTableWithId(db, tableId, orderedColumns,
          cvValues, uuid);
    } catch (Exception e) {
      thrown = true;
      e.printStackTrace();
    }

    assertFalse(thrown);

    // Drop the table now that the test is done
    db.execSQL("DROP TABLE " + tableId);
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
      ODKDatabaseImplUtils.get().insertDataIntoExistingDBTableWithId(db, tableId, orderedColumns,
          cvValues, uuid);
    } catch (Exception e) {
      thrown = true;
      e.printStackTrace();
    }

    // TODO: should this fail or succeed?
    // assertTrue(thrown);
    assertFalse(thrown);

    // Drop the table now that the test is done
    db.execSQL("DROP TABLE " + tableId);
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
    ODKDatabaseImplUtils.get().insertDataIntoExistingDBTableWithId(db, tableId, orderedColumns,
        cvValues, ODKDataUtils.genUUID());

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
    db.execSQL("DROP TABLE " + tableId);
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
    ODKDatabaseImplUtils.get().insertDataIntoExistingDBTableWithId(db, tableId, orderedColumns,
        cvValues, ODKDataUtils.genUUID());

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
    db.execSQL("DROP TABLE " + tableId);
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
    ODKDatabaseImplUtils.get().insertDataIntoExistingDBTableWithId(db, tableId, orderedColumns,
        cvValues, ODKDataUtils.genUUID());

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
    db.execSQL("DROP TABLE " + tableId);
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
    ODKDatabaseImplUtils.get().insertDataIntoExistingDBTableWithId(db, tableId, orderedColumns,
        cvValues, ODKDataUtils.genUUID());

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
    db.execSQL("DROP TABLE " + tableId);
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

    ODKDatabaseImplUtils.get().insertDataIntoExistingDBTableWithId(db, tableId, orderedColumns,
        cvValues, ODKDataUtils.genUUID());

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
    db.execSQL("DROP TABLE " + tableId);
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
    ODKDatabaseImplUtils.get().insertDataIntoExistingDBTableWithId(db, tableId, orderedColumns,
        cvValues, ODKDataUtils.genUUID());

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
    db.execSQL("DROP TABLE " + tableId);
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
    ODKDatabaseImplUtils.get().insertDataIntoExistingDBTableWithId(db, tableId, orderedColumns,
        cvValues, uuid);

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
    db.execSQL("DROP TABLE " + tableId);
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
    ODKDatabaseImplUtils.get().insertDataIntoExistingDBTableWithId(db, tableId, orderedColumns,
        cvValues, ODKDataUtils.genUUID());

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
    db.execSQL("DROP TABLE " + tableId);
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
    ODKDatabaseImplUtils.get().insertDataIntoExistingDBTableWithId(db, tableId, orderedColumns,
        cvValues, ODKDataUtils.genUUID());

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
    db.execSQL("DROP TABLE " + tableId);
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
      Log.i(TAG, "Time string is " + interMed);
    }

    Log.i(TAG, "Time string is " + testVal);

    ContentValues cvValues = new ContentValues();
    cvValues.put(testCol, testVal);
    ODKDatabaseImplUtils.get().insertDataIntoExistingDBTableWithId(db, tableId, orderedColumns,
        cvValues, ODKDataUtils.genUUID());

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
    db.execSQL("DROP TABLE " + tableId);
  }

  public void testFailureForCheckins() {
    assertTrue(true);
  }

}
