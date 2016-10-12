/*
 * Copyright (C) 2016 University of Washington
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

package org.opendatakit.utilities.test;

import android.content.ContentValues;
import org.opendatakit.aggregate.odktables.rest.entity.RowFilterScope;
import org.opendatakit.database.data.OrderedColumns;
import org.opendatakit.exception.ActionNotAuthorizedException;
import org.opendatakit.services.database.utlities.ODKDatabaseImplUtils;

import java.util.ArrayList;

/**
 * Permissions tests in the database.
 */
public class ODKDatabaseUtilsInsertUpdatePermissionsTest extends AbstractPermissionsTestCase {

  private static final String TAG = "ODKDatabaseUtilsInsertUpdatePermissionsTest";

  public void testUpdateUnlockedNoAnonCreate() throws ActionNotAuthorizedException {

    String tableId = testTableUnlockedNoAnonCreate;
    OrderedColumns oc = assertPopulatedTestTable(testTableUnlockedNoAnonCreate,
        false, false, RowFilterScope.Type.DEFAULT.name());

    ContentValues cvValues = new ContentValues();
    cvValues.put("col0", 1); // myothertype:integer

    ArrayList<AuthParamAndOutcome> cases = buildOutcomesListUpdateUnlockedNoAnonCreate();

    for ( AuthParamAndOutcome ap : cases ) {
      try {
        ODKDatabaseImplUtils.get()
            .updateRowWithId(db,ap.tableId, oc, cvValues, ap.rowId, ap.username, ap.roles,
                currentLocale);
        assertFalse("Expecting to throw an error: " + ap.toString(), ap.throwsAccessException);
      } catch ( ActionNotAuthorizedException ex ) {
        assertTrue("Expecting to not throw an error: " + ap.toString(), ap.throwsAccessException);
      }
    }
  }


  public void testInsertCheckpointAsUpdateUnlockedNoAnonCreate()
      throws  ActionNotAuthorizedException {

    String tableId = testTableUnlockedNoAnonCreate;
    OrderedColumns oc = assertPopulatedTestTable(testTableUnlockedNoAnonCreate,
        false, false, RowFilterScope.Type.DEFAULT.name());

    ContentValues cvValues = new ContentValues();
    cvValues.put("col0", 1); // myothertype:integer

    ArrayList<AuthParamAndOutcome> cases = buildOutcomesListUpdateUnlockedNoAnonCreate();

    for ( AuthParamAndOutcome ap : cases ) {
      try {
        ODKDatabaseImplUtils.get()
            .insertCheckpointRowWithId(db,ap.tableId, oc, cvValues, ap.rowId, ap.username, ap.roles,
                currentLocale);
        assertFalse("Expecting to throw an error: " + ap.toString(), ap.throwsAccessException);
      } catch ( ActionNotAuthorizedException ex ) {
        assertTrue("Expecting to not throw an error: " + ap.toString(), ap.throwsAccessException);
      }
    }
  }

  public void testInsertUnlockedNoAnonCreate() throws ActionNotAuthorizedException {

    String tableId = testTableUnlockedNoAnonCreate;
    OrderedColumns oc = assertEmptyTestTable(testTableUnlockedNoAnonCreate,
        false, false, RowFilterScope.Type.DEFAULT.name());

    ContentValues cvValues = buildUnprivilegedInsertableRowContent(tableId);

    ArrayList<AuthParamAndOutcome> cases = buildOutcomesListInsertUnlockedNoAnonCreate();

    for ( AuthParamAndOutcome ap : cases ) {
      try {
        ODKDatabaseImplUtils.get()
            .insertRowWithId(db,ap.tableId, oc, cvValues, ap.rowId, ap.username, ap.roles,
                currentLocale);
        assertFalse("Expecting to throw an error: " + ap.toString(), ap.throwsAccessException);
      } catch ( ActionNotAuthorizedException ex ) {
        assertTrue("Expecting to not throw an error: " + ap.toString(), ap.throwsAccessException);
      }
    }
  }

  public void testInsertCheckpointAsInsertUnlockedNoAnonCreate()
      throws ActionNotAuthorizedException {

    String tableId = testTableUnlockedNoAnonCreate;
    OrderedColumns oc = assertEmptyTestTable(testTableUnlockedNoAnonCreate,
        false, false, RowFilterScope.Type.DEFAULT.name());

    ContentValues cvValues = buildUnprivilegedInsertableRowContent(tableId);

    ArrayList<AuthParamAndOutcome> cases = buildOutcomesListInsertUnlockedNoAnonCreate();

    for ( AuthParamAndOutcome ap : cases ) {
      try {
        ODKDatabaseImplUtils.get()
            .insertCheckpointRowWithId(db,ap.tableId, oc, cvValues, ap.rowId, ap.username, ap.roles,
                currentLocale);
        assertFalse("Expecting to throw an error: " + ap.toString(), ap.throwsAccessException);
      } catch ( ActionNotAuthorizedException ex ) {
        assertTrue("Expecting to not throw an error: " + ap.toString(), ap.throwsAccessException);
      }
    }
  }

  public void testUpdateUnlockedYesAnonCreate() throws ActionNotAuthorizedException {

    String tableId = testTableUnlockedYesAnonCreate;
    OrderedColumns oc = assertPopulatedTestTable(testTableUnlockedYesAnonCreate,
        false, true, RowFilterScope.Type.DEFAULT.name());

    ContentValues cvValues = new ContentValues();
    cvValues.put("col0", 1); // myothertype:integer

    ArrayList<AuthParamAndOutcome> cases = buildOutcomesListUpdateUnlockedYesAnonCreate();

    for ( AuthParamAndOutcome ap : cases ) {
      try {
        ODKDatabaseImplUtils.get()
            .updateRowWithId(db,ap.tableId, oc, cvValues, ap.rowId, ap.username, ap.roles,
                currentLocale);
        assertFalse("Expecting to throw an error: " + ap.toString(), ap.throwsAccessException);
      } catch ( ActionNotAuthorizedException ex ) {
        assertTrue("Expecting to not throw an error: " + ap.toString(), ap.throwsAccessException);
      }
    }
  }

  public void testInsertCheckpointAsUpdateUnlockedYesAnonCreate()
      throws  ActionNotAuthorizedException {

    String tableId = testTableUnlockedYesAnonCreate;
    OrderedColumns oc = assertPopulatedTestTable(testTableUnlockedYesAnonCreate,
        false, true, RowFilterScope.Type.DEFAULT.name());

    ContentValues cvValues = new ContentValues();
    cvValues.put("col0", 1); // myothertype:integer

    ArrayList<AuthParamAndOutcome> cases = buildOutcomesListUpdateUnlockedYesAnonCreate();

    for ( AuthParamAndOutcome ap : cases ) {
      try {
        ODKDatabaseImplUtils.get()
            .insertCheckpointRowWithId(db,ap.tableId, oc, cvValues, ap.rowId, ap.username, ap
                .roles,
                currentLocale);
        assertFalse("Expecting to throw an error: " + ap.toString(), ap.throwsAccessException);
      } catch ( ActionNotAuthorizedException ex ) {
        assertTrue("Expecting to not throw an error: " + ap.toString(), ap.throwsAccessException);
      }
    }
  }

  public void testInsertUnlockedYesAnonCreate() throws ActionNotAuthorizedException {

    String tableId = testTableUnlockedYesAnonCreate;
    OrderedColumns oc = assertEmptyTestTable(testTableUnlockedYesAnonCreate,
        false, true, RowFilterScope.Type.DEFAULT.name());

    ContentValues cvValues = buildUnprivilegedInsertableRowContent(tableId);

    ArrayList<AuthParamAndOutcome> cases = buildOutcomesListInsertUnlockedYesAnonCreate();

    for ( AuthParamAndOutcome ap : cases ) {
      try {
        ODKDatabaseImplUtils.get()
            .insertRowWithId(db,ap.tableId, oc, cvValues, ap.rowId, ap.username, ap.roles,
                currentLocale);
        assertFalse("Expecting to throw an error: " + ap.toString(), ap.throwsAccessException);
      } catch ( ActionNotAuthorizedException ex ) {
        assertTrue("Expecting to not throw an error: " + ap.toString(), ap.throwsAccessException);
      }
    }
  }

  public void testInsertCheckpointAsInsertUnlockedYesAnonCreate()
      throws ActionNotAuthorizedException {

    String tableId = testTableUnlockedYesAnonCreate;
    OrderedColumns oc = assertEmptyTestTable(testTableUnlockedYesAnonCreate,
        false, true, RowFilterScope.Type.DEFAULT.name());

    ContentValues cvValues = buildUnprivilegedInsertableRowContent(tableId);

    ArrayList<AuthParamAndOutcome> cases = buildOutcomesListInsertUnlockedYesAnonCreate();

    for ( AuthParamAndOutcome ap : cases ) {
      try {
        ODKDatabaseImplUtils.get()
            .insertCheckpointRowWithId(db,ap.tableId, oc, cvValues, ap.rowId, ap.username, ap.roles,
                currentLocale);
        assertFalse("Expecting to throw an error: " + ap.toString(), ap.throwsAccessException);
      } catch ( ActionNotAuthorizedException ex ) {
        assertTrue("Expecting to not throw an error: " + ap.toString(), ap.throwsAccessException);
      }
    }
  }

  public void testUpdateLockedNoAnonCreate() throws ActionNotAuthorizedException {

    String tableId = testTableLockedNoAnonCreate;
    OrderedColumns oc = assertPopulatedTestTable(testTableLockedNoAnonCreate,
        true, false, RowFilterScope.Type.DEFAULT.name());

    ContentValues cvValues = new ContentValues();
    cvValues.put("col0", 1); // myothertype:integer

    ArrayList<AuthParamAndOutcome> cases = buildOutcomesListUpdateLockedNoAnonCreate();

    for ( AuthParamAndOutcome ap : cases ) {
      try {
        ODKDatabaseImplUtils.get()
            .updateRowWithId(db,ap.tableId, oc, cvValues, ap.rowId, ap.username, ap.roles,
                currentLocale);
        assertFalse("Expecting to throw an error: " + ap.toString(), ap.throwsAccessException);
      } catch ( ActionNotAuthorizedException ex ) {
        assertTrue("Expecting to not throw an error: " + ap.toString(), ap.throwsAccessException);
      }
    }
  }


  public void testInsertCheckpointAsUpdateLockedNoAnonCreate() throws ActionNotAuthorizedException {

    String tableId = testTableLockedNoAnonCreate;
    OrderedColumns oc = assertPopulatedTestTable(testTableLockedNoAnonCreate,
        true, false, RowFilterScope.Type.DEFAULT.name());

    ContentValues cvValues = new ContentValues();
    cvValues.put("col0", 1); // myothertype:integer

    ArrayList<AuthParamAndOutcome> cases = buildOutcomesListUpdateLockedNoAnonCreate();

    for ( AuthParamAndOutcome ap : cases ) {
      try {
        ODKDatabaseImplUtils.get()
            .insertCheckpointRowWithId(db,ap.tableId, oc, cvValues, ap.rowId, ap.username, ap.roles,
                currentLocale);
        assertFalse("Expecting to throw an error: " + ap.toString(), ap.throwsAccessException);
      } catch ( ActionNotAuthorizedException ex ) {
        assertTrue("Expecting to not throw an error: " + ap.toString(), ap.throwsAccessException);
      }
    }
  }

  public void testInsertLockedNoAnonCreate() throws ActionNotAuthorizedException {

    String tableId = testTableLockedNoAnonCreate;
    OrderedColumns oc = assertEmptyTestTable(testTableLockedNoAnonCreate,
        true, false, RowFilterScope.Type.DEFAULT.name());

    ContentValues cvValues = buildUnprivilegedInsertableRowContent(tableId);

    ArrayList<AuthParamAndOutcome> cases = buildOutcomesListInsertLockedNoAnonCreate();

    for ( AuthParamAndOutcome ap : cases ) {
      try {
        ODKDatabaseImplUtils.get()
            .insertRowWithId(db,ap.tableId, oc, cvValues, ap.rowId, ap.username, ap.roles,
                currentLocale);
        assertFalse("Expecting to throw an error: " + ap.toString(), ap.throwsAccessException);
      } catch ( ActionNotAuthorizedException ex ) {
        assertTrue("Expecting to not throw an error: " + ap.toString(), ap.throwsAccessException);
      }
    }
  }

  public void testInsertCheckpointAsInsertLockedNoAnonCreate() throws ActionNotAuthorizedException {

    String tableId = testTableLockedNoAnonCreate;
    OrderedColumns oc = assertEmptyTestTable(testTableLockedNoAnonCreate,
        true, false, RowFilterScope.Type.DEFAULT.name());

    ContentValues cvValues = buildUnprivilegedInsertableRowContent(tableId);

    ArrayList<AuthParamAndOutcome> cases = buildOutcomesListInsertLockedNoAnonCreate();

    for ( AuthParamAndOutcome ap : cases ) {
      try {
        ODKDatabaseImplUtils.get()
            .insertCheckpointRowWithId(db,ap.tableId, oc, cvValues, ap.rowId, ap.username, ap.roles,
                currentLocale);
        assertFalse("Expecting to throw an error: " + ap.toString(), ap.throwsAccessException);
      } catch ( ActionNotAuthorizedException ex ) {
        assertTrue("Expecting to not throw an error: " + ap.toString(), ap.throwsAccessException);
      }
    }
  }

  public void testUpdateLockedYesAnonCreate() throws ActionNotAuthorizedException {

    String tableId = testTableLockedYesAnonCreate;
    OrderedColumns oc = assertPopulatedTestTable(testTableLockedYesAnonCreate,
        true, true, RowFilterScope.Type.DEFAULT.name());

    ContentValues cvValues = new ContentValues();
    cvValues.put("col0", 1); // myothertype:integer

    ArrayList<AuthParamAndOutcome> cases = buildOutcomesListUpdateLockedYesAnonCreate();

    for ( AuthParamAndOutcome ap : cases ) {
      try {
        ODKDatabaseImplUtils.get()
            .updateRowWithId(db,ap.tableId, oc, cvValues, ap.rowId, ap.username, ap.roles,
                currentLocale);
        assertFalse("Expecting to throw an error: " + ap.toString(), ap.throwsAccessException);
      } catch ( ActionNotAuthorizedException ex ) {
        assertTrue("Expecting to not throw an error: " + ap.toString(), ap.throwsAccessException);
      }
    }
  }

  public void testInsertCheckpointAsUpdateLockedYesAnonCreate() throws
      ActionNotAuthorizedException {

    String tableId = testTableLockedYesAnonCreate;
    OrderedColumns oc = assertPopulatedTestTable(testTableLockedYesAnonCreate,
        true, true, RowFilterScope.Type.DEFAULT.name());

    ContentValues cvValues = new ContentValues();
    cvValues.put("col0", 1); // myothertype:integer

    ArrayList<AuthParamAndOutcome> cases = buildOutcomesListUpdateLockedYesAnonCreate();

    for ( AuthParamAndOutcome ap : cases ) {
      try {
        ODKDatabaseImplUtils.get()
            .insertCheckpointRowWithId(db,ap.tableId, oc, cvValues, ap.rowId, ap.username, ap.roles,
                currentLocale);
        assertFalse("Expecting to throw an error: " + ap.toString(), ap.throwsAccessException);
      } catch ( ActionNotAuthorizedException ex ) {
        assertTrue("Expecting to not throw an error: " + ap.toString(), ap.throwsAccessException);
      }
    }
  }

  public void testInsertLockedYesAnonCreate() throws ActionNotAuthorizedException {

    String tableId = testTableLockedYesAnonCreate;
    OrderedColumns oc = assertEmptyTestTable(testTableLockedYesAnonCreate,
        true, true, RowFilterScope.Type.DEFAULT.name());

    ContentValues cvValues = buildUnprivilegedInsertableRowContent(tableId);

    ArrayList<AuthParamAndOutcome> cases = buildOutcomesListInsertLockedYesAnonCreate();

    for ( AuthParamAndOutcome ap : cases ) {
      try {
        ODKDatabaseImplUtils.get()
            .insertRowWithId(db,ap.tableId, oc, cvValues, ap.rowId, ap.username, ap.roles,
                currentLocale);
        assertFalse("Expecting to throw an error: " + ap.toString(), ap.throwsAccessException);
      } catch ( ActionNotAuthorizedException ex ) {
        assertTrue("Expecting to not throw an error: " + ap.toString(), ap.throwsAccessException);
      }
    }
  }

  public void testInsertCheckpointAsInsertLockedYesAnonCreate()
      throws ActionNotAuthorizedException {

    String tableId = testTableLockedYesAnonCreate;
    OrderedColumns oc = assertEmptyTestTable(testTableLockedYesAnonCreate,
        true, true, RowFilterScope.Type.DEFAULT.name());

    ContentValues cvValues = buildUnprivilegedInsertableRowContent(tableId);

    ArrayList<AuthParamAndOutcome> cases = buildOutcomesListInsertLockedYesAnonCreate();

    for ( AuthParamAndOutcome ap : cases ) {
      try {
        ODKDatabaseImplUtils.get()
            .insertCheckpointRowWithId(db,ap.tableId, oc, cvValues, ap.rowId, ap.username, ap.roles,
                currentLocale);
        assertFalse("Expecting to throw an error: " + ap.toString(), ap.throwsAccessException);
      } catch ( ActionNotAuthorizedException ex ) {
        assertTrue("Expecting to not throw an error: " + ap.toString(), ap.throwsAccessException);
      }
    }
  }

//  /*
//   * Test query when there is data
//   */
//  public void testQueryWithData_ExpectPass() {
//    String tableId = testTable;
//    List<Column> columns = new ArrayList<Column>();
//    columns.add(new Column("col1", "col1", "string", "[]"));
//    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
//        .createOrOpenTableWithColumns(db, getAppName(), tableId, columns);
//
//    // Check that the user defined rows are in the table
//    Cursor cursor = ODKDatabaseImplUtils.get().query(db, tableId, null, null, null, null, null,
//        null, null);
//    Cursor refCursor = db.query(tableId, null, null, null, null, null, null, null);
//
//    if (cursor != null && refCursor != null) {
//      int index = 0;
//      while (cursor.moveToNext() && refCursor.moveToNext()) {
//        int testType = cursor.getType(index);
//        int refType = refCursor.getType(index);
//        assertEquals(testType, refType);
//
//        switch (refType) {
//        case Cursor.FIELD_TYPE_BLOB:
//          byte[] byteArray = cursor.getBlob(index);
//          byte[] refByteArray = refCursor.getBlob(index);
//          assertEquals(byteArray, refByteArray);
//          break;
//        case Cursor.FIELD_TYPE_FLOAT:
//          float valueFloat = cursor.getFloat(index);
//          float refValueFloat = refCursor.getFloat(index);
//          assertEquals(valueFloat, refValueFloat);
//          break;
//        case Cursor.FIELD_TYPE_INTEGER:
//          int valueInt = cursor.getInt(index);
//          int refValueInt = refCursor.getInt(index);
//          assertEquals(valueInt, refValueInt);
//          break;
//        case Cursor.FIELD_TYPE_STRING:
//          String valueStr = cursor.getString(index);
//          String refValueStr = refCursor.getString(index);
//          assertEquals(valueStr, refValueStr);
//          break;
//        case Cursor.FIELD_TYPE_NULL:
//        default:
//          break;
//        }
//      }
//    }
//
//    // Drop the table now that the test is done
//    ODKDatabaseImplUtils.get().deleteTableAndAllData(db, getAppName(), tableId);
//  }
//
//  /*
//   * Test raw query when there is no data
//   */
//  public void testRawQueryWithData_ExpectPass() {
//    String tableId = testTable;
//    String query = "SELECT * FROM " + tableId;
//    List<Column> columns = new ArrayList<Column>();
//    columns.add(new Column("col1", "col1", "string", "[]"));
//    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
//        .createOrOpenTableWithColumns(db, getAppName(), tableId, columns);
//
//    // Check that the user defined rows are in the table
//    Cursor cursor = ODKDatabaseImplUtils.get().rawQuery(db, query, null);
//    Cursor refCursor = db.rawQuery(query, null);
//
//    if (cursor != null && refCursor != null) {
//      int index = 0;
//      while (cursor.moveToNext() && refCursor.moveToNext()) {
//        int testType = cursor.getType(index);
//        int refType = refCursor.getType(index);
//        assertEquals(testType, refType);
//
//        switch (refType) {
//        case Cursor.FIELD_TYPE_BLOB:
//          byte[] byteArray = cursor.getBlob(index);
//          byte[] refByteArray = refCursor.getBlob(index);
//          assertEquals(byteArray, refByteArray);
//          break;
//        case Cursor.FIELD_TYPE_FLOAT:
//          float valueFloat = cursor.getFloat(index);
//          float refValueFloat = refCursor.getFloat(index);
//          assertEquals(valueFloat, refValueFloat);
//          break;
//        case Cursor.FIELD_TYPE_INTEGER:
//          int valueInt = cursor.getInt(index);
//          int refValueInt = refCursor.getInt(index);
//          assertEquals(valueInt, refValueInt);
//          break;
//        case Cursor.FIELD_TYPE_STRING:
//          String valueStr = cursor.getString(index);
//          String refValueStr = refCursor.getString(index);
//          assertEquals(valueStr, refValueStr);
//          break;
//        case Cursor.FIELD_TYPE_NULL:
//        default:
//          break;
//        }
//      }
//    }
//
//    // Drop the table now that the test is done
//    ODKDatabaseImplUtils.get().deleteTableAndAllData(db, getAppName(), tableId);
//  }
//
//
//
//
//  private void columnPropertiesHelper(String tableId, List<Column> columns,
//      List<KeyValueStoreEntry> kvsEntries) {
//    // reading data
//    File file = null;
//    FileInputStream in = null;
//    InputStreamReader input = null;
//    RFC4180CsvReader cr = null;
//    try {
//      file = new File(ODKFileUtils.getTableDefinitionCsvFile(getAppName(), tableId));
//      in = new FileInputStream(file);
//      input = new InputStreamReader(in, CharEncoding.UTF_8);
//      cr = new RFC4180CsvReader(input);
//
//      String[] row;
//
//      // Read ColumnDefinitions
//      // get the column headers
//      String[] colHeaders = cr.readNext();
//      int colHeadersLength = countUpToLastNonNullElement(colHeaders);
//      // get the first row
//      row = cr.readNext();
//      while (row != null && countUpToLastNonNullElement(row) != 0) {
//
//        String elementKeyStr = null;
//        String elementNameStr = null;
//        String elementTypeStr = null;
//        String listChildElementKeysStr = null;
//        int rowLength = countUpToLastNonNullElement(row);
//        for (int i = 0; i < rowLength; ++i) {
//          if (i >= colHeadersLength) {
//            throw new IllegalStateException("data beyond header row of ColumnDefinitions table");
//          }
//          if (ColumnDefinitionsColumns.ELEMENT_KEY.equals(colHeaders[i])) {
//            elementKeyStr = row[i];
//          }
//          if (ColumnDefinitionsColumns.ELEMENT_NAME.equals(colHeaders[i])) {
//            elementNameStr = row[i];
//          }
//          if (ColumnDefinitionsColumns.ELEMENT_TYPE.equals(colHeaders[i])) {
//            elementTypeStr = row[i];
//          }
//          if (ColumnDefinitionsColumns.LIST_CHILD_ELEMENT_KEYS.equals(colHeaders[i])) {
//            listChildElementKeysStr = row[i];
//          }
//        }
//
//        if (elementKeyStr == null || elementTypeStr == null) {
//          throw new IllegalStateException("ElementKey and ElementType must be specified");
//        }
//
//        columns.add(new Column(elementKeyStr, elementNameStr, elementTypeStr,
//            listChildElementKeysStr));
//
//        // get next row or blank to end...
//        row = cr.readNext();
//      }
//
//      cr.close();
//      try {
//        input.close();
//      } catch (IOException e) {
//      }
//      try {
//        in.close();
//      } catch (IOException e) {
//      }
//
//      file = new File(ODKFileUtils.getTablePropertiesCsvFile(getAppName(), tableId));
//      in = new FileInputStream(file);
//      input = new InputStreamReader(in, CharEncoding.UTF_8);
//      cr = new RFC4180CsvReader(input);
//      // Read KeyValueStore
//      // read the column headers
//      String[] kvsHeaders = cr.readNext();
//      // read the first row
//      row = cr.readNext();
//      while (row != null && countUpToLastNonNullElement(row) != 0) {
//        String partition = null;
//        String aspect = null;
//        String key = null;
//        String type = null;
//        String value = null;
//        int rowLength = countUpToLastNonNullElement(row);
//        for (int i = 0; i < rowLength; ++i) {
//          if (KeyValueStoreColumns.PARTITION.equals(kvsHeaders[i])) {
//            partition = row[i];
//          }
//          if (KeyValueStoreColumns.ASPECT.equals(kvsHeaders[i])) {
//            aspect = row[i];
//          }
//          if (KeyValueStoreColumns.KEY.equals(kvsHeaders[i])) {
//            key = row[i];
//          }
//          if (KeyValueStoreColumns.VALUE_TYPE.equals(kvsHeaders[i])) {
//            type = row[i];
//          }
//          if (KeyValueStoreColumns.VALUE.equals(kvsHeaders[i])) {
//            value = row[i];
//          }
//        }
//        KeyValueStoreEntry kvsEntry = KeyValueStoreUtils.buildEntry(tableId, partition, aspect, key,
//            ElementDataType.valueOf(type), value);
//        kvsEntries.add(kvsEntry);
//        // get next row or blank to end...
//        row = cr.readNext();
//      }
//      cr.close();
//      try {
//        input.close();
//      } catch (IOException e) {
//      }
//      try {
//        in.close();
//      } catch (IOException e) {
//      }
//
//    } catch (Exception e) {
//      e.printStackTrace();
//    } finally {
//      try {
//        if (input != null) {
//          input.close();
//        }
//      } catch (IOException e) {
//      }
//    }
//
//    // Go through the KVS list and replace all the choiceList entries with their choiceListId
//    for ( KeyValueStoreEntry entry : kvsEntries ) {
//      if ( entry.partition.equals(KeyValueStoreConstants.PARTITION_COLUMN) &&
//          entry.key.equals(KeyValueStoreConstants.COLUMN_DISPLAY_CHOICES_LIST) ) {
//        // stored type is a string -- the choiceListId
//        entry.type = ElementDataType.string.name();
//        if ((entry.value != null) && (entry.value.trim().length() != 0)) {
//          String choiceListId = ODKDatabaseImplUtils.get().setChoiceList(db, getAppName(), entry
//              .value);
//          entry.value = choiceListId;
//        } else {
//          entry.value = null;
//        }
//      }
//    }
//
//  }
//
}
