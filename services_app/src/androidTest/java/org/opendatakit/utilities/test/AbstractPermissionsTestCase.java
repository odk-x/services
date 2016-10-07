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
import android.database.Cursor;
import android.test.AndroidTestCase;
import android.util.Log;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.opendatakit.database.RoleConsts;
import org.opendatakit.TestConsts;
import org.opendatakit.aggregate.odktables.rest.ConflictType;
import org.opendatakit.aggregate.odktables.rest.ElementDataType;
import org.opendatakit.aggregate.odktables.rest.KeyValueStoreConstants;
import org.opendatakit.aggregate.odktables.rest.SavepointTypeManipulator;
import org.opendatakit.aggregate.odktables.rest.SyncState;
import org.opendatakit.aggregate.odktables.rest.TableConstants;
import org.opendatakit.aggregate.odktables.rest.entity.Column;
import org.opendatakit.aggregate.odktables.rest.entity.RowFilterScope;
import org.opendatakit.database.data.BaseTable;
import org.opendatakit.database.data.ColumnDefinition;
import org.opendatakit.database.data.OrderedColumns;
import org.opendatakit.database.data.Row;
import org.opendatakit.services.database.AndroidConnectFactory;
import org.opendatakit.services.database.OdkConnectionFactorySingleton;
import org.opendatakit.services.database.OdkConnectionInterface;
import org.opendatakit.exception.ActionNotAuthorizedException;
import org.opendatakit.provider.DataTableColumns;
import org.opendatakit.database.utilities.KeyValueStoreUtils;
import org.opendatakit.database.LocalKeyValueStoreConstants;
import org.opendatakit.services.database.utlities.ODKDatabaseImplUtils;
import org.opendatakit.utilities.DataHelper;
import org.opendatakit.utilities.ODKFileUtils;
import org.opendatakit.database.data.KeyValueStoreEntry;
import org.opendatakit.database.service.DbHandle;
import org.sqlite.database.sqlite.SQLiteException;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Permissions tests in the database.
 */
public class AbstractPermissionsTestCase extends AndroidTestCase {

  public static final String SAVEPOINT_TIMESTAMP_LOCAL = "2016-10-07T00:11:59.893000000";

  protected static class AuthParamAndOutcome {
    public final String tableId;
    public final String rowId;
    public final String username;
    public final String roles;
    public final boolean throwsAccessException;

    private AuthParamAndOutcome(String tableId, String rowId, String username, String roles,
        boolean throwsAccessException) {
      this.tableId = tableId;
      this.rowId = rowId + username; // use a username-specific rowId so as to retain independence
      this.username = username;
      this.roles = roles;
      this.throwsAccessException = throwsAccessException;
    }

    public String toString() {
      return "tableId: " + tableId + " rowId: " + rowId +
          " username: " + username + " roles: " + roles;
    }
  }

  protected static final String testTableUnlockedNoAnonCreate = "testAccessTableUnlockedNoAnonCreate";
  protected static final String testTableUnlockedYesAnonCreate = "testAccessTableUnlockedYesAnonCreate";
  protected static final String testTableLockedNoAnonCreate = "testAccessTableLockedNoAnonCreate";
  protected static final String testTableLockedYesAnonCreate = "testAccessTableLockedYesAnonCreate";

  protected static final String testFormId = "testAccessForm";
  protected static final String currentLocale = "en_US";

  protected static final String rowIdInserted = "rowIdInserted";
  protected static final String rowIdDefaultNull = "rowIdDefaultNull";
  protected static final String rowIdDefaultCommonNew = "rowIdDefaultCommonNew";
  protected static final String rowIdDefaultCommon = "rowIdDefaultCommon";
  protected static final String rowIdHiddenCommon = "rowIdHiddenCommon";
  protected static final String rowIdReadOnlyCommon = "rowIdReadOnlyCommon";
  protected static final String rowIdModifyCommon = "rowIdModifyCommon";

  protected static final String serverUser = "mailto:server_user@gmail.com";
  protected static final String commonUser = "mailto:common@gmail.com";
  protected static final String otherUser = "mailto:other@gmail.com";
  protected static final String superUser = "mailto:super@gmail.com";
  protected static final String adminUser = "mailto:admin@gmail.com";
  protected static final String anonymousUser = "anonymous";

  protected static boolean initialized = false;
  protected static final String APPNAME = TestConsts.APPNAME;
  protected static final DbHandle uniqueKey = new DbHandle(AbstractPermissionsTestCase.class.getSimpleName() + AndroidConnectFactory.INTERNAL_TYPE_SUFFIX);

  // upon applying the server row, there are 4 outcomes:
  // 1. row is updated locally into synced state
  // 2. row is updated locally into synced_pending_files state
  // 3. row is locally deleted
  // 4. row is placed into conflict
  public enum ServerChangeOutcome {
    LOCALLY_SYNCED,
    LOCALLY_SYNCED_PENDING_FILES,
    LOCALLY_DELETED,
    LOCALLY_IN_CONFLICT
  }

  public static class SyncParamOutcome {

    String rowId;
    boolean isServerRowDeleted;
    boolean changeServerBoolean;
    boolean changeServerInteger;
    boolean changeServerNumber;
    boolean changeServerString;
    boolean changeServerRowPath;
    boolean changeServerFormIdMetadata;
    boolean changeServerPrivilegedMetadata;

    ServerChangeOutcome outcome;

    public SyncParamOutcome(String rowId, boolean isServerRowDeleted,
        boolean changeServerBoolean, boolean changeServerInteger,
        boolean changeServerNumber, boolean changeServerString,
        boolean changeServerRowPath, boolean changeServerFormIdMetadata,
        boolean changeServerPrivilegedMetadata, ServerChangeOutcome outcome) {

      this.rowId = rowId;
      this.isServerRowDeleted = isServerRowDeleted;
      this.changeServerBoolean = changeServerBoolean;
      this.changeServerInteger = changeServerInteger;
      this.changeServerNumber = changeServerNumber;
      this.changeServerString = changeServerString;
      this.changeServerRowPath = changeServerRowPath;
      this.changeServerFormIdMetadata = changeServerFormIdMetadata;
      this.changeServerPrivilegedMetadata = changeServerPrivilegedMetadata;

      this.outcome = outcome;
    }
  }

  protected OdkConnectionInterface db;

  protected String getAppName() {
    return APPNAME;
  }

  /*
* Set up the database for the tests(non-Javadoc)
*
* @see android.test.AndroidTestCase#setUp()
*/
  @Override
  protected synchronized void setUp() throws Exception {
    super.setUp();
    ODKFileUtils.verifyExternalStorageAvailability();
    ODKFileUtils.assertDirectoryStructure(APPNAME);

    boolean beganUninitialized = !initialized;
    if ( beganUninitialized ) {
      initialized = true;
      // Used to ensure that the singleton has been initialized properly
      AndroidConnectFactory.configure();
    }

    // +1 referenceCount if db is returned (non-null)
    db = OdkConnectionFactorySingleton
        .getOdkConnectionFactoryInterface().getConnection(getAppName(), uniqueKey);
    if ( beganUninitialized ) {
      // start clean
      List<String> tableIds = ODKDatabaseImplUtils.get().getAllTableIds(db);

      // Drop any leftover table now that the test is done
      for(String id : tableIds) {
        ODKDatabaseImplUtils.get().deleteTableAndAllData(db, id);
      }
    } else {
      verifyNoTablesExistNCleanAllTables();
    }
  }


  /*
* Destroy all test data once tests are done(non-Javadoc)
*
* @see android.test.AndroidTestCase#tearDown()
*/
  @Override
  protected void tearDown() throws Exception {
    verifyNoTablesExistNCleanAllTables();

    if (db != null) {
      db.releaseReference();
    }

    super.tearDown();
  }

  protected void verifyNoTablesExistNCleanAllTables() {
    /* NOTE: if there is a problem it might be the fault of the previous test if the assertion
       failure is happening in the setUp function as opposed to the tearDown function */

    List<String> tableIds = ODKDatabaseImplUtils.get().getAllTableIds(db);

    // Drop any leftover table now that the test is done
    for(String id : tableIds) {
      ODKDatabaseImplUtils.get().deleteTableAndAllData(db, id);
    }

    tableIds = ODKDatabaseImplUtils.get().getAllTableIds(db);

    boolean tablesGone = (tableIds.size() == 0);

    assertTrue(tablesGone);
  }

  /*
   * Check that the database is setup
   */
  public void testPreConditions() {
    assertNotNull(db);
  }

  protected OrderedColumns assertEmptyTestTable(String tableId, boolean isLocked,
      boolean anonymousCanCreate, String defaultFilterType) {

    List<Column> columns = new ArrayList<Column>();
    // arbitrary type derived from integer
    columns.add(new Column("col0", "col0", "myothertype:integer", "[]"));
    // primitive types
    columns.add(new Column("col1", "col1", "boolean", "[]"));
    columns.add(new Column("col2", "col2", "integer", "[]"));
    columns.add(new Column("col3", "col3", "number", "[]"));
    columns.add(new Column("col4", "col4", "string", "[]"));
    // string with 500 varchars allocated to it
    columns.add(new Column("col5", "col5", "string(500)", "[]"));
    columns.add(new Column("col6", "col6", "configpath", "[]"));
    columns.add(new Column("col7", "col7", "rowpath", "[]"));
    // object type (geopoint)
    columns.add(new Column("col8", "col8", "geopoint",
        "[\"col8_accuracy\",\"col8_altitude\",\"col8_latitude\",\"col8_longitude\"]"));
    columns.add(new Column("col8_accuracy", "accuracy", "number", "[]"));
    columns.add(new Column("col8_altitude", "altitude", "number", "[]"));
    columns.add(new Column("col8_latitude", "latitude", "number", "[]"));
    columns.add(new Column("col8_longitude", "longitude", "number", "[]"));
    // arbitrary type derived from string
    columns.add(new Column("col9", "col9", "mytype", "[]"));

    // arrays
    columns.add(new Column("col12", "col12", "array", "[\"col12_items\"]"));
    columns.add(new Column("col12_items", "items", "integer", "[]"));
    // array with 500 varchars allocated to it
    columns.add(new Column("col14", "col14", "array(400)", "[\"col14_items\"]"));
    columns.add(new Column("col14_items", "items", "string", "[]"));

    OrderedColumns orderedColumns;
    try {
      List<KeyValueStoreEntry> metaData = new ArrayList<KeyValueStoreEntry>();
      KeyValueStoreEntry entry;

      entry = KeyValueStoreUtils.buildEntry( tableId,
          KeyValueStoreConstants.PARTITION_TABLE,
          KeyValueStoreConstants.ASPECT_DEFAULT,
          KeyValueStoreConstants.TABLE_DISPLAY_NAME,
          ElementDataType.object,
          ODKFileUtils.mapper.writeValueAsString("Access Test Table"));
      metaData.add(entry);

      entry = KeyValueStoreUtils.buildEntry( tableId,
          KeyValueStoreConstants.PARTITION_TABLE,
          LocalKeyValueStoreConstants.TableSecurity.ASPECT,
          LocalKeyValueStoreConstants.TableSecurity.KEY_LOCKED,
          ElementDataType.bool,
          Boolean.toString(isLocked).toLowerCase(Locale.ENGLISH));
      metaData.add(entry);

      entry = KeyValueStoreUtils.buildEntry( tableId,
          KeyValueStoreConstants.PARTITION_TABLE,
          LocalKeyValueStoreConstants.TableSecurity.ASPECT,
          LocalKeyValueStoreConstants.TableSecurity.KEY_UNVERIFIED_USER_CAN_CREATE,
          ElementDataType.bool,
          Boolean.toString(anonymousCanCreate).toLowerCase(Locale.ENGLISH));
      metaData.add(entry);

      entry = KeyValueStoreUtils.buildEntry( tableId,
          KeyValueStoreConstants.PARTITION_TABLE,
          LocalKeyValueStoreConstants.TableSecurity.ASPECT,
          LocalKeyValueStoreConstants.TableSecurity.KEY_FILTER_TYPE_ON_CREATION,
          ElementDataType.string,
          defaultFilterType);
      metaData.add(entry);

      return orderedColumns = ODKDatabaseImplUtils.get()
          .createOrOpenTableWithColumnsAndProperties(db, tableId, columns, metaData, true);
    } catch (JsonProcessingException e) {
      e.printStackTrace();
      throw new IllegalStateException("Unable to create table with properties");
    }
  }

  protected ContentValues buildServerRowContent(String tableId, String rowId,
      boolean isServerRowDeleted,
      RowFilterScope.Type type, Boolean[] changeArray) {

    // and now add rows to that table
    ContentValues cvValues = buildUnprivilegedInsertableRowContent(tableId);

    cvValues.putNull(DataTableColumns.CONFLICT_TYPE);

    cvValues.put(DataTableColumns.SAVEPOINT_TIMESTAMP, SAVEPOINT_TIMESTAMP_LOCAL);
    cvValues.put(DataTableColumns.SAVEPOINT_TYPE, SavepointTypeManipulator.complete());
    cvValues.put(DataTableColumns.SAVEPOINT_CREATOR, adminUser);

    // first -- insert row as "new_row" with no rowETag
    cvValues.put(DataTableColumns.ROW_ETAG, "server content");
    cvValues.put(DataTableColumns.SYNC_STATE, (isServerRowDeleted ?
        SyncState.deleted.name() : SyncState.changed.name()));

    RowFilterScope.Type localType;
    if ( rowId.equals(rowIdDefaultNull) ) {
      localType = RowFilterScope.Type.DEFAULT;
      cvValues.put(DataTableColumns.FILTER_TYPE, localType.name());
      cvValues.putNull(DataTableColumns.FILTER_VALUE);
    } else if ( rowId.equals(rowIdDefaultCommon) ) {
      localType = RowFilterScope.Type.DEFAULT;
      cvValues.put(DataTableColumns.FILTER_TYPE, localType.name());
      cvValues.put(DataTableColumns.FILTER_VALUE, commonUser);
    } else if ( rowId.equals(rowIdHiddenCommon) ) {
      localType = RowFilterScope.Type.HIDDEN;
      cvValues.put(DataTableColumns.FILTER_TYPE, localType.name());
      cvValues.put(DataTableColumns.FILTER_VALUE, commonUser);
    } else if ( rowId.equals(rowIdReadOnlyCommon) ) {
      localType = RowFilterScope.Type.READ_ONLY;
      cvValues.put(DataTableColumns.FILTER_TYPE, localType.name());
      cvValues.put(DataTableColumns.FILTER_VALUE, commonUser);
    } else if ( rowId.equals(rowIdModifyCommon) ) {
      localType = RowFilterScope.Type.MODIFY;
      cvValues.put(DataTableColumns.FILTER_TYPE, localType.name());
      cvValues.put(DataTableColumns.FILTER_VALUE, commonUser);
    } else {
      throw new IllegalArgumentException("unexpected rowId value");
    }

    if (changeArray != null && changeArray.length >= 1 && changeArray[0]) {
      cvValues.put("col1", false); // boolean
    }
    if (changeArray != null && changeArray.length >= 2 && changeArray[1]) {
      cvValues.put("col2", 4); // integer
    }
    if (changeArray != null && changeArray.length >= 3 && changeArray[2]) {
      cvValues.put("col3", 2 * Math.PI); // number
    }
    if (changeArray != null && changeArray.length >= 4 && changeArray[3]) {
      cvValues.put("col4", "a different string from the server"); // string
    }
    if (changeArray != null && changeArray.length >= 5 && changeArray[4]) {
      File rowFile = new File(ODKFileUtils.getInstanceFolder(getAppName(), tableId, rowIdDefaultCommon),
          "server_sample.jpg");
      cvValues.put("col7",
          ODKFileUtils.asRowpathUri(getAppName(), tableId, rowIdDefaultCommon, rowFile)); // rowpath
    }
    if (changeArray != null && changeArray.length >= 7 && changeArray[6]) {
      if ( type == localType ) {
        cvValues.put(DataTableColumns.FILTER_VALUE, serverUser);
      } else {
        cvValues.put(DataTableColumns.FILTER_TYPE, type.name());
      }
    }
    return cvValues;
  }

  /**
   * These are content values that don't set any metadata fields that are privileged.
   */
  protected ContentValues buildUnprivilegedInsertableRowContent(String tableId) {

    // build the content for a row that would be inserted into the table
    ContentValues cvValues = new ContentValues();
    cvValues.put("col0", -1); // myothertype:integer
    // primitive types
    cvValues.put("col1", true); // boolean
    cvValues.put("col2", 5); // integer
    cvValues.put("col3", Math.PI); // number;
    cvValues.put("col4", "this is a test"); // string
    // string with 500 varchars allocated to it
    cvValues.put("col5", "and a long string test"); // string(500)
    File configFile = new File(
        ODKFileUtils.getAssetsCsvInstanceFolder(getAppName(), tableId, rowIdDefaultCommon),
        "sample.jpg");
    cvValues.put("col6", ODKFileUtils.asConfigRelativePath(getAppName(), configFile)); // configpath
    File rowFile = new File(ODKFileUtils.getInstanceFolder(getAppName(), tableId, rowIdDefaultCommon), "sample.jpg");
    try {
      Writer writer = new FileWriter(rowFile);
      writer.write("testFile");
      writer.flush();
      writer.close();
    } catch (IOException e) {
      // ignore
    }
    cvValues.put("col7",
        ODKFileUtils.asRowpathUri(getAppName(), tableId, rowIdDefaultCommon, rowFile)); // rowpath
    // object type (geopoint)
    cvValues.put("col8_accuracy", 45.2); // number
    cvValues.put("col8_altitude", 45.3); // number
    cvValues.put("col8_latitude", 45.5); // number
    cvValues.put("col8_longitude", 45.4); // number
    // arbitrary type derived from string
    cvValues.put("col9", "a string based custom type"); // mytype

    // arrays
    cvValues.put("col12", "[1,-1]"); // array of integers
    // array with 500 varchars allocated to it
    cvValues.put("col14", "[\"Test\",\"this\"]"); // array of strings -- varchar 500 allocation

    cvValues.put(DataTableColumns.FORM_ID, testFormId);
    cvValues.put(DataTableColumns.LOCALE, currentLocale);

    return cvValues;
  }

  protected OrderedColumns assertConflictPopulatedTestTable(String tableId, boolean isLocked,
      boolean anonymousCanCreate, String defaultFilterType) {

    OrderedColumns orderedColumns =
        assertEmptyTestTable(tableId, isLocked, anonymousCanCreate, defaultFilterType);

    // and now add rows to that table
    ContentValues cvValues = buildUnprivilegedInsertableRowContent(tableId);

    cvValues.put(DataTableColumns.SAVEPOINT_TIMESTAMP, TableConstants.nanoSecondsFromMillis
        (System.currentTimeMillis()));
    cvValues.put(DataTableColumns.SAVEPOINT_TYPE, SavepointTypeManipulator.complete());
    cvValues.put(DataTableColumns.SAVEPOINT_CREATOR, adminUser);

    // no New rows are possible; always changed or deleted.

    // start with local row
    cvValues.putNull(DataTableColumns.CONFLICT_TYPE);

    // now insert rows that have been synced
    cvValues.put(DataTableColumns.ROW_ETAG, "not_null_because_has_been_synced");
    cvValues.put(DataTableColumns.SYNC_STATE, SyncState.synced.name());

    cvValues.put(DataTableColumns.FILTER_TYPE, RowFilterScope.Type.DEFAULT.name());
    cvValues.putNull(DataTableColumns.FILTER_VALUE);
    spewConflictInsert(tableId, orderedColumns, cvValues,
        rowIdDefaultNull, commonUser, currentLocale);

    cvValues.put(DataTableColumns.FILTER_TYPE, RowFilterScope.Type.DEFAULT.name());
    cvValues.put(DataTableColumns.FILTER_VALUE, commonUser);
    spewConflictInsert(tableId, orderedColumns, cvValues,
        rowIdDefaultCommon, commonUser, currentLocale);

    cvValues.put(DataTableColumns.FILTER_TYPE, RowFilterScope.Type.HIDDEN.name());
    cvValues.put(DataTableColumns.FILTER_VALUE, commonUser);
    spewConflictInsert(tableId, orderedColumns, cvValues,
        rowIdHiddenCommon, commonUser, currentLocale);

    cvValues.put(DataTableColumns.FILTER_TYPE, RowFilterScope.Type.READ_ONLY.name());
    cvValues.put(DataTableColumns.FILTER_VALUE, commonUser);
    spewConflictInsert(tableId, orderedColumns, cvValues,
        rowIdReadOnlyCommon, commonUser, currentLocale);

    cvValues.put(DataTableColumns.FILTER_TYPE, RowFilterScope.Type.MODIFY.name());
    cvValues.put(DataTableColumns.FILTER_VALUE, commonUser);
    spewConflictInsert(tableId, orderedColumns, cvValues,
        rowIdModifyCommon, commonUser, currentLocale);

    return orderedColumns;
  }

  protected OrderedColumns assertPopulatedTestTable(String tableId, boolean isLocked,
      boolean anonymousCanCreate, String defaultFilterType) {

    OrderedColumns orderedColumns =
        assertEmptyTestTable(tableId, isLocked, anonymousCanCreate, defaultFilterType);

    // and now add rows to that table
    ContentValues cvValues = buildUnprivilegedInsertableRowContent(tableId);

    cvValues.putNull(DataTableColumns.CONFLICT_TYPE);

    cvValues.put(DataTableColumns.SAVEPOINT_TIMESTAMP, TableConstants.nanoSecondsFromMillis
        (System.currentTimeMillis()));
    cvValues.put(DataTableColumns.SAVEPOINT_TYPE, SavepointTypeManipulator.complete());
    cvValues.put(DataTableColumns.SAVEPOINT_CREATOR, adminUser);

    // first -- insert row as "new_row" with no rowETag
    cvValues.putNull(DataTableColumns.ROW_ETAG);
    cvValues.put(DataTableColumns.SYNC_STATE, SyncState.new_row.name());

    cvValues.put(DataTableColumns.FILTER_TYPE, RowFilterScope.Type.DEFAULT.name());
    cvValues.put(DataTableColumns.FILTER_VALUE, commonUser);
    spewInsert(tableId, orderedColumns, cvValues,
        rowIdDefaultCommonNew, commonUser, currentLocale);


    // now insert rows that have been synced
    cvValues.put(DataTableColumns.ROW_ETAG, "not_null_because_has_been_synced");
    cvValues.put(DataTableColumns.SYNC_STATE, SyncState.synced.name());

    cvValues.put(DataTableColumns.FILTER_TYPE, RowFilterScope.Type.DEFAULT.name());
    cvValues.putNull(DataTableColumns.FILTER_VALUE);
    spewInsert(tableId, orderedColumns, cvValues,
        rowIdDefaultNull, commonUser, currentLocale);

    cvValues.put(DataTableColumns.FILTER_TYPE, RowFilterScope.Type.DEFAULT.name());
    cvValues.put(DataTableColumns.FILTER_VALUE, commonUser);
    spewInsert(tableId, orderedColumns, cvValues,
        rowIdDefaultCommon, commonUser, currentLocale);

    cvValues.put(DataTableColumns.FILTER_TYPE, RowFilterScope.Type.HIDDEN.name());
    cvValues.put(DataTableColumns.FILTER_VALUE, commonUser);
    spewInsert(tableId, orderedColumns, cvValues,
        rowIdHiddenCommon, commonUser, currentLocale);

    cvValues.put(DataTableColumns.FILTER_TYPE, RowFilterScope.Type.READ_ONLY.name());
    cvValues.put(DataTableColumns.FILTER_VALUE, commonUser);
    spewInsert(tableId, orderedColumns, cvValues,
        rowIdReadOnlyCommon, commonUser, currentLocale);

    cvValues.put(DataTableColumns.FILTER_TYPE, RowFilterScope.Type.MODIFY.name());
    cvValues.put(DataTableColumns.FILTER_VALUE, commonUser);
    spewInsert(tableId, orderedColumns, cvValues,
        rowIdModifyCommon, commonUser, currentLocale);

    return orderedColumns;
  }

  protected OrderedColumns assertEmptySyncStateTestTable(String tableId, boolean isLocked,
      boolean anonymousCanCreate, String defaultFilterType) {

    ODKDatabaseImplUtils.get().deleteTableAndAllData(db, tableId);

    OrderedColumns orderedColumns =
        assertEmptyTestTable(tableId, isLocked, anonymousCanCreate, defaultFilterType);

    return orderedColumns;
  }

  protected void assertRowInSyncStateTestTable(String tableId,
      OrderedColumns orderedColumns,
      String rowId, SyncState state) throws ActionNotAuthorizedException {

    ODKDatabaseImplUtils.get().privilegedDeleteRowWithId(db, tableId, rowId, adminUser);

    // and now add rows to that table
    ContentValues cvValues = buildUnprivilegedInsertableRowContent(tableId);

    cvValues.putNull(DataTableColumns.CONFLICT_TYPE);

    cvValues.put(DataTableColumns.SAVEPOINT_TIMESTAMP, SAVEPOINT_TIMESTAMP_LOCAL);
    cvValues.put(DataTableColumns.SAVEPOINT_TYPE, SavepointTypeManipulator.complete());
    cvValues.put(DataTableColumns.SAVEPOINT_CREATOR, adminUser);

    // first -- insert row as "new_row" with no rowETag
    if ( state == SyncState.new_row ) {
      cvValues.putNull(DataTableColumns.ROW_ETAG);
    } else {
      cvValues.put(DataTableColumns.ROW_ETAG, "not_null_because_has_been_synced");
    }
    cvValues.put(DataTableColumns.SYNC_STATE, state.name());

    cvValues.put(DataTableColumns.FILTER_TYPE, RowFilterScope.Type.DEFAULT.name());
    cvValues.putNull(DataTableColumns.FILTER_VALUE);
    if ( rowIdDefaultNull.equals(rowId) ) {
      ODKDatabaseImplUtils.get()
          .privilegedInsertRowWithId(db, tableId, orderedColumns, cvValues, rowIdDefaultNull,
              commonUser, currentLocale, false);
    }

    cvValues.put(DataTableColumns.FILTER_TYPE, RowFilterScope.Type.DEFAULT.name());
    cvValues.put(DataTableColumns.FILTER_VALUE, commonUser);
    if ( rowIdDefaultCommon.equals(rowId) ) {
      ODKDatabaseImplUtils.get()
          .privilegedInsertRowWithId(db, tableId, orderedColumns, cvValues, rowIdDefaultCommon,
              commonUser, currentLocale, false);
    }

    cvValues.put(DataTableColumns.FILTER_TYPE, RowFilterScope.Type.HIDDEN.name());
    cvValues.put(DataTableColumns.FILTER_VALUE, commonUser);
    if ( rowIdHiddenCommon.equals(rowId) ) {
      ODKDatabaseImplUtils.get()
          .privilegedInsertRowWithId(db, tableId, orderedColumns, cvValues, rowIdHiddenCommon,
              commonUser, currentLocale, false);
    }

    cvValues.put(DataTableColumns.FILTER_TYPE, RowFilterScope.Type.READ_ONLY.name());
    cvValues.put(DataTableColumns.FILTER_VALUE, commonUser);
    if ( rowIdReadOnlyCommon.equals(rowId) ) {
      ODKDatabaseImplUtils.get()
          .privilegedInsertRowWithId(db, tableId, orderedColumns, cvValues, rowIdReadOnlyCommon,
              commonUser, currentLocale, false);
    }

    cvValues.put(DataTableColumns.FILTER_TYPE, RowFilterScope.Type.MODIFY.name());
    cvValues.put(DataTableColumns.FILTER_VALUE, commonUser);
    if ( rowIdModifyCommon.equals(rowId) ) {
      ODKDatabaseImplUtils.get()
          .privilegedInsertRowWithId(db, tableId, orderedColumns, cvValues, rowIdModifyCommon,
              commonUser, currentLocale, false);
    }
  }


  protected void assertInConflictRowInSyncStateTestTable(String tableId,
      OrderedColumns orderedColumns,
      String rowId, int localConflictType, int serverConflictType)
      throws ActionNotAuthorizedException {

    ODKDatabaseImplUtils.get().privilegedDeleteRowWithId(db, tableId, rowId, adminUser);

    // and now add rows to that table
    ContentValues cvValues = buildUnprivilegedInsertableRowContent(tableId);

    cvValues.put(DataTableColumns.SAVEPOINT_TIMESTAMP, SAVEPOINT_TIMESTAMP_LOCAL);
    cvValues.put(DataTableColumns.SAVEPOINT_TYPE, SavepointTypeManipulator.complete());
    cvValues.put(DataTableColumns.SAVEPOINT_CREATOR, adminUser);

    // first -- insert row as "new_row" with no rowETag
    cvValues.put(DataTableColumns.SYNC_STATE, SyncState.in_conflict.name());
    cvValues.put(DataTableColumns.ROW_ETAG, "not_null_because_has_been_synced");

    cvValues.put(DataTableColumns.FILTER_TYPE, RowFilterScope.Type.DEFAULT.name());
    cvValues.putNull(DataTableColumns.FILTER_VALUE);

    if ( rowIdDefaultNull.equals(rowId) ) {
      cvValues.put(DataTableColumns.CONFLICT_TYPE, localConflictType);
      ODKDatabaseImplUtils.get()
          .privilegedInsertRowWithId(db, tableId, orderedColumns, cvValues, rowIdDefaultNull,
              commonUser, currentLocale, false);
      cvValues.put("col3",2*Math.PI);
      cvValues.put(DataTableColumns.CONFLICT_TYPE, serverConflictType);
      ODKDatabaseImplUtils.get()
          .privilegedInsertRowWithId(db, tableId, orderedColumns, cvValues, rowIdDefaultNull,
              commonUser, currentLocale, false);
    }

    cvValues.put(DataTableColumns.FILTER_TYPE, RowFilterScope.Type.DEFAULT.name());
    cvValues.put(DataTableColumns.FILTER_VALUE, commonUser);
    if ( rowIdDefaultCommon.equals(rowId) ) {
      cvValues.put(DataTableColumns.CONFLICT_TYPE, localConflictType);
      ODKDatabaseImplUtils.get()
          .privilegedInsertRowWithId(db, tableId, orderedColumns, cvValues, rowIdDefaultCommon,
              commonUser, currentLocale, false);
      cvValues.put("col3",2*Math.PI);
      cvValues.put(DataTableColumns.CONFLICT_TYPE, serverConflictType);
      ODKDatabaseImplUtils.get()
          .privilegedInsertRowWithId(db, tableId, orderedColumns, cvValues, rowIdDefaultCommon,
              commonUser, currentLocale, false);
    }

    cvValues.put(DataTableColumns.FILTER_TYPE, RowFilterScope.Type.HIDDEN.name());
    cvValues.put(DataTableColumns.FILTER_VALUE, commonUser);
    if ( rowIdHiddenCommon.equals(rowId) ) {
      cvValues.put(DataTableColumns.CONFLICT_TYPE, localConflictType);
      ODKDatabaseImplUtils.get()
          .privilegedInsertRowWithId(db, tableId, orderedColumns, cvValues, rowIdHiddenCommon,
              commonUser, currentLocale, false);
      cvValues.put("col3",2*Math.PI);
      cvValues.put(DataTableColumns.CONFLICT_TYPE, serverConflictType);
      ODKDatabaseImplUtils.get()
          .privilegedInsertRowWithId(db, tableId, orderedColumns, cvValues, rowIdHiddenCommon,
              commonUser, currentLocale, false);
    }

    cvValues.put(DataTableColumns.FILTER_TYPE, RowFilterScope.Type.READ_ONLY.name());
    cvValues.put(DataTableColumns.FILTER_VALUE, commonUser);
    if ( rowIdReadOnlyCommon.equals(rowId) ) {
      cvValues.put(DataTableColumns.CONFLICT_TYPE, localConflictType);
      ODKDatabaseImplUtils.get()
          .privilegedInsertRowWithId(db, tableId, orderedColumns, cvValues, rowIdReadOnlyCommon,
              commonUser, currentLocale, false);
      cvValues.put("col3",2*Math.PI);
      cvValues.put(DataTableColumns.CONFLICT_TYPE, serverConflictType);
      ODKDatabaseImplUtils.get()
          .privilegedInsertRowWithId(db, tableId, orderedColumns, cvValues, rowIdReadOnlyCommon,
              commonUser, currentLocale, false);
    }

    cvValues.put(DataTableColumns.FILTER_TYPE, RowFilterScope.Type.MODIFY.name());
    cvValues.put(DataTableColumns.FILTER_VALUE, commonUser);
    if ( rowIdModifyCommon.equals(rowId) ) {
      cvValues.put(DataTableColumns.CONFLICT_TYPE, localConflictType);
      ODKDatabaseImplUtils.get()
          .privilegedInsertRowWithId(db, tableId, orderedColumns, cvValues, rowIdModifyCommon,
              commonUser, currentLocale, false);
      cvValues.put("col3",2*Math.PI);
      cvValues.put(DataTableColumns.CONFLICT_TYPE, serverConflictType);
      ODKDatabaseImplUtils.get()
          .privilegedInsertRowWithId(db, tableId, orderedColumns, cvValues, rowIdModifyCommon,
              commonUser, currentLocale, false);
    }
  }

  protected OrderedColumns assertOneCheckpointAsUpdatePopulatedTestTable(
      String tableId, boolean isLocked,
      boolean anonymousCanCreate, String defaultFilterType) throws ActionNotAuthorizedException {

    OrderedColumns orderedColumns =
        assertPopulatedTestTable(tableId, isLocked, anonymousCanCreate, defaultFilterType);

    // and now add checkpoint row to that table.
    // Add these as the commonUser, but with admin roles so that the insert of
    // the checkpoint succeeds. We will be testing the deletion capability of the
    // system, so the admin roles for insert are just to facilitate the processing.
    ContentValues cvValues = buildUnprivilegedInsertableRowContent(tableId);

    cvValues.put("col0", -11); // myothertype:integer

    spewInsertCheckpoint(tableId, orderedColumns, cvValues,
        rowIdDefaultCommonNew, commonUser, currentLocale);

    spewInsertCheckpoint(tableId, orderedColumns, cvValues,
        rowIdDefaultNull, commonUser, currentLocale);

    spewInsertCheckpoint(tableId, orderedColumns, cvValues,
        rowIdDefaultCommon, commonUser, currentLocale);

    spewInsertCheckpoint(tableId, orderedColumns, cvValues,
        rowIdHiddenCommon, commonUser, currentLocale);

    spewInsertCheckpoint(tableId, orderedColumns, cvValues,
        rowIdReadOnlyCommon, commonUser, currentLocale);

    spewInsertCheckpoint(tableId, orderedColumns, cvValues,
        rowIdModifyCommon, commonUser, currentLocale);

    return orderedColumns;
  }

  protected OrderedColumns assertTwoCheckpointAsUpdatePopulatedTestTable(
      String tableId, boolean isLocked,
      boolean anonymousCanCreate, String defaultFilterType) throws ActionNotAuthorizedException {

    OrderedColumns orderedColumns =
        assertOneCheckpointAsUpdatePopulatedTestTable(tableId, isLocked, anonymousCanCreate, defaultFilterType);

    // and now add checkpoint row to that table.
    // Add these as the commonUser, but with admin roles so that the insert of
    // the checkpoint succeeds. We will be testing the deletion capability of the
    // system, so the admin roles for insert are just to facilitate the processing.
    ContentValues cvValues = buildUnprivilegedInsertableRowContent(tableId);

    cvValues.put("col0", -12); // myothertype:integer

    spewInsertCheckpoint(tableId, orderedColumns, cvValues,
        rowIdDefaultCommonNew, commonUser, currentLocale);

    spewInsertCheckpoint(tableId, orderedColumns, cvValues,
        rowIdDefaultNull, commonUser, currentLocale);

    spewInsertCheckpoint(tableId, orderedColumns, cvValues,
        rowIdDefaultCommon, commonUser, currentLocale);

    spewInsertCheckpoint(tableId, orderedColumns, cvValues,
        rowIdHiddenCommon, commonUser, currentLocale);

    spewInsertCheckpoint(tableId, orderedColumns, cvValues,
        rowIdReadOnlyCommon, commonUser, currentLocale);

    spewInsertCheckpoint(tableId, orderedColumns, cvValues,
        rowIdModifyCommon, commonUser, currentLocale);

    return orderedColumns;
  }

  protected OrderedColumns assertOneCheckpointAsInsertPopulatedTestTable(
      String tableId, boolean isLocked,
      boolean anonymousCanCreate, String defaultFilterType) throws ActionNotAuthorizedException {

    OrderedColumns orderedColumns =
        assertEmptyTestTable(tableId, isLocked, anonymousCanCreate, defaultFilterType);

    // and now add checkpoint row to that table.
    // Add these as the commonUser, but with admin roles so that the insert of
    // the checkpoint succeeds. We will be testing the deletion capability of the
    // system, so the admin roles for insert are just to facilitate the processing.
    ContentValues cvValues = buildUnprivilegedInsertableRowContent(tableId);

    cvValues.put("col0", -11); // myothertype:integer

    spewInsertCheckpoint(tableId, orderedColumns, cvValues,
        rowIdDefaultCommonNew, commonUser, currentLocale);

    spewInsertCheckpoint(tableId, orderedColumns, cvValues,
        rowIdDefaultNull, commonUser, currentLocale);

    spewInsertCheckpoint(tableId, orderedColumns, cvValues,
        rowIdDefaultCommon, commonUser, currentLocale);

    spewInsertCheckpoint(tableId, orderedColumns, cvValues,
        rowIdHiddenCommon, commonUser, currentLocale);

    spewInsertCheckpoint(tableId, orderedColumns, cvValues,
        rowIdReadOnlyCommon, commonUser, currentLocale);

    spewInsertCheckpoint(tableId, orderedColumns, cvValues,
        rowIdModifyCommon, commonUser, currentLocale);

    return orderedColumns;
  }

  protected OrderedColumns assertTwoCheckpointAsInsertPopulatedTestTable(
      String tableId, boolean isLocked,
      boolean anonymousCanCreate, String defaultFilterType) throws ActionNotAuthorizedException {

    OrderedColumns orderedColumns =
        assertOneCheckpointAsInsertPopulatedTestTable(tableId, isLocked, anonymousCanCreate, defaultFilterType);

    // and now add checkpoint row to that table.
    // Add these as the commonUser, but with admin roles so that the insert of
    // the checkpoint succeeds. We will be testing the deletion capability of the
    // system, so the admin roles for insert are just to facilitate the processing.
    ContentValues cvValues = buildUnprivilegedInsertableRowContent(tableId);

    cvValues.put("col0", -12); // myothertype:integer

    spewInsertCheckpoint(tableId, orderedColumns, cvValues,
        rowIdDefaultCommonNew, commonUser, currentLocale);

    spewInsertCheckpoint(tableId, orderedColumns, cvValues,
        rowIdDefaultNull, commonUser, currentLocale);

    spewInsertCheckpoint(tableId, orderedColumns, cvValues,
        rowIdDefaultCommon, commonUser, currentLocale);

    spewInsertCheckpoint(tableId, orderedColumns, cvValues,
        rowIdHiddenCommon, commonUser, currentLocale);

    spewInsertCheckpoint(tableId, orderedColumns, cvValues,
        rowIdReadOnlyCommon, commonUser, currentLocale);

    spewInsertCheckpoint(tableId, orderedColumns, cvValues,
        rowIdModifyCommon, commonUser, currentLocale);

    return orderedColumns;
  }

  protected void spewInsertCheckpoint(String tableId, OrderedColumns orderedColumns, ContentValues cvValues,
      String rowIdBase, String username, String locale) throws ActionNotAuthorizedException {

    // to eliminate impacts of sequential tests, use a different rowId for each possible user
    ODKDatabaseImplUtils.get().insertCheckpointRowWithId(db, tableId, orderedColumns, cvValues,
        rowIdBase + anonymousUser, username, RoleConsts.ADMIN_ROLES_LIST, locale);
    ODKDatabaseImplUtils.get().insertCheckpointRowWithId(db, tableId, orderedColumns, cvValues,
        rowIdBase + commonUser, username, RoleConsts.ADMIN_ROLES_LIST, locale);
    ODKDatabaseImplUtils.get().insertCheckpointRowWithId(db, tableId, orderedColumns, cvValues,
        rowIdBase + otherUser, username, RoleConsts.ADMIN_ROLES_LIST, locale);
    ODKDatabaseImplUtils.get().insertCheckpointRowWithId(db, tableId, orderedColumns, cvValues,
        rowIdBase + superUser, username, RoleConsts.ADMIN_ROLES_LIST, locale);
    ODKDatabaseImplUtils.get().insertCheckpointRowWithId(db, tableId, orderedColumns, cvValues,
        rowIdBase + adminUser, username, RoleConsts.ADMIN_ROLES_LIST, locale);
  }

  /** write into an empty rowId */
  protected void insertRowWIthAdminRights(AuthParamAndOutcome ap, OrderedColumns
      orderedColumns) throws ActionNotAuthorizedException {
    // insert 1 row
    ContentValues cvValues = buildUnprivilegedInsertableRowContent(ap.tableId);

    ODKDatabaseImplUtils.get()
        .insertRowWithId(db, ap.tableId, orderedColumns, cvValues, ap.rowId,
            commonUser, RoleConsts.ADMIN_ROLES_LIST, currentLocale);
  }

  /** write checkpoints into an empty rowId */
  protected void insert2CheckpointsWithAdminRights(AuthParamAndOutcome ap, OrderedColumns
      orderedColumns) throws ActionNotAuthorizedException {
    // insert 2 checkpoints
      ContentValues cvValues = buildUnprivilegedInsertableRowContent(ap.tableId);

      ODKDatabaseImplUtils.get()
          .insertCheckpointRowWithId(db, ap.tableId, orderedColumns, cvValues, ap.rowId,
              commonUser, RoleConsts.ADMIN_ROLES_LIST, currentLocale);

      cvValues.clear();
      cvValues.put("col0", 12);

      ODKDatabaseImplUtils.get()
          .insertCheckpointRowWithId(db, ap.tableId, orderedColumns, cvValues, ap.rowId,
              commonUser, RoleConsts.ADMIN_ROLES_LIST, currentLocale);
  }

  protected void spewInsert(String tableId, OrderedColumns orderedColumns, ContentValues cvValues,
      String rowIdBase, String username, String locale) {

    // to eliminate impacts of sequential tests, use a different rowId for each possible user
    ODKDatabaseImplUtils.get().privilegedInsertRowWithId(db, tableId, orderedColumns, cvValues,
        rowIdBase + anonymousUser, username, locale, false);
    ODKDatabaseImplUtils.get().privilegedInsertRowWithId(db, tableId, orderedColumns, cvValues,
        rowIdBase + commonUser, username, locale, false);
    ODKDatabaseImplUtils.get().privilegedInsertRowWithId(db, tableId, orderedColumns, cvValues,
        rowIdBase + otherUser, username, locale, false);
    ODKDatabaseImplUtils.get().privilegedInsertRowWithId(db, tableId, orderedColumns, cvValues,
        rowIdBase + superUser, username, locale, false);
    ODKDatabaseImplUtils.get().privilegedInsertRowWithId(db, tableId, orderedColumns, cvValues,
        rowIdBase + adminUser, username, locale, false);
  }

  protected void spewConflictInsert(String tableId, OrderedColumns orderedColumns, ContentValues
      cvValues,
      String rowIdBase, String username, String locale) {

    ContentValues serverValues = new ContentValues(cvValues);

    // have different status for form and savepoint info
    serverValues.put(DataTableColumns.FORM_ID, "a_different_server_form");
    serverValues.put(DataTableColumns.SAVEPOINT_TIMESTAMP, TableConstants.nanoSecondsFromMillis
        (System.currentTimeMillis() - 864000000L));
    serverValues.put(DataTableColumns.SAVEPOINT_TYPE, SavepointTypeManipulator.complete());
    serverValues.put(DataTableColumns.SAVEPOINT_CREATOR, "server@gmail.com");

    // server row has a different rowETag and is in conflict
    serverValues.put(DataTableColumns.ROW_ETAG, "a different string");
    serverValues.put(DataTableColumns.SYNC_STATE, SyncState.in_conflict.name());

    // will change a user field and set the conflict type within the loop below

    int localConflict;
    int serverConflict;
    for ( localConflict = ConflictType.LOCAL_DELETED_OLD_VALUES;
          localConflict < ConflictType.SERVER_DELETED_OLD_VALUES ; ++ localConflict ) {

      for ( serverConflict = ConflictType.SERVER_DELETED_OLD_VALUES ;
            serverConflict <= ConflictType.SERVER_UPDATED_UPDATED_VALUES ; ++ serverConflict ) {

        if ( localConflict == ConflictType.LOCAL_DELETED_OLD_VALUES &&
            serverConflict == ConflictType.SERVER_DELETED_OLD_VALUES ) {
          // automatically deleted when syncing
          continue;
        }

        String rowIdConflictBase = rowIdBase + localConflict + serverConflict;

        // to eliminate impacts of sequential tests, use a different rowId for each possible user
        ODKDatabaseImplUtils.get().privilegedInsertRowWithId(db, tableId, orderedColumns, cvValues,
            rowIdConflictBase + anonymousUser, username, locale, false);
        ODKDatabaseImplUtils.get().privilegedInsertRowWithId(db, tableId, orderedColumns, cvValues,
            rowIdConflictBase + commonUser, username, locale, false);
        ODKDatabaseImplUtils.get().privilegedInsertRowWithId(db, tableId, orderedColumns, cvValues,
            rowIdConflictBase + otherUser, username, locale, false);
        ODKDatabaseImplUtils.get().privilegedInsertRowWithId(db, tableId, orderedColumns, cvValues,
            rowIdConflictBase + superUser, username, locale, false);
        ODKDatabaseImplUtils.get().privilegedInsertRowWithId(db, tableId, orderedColumns, cvValues,
            rowIdConflictBase + adminUser, username, locale, false);


        // change a user field
        serverValues.put("col0", 100*(localConflict+1) + serverConflict );
        // set the server conflict type
        serverValues.put(DataTableColumns.CONFLICT_TYPE, serverConflict);

        ODKDatabaseImplUtils.get().privilegedPlaceRowIntoConflictWithId(db, tableId,
            orderedColumns,
            serverValues, rowIdConflictBase + anonymousUser, localConflict, username, locale);
        ODKDatabaseImplUtils.get().privilegedPlaceRowIntoConflictWithId(db, tableId,
            orderedColumns,
            serverValues, rowIdConflictBase + commonUser, localConflict, username, locale);
        ODKDatabaseImplUtils.get().privilegedPlaceRowIntoConflictWithId(db, tableId,
            orderedColumns,
            serverValues, rowIdConflictBase + otherUser, localConflict, username, locale);
        ODKDatabaseImplUtils.get().privilegedPlaceRowIntoConflictWithId(db, tableId,
            orderedColumns,
            serverValues, rowIdConflictBase + superUser, localConflict, username, locale);
        ODKDatabaseImplUtils.get().privilegedPlaceRowIntoConflictWithId(db, tableId,
            orderedColumns,
            serverValues, rowIdConflictBase + adminUser, localConflict, username, locale);
      }
    }
  }

  protected enum FirstSavepointTimestampType { CHECKPOINT_NEW_ROW, NEW_ROW, CHANGED, SYNCED,
    SYNCED_PENDING_FILES, IN_CONFLICT, DELETED }

  protected boolean verifyRowSyncStateAndCheckpoints(String tableId, String rowId, int expectedRowCount,
      FirstSavepointTimestampType expectFirstRemainingType, String identifyingDescription) {

    return verifyRowSyncStateFilterTypeAndCheckpoints(tableId, rowId, expectedRowCount,
        expectFirstRemainingType, null, identifyingDescription);
  }

  protected boolean verifyRowSyncStateFilterTypeAndCheckpoints(String tableId, String rowId, int
      expectedRowCount,
      FirstSavepointTimestampType expectFirstRemainingType, RowFilterScope.Type type,
      String identifyingDescription) {
    Cursor c = null;
    try {
      c = ODKDatabaseImplUtils.get().queryForTest(db, tableId,
          new String[] { DataTableColumns.FILTER_TYPE, DataTableColumns.FILTER_VALUE,
              DataTableColumns.SYNC_STATE, DataTableColumns.SAVEPOINT_TYPE},
          DataTableColumns.ID + "=?",
          new String[] { rowId }, null, null,
                DataTableColumns.CONFLICT_TYPE + " ASC, "
              + DataTableColumns.SAVEPOINT_TIMESTAMP + " ASC", null);
      c.moveToFirst();

      assertEquals("Wrong row count: " + identifyingDescription, expectedRowCount, c.getCount());

      if (c.getCount() == 0 ) {
        return true;
      }

      int idxSyncState = c.getColumnIndex(DataTableColumns.SYNC_STATE);
      int idxType = c.getColumnIndex(DataTableColumns.SAVEPOINT_TYPE);
      int idxFilterType = c.getColumnIndex(DataTableColumns.FILTER_TYPE);
      int idxFilterValue = c.getColumnIndex(DataTableColumns.FILTER_VALUE);

      String syncState = c.getString(idxSyncState);

      // we are ascending timestamp

      // first entry should match expectation
      if ( (expectFirstRemainingType == FirstSavepointTimestampType.CHECKPOINT_NEW_ROW) &&
          !c.isNull(idxType) ) {
        assertTrue("Expected first row to be a new-row checkpoint: " + identifyingDescription,
            false);
      }
      if ( (expectFirstRemainingType != FirstSavepointTimestampType.CHECKPOINT_NEW_ROW) &&
          c.isNull(idxType) ) {
        assertTrue("Expected first row to not be a checkpoint: " + identifyingDescription,
            false);
      }

      if ( expectFirstRemainingType == FirstSavepointTimestampType.CHECKPOINT_NEW_ROW ||
           expectFirstRemainingType == FirstSavepointTimestampType.NEW_ROW ) {
        if ( !syncState.equals(SyncState.new_row.name()) ) {
          assertEquals("Expected first row to be a new_row sync state: " + identifyingDescription,
              SyncState.new_row.name(), syncState);
        }
      }

      if ( expectFirstRemainingType == FirstSavepointTimestampType.CHANGED ) {
        if ( !syncState.equals(SyncState.changed.name()) ) {
          assertEquals("Expected first row to be a changed sync state: " + identifyingDescription,
              SyncState.changed.name(), syncState);
        }
      }

      if ( expectFirstRemainingType == FirstSavepointTimestampType.SYNCED ) {
        if ( !syncState.equals(SyncState.synced.name()) ) {
          assertEquals("Expected first row to be a synced sync state: " + identifyingDescription,
              SyncState.synced.name(), syncState);
        }
        // subsequent checkpoints we expect to be tagged as changed
        syncState = SyncState.changed.name();
      }

      if ( expectFirstRemainingType == FirstSavepointTimestampType.SYNCED_PENDING_FILES ) {
        if ( !syncState.equals(SyncState.synced_pending_files.name()) ) {
          assertEquals("Expected first row to be a synced_pending_files sync state: " + identifyingDescription,
              SyncState.synced_pending_files.name(), syncState);
        }
        // subsequent checkpoints we expect to be tagged as changed
        syncState = SyncState.changed.name();
      }

      if ( expectFirstRemainingType == FirstSavepointTimestampType.IN_CONFLICT ) {
        if ( !syncState.equals(SyncState.in_conflict.name()) ) {
          assertEquals("Expected first row to be a in_conflict sync state: " + identifyingDescription,
              SyncState.in_conflict.name(), syncState);
        }
        // subsequent entries are also in_conflict
        syncState = SyncState.in_conflict.name();
      }

      if ( expectFirstRemainingType == FirstSavepointTimestampType.DELETED ) {
        if ( !syncState.equals(SyncState.deleted.name()) ) {
          assertEquals("Expected first row to be a deleted sync state: " + identifyingDescription,
              SyncState.deleted.name(), syncState);
        }
      }

      if ( type != null ) {
        assertEquals("Expected first row to have filter type: " + identifyingDescription,
            type.name(), c.getString(idxFilterType));
      }

      // subsequent updates should all be checkpoints
      while ( c.moveToNext() ) {
        if ( !syncState.equals(SyncState.in_conflict.name()) && !c.isNull(idxType) ) {
          assertTrue("Expected subsequent rows to be checkpoints: " + identifyingDescription,
              false);
          return false;
        }
        if ( syncState.equals(SyncState.in_conflict.name()) && c.isNull(idxType) ) {
          assertTrue("Expected subsequent rows to not be checkpoints: " + identifyingDescription,
              false);
          return false;
        }
        assertEquals("Expected subsequent rows to be " + syncState + " sync state: " +
            identifyingDescription,
            syncState, c.getString(idxSyncState));

        if ( type != null ) {
          assertEquals("Expected subsequent rows to have filter type: " + identifyingDescription,
              type.name(), c.getString(idxFilterType));
        }
      }

      return true;
    } catch (SQLiteException e) {
      assertFalse("shouldn't get an error", true);
      return false;
    } finally {
      if ( c != null && !c.isClosed() ) {
        c.close();
      }
    }
  }

  protected void verifySyncOutcome(String tableId, OrderedColumns oc, boolean
      asPrivilegedUser, RowFilterScope.Type type, SyncParamOutcome spo) {

    String characterizer;
    {
      StringBuilder b = new StringBuilder();
      b.append(asPrivilegedUser ? "privlged |" : "ordinary |")
          .append(spo.isServerRowDeleted ? "deletedOnServer |" : "changedOnServer |")
          .append(spo.changeServerBoolean ? "B" : "-").append(spo.changeServerInteger ? "I" : "-")
          .append(spo.changeServerNumber ? "D" : "-").append(spo.changeServerString ? "S" : "-")
          .append(spo.changeServerRowPath ? "R" : "-")
          .append(spo.changeServerFormIdMetadata ? "F" : "-")
          .append(spo.changeServerPrivilegedMetadata ? "P" : "-");
      b.append(" @ ").append(spo.rowId);
      characterizer = b.toString();
    }

    Log.i(tableId, "processing: " + characterizer);

    BaseTable original = ODKDatabaseImplUtils.get().privilegedGetRowsWithId(db, tableId, spo.rowId,
        commonUser);

    // and now handle server row changes.
    // this may be deletes with or without changes to any values or filter scopes
    ContentValues cvValues = buildServerRowContent(tableId, spo.rowId,
        spo.isServerRowDeleted, type,
        new Boolean[] { spo.changeServerBoolean, spo.changeServerInteger, spo.changeServerNumber,
            spo.changeServerString, spo.changeServerRowPath, spo.changeServerFormIdMetadata,
            spo.changeServerPrivilegedMetadata });

    ODKDatabaseImplUtils.get().privilegedPerhapsPlaceRowIntoConflictWithId(db, tableId, oc,
        cvValues, spo.rowId, commonUser,
        (asPrivilegedUser ? RoleConsts.ADMIN_ROLES_LIST : RoleConsts.USER_ROLES_LIST),
        currentLocale);

    BaseTable baseTable = ODKDatabaseImplUtils.get().privilegedGetRowsWithId(db, tableId,
        spo.rowId, commonUser);

    switch ( spo.outcome ) {
    case LOCALLY_DELETED:
      assertEquals("row not deleted: " + characterizer, 0, baseTable.getNumberOfRows());
      break;
    case LOCALLY_SYNCED:
    case LOCALLY_SYNCED_PENDING_FILES:
      assertEquals("row not resolved: " + characterizer, 1, baseTable.getNumberOfRows());
      // content should match server cvValues
      for ( String key : cvValues.keySet() ) {
        if ( key.equals(DataTableColumns.SYNC_STATE) ) {
          String localValue = baseTable.getRowAtIndex(0).getDataByKey(key);
          if ( spo.outcome == ServerChangeOutcome.LOCALLY_SYNCED ) {
            assertEquals("syncState not synced: " + characterizer,
                SyncState.synced.name(), localValue);
          } else {
            assertEquals("syncState not synced_pending_files: " + characterizer,
                SyncState.synced_pending_files.name(), localValue );
          }
        } else {
          String localValue = baseTable.getRowAtIndex(0).getDataByKey(key);
          String serverValue = cvValues.getAsString(key);

          ElementDataType dt = ElementDataType.string;
          try {
            ColumnDefinition cd = oc.find(key);
            dt = cd.getType().getDataType();
          } catch ( IllegalArgumentException e ) {
            // ignore
          }
          if ( dt == ElementDataType.bool ) {
            serverValue = Integer.toString(DataHelper.boolToInt(cvValues.getAsBoolean(key)));
          }
          assertTrue("column " + key + " not identical (" + localValue + " != " + serverValue
              + " ): " + characterizer,
              ODKDatabaseImplUtils.get().identicalValue(localValue, serverValue, dt) );
        }
      }
      break;
    case LOCALLY_IN_CONFLICT:
      assertEquals(2, baseTable.getNumberOfRows());
      Row localRow = null;
      Row serverRow = null;
      for ( int i = 0 ; i < 2 ; ++i ) {
        Row r = baseTable.getRowAtIndex(i);
        assertEquals("syncState not in_conflict: " + characterizer, SyncState.in_conflict.name(),
            r.getDataByKey(DataTableColumns.SYNC_STATE));
        String conflictType = r.getDataByKey(DataTableColumns.CONFLICT_TYPE);
        Integer ct = Integer.valueOf(conflictType);
        if ( ct == ConflictType.LOCAL_DELETED_OLD_VALUES || ct == ConflictType
            .LOCAL_UPDATED_UPDATED_VALUES ) {
          localRow = r;
        }
        if ( ct == ConflictType.SERVER_DELETED_OLD_VALUES || ct == ConflictType
            .SERVER_UPDATED_UPDATED_VALUES ) {
          serverRow = r;
        }
      }
      assertNotNull("did not find local conflict: " + characterizer, localRow);
      assertNotNull("did not find server conflict: " + characterizer, serverRow);

      // verify serverRow content
      // content should match server cvValues
      for ( String key : cvValues.keySet() ) {
        if ( key.equals(DataTableColumns.SYNC_STATE) ||
            key.equals(DataTableColumns.CONFLICT_TYPE)) {
        } else {
          String localValue = serverRow.getDataByKey(key);
          String serverValue = cvValues.getAsString(key);

          ElementDataType dt = ElementDataType.string;
          try {
            ColumnDefinition cd = oc.find(key);
            dt = cd.getType().getDataType();
          } catch ( IllegalArgumentException e ) {
            // ignore
          }
          if ( dt == ElementDataType.bool ) {
            serverValue = Integer.toString(DataHelper.boolToInt(cvValues.getAsBoolean(key)));
          }
          assertTrue("column " + key + " not identical (" + localValue + " != " + serverValue
                  + " ): " + characterizer,
              ODKDatabaseImplUtils.get().identicalValue(localValue, serverValue, dt) );
        }
      }

      // verify localRow content
      // content should match original local row
      for ( String key : cvValues.keySet() ) {
        if ( key.equals(DataTableColumns.SYNC_STATE) ||
            key.equals(DataTableColumns.CONFLICT_TYPE)) {
        } else {
          String localValue = localRow.getDataByKey(key);
          String originalValue = original.getRowAtIndex(0).getDataByKey(key);

          ElementDataType dt = ElementDataType.string;
          try {
            ColumnDefinition cd = oc.find(key);
            dt = cd.getType().getDataType();
          } catch ( IllegalArgumentException e ) {
            // ignore
          }
          assertTrue("column " + key + " not identical (" + localValue + " != " + originalValue
                  + " ): " + characterizer,
              ODKDatabaseImplUtils.get().identicalValue(localValue, originalValue, dt) );
        }
      }

      break;
    }
  }

  protected ArrayList<SyncParamOutcome> buildSyncParamOutcomesList(boolean privilegedUser,
      SyncState localRowSyncState,
      boolean isServerRowDeleted,
      boolean changeServerBoolean, boolean changeServerInteger,
      boolean changeServerNumber, boolean changeServerString,
      boolean changeServerRowPath, boolean changeServerFormIdMetadata,
      boolean changeServerPrivilegedMetadata) {

    ArrayList<SyncParamOutcome> cases = new ArrayList<SyncParamOutcome>();
    String[] rowIds = {  rowIdDefaultNull,
        rowIdDefaultCommon, rowIdHiddenCommon, rowIdReadOnlyCommon, rowIdModifyCommon };

    for ( String rowId : rowIds ) {

      ServerChangeOutcome sco;
      if ( isServerRowDeleted ) {
        switch (localRowSyncState) {
        case deleted:
        case synced:
        case synced_pending_files:
          sco = ServerChangeOutcome.LOCALLY_DELETED;
          break;
        case new_row:
        case changed:
          sco = ServerChangeOutcome.LOCALLY_IN_CONFLICT;
          break;
        default:
          throw new IllegalStateException("unhandled sync state");
        }
      } else {
        switch (localRowSyncState) {
        case deleted:
          sco = ServerChangeOutcome.LOCALLY_IN_CONFLICT;
          break;
        case synced:
          if ( !changeServerRowPath ) {
            sco = ServerChangeOutcome.LOCALLY_SYNCED;
          } else {
            sco = ServerChangeOutcome.LOCALLY_SYNCED_PENDING_FILES;
          }
          break;
        case synced_pending_files:
          sco = ServerChangeOutcome.LOCALLY_SYNCED_PENDING_FILES;
          break;
        case new_row:
        case changed:
          if ( changeServerBoolean || changeServerInteger ||
               changeServerNumber || changeServerString ||
               changeServerRowPath ) {
            sco = ServerChangeOutcome.LOCALLY_IN_CONFLICT;
          } else if ( changeServerPrivilegedMetadata && privilegedUser ) {
            sco = ServerChangeOutcome.LOCALLY_IN_CONFLICT;
          } else {
            // user has no say in metadata -- take server's version for those and resolve
            sco = ServerChangeOutcome.LOCALLY_SYNCED;
          }
          break;
        default:
          throw new IllegalStateException("Unexpected sync state");
        }
      }
      SyncParamOutcome spo = new SyncParamOutcome(rowId, isServerRowDeleted, changeServerBoolean,
          changeServerInteger, changeServerNumber, changeServerString, changeServerRowPath,
          changeServerFormIdMetadata, changeServerPrivilegedMetadata, sco);
      cases.add(spo);
    }
    return cases;
  }

  protected ArrayList<SyncParamOutcome> buildConflictingSyncParamOutcomesList(boolean
      privilegedUser,
      int localConflictType,
      boolean isServerRowDeleted,
      boolean changeServerBoolean, boolean changeServerInteger,
      boolean changeServerNumber, boolean changeServerString,
      boolean changeServerRowPath, boolean changeServerFormIdMetadata,
      boolean changeServerPrivilegedMetadata) {

    ArrayList<SyncParamOutcome> cases = new ArrayList<SyncParamOutcome>();
    String[] rowIds = {  rowIdDefaultNull,
        rowIdDefaultCommon, rowIdHiddenCommon, rowIdReadOnlyCommon, rowIdModifyCommon };

    for ( String rowId : rowIds ) {

      ServerChangeOutcome sco;
      if ( isServerRowDeleted ) {
        switch (localConflictType) {
        case ConflictType.LOCAL_DELETED_OLD_VALUES:
          sco = ServerChangeOutcome.LOCALLY_DELETED;
          break;
        case ConflictType.LOCAL_UPDATED_UPDATED_VALUES:
          sco = ServerChangeOutcome.LOCALLY_IN_CONFLICT;
          break;
        default:
          throw new IllegalStateException("unhandled local conflict type");
        }
      } else {
        switch (localConflictType) {
        case ConflictType.LOCAL_DELETED_OLD_VALUES:
          sco = ServerChangeOutcome.LOCALLY_IN_CONFLICT;
          break;
        case ConflictType.LOCAL_UPDATED_UPDATED_VALUES:
          if ( changeServerBoolean || changeServerInteger ||
              changeServerNumber || changeServerString ||
              changeServerRowPath ) {
            sco = ServerChangeOutcome.LOCALLY_IN_CONFLICT;
          } else if ( changeServerPrivilegedMetadata && privilegedUser ) {
            sco = ServerChangeOutcome.LOCALLY_IN_CONFLICT;
          } else {
            // user has no say in metadata -- take server's version for those and resolve
            sco = ServerChangeOutcome.LOCALLY_SYNCED;
          }
          break;
        default:
          throw new IllegalStateException("unhandled local conflict type");
        }
      }
      SyncParamOutcome spo = new SyncParamOutcome(rowId, isServerRowDeleted, changeServerBoolean,
          changeServerInteger, changeServerNumber, changeServerString, changeServerRowPath,
          changeServerFormIdMetadata, changeServerPrivilegedMetadata, sco);
      cases.add(spo);
    }
    return cases;
  }

  protected ArrayList<AuthParamAndOutcome> buildOutcomesListResolveTakeServer(String tableId) {

    ArrayList<AuthParamAndOutcome> cases = new ArrayList<AuthParamAndOutcome>();

    int localConflict;
    int serverConflict;
    for ( localConflict = ConflictType.LOCAL_DELETED_OLD_VALUES;
          localConflict < ConflictType.SERVER_DELETED_OLD_VALUES ; ++ localConflict ) {

      for (serverConflict = ConflictType.SERVER_DELETED_OLD_VALUES;
           serverConflict <= ConflictType.SERVER_UPDATED_UPDATED_VALUES; ++serverConflict) {

        if ( localConflict == ConflictType.LOCAL_DELETED_OLD_VALUES &&
            serverConflict == ConflictType.SERVER_DELETED_OLD_VALUES ) {
          // automatically deleted when syncing
          continue;
        }

        // anon user can't modify hidden or read-only entries
        cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultNull + localConflict + serverConflict,
            anonymousUser, RoleConsts.ANONYMOUS_ROLES_LIST, false));
        cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommon + localConflict + serverConflict,
            anonymousUser, RoleConsts.ANONYMOUS_ROLES_LIST, false));
        cases.add(new AuthParamAndOutcome(tableId, rowIdHiddenCommon + localConflict + serverConflict,
            anonymousUser, RoleConsts.ANONYMOUS_ROLES_LIST, false));
        cases.add(new AuthParamAndOutcome(tableId, rowIdReadOnlyCommon + localConflict + serverConflict,
            anonymousUser, RoleConsts.ANONYMOUS_ROLES_LIST, false));
        cases.add(new AuthParamAndOutcome(tableId, rowIdModifyCommon + localConflict + serverConflict,
            anonymousUser, RoleConsts.ANONYMOUS_ROLES_LIST, false));
        // matching filter value user can do anything
        cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultNull + localConflict + serverConflict,
            commonUser, RoleConsts.USER_ROLES_LIST, false));
        cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommon + localConflict + serverConflict,
            commonUser, RoleConsts.USER_ROLES_LIST, false));
        cases.add(new AuthParamAndOutcome(tableId, rowIdHiddenCommon + localConflict + serverConflict,
            commonUser, RoleConsts.USER_ROLES_LIST, false));
        cases.add(new AuthParamAndOutcome(tableId, rowIdReadOnlyCommon + localConflict + serverConflict,
            commonUser, RoleConsts.USER_ROLES_LIST, false));
        cases.add(new AuthParamAndOutcome(tableId, rowIdModifyCommon + localConflict + serverConflict,
            commonUser, RoleConsts.USER_ROLES_LIST, false));
        // non-matching filter value user can't modify hidden or read-only entries
        cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultNull + localConflict + serverConflict,
            otherUser, RoleConsts.USER_ROLES_LIST, false));
        cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommon + localConflict + serverConflict,
            otherUser, RoleConsts.USER_ROLES_LIST, false));
        cases.add(new AuthParamAndOutcome(tableId, rowIdHiddenCommon + localConflict + serverConflict,
            otherUser, RoleConsts.USER_ROLES_LIST, false));
        cases.add(new AuthParamAndOutcome(tableId, rowIdReadOnlyCommon + localConflict + serverConflict,
            otherUser, RoleConsts.USER_ROLES_LIST, false));
        cases.add(new AuthParamAndOutcome(tableId, rowIdModifyCommon + localConflict + serverConflict,
            otherUser, RoleConsts.USER_ROLES_LIST, false));
        // super-user can do anything
        cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultNull + localConflict + serverConflict,
            superUser, RoleConsts.SUPER_USER_ROLES_LIST, false));
        cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommon + localConflict + serverConflict,
            superUser, RoleConsts.SUPER_USER_ROLES_LIST, false));
        cases.add(new AuthParamAndOutcome(tableId, rowIdHiddenCommon + localConflict + serverConflict,
            superUser, RoleConsts.SUPER_USER_ROLES_LIST, false));
        cases.add(new AuthParamAndOutcome(tableId, rowIdReadOnlyCommon + localConflict + serverConflict,
            superUser, RoleConsts.SUPER_USER_ROLES_LIST, false));
        cases.add(new AuthParamAndOutcome(tableId, rowIdModifyCommon + localConflict + serverConflict,
            superUser, RoleConsts.SUPER_USER_ROLES_LIST, false));
        // admin user can do anything
        cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultNull + localConflict + serverConflict,
            adminUser, RoleConsts.ADMIN_ROLES_LIST, false));
        cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommon + localConflict + serverConflict,
            adminUser, RoleConsts.ADMIN_ROLES_LIST, false));
        cases.add(new AuthParamAndOutcome(tableId, rowIdHiddenCommon + localConflict + serverConflict,
            adminUser, RoleConsts.ADMIN_ROLES_LIST, false));
        cases.add(new AuthParamAndOutcome(tableId, rowIdReadOnlyCommon + localConflict + serverConflict,
            adminUser, RoleConsts.ADMIN_ROLES_LIST, false));
        cases.add(new AuthParamAndOutcome(tableId, rowIdModifyCommon + localConflict + serverConflict,
            adminUser, RoleConsts.ADMIN_ROLES_LIST, false));
      }
    }

    return cases;
  }

  protected ArrayList<AuthParamAndOutcome> buildOutcomesListResolveTakeLocal(String tableId,
      boolean isLocked) {

    ArrayList<AuthParamAndOutcome> cases = new ArrayList<AuthParamAndOutcome>();

    int localConflict;
    int serverConflict;
    for ( localConflict = ConflictType.LOCAL_DELETED_OLD_VALUES;
          localConflict < ConflictType.SERVER_DELETED_OLD_VALUES ; ++ localConflict ) {

      boolean isLocalDelete = localConflict == ConflictType.LOCAL_DELETED_OLD_VALUES;

      // matched users can't delete locally
      boolean isLockedAndIsLocalDelete = isLocked && isLocalDelete;

      for (serverConflict = ConflictType.SERVER_DELETED_OLD_VALUES;
           serverConflict <= ConflictType.SERVER_UPDATED_UPDATED_VALUES; ++serverConflict) {

        if ( localConflict == ConflictType.LOCAL_DELETED_OLD_VALUES &&
             serverConflict == ConflictType.SERVER_DELETED_OLD_VALUES ) {
          // automatically deleted when syncing
          continue;
        }

        // anon user can't modify hidden or read-only entries
        // only modify if not local change delete
        // and can't do anything if locked table.
        cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultNull + localConflict + serverConflict,
            anonymousUser, RoleConsts.ANONYMOUS_ROLES_LIST, isLocked));
        cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommon + localConflict + serverConflict,
            anonymousUser, RoleConsts.ANONYMOUS_ROLES_LIST, isLocked));
        cases.add(new AuthParamAndOutcome(tableId, rowIdHiddenCommon + localConflict + serverConflict,
            anonymousUser, RoleConsts.ANONYMOUS_ROLES_LIST, true));
        cases.add(new AuthParamAndOutcome(tableId, rowIdReadOnlyCommon + localConflict + serverConflict,
            anonymousUser, RoleConsts.ANONYMOUS_ROLES_LIST, true));
        cases.add(new AuthParamAndOutcome(tableId, rowIdModifyCommon + localConflict + serverConflict,
            anonymousUser, RoleConsts.ANONYMOUS_ROLES_LIST, isLocked || isLocalDelete));
        // matching filter value user can do anything
        // except delete in a locked table
        cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultNull + localConflict + serverConflict,
            commonUser, RoleConsts.USER_ROLES_LIST, isLocked));
        cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommon + localConflict + serverConflict,
            commonUser, RoleConsts.USER_ROLES_LIST, isLockedAndIsLocalDelete));
        cases.add(new AuthParamAndOutcome(tableId, rowIdHiddenCommon + localConflict + serverConflict,
            commonUser, RoleConsts.USER_ROLES_LIST, isLockedAndIsLocalDelete));
        cases.add(new AuthParamAndOutcome(tableId, rowIdReadOnlyCommon + localConflict + serverConflict,
            commonUser, RoleConsts.USER_ROLES_LIST, isLockedAndIsLocalDelete));
        cases.add(new AuthParamAndOutcome(tableId, rowIdModifyCommon + localConflict + serverConflict,
            commonUser, RoleConsts.USER_ROLES_LIST, isLockedAndIsLocalDelete));
        // non-matching filter value user can't modify hidden or read-only entries
        // only modify if not locked and not local change delete
        cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultNull + localConflict + serverConflict,
            otherUser, RoleConsts.USER_ROLES_LIST, isLocked));
        cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommon + localConflict + serverConflict,
            otherUser, RoleConsts.USER_ROLES_LIST, isLocked));
        cases.add(new AuthParamAndOutcome(tableId, rowIdHiddenCommon + localConflict + serverConflict,
            otherUser, RoleConsts.USER_ROLES_LIST, true));
        cases.add(new AuthParamAndOutcome(tableId, rowIdReadOnlyCommon + localConflict + serverConflict,
            otherUser, RoleConsts.USER_ROLES_LIST, true));
        cases.add(new AuthParamAndOutcome(tableId, rowIdModifyCommon + localConflict + serverConflict,
            otherUser, RoleConsts.USER_ROLES_LIST, isLocked || isLocalDelete));
        // super-user can do anything
        cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultNull + localConflict + serverConflict,
            superUser, RoleConsts.SUPER_USER_ROLES_LIST, false));
        cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommon + localConflict + serverConflict,
            superUser, RoleConsts.SUPER_USER_ROLES_LIST, false));
        cases.add(new AuthParamAndOutcome(tableId, rowIdHiddenCommon + localConflict + serverConflict,
            superUser, RoleConsts.SUPER_USER_ROLES_LIST, false));
        cases.add(new AuthParamAndOutcome(tableId, rowIdReadOnlyCommon + localConflict + serverConflict,
            superUser, RoleConsts.SUPER_USER_ROLES_LIST, false));
        cases.add(new AuthParamAndOutcome(tableId, rowIdModifyCommon + localConflict + serverConflict,
            superUser, RoleConsts.SUPER_USER_ROLES_LIST, false));
        // admin user can do anything
        cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultNull + localConflict + serverConflict,
            adminUser, RoleConsts.ADMIN_ROLES_LIST, false));
        cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommon + localConflict + serverConflict,
            adminUser, RoleConsts.ADMIN_ROLES_LIST, false));
        cases.add(new AuthParamAndOutcome(tableId, rowIdHiddenCommon + localConflict + serverConflict,
            adminUser, RoleConsts.ADMIN_ROLES_LIST, false));
        cases.add(new AuthParamAndOutcome(tableId, rowIdReadOnlyCommon + localConflict + serverConflict,
            adminUser, RoleConsts.ADMIN_ROLES_LIST, false));
        cases.add(new AuthParamAndOutcome(tableId, rowIdModifyCommon + localConflict + serverConflict,
            adminUser, RoleConsts.ADMIN_ROLES_LIST, false));
      }
    }

    return cases;
  }

  protected ArrayList<AuthParamAndOutcome> buildOutcomesListUpdateUnlockedNoAnonCreate() {
    String tableId = testTableUnlockedNoAnonCreate;

    ArrayList<AuthParamAndOutcome> cases = new ArrayList<AuthParamAndOutcome>();
    // anon user can't modify hidden or read-only entries
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultNull,
        anonymousUser, RoleConsts.ANONYMOUS_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommonNew,
        anonymousUser, RoleConsts.ANONYMOUS_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommon,
        anonymousUser, RoleConsts.ANONYMOUS_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdHiddenCommon,
        anonymousUser, RoleConsts.ANONYMOUS_ROLES_LIST, true));
    cases.add(new AuthParamAndOutcome(tableId, rowIdReadOnlyCommon,
        anonymousUser, RoleConsts.ANONYMOUS_ROLES_LIST, true));
    cases.add(new AuthParamAndOutcome(tableId, rowIdModifyCommon,
        anonymousUser, RoleConsts.ANONYMOUS_ROLES_LIST, false));
    // matching filter value user can do anything
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultNull,
        commonUser, RoleConsts.USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommonNew,
        commonUser, RoleConsts.USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommon,
        commonUser, RoleConsts.USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdHiddenCommon,
        commonUser, RoleConsts.USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdReadOnlyCommon,
        commonUser, RoleConsts.USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdModifyCommon,
        commonUser, RoleConsts.USER_ROLES_LIST, false));
    // non-matching filter value user can't modify hidden or read-only entries
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultNull,
        otherUser, RoleConsts.USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommonNew,
        otherUser, RoleConsts.USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommon,
        otherUser, RoleConsts.USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdHiddenCommon,
        otherUser, RoleConsts.USER_ROLES_LIST, true));
    cases.add(new AuthParamAndOutcome(tableId, rowIdReadOnlyCommon,
        otherUser, RoleConsts.USER_ROLES_LIST, true));
    cases.add(new AuthParamAndOutcome(tableId, rowIdModifyCommon,
        otherUser, RoleConsts.USER_ROLES_LIST, false));
    // super-user can do anything
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultNull,
        superUser, RoleConsts.SUPER_USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommonNew,
        superUser, RoleConsts.SUPER_USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommon,
        superUser, RoleConsts.SUPER_USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdHiddenCommon,
        superUser, RoleConsts.SUPER_USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdReadOnlyCommon,
        superUser, RoleConsts.SUPER_USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdModifyCommon,
        superUser, RoleConsts.SUPER_USER_ROLES_LIST, false));
    // admin user can do anything
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultNull,
        adminUser, RoleConsts.ADMIN_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommonNew,
        adminUser, RoleConsts.ADMIN_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommon,
        adminUser, RoleConsts.ADMIN_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdHiddenCommon,
        adminUser, RoleConsts.ADMIN_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdReadOnlyCommon,
        adminUser, RoleConsts.ADMIN_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdModifyCommon,
        adminUser, RoleConsts.ADMIN_ROLES_LIST, false));

    return cases;
  }

  protected ArrayList<AuthParamAndOutcome> buildOutcomesListInsertUnlockedNoAnonCreate() {
    String tableId = testTableUnlockedNoAnonCreate;

    ArrayList<AuthParamAndOutcome> cases = new ArrayList<AuthParamAndOutcome>();
    // anon user can't insert
    cases.add(new AuthParamAndOutcome(tableId, rowIdInserted,
        anonymousUser, RoleConsts.ANONYMOUS_ROLES_LIST, true));
    // user can do anything
    cases.add(new AuthParamAndOutcome(tableId, rowIdInserted,
        commonUser, RoleConsts.USER_ROLES_LIST, false));
    // other user can do anything
    cases.add(new AuthParamAndOutcome(tableId, rowIdInserted,
        otherUser, RoleConsts.USER_ROLES_LIST, false));
    // super-user can do anything
    cases.add(new AuthParamAndOutcome(tableId, rowIdInserted,
        superUser, RoleConsts.SUPER_USER_ROLES_LIST, false));
    // admin user can do anything
    cases.add(new AuthParamAndOutcome(tableId, rowIdInserted,
        adminUser, RoleConsts.ADMIN_ROLES_LIST, false));

    return cases;
  }

  protected ArrayList<AuthParamAndOutcome> buildOutcomesListDeleteUnlockedNoAnonCreate() {
    String tableId = testTableUnlockedNoAnonCreate;

    ArrayList<AuthParamAndOutcome> cases = new ArrayList<AuthParamAndOutcome>();
    // anon user can't delete, modify, hidden or read-only entries
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultNull,
        anonymousUser, RoleConsts.ANONYMOUS_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommonNew,
        anonymousUser, RoleConsts.ANONYMOUS_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommon,
        anonymousUser, RoleConsts.ANONYMOUS_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdHiddenCommon,
        anonymousUser, RoleConsts.ANONYMOUS_ROLES_LIST, true));
    cases.add(new AuthParamAndOutcome(tableId, rowIdReadOnlyCommon,
        anonymousUser, RoleConsts.ANONYMOUS_ROLES_LIST, true));
    cases.add(new AuthParamAndOutcome(tableId, rowIdModifyCommon,
        anonymousUser, RoleConsts.ANONYMOUS_ROLES_LIST, true));
    // matching filter value user can do anything
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultNull,
        commonUser, RoleConsts.USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommonNew,
        commonUser, RoleConsts.USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommon,
        commonUser, RoleConsts.USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdHiddenCommon,
        commonUser, RoleConsts.USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdReadOnlyCommon,
        commonUser, RoleConsts.USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdModifyCommon,
        commonUser, RoleConsts.USER_ROLES_LIST, false));
    // non-matching filter value user can't delete  modify, hidden or read-only entries
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultNull,
        otherUser, RoleConsts.USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommonNew,
        otherUser, RoleConsts.USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommon,
        otherUser, RoleConsts.USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdHiddenCommon,
        otherUser, RoleConsts.USER_ROLES_LIST, true));
    cases.add(new AuthParamAndOutcome(tableId, rowIdReadOnlyCommon,
        otherUser, RoleConsts.USER_ROLES_LIST, true));
    cases.add(new AuthParamAndOutcome(tableId, rowIdModifyCommon,
        otherUser, RoleConsts.USER_ROLES_LIST, true));
    // super-user can do anything
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultNull,
        superUser, RoleConsts.SUPER_USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommonNew,
        superUser, RoleConsts.SUPER_USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommon,
        superUser, RoleConsts.SUPER_USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdHiddenCommon,
        superUser, RoleConsts.SUPER_USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdReadOnlyCommon,
        superUser, RoleConsts.SUPER_USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdModifyCommon,
        superUser, RoleConsts.SUPER_USER_ROLES_LIST, false));
    // admin user can do anything
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultNull,
        adminUser, RoleConsts.ADMIN_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommonNew,
        adminUser, RoleConsts.ADMIN_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommon,
        adminUser, RoleConsts.ADMIN_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdHiddenCommon,
        adminUser, RoleConsts.ADMIN_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdReadOnlyCommon,
        adminUser, RoleConsts.ADMIN_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdModifyCommon,
        adminUser, RoleConsts.ADMIN_ROLES_LIST, false));


    return cases;
  }

  protected ArrayList<AuthParamAndOutcome> buildOutcomesListUpdateUnlockedYesAnonCreate() {
    String tableId = testTableUnlockedYesAnonCreate;

    ArrayList<AuthParamAndOutcome> cases = new ArrayList<AuthParamAndOutcome>();
    // anon user can't modify hidden or read-only entries
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultNull,
        anonymousUser, RoleConsts.ANONYMOUS_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommonNew,
        anonymousUser, RoleConsts.ANONYMOUS_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommon,
        anonymousUser, RoleConsts.ANONYMOUS_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdHiddenCommon,
        anonymousUser, RoleConsts.ANONYMOUS_ROLES_LIST, true));
    cases.add(new AuthParamAndOutcome(tableId, rowIdReadOnlyCommon,
        anonymousUser, RoleConsts.ANONYMOUS_ROLES_LIST, true));
    cases.add(new AuthParamAndOutcome(tableId, rowIdModifyCommon,
        anonymousUser, RoleConsts.ANONYMOUS_ROLES_LIST, false));
    // matching filter value user can do anything
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultNull,
        commonUser, RoleConsts.USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommonNew,
        commonUser, RoleConsts.USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommon,
        commonUser, RoleConsts.USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdHiddenCommon,
        commonUser, RoleConsts.USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdReadOnlyCommon,
        commonUser, RoleConsts.USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdModifyCommon,
        commonUser, RoleConsts.USER_ROLES_LIST, false));
    // non-matching filter value user can't modify hidden or read-only entries
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultNull,
        otherUser, RoleConsts.USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommonNew,
        otherUser, RoleConsts.USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommon,
        otherUser, RoleConsts.USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdHiddenCommon,
        otherUser, RoleConsts.USER_ROLES_LIST, true));
    cases.add(new AuthParamAndOutcome(tableId, rowIdReadOnlyCommon,
        otherUser, RoleConsts.USER_ROLES_LIST, true));
    cases.add(new AuthParamAndOutcome(tableId, rowIdModifyCommon,
        otherUser, RoleConsts.USER_ROLES_LIST, false));
    // super-user can do anything
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultNull,
        superUser, RoleConsts.SUPER_USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommonNew,
        superUser, RoleConsts.SUPER_USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommon,
        superUser, RoleConsts.SUPER_USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdHiddenCommon,
        superUser, RoleConsts.SUPER_USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdReadOnlyCommon,
        superUser, RoleConsts.SUPER_USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdModifyCommon,
        superUser, RoleConsts.SUPER_USER_ROLES_LIST, false));
    // admin user can do anything
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultNull,
        adminUser, RoleConsts.ADMIN_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommonNew,
        adminUser, RoleConsts.ADMIN_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommon,
        adminUser, RoleConsts.ADMIN_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdHiddenCommon,
        adminUser, RoleConsts.ADMIN_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdReadOnlyCommon,
        adminUser, RoleConsts.ADMIN_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdModifyCommon,
        adminUser, RoleConsts.ADMIN_ROLES_LIST, false));

    return cases;
  }

  protected ArrayList<AuthParamAndOutcome> buildOutcomesListInsertUnlockedYesAnonCreate() {
    String tableId = testTableUnlockedYesAnonCreate;

    ArrayList<AuthParamAndOutcome> cases = new ArrayList<AuthParamAndOutcome>();
    // anon user can insert
    cases.add(new AuthParamAndOutcome(tableId, rowIdInserted,
        anonymousUser, RoleConsts.ANONYMOUS_ROLES_LIST, false));
    // user can do anything
    cases.add(new AuthParamAndOutcome(tableId, rowIdInserted,
        commonUser, RoleConsts.USER_ROLES_LIST, false));
    // other user can do anything
    cases.add(new AuthParamAndOutcome(tableId, rowIdInserted,
        otherUser, RoleConsts.USER_ROLES_LIST, false));
    // super-user can do anything
    cases.add(new AuthParamAndOutcome(tableId, rowIdInserted,
        superUser, RoleConsts.SUPER_USER_ROLES_LIST, false));
    // admin user can do anything
    cases.add(new AuthParamAndOutcome(tableId, rowIdInserted,
        adminUser, RoleConsts.ADMIN_ROLES_LIST, false));

    return cases;
  }

  protected ArrayList<AuthParamAndOutcome> buildOutcomesListDeleteUnlockedYesAnonCreate() {
    String tableId = testTableUnlockedYesAnonCreate;

    ArrayList<AuthParamAndOutcome> cases = new ArrayList<AuthParamAndOutcome>();
    // anon user can't delete modify, hidden or read-only entries
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultNull,
        anonymousUser, RoleConsts.ANONYMOUS_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommonNew,
        anonymousUser, RoleConsts.ANONYMOUS_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommon,
        anonymousUser, RoleConsts.ANONYMOUS_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdHiddenCommon,
        anonymousUser, RoleConsts.ANONYMOUS_ROLES_LIST, true));
    cases.add(new AuthParamAndOutcome(tableId, rowIdReadOnlyCommon,
        anonymousUser, RoleConsts.ANONYMOUS_ROLES_LIST, true));
    cases.add(new AuthParamAndOutcome(tableId, rowIdModifyCommon,
        anonymousUser, RoleConsts.ANONYMOUS_ROLES_LIST, true));
    // matching filter value user can do anything
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultNull,
        commonUser, RoleConsts.USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommonNew,
        commonUser, RoleConsts.USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommon,
        commonUser, RoleConsts.USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdHiddenCommon,
        commonUser, RoleConsts.USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdReadOnlyCommon,
        commonUser, RoleConsts.USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdModifyCommon,
        commonUser, RoleConsts.USER_ROLES_LIST, false));
    // non-matching filter value user can't delete modify, hidden or read-only entries
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultNull,
        otherUser, RoleConsts.USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommonNew,
        otherUser, RoleConsts.USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommon,
        otherUser, RoleConsts.USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdHiddenCommon,
        otherUser, RoleConsts.USER_ROLES_LIST, true));
    cases.add(new AuthParamAndOutcome(tableId, rowIdReadOnlyCommon,
        otherUser, RoleConsts.USER_ROLES_LIST, true));
    cases.add(new AuthParamAndOutcome(tableId, rowIdModifyCommon,
        otherUser, RoleConsts.USER_ROLES_LIST, true));
    // super-user can do anything
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultNull,
        superUser, RoleConsts.SUPER_USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommonNew,
        superUser, RoleConsts.SUPER_USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommon,
        superUser, RoleConsts.SUPER_USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdHiddenCommon,
        superUser, RoleConsts.SUPER_USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdReadOnlyCommon,
        superUser, RoleConsts.SUPER_USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdModifyCommon,
        superUser, RoleConsts.SUPER_USER_ROLES_LIST, false));
    // admin user can do anything
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultNull,
        adminUser, RoleConsts.ADMIN_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommonNew,
        adminUser, RoleConsts.ADMIN_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommon,
        adminUser, RoleConsts.ADMIN_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdHiddenCommon,
        adminUser, RoleConsts.ADMIN_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdReadOnlyCommon,
        adminUser, RoleConsts.ADMIN_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdModifyCommon,
        adminUser, RoleConsts.ADMIN_ROLES_LIST, false));

    return cases;
  }

  protected ArrayList<AuthParamAndOutcome> buildOutcomesListUpdateLockedNoAnonCreate() {
    String tableId = testTableLockedNoAnonCreate;

    ArrayList<AuthParamAndOutcome> cases = new ArrayList<AuthParamAndOutcome>();
    // anon user can't modify anything other than the new row
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultNull,
        anonymousUser, RoleConsts.ANONYMOUS_ROLES_LIST, true));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommonNew,
        anonymousUser, RoleConsts.ANONYMOUS_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommon,
        anonymousUser, RoleConsts.ANONYMOUS_ROLES_LIST, true));
    cases.add(new AuthParamAndOutcome(tableId, rowIdHiddenCommon,
        anonymousUser, RoleConsts.ANONYMOUS_ROLES_LIST, true));
    cases.add(new AuthParamAndOutcome(tableId, rowIdReadOnlyCommon,
        anonymousUser, RoleConsts.ANONYMOUS_ROLES_LIST, true));
    cases.add(new AuthParamAndOutcome(tableId, rowIdModifyCommon,
        anonymousUser, RoleConsts.ANONYMOUS_ROLES_LIST, true));
    // matching filter value user can modify anything (note that first entry does not match)
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultNull,
        commonUser, RoleConsts.USER_ROLES_LIST, true));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommonNew,
        commonUser, RoleConsts.USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommon,
        commonUser, RoleConsts.USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdHiddenCommon,
        commonUser, RoleConsts.USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdReadOnlyCommon,
        commonUser, RoleConsts.USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdModifyCommon,
        commonUser, RoleConsts.USER_ROLES_LIST, false));
    // non-matching filter value user can't modify anything other than the new row
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultNull,
        otherUser, RoleConsts.USER_ROLES_LIST, true));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommonNew,
        otherUser, RoleConsts.USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommon,
        otherUser, RoleConsts.USER_ROLES_LIST, true));
    cases.add(new AuthParamAndOutcome(tableId, rowIdHiddenCommon,
        otherUser, RoleConsts.USER_ROLES_LIST, true));
    cases.add(new AuthParamAndOutcome(tableId, rowIdReadOnlyCommon,
        otherUser, RoleConsts.USER_ROLES_LIST, true));
    cases.add(new AuthParamAndOutcome(tableId, rowIdModifyCommon,
        otherUser, RoleConsts.USER_ROLES_LIST, true));
    // super-user can do anything
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultNull,
        superUser, RoleConsts.SUPER_USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommonNew,
        superUser, RoleConsts.SUPER_USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommon,
        superUser, RoleConsts.SUPER_USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdHiddenCommon,
        superUser, RoleConsts.SUPER_USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdReadOnlyCommon,
        superUser, RoleConsts.SUPER_USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdModifyCommon,
        superUser, RoleConsts.SUPER_USER_ROLES_LIST, false));
    // admin user can do anything
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultNull,
        adminUser, RoleConsts.ADMIN_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommonNew,
        adminUser, RoleConsts.ADMIN_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommon,
        adminUser, RoleConsts.ADMIN_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdHiddenCommon,
        adminUser, RoleConsts.ADMIN_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdReadOnlyCommon,
        adminUser, RoleConsts.ADMIN_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdModifyCommon,
        adminUser, RoleConsts.ADMIN_ROLES_LIST, false));

    return cases;
  }

  protected ArrayList<AuthParamAndOutcome> buildOutcomesListInsertLockedNoAnonCreate() {
    String tableId = testTableLockedNoAnonCreate;

    ArrayList<AuthParamAndOutcome> cases = new ArrayList<AuthParamAndOutcome>();
    // anon user can't insert
    cases.add(new AuthParamAndOutcome(tableId, rowIdInserted,
        anonymousUser, RoleConsts.ANONYMOUS_ROLES_LIST, true));
    // user can't insert
    cases.add(new AuthParamAndOutcome(tableId, rowIdInserted,
        commonUser, RoleConsts.USER_ROLES_LIST, true));
    // other user can't insert
    cases.add(new AuthParamAndOutcome(tableId, rowIdInserted,
        otherUser, RoleConsts.USER_ROLES_LIST, true));
    // super-user can do anything
    cases.add(new AuthParamAndOutcome(tableId, rowIdInserted,
        superUser, RoleConsts.SUPER_USER_ROLES_LIST, false));
    // admin user can do anything
    cases.add(new AuthParamAndOutcome(tableId, rowIdInserted,
        adminUser, RoleConsts.ADMIN_ROLES_LIST, false));

    return cases;
  }

  protected ArrayList<AuthParamAndOutcome> buildOutcomesListDeleteLockedNoAnonCreate() {
    String tableId = testTableLockedNoAnonCreate;

    ArrayList<AuthParamAndOutcome> cases = new ArrayList<AuthParamAndOutcome>();
    // anon user can only delete new row
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultNull,
        anonymousUser, RoleConsts.ANONYMOUS_ROLES_LIST, true));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommonNew,
        anonymousUser, RoleConsts.ANONYMOUS_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommon,
        anonymousUser, RoleConsts.ANONYMOUS_ROLES_LIST, true));
    cases.add(new AuthParamAndOutcome(tableId, rowIdHiddenCommon,
        anonymousUser, RoleConsts.ANONYMOUS_ROLES_LIST, true));
    cases.add(new AuthParamAndOutcome(tableId, rowIdReadOnlyCommon,
        anonymousUser, RoleConsts.ANONYMOUS_ROLES_LIST, true));
    cases.add(new AuthParamAndOutcome(tableId, rowIdModifyCommon,
        anonymousUser, RoleConsts.ANONYMOUS_ROLES_LIST, true));
    // matching filter value user can only delete new row
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultNull,
        commonUser, RoleConsts.USER_ROLES_LIST, true));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommonNew,
        commonUser, RoleConsts.USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommon,
        commonUser, RoleConsts.USER_ROLES_LIST, true));
    cases.add(new AuthParamAndOutcome(tableId, rowIdHiddenCommon,
        commonUser, RoleConsts.USER_ROLES_LIST, true));
    cases.add(new AuthParamAndOutcome(tableId, rowIdReadOnlyCommon,
        commonUser, RoleConsts.USER_ROLES_LIST, true));
    cases.add(new AuthParamAndOutcome(tableId, rowIdModifyCommon,
        commonUser, RoleConsts.USER_ROLES_LIST, true));
    // non-matching filter value user can only delete new row
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultNull,
        otherUser, RoleConsts.USER_ROLES_LIST, true));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommonNew,
        otherUser, RoleConsts.USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommon,
        otherUser, RoleConsts.USER_ROLES_LIST, true));
    cases.add(new AuthParamAndOutcome(tableId, rowIdHiddenCommon,
        otherUser, RoleConsts.USER_ROLES_LIST, true));
    cases.add(new AuthParamAndOutcome(tableId, rowIdReadOnlyCommon,
        otherUser, RoleConsts.USER_ROLES_LIST, true));
    cases.add(new AuthParamAndOutcome(tableId, rowIdModifyCommon,
        otherUser, RoleConsts.USER_ROLES_LIST, true));
    // super-user can do anything
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultNull,
        superUser, RoleConsts.SUPER_USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommonNew,
        superUser, RoleConsts.SUPER_USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommon,
        superUser, RoleConsts.SUPER_USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdHiddenCommon,
        superUser, RoleConsts.SUPER_USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdReadOnlyCommon,
        superUser, RoleConsts.SUPER_USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdModifyCommon,
        superUser, RoleConsts.SUPER_USER_ROLES_LIST, false));
    // admin user can do anything
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultNull,
        adminUser, RoleConsts.ADMIN_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommonNew,
        adminUser, RoleConsts.ADMIN_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommon,
        adminUser, RoleConsts.ADMIN_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdHiddenCommon,
        adminUser, RoleConsts.ADMIN_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdReadOnlyCommon,
        adminUser, RoleConsts.ADMIN_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdModifyCommon,
        adminUser, RoleConsts.ADMIN_ROLES_LIST, false));

    return cases;
  }

  protected ArrayList<AuthParamAndOutcome> buildOutcomesListUpdateLockedYesAnonCreate() {
    String tableId = testTableLockedYesAnonCreate;

    ArrayList<AuthParamAndOutcome> cases = new ArrayList<AuthParamAndOutcome>();
    // anon user can't modify anything other than the new row
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultNull,
        anonymousUser, RoleConsts.ANONYMOUS_ROLES_LIST, true));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommonNew,
        anonymousUser, RoleConsts.ANONYMOUS_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommon,
        anonymousUser, RoleConsts.ANONYMOUS_ROLES_LIST, true));
    cases.add(new AuthParamAndOutcome(tableId, rowIdHiddenCommon,
        anonymousUser, RoleConsts.ANONYMOUS_ROLES_LIST, true));
    cases.add(new AuthParamAndOutcome(tableId, rowIdReadOnlyCommon,
        anonymousUser, RoleConsts.ANONYMOUS_ROLES_LIST, true));
    cases.add(new AuthParamAndOutcome(tableId, rowIdModifyCommon,
        anonymousUser, RoleConsts.ANONYMOUS_ROLES_LIST, true));
    // matching filter value user can modify anything (note that first entry does not match)
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultNull,
        commonUser, RoleConsts.USER_ROLES_LIST, true));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommonNew,
        commonUser, RoleConsts.USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommon,
        commonUser, RoleConsts.USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdHiddenCommon,
        commonUser, RoleConsts.USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdReadOnlyCommon,
        commonUser, RoleConsts.USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdModifyCommon,
        commonUser, RoleConsts.USER_ROLES_LIST, false));
    // non-matching filter value user can't modify anything other than the new row
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultNull,
        otherUser, RoleConsts.USER_ROLES_LIST, true));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommonNew,
        otherUser, RoleConsts.USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommon,
        otherUser, RoleConsts.USER_ROLES_LIST, true));
    cases.add(new AuthParamAndOutcome(tableId, rowIdHiddenCommon,
        otherUser, RoleConsts.USER_ROLES_LIST, true));
    cases.add(new AuthParamAndOutcome(tableId, rowIdReadOnlyCommon,
        otherUser, RoleConsts.USER_ROLES_LIST, true));
    cases.add(new AuthParamAndOutcome(tableId, rowIdModifyCommon,
        otherUser, RoleConsts.USER_ROLES_LIST, true));
    // super-user can do anything
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultNull,
        superUser, RoleConsts.SUPER_USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommonNew,
        superUser, RoleConsts.SUPER_USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommon,
        superUser, RoleConsts.SUPER_USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdHiddenCommon,
        superUser, RoleConsts.SUPER_USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdReadOnlyCommon,
        superUser, RoleConsts.SUPER_USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdModifyCommon,
        superUser, RoleConsts.SUPER_USER_ROLES_LIST, false));
    // admin user can do anything
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultNull,
        adminUser, RoleConsts.ADMIN_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommonNew,
        adminUser, RoleConsts.ADMIN_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommon,
        adminUser, RoleConsts.ADMIN_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdHiddenCommon,
        adminUser, RoleConsts.ADMIN_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdReadOnlyCommon,
        adminUser, RoleConsts.ADMIN_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdModifyCommon,
        adminUser, RoleConsts.ADMIN_ROLES_LIST, false));

    return cases;
  }

  protected ArrayList<AuthParamAndOutcome> buildOutcomesListInsertLockedYesAnonCreate() {
    String tableId = testTableLockedYesAnonCreate;

    ArrayList<AuthParamAndOutcome> cases = new ArrayList<AuthParamAndOutcome>();
    // anon user can't insert -- locking trumps allowing to create
    cases.add(new AuthParamAndOutcome(tableId, rowIdInserted,
        anonymousUser, RoleConsts.ANONYMOUS_ROLES_LIST, true));
    // user can't insert
    cases.add(new AuthParamAndOutcome(tableId, rowIdInserted,
        commonUser, RoleConsts.USER_ROLES_LIST, true));
    // other user can't insert
    cases.add(new AuthParamAndOutcome(tableId, rowIdInserted,
        otherUser, RoleConsts.USER_ROLES_LIST, true));
    // super-user can do anything
    cases.add(new AuthParamAndOutcome(tableId, rowIdInserted,
        superUser, RoleConsts.SUPER_USER_ROLES_LIST, false));
    // admin user can do anything
    cases.add(new AuthParamAndOutcome(tableId, rowIdInserted,
        adminUser, RoleConsts.ADMIN_ROLES_LIST, false));

    return cases;
  }

  protected ArrayList<AuthParamAndOutcome> buildOutcomesListDeleteLockedYesAnonCreate() {
    String tableId = testTableLockedYesAnonCreate;

    ArrayList<AuthParamAndOutcome> cases = new ArrayList<AuthParamAndOutcome>();
    // anon user can't delete anything other than the new row
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultNull,
        anonymousUser, RoleConsts.ANONYMOUS_ROLES_LIST, true));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommonNew,
        anonymousUser, RoleConsts.ANONYMOUS_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommon,
        anonymousUser, RoleConsts.ANONYMOUS_ROLES_LIST, true));
    cases.add(new AuthParamAndOutcome(tableId, rowIdHiddenCommon,
        anonymousUser, RoleConsts.ANONYMOUS_ROLES_LIST, true));
    cases.add(new AuthParamAndOutcome(tableId, rowIdReadOnlyCommon,
        anonymousUser, RoleConsts.ANONYMOUS_ROLES_LIST, true));
    cases.add(new AuthParamAndOutcome(tableId, rowIdModifyCommon,
        anonymousUser, RoleConsts.ANONYMOUS_ROLES_LIST, true));
    // matching filter value user can't delete anything other than the new row
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultNull,
        commonUser, RoleConsts.USER_ROLES_LIST, true));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommonNew,
        commonUser, RoleConsts.USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommon,
        commonUser, RoleConsts.USER_ROLES_LIST, true));
    cases.add(new AuthParamAndOutcome(tableId, rowIdHiddenCommon,
        commonUser, RoleConsts.USER_ROLES_LIST, true));
    cases.add(new AuthParamAndOutcome(tableId, rowIdReadOnlyCommon,
        commonUser, RoleConsts.USER_ROLES_LIST, true));
    cases.add(new AuthParamAndOutcome(tableId, rowIdModifyCommon,
        commonUser, RoleConsts.USER_ROLES_LIST, true));
    // non-matching filter value user can't delete anything other than the new row
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultNull,
        otherUser, RoleConsts.USER_ROLES_LIST, true));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommonNew,
        otherUser, RoleConsts.USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommon,
        otherUser, RoleConsts.USER_ROLES_LIST, true));
    cases.add(new AuthParamAndOutcome(tableId, rowIdHiddenCommon,
        otherUser, RoleConsts.USER_ROLES_LIST, true));
    cases.add(new AuthParamAndOutcome(tableId, rowIdReadOnlyCommon,
        otherUser, RoleConsts.USER_ROLES_LIST, true));
    cases.add(new AuthParamAndOutcome(tableId, rowIdModifyCommon,
        otherUser, RoleConsts.USER_ROLES_LIST, true));
    // super-user can do anything
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultNull,
        superUser, RoleConsts.SUPER_USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommonNew,
        superUser, RoleConsts.SUPER_USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommon,
        superUser, RoleConsts.SUPER_USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdHiddenCommon,
        superUser, RoleConsts.SUPER_USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdReadOnlyCommon,
        superUser, RoleConsts.SUPER_USER_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdModifyCommon,
        superUser, RoleConsts.SUPER_USER_ROLES_LIST, false));
    // admin user can do anything
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultNull,
        adminUser, RoleConsts.ADMIN_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommonNew,
        adminUser, RoleConsts.ADMIN_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdDefaultCommon,
        adminUser, RoleConsts.ADMIN_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdHiddenCommon,
        adminUser, RoleConsts.ADMIN_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdReadOnlyCommon,
        adminUser, RoleConsts.ADMIN_ROLES_LIST, false));
    cases.add(new AuthParamAndOutcome(tableId, rowIdModifyCommon,
        adminUser, RoleConsts.ADMIN_ROLES_LIST, false));

    return cases;
  }
}
