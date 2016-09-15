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

package org.opendatakit.common.android.utilities.test;

import android.content.ContentValues;
import android.database.Cursor;
import android.test.AndroidTestCase;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.opendatakit.common.android.database.RoleConsts;
import org.opendatakit.TestConsts;
import org.opendatakit.aggregate.odktables.rest.ConflictType;
import org.opendatakit.aggregate.odktables.rest.ElementDataType;
import org.opendatakit.aggregate.odktables.rest.KeyValueStoreConstants;
import org.opendatakit.aggregate.odktables.rest.SavepointTypeManipulator;
import org.opendatakit.aggregate.odktables.rest.SyncState;
import org.opendatakit.aggregate.odktables.rest.TableConstants;
import org.opendatakit.aggregate.odktables.rest.entity.Column;
import org.opendatakit.aggregate.odktables.rest.entity.RowFilterScope;
import org.opendatakit.common.android.database.data.OrderedColumns;
import org.opendatakit.common.android.database.AndroidConnectFactory;
import org.opendatakit.common.android.database.OdkConnectionFactorySingleton;
import org.opendatakit.common.android.database.OdkConnectionInterface;
import org.opendatakit.common.android.exception.ActionNotAuthorizedException;
import org.opendatakit.common.android.provider.DataTableColumns;
import org.opendatakit.common.android.database.utilities.KeyValueStoreUtils;
import org.opendatakit.common.android.database.LocalKeyValueStoreConstants;
import org.opendatakit.common.android.database.utilities.ODKDatabaseImplUtils;
import org.opendatakit.common.android.utilities.ODKFileUtils;
import org.opendatakit.common.android.database.data.KeyValueStoreEntry;
import org.opendatakit.common.android.database.service.DbHandle;
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

  protected static final String commonUser = "mailto:common@gmail.com";
  protected static final String otherUser = "mailto:other@gmail.com";
  protected static final String superUser = "mailto:super@gmail.com";
  protected static final String adminUser = "mailto:admin@gmail.com";
  protected static final String anonymousUser = "anonymous";

  protected static boolean initialized = false;
  protected static final String APPNAME = TestConsts.APPNAME;
  protected static final DbHandle uniqueKey = new DbHandle(AbstractPermissionsTestCase.class.getSimpleName() + AndroidConnectFactory.INTERNAL_TYPE_SUFFIX);

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
        ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, id);
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
      ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, id);
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
          .createOrOpenDBTableWithColumnsAndProperties(db, tableId, columns, metaData, true);
    } catch (JsonProcessingException e) {
      e.printStackTrace();
      throw new IllegalStateException("Unable to create table with properties");
    }
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
    cvValues.put("col3", 3.3); // number;
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
    SYNCED_PENDING_FILES, IN_CONFLICT, DELETED };

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
