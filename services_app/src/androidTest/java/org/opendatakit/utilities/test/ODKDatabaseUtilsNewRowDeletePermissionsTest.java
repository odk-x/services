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

import org.opendatakit.aggregate.odktables.rest.entity.RowFilterScope;
import org.opendatakit.database.data.OrderedColumns;
import org.opendatakit.exception.ActionNotAuthorizedException;
import org.opendatakit.services.database.utlities.ODKDatabaseImplUtils;

import java.util.ArrayList;

/**
 * Permissions tests in the database.
 */
public class ODKDatabaseUtilsNewRowDeletePermissionsTest extends AbstractPermissionsTestCase {

  private static final String TAG = "ODKDatabaseUtilsNewRowDeletePermissionsTest";

  private void baseDelete_Type_UnlockedNoAnonCreate0(RowFilterScope.Type type) throws
      ActionNotAuthorizedException {

    String tableId = testTableUnlockedNoAnonCreate;
    OrderedColumns oc = assertEmptyTestTable(testTableUnlockedNoAnonCreate,
        false, false, type.name());

    ArrayList<AuthParamAndOutcome> cases = buildOutcomesListDeleteUnlockedNoAnonCreate();

    for ( AuthParamAndOutcome ap : cases ) {
      if ( !ap.rowId.contains("New") ) {
        continue;
      }
      insertRowWIthAdminRights(ap, oc);

      try {
        // expect one row
        verifyRowSyncStateFilterTypeAndCheckpoints(ap.tableId, ap.rowId, 1, FirstSavepointTimestampType
                .NEW_ROW, type,
            ap.toString());

        ODKDatabaseImplUtils.get()
            .deleteRowWithId(db, ap.tableId, ap.rowId, ap.username, ap.roles);
        assertFalse("Expecting to throw an error: " + ap.toString(), ap.throwsAccessException);

        assertTrue("Expected no rows to remain: " + ap.toString(),
            verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 0, FirstSavepointTimestampType.NEW_ROW,
                ap.toString()));

      } catch ( ActionNotAuthorizedException ex ) {
        assertTrue("Expecting to not throw an error: " + ap.toString(), ap.throwsAccessException);
      }
    }
  }

  public void testDelete_DEFAULT_UnlockedNoAnonCreate0() throws ActionNotAuthorizedException {

    baseDelete_Type_UnlockedNoAnonCreate0(RowFilterScope.Type.DEFAULT);
  }

  public void testDelete_HIDDEN_UnlockedNoAnonCreate0() throws ActionNotAuthorizedException {

    baseDelete_Type_UnlockedNoAnonCreate0(RowFilterScope.Type.HIDDEN);
  }

  public void testDelete_READ_ONLY_UnlockedNoAnonCreate0() throws ActionNotAuthorizedException {

    baseDelete_Type_UnlockedNoAnonCreate0(RowFilterScope.Type.READ_ONLY);
  }

  public void testDelete_MODIFY_UnlockedNoAnonCreate0() throws ActionNotAuthorizedException {

    baseDelete_Type_UnlockedNoAnonCreate0(RowFilterScope.Type.MODIFY);
  }

  private void baseDelete_Type_UnlockedYesAnonCreate0(RowFilterScope.Type type) throws
      ActionNotAuthorizedException {

    String tableId = testTableUnlockedYesAnonCreate;
    OrderedColumns oc = assertEmptyTestTable(testTableUnlockedYesAnonCreate,
        false, true, type.name());

    ArrayList<AuthParamAndOutcome> cases = buildOutcomesListDeleteUnlockedYesAnonCreate();

    for ( AuthParamAndOutcome ap : cases ) {
      if ( !ap.rowId.contains("New") ) {
        continue;
      }
      insertRowWIthAdminRights(ap, oc);

      try {
        // expect one row
        verifyRowSyncStateFilterTypeAndCheckpoints(ap.tableId, ap.rowId, 1, FirstSavepointTimestampType
                .NEW_ROW, type,
            ap.toString());

        ODKDatabaseImplUtils.get()
            .deleteRowWithId(db, ap.tableId, ap.rowId, ap.username, ap.roles);
        assertFalse("Expecting to throw an error: " + ap.toString(), ap.throwsAccessException);

        assertTrue("Expected no rows to remain: " + ap.toString(),
            verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 0, FirstSavepointTimestampType.NEW_ROW,
                ap.toString()));

      } catch ( ActionNotAuthorizedException ex ) {
        assertTrue("Expecting to not throw an error: " + ap.toString(), ap.throwsAccessException);
      }
    }
  }

  public void testDelete_DEFAULT_UnlockedYesAnonCreate0() throws ActionNotAuthorizedException {

    baseDelete_Type_UnlockedYesAnonCreate0(RowFilterScope.Type.DEFAULT);
  }

  public void testDelete_HIDDEN_UnlockedYesAnonCreate0() throws ActionNotAuthorizedException {

    baseDelete_Type_UnlockedYesAnonCreate0(RowFilterScope.Type.HIDDEN);
  }

  public void testDelete_READ_ONLY_UnlockedYesAnonCreate0() throws ActionNotAuthorizedException {

    baseDelete_Type_UnlockedYesAnonCreate0(RowFilterScope.Type.READ_ONLY);
  }

  public void testDelete_MODIFY_UnlockedYesAnonCreate0() throws ActionNotAuthorizedException {

    baseDelete_Type_UnlockedYesAnonCreate0(RowFilterScope.Type.MODIFY);
  }

  private void baseDelete_Type_LockedNoAnonCreate0(RowFilterScope.Type type) throws
      ActionNotAuthorizedException {

    String tableId = testTableLockedNoAnonCreate;
    OrderedColumns oc = assertEmptyTestTable(testTableLockedNoAnonCreate,
        true, false, type.name());

    ArrayList<AuthParamAndOutcome> cases = buildOutcomesListDeleteLockedNoAnonCreate();

    for ( AuthParamAndOutcome ap : cases ) {
      if ( !ap.rowId.contains("New") ) {
        continue;
      }
      insertRowWIthAdminRights(ap, oc);

      try {
        // expect one row
        verifyRowSyncStateFilterTypeAndCheckpoints(ap.tableId, ap.rowId, 1, FirstSavepointTimestampType
            .NEW_ROW, type,
            ap.toString());

        ODKDatabaseImplUtils.get()
            .deleteRowWithId(db, ap.tableId, ap.rowId, ap.username, ap.roles);
        assertFalse("Expecting to throw an error: " + ap.toString(), ap.throwsAccessException);

        assertTrue("Expected no rows to remain: " + ap.toString(),
            verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 0, FirstSavepointTimestampType.NEW_ROW,
                ap.toString()));

      } catch ( ActionNotAuthorizedException ex ) {
        assertTrue("Expecting to not throw an error: " + ap.toString(), ap.throwsAccessException);
      }
    }
  }

  public void testDelete_DEFAULT_LockedNoAnonCreate0() throws ActionNotAuthorizedException {

    baseDelete_Type_LockedNoAnonCreate0(RowFilterScope.Type.DEFAULT);
  }

  public void testDelete_HIDDEN_LockedNoAnonCreate0() throws ActionNotAuthorizedException {

    baseDelete_Type_LockedNoAnonCreate0(RowFilterScope.Type.HIDDEN);
  }

  public void testDelete_READ_ONLY_LockedNoAnonCreate0() throws ActionNotAuthorizedException {

    baseDelete_Type_LockedNoAnonCreate0(RowFilterScope.Type.READ_ONLY);
  }

  public void testDelete_MODIFY_LockedNoAnonCreate0() throws ActionNotAuthorizedException {

    baseDelete_Type_LockedNoAnonCreate0(RowFilterScope.Type.MODIFY);
  }

  private void baseDelete_Type_LockedYesAnonCreate0(RowFilterScope.Type type) throws
      ActionNotAuthorizedException {

    String tableId = testTableLockedYesAnonCreate;
    OrderedColumns oc = assertEmptyTestTable(testTableLockedYesAnonCreate,
        true, true, type.name());

    ArrayList<AuthParamAndOutcome> cases = buildOutcomesListDeleteLockedYesAnonCreate();

    for ( AuthParamAndOutcome ap : cases ) {
      if ( !ap.rowId.contains("New") ) {
        continue;
      }
      insertRowWIthAdminRights(ap, oc);

      try {
        // expect one row
        verifyRowSyncStateFilterTypeAndCheckpoints(ap.tableId, ap.rowId, 1, FirstSavepointTimestampType
            .NEW_ROW, type,
            ap.toString());

        ODKDatabaseImplUtils.get()
            .deleteRowWithId(db, ap.tableId, ap.rowId, ap.username, ap.roles);
        assertFalse("Expecting to throw an error: " + ap.toString(), ap.throwsAccessException);

        assertTrue("Expected no rows to remain: " + ap.toString(),
            verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 0, FirstSavepointTimestampType.NEW_ROW,
                ap.toString()));

      } catch ( ActionNotAuthorizedException ex ) {
        assertTrue("Expecting to not throw an error: " + ap.toString(), ap.throwsAccessException);
      }
    }
  }

  public void testDelete_DEFAULT_LockedYesAnonCreate0() throws ActionNotAuthorizedException {

    baseDelete_Type_LockedYesAnonCreate0(RowFilterScope.Type.DEFAULT);
  }

  public void testDelete_HIDDEN_LockedYesAnonCreate0() throws ActionNotAuthorizedException {

    baseDelete_Type_LockedYesAnonCreate0(RowFilterScope.Type.HIDDEN);
  }

  public void testDelete_READ_ONLY_LockedYesAnonCreate0() throws ActionNotAuthorizedException {

    baseDelete_Type_LockedYesAnonCreate0(RowFilterScope.Type.READ_ONLY);
  }

  public void testDelete_MODIFY_LockedYesAnonCreate0() throws ActionNotAuthorizedException {

    baseDelete_Type_LockedYesAnonCreate0(RowFilterScope.Type.MODIFY);
  }

  private void baseDelete_Type_AllCheckpointsAsInsertUnlockedNoAnonCreate(RowFilterScope.Type type)
      throws  ActionNotAuthorizedException {

    String tableId = testTableUnlockedNoAnonCreate;
    OrderedColumns oc = assertEmptyTestTable(testTableUnlockedNoAnonCreate,
        false, false, type.name());

    ArrayList<AuthParamAndOutcome> cases = buildOutcomesListDeleteUnlockedNoAnonCreate();

    for ( AuthParamAndOutcome ap : cases ) {
      if ( !ap.rowId.contains("New") ) {
        continue;
      }

      // insert 2 checkpoints
      insert2CheckpointsWithAdminRights(ap, oc);

      // expect two checkpoints in new_row state
      verifyRowSyncStateFilterTypeAndCheckpoints(ap.tableId, ap.rowId, 2, FirstSavepointTimestampType
              .CHECKPOINT_NEW_ROW, type,
          ap.toString());

      try {
        ODKDatabaseImplUtils.get()
            .deleteAllCheckpointRowsWithId(db, ap.tableId, ap.rowId, ap.username, ap.roles);
        assertFalse("Expecting to throw an error: " + ap.toString(), ap.throwsAccessException);

        assertTrue("Expected no rows to remain: " + ap.toString(),
            verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 0, FirstSavepointTimestampType
                    .NEW_ROW,
                ap.toString()));

      } catch (ActionNotAuthorizedException ex) {
        assertTrue("Expecting to not throw an error: " + ap.toString(), ap.throwsAccessException);
      }
    }
  }

  public void testDelete_DEFAULT_AllCheckpointsAsInsertUnlockedNoAnonCreate()
      throws  ActionNotAuthorizedException {

    baseDelete_Type_AllCheckpointsAsInsertUnlockedNoAnonCreate(RowFilterScope.Type.DEFAULT);
  }

  public void testDelete_HIDDEN_AllCheckpointsAsInsertUnlockedNoAnonCreate()
      throws  ActionNotAuthorizedException {

    baseDelete_Type_AllCheckpointsAsInsertUnlockedNoAnonCreate(RowFilterScope.Type.HIDDEN);
  }

  public void testDelete_READ_ONLY_AllCheckpointsAsInsertUnlockedNoAnonCreate()
      throws  ActionNotAuthorizedException {

    baseDelete_Type_AllCheckpointsAsInsertUnlockedNoAnonCreate(RowFilterScope.Type.READ_ONLY);
  }

  public void testDelete_MODIFY_AllCheckpointsAsInsertUnlockedNoAnonCreate()
      throws  ActionNotAuthorizedException {

    baseDelete_Type_AllCheckpointsAsInsertUnlockedNoAnonCreate(RowFilterScope.Type.MODIFY);
  }

  private void baseDelete_Type_AllCheckpointsAsInsertUnlockedYesAnonCreate(RowFilterScope.Type type)
      throws  ActionNotAuthorizedException {

    String tableId = testTableUnlockedYesAnonCreate;
    OrderedColumns oc = assertEmptyTestTable
        (testTableUnlockedYesAnonCreate,
            false, true, type.name());

    ArrayList<AuthParamAndOutcome> cases = buildOutcomesListDeleteUnlockedYesAnonCreate();

    for ( AuthParamAndOutcome ap : cases ) {
      if ( !ap.rowId.contains("New") ) {
        continue;
      }

      // insert 2 checkpoints
      insert2CheckpointsWithAdminRights(ap, oc);

      // expect two checkpoints in new_row state
      verifyRowSyncStateFilterTypeAndCheckpoints(ap.tableId, ap.rowId, 2, FirstSavepointTimestampType
              .CHECKPOINT_NEW_ROW, type,
          ap.toString());

      try {
        ODKDatabaseImplUtils.get()
            .deleteAllCheckpointRowsWithId(db, ap.tableId, ap.rowId, ap.username, ap.roles);
        assertFalse("Expecting to throw an error: " + ap.toString(), ap.throwsAccessException);

        assertTrue("Expected no rows to remain: " + ap.toString(),
            verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 0,
                FirstSavepointTimestampType.NEW_ROW,
                ap.toString()));

      } catch (ActionNotAuthorizedException ex) {
        assertTrue("Expecting to not throw an error: " + ap.toString(), ap.throwsAccessException);
      }
    }
  }

  public void testDelete_DEFAULT_AllCheckpointsAsInsertUnlockedYesAnonCreate()
      throws  ActionNotAuthorizedException {

    baseDelete_Type_AllCheckpointsAsInsertUnlockedYesAnonCreate(RowFilterScope.Type.DEFAULT);
  }

  public void testDelete_HIDDEN_AllCheckpointsAsInsertUnlockedYesAnonCreate()
      throws  ActionNotAuthorizedException {

    baseDelete_Type_AllCheckpointsAsInsertUnlockedYesAnonCreate(RowFilterScope.Type.HIDDEN);
  }

  public void testDelete_READ_ONLY_AllCheckpointsAsInsertUnlockedYesAnonCreate()
      throws  ActionNotAuthorizedException {

    baseDelete_Type_AllCheckpointsAsInsertUnlockedYesAnonCreate(RowFilterScope.Type.READ_ONLY);
  }

  public void testDelete_MODIFY_AllCheckpointsAsInsertUnlockedYesAnonCreate()
      throws  ActionNotAuthorizedException {

    baseDelete_Type_AllCheckpointsAsInsertUnlockedYesAnonCreate(RowFilterScope.Type.MODIFY);
  }

  private void baseDelete_Type_AllCheckpointsAsInsertLockedNoAnonCreate(RowFilterScope.Type type)
      throws  ActionNotAuthorizedException {

    String tableId = testTableLockedNoAnonCreate;
    OrderedColumns oc = assertEmptyTestTable
        (testTableLockedNoAnonCreate,
            true, false, type.name());

    ArrayList<AuthParamAndOutcome> cases = buildOutcomesListDeleteLockedNoAnonCreate();

    for ( AuthParamAndOutcome ap : cases ) {
      if ( !ap.rowId.contains("New") ) {
        continue;
      }

      // insert 2 checkpoints
      insert2CheckpointsWithAdminRights(ap, oc);

      // expect two checkpoints in new_row state
      verifyRowSyncStateFilterTypeAndCheckpoints(ap.tableId, ap.rowId, 2, FirstSavepointTimestampType
              .CHECKPOINT_NEW_ROW, type,
          ap.toString());

      try {
        ODKDatabaseImplUtils.get()
            .deleteAllCheckpointRowsWithId(db, ap.tableId, ap.rowId, ap.username, ap.roles);
        assertFalse("Expecting to throw an error: " + ap.toString(), ap.throwsAccessException);

        assertTrue("Expected no rows to remain: " + ap.toString(),
            verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 0,
                FirstSavepointTimestampType.NEW_ROW,
                ap.toString()));

      } catch (ActionNotAuthorizedException ex) {
        assertTrue("Expecting to not throw an error: " + ap.toString(), ap.throwsAccessException);
      }
    }
  }

  public void testDelete_DEFAULT_AllCheckpointsAsInsertLockedNoAnonCreate()
      throws  ActionNotAuthorizedException {

    baseDelete_Type_AllCheckpointsAsInsertLockedNoAnonCreate(RowFilterScope.Type.DEFAULT);
  }

  public void testDelete_HIDDEN_AllCheckpointsAsInsertLockedNoAnonCreate()
      throws  ActionNotAuthorizedException {

    baseDelete_Type_AllCheckpointsAsInsertLockedNoAnonCreate(RowFilterScope.Type.HIDDEN);
  }

  public void testDelete_READ_ONLY_AllCheckpointsAsInsertLockedNoAnonCreate()
      throws  ActionNotAuthorizedException {

    baseDelete_Type_AllCheckpointsAsInsertLockedNoAnonCreate(RowFilterScope.Type.READ_ONLY);
  }

  public void testDelete_MODIFY_AllCheckpointsAsInsertLockedNoAnonCreate()
      throws  ActionNotAuthorizedException {

    baseDelete_Type_AllCheckpointsAsInsertLockedNoAnonCreate(RowFilterScope.Type.MODIFY);
  }

  private void baseDelete_Type_AllCheckpointsAsInsertLockedYesAnonCreate(RowFilterScope.Type type)
      throws  ActionNotAuthorizedException {

    String tableId = testTableLockedYesAnonCreate;
    OrderedColumns oc = assertEmptyTestTable
        (testTableLockedYesAnonCreate,
            true, true, type.name());

    ArrayList<AuthParamAndOutcome> cases = buildOutcomesListDeleteLockedYesAnonCreate();

    for ( AuthParamAndOutcome ap : cases ) {
      if ( !ap.rowId.contains("New") ) {
        continue;
      }

      // insert 2 checkpoints
      insert2CheckpointsWithAdminRights(ap, oc);

      // expect two checkpoints in new_row state
      verifyRowSyncStateFilterTypeAndCheckpoints(ap.tableId, ap.rowId, 2, FirstSavepointTimestampType
              .CHECKPOINT_NEW_ROW, type,
          ap.toString());

      try {
        ODKDatabaseImplUtils.get()
            .deleteAllCheckpointRowsWithId(db, ap.tableId, ap.rowId, ap.username, ap.roles);
        assertFalse("Expecting to throw an error: " + ap.toString(), ap.throwsAccessException);

        assertTrue("Expected no rows to remain: " + ap.toString(),
            verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 0,
                FirstSavepointTimestampType.NEW_ROW,
                ap.toString()));

      } catch (ActionNotAuthorizedException ex) {
        assertTrue("Expecting to not throw an error: " + ap.toString(), ap.throwsAccessException);
      }
    }
  }

  public void testDelete_DEFAULT_AllCheckpointsAsInsertLockedYesAnonCreate()
      throws  ActionNotAuthorizedException {

    baseDelete_Type_AllCheckpointsAsInsertLockedYesAnonCreate(RowFilterScope.Type.DEFAULT);
  }

  public void testDelete_HIDDEN_AllCheckpointsAsInsertLockedYesAnonCreate()
      throws  ActionNotAuthorizedException {

    baseDelete_Type_AllCheckpointsAsInsertLockedYesAnonCreate(RowFilterScope.Type.HIDDEN);
  }

  public void testDelete_READ_ONLY_AllCheckpointsAsInsertLockedYesAnonCreate()
      throws  ActionNotAuthorizedException {

    baseDelete_Type_AllCheckpointsAsInsertLockedYesAnonCreate(RowFilterScope.Type.READ_ONLY);
  }

  public void testDelete_MODIFY_AllCheckpointsAsInsertLockedYesAnonCreate()
      throws  ActionNotAuthorizedException {

    baseDelete_Type_AllCheckpointsAsInsertLockedYesAnonCreate(RowFilterScope.Type.MODIFY);
  }


}
