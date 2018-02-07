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

package org.opendatakit.utilities;

import android.support.test.runner.AndroidJUnit4;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opendatakit.aggregate.odktables.rest.entity.RowFilterScope;
import org.opendatakit.database.data.OrderedColumns;
import org.opendatakit.exception.ActionNotAuthorizedException;
import org.opendatakit.services.database.utilities.ODKDatabaseImplUtils;

import java.util.ArrayList;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Permissions tests in the database.
 */
@RunWith(AndroidJUnit4.class)
public class ODKDatabaseUtilsNewRowDeletePermissionsTest extends AbstractPermissionsTestCase {

  private static final String TAG = "ODKDatabaseUtilsNewRowDeletePermissionsTest";

  private void baseDelete_Type_UnlockedNoAnonCreate0(RowFilterScope.Access access) throws
      ActionNotAuthorizedException {

    String tableId = testTableUnlockedNoAnonCreate;
    OrderedColumns oc = assertEmptyTestTable(testTableUnlockedNoAnonCreate,
        false, false, access.name());

    ArrayList<AuthParamAndOutcome> cases = buildOutcomesListDeleteUnlockedNoAnonCreate();

    for ( AuthParamAndOutcome ap : cases ) {
      if ( !ap.rowId.contains("New") ) {
        continue;
      }
      insertRowWIthAdminRights(ap, oc);

      try {
        // expect one row
        verifyRowSyncStateDefaultAccessAndCheckpoints(ap.tableId, ap.rowId, 1, FirstSavepointTimestampType
                .NEW_ROW, access,
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

  @Test
  public void testDelete_DEFAULT_UnlockedNoAnonCreate0() throws ActionNotAuthorizedException {

    baseDelete_Type_UnlockedNoAnonCreate0(RowFilterScope.Access.FULL);
  }

  @Test
  public void testDelete_HIDDEN_UnlockedNoAnonCreate0() throws ActionNotAuthorizedException {

    baseDelete_Type_UnlockedNoAnonCreate0(RowFilterScope.Access.HIDDEN);
  }

  @Test
  public void testDelete_READ_ONLY_UnlockedNoAnonCreate0() throws ActionNotAuthorizedException {

    baseDelete_Type_UnlockedNoAnonCreate0(RowFilterScope.Access.READ_ONLY);
  }

  @Test
  public void testDelete_MODIFY_UnlockedNoAnonCreate0() throws ActionNotAuthorizedException {

    baseDelete_Type_UnlockedNoAnonCreate0(RowFilterScope.Access.MODIFY);
  }

  private void baseDelete_Type_UnlockedYesAnonCreate0(RowFilterScope.Access access) throws
      ActionNotAuthorizedException {

    String tableId = testTableUnlockedYesAnonCreate;
    OrderedColumns oc = assertEmptyTestTable(testTableUnlockedYesAnonCreate,
        false, true, access.name());

    ArrayList<AuthParamAndOutcome> cases = buildOutcomesListDeleteUnlockedYesAnonCreate();

    for ( AuthParamAndOutcome ap : cases ) {
      if ( !ap.rowId.contains("New") ) {
        continue;
      }
      insertRowWIthAdminRights(ap, oc);

      try {
        // expect one row
        verifyRowSyncStateDefaultAccessAndCheckpoints(ap.tableId, ap.rowId, 1, FirstSavepointTimestampType
                .NEW_ROW, access,
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

  @Test
  public void testDelete_DEFAULT_UnlockedYesAnonCreate0() throws ActionNotAuthorizedException {

    baseDelete_Type_UnlockedYesAnonCreate0(RowFilterScope.Access.FULL);
  }

  @Test
  public void testDelete_HIDDEN_UnlockedYesAnonCreate0() throws ActionNotAuthorizedException {

    baseDelete_Type_UnlockedYesAnonCreate0(RowFilterScope.Access.HIDDEN);
  }

  @Test
  public void testDelete_READ_ONLY_UnlockedYesAnonCreate0() throws ActionNotAuthorizedException {

    baseDelete_Type_UnlockedYesAnonCreate0(RowFilterScope.Access.READ_ONLY);
  }

  @Test
  public void testDelete_MODIFY_UnlockedYesAnonCreate0() throws ActionNotAuthorizedException {

    baseDelete_Type_UnlockedYesAnonCreate0(RowFilterScope.Access.MODIFY);
  }

  private void baseDelete_Type_LockedNoAnonCreate0(RowFilterScope.Access access) throws
      ActionNotAuthorizedException {

    String tableId = testTableLockedNoAnonCreate;
    OrderedColumns oc = assertEmptyTestTable(testTableLockedNoAnonCreate,
        true, false, access.name());

    ArrayList<AuthParamAndOutcome> cases = buildOutcomesListDeleteLockedNoAnonCreate();

    for ( AuthParamAndOutcome ap : cases ) {
      if ( !ap.rowId.contains("New") ) {
        continue;
      }
      insertRowWIthAdminRights(ap, oc);

      try {
        // expect one row
        verifyRowSyncStateDefaultAccessAndCheckpoints(ap.tableId, ap.rowId, 1, FirstSavepointTimestampType
            .NEW_ROW, access,
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

  @Test
  public void testDelete_DEFAULT_LockedNoAnonCreate0() throws ActionNotAuthorizedException {

    baseDelete_Type_LockedNoAnonCreate0(RowFilterScope.Access.FULL);
  }

  @Test
  public void testDelete_HIDDEN_LockedNoAnonCreate0() throws ActionNotAuthorizedException {

    baseDelete_Type_LockedNoAnonCreate0(RowFilterScope.Access.HIDDEN);
  }

  @Test
  public void testDelete_READ_ONLY_LockedNoAnonCreate0() throws ActionNotAuthorizedException {

    baseDelete_Type_LockedNoAnonCreate0(RowFilterScope.Access.READ_ONLY);
  }

  @Test
  public void testDelete_MODIFY_LockedNoAnonCreate0() throws ActionNotAuthorizedException {

    baseDelete_Type_LockedNoAnonCreate0(RowFilterScope.Access.MODIFY);
  }

  private void baseDelete_Type_LockedYesAnonCreate0(RowFilterScope.Access access) throws
      ActionNotAuthorizedException {

    String tableId = testTableLockedYesAnonCreate;
    OrderedColumns oc = assertEmptyTestTable(testTableLockedYesAnonCreate,
        true, true, access.name());

    ArrayList<AuthParamAndOutcome> cases = buildOutcomesListDeleteLockedYesAnonCreate();

    for ( AuthParamAndOutcome ap : cases ) {
      if ( !ap.rowId.contains("New") ) {
        continue;
      }
      insertRowWIthAdminRights(ap, oc);

      try {
        // expect one row
        verifyRowSyncStateDefaultAccessAndCheckpoints(ap.tableId, ap.rowId, 1, FirstSavepointTimestampType
            .NEW_ROW, access,
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

  @Test
  public void testDelete_DEFAULT_LockedYesAnonCreate0() throws ActionNotAuthorizedException {

    baseDelete_Type_LockedYesAnonCreate0(RowFilterScope.Access.FULL);
  }

  @Test
  public void testDelete_HIDDEN_LockedYesAnonCreate0() throws ActionNotAuthorizedException {

    baseDelete_Type_LockedYesAnonCreate0(RowFilterScope.Access.HIDDEN);
  }

  @Test
  public void testDelete_READ_ONLY_LockedYesAnonCreate0() throws ActionNotAuthorizedException {

    baseDelete_Type_LockedYesAnonCreate0(RowFilterScope.Access.READ_ONLY);
  }

  @Test
  public void testDelete_MODIFY_LockedYesAnonCreate0() throws ActionNotAuthorizedException {

    baseDelete_Type_LockedYesAnonCreate0(RowFilterScope.Access.MODIFY);
  }

  private void baseDelete_Type_AllCheckpointsAsInsertUnlockedNoAnonCreate(RowFilterScope.Access access)
      throws  ActionNotAuthorizedException {

    String tableId = testTableUnlockedNoAnonCreate;
    OrderedColumns oc = assertEmptyTestTable(testTableUnlockedNoAnonCreate,
        false, false, access.name());

    ArrayList<AuthParamAndOutcome> cases = buildOutcomesListDeleteUnlockedNoAnonCreate();

    for ( AuthParamAndOutcome ap : cases ) {
      if ( !ap.rowId.contains("New") ) {
        continue;
      }

      // insert 2 checkpoints
      insert2CheckpointsWithAdminRights(ap, oc);

      // expect two checkpoints in new_row state
      verifyRowSyncStateDefaultAccessAndCheckpoints(ap.tableId, ap.rowId, 2, FirstSavepointTimestampType
              .CHECKPOINT_NEW_ROW, access,
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

  @Test
  public void testDelete_DEFAULT_AllCheckpointsAsInsertUnlockedNoAnonCreate()
      throws  ActionNotAuthorizedException {

    baseDelete_Type_AllCheckpointsAsInsertUnlockedNoAnonCreate(RowFilterScope.Access.FULL);
  }

  @Test
  public void testDelete_HIDDEN_AllCheckpointsAsInsertUnlockedNoAnonCreate()
      throws  ActionNotAuthorizedException {

    baseDelete_Type_AllCheckpointsAsInsertUnlockedNoAnonCreate(RowFilterScope.Access.HIDDEN);
  }

  @Test
  public void testDelete_READ_ONLY_AllCheckpointsAsInsertUnlockedNoAnonCreate()
      throws  ActionNotAuthorizedException {

    baseDelete_Type_AllCheckpointsAsInsertUnlockedNoAnonCreate(RowFilterScope.Access.READ_ONLY);
  }

  @Test
  public void testDelete_MODIFY_AllCheckpointsAsInsertUnlockedNoAnonCreate()
      throws  ActionNotAuthorizedException {

    baseDelete_Type_AllCheckpointsAsInsertUnlockedNoAnonCreate(RowFilterScope.Access.MODIFY);
  }

  private void baseDelete_Type_AllCheckpointsAsInsertUnlockedYesAnonCreate(RowFilterScope.Access access)
      throws  ActionNotAuthorizedException {

    String tableId = testTableUnlockedYesAnonCreate;
    OrderedColumns oc = assertEmptyTestTable
        (testTableUnlockedYesAnonCreate,
            false, true, access.name());

    ArrayList<AuthParamAndOutcome> cases = buildOutcomesListDeleteUnlockedYesAnonCreate();

    for ( AuthParamAndOutcome ap : cases ) {
      if ( !ap.rowId.contains("New") ) {
        continue;
      }

      // insert 2 checkpoints
      insert2CheckpointsWithAdminRights(ap, oc);

      // expect two checkpoints in new_row state
      verifyRowSyncStateDefaultAccessAndCheckpoints(ap.tableId, ap.rowId, 2, FirstSavepointTimestampType
              .CHECKPOINT_NEW_ROW, access,
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

  @Test
  public void testDelete_DEFAULT_AllCheckpointsAsInsertUnlockedYesAnonCreate()
      throws  ActionNotAuthorizedException {

    baseDelete_Type_AllCheckpointsAsInsertUnlockedYesAnonCreate(RowFilterScope.Access.FULL);
  }

  @Test
  public void testDelete_HIDDEN_AllCheckpointsAsInsertUnlockedYesAnonCreate()
      throws  ActionNotAuthorizedException {

    baseDelete_Type_AllCheckpointsAsInsertUnlockedYesAnonCreate(RowFilterScope.Access.HIDDEN);
  }

  @Test
  public void testDelete_READ_ONLY_AllCheckpointsAsInsertUnlockedYesAnonCreate()
      throws  ActionNotAuthorizedException {

    baseDelete_Type_AllCheckpointsAsInsertUnlockedYesAnonCreate(RowFilterScope.Access.READ_ONLY);
  }

  @Test
  public void testDelete_MODIFY_AllCheckpointsAsInsertUnlockedYesAnonCreate()
      throws  ActionNotAuthorizedException {

    baseDelete_Type_AllCheckpointsAsInsertUnlockedYesAnonCreate(RowFilterScope.Access.MODIFY);
  }

  private void baseDelete_Type_AllCheckpointsAsInsertLockedNoAnonCreate(RowFilterScope.Access access)
      throws  ActionNotAuthorizedException {

    String tableId = testTableLockedNoAnonCreate;
    OrderedColumns oc = assertEmptyTestTable
        (testTableLockedNoAnonCreate,
            true, false, access.name());

    ArrayList<AuthParamAndOutcome> cases = buildOutcomesListDeleteLockedNoAnonCreate();

    for ( AuthParamAndOutcome ap : cases ) {
      if ( !ap.rowId.contains("New") ) {
        continue;
      }

      // insert 2 checkpoints
      insert2CheckpointsWithAdminRights(ap, oc);

      // expect two checkpoints in new_row state
      verifyRowSyncStateDefaultAccessAndCheckpoints(ap.tableId, ap.rowId, 2, FirstSavepointTimestampType
              .CHECKPOINT_NEW_ROW, access,
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

  @Test
  public void testDelete_DEFAULT_AllCheckpointsAsInsertLockedNoAnonCreate()
      throws  ActionNotAuthorizedException {

    baseDelete_Type_AllCheckpointsAsInsertLockedNoAnonCreate(RowFilterScope.Access.FULL);
  }

  @Test
  public void testDelete_HIDDEN_AllCheckpointsAsInsertLockedNoAnonCreate()
      throws  ActionNotAuthorizedException {

    baseDelete_Type_AllCheckpointsAsInsertLockedNoAnonCreate(RowFilterScope.Access.HIDDEN);
  }

  @Test
  public void testDelete_READ_ONLY_AllCheckpointsAsInsertLockedNoAnonCreate()
      throws  ActionNotAuthorizedException {

    baseDelete_Type_AllCheckpointsAsInsertLockedNoAnonCreate(RowFilterScope.Access.READ_ONLY);
  }

  @Test
  public void testDelete_MODIFY_AllCheckpointsAsInsertLockedNoAnonCreate()
      throws  ActionNotAuthorizedException {

    baseDelete_Type_AllCheckpointsAsInsertLockedNoAnonCreate(RowFilterScope.Access.MODIFY);
  }

  private void baseDelete_Type_AllCheckpointsAsInsertLockedYesAnonCreate(RowFilterScope.Access access)
      throws  ActionNotAuthorizedException {

    String tableId = testTableLockedYesAnonCreate;
    OrderedColumns oc = assertEmptyTestTable
        (testTableLockedYesAnonCreate,
            true, true, access.name());

    ArrayList<AuthParamAndOutcome> cases = buildOutcomesListDeleteLockedYesAnonCreate();

    for ( AuthParamAndOutcome ap : cases ) {
      if ( !ap.rowId.contains("New") ) {
        continue;
      }

      // insert 2 checkpoints
      insert2CheckpointsWithAdminRights(ap, oc);

      // expect two checkpoints in new_row state
      verifyRowSyncStateDefaultAccessAndCheckpoints(ap.tableId, ap.rowId, 2, FirstSavepointTimestampType
              .CHECKPOINT_NEW_ROW, access,
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

  @Test
  public void testDelete_DEFAULT_AllCheckpointsAsInsertLockedYesAnonCreate()
      throws  ActionNotAuthorizedException {

    baseDelete_Type_AllCheckpointsAsInsertLockedYesAnonCreate(RowFilterScope.Access.FULL);
  }

  @Test
  public void testDelete_HIDDEN_AllCheckpointsAsInsertLockedYesAnonCreate()
      throws  ActionNotAuthorizedException {

    baseDelete_Type_AllCheckpointsAsInsertLockedYesAnonCreate(RowFilterScope.Access.HIDDEN);
  }

  @Test
  public void testDelete_READ_ONLY_AllCheckpointsAsInsertLockedYesAnonCreate()
      throws  ActionNotAuthorizedException {

    baseDelete_Type_AllCheckpointsAsInsertLockedYesAnonCreate(RowFilterScope.Access.READ_ONLY);
  }

  @Test
  public void testDelete_MODIFY_AllCheckpointsAsInsertLockedYesAnonCreate()
      throws  ActionNotAuthorizedException {

    baseDelete_Type_AllCheckpointsAsInsertLockedYesAnonCreate(RowFilterScope.Access.MODIFY);
  }


}
