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
public class ODKDatabaseUtilsDeletePermissionsTest extends AbstractPermissionsTestCase {

  private static final String TAG = "ODKDatabaseUtilsDeletePermissionsTest";

  public void testDeleteUnlockedNoAnonCreate() throws ActionNotAuthorizedException {

    String tableId = testTableUnlockedNoAnonCreate;
    OrderedColumns oc = assertPopulatedTestTable(testTableUnlockedNoAnonCreate,
        false, false, RowFilterScope.Type.DEFAULT.name());

    ArrayList<AuthParamAndOutcome> cases = buildOutcomesListDeleteUnlockedNoAnonCreate();

    for ( AuthParamAndOutcome ap : cases ) {
      try {
        if ( ap.rowId.contains("New") ) {
          // expect one row in new_row state
          verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 1, FirstSavepointTimestampType.NEW_ROW,
              ap.toString());
        } else {
          // expect one row in synced state
          verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 1, FirstSavepointTimestampType.SYNCED,
              ap.toString());
        }

        ODKDatabaseImplUtils.get()
            .deleteRowWithId(db, ap.tableId, ap.rowId, ap.username, ap.roles);
        assertFalse("Expecting to throw an error: " + ap.toString(), ap.throwsAccessException);

        if ( ap.rowId.contains("New") ) {
          assertTrue("Expected no rows to remain: " + ap.toString(),
              verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 0, FirstSavepointTimestampType.NEW_ROW,
                  ap.toString()));
        } else {
          assertTrue("Expected row to be marked as deleted: " + ap.toString(),
              verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 1, FirstSavepointTimestampType.DELETED,
                  ap.toString()));
        }

      } catch ( ActionNotAuthorizedException ex ) {
        assertTrue("Expecting to not throw an error: " + ap.toString(), ap.throwsAccessException);
      }
    }
  }

  public void testDeleteUnlockedYesAnonCreate() throws ActionNotAuthorizedException {

    String tableId = testTableUnlockedYesAnonCreate;
    OrderedColumns oc = assertPopulatedTestTable(testTableUnlockedYesAnonCreate,
        false, true, RowFilterScope.Type.DEFAULT.name());

    ArrayList<AuthParamAndOutcome> cases = buildOutcomesListDeleteUnlockedYesAnonCreate();

    for ( AuthParamAndOutcome ap : cases ) {
      try {
        if ( ap.rowId.contains("New") ) {
          // expect one row in new_row state
          verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 1, FirstSavepointTimestampType.NEW_ROW,
              ap.toString());
        } else {
          // expect one row in synced state
          verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 1, FirstSavepointTimestampType.SYNCED,
              ap.toString());
        }

        ODKDatabaseImplUtils.get()
            .deleteRowWithId(db, ap.tableId, ap.rowId, ap.username, ap.roles);
        assertFalse("Expecting to throw an error: " + ap.toString(), ap.throwsAccessException);

        if ( ap.rowId.contains("New") ) {
          assertTrue("Expected no rows to remain: " + ap.toString(),
              verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 0, FirstSavepointTimestampType.NEW_ROW,
                  ap.toString()));
        } else {
          assertTrue("Expected row to be marked as deleted: " + ap.toString(),
              verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 1, FirstSavepointTimestampType.DELETED,
                  ap.toString()));
        }

      } catch ( ActionNotAuthorizedException ex ) {
        assertTrue("Expecting to not throw an error: " + ap.toString(), ap.throwsAccessException);
      }
    }
  }

  public void testDeleteLockedNoAnonCreate() throws ActionNotAuthorizedException {

    String tableId = testTableLockedNoAnonCreate;
    OrderedColumns oc = assertPopulatedTestTable(testTableLockedNoAnonCreate,
        true, false, RowFilterScope.Type.DEFAULT.name());

    ArrayList<AuthParamAndOutcome> cases = buildOutcomesListDeleteLockedNoAnonCreate();

    for ( AuthParamAndOutcome ap : cases ) {
      try {
        if ( ap.rowId.contains("New") ) {
          // expect one row in new_row state
          verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 1, FirstSavepointTimestampType.NEW_ROW,
              ap.toString());
        } else {
          // expect one row in synced state
          verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 1, FirstSavepointTimestampType.SYNCED,
              ap.toString());
        }

        ODKDatabaseImplUtils.get()
            .deleteRowWithId(db, ap.tableId, ap.rowId, ap.username, ap.roles);
        assertFalse("Expecting to throw an error: " + ap.toString(), ap.throwsAccessException);

        if ( ap.rowId.contains("New") ) {
          assertTrue("Expected no rows to remain: " + ap.toString(),
              verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 0, FirstSavepointTimestampType.NEW_ROW,
                  ap.toString()));
        } else {
          assertTrue("Expected row to be marked as deleted: " + ap.toString(),
              verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 1, FirstSavepointTimestampType.DELETED,
                  ap.toString()));
        }

      } catch ( ActionNotAuthorizedException ex ) {
        assertTrue("Expecting to not throw an error: " + ap.toString(), ap.throwsAccessException);
      }
    }
  }

  public void testDeleteLockedYesAnonCreate() throws ActionNotAuthorizedException {

    String tableId = testTableLockedYesAnonCreate;
    OrderedColumns oc = assertPopulatedTestTable(testTableLockedYesAnonCreate,
        true, true, RowFilterScope.Type.DEFAULT.name());

    ArrayList<AuthParamAndOutcome> cases = buildOutcomesListDeleteLockedYesAnonCreate();

    for ( AuthParamAndOutcome ap : cases ) {
      try {
        if ( ap.rowId.contains("New") ) {
          // expect one row in new_row state
          verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 1, FirstSavepointTimestampType.NEW_ROW,
              ap.toString());
        } else {
          // expect one row in synced state
          verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 1, FirstSavepointTimestampType.SYNCED,
              ap.toString());
        }

        ODKDatabaseImplUtils.get()
            .deleteRowWithId(db, ap.tableId, ap.rowId, ap.username, ap.roles);
        assertFalse("Expecting to throw an error: " + ap.toString(), ap.throwsAccessException);

        if ( ap.rowId.contains("New") ) {
          assertTrue("Expected no rows to remain: " + ap.toString(),
              verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 0, FirstSavepointTimestampType.NEW_ROW,
                  ap.toString()));
        } else {
          assertTrue("Expected row to be marked as deleted: " + ap.toString(),
              verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 1, FirstSavepointTimestampType.DELETED,
                  ap.toString()));
        }

      } catch ( ActionNotAuthorizedException ex ) {
        assertTrue("Expecting to not throw an error: " + ap.toString(), ap.throwsAccessException);
      }
    }
  }

  public void testDeleteUnlockedNoAnonCreate0() throws ActionNotAuthorizedException {

    String tableId = testTableUnlockedNoAnonCreate;
    OrderedColumns oc = assertEmptyTestTable(testTableUnlockedNoAnonCreate,
        false, false, RowFilterScope.Type.DEFAULT.name());

    ArrayList<AuthParamAndOutcome> cases = buildOutcomesListDeleteUnlockedNoAnonCreate();

    for ( AuthParamAndOutcome ap : cases ) {
      try {
        // expect no rows
        verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 0, FirstSavepointTimestampType
                .NEW_ROW,
            ap.toString());

        // should be a no-op
        ODKDatabaseImplUtils.get()
            .deleteRowWithId(db, ap.tableId, ap.rowId, ap.username, ap.roles);

        assertTrue("Expected no rows to remain: " + ap.toString(),
            verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 0, FirstSavepointTimestampType.NEW_ROW,
                ap.toString()));

      } catch ( ActionNotAuthorizedException ex ) {
        assertTrue("Expecting to not throw an error: " + ap.toString(), false);
      }
    }
  }

  public void testDeleteUnlockedYesAnonCreate0() throws ActionNotAuthorizedException {

    String tableId = testTableUnlockedYesAnonCreate;
    OrderedColumns oc = assertEmptyTestTable(testTableUnlockedYesAnonCreate,
        false, true, RowFilterScope.Type.DEFAULT.name());

    ArrayList<AuthParamAndOutcome> cases = buildOutcomesListDeleteUnlockedYesAnonCreate();

    for ( AuthParamAndOutcome ap : cases ) {
      try {
        // expect no rows
        verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 0, FirstSavepointTimestampType
                .NEW_ROW,
            ap.toString());

        // should be a no-op
        ODKDatabaseImplUtils.get()
            .deleteRowWithId(db, ap.tableId, ap.rowId, ap.username, ap.roles);

        assertTrue("Expected no rows to remain: " + ap.toString(),
            verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 0, FirstSavepointTimestampType.NEW_ROW,
                ap.toString()));

      } catch ( ActionNotAuthorizedException ex ) {
        assertTrue("Expecting to not throw an error: " + ap.toString(), false);
      }
    }
  }

  public void testDeleteLockedNoAnonCreate0() throws ActionNotAuthorizedException {

    String tableId = testTableLockedNoAnonCreate;
    OrderedColumns oc = assertEmptyTestTable(testTableLockedNoAnonCreate,
        true, false, RowFilterScope.Type.DEFAULT.name());

    ArrayList<AuthParamAndOutcome> cases = buildOutcomesListDeleteLockedNoAnonCreate();

    for ( AuthParamAndOutcome ap : cases ) {
      try {
        // expect no rows
        verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 0, FirstSavepointTimestampType
            .NEW_ROW,
            ap.toString());

        // should be a no-op
        ODKDatabaseImplUtils.get()
            .deleteRowWithId(db, ap.tableId, ap.rowId, ap.username, ap.roles);

        assertTrue("Expected no rows to remain: " + ap.toString(),
            verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 0, FirstSavepointTimestampType.NEW_ROW,
                ap.toString()));

      } catch ( ActionNotAuthorizedException ex ) {
        assertTrue("Expecting to not throw an error: " + ap.toString(), false);
      }
    }
  }

  public void testDeleteLockedYesAnonCreate0() throws ActionNotAuthorizedException {

    String tableId = testTableLockedYesAnonCreate;
    OrderedColumns oc = assertEmptyTestTable(testTableLockedYesAnonCreate,
        true, true, RowFilterScope.Type.DEFAULT.name());

    ArrayList<AuthParamAndOutcome> cases = buildOutcomesListDeleteLockedYesAnonCreate();

    for ( AuthParamAndOutcome ap : cases ) {
      try {
        // expect no rows
        verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 0, FirstSavepointTimestampType
            .NEW_ROW,
            ap.toString());

        // should be a no-op
        ODKDatabaseImplUtils.get()
            .deleteRowWithId(db, ap.tableId, ap.rowId, ap.username, ap.roles);

        assertTrue("Expected no rows to remain: " + ap.toString(),
            verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 0, FirstSavepointTimestampType.NEW_ROW,
                ap.toString()));

      } catch ( ActionNotAuthorizedException ex ) {
        assertTrue("Expecting to not throw an error: " + ap.toString(), false);
      }
    }
  }

  public void testDeleteWithCheckpointsUnlockedNoAnonCreate() throws ActionNotAuthorizedException {

    String tableId = testTableUnlockedNoAnonCreate;
    OrderedColumns oc = assertTwoCheckpointAsUpdatePopulatedTestTable(testTableUnlockedNoAnonCreate,
        false, false, RowFilterScope.Type.DEFAULT.name());

    ArrayList<AuthParamAndOutcome> cases = buildOutcomesListDeleteUnlockedNoAnonCreate();

    for ( AuthParamAndOutcome ap : cases ) {
      if ( ap.rowId.contains("New") ) {
        // expect three row in new_row and two checkpoints in new_row state
        verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 3, FirstSavepointTimestampType
            .NEW_ROW,
            ap.toString());
      } else {
        // expect three rows in synced and two checkpoints in changed state
        verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 3, FirstSavepointTimestampType
            .SYNCED,
            ap.toString());
      }

      try {
        ODKDatabaseImplUtils.get()
            .deleteRowWithId(db, ap.tableId, ap.rowId, ap.username, ap.roles);
        assertFalse("Expecting to throw an error: " + ap.toString(), ap.throwsAccessException);

        if ( ap.rowId.contains("New") ) {
          assertTrue("Expected no rows to remain: " + ap.toString(),
              verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 0, FirstSavepointTimestampType.NEW_ROW,
                  ap.toString()));
        } else {
          assertTrue("Expected row to be marked as deleted: " + ap.toString(),
              verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 1, FirstSavepointTimestampType.DELETED,
                  ap.toString()));
        }

      } catch ( ActionNotAuthorizedException ex ) {
        assertTrue("Expecting to not throw an error: " + ap.toString(), ap.throwsAccessException);
      }
    }
  }

  public void testDeleteWithCheckpointsUnlockedYesAnonCreate() throws ActionNotAuthorizedException {

    String tableId = testTableUnlockedYesAnonCreate;
    OrderedColumns oc = assertTwoCheckpointAsUpdatePopulatedTestTable(testTableUnlockedYesAnonCreate,
        false, true, RowFilterScope.Type.DEFAULT.name());

    ArrayList<AuthParamAndOutcome> cases = buildOutcomesListDeleteUnlockedYesAnonCreate();

    for ( AuthParamAndOutcome ap : cases ) {
      if ( ap.rowId.contains("New") ) {
        // expect three row in new_row and two checkpoints in new_row state
        verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 3, FirstSavepointTimestampType
                .NEW_ROW,
            ap.toString());
      } else {
        // expect three rows in synced and two checkpoints in changed state
        verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 3, FirstSavepointTimestampType
                .SYNCED,
            ap.toString());
      }

      try {
        ODKDatabaseImplUtils.get()
            .deleteRowWithId(db, ap.tableId, ap.rowId, ap.username, ap.roles);
        assertFalse("Expecting to throw an error: " + ap.toString(), ap.throwsAccessException);

        if ( ap.rowId.contains("New") ) {
          assertTrue("Expected no rows to remain: " + ap.toString(),
              verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 0, FirstSavepointTimestampType.NEW_ROW,
                  ap.toString()));
        } else {
          assertTrue("Expected row to be marked as deleted: " + ap.toString(),
              verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 1, FirstSavepointTimestampType.DELETED,
                  ap.toString()));
        }

      } catch ( ActionNotAuthorizedException ex ) {
        assertTrue("Expecting to not throw an error: " + ap.toString(), ap.throwsAccessException);
      }
    }
  }

  public void testDeleteWithCheckpointsLockedNoAnonCreate() throws ActionNotAuthorizedException {

    String tableId = testTableLockedNoAnonCreate;
    OrderedColumns oc = assertTwoCheckpointAsUpdatePopulatedTestTable(testTableLockedNoAnonCreate,
        true, false, RowFilterScope.Type.DEFAULT.name());

    ArrayList<AuthParamAndOutcome> cases = buildOutcomesListDeleteLockedNoAnonCreate();

    for ( AuthParamAndOutcome ap : cases ) {
      if ( ap.rowId.contains("New") ) {
        // expect three row in new_row and two checkpoints in new_row state
        verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 3, FirstSavepointTimestampType
                .NEW_ROW,
            ap.toString());
      } else {
        // expect three rows in synced and two checkpoints in changed state
        verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 3, FirstSavepointTimestampType
                .SYNCED,
            ap.toString());
      }

      try {
        ODKDatabaseImplUtils.get()
            .deleteRowWithId(db, ap.tableId, ap.rowId, ap.username, ap.roles);
        assertFalse("Expecting to throw an error: " + ap.toString(), ap.throwsAccessException);

        if ( ap.rowId.contains("New") ) {
          assertTrue("Expected no rows to remain: " + ap.toString(),
              verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 0, FirstSavepointTimestampType.NEW_ROW,
                  ap.toString()));
        } else {
          assertTrue("Expected row to be marked as deleted: " + ap.toString(),
              verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 1, FirstSavepointTimestampType.DELETED,
                  ap.toString()));
        }

      } catch ( ActionNotAuthorizedException ex ) {
        assertTrue("Expecting to not throw an error: " + ap.toString(), ap.throwsAccessException);
      }
    }
  }

  public void testDeleteWithCheckpointsLockedYesAnonCreate() throws ActionNotAuthorizedException {

    String tableId = testTableLockedYesAnonCreate;
    OrderedColumns oc = assertTwoCheckpointAsUpdatePopulatedTestTable(testTableLockedYesAnonCreate,
        true, true, RowFilterScope.Type.DEFAULT.name());

    ArrayList<AuthParamAndOutcome> cases = buildOutcomesListDeleteLockedYesAnonCreate();

    for ( AuthParamAndOutcome ap : cases ) {
      if ( ap.rowId.contains("New") ) {
        // expect three row in new_row and two checkpoints in new_row state
        verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 3, FirstSavepointTimestampType
                .NEW_ROW,
            ap.toString());
      } else {
        // expect three rows in synced and two checkpoints in changed state
        verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 3, FirstSavepointTimestampType
                .SYNCED,
            ap.toString());
      }

      try {
        ODKDatabaseImplUtils.get()
            .deleteRowWithId(db, ap.tableId, ap.rowId, ap.username, ap.roles);
        assertFalse("Expecting to throw an error: " + ap.toString(), ap.throwsAccessException);

        if ( ap.rowId.contains("New") ) {
          assertTrue("Expected no rows to remain: " + ap.toString(),
              verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 0, FirstSavepointTimestampType.NEW_ROW,
                  ap.toString()));
        } else {
          assertTrue("Expected row to be marked as deleted: " + ap.toString(),
              verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 1, FirstSavepointTimestampType.DELETED,
                  ap.toString()));
        }

      } catch ( ActionNotAuthorizedException ex ) {
        assertTrue("Expecting to not throw an error: " + ap.toString(), ap.throwsAccessException);
      }
    }
  }

  public void testDeleteAllCheckpointsAsUpdateUnlockedNoAnonCreate()
      throws  ActionNotAuthorizedException {

    String tableId = testTableUnlockedNoAnonCreate;
    OrderedColumns oc = assertTwoCheckpointAsUpdatePopulatedTestTable
        (testTableUnlockedNoAnonCreate,
        false, false, RowFilterScope.Type.DEFAULT.name());

    ArrayList<AuthParamAndOutcome> cases = buildOutcomesListDeleteUnlockedNoAnonCreate();

    for ( AuthParamAndOutcome ap : cases ) {
      if ( ap.rowId.contains("New") ) {
        // expect three row in new_row and two checkpoints in new_row state
        verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 3, FirstSavepointTimestampType
                .NEW_ROW,
            ap.toString());
      } else {
        // expect three rows in synced and two checkpoints in changed state
        verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 3, FirstSavepointTimestampType
                .SYNCED,
            ap.toString());
      }

      try {
        ODKDatabaseImplUtils.get()
            .deleteAllCheckpointRowsWithId(db, ap.tableId, ap.rowId, ap.username, ap.roles);
        assertFalse("Expecting to throw an error: " + ap.toString(), ap.throwsAccessException);

        if ( ap.rowId.contains("New") ) {
          assertTrue("Expected no rows to remain: " + ap.toString(),
              verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 1, FirstSavepointTimestampType.NEW_ROW,
                  ap.toString()));
        } else {
          assertTrue("Expected row to be marked as deleted: " + ap.toString(),
              verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 1, FirstSavepointTimestampType.SYNCED,
                  ap.toString()));
        }

      } catch (ActionNotAuthorizedException ex) {
        assertTrue("Expecting to not throw an error: " + ap.toString(), ap.throwsAccessException);
      }
    }
  }

  public void testDeleteAllCheckpointsAsUpdateUnlockedYesAnonCreate()
      throws  ActionNotAuthorizedException {

    String tableId = testTableUnlockedYesAnonCreate;
    OrderedColumns oc = assertTwoCheckpointAsUpdatePopulatedTestTable
        (testTableUnlockedYesAnonCreate,
            false, true, RowFilterScope.Type.DEFAULT.name());

    ArrayList<AuthParamAndOutcome> cases = buildOutcomesListDeleteUnlockedYesAnonCreate();

    for ( AuthParamAndOutcome ap : cases ) {
      if ( ap.rowId.contains("New") ) {
        // expect three row in new_row and two checkpoints in new_row state
        verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 3, FirstSavepointTimestampType
                .NEW_ROW,
            ap.toString());
      } else {
        // expect three rows in synced and two checkpoints in changed state
        verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 3, FirstSavepointTimestampType
                .SYNCED,
            ap.toString());
      }

      try {
        ODKDatabaseImplUtils.get()
            .deleteAllCheckpointRowsWithId(db, ap.tableId, ap.rowId, ap.username, ap.roles);
        assertFalse("Expecting to throw an error: " + ap.toString(), ap.throwsAccessException);

        if ( ap.rowId.contains("New") ) {
          assertTrue("Expected no rows to remain: " + ap.toString(),
              verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 1,
                  FirstSavepointTimestampType.NEW_ROW,
                  ap.toString()));
        } else {
          assertTrue("Expected row to be marked as deleted: " + ap.toString(),
              verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 1, FirstSavepointTimestampType.SYNCED,
                  ap.toString()));
        }

      } catch (ActionNotAuthorizedException ex) {
        assertTrue("Expecting to not throw an error: " + ap.toString(), ap.throwsAccessException);
      }
    }
  }

  public void testDeleteAllCheckpointsAsUpdateLockedNoAnonCreate()
      throws  ActionNotAuthorizedException {

    String tableId = testTableLockedNoAnonCreate;
    OrderedColumns oc = assertTwoCheckpointAsUpdatePopulatedTestTable
        (testTableLockedNoAnonCreate,
            true, false, RowFilterScope.Type.DEFAULT.name());

    ArrayList<AuthParamAndOutcome> cases = buildOutcomesListDeleteLockedNoAnonCreate();

    for ( AuthParamAndOutcome ap : cases ) {
      if ( ap.rowId.contains("New") ) {
        // expect three row in new_row and two checkpoints in new_row state
        verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 3, FirstSavepointTimestampType
                .NEW_ROW,
            ap.toString());
      } else {
        // expect three rows in synced and two checkpoints in changed state
        verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 3, FirstSavepointTimestampType
                .SYNCED,
            ap.toString());
      }

      try {
        ODKDatabaseImplUtils.get()
            .deleteAllCheckpointRowsWithId(db, ap.tableId, ap.rowId, ap.username, ap.roles);
        assertFalse("Expecting to throw an error: " + ap.toString(), ap.throwsAccessException);

        if ( ap.rowId.contains("New") ) {
          assertTrue("Expected no rows to remain: " + ap.toString(),
              verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 1,
                  FirstSavepointTimestampType.NEW_ROW,
                  ap.toString()));
        } else {
          assertTrue("Expected row to be marked as deleted: " + ap.toString(),
              verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 1, FirstSavepointTimestampType.SYNCED,
                  ap.toString()));
        }

      } catch (ActionNotAuthorizedException ex) {
        assertTrue("Expecting to not throw an error: " + ap.toString(), ap.throwsAccessException);
      }
    }
  }

  public void testDeleteAllCheckpointsAsUpdateLockedYesAnonCreate()
      throws  ActionNotAuthorizedException {

    String tableId = testTableLockedYesAnonCreate;
    OrderedColumns oc = assertTwoCheckpointAsUpdatePopulatedTestTable
        (testTableLockedYesAnonCreate,
            true, true, RowFilterScope.Type.DEFAULT.name());

    ArrayList<AuthParamAndOutcome> cases = buildOutcomesListDeleteLockedYesAnonCreate();

    for ( AuthParamAndOutcome ap : cases ) {
      if ( ap.rowId.contains("New") ) {
        // expect three row in new_row and two checkpoints in new_row state
        verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 3, FirstSavepointTimestampType
                .NEW_ROW,
            ap.toString());
      } else {
        // expect three rows in synced and two checkpoints in changed state
        verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 3, FirstSavepointTimestampType
                .SYNCED,
            ap.toString());
      }

      try {
        ODKDatabaseImplUtils.get()
            .deleteAllCheckpointRowsWithId(db, ap.tableId, ap.rowId, ap.username, ap.roles);
        assertFalse("Expecting to throw an error: " + ap.toString(), ap.throwsAccessException);

        if ( ap.rowId.contains("New") ) {
          assertTrue("Expected no rows to remain: " + ap.toString(),
              verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 1,
                  FirstSavepointTimestampType.NEW_ROW,
                  ap.toString()));
        } else {
          assertTrue("Expected row to be marked as deleted: " + ap.toString(),
              verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 1, FirstSavepointTimestampType.SYNCED,
                  ap.toString()));
        }

      } catch (ActionNotAuthorizedException ex) {
        assertTrue("Expecting to not throw an error: " + ap.toString(), ap.throwsAccessException);
      }
    }
  }

  public void testDeleteAllCheckpointsAsUpdateUnlockedNoAnonCreate1()
      throws  ActionNotAuthorizedException {

    String tableId = testTableUnlockedNoAnonCreate;
    OrderedColumns oc = assertOneCheckpointAsUpdatePopulatedTestTable
        (testTableUnlockedNoAnonCreate,
            false, false, RowFilterScope.Type.DEFAULT.name());

    ArrayList<AuthParamAndOutcome> cases = buildOutcomesListDeleteUnlockedNoAnonCreate();

    for ( AuthParamAndOutcome ap : cases ) {
      if ( ap.rowId.contains("New") ) {
        // expect three row in new_row and two checkpoints in new_row state
        verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 2, FirstSavepointTimestampType
                .NEW_ROW,
            ap.toString());
      } else {
        // expect three rows in synced and two checkpoints in changed state
        verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 2, FirstSavepointTimestampType
                .SYNCED,
            ap.toString());
      }

      try {
        ODKDatabaseImplUtils.get()
            .deleteAllCheckpointRowsWithId(db, ap.tableId, ap.rowId, ap.username, ap.roles);
        assertFalse("Expecting to throw an error: " + ap.toString(), ap.throwsAccessException);

        if ( ap.rowId.contains("New") ) {
          assertTrue("Expected no rows to remain: " + ap.toString(),
              verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 1, FirstSavepointTimestampType.NEW_ROW,
                  ap.toString()));
        } else {
          assertTrue("Expected row to be marked as deleted: " + ap.toString(),
              verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 1, FirstSavepointTimestampType.SYNCED,
                  ap.toString()));
        }

      } catch (ActionNotAuthorizedException ex) {
        assertTrue("Expecting to not throw an error: " + ap.toString(), ap.throwsAccessException);
      }
    }
  }

  public void testDeleteAllCheckpointsAsUpdateUnlockedYesAnonCreate1()
      throws  ActionNotAuthorizedException {

    String tableId = testTableUnlockedYesAnonCreate;
    OrderedColumns oc = assertOneCheckpointAsUpdatePopulatedTestTable
        (testTableUnlockedYesAnonCreate,
            false, true, RowFilterScope.Type.DEFAULT.name());

    ArrayList<AuthParamAndOutcome> cases = buildOutcomesListDeleteUnlockedYesAnonCreate();

    for ( AuthParamAndOutcome ap : cases ) {
      if ( ap.rowId.contains("New") ) {
        // expect three row in new_row and two checkpoints in new_row state
        verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 2, FirstSavepointTimestampType
                .NEW_ROW,
            ap.toString());
      } else {
        // expect three rows in synced and two checkpoints in changed state
        verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 2, FirstSavepointTimestampType
                .SYNCED,
            ap.toString());
      }

      try {
        ODKDatabaseImplUtils.get()
            .deleteAllCheckpointRowsWithId(db, ap.tableId, ap.rowId, ap.username, ap.roles);
        assertFalse("Expecting to throw an error: " + ap.toString(), ap.throwsAccessException);

        if ( ap.rowId.contains("New") ) {
          assertTrue("Expected no rows to remain: " + ap.toString(),
              verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 1,
                  FirstSavepointTimestampType.NEW_ROW,
                  ap.toString()));
        } else {
          assertTrue("Expected row to be marked as deleted: " + ap.toString(),
              verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 1, FirstSavepointTimestampType.SYNCED,
                  ap.toString()));
        }

      } catch (ActionNotAuthorizedException ex) {
        assertTrue("Expecting to not throw an error: " + ap.toString(), ap.throwsAccessException);
      }
    }
  }

  public void testDeleteAllCheckpointsAsUpdateLockedNoAnonCreate1()
      throws  ActionNotAuthorizedException {

    String tableId = testTableLockedNoAnonCreate;
    OrderedColumns oc = assertOneCheckpointAsUpdatePopulatedTestTable
        (testTableLockedNoAnonCreate,
            true, false, RowFilterScope.Type.DEFAULT.name());

    ArrayList<AuthParamAndOutcome> cases = buildOutcomesListDeleteLockedNoAnonCreate();

    for ( AuthParamAndOutcome ap : cases ) {
      if ( ap.rowId.contains("New") ) {
        // expect three row in new_row and two checkpoints in new_row state
        verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 2, FirstSavepointTimestampType
                .NEW_ROW,
            ap.toString());
      } else {
        // expect three rows in synced and two checkpoints in changed state
        verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 2, FirstSavepointTimestampType
                .SYNCED,
            ap.toString());
      }

      try {
        ODKDatabaseImplUtils.get()
            .deleteAllCheckpointRowsWithId(db, ap.tableId, ap.rowId, ap.username, ap.roles);
        assertFalse("Expecting to throw an error: " + ap.toString(), ap.throwsAccessException);

        if ( ap.rowId.contains("New") ) {
          assertTrue("Expected no rows to remain: " + ap.toString(),
              verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 1,
                  FirstSavepointTimestampType.NEW_ROW,
                  ap.toString()));
        } else {
          assertTrue("Expected row to be marked as deleted: " + ap.toString(),
              verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 1, FirstSavepointTimestampType.SYNCED,
                  ap.toString()));
        }

      } catch (ActionNotAuthorizedException ex) {
        assertTrue("Expecting to not throw an error: " + ap.toString(), ap.throwsAccessException);
      }
    }
  }

  public void testDeleteAllCheckpointsAsUpdateLockedYesAnonCreate1()
      throws  ActionNotAuthorizedException {

    String tableId = testTableLockedYesAnonCreate;
    OrderedColumns oc = assertOneCheckpointAsUpdatePopulatedTestTable
        (testTableLockedYesAnonCreate,
            true, true, RowFilterScope.Type.DEFAULT.name());

    ArrayList<AuthParamAndOutcome> cases = buildOutcomesListDeleteLockedYesAnonCreate();

    for ( AuthParamAndOutcome ap : cases ) {
      if ( ap.rowId.contains("New") ) {
        // expect three row in new_row and two checkpoints in new_row state
        verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 2, FirstSavepointTimestampType
                .NEW_ROW,
            ap.toString());
      } else {
        // expect three rows in synced and two checkpoints in changed state
        verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 2, FirstSavepointTimestampType
                .SYNCED,
            ap.toString());
      }

      try {
        ODKDatabaseImplUtils.get()
            .deleteAllCheckpointRowsWithId(db, ap.tableId, ap.rowId, ap.username, ap.roles);
        assertFalse("Expecting to throw an error: " + ap.toString(), ap.throwsAccessException);

        if ( ap.rowId.contains("New") ) {
          assertTrue("Expected no rows to remain: " + ap.toString(),
              verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 1,
                  FirstSavepointTimestampType.NEW_ROW,
                  ap.toString()));
        } else {
          assertTrue("Expected row to be marked as deleted: " + ap.toString(),
              verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 1, FirstSavepointTimestampType.SYNCED,
                  ap.toString()));
        }

      } catch (ActionNotAuthorizedException ex) {
        assertTrue("Expecting to not throw an error: " + ap.toString(), ap.throwsAccessException);
      }
    }
  }

  public void testDeleteAllCheckpointsAsUpdateUnlockedNoAnonCreate0()
      throws  ActionNotAuthorizedException {

    String tableId = testTableUnlockedNoAnonCreate;
    OrderedColumns oc = assertPopulatedTestTable
        (testTableUnlockedNoAnonCreate,
            false, false, RowFilterScope.Type.DEFAULT.name());

    ArrayList<AuthParamAndOutcome> cases = buildOutcomesListDeleteUnlockedNoAnonCreate();

    for ( AuthParamAndOutcome ap : cases ) {
      if ( ap.rowId.contains("New") ) {
        // expect three row in new_row and two checkpoints in new_row state
        verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 1, FirstSavepointTimestampType
                .NEW_ROW,
            ap.toString());
      } else {
        // expect three rows in synced and two checkpoints in changed state
        verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 1, FirstSavepointTimestampType
                .SYNCED,
            ap.toString());
      }

      try {
        // should be a no-op
        ODKDatabaseImplUtils.get()
            .deleteAllCheckpointRowsWithId(db, ap.tableId, ap.rowId, ap.username, ap.roles);

        if ( ap.rowId.contains("New") ) {
          assertTrue("Expected no rows to remain: " + ap.toString(),
              verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 1, FirstSavepointTimestampType.NEW_ROW,
                  ap.toString()));
        } else {
          assertTrue("Expected row to be marked as deleted: " + ap.toString(),
              verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 1, FirstSavepointTimestampType.SYNCED,
                  ap.toString()));
        }

      } catch (ActionNotAuthorizedException ex) {
        assertTrue("Expecting to not throw an error: " + ap.toString(), false);
      }
    }
  }

  public void testDeleteAllCheckpointsAsUpdateUnlockedYesAnonCreate0()
      throws  ActionNotAuthorizedException {

    String tableId = testTableUnlockedYesAnonCreate;
    OrderedColumns oc = assertPopulatedTestTable
        (testTableUnlockedYesAnonCreate,
            false, true, RowFilterScope.Type.DEFAULT.name());

    ArrayList<AuthParamAndOutcome> cases = buildOutcomesListDeleteUnlockedYesAnonCreate();

    for ( AuthParamAndOutcome ap : cases ) {
      if ( ap.rowId.contains("New") ) {
        // expect three row in new_row and two checkpoints in new_row state
        verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 1, FirstSavepointTimestampType
                .NEW_ROW,
            ap.toString());
      } else {
        // expect three rows in synced and two checkpoints in changed state
        verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 1, FirstSavepointTimestampType
                .SYNCED,
            ap.toString());
      }

      try {
        // should be a no-op
        ODKDatabaseImplUtils.get()
            .deleteAllCheckpointRowsWithId(db, ap.tableId, ap.rowId, ap.username, ap.roles);

        if ( ap.rowId.contains("New") ) {
          assertTrue("Expected no rows to remain: " + ap.toString(),
              verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 1,
                  FirstSavepointTimestampType.NEW_ROW,
                  ap.toString()));
        } else {
          assertTrue("Expected row to be marked as deleted: " + ap.toString(),
              verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 1, FirstSavepointTimestampType.SYNCED,
                  ap.toString()));
        }

      } catch (ActionNotAuthorizedException ex) {
        assertTrue("Expecting to not throw an error: " + ap.toString(), false);
      }
    }
  }

  public void testDeleteAllCheckpointsAsUpdateLockedNoAnonCreate0()
      throws  ActionNotAuthorizedException {

    String tableId = testTableLockedNoAnonCreate;
    OrderedColumns oc = assertPopulatedTestTable
        (testTableLockedNoAnonCreate,
            true, false, RowFilterScope.Type.DEFAULT.name());

    ArrayList<AuthParamAndOutcome> cases = buildOutcomesListDeleteLockedNoAnonCreate();

    for ( AuthParamAndOutcome ap : cases ) {
      if ( ap.rowId.contains("New") ) {
        // expect three row in new_row and two checkpoints in new_row state
        verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 1, FirstSavepointTimestampType
                .NEW_ROW,
            ap.toString());
      } else {
        // expect three rows in synced and two checkpoints in changed state
        verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 1, FirstSavepointTimestampType
                .SYNCED,
            ap.toString());
      }

      try {
        // should be a no-op
        ODKDatabaseImplUtils.get()
            .deleteAllCheckpointRowsWithId(db, ap.tableId, ap.rowId, ap.username, ap.roles);

        if ( ap.rowId.contains("New") ) {
          assertTrue("Expected no rows to remain: " + ap.toString(),
              verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 1,
                  FirstSavepointTimestampType.NEW_ROW,
                  ap.toString()));
        } else {
          assertTrue("Expected row to be marked as deleted: " + ap.toString(),
              verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 1, FirstSavepointTimestampType.SYNCED,
                  ap.toString()));
        }

      } catch (ActionNotAuthorizedException ex) {
        assertTrue("Expecting to not throw an error: " + ap.toString(), false);
      }
    }
  }

  public void testDeleteAllCheckpointsAsUpdateLockedYesAnonCreate0()
      throws  ActionNotAuthorizedException {

    String tableId = testTableLockedYesAnonCreate;
    OrderedColumns oc = assertPopulatedTestTable
        (testTableLockedYesAnonCreate,
            true, true, RowFilterScope.Type.DEFAULT.name());

    ArrayList<AuthParamAndOutcome> cases = buildOutcomesListDeleteLockedYesAnonCreate();

    for ( AuthParamAndOutcome ap : cases ) {
      if ( ap.rowId.contains("New") ) {
        // expect three row in new_row and two checkpoints in new_row state
        verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 1, FirstSavepointTimestampType
                .NEW_ROW,
            ap.toString());
      } else {
        // expect three rows in synced and two checkpoints in changed state
        verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 1, FirstSavepointTimestampType
                .SYNCED,
            ap.toString());
      }

      try {
        // should be a no-op
        ODKDatabaseImplUtils.get()
            .deleteAllCheckpointRowsWithId(db, ap.tableId, ap.rowId, ap.username, ap.roles);

        if ( ap.rowId.contains("New") ) {
          assertTrue("Expected no rows to remain: " + ap.toString(),
              verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 1,
                  FirstSavepointTimestampType.NEW_ROW,
                  ap.toString()));
        } else {
          assertTrue("Expected row to be marked as deleted: " + ap.toString(),
              verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 1, FirstSavepointTimestampType.SYNCED,
                  ap.toString()));
        }

      } catch (ActionNotAuthorizedException ex) {
        assertTrue("Expecting to not throw an error: " + ap.toString(), false);
      }
    }
  }

  public void testDeleteAllCheckpointsAsInsertUnlockedNoAnonCreate()
      throws  ActionNotAuthorizedException {

    String tableId = testTableUnlockedNoAnonCreate;
    OrderedColumns oc = assertEmptyTestTable(testTableUnlockedNoAnonCreate,
            false, false, RowFilterScope.Type.DEFAULT.name());

    ArrayList<AuthParamAndOutcome> cases = buildOutcomesListDeleteUnlockedNoAnonCreate();

    for ( AuthParamAndOutcome ap : cases ) {
      if ( !ap.rowId.contains("New") ) {
        continue;
      }

      // insert 2 checkpoints
      insert2CheckpointsWithAdminRights(ap, oc);

      // expect two checkpoints in new_row state
      verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 2, FirstSavepointTimestampType
              .CHECKPOINT_NEW_ROW,
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

  public void testDeleteAllCheckpointsAsInsertUnlockedYesAnonCreate()
      throws  ActionNotAuthorizedException {

    String tableId = testTableUnlockedYesAnonCreate;
    OrderedColumns oc = assertEmptyTestTable
        (testTableUnlockedYesAnonCreate,
            false, true, RowFilterScope.Type.DEFAULT.name());

    ArrayList<AuthParamAndOutcome> cases = buildOutcomesListDeleteUnlockedYesAnonCreate();

    for ( AuthParamAndOutcome ap : cases ) {
      if ( !ap.rowId.contains("New") ) {
        continue;
      }

      // insert 2 checkpoints
      insert2CheckpointsWithAdminRights(ap, oc);

      // expect two checkpoints in new_row state
      verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 2, FirstSavepointTimestampType
              .CHECKPOINT_NEW_ROW,
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

  public void testDeleteAllCheckpointsAsInsertLockedNoAnonCreate()
      throws  ActionNotAuthorizedException {

    String tableId = testTableLockedNoAnonCreate;
    OrderedColumns oc = assertEmptyTestTable
        (testTableLockedNoAnonCreate,
            true, false, RowFilterScope.Type.DEFAULT.name());

    ArrayList<AuthParamAndOutcome> cases = buildOutcomesListDeleteLockedNoAnonCreate();

    for ( AuthParamAndOutcome ap : cases ) {
      if ( !ap.rowId.contains("New") ) {
        continue;
      }

      // insert 2 checkpoints
      insert2CheckpointsWithAdminRights(ap, oc);

      // expect two checkpoints in new_row state
      verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 2, FirstSavepointTimestampType
              .CHECKPOINT_NEW_ROW,
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

  public void testDeleteAllCheckpointsAsInsertLockedYesAnonCreate()
      throws  ActionNotAuthorizedException {

    String tableId = testTableLockedYesAnonCreate;
    OrderedColumns oc = assertEmptyTestTable
        (testTableLockedYesAnonCreate,
            true, true, RowFilterScope.Type.DEFAULT.name());

    ArrayList<AuthParamAndOutcome> cases = buildOutcomesListDeleteLockedYesAnonCreate();

    for ( AuthParamAndOutcome ap : cases ) {
      if ( !ap.rowId.contains("New") ) {
        continue;
      }

      // insert 2 checkpoints
      insert2CheckpointsWithAdminRights(ap, oc);

      // expect two checkpoints in new_row state
      verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 2, FirstSavepointTimestampType
              .CHECKPOINT_NEW_ROW,
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

}
