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

import android.content.ContentValues;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.opendatakit.aggregate.odktables.rest.ConflictType;
import org.opendatakit.aggregate.odktables.rest.entity.RowFilterScope;
import org.opendatakit.database.data.OrderedColumns;
import org.opendatakit.exception.ActionNotAuthorizedException;
import org.opendatakit.services.database.utilities.ODKDatabaseImplUtils;

import java.util.ArrayList;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Permissions tests in the database.
 *
 * These are specific to the functionality that can occur during sync and conflict resolution
 *
 */

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ODKDatabaseUtilsConflictInteractionsPermissionsTest extends AbstractPermissionsTestCase {

  private static final String TAG = "ODKDatabaseUtilsConflictInteractionsPermissionsTest";

  private void base_Type_ResolveLocalRow_Table(boolean isLocked, boolean canAnonCreate,
      RowFilterScope.Access access) {

    String tableId;
    if ( isLocked ) {
      if ( canAnonCreate ) {
        tableId = testTableLockedYesAnonCreate;
      } else {
        tableId = testTableLockedNoAnonCreate;
      }
    } else {
      if ( canAnonCreate ) {
        tableId = testTableUnlockedYesAnonCreate;
      } else {
        tableId = testTableUnlockedNoAnonCreate;
      }
    }

    OrderedColumns oc = assertConflictPopulatedTestTable(tableId,
        isLocked, canAnonCreate, access.name());

    ArrayList<AuthParamAndOutcome> cases = buildOutcomesListResolveTakeLocal(tableId, isLocked);

    for ( AuthParamAndOutcome ap : cases ) {
      try {
        // expect one row in synced state
        verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 2, FirstSavepointTimestampType.IN_CONFLICT,
            ap.toString());

        // should always succeed
        ODKDatabaseImplUtils.get()
            .resolveServerConflictTakeLocalRowWithId(db, ap.tableId, ap.rowId, ap.username, ap.roles, currentLocale);
        assertFalse("Expecting to throw an error: " + ap.toString(), ap.throwsAccessException);

        if (ap.rowId.contains("" + ConflictType.LOCAL_DELETED_OLD_VALUES)) {
          assertTrue("Expected no rows to remain: " + ap.toString(),
              verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 1,
                  FirstSavepointTimestampType.DELETED, ap.toString()));
        } else {
          assertTrue("Expected row to be marked as changed: " + ap.toString(),
              verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 1,
                  FirstSavepointTimestampType.CHANGED, ap.toString()));
        }

      } catch (ActionNotAuthorizedException ex ) {
        assertTrue("Expecting to not throw an error: " + ex.toString() +" for " + ap.toString(),
            ap.throwsAccessException);
      } catch ( Exception ex ) {
          fail("Unexpected exception: " + ex.toString() + " for " + ap.toString());
      }
    }
  }

  private void base_Type_ResolveLocalRowWithServerChanges_Table(boolean isLocked,
      boolean canAnonCreate, RowFilterScope.Access access) {

    String tableId;
    if ( isLocked ) {
      if ( canAnonCreate ) {
        tableId = testTableLockedYesAnonCreate;
      } else {
        tableId = testTableLockedNoAnonCreate;
      }
    } else {
      if ( canAnonCreate ) {
        tableId = testTableUnlockedYesAnonCreate;
      } else {
        tableId = testTableUnlockedNoAnonCreate;
      }
    }

    OrderedColumns oc = assertConflictPopulatedTestTable(tableId,
        isLocked, canAnonCreate, access.name());

    ArrayList<AuthParamAndOutcome> cases = buildOutcomesListResolveTakeLocal(tableId, isLocked);

    for ( AuthParamAndOutcome ap : cases ) {
      try {
        if (ap.rowId.contains("" + ConflictType.LOCAL_DELETED_OLD_VALUES ) ) {
          // if we are deleting locally, blending makes no sense.
          continue;
        }

        // expect one row in synced state
        verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 2, FirstSavepointTimestampType
                .IN_CONFLICT,
            ap.toString());

        ContentValues cvValues = new ContentValues();
        cvValues.put("col3", 3.3); // number;

        // should always succeed
        ODKDatabaseImplUtils.get()
            .resolveServerConflictTakeLocalRowPlusServerDeltasWithId(db, ap.tableId, cvValues,
                ap.rowId,
                ap.username, ap.roles, currentLocale);
        assertFalse("Expecting to throw an error: " + ap.toString(), ap.throwsAccessException);

        if ( ap.rowId.contains("" + ConflictType.LOCAL_DELETED_OLD_VALUES) ) {
          assertTrue("Expected no rows to remain: " + ap.toString(),
              verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 1,
                  FirstSavepointTimestampType.DELETED,
                  ap.toString()));
        } else {
          assertTrue("Expected row to be marked as changed: " + ap.toString(),
              verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 1,
                  FirstSavepointTimestampType.CHANGED,
                  ap.toString()));
        }

      } catch (ActionNotAuthorizedException ex ) {
        assertTrue("Expecting to not throw an error: " + ex.toString() +" for " + ap.toString(),
            ap.throwsAccessException);
      } catch ( Exception ex ) {
          fail("Unexpected exception: " + ex.toString() + " for " + ap.toString());
      }
    }
  }

  private void base_Type_ResolveServerRow_Table(boolean isLocked, boolean canAnonCreate,
      RowFilterScope.Access access) {

    String tableId;
    if ( isLocked ) {
      if ( canAnonCreate ) {
        tableId = testTableLockedYesAnonCreate;
      } else {
        tableId = testTableLockedNoAnonCreate;
      }
    } else {
      if ( canAnonCreate ) {
        tableId = testTableUnlockedYesAnonCreate;
      } else {
        tableId = testTableUnlockedNoAnonCreate;
      }
    }

    OrderedColumns oc = assertConflictPopulatedTestTable(tableId,
        isLocked, canAnonCreate, access.name());

    ArrayList<AuthParamAndOutcome> cases = buildOutcomesListResolveTakeServer(tableId);

    for ( AuthParamAndOutcome ap : cases ) {
      try {
        // expect one row in synced state
        verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 2, FirstSavepointTimestampType
                .IN_CONFLICT,
            ap.toString());

        // should always succeed
        ODKDatabaseImplUtils.get()
            .resolveServerConflictTakeServerRowWithId(db, ap.tableId, ap.rowId, ap.username, ap.roles);
        assertFalse("Expecting to throw an error: " + ap.toString(), ap.throwsAccessException);

        if ( ap.rowId.contains("" + ConflictType.SERVER_DELETED_OLD_VALUES) ) {
          assertTrue("Expected no rows to remain: " + ap.toString(),
              verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 0, FirstSavepointTimestampType.NEW_ROW,
                  ap.toString()));
        } else {
          assertTrue("Expected row to be marked as deleted: " + ap.toString(),
              verifyRowSyncStateAndCheckpoints(ap.tableId, ap.rowId, 1,
                  FirstSavepointTimestampType.SYNCED_PENDING_FILES,
                  ap.toString()));
        }

      } catch ( Exception ex ) {
          fail("Unexpected exception: " + ex.toString() + " for " + ap.toString());
      }
    }
  }

  @Test
  public void testResolveLocalRowUnlockedNoAnonCreate() {

    base_Type_ResolveLocalRow_Table(false, false, RowFilterScope.Access.FULL);
  }

  @Test
  public void testResolveLocalRowUnlockedYesAnonCreate() {

    base_Type_ResolveLocalRow_Table(false, true, RowFilterScope.Access.FULL);
  }

  @Test
  public void testResolveLocalRowLockedNoAnonCreate() {

    base_Type_ResolveLocalRow_Table(true, false, RowFilterScope.Access.FULL);
  }

  @Test
  public void testResolveLocalRowLockedYesAnonCreate() {

    base_Type_ResolveLocalRow_Table(true, true, RowFilterScope.Access.FULL);
  }

  ///////////////////

  @Test
  public void testResolveLocalRowWithServerChangesUnlockedNoAnonCreate() {

    base_Type_ResolveLocalRowWithServerChanges_Table(false, false, RowFilterScope.Access.FULL);
  }

  @Test
  public void testResolveLocalRowWithServerChangesUnlockedYesAnonCreate() {

    base_Type_ResolveLocalRowWithServerChanges_Table(false, true, RowFilterScope.Access.FULL);
  }

  @Test
  public void testResolveLocalRowWithServerChangesLockedNoAnonCreate() {

    base_Type_ResolveLocalRowWithServerChanges_Table(true, false, RowFilterScope.Access.FULL);
  }

  @Test
  public void testResolveLocalRowWithServerChangesLockedYesAnonCreate() {

    base_Type_ResolveLocalRowWithServerChanges_Table(true, true, RowFilterScope.Access.FULL);
  }

  ///////////////////

  @Test
  public void testResolveServerRowUnlockedNoAnonCreate() {

    base_Type_ResolveServerRow_Table(false, false, RowFilterScope.Access.FULL);
  }

  @Test
  public void testResolveServerRowUnlockedYesAnonCreate() {

    base_Type_ResolveServerRow_Table(false, true, RowFilterScope.Access.FULL);
  }

  @Test
  public void testResolveServerRowLockedNoAnonCreate() {

    base_Type_ResolveServerRow_Table(true, false, RowFilterScope.Access.FULL);
  }

  @Test
  public void testResolveServerRowLockedYesAnonCreate() {

    base_Type_ResolveServerRow_Table(true, true, RowFilterScope.Access.FULL);
  }
}
