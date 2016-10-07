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

import org.opendatakit.aggregate.odktables.rest.SyncState;
import org.opendatakit.aggregate.odktables.rest.entity.RowFilterScope;
import org.opendatakit.database.data.OrderedColumns;
import org.opendatakit.exception.ActionNotAuthorizedException;

import java.util.ArrayList;

/**
 * Verifies the privilegedPerhapsPlaceRowIntoConflictWithId API in the database.
 *
 * @author mitchellsundt@gmail.com
 */
public class ODKDatabaseUtilsSyncInteractionsPermissionsTest extends AbstractPermissionsTestCase {

  private static final String TAG = "ODKDatabaseUtilsSyncInteractionsPermissionsTest";

  private void base_Type_PerhapsPlaceRowIntoConflict_Table(boolean isLocked, boolean canAnonCreate,
      RowFilterScope.Type type, SyncState localRowSyncState, boolean asPrivilegedUser) throws
      ActionNotAuthorizedException {

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

    Boolean[] options = new Boolean[]{ false, false, false, false, false, false, false, false};

    ArrayList<Integer> testVector = new ArrayList<Integer>();
    int maskChangesInt = (1 << (options.length-1)) -1;
    int maskDeleted = 1 << (options.length-1);
    testVector.add(0);
    testVector.add(0 | maskDeleted);
    testVector.add(maskChangesInt);
    testVector.add(maskChangesInt | maskDeleted);
    for ( int i = 0 ; i < options.length-1 ; ++i ) {
      testVector.add(1 << i);
      testVector.add((1 << i) | maskDeleted);
      testVector.add(maskChangesInt ^ (1 << i));
      testVector.add((maskChangesInt ^ (1 << i)) | maskDeleted);
    }

    for ( Integer i : testVector ) {
      for ( int pos = 0 ; pos < options.length ; ++pos ) {
        options[pos] = (i & (1 << pos)) != 0;
      }

      // add local content
      OrderedColumns oc = assertPopulatedSyncStateTestTable(tableId,
          isLocked, canAnonCreate, type.name(), localRowSyncState);

      // loop over rowId :
      ArrayList<SyncParamOutcome> spoList = buildSyncParamOutcomesList
          (asPrivilegedUser, localRowSyncState,
              options[7], options[0], options[1], options[2], options[3], options[4], options[5],
              options[6] );

      for ( SyncParamOutcome spo : spoList ) {
        verifySyncOutcome(tableId, oc, asPrivilegedUser, spo);
      }
    }
  }

  public void testPerhapsPlaceRowIntoConflictUnlockedNoAnonCreate_new_row() throws
      ActionNotAuthorizedException {

    base_Type_PerhapsPlaceRowIntoConflict_Table(false, false, RowFilterScope.Type.DEFAULT, SyncState.new_row,
        true);
    base_Type_PerhapsPlaceRowIntoConflict_Table(false, false, RowFilterScope.Type.DEFAULT, SyncState.new_row,
        false);
  }

  public void testPerhapsPlaceRowIntoConflictUnlockedNoAnonCreate_changed() throws
      ActionNotAuthorizedException {

    base_Type_PerhapsPlaceRowIntoConflict_Table(false, false, RowFilterScope.Type.DEFAULT, SyncState.changed, true);
    base_Type_PerhapsPlaceRowIntoConflict_Table(false, false, RowFilterScope.Type.DEFAULT, SyncState.changed, false);
  }

  public void testPerhapsPlaceRowIntoConflictUnlockedNoAnonCreate_deleted() throws
      ActionNotAuthorizedException {

    base_Type_PerhapsPlaceRowIntoConflict_Table(false, false, RowFilterScope.Type.DEFAULT, SyncState.deleted, true);
    base_Type_PerhapsPlaceRowIntoConflict_Table(false, false, RowFilterScope.Type.DEFAULT, SyncState.deleted, false);
  }

  public void testPerhapsPlaceRowIntoConflictUnlockedNoAnonCreate_synced() throws
      ActionNotAuthorizedException {

    base_Type_PerhapsPlaceRowIntoConflict_Table(false, false, RowFilterScope.Type.DEFAULT, SyncState.synced, true);
    base_Type_PerhapsPlaceRowIntoConflict_Table(false, false, RowFilterScope.Type.DEFAULT, SyncState.synced, false);
  }

  public void testPerhapsPlaceRowIntoConflictUnlockedNoAnonCreate_synced_pending_files() throws
      ActionNotAuthorizedException {

    base_Type_PerhapsPlaceRowIntoConflict_Table(false, false, RowFilterScope.Type.DEFAULT, SyncState.synced_pending_files, true);
    base_Type_PerhapsPlaceRowIntoConflict_Table(false, false, RowFilterScope.Type.DEFAULT, SyncState.synced_pending_files, false);
  }

  public void testPerhapsPlaceRowIntoConflictUnlockedYesAnonCreate_new_row() throws
      ActionNotAuthorizedException {

    base_Type_PerhapsPlaceRowIntoConflict_Table(false, true, RowFilterScope.Type.DEFAULT, SyncState.new_row, true);
    base_Type_PerhapsPlaceRowIntoConflict_Table(false, true, RowFilterScope.Type.DEFAULT, SyncState.new_row, false);
  }

  public void testPerhapsPlaceRowIntoConflictUnlockedYesAnonCreate_changed() throws
      ActionNotAuthorizedException {

    base_Type_PerhapsPlaceRowIntoConflict_Table(false, true, RowFilterScope.Type.DEFAULT, SyncState.changed, true);
    base_Type_PerhapsPlaceRowIntoConflict_Table(false, true, RowFilterScope.Type.DEFAULT, SyncState.changed, false);
  }

  public void testPerhapsPlaceRowIntoConflictUnlockedYesAnonCreate_deleted() throws
      ActionNotAuthorizedException {

    base_Type_PerhapsPlaceRowIntoConflict_Table(false, true, RowFilterScope.Type.DEFAULT, SyncState.deleted, true);
    base_Type_PerhapsPlaceRowIntoConflict_Table(false, true, RowFilterScope.Type.DEFAULT, SyncState.deleted, false);
  }

  public void testPerhapsPlaceRowIntoConflictUnlockedYesAnonCreate_synced() throws
      ActionNotAuthorizedException {

    base_Type_PerhapsPlaceRowIntoConflict_Table(false, true, RowFilterScope.Type.DEFAULT, SyncState.synced, true);
    base_Type_PerhapsPlaceRowIntoConflict_Table(false, true, RowFilterScope.Type.DEFAULT, SyncState.synced, false);
  }

  public void testPerhapsPlaceRowIntoConflictUnlockedYesAnonCreate_synced_pending_files() throws
      ActionNotAuthorizedException {

    base_Type_PerhapsPlaceRowIntoConflict_Table(false, true, RowFilterScope.Type.DEFAULT, SyncState.synced_pending_files, true);
    base_Type_PerhapsPlaceRowIntoConflict_Table(false, true, RowFilterScope.Type.DEFAULT, SyncState.synced_pending_files, false);
  }

  public void testPerhapsPlaceRowIntoConflictLockedNoAnonCreate_new_row() throws ActionNotAuthorizedException {

    base_Type_PerhapsPlaceRowIntoConflict_Table(true, false, RowFilterScope.Type.DEFAULT, SyncState.new_row, true);
    base_Type_PerhapsPlaceRowIntoConflict_Table(true, false, RowFilterScope.Type.DEFAULT, SyncState.new_row, false);
  }

  public void testPerhapsPlaceRowIntoConflictLockedNoAnonCreate_changed() throws ActionNotAuthorizedException {

    base_Type_PerhapsPlaceRowIntoConflict_Table(true, false, RowFilterScope.Type.DEFAULT, SyncState.changed, true);
    base_Type_PerhapsPlaceRowIntoConflict_Table(true, false, RowFilterScope.Type.DEFAULT, SyncState.changed, false);
  }

  public void testPerhapsPlaceRowIntoConflictLockedNoAnonCreate_deleted() throws ActionNotAuthorizedException {

    base_Type_PerhapsPlaceRowIntoConflict_Table(true, false, RowFilterScope.Type.DEFAULT, SyncState.deleted, true);
    base_Type_PerhapsPlaceRowIntoConflict_Table(true, false, RowFilterScope.Type.DEFAULT, SyncState.deleted, false);
  }

  public void testPerhapsPlaceRowIntoConflictLockedNoAnonCreate_synced() throws ActionNotAuthorizedException {

    base_Type_PerhapsPlaceRowIntoConflict_Table(true, false, RowFilterScope.Type.DEFAULT, SyncState.synced, true);
    base_Type_PerhapsPlaceRowIntoConflict_Table(true, false, RowFilterScope.Type.DEFAULT, SyncState.synced, false);
  }

  public void testPerhapsPlaceRowIntoConflictLockedNoAnonCreate_synced_pending_files() throws
      ActionNotAuthorizedException {

    base_Type_PerhapsPlaceRowIntoConflict_Table(true, false, RowFilterScope.Type.DEFAULT, SyncState.synced_pending_files, true);
    base_Type_PerhapsPlaceRowIntoConflict_Table(true, false, RowFilterScope.Type.DEFAULT, SyncState.synced_pending_files, false);
  }

  public void testPerhapsPlaceRowIntoConflictLockedYesAnonCreate_new_row() throws ActionNotAuthorizedException {

    base_Type_PerhapsPlaceRowIntoConflict_Table(true, true, RowFilterScope.Type.DEFAULT, SyncState.new_row, true);
    base_Type_PerhapsPlaceRowIntoConflict_Table(true, true, RowFilterScope.Type.DEFAULT, SyncState.new_row, false);
  }

  public void testPerhapsPlaceRowIntoConflictLockedYesAnonCreate_changed() throws ActionNotAuthorizedException {

    base_Type_PerhapsPlaceRowIntoConflict_Table(true, true, RowFilterScope.Type.DEFAULT, SyncState.changed, true);
    base_Type_PerhapsPlaceRowIntoConflict_Table(true, true, RowFilterScope.Type.DEFAULT, SyncState.changed, false);
  }

  public void testPerhapsPlaceRowIntoConflictLockedYesAnonCreate_deleted() throws ActionNotAuthorizedException {

    base_Type_PerhapsPlaceRowIntoConflict_Table(true, true, RowFilterScope.Type.DEFAULT, SyncState.deleted, true);
    base_Type_PerhapsPlaceRowIntoConflict_Table(true, true, RowFilterScope.Type.DEFAULT, SyncState.deleted, false);
  }

  public void testPerhapsPlaceRowIntoConflictLockedYesAnonCreate_synced() throws ActionNotAuthorizedException {

    base_Type_PerhapsPlaceRowIntoConflict_Table(true, true, RowFilterScope.Type.DEFAULT, SyncState.synced, true);
    base_Type_PerhapsPlaceRowIntoConflict_Table(true, true, RowFilterScope.Type.DEFAULT, SyncState.synced, false);
  }

  public void testPerhapsPlaceRowIntoConflictLockedYesAnonCreate_synced_pending_files() throws
      ActionNotAuthorizedException {

    base_Type_PerhapsPlaceRowIntoConflict_Table(true, true, RowFilterScope.Type.DEFAULT, SyncState
        .synced_pending_files, true);
    base_Type_PerhapsPlaceRowIntoConflict_Table(true, true, RowFilterScope.Type.DEFAULT, SyncState.synced_pending_files, false);
  }

}
