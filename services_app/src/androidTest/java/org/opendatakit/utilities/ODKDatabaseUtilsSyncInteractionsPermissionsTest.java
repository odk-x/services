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

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.opendatakit.aggregate.odktables.rest.ConflictType;
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

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ODKDatabaseUtilsSyncInteractionsPermissionsTest extends AbstractPermissionsTestCase {

  private static final String TAG = "ODKDatabaseUtilsSyncInteractionsPermissionsTest";

  private void base_Type_Users_PerhapsPlaceRowIntoConflict_Table(boolean isLocked,
      boolean canAnonCreate, SyncState localRowSyncState) throws
      ActionNotAuthorizedException {

    base_Type_PerhapsPlaceRowIntoConflict_Table(isLocked, canAnonCreate,
        localRowSyncState,
        false);

    base_Type_PerhapsPlaceRowIntoConflict_Table(isLocked, canAnonCreate,
        localRowSyncState,
        true);
  }

  private void base_Type_PerhapsPlaceRowIntoConflict_Table(boolean isLocked, boolean canAnonCreate,
      SyncState localRowSyncState, boolean asPrivilegedUser) throws
      ActionNotAuthorizedException {

    base_Type_PerhapsPlaceRowIntoConflict_Table_RowFilterScopeType(isLocked, canAnonCreate,
        RowFilterScope.Access.FULL,
        localRowSyncState,
        asPrivilegedUser);

    base_Type_PerhapsPlaceRowIntoConflict_Table_RowFilterScopeType(isLocked, canAnonCreate,
        RowFilterScope.Access.READ_ONLY,
        localRowSyncState,
        asPrivilegedUser);
  }

  private void base_Type_PerhapsPlaceRowIntoConflict_Table_RowFilterScopeType(boolean isLocked,
      boolean canAnonCreate, RowFilterScope.Access serverRowDefaultAccessValue,
      SyncState localRowSyncState, boolean asPrivilegedUser) throws ActionNotAuthorizedException {

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
      // this sets up the key-value-store fields for the table, using the
      // serverRowDefaultAccessValue as the initial value for the default access
      // on rows created in the table.
      OrderedColumns oc = assertEmptySyncStateTestTable(tableId,
          isLocked, canAnonCreate, serverRowDefaultAccessValue.name());

      // outcome depends upon the server row's DefaultAccess value.
      // initially assume this will be the incoming value.
      {
        // loop over rowId :
        ArrayList<SyncParamOutcome> spoList = buildSyncParamOutcomesList(isLocked, asPrivilegedUser,
            localRowSyncState, true, serverRowDefaultAccessValue, options[7], options[0], options[1], options[2],
            options[3], options[4], options[5], options[6]);

        for (SyncParamOutcome spo : spoList) {
          assertRowInSyncStateTestTable(tableId, oc, spo.rowId, localRowSyncState);
          verifySyncOutcome(tableId, oc, asPrivilegedUser, serverRowDefaultAccessValue, spo);
        }
      }

      // outcome depends upon the server row's DefaultAccess value.
      // now limit it to either hidden or read-only.
      // TODO: It is unclear if this is actually testing new code paths, or just duplicated effort.
      {
        RowFilterScope.Access restrictedType = (serverRowDefaultAccessValue == RowFilterScope.Access.READ_ONLY) ?
            RowFilterScope.Access.HIDDEN : RowFilterScope.Access.READ_ONLY;
        // loop over rowId :
        ArrayList<SyncParamOutcome> spoList = buildSyncParamOutcomesList(isLocked, asPrivilegedUser,
            localRowSyncState, true, restrictedType, options[7], options[0], options[1],
            options[2], options[3], options[4], options[5], options[6]);

        for (SyncParamOutcome spo : spoList) {
          if (spo.changeServerPrivilegedMetadata) {
            // if we are modifying the filter scopes, then supply read-only scope as a challenge
            // or, if the incoming type is read-only, use hidden
            assertRowInSyncStateTestTable(tableId, oc, spo.rowId, localRowSyncState);
            verifySyncOutcome(tableId, oc, asPrivilegedUser, restrictedType, spo);
          }
        }
      }
    }
  }

  private void base_Type_UsersConflicting_PerhapsPlaceRowIntoConflict_Table(boolean isLocked,
      boolean canAnonCreate, int localConflictType, int serverConflictType) throws
      ActionNotAuthorizedException {

    base_Type_Conflicting_PerhapsPlaceRowIntoConflict_Table(isLocked, canAnonCreate,
        localConflictType, serverConflictType,
        false);

    base_Type_Conflicting_PerhapsPlaceRowIntoConflict_Table(isLocked, canAnonCreate,
        localConflictType, serverConflictType,
        true);
  }


  private void base_Type_Conflicting_PerhapsPlaceRowIntoConflict_Table(boolean isLocked, boolean canAnonCreate,
      int localConflictType, int serverConflictType, boolean asPrivilegedUser) throws
      ActionNotAuthorizedException {

    base_Type_Conflicting_PerhapsPlaceRowIntoConflict_Table_RowFilterScopeType(isLocked, canAnonCreate,
        RowFilterScope.Access.FULL,
        localConflictType, serverConflictType,
        asPrivilegedUser);

    base_Type_Conflicting_PerhapsPlaceRowIntoConflict_Table_RowFilterScopeType(isLocked, canAnonCreate,
        RowFilterScope.Access.READ_ONLY,
        localConflictType, serverConflictType,
        asPrivilegedUser);
  }

  private void base_Type_Conflicting_PerhapsPlaceRowIntoConflict_Table_RowFilterScopeType(
      boolean isLocked, boolean canAnonCreate, RowFilterScope.Access serverRowDefaultAccessValue,
      int localConflictType, int serverConflictType, boolean asPrivilegedUser) throws
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
      // this sets up the key-value-store fields for the table, using the
      // serverRowDefaultAccessValue as the initial value for the default access
      // on rows created in the table.
      OrderedColumns oc = assertEmptySyncStateTestTable(tableId,
          isLocked, canAnonCreate, serverRowDefaultAccessValue.name());

      // outcome depends upon the server row's DefaultAccess value.
      // initially assume this will be the incoming value.
      {
        // loop over rowId :
        ArrayList<SyncParamOutcome> spoList = buildConflictingSyncParamOutcomesList
            (isLocked, asPrivilegedUser, localConflictType,
            serverRowDefaultAccessValue, options[7], options[0], options[1], options[2], options[3], options[4], options[5],
            options[6]);

        for (SyncParamOutcome spo : spoList) {
          assertInConflictRowInSyncStateTestTable(tableId, oc, spo.rowId, localConflictType,
              serverConflictType);
          verifySyncOutcome(tableId, oc, asPrivilegedUser, serverRowDefaultAccessValue, spo);
        }
      }

      // outcome depends upon the server row's DefaultAccess value.
      // now limit it to either hidden or read-only.
      // TODO: It is unclear if this is actually testing new code paths, or just duplicated effort.
      {
        RowFilterScope.Access unmodifiableType =  (serverRowDefaultAccessValue == RowFilterScope.Access.READ_ONLY) ?
            RowFilterScope.Access.HIDDEN : RowFilterScope.Access.READ_ONLY;
        // loop over rowId :
        ArrayList<SyncParamOutcome> spoList = buildConflictingSyncParamOutcomesList(
            isLocked, asPrivilegedUser,
            localConflictType, unmodifiableType, options[7], options[0], options[1], options[2],
            options[3], options[4], options[5],
            options[6]);

        for (SyncParamOutcome spo : spoList) {
          assertInConflictRowInSyncStateTestTable(tableId, oc, spo.rowId, localConflictType,
              serverConflictType);
          verifySyncOutcome(tableId, oc, asPrivilegedUser,
              unmodifiableType, spo);
        }
      }
    }
  }

  @Test
  public void testPerhapsPlaceRowIntoConflictUnlockedNoAnonCreate_new_row() throws
      ActionNotAuthorizedException {

    base_Type_Users_PerhapsPlaceRowIntoConflict_Table(false, false, SyncState.new_row);
  }

  @Test
  public void testPerhapsPlaceRowIntoConflictUnlockedNoAnonCreate_changed() throws
      ActionNotAuthorizedException {

    base_Type_Users_PerhapsPlaceRowIntoConflict_Table(false, false, SyncState.changed);
  }

  @Test
  public void testPerhapsPlaceRowIntoConflictUnlockedNoAnonCreate_deleted() throws
      ActionNotAuthorizedException {

    base_Type_Users_PerhapsPlaceRowIntoConflict_Table(false, false, SyncState.deleted);
  }

  @Test
  public void testPerhapsPlaceRowIntoConflictUnlockedNoAnonCreate_synced() throws
      ActionNotAuthorizedException {

    base_Type_Users_PerhapsPlaceRowIntoConflict_Table(false, false, SyncState.synced);
  }

  @Test
  public void testPerhapsPlaceRowIntoConflictUnlockedNoAnonCreate_synced_pending_files() throws
      ActionNotAuthorizedException {

    base_Type_Users_PerhapsPlaceRowIntoConflict_Table(false, false, SyncState.synced_pending_files);
  }

  @Test
  public void testPerhapsPlaceRowIntoConflictUnlockedYesAnonCreate_new_row() throws
      ActionNotAuthorizedException {

    base_Type_Users_PerhapsPlaceRowIntoConflict_Table(false, true, SyncState.new_row);
  }

  @Test
  public void testPerhapsPlaceRowIntoConflictUnlockedYesAnonCreate_changed() throws
      ActionNotAuthorizedException {

    base_Type_Users_PerhapsPlaceRowIntoConflict_Table(false, true, SyncState.changed);
  }

  @Test
  public void testPerhapsPlaceRowIntoConflictUnlockedYesAnonCreate_deleted() throws
      ActionNotAuthorizedException {

    base_Type_Users_PerhapsPlaceRowIntoConflict_Table(false, true, SyncState.deleted);
  }

  @Test
  public void testPerhapsPlaceRowIntoConflictUnlockedYesAnonCreate_synced() throws
      ActionNotAuthorizedException {

    base_Type_Users_PerhapsPlaceRowIntoConflict_Table(false, true, SyncState.synced);
  }

  @Test
  public void testPerhapsPlaceRowIntoConflictUnlockedYesAnonCreate_synced_pending_files() throws
      ActionNotAuthorizedException {

    base_Type_Users_PerhapsPlaceRowIntoConflict_Table(false, true, SyncState.synced_pending_files);
  }

  @Test
  public void testPerhapsPlaceRowIntoConflictLockedNoAnonCreate_new_row() throws ActionNotAuthorizedException {

    base_Type_Users_PerhapsPlaceRowIntoConflict_Table(true, false, SyncState.new_row);
  }

  @Test
  public void testPerhapsPlaceRowIntoConflictLockedNoAnonCreate_changed() throws ActionNotAuthorizedException {

    base_Type_Users_PerhapsPlaceRowIntoConflict_Table(true, false, SyncState.changed);
  }

  @Test
  public void testPerhapsPlaceRowIntoConflictLockedNoAnonCreate_deleted() throws ActionNotAuthorizedException {

    base_Type_Users_PerhapsPlaceRowIntoConflict_Table(true, false, SyncState.deleted);
  }

  @Test
  public void testPerhapsPlaceRowIntoConflictLockedNoAnonCreate_synced() throws ActionNotAuthorizedException {

    base_Type_Users_PerhapsPlaceRowIntoConflict_Table(true, false, SyncState.synced);
  }

  @Test
  public void testPerhapsPlaceRowIntoConflictLockedNoAnonCreate_synced_pending_files() throws
      ActionNotAuthorizedException {

    base_Type_Users_PerhapsPlaceRowIntoConflict_Table(true, false, SyncState.synced_pending_files);
  }

  @Test
  public void testPerhapsPlaceRowIntoConflictLockedYesAnonCreate_new_row() throws ActionNotAuthorizedException {

    base_Type_Users_PerhapsPlaceRowIntoConflict_Table(true, true, SyncState.new_row);
  }

  @Test
  public void testPerhapsPlaceRowIntoConflictLockedYesAnonCreate_changed() throws ActionNotAuthorizedException {

    base_Type_Users_PerhapsPlaceRowIntoConflict_Table(true, true, SyncState.changed);
  }

  @Test
  public void testPerhapsPlaceRowIntoConflictLockedYesAnonCreate_deleted() throws ActionNotAuthorizedException {

    base_Type_Users_PerhapsPlaceRowIntoConflict_Table(true, true, SyncState.deleted);
  }

  @Test
  public void testPerhapsPlaceRowIntoConflictLockedYesAnonCreate_synced() throws ActionNotAuthorizedException {

    base_Type_Users_PerhapsPlaceRowIntoConflict_Table(true, true, SyncState.synced);
  }

  @Test
  public void testPerhapsPlaceRowIntoConflictLockedYesAnonCreate_synced_pending_files() throws
      ActionNotAuthorizedException {

    base_Type_Users_PerhapsPlaceRowIntoConflict_Table(true, true, SyncState.synced_pending_files);
  }

  //===============================

  @Test
  public void testConflictingPerhapsPlaceRowIntoConflictUnlockedNoAnonCreate_U_U() throws
      ActionNotAuthorizedException {

    base_Type_UsersConflicting_PerhapsPlaceRowIntoConflict_Table(false, false,
        ConflictType.LOCAL_UPDATED_UPDATED_VALUES, ConflictType.SERVER_UPDATED_UPDATED_VALUES);
  }

  @Test
  public void testConflictingPerhapsPlaceRowIntoConflictUnlockedNoAnonCreate_U_D() throws
      ActionNotAuthorizedException {

    base_Type_UsersConflicting_PerhapsPlaceRowIntoConflict_Table(false, false,
        ConflictType.LOCAL_UPDATED_UPDATED_VALUES, ConflictType.SERVER_DELETED_OLD_VALUES);
  }

  @Test
  public void testConflictingPerhapsPlaceRowIntoConflictUnlockedNoAnonCreate_D_U() throws
      ActionNotAuthorizedException {

    base_Type_UsersConflicting_PerhapsPlaceRowIntoConflict_Table(false, false,
        ConflictType.LOCAL_DELETED_OLD_VALUES, ConflictType.SERVER_UPDATED_UPDATED_VALUES);
  }

  @Test
  public void testConflictingPerhapsPlaceRowIntoConflictUnlockedNoAnonCreate_D_D() throws
      ActionNotAuthorizedException {

    base_Type_UsersConflicting_PerhapsPlaceRowIntoConflict_Table(false, false,
        ConflictType.LOCAL_DELETED_OLD_VALUES, ConflictType.SERVER_DELETED_OLD_VALUES);
  }



  @Test
  public void testConflictingPerhapsPlaceRowIntoConflictUnlockedYesAnonCreate_U_U() throws
      ActionNotAuthorizedException {

    base_Type_UsersConflicting_PerhapsPlaceRowIntoConflict_Table(false, true,
        ConflictType.LOCAL_UPDATED_UPDATED_VALUES, ConflictType.SERVER_UPDATED_UPDATED_VALUES);
  }

  @Test
  public void testConflictingPerhapsPlaceRowIntoConflictUnlockedYesAnonCreate_U_D() throws
      ActionNotAuthorizedException {

    base_Type_UsersConflicting_PerhapsPlaceRowIntoConflict_Table(false, true,
        ConflictType.LOCAL_UPDATED_UPDATED_VALUES, ConflictType.SERVER_DELETED_OLD_VALUES);
  }

  @Test
  public void testConflictingPerhapsPlaceRowIntoConflictUnlockedYesAnonCreate_D_U() throws
      ActionNotAuthorizedException {

    base_Type_UsersConflicting_PerhapsPlaceRowIntoConflict_Table(false, true,
        ConflictType.LOCAL_DELETED_OLD_VALUES, ConflictType.SERVER_UPDATED_UPDATED_VALUES);
  }

  @Test
  public void testConflictingPerhapsPlaceRowIntoConflictUnlockedYesAnonCreate_D_D() throws
      ActionNotAuthorizedException {

    base_Type_UsersConflicting_PerhapsPlaceRowIntoConflict_Table(false, true,
        ConflictType.LOCAL_DELETED_OLD_VALUES, ConflictType.SERVER_DELETED_OLD_VALUES);
  }



  @Test
  public void testConflictingPerhapsPlaceRowIntoConflictLockedNoAnonCreate_U_U() throws
      ActionNotAuthorizedException {

    base_Type_UsersConflicting_PerhapsPlaceRowIntoConflict_Table(true, false,
        ConflictType.LOCAL_UPDATED_UPDATED_VALUES, ConflictType.SERVER_UPDATED_UPDATED_VALUES);
  }

  @Test
  public void testConflictingPerhapsPlaceRowIntoConflictLockedNoAnonCreate_U_D() throws
      ActionNotAuthorizedException {

    base_Type_UsersConflicting_PerhapsPlaceRowIntoConflict_Table(true, false,
        ConflictType.LOCAL_UPDATED_UPDATED_VALUES, ConflictType.SERVER_DELETED_OLD_VALUES);
  }

  @Test
  public void testConflictingPerhapsPlaceRowIntoConflictLockedNoAnonCreate_D_U() throws
      ActionNotAuthorizedException {

    base_Type_UsersConflicting_PerhapsPlaceRowIntoConflict_Table(true, false,
        ConflictType.LOCAL_DELETED_OLD_VALUES, ConflictType.SERVER_UPDATED_UPDATED_VALUES);
  }

  @Test
  public void testConflictingPerhapsPlaceRowIntoConflictLockedNoAnonCreate_D_D() throws
      ActionNotAuthorizedException {

    base_Type_UsersConflicting_PerhapsPlaceRowIntoConflict_Table(true, false,
        ConflictType.LOCAL_DELETED_OLD_VALUES, ConflictType.SERVER_DELETED_OLD_VALUES);
  }




  @Test
  public void testConflictingPerhapsPlaceRowIntoConflictLockedYesAnonCreate_U_U() throws
      ActionNotAuthorizedException {

    base_Type_UsersConflicting_PerhapsPlaceRowIntoConflict_Table(true, true,
        ConflictType.LOCAL_UPDATED_UPDATED_VALUES, ConflictType.SERVER_UPDATED_UPDATED_VALUES);
  }

  @Test
  public void testConflictingPerhapsPlaceRowIntoConflictLockedYesAnonCreate_U_D() throws
      ActionNotAuthorizedException {

    base_Type_UsersConflicting_PerhapsPlaceRowIntoConflict_Table(true, true,
        ConflictType.LOCAL_UPDATED_UPDATED_VALUES, ConflictType.SERVER_DELETED_OLD_VALUES);
  }

  @Test
  public void testConflictingPerhapsPlaceRowIntoConflictLockedYesAnonCreate_D_U() throws
      ActionNotAuthorizedException {

    base_Type_UsersConflicting_PerhapsPlaceRowIntoConflict_Table(true, true,
        ConflictType.LOCAL_DELETED_OLD_VALUES, ConflictType.SERVER_UPDATED_UPDATED_VALUES);
  }

  @Test
  public void testConflictingPerhapsPlaceRowIntoConflictLockedYesAnonCreate_D_D() throws
      ActionNotAuthorizedException {

    base_Type_UsersConflicting_PerhapsPlaceRowIntoConflict_Table(true, true,
        ConflictType.LOCAL_DELETED_OLD_VALUES, ConflictType.SERVER_DELETED_OLD_VALUES);
  }

}
