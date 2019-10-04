/*
 * Copyright (C) 2015 University of Washington
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.opendatakit.services.resolve.conflict;

import android.content.Context;
import android.database.Cursor;

import androidx.loader.content.AsyncTaskLoader;

import org.opendatakit.aggregate.odktables.rest.ConflictType;
import org.opendatakit.aggregate.odktables.rest.KeyValueStoreConstants;
import org.opendatakit.aggregate.odktables.rest.SyncState;
import org.opendatakit.database.DatabaseConstants;
import org.opendatakit.database.RoleConsts;
import org.opendatakit.database.data.BaseTable;
import org.opendatakit.database.data.KeyValueStoreEntry;
import org.opendatakit.database.data.OrderedColumns;
import org.opendatakit.database.data.TypedRow;
import org.opendatakit.database.data.UserTable;
import org.opendatakit.database.service.DbHandle;
import org.opendatakit.database.utilities.QueryUtil;
import org.opendatakit.logging.WebLogger;
import org.opendatakit.provider.DataTableColumns;
import org.opendatakit.provider.FormsColumns;
import org.opendatakit.services.database.OdkConnectionFactorySingleton;
import org.opendatakit.services.database.OdkConnectionInterface;
import org.opendatakit.services.database.utilities.ODKDatabaseImplUtils;
import org.opendatakit.services.resolve.views.components.ResolveActionList;
import org.opendatakit.services.resolve.views.components.ResolveRowEntry;
import org.opendatakit.services.utilities.ActiveUserAndLocale;
import org.opendatakit.utilities.LocalizationUtils;
import org.opendatakit.utilities.NameUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * @author mitchellsundt@gmail.com
 */
class OdkResolveConflictRowLoader extends AsyncTaskLoader<ArrayList<ResolveRowEntry>> {

  private final String mAppName;
  private final String mTableId;
  private final boolean mHaveResolvedMetadataConflicts;
  private int mNumberRowsSilentlyResolved = 0;

  private static class FormDefinition {
    String instanceName;
    String formDisplayName;
    String formId;
  }

  public OdkResolveConflictRowLoader(Context context, String appName, String tableId,
      boolean haveResolvedMetadataConflicts) {
    super(context);
    this.mAppName = appName;
    this.mTableId = tableId;
    this.mHaveResolvedMetadataConflicts = haveResolvedMetadataConflicts;
  }

  public int getNumberRowsSilentlyResolved() {
    return mNumberRowsSilentlyResolved;
  }

  @Override public ArrayList<ResolveRowEntry> loadInBackground() {

    OdkConnectionInterface db = null;

    ActiveUserAndLocale aul =
        ActiveUserAndLocale.getActiveUserAndLocale(getContext(), mAppName);

    DbHandle dbHandleName = new DbHandle(UUID.randomUUID().toString());

    ArrayList<FormDefinition> formDefinitions = new ArrayList<FormDefinition>();
    String tableDisplayName = null;
    Cursor forms = null;
    UserTable table = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(mAppName, dbHandleName);

      OrderedColumns orderedDefns = ODKDatabaseImplUtils.get()
          .getUserDefinedColumns(db, mTableId);
      String whereClause = DataTableColumns.CONFLICT_TYPE + " IN ( ?, ?)";
      Object[] selectionArgs = new Object[] {
          ConflictType.LOCAL_DELETED_OLD_VALUES, ConflictType.LOCAL_UPDATED_UPDATED_VALUES };
      String[] groupBy = { DataTableColumns.ID };
      String[] orderByKeys = new String[] { DataTableColumns.SAVEPOINT_TIMESTAMP };
      String[] orderByDir = new String[] { "DESC" };
      List<String> adminColumns = ODKDatabaseImplUtils.get().getAdminColumns();
      String[] adminColArr = adminColumns.toArray(new String[adminColumns.size()]);

      ODKDatabaseImplUtils.AccessContext accessContextBase =
          ODKDatabaseImplUtils.get().getAccessContext(db, mTableId, aul.activeUser,
              aul.rolesList);

      ODKDatabaseImplUtils.AccessContext accessContextPrivileged =
          ODKDatabaseImplUtils.get().getAccessContext(db, mTableId, aul.activeUser,
              RoleConsts.ADMIN_ROLES_LIST);

      BaseTable baseTable = ODKDatabaseImplUtils.get().privilegedQuery(db, mTableId, QueryUtil
              .buildSqlStatement(mTableId, whereClause, groupBy, null, orderByKeys, orderByDir),
          selectionArgs, null, accessContextPrivileged);
      table = new UserTable(baseTable, orderedDefns, adminColArr);

      // NOTE: Logic is slightly different from checkpoints, as the privilege-change logic below
      // may alter the permissions fields of a row (force the server's permissions into the local
      // change).  We therefore make the if-no-changes clean-up after this logic, rather than
      // before it.

      // Now run a similar query, but pulling the server conflict records as an unprivileged
      // query (with the current user's permissions). This gets the effectiveAccess (**from the
      // server change**) that the user would have when resolving a conflict. If the user does
      // not have "w" access and is trying to do a local update, or if the user does not have
      // "d" access and is trying to do a local delete, then the conflict should be resolved by
      // taking the server's change.
      //
      // i.e., the server conflict row imposes its privilege restrictions prior to consideration
      // of the user's row changes. So a change from the server can revoke privileges for a
      // class of users, and, when resolving conflicts, any users whose privileges have been
      // revoked should be forced to take the server's changes. This happens implicitly in the
      // ODKDatabaseImplUtils.privilegedPerhapsPlaceRowIntoConflictWithId() method. We apply
      // that same logic here to handle loss-of-privilege between the time of Sync and the time
      // we hit this conflict-resolution screen.
      //
      // We also enforce permissions-change restrictions on all of the local conflicts. This
      // makes it necessary to always re-fetch the table whether or not the number of conflicts
      // has actually changed (unlike in the checkpoint resolution case).
      //
      // For bookkeeping, we first create a map of rowId -> local conflict type
      // and then iterate over the result set from the server query.
      {
        // create a map of rowId -> local conflict type from the privileged query.
        //
        // then build up the set of ids missing from the unprivilegedTable vs. the (privilegedQuery)
        // table and any ids that the user does not have the ability to modify ("w" access) or
        // delete ("d" access) as appropriate for the conflict type.
        HashMap<String, Integer> localConflictTypeMap = new HashMap<>();
        Set<String> ids = new HashSet<String>();
        for (int i = 0; i < table.getNumberOfRows(); ++i) {
          // full set of ids in conflict
          ids.add(table.getRowId(i));
          TypedRow theRow = table.getRowAtIndex(i);
          String strLocalConflictValue = theRow.getRawStringByKey(DataTableColumns.CONFLICT_TYPE);
          // the strLocalConflictValue will always be an integer because of the where clause
          int localConflictValue = Integer.valueOf(strLocalConflictValue);
          localConflictTypeMap.put(table.getRowId(i), localConflictValue);
        }

        int removedRows = 0;

        // do the unprivileged query to get the server rows
        Object[] serverSelectionArgs = new Object[] {
            ConflictType.SERVER_DELETED_OLD_VALUES, ConflictType.SERVER_UPDATED_UPDATED_VALUES };
        BaseTable unprivilegedBaseTable = ODKDatabaseImplUtils.get().query(db, mTableId, QueryUtil
                .buildSqlStatement(mTableId, whereClause, groupBy, null, orderByKeys, orderByDir),
            serverSelectionArgs, null, accessContextBase);
        UserTable unprivilegedTable = new UserTable(unprivilegedBaseTable, orderedDefns, adminColArr);

        for (int i = 0; i < unprivilegedTable.getNumberOfRows(); ++i) {
          String rowId = unprivilegedTable.getRowId(i);
          // fetch the local conflict value from the map created above.
          // there will always be a value because the localConflictTypeMap was built
          // from results of a privileged query. The unprivileged query is always
          // a subset of the privileged query result set.
          int localConflictValue = localConflictTypeMap.get(rowId);

          TypedRow theRow = unprivilegedTable.getRowAtIndex(i);
          String effectiveAccess = theRow.getRawStringByKey(DataTableColumns.EFFECTIVE_ACCESS);

          if ( localConflictValue == ConflictType.LOCAL_UPDATED_UPDATED_VALUES &&
              effectiveAccess.contains("w") ) {
            // local update and user can modify the row -- do not auto-resolve this
            // HOWEVER: enforce permissions-update rules (whether the user has "p" privileges)
            // this may resolve the conflict...
            if ( !ODKDatabaseImplUtils.get().enforcePermissionsAndOptimizeConflictProcessing
                (db, mTableId, orderedDefns, rowId, SyncState.in_conflict,
                    accessContextBase, aul.locale) ) {
              ++removedRows;
            }
            // and then present it to the user (remove it from this list).
            ids.remove(rowId);
          } else if ( localConflictValue == ConflictType.LOCAL_DELETED_OLD_VALUES &&
              effectiveAccess.contains("d") ) {
            // local delete and user can delete the row -- do not auto-resolve this
            // HOWEVER: enforce permissions-update rules (whether the user has "p" privileges)
            // this may resolve the conflict...
            if ( !ODKDatabaseImplUtils.get().enforcePermissionsAndOptimizeConflictProcessing
                (db, mTableId, orderedDefns, rowId, SyncState.in_conflict,
                    accessContextBase, aul.locale) ) {
              ++removedRows;
            }
            // and then present it to the user (remove it from this list).
            ids.remove(rowId);
          }
        }
        mNumberRowsSilentlyResolved = ids.size() + removedRows;

        // the ids remaining in 'ids' are either hidden to the user or the user does not have
        // the ability to perform the local change they want in those rows.
        for (String rowId : ids) {
          // act as a privileged user so that we always restore to original row
          ODKDatabaseImplUtils.get().resolveServerConflictTakeServerRowWithId(db, mTableId, rowId,
              aul.activeUser, RoleConsts.ADMIN_ROLES_LIST);
        }

        {
          // update the privileged query table again
          // we always do this because we may have updated the permissions fields
          selectionArgs = new Object[] { ConflictType.LOCAL_DELETED_OLD_VALUES,
              ConflictType.LOCAL_UPDATED_UPDATED_VALUES };

          baseTable = ODKDatabaseImplUtils.get().privilegedQuery(db, mTableId, QueryUtil
                  .buildSqlStatement(mTableId, whereClause, groupBy, null, orderByKeys, orderByDir),
              selectionArgs, null, accessContextPrivileged);
          table = new UserTable(baseTable, orderedDefns, adminColArr);
        }
      }

      // and now scan to see if we can resolve the conflicts because the rows are
      // identical in all fields that the user should be able to select. This is after
      // the above processing because the
      // ODKDatabaseImplUtils.enforcePermissionsAndOptimizeConflictProcessing()
      // function may have eliminated all of the differences in the row (if they were
      // only impacting the permissions fields).
      //
      if ( !mHaveResolvedMetadataConflicts ) {

        boolean tableSetChanged = false;
        // resolve the automatically-resolvable ones
        // (the ones that differ only in their metadata).
        for ( int i = 0 ; i < table.getNumberOfRows(); ++i ) {
          TypedRow row = table.getRowAtIndex(i);
          String rowId = row.getRawStringByKey(DataTableColumns.ID);
          OdkResolveConflictFieldLoader loader = new OdkResolveConflictFieldLoader(getContext()
              , mAppName, mTableId, rowId);
          ResolveActionList resolveActionList = loader.doWork(dbHandleName);

          if ( resolveActionList.noChangesInUserDefinedFieldValues() ) {
            tableSetChanged = true;
            // all users can resolve taking the server's changes
            // Use privileged user roles since we are taking the server's values
            ODKDatabaseImplUtils.get().resolveServerConflictTakeServerRowWithId(db,
                mTableId, rowId, aul.activeUser, aul.locale);
          }
        }

        if (tableSetChanged) {
          selectionArgs = new Object[] { ConflictType.LOCAL_DELETED_OLD_VALUES,
              ConflictType.LOCAL_UPDATED_UPDATED_VALUES };

          baseTable = ODKDatabaseImplUtils.get().privilegedQuery(db, mTableId, QueryUtil
                  .buildSqlStatement(mTableId, whereClause, groupBy, null, orderByKeys, orderByDir),
              selectionArgs, null, accessContextPrivileged);
          table = new UserTable(baseTable, orderedDefns, adminColArr);
        }
      }

      // at this point, the conflict rows that remain can be resolved either by taking the
      // server changes or by taking the local changes -- the user has the ability to make
      // that choice.

      // The display name is the table display name, not the form display name...
      ArrayList<KeyValueStoreEntry> entries = ODKDatabaseImplUtils.get().getTableMetadata(db,
          mTableId, KeyValueStoreConstants.PARTITION_TABLE, KeyValueStoreConstants.ASPECT_DEFAULT,
          KeyValueStoreConstants.TABLE_DISPLAY_NAME).getEntries();

      tableDisplayName = entries.isEmpty() ?  NameUtil.normalizeDisplayName(NameUtil
          .constructSimpleDisplayName(mTableId)) : entries.get(0).value;

      forms = ODKDatabaseImplUtils.get().rawQuery(db,
          "SELECT " + FormsColumns.INSTANCE_NAME +
              " , " + FormsColumns.FORM_ID +
              " , " + FormsColumns.DISPLAY_NAME +
              " FROM " + DatabaseConstants.FORMS_TABLE_NAME +
              " WHERE " + FormsColumns.TABLE_ID + "=?" +
              " ORDER BY " + FormsColumns.FORM_ID + " ASC",
          new String[]{ mTableId }, null,
          accessContextPrivileged);

      if ( forms != null && forms.moveToFirst() ) {
        int idxInstanceName = forms.getColumnIndex(FormsColumns.INSTANCE_NAME);
        int idxFormId = forms.getColumnIndex(FormsColumns.FORM_ID);
        int idxFormDisplayName = forms.getColumnIndex(FormsColumns.DISPLAY_NAME);
        do {
          if ( forms.isNull(idxInstanceName) ) {
            continue;
          }

          String instanceName = forms.getString(idxInstanceName);
          if ( instanceName == null || instanceName.length() == 0 ) {
            continue;
          }

          String formId = forms.getString(idxFormId);
          String displayName = forms.getString(idxFormDisplayName);

          FormDefinition fd = new FormDefinition();
          fd.instanceName = instanceName;
          fd.formDisplayName = displayName;
          fd.formId = formId;
          formDefinitions.add(fd);
        } while (forms.moveToNext());
      }
      if ( forms != null ) {
        forms.close();
      }
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if (msg == null)
        msg = e.getMessage();
      if (msg == null)
        msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(mAppName).e("OdkResolveConflictRowLoader",
          mAppName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
      WebLogger.getLogger(mAppName).printStackTrace(e);
      throw new IllegalStateException(msg);
    } finally {
      if ( forms != null ) {
        if ( !forms.isClosed() ) {
          forms.close();
        }
        forms = null;
      }
      if (db != null) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }

    // use the instanceName in the tableId form, if defined.
    FormDefinition nameToUse = null;
    for ( FormDefinition fd : formDefinitions ) {
      if ( fd.formId.equals(mTableId) ) {
        nameToUse = fd;
        break;
      }
    }
    if ( nameToUse == null ) {
      if ( formDefinitions.isEmpty() ) {
        nameToUse = new FormDefinition();
        nameToUse.formId = null;
        nameToUse.formDisplayName = tableDisplayName;
        nameToUse.instanceName = DataTableColumns.SAVEPOINT_TIMESTAMP;
      } else {
        // otherwise use the name from the first formId that gave one.
        nameToUse = formDefinitions.get(0);
      }
    }
    String formDisplayName = LocalizationUtils.getLocalizedDisplayName(mAppName, mTableId,
        aul.locale, nameToUse.formDisplayName);

    ArrayList<ResolveRowEntry> results = new ArrayList<ResolveRowEntry>();
    for (int i = 0; i < table.getNumberOfRows(); i++) {
      TypedRow row = table.getRowAtIndex(i);
      String rowId = row.getRawStringByKey(DataTableColumns.ID);
      String instanceName = row.getRawStringByKey(nameToUse.instanceName);
      ResolveRowEntry re = new ResolveRowEntry(rowId, formDisplayName + ": " + instanceName);
      results.add(re);
    }

    return results;
  }

  @Override protected void onStartLoading() {
    super.onStartLoading();
    forceLoad();
  }
}
