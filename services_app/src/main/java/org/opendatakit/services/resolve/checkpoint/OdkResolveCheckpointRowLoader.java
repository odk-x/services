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
package org.opendatakit.services.resolve.checkpoint;

import android.content.AsyncTaskLoader;
import android.content.Context;
import android.database.Cursor;
import org.opendatakit.database.RoleConsts;
import org.opendatakit.aggregate.odktables.rest.KeyValueStoreConstants;
import org.opendatakit.database.data.OrderedColumns;
import org.opendatakit.database.data.UserTable;
import org.opendatakit.database.DatabaseConstants;
import org.opendatakit.services.database.OdkConnectionFactorySingleton;
import org.opendatakit.services.database.OdkConnectionInterface;
import org.opendatakit.provider.DataTableColumns;
import org.opendatakit.provider.FormsColumns;
import org.opendatakit.utilities.NameUtil;
import org.opendatakit.utilities.LocalizationUtils;
import org.opendatakit.services.database.utlities.ODKDatabaseImplUtils;
import org.opendatakit.logging.WebLogger;
import org.opendatakit.database.data.KeyValueStoreEntry;
import org.opendatakit.database.service.DbHandle;
import org.opendatakit.database.data.Row;
import org.opendatakit.database.data.BaseTable;
import org.opendatakit.database.utilities.QueryUtil;
import org.opendatakit.services.resolve.ActiveUserAndLocale;
import org.opendatakit.services.resolve.views.components.ResolveActionList;
import org.opendatakit.services.resolve.views.components.ResolveRowEntry;
import org.opendatakit.services.R;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * @author mitchellsundt@gmail.com
 */
class OdkResolveCheckpointRowLoader extends AsyncTaskLoader<ArrayList<ResolveRowEntry>> {

  private final String mAppName;
  private final String mTableId;
  private final boolean mHaveResolvedMetadataConflicts;

  private static class FormDefinition {
    String instanceName;
    String formDisplayName;
    String formId;
  }

  public OdkResolveCheckpointRowLoader(Context context, String appName, String tableId,
      boolean haveResolvedMetadataConflicts) {
    super(context);
    this.mAppName = appName;
    this.mTableId = tableId;
    this.mHaveResolvedMetadataConflicts = haveResolvedMetadataConflicts;
  }

  @Override
  public ArrayList<ResolveRowEntry> loadInBackground() {

    DbHandle dbHandleName = new DbHandle(UUID.randomUUID().toString());

    return doWork(dbHandleName);
  }

  private ArrayList<ResolveRowEntry> doWork(DbHandle dbHandleName) {

    OdkConnectionInterface db = null;

    ArrayList<FormDefinition> formDefinitions = new ArrayList<FormDefinition>();
    String tableDisplayName = null;
    Cursor forms = null;
    UserTable table = null;

    ActiveUserAndLocale aul =
        ActiveUserAndLocale.getActiveUserAndLocale(getContext(), mAppName);

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(mAppName, dbHandleName);

      OrderedColumns orderedDefns = ODKDatabaseImplUtils.get()
          .getUserDefinedColumns(db, mTableId);
      String whereClause = DataTableColumns.SAVEPOINT_TYPE + " IS NULL";
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
          null, null, accessContextPrivileged);
      table = new UserTable(baseTable, orderedDefns, adminColArr);

      if ( !mHaveResolvedMetadataConflicts ) {

        boolean tableSetChanged = false;
        // resolve the automatically-resolvable ones
        // (the ones that differ only in their metadata).
        for (int i = 0; i < table.getNumberOfRows(); ++i) {
          Row row = table.getRowAtIndex(i);
          String rowId = row.getDataByKey(DataTableColumns.ID);

          OdkResolveCheckpointFieldLoader loader = new OdkResolveCheckpointFieldLoader(getContext(),
              mAppName, mTableId, rowId);
          ResolveActionList resolveActionList = loader.doWork(dbHandleName);

          if (resolveActionList.noChangesInUserDefinedFieldValues()) {
            tableSetChanged = true;
            ODKDatabaseImplUtils.get().deleteAllCheckpointRowsWithId(db, mTableId,
                rowId, aul.activeUser, aul.rolesList);
          }
        }

        if ( tableSetChanged ) {
          baseTable = ODKDatabaseImplUtils.get().privilegedQuery(db, mTableId, QueryUtil
              .buildSqlStatement(mTableId, whereClause, groupBy, null, orderByKeys, orderByDir),
              null, null, accessContextPrivileged);
          table = new UserTable(baseTable, orderedDefns, adminColArr);
        }
      }

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
          new String[]{ mTableId }, null, accessContextBase);

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
          String formDisplayName = forms.getString(idxFormDisplayName);

          FormDefinition fd = new FormDefinition();
          fd.instanceName = instanceName;
          fd.formDisplayName = formDisplayName;
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
      WebLogger.getLogger(mAppName).e("OdkResolveCheckpointRowLoader",
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
    String formDisplayName = LocalizationUtils.getLocalizedDisplayName(nameToUse.formDisplayName);

    ArrayList<ResolveRowEntry> results = new ArrayList<ResolveRowEntry>();
    for (int i = 0; i < table.getNumberOfRows(); i++) {
      Row row = table.getRowAtIndex(i);
      String rowId = row.getDataByKey(DataTableColumns.ID);
      String instanceName = row.getDataByKey(nameToUse.instanceName);
      ResolveRowEntry re = new ResolveRowEntry(rowId,
          getContext().getString(R.string.resolve_row_display_name, formDisplayName, instanceName));
      results.add(re);
    }
    return results;
  }

  @Override protected void onStartLoading() {
    super.onStartLoading();
    forceLoad();
  }
}
