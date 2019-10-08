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

import androidx.loader.content.AsyncTaskLoader;

import org.opendatakit.aggregate.odktables.rest.ConflictType;
import org.opendatakit.aggregate.odktables.rest.ElementType;
import org.opendatakit.aggregate.odktables.rest.KeyValueStoreConstants;
import org.opendatakit.database.RoleConsts;
import org.opendatakit.database.data.BaseTable;
import org.opendatakit.database.data.ColumnDefinition;
import org.opendatakit.database.data.KeyValueStoreEntry;
import org.opendatakit.database.data.OrderedColumns;
import org.opendatakit.database.data.TypedRow;
import org.opendatakit.database.data.UserTable;
import org.opendatakit.database.service.DbHandle;
import org.opendatakit.database.utilities.QueryUtil;
import org.opendatakit.logging.WebLogger;
import org.opendatakit.provider.DataTableColumns;
import org.opendatakit.services.database.OdkConnectionFactorySingleton;
import org.opendatakit.services.database.OdkConnectionInterface;
import org.opendatakit.services.database.utilities.ODKDatabaseImplUtils;
import org.opendatakit.services.resolve.views.components.ConcordantColumn;
import org.opendatakit.services.resolve.views.components.ConflictColumn;
import org.opendatakit.services.resolve.views.components.ResolveActionList;
import org.opendatakit.services.utilities.ActiveUserAndLocale;
import org.opendatakit.utilities.LocalizationUtils;
import org.opendatakit.utilities.NameUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * @author mitchellsundt@gmail.com
 */
class OdkResolveConflictFieldLoader extends AsyncTaskLoader<ResolveActionList> {

  private final String mAppName;
  private final String mTableId;
  private final String mRowId;

  public OdkResolveConflictFieldLoader(Context context, String appName, String tableId,
      String rowId) {
    super(context);
    this.mAppName = appName;
    this.mTableId = tableId;
    this.mRowId = rowId;
  }

  /**
   * API used by the list loader to test whether a conflict has any differences
   * among its user-defined fields. If it doesn't, the list loader can auto-resolve
   * them (taking the server version).
   *
   * @param dbHandleName
   * @return
   */
  public ResolveActionList doWork(DbHandle dbHandleName) {

    OrderedColumns orderedDefns;
    Map<String, String> persistedDisplayNames = new HashMap<String, String>();

    ActiveUserAndLocale aul =
        ActiveUserAndLocale.getActiveUserAndLocale(getContext(), mAppName);

    OdkConnectionInterface db = null;

    UserTable table = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(mAppName, dbHandleName);

      orderedDefns = ODKDatabaseImplUtils.get().getUserDefinedColumns(db, mTableId);


      List<KeyValueStoreEntry> columnDisplayNames =
          ODKDatabaseImplUtils.get().getTableMetadata(db, mTableId,
              KeyValueStoreConstants.PARTITION_COLUMN, null,
              KeyValueStoreConstants.COLUMN_DISPLAY_NAME).getEntries();

      for (KeyValueStoreEntry e : columnDisplayNames) {
        try {
          // this skips erroneous KVS entries that
          // do not correspond to a user-defined column.
          orderedDefns.find(e.aspect);
          persistedDisplayNames.put(e.aspect, e.value);
        } catch (IllegalArgumentException ex) {
          // ignore
        }
      }

      // get both conflict records for this row.
      // the local record is always before the server record (due to conflict_type values)
      String whereClause = DataTableColumns.ID + "=?" +
          " AND " + DataTableColumns.CONFLICT_TYPE + " IS NOT NULL";
      List<String> adminColumns = ODKDatabaseImplUtils.get().getAdminColumns();
      String[] adminColArr = adminColumns.toArray(new String[adminColumns.size()]);

      ODKDatabaseImplUtils.AccessContext accessContextPrivileged =
          ODKDatabaseImplUtils.get().getAccessContext(db, mTableId, aul.activeUser,
              RoleConsts.ADMIN_ROLES_LIST);

      BaseTable baseTable = ODKDatabaseImplUtils.get().privilegedQuery(db, mTableId, QueryUtil
              .buildSqlStatement(mTableId, whereClause, null, null,
                  new String[] { DataTableColumns.CONFLICT_TYPE }, new String[] { "ASC" }),
          new String[] { mRowId }, null, accessContextPrivileged);
      table = new UserTable(baseTable, orderedDefns, adminColArr);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if (msg == null)
        msg = e.getMessage();
      if (msg == null)
        msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(mAppName).e("OdkResolveConflictFieldLoader",
          mAppName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
      WebLogger.getLogger(mAppName).printStackTrace(e);
      return null;
    } finally {
      if (db != null) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }


    if (table.getNumberOfRows() == 0) {
      // another process deleted this row?
      WebLogger.getLogger(mAppName).w("OdkResolveCheckpointFieldLoader",
          "Unexpectedly did not find row in database!");
      return null;
    }

    if ( table.getNumberOfRows() != 2) {
      // something is badly wrong?
      WebLogger.getLogger(mAppName).w("OdkResolveCheckpointFieldLoader",
          "Unexpectedly found that row does not have exactly 2 conflict records");
      return null;
    }

    // the first row is the localRow, the second is the serverRow.
    int localRowIndex = 0;
    int serverRowIndex = 1;
    TypedRow localRow = table.getRowAtIndex(localRowIndex);
    TypedRow serverRow = table.getRowAtIndex(serverRowIndex);

    int localConflictType = Integer.parseInt(localRow.getRawStringByKey(DataTableColumns
        .CONFLICT_TYPE));
    int serverConflictType = Integer
        .parseInt(serverRow.getRawStringByKey(DataTableColumns.CONFLICT_TYPE));
    //
    // And now we need to construct up the adapter.

    // There are several things to do be aware of. We need to get all the
    // section headings, which will be the column names. We also need to get
    // all the values which are in conflict, as well as those that are not.
    // We'll present them in user-defined order, as they may have set up the
    // useful information together.
    // This will be the number of rows down we are in the adapter. Each
    // heading and each cell value gets its own row. Columns in conflict get
    // two, as we'll need to display each one to the user.
    int adapterOffset = 0;
    ArrayList<ConflictColumn> conflictColumns = new ArrayList<ConflictColumn>();
    ArrayList<ConcordantColumn> concordantColumns = new ArrayList<ConcordantColumn>();
    for (ColumnDefinition cd : orderedDefns.getColumnDefinitions()) {
      if (!cd.isUnitOfRetention()) {
        continue;
      }
      String elementKey = cd.getElementKey();
      ElementType elementType = cd.getType();
      String columnDisplayName = persistedDisplayNames.get(elementKey);
      if (columnDisplayName != null) {
        columnDisplayName = LocalizationUtils.getLocalizedDisplayName(mAppName,
            mTableId, aul.locale, columnDisplayName);
      } else {
        columnDisplayName = LocalizationUtils.getLocalizedDisplayName(mAppName,
            mTableId, aul.locale, NameUtil.constructSimpleDisplayName(elementKey));
      }
      String localRawValue = localRow.getRawStringByKey(elementKey);
      String localDisplayValue = table.getDisplayTextOfData(localRowIndex, elementType, elementKey);
      String serverRawValue = serverRow.getRawStringByKey(elementKey);
      String serverDisplayValue = table
          .getDisplayTextOfData(serverRowIndex, elementType, elementKey);
      if ((localConflictType == ConflictType.LOCAL_DELETED_OLD_VALUES) ||
          (serverConflictType == ConflictType.SERVER_DELETED_OLD_VALUES) ||
          (localRawValue == null && serverRawValue == null) ||
          (localRawValue != null && serverRawValue != null &&
              localRawValue.equals(serverRawValue))) {
        // We only want to display a single row, b/c there are no choices to
        // be made by the user.
        ConcordantColumn concordance = new ConcordantColumn(adapterOffset, columnDisplayName,
            localDisplayValue);
        concordantColumns.add(concordance);
        ++adapterOffset;
      } else {
        // We need to display both the server and local versions.
        ConflictColumn conflictColumn = new ConflictColumn(adapterOffset, columnDisplayName,
            elementKey,
            localRawValue, localDisplayValue, serverRawValue, serverDisplayValue);
        ++adapterOffset;
        conflictColumns.add(conflictColumn);
      }
    }
    return new ResolveActionList(localConflictType, serverConflictType,
        concordantColumns, conflictColumns);
  }

  @Override public ResolveActionList loadInBackground() {

    DbHandle dbHandleName = new DbHandle(UUID.randomUUID().toString());

    return doWork(dbHandleName);
  }

  @Override protected void onStartLoading() {
    super.onStartLoading();
    forceLoad();
  }
}
