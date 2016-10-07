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
import org.opendatakit.aggregate.odktables.rest.ElementType;
import org.opendatakit.aggregate.odktables.rest.KeyValueStoreConstants;
import org.opendatakit.aggregate.odktables.rest.SavepointTypeManipulator;
import org.opendatakit.database.data.ColumnDefinition;
import org.opendatakit.database.data.OrderedColumns;
import org.opendatakit.database.data.UserTable;
import org.opendatakit.services.database.OdkConnectionFactorySingleton;
import org.opendatakit.services.database.OdkConnectionInterface;
import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.provider.DataTableColumns;
import org.opendatakit.utilities.NameUtil;
import org.opendatakit.utilities.LocalizationUtils;
import org.opendatakit.services.database.utlities.ODKDatabaseImplUtils;
import org.opendatakit.logging.WebLogger;
import org.opendatakit.database.data.KeyValueStoreEntry;
import org.opendatakit.database.service.DbHandle;
import org.opendatakit.database.data.Row;
import org.opendatakit.database.data.BaseTable;
import org.opendatakit.services.resolve.views.components.*;

import java.util.*;

/**
 * @author mitchellsundt@gmail.com
 */
class OdkResolveCheckpointFieldLoader extends AsyncTaskLoader<ResolveActionList> {

  private final String mAppName;
  private final String mTableId;
  private final String mRowId;

  public OdkResolveCheckpointFieldLoader(Context context, String appName, String tableId,
      String rowId) {
    super(context);
    this.mAppName = appName;
    this.mTableId = tableId;
    this.mRowId = rowId;
  }

  /**
   * API used by the list loader to test and potentially auto-resolve the record
   * that do not have any differences in the user-defined fields.
   *
   * @param dbHandleName
   * @return
   */
  public ResolveActionList doWork(DbHandle dbHandleName) {

    OrderedColumns orderedDefns;
    Map<String, String> persistedDisplayNames = new HashMap<String, String>();

    OdkConnectionInterface db = null;

    PropertiesSingleton props =
        CommonToolProperties.get(getContext(), mAppName);
    String activeUser = props.getActiveUser();

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
          // this skips the non-user-defined column values
          orderedDefns.find(e.aspect);
          persistedDisplayNames.put(e.aspect, e.value);
        } catch (IllegalArgumentException ex) {
          // ignore
        }
      }

      List<String> adminColumns = ODKDatabaseImplUtils.get().getAdminColumns();
      String[] adminColArr = adminColumns.toArray(new String[adminColumns.size()]);

      BaseTable baseTable = ODKDatabaseImplUtils.get().privilegedGetRowsWithId(db, mTableId,
          mRowId, activeUser);
      table = new UserTable(baseTable, orderedDefns, adminColArr);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if (msg == null)
        msg = e.getMessage();
      if (msg == null)
        msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(mAppName).e("OdkResolveCheckpointFieldLoader",
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

    // the last row is the oldest -- it should be a COMPLETE or INCOMPLETE row
    // if it isn't, then this is a new row and the option is to delete it or
    // save as incomplete. Otherwise, it is to roll back or update to
    // incomplete.

    int rowStartingIndex = table.getNumberOfRows() - 1;
    Row rowStarting = table.getRowAtIndex(rowStartingIndex);
    String type = rowStarting.getDataByKey(DataTableColumns.SAVEPOINT_TYPE);
    boolean deleteEntirely = (type == null || type.length() == 0);

    if (!deleteEntirely) {
      if (table.getNumberOfRows() == 1
          && (SavepointTypeManipulator.isComplete(type) ||
          SavepointTypeManipulator.isIncomplete(type))) {
        // something else seems to have resolved this?
        WebLogger.getLogger(mAppName).w("OdkResolveCheckpointFieldLoader",
            "Unexpectedly found that row does not need to be resolved");
        return null;
      }
    }

    Row rowEnding = table.getRowAtIndex(0);
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
        columnDisplayName = LocalizationUtils.getLocalizedDisplayName(columnDisplayName);
      } else {
        columnDisplayName = NameUtil.constructSimpleDisplayName(elementKey);
      }
      String localRawValue = rowEnding.getDataByKey(elementKey);
      String localDisplayValue = table
          .getDisplayTextOfData(rowStartingIndex, elementType, elementKey);
      String serverRawValue = rowStarting.getDataByKey(elementKey);
      String serverDisplayValue = table
          .getDisplayTextOfData(rowStartingIndex, elementType, elementKey);
      if (deleteEntirely ||
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
    ResolveActionType actionType = deleteEntirely ?
        ResolveActionType.DELETE :
        (SavepointTypeManipulator.isComplete(type) ?
            ResolveActionType.RESTORE_TO_COMPLETE :
            ResolveActionType.RESTORE_TO_INCOMPLETE);

    return new ResolveActionList(actionType, concordantColumns, conflictColumns);
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
