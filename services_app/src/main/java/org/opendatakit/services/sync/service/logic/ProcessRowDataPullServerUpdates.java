/*
 * Copyright (C) 2012 University of Washington
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
package org.opendatakit.services.sync.service.logic;

import android.content.ContentValues;

import org.opendatakit.aggregate.odktables.rest.ElementDataType;
import org.opendatakit.aggregate.odktables.rest.SyncState;
import org.opendatakit.aggregate.odktables.rest.entity.Column;
import org.opendatakit.aggregate.odktables.rest.entity.DataKeyValue;
import org.opendatakit.aggregate.odktables.rest.entity.RowFilterScope;
import org.opendatakit.aggregate.odktables.rest.entity.RowResource;
import org.opendatakit.aggregate.odktables.rest.entity.RowResourceList;
import org.opendatakit.aggregate.odktables.rest.entity.TableResource;
import org.opendatakit.database.data.ColumnDefinition;
import org.opendatakit.database.data.ColumnList;
import org.opendatakit.database.data.OrderedColumns;
import org.opendatakit.database.data.TableDefinitionEntry;
import org.opendatakit.database.data.TypedRow;
import org.opendatakit.database.data.UserTable;
import org.opendatakit.database.service.DbHandle;
import org.opendatakit.exception.ServicesAvailabilityException;
import org.opendatakit.provider.DataTableColumns;
import org.opendatakit.services.R;
import org.opendatakit.services.sync.service.SyncExecutionContext;
import org.opendatakit.sync.service.SyncAttachmentState;
import org.opendatakit.sync.service.SyncOutcome;
import org.opendatakit.sync.service.TableLevelResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The refactored control loop for retrieving row changes from the server and
 * applying them to the local database. These now minimize memory usage by
 * limiting the size of the UserTable held in memory during the processing of the
 * result set. SyncProcessor
 * implements
 * the cloud synchronization
 * logic for Tables.
 *
 * @author the.dylan.price@gmail.com
 * @author sudar.sam@gmail.com
 * @author mitchellsundt@gmail.com
 */
class ProcessRowDataPullServerUpdates extends ProcessRowDataSharedBase {

  private static final String TAG = ProcessRowDataPullServerUpdates.class.getSimpleName();

  private static final double minPercentage = 0.0;
  private static final double maxPercentage = 50.0;
  private static final int numberOfPhases = 2;

  private final ProcessManifestContentAndFileChanges manifestProcessor;

  public ProcessRowDataPullServerUpdates(SyncExecutionContext sharedContext) {
    super(sharedContext);
    this.manifestProcessor = new ProcessManifestContentAndFileChanges(sc);

    setUpdateNotificationBounds(minPercentage, maxPercentage, 1);
  }

  /**
   * Processes one set of changed Rows reported by the server.
   * <p/>
   * This will set the tableResult SyncOutcome if there is a non-recoverable error of
   * some sort. It may also throw an exception if there are internal errors or database
   * errors.
   * <p/>
   * Otherwise, it will leave the tableResult SyncOutcome as working.
   *
   * @param tableResource
   * @param orderedColumns
   * @param fileAttachmentColumns
   * @param rows
   * @return
   * @throws IOException
   * @throws ServicesAvailabilityException
   */
  private void updateLocalRowsFromServerRowResourceList(TableResource tableResource,
      OrderedColumns orderedColumns, ArrayList<ColumnDefinition> fileAttachmentColumns,
      RowResourceList rows) throws IOException, ServicesAvailabilityException {
    String tableId = tableResource.getTableId();
    TableLevelResult tableLevelResult = sc.getTableLevelResult(tableId);

    if (rows.getRows().isEmpty()) {
      // nothing here -- let caller determine whether we are done or
      // whether we need to issue another request to the server.
      return;
    }

    publishUpdateNotification(R.string.sync_applying_batch_server_row_changes, tableId, -1.0);

    HashMap<String,Integer> serverElementKeyToIndex = new HashMap<String,Integer>();
    {
      RowResource serverRow = rows.getRows().get(0);
      for (int i = 0; i < serverRow.getValues().size(); ++i) {
        DataKeyValue dkv = serverRow.getValues().get(i);
        serverElementKeyToIndex.put(dkv.column, i);
      }
    }

    Map<String, RowResource> changedServerRows = new HashMap<String, RowResource>();
    for (RowResource row : rows.getRows()) {
      changedServerRows.put(row.getRowId(), row);
    }

    {
      DbHandle db = null;
      try {
        db = sc.getDatabase();

        /**************************
         * PART 2: UPDATE THE DATA
         **************************/
        getLogger().d(TAG, "updateLocalRowsFromServerRowResourceList setServerHadDataChanges(true)");
        tableLevelResult.setServerHadDataChanges(true);
        // these are all the various actions we will need to take:

        publishUpdateNotification(R.string.sync_fetching_local_rows_in_batch_server_row_changes,
                tableId, -1.0);

        // get all the rows in the data table -- we will iterate through
        // them all.
        UserTable localDataTable;
        {
            // To get all the rows that match those sent from the server
            // we create a local table and insert all of the row ids into
            // that table. Then filter against the row ids in that table
            // to pull out the matching rows in our table.

            List<Column> columns = new ArrayList<Column>();
            columns.add(
                new Column(ID_COLUMN, ID_COLUMN, ElementDataType.string.name(),
                    "[]"));
            ColumnList columnList = new ColumnList(columns);

            String local_id_table = "L__" + tableId;

            // create the table (drop it first -- to get an empty table)
            sc.getDatabaseService().deleteLocalOnlyTable(sc.getAppName(), db, local_id_table);
            sc.getDatabaseService()
                .createLocalOnlyTableWithColumns(sc.getAppName(), db, local_id_table, columnList);

            // insert the row ids from the server
            {
              ContentValues cv = new ContentValues();
              for (String id : changedServerRows.keySet()) {
                cv.clear();
                cv.put(ID_COLUMN, id);
                sc.getDatabaseService().insertLocalOnlyRow(sc.getAppName(), db, local_id_table, cv);
              }
            }

            // construct where clause filter
            StringBuilder b = new StringBuilder();
            b.append(DataTableColumns.ID).append(" IN (SELECT ").append(ID_COLUMN)
                .append(" FROM ").append(local_id_table).append(")");

            localDataTable = sc.getDatabaseService()
                .privilegedSimpleQuery(sc.getAppName(), db, tableId, orderedColumns, b.toString(),
                    null, null, null,
                    new String[] { DataTableColumns.ID }, new String[] { "ASC" },
                    null, null);
        }

        // //////////////////////////////////////////////////
        // //////////////////////////////////////////////////
        // fail the sync on this table if there are checkpoint rows.

        if (localDataTable.hasCheckpointRows()) {
          // should only be reachable on the first time through this for
          // loop...
          tableLevelResult.setMessage(sc.getString(R.string.sync_table_contains_checkpoints));
          tableLevelResult.setSyncOutcome(SyncOutcome.TABLE_CONTAINS_CHECKPOINTS);
          return;
        }

        // loop through the localRow table
        for (int i = 0; i < localDataTable.getNumberOfRows(); i++) {
          TypedRow localRow = localDataTable.getRowAtIndex(i);
          String stateStr = localRow.getRawStringByKey(DataTableColumns.SYNC_STATE);
          SyncState state = stateStr == null ? null : SyncState.valueOf(stateStr);

          String rowId = localDataTable.getRowId(i);

          // see if there is a change to this row from our current
          // server change set.
          RowResource serverRow = changedServerRows.get(rowId);

          if (serverRow == null) {
            // we are selecting only the rows with ids matching those in the changedServerRows
            // map. It should be impossible for this to be null.
            tableLevelResult.setMessage(sc.getString(R.string.sync_table_erroneous_filter));
            tableLevelResult.setSyncOutcome(SyncOutcome.LOCAL_DATABASE_EXCEPTION);
            return;
          }

          if (state == SyncState.synced_pending_files && serverRow.isDeleted() ) {
            manifestProcessor.syncRowLevelFileAttachments(
                tableResource.getInstanceFilesUri(),
                tableResource.getTableId(), localRow, fileAttachmentColumns, SyncAttachmentState.UPLOAD);

          }

          // set up to insert the in_conflict row from the server
          ContentValues values = dataKeyValueListToContentValues(
              serverRow.getValues(),
              orderedColumns
          );

          // insert in_conflict server row
          values.put(DataTableColumns.ID, serverRow.getRowId());
          values.put(DataTableColumns.ROW_ETAG, serverRow.getRowETag());
          values.put(DataTableColumns.SYNC_STATE, (serverRow.isDeleted() ?
              SyncState.deleted.name() : SyncState.changed.name()));
          values.put(DataTableColumns.FORM_ID, serverRow.getFormId());
          values.put(DataTableColumns.LOCALE, serverRow.getLocale());
          values.put(DataTableColumns.SAVEPOINT_TIMESTAMP, serverRow.getSavepointTimestamp());
          values.put(DataTableColumns.SAVEPOINT_CREATOR, serverRow.getSavepointCreator());
          values.put(DataTableColumns.SAVEPOINT_TYPE, serverRow.getSavepointType());
          RowFilterScope.Access type = serverRow.getRowFilterScope().getDefaultAccess();
          values.put(DataTableColumns.DEFAULT_ACCESS,
              (type == null) ? RowFilterScope.Access.FULL.name() : type.name());
          values.put(DataTableColumns.ROW_OWNER, serverRow.getRowFilterScope().getRowOwner());
          values.putNull(DataTableColumns.CONFLICT_TYPE);

          values.put(DataTableColumns.GROUP_MODIFY, serverRow.getRowFilterScope().getGroupModify());
          values.put(DataTableColumns.GROUP_PRIVILEGED, serverRow.getRowFilterScope().getGroupPrivileged());
          values.put(DataTableColumns.GROUP_READ_ONLY, serverRow.getRowFilterScope().getGroupReadOnly());

          sc.getDatabaseService().privilegedPerhapsPlaceRowIntoConflictWithId(sc.getAppName(), sc
              .getDatabase(), tableId, orderedColumns, values, rowId);

          // remove this server row from the map of changes reported by the server.
          // the following decision tree will always place the row into one of the
          // local action lists.
          changedServerRows.remove(rowId);
        }

        // Now, go through the remaining serverRows in the rows map. That
        // map now contains only row changes that don't affect any existing
        // localRow. If the server change is not a row-deletion / revoke-row
        // action, then insert the serverRow locally.
        for (RowResource serverRow : changedServerRows.values()) {
          boolean isDeleted = serverRow.isDeleted();
          if (!isDeleted) {

            boolean hasNonNullAttachments = false;

            for ( ColumnDefinition cd : fileAttachmentColumns ) {
              Integer idx = serverElementKeyToIndex.get(cd.getElementKey());
              if ( idx != null ) {
                String uriFragment = serverRow.getValues().get(idx).value;
                if ( uriFragment != null ) {
                  hasNonNullAttachments = true;
                  break;
                }
              }
            }

            ContentValues values = dataKeyValueListToContentValues(
                serverRow.getValues(),
                orderedColumns
            );

            // set all the metadata fields
            values.put(DataTableColumns.ID, serverRow.getRowId());
            values.put(DataTableColumns.ROW_ETAG, serverRow.getRowETag());
            values.put(DataTableColumns.SYNC_STATE, hasNonNullAttachments
                ? SyncState.synced_pending_files.name() : SyncState.synced.name());
            values.put(DataTableColumns.FORM_ID, serverRow.getFormId());
            values.put(DataTableColumns.LOCALE, serverRow.getLocale());
            values.put(DataTableColumns.SAVEPOINT_TIMESTAMP, serverRow.getSavepointTimestamp());
            values.put(DataTableColumns.SAVEPOINT_CREATOR, serverRow.getSavepointCreator());
            values.put(DataTableColumns.SAVEPOINT_TYPE, serverRow.getSavepointType());
            RowFilterScope.Access type = serverRow.getRowFilterScope().getDefaultAccess();
            values.put(DataTableColumns.DEFAULT_ACCESS,
                (type == null) ? RowFilterScope.Access.FULL.name() : type.name());
            values.put(DataTableColumns.ROW_OWNER, serverRow.getRowFilterScope().getRowOwner());

            values.put(DataTableColumns.GROUP_READ_ONLY, serverRow.getRowFilterScope().getGroupReadOnly());
            values.put(DataTableColumns.GROUP_MODIFY, serverRow.getRowFilterScope().getGroupModify());
            values.put(DataTableColumns.GROUP_PRIVILEGED, serverRow.getRowFilterScope().getGroupPrivileged());

            values.putNull(DataTableColumns.CONFLICT_TYPE);

            sc.getDatabaseService().privilegedInsertRowWithId(sc.getAppName(), db,
                tableId, orderedColumns, values, serverRow.getRowId(), false);
            tableLevelResult.incLocalInserts();

            publishUpdateNotification(R.string.sync_inserting_local_row, tableId);
          }
        }

      } finally {
        if (db != null) {
          sc.releaseDatabase(db);
          db = null;
        }
      }
    }
  }

  /**
   * Synchronize the table data rows.
   * <p/>
   * Note that if the db changes under you when calling this method, the tp
   * parameter will become out of date. It should be refreshed after calling
   * this method.
   * <p/>
   * This method does NOT synchronize any non-instance files; it assumes the
   * database schema has already been sync'd.
   *
   * @param tableResource   the table resource from the server, either from the getTables()
   *                        call or from a createTable() response.
   * @param te              definition of the table to synchronize
   * @param orderedColumns  well-formed ordered list of columns in this table.
   * @param fileAttachmentColumns  columns that can store file attachment filenames.
   * @throws ServicesAvailabilityException
   */
  void updateLocalRowsFromServer(TableResource tableResource, TableDefinitionEntry te,
      OrderedColumns orderedColumns, ArrayList<ColumnDefinition> fileAttachmentColumns)
      throws ServicesAvailabilityException {

    String tableId = te.getTableId();
    TableLevelResult tableLevelResult = sc.getTableLevelResult(tableId);
    getLogger().i(TAG, "updateLocalRowsFromServer " + tableId);

    tableLevelResult.setMessage("beginning row data sync");

    publishUpdateNotification(R.string.sync_verifying_table_schema_on_server, tableId, -1.0);

    try {

      // //////////////////////////////////////////////////
      // //////////////////////////////////////////////////
      // Pull changes from the server...

      String lastDataETag = null;

      {
        String firstDataETag = null;
        String websafeResumeCursor = null;

        int serverFetchNumber = -1;

        // may set tableResult syncOutcome
        for (; ; ) {
          ++serverFetchNumber;
          RowResourceList rows = null;

          // By default, the server uses a 2000-row limit in what it returns.
          // if the table has more than 200 columns, reduce this to 200 rows.
          int fetchLimit = (orderedColumns.getColumnDefinitions().size() > maxColumnsToUseLargeFetchLimit)
              ? smallFetchLimit : largeFetchLimit;

          double percentPerPhase = (maxPercentage - minPercentage) / ((double) numberOfPhases);
          double baseForPhase = (serverFetchNumber % numberOfPhases) * percentPerPhase;
          setUpdateNotificationBounds(baseForPhase, baseForPhase + percentPerPhase, fetchLimit);

          publishUpdateNotification(R.string.sync_getting_changed_rows_on_server, tableId, baseForPhase);

          try {
            rows = sc.getSynchronizer()
                .getUpdates(tableResource, te.getLastDataETag(), websafeResumeCursor, fetchLimit);
            if (firstDataETag == null) {
              firstDataETag = rows.getDataETag();
            }
            lastDataETag = rows.getDataETag();
          } catch (Exception e) {
            exception("synchronizeTable -  pulling data down from server", tableId, e,
                tableLevelResult);
            return;
          }

          updateLocalRowsFromServerRowResourceList(tableResource, orderedColumns,
              fileAttachmentColumns, rows);

          if (tableLevelResult.getSyncOutcome() != SyncOutcome.WORKING) {
            // something went wrong -- do not proceed.
            return;
          }

          if ( lastDataETag == null ) {
            // there were no rows for this table on the server
            break;
          } else if (!lastDataETag.equals(firstDataETag)) {
            // there were intervening updates by other clients.
            // re-issue request for updates and process these
            // until we have no updates pending.
            websafeResumeCursor = null;
            firstDataETag = null;
          } else if (rows.isHasMoreResults()) {
            websafeResumeCursor = rows.getWebSafeResumeCursor();
          } else {
            // there were no intervening updates by other clients.
            // success -- exit the update loop...
            break;
          }
        }
      }

      if (tableLevelResult.getSyncOutcome() != SyncOutcome.WORKING) {
        // something went wrong -- do not proceed.
        return;
      } else if (lastDataETag != null) {

        // ////////////////////////////////
        // ////////////////////////////////
        // Success
        //
        // We need to update our dataETag here so that the server
        // knows we saw its changes. Otherwise it won't let us
        // put up new information.
        DbHandle db = null;
        try {
          db = sc.getDatabase();
          // update the dataETag to the one returned by the first
          // of the fetch queries, above.
          sc.getDatabaseService().privilegedUpdateTableETags(sc.getAppName(), db, tableId,
              tableResource.getSchemaETag(), lastDataETag);
          // the above will throw a ServicesAvailabilityException if the change is not committed
          // and be sure to update our in-memory objects...
          te.setSchemaETag(tableResource.getSchemaETag());
          te.setLastDataETag(lastDataETag);
          tableResource.setDataETag(lastDataETag);
        } finally {
          sc.releaseDatabase(db);
          db = null;
        }
      }

      // If we made it here, whether or not there was data, then we
      // successfully updated the localDataTable from the server.
      tableLevelResult.setPulledServerData(true);

    } catch (Exception e) {
      exception("synchronizeTable - pulling data down from server", tableId, e, tableLevelResult);
    } finally {
      if ( tableLevelResult.getSyncOutcome() == SyncOutcome.WORKING ) {
        publishUpdateNotification(R.string.sync_succeeded_pulling_rows_from_server,
            tableId, maxPercentage);
      } else {
        publishUpdateNotification(R.string.sync_failed_pulling_rows_from_server,
            tableId, maxPercentage);
      }
    }
  }

  /**
   * Populates a ContentValue instance with data from an ArrayList of DataKeyValue
   * and using type information from OrderedColumns
   *
   * @param dkvl Sorted ArrayList of DataKeyValue
   * @param columns OrderedColumns
   * @return ContentValues with data contained in dkvl
   */
  ContentValues dataKeyValueListToContentValues(ArrayList<DataKeyValue> dkvl, OrderedColumns columns) {
    ContentValues cv = new ContentValues();

    for (int i = 0; i < dkvl.size(); i++) {
      DataKeyValue dkv = dkvl.get(i);

      if (dkv.value == null) {
        cv.putNull(dkv.column);
        continue;
      }

      ElementDataType type = columns.find(dkv.column).getType().getDataType();

      if (ElementDataType.string.equals(type)) {
        cv.put(dkv.column, dkv.value);
      } else if (ElementDataType.integer.equals(type)) {
        cv.put(dkv.column, Long.parseLong(dkv.value));
      } else if (ElementDataType.number.equals(type)) {
        cv.put(dkv.column, Double.parseDouble(dkv.value));
      } else if (ElementDataType.bool.equals(type)) {
        cv.put(dkv.column, Boolean.parseBoolean(dkv.value));
      } else {
        // for all other data types, String would be sufficient
        cv.put(dkv.column, dkv.value);
      }
    }

    return cv;
  }
}
