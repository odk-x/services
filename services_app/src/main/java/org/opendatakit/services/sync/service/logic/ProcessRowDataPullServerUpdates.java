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
import org.opendatakit.aggregate.odktables.rest.ConflictType;
import org.opendatakit.aggregate.odktables.rest.ElementDataType;
import org.opendatakit.aggregate.odktables.rest.SyncState;
import org.opendatakit.aggregate.odktables.rest.entity.Column;
import org.opendatakit.aggregate.odktables.rest.entity.DataKeyValue;
import org.opendatakit.aggregate.odktables.rest.entity.RowResource;
import org.opendatakit.aggregate.odktables.rest.entity.RowResourceList;
import org.opendatakit.aggregate.odktables.rest.entity.TableResource;
import org.opendatakit.database.data.ColumnDefinition;
import org.opendatakit.database.data.ColumnList;
import org.opendatakit.database.data.OrderedColumns;
import org.opendatakit.database.data.Row;
import org.opendatakit.database.data.TableDefinitionEntry;
import org.opendatakit.database.data.UserTable;
import org.opendatakit.database.service.DbHandle;
import org.opendatakit.exception.ServicesAvailabilityException;
import org.opendatakit.provider.DataTableColumns;
import org.opendatakit.services.R;
import org.opendatakit.services.sync.service.SyncExecutionContext;
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
public class ProcessRowDataPullServerUpdates extends ProcessRowDataSharedBase {

  private static final String TAG = ProcessRowDataPullServerUpdates.class.getSimpleName();

  private static final double minPercentage = 0.0;
  private static final double maxPercentage = 50.0;
  private static final int numberOfPhases = 2;

  private RowDataServerUpdateActions rowProcessor;

  public ProcessRowDataPullServerUpdates(SyncExecutionContext sharedContext) {
    super(sharedContext);
    this.rowProcessor = new RowDataServerUpdateActions(this);

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
            String[] empty = {};

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
                    empty, empty, null, new String[] { DataTableColumns.ID }, new String[] { "ASC" },
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
          Row localRow = localDataTable.getRowAtIndex(i);
          String stateStr = localRow.getDataByKey(DataTableColumns.SYNC_STATE);
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

          // OK -- the server is reporting a change (in serverRow) to the
          // localRow.
          // if the localRow is already in a in_conflict state, determine
          // what its
          // ConflictType is. If the localRow holds the earlier server-side
          // change,
          // then skip and look at the next record.
          int localRowConflictTypeBeforeSync = -1;
          if (state == SyncState.in_conflict) {
            // we need to remove the in_conflict records that refer to the
            // prior state of the server
            String localRowConflictTypeBeforeSyncStr = localRow
                .getDataByKey(DataTableColumns.CONFLICT_TYPE);
            if (localRowConflictTypeBeforeSyncStr == null) {
              // this row is in conflict. It MUST have a non-null conflict type.
              tableLevelResult.setMessage(sc.getString(R.string.sync_table_erroneous_conflict_type));
              tableLevelResult.setSyncOutcome(SyncOutcome.LOCAL_DATABASE_EXCEPTION);
              return;
            }

            localRowConflictTypeBeforeSync = Integer.parseInt(localRowConflictTypeBeforeSyncStr);
            if (localRowConflictTypeBeforeSync == ConflictType.SERVER_DELETED_OLD_VALUES
                || localRowConflictTypeBeforeSync == ConflictType.SERVER_UPDATED_UPDATED_VALUES) {
              // This localRow holds the server values from a
              // previously-identified conflict.
              // Skip it -- we will clean up this copy later once we find
              // the matching localRow
              // that holds the locally-changed values that were in conflict
              // with this earlier
              // set of server values.
              continue;
            }
          }

          // remove this server row from the map of changes reported by the server.
          // the following decision tree will always place the row into one of the
          // local action lists.
          changedServerRows.remove(rowId);

          if (state == SyncState.synced || state == SyncState.synced_pending_files) {
            // the server's change should be applied locally.
            //
            // the file attachments might be stale locally,
            // but those are dealt with separately.

            if (serverRow.isDeleted()) {

              rowProcessor
                  .deleteRowInDb(db, tableResource, orderedColumns, fileAttachmentColumns,
                      localRow, serverRow.getRowETag(), (state == SyncState.synced_pending_files),
                      tableLevelResult);
            } else {
              // When a prior sync ends with conflicts, we will not update the table's "lastDataETag"
              // and when we next sync, we will pull the same server row updates as when the
              // conflicts were raised (elsewhere in the table).
              //
              // Therefore, we can expect many server row updates to have already been locally
              // applied (for the rows were not in conflict). Detect and ignore these already-
              // processed changes by testing for the server and device having identical field values.
              //

              if (!rowProcessor.identicalValues(orderedColumns, serverElementKeyToIndex, localRow,
                  serverRow)) {
                // Only add a local-update if the server and device rows have different values.
                //
                rowProcessor.updateRowInDb(db, tableResource,
                    orderedColumns,
                    serverElementKeyToIndex,
                    fileAttachmentColumns,
                    localRow, serverRow, (state == SyncState.synced_pending_files),
                    tableLevelResult);
              }
            }
          } else if (serverRow.isDeleted() && (state == SyncState.deleted || (
              state == SyncState.in_conflict
                  && localRowConflictTypeBeforeSync == ConflictType.LOCAL_DELETED_OLD_VALUES))) {
            // this occurs if
            // (1) a delete request was never ACKed but it was performed
            // on the server.
            // (2) if there is an unresolved conflict held locally with the
            // local action being to delete the record, and the prior server
            // state being a value change, but the newly sync'd state now
            // reflects a deletion by another party.
            //

            // no need to worry about server in_conflict records.
            // any server in_conflict rows will be deleted during the delete
            // step

            rowProcessor
                .deleteRowInDb(db, tableResource, orderedColumns, fileAttachmentColumns,
                    localRow, serverRow.getRowETag(),
                    false /* (state == SyncState.synced_pending_files) */,
                    tableLevelResult);

          } else {
            // SyncState.deleted and server is not deleting
            // SyncState.new_row and record exists on server
            // SyncState.changed and new change on server
            // SyncState.in_conflict and new change on server

            // ALSO: this case can occur when our prior sync attempt pulled down changes that
            // placed local row(s) in conflict -- which we have since resolved -- and we are now
            // issuing a sync to push those changes up to the server.
            //
            // This is because we do not update our local table's "lastDataETag" until we have
            // sync'd and applied all changes from the server and have no local conflicts or
            // checkpoints on the table. Because of this, when we issue a sync after resolving
            // a conflict, we will get the set of server row changes that include the row(s)
            // that were previously in conflict. This will appear as one of:
            //    changed | changed
            //    changed | deleted
            //    deleted | changed
            //    deleted | deleted
            //
            // BUT, this time, however, the local records will have the same rowETag as the
            // server record, indicating that they are valid changes (or deletions) on top of
            // the server's current version of this same row.
            //
            // If this is the case (the rowETags match), then we should not place the row into
            // conflict, but should instead ignore the reported content from the server and push
            // the local row's change or delete up to the server in the next section.
            //
            // If not, when we reach this point in the code, the rowETag of the server row
            // should **not match** our local row -- indicating that the server has a change from
            // another source, and that we should place the row into conflict.
            //
            String localRowETag = localRow.getDataByKey(DataTableColumns.ROW_ETAG);
            if (localRowETag != null && localRowETag.equals(serverRow.getRowETag())) {
              // ignore the server record. This is an update we will push to the server.
              continue;
            }

            // no need to worry about server in_conflict records.
            // any server in_conflict rows will be cleaned up during the
            // update of the in_conflict state.

            // figure out what the localRow conflict type should be...
            int localRowConflictType;
            if (state == SyncState.changed) {
              // SyncState.changed and new change on server
              localRowConflictType = ConflictType.LOCAL_UPDATED_UPDATED_VALUES;
              getLogger().i(TAG, "local row was in sync state CHANGED, changing to "
                  + "IN_CONFLICT and setting conflict type to: " + localRowConflictType);
            } else if (state == SyncState.new_row) {
              // SyncState.new_row and record exists on server
              // The 'new_row' case occurs if an insert is never ACKed but
              // completes successfully on the server.
              localRowConflictType = ConflictType.LOCAL_UPDATED_UPDATED_VALUES;
              getLogger().i(TAG, "local row was in sync state NEW_ROW, changing to "
                  + "IN_CONFLICT and setting conflict type to: " + localRowConflictType);
            } else if (state == SyncState.deleted) {
              // SyncState.deleted and server is not deleting
              localRowConflictType = ConflictType.LOCAL_DELETED_OLD_VALUES;
              getLogger().i(TAG, "local row was in sync state DELETED, changing to "
                  + "IN_CONFLICT and updating conflict type to: " + localRowConflictType);
            } else if (state == SyncState.in_conflict) {
              // SyncState.in_conflict and new change on server
              // leave the local conflict type unchanged (retrieve it and
              // use it).
              localRowConflictType = localRowConflictTypeBeforeSync;
              getLogger().i(TAG, "local row was in sync state IN_CONFLICT, leaving as "
                  + "IN_CONFLICT and leaving conflict type unchanged as: "
                  + localRowConflictTypeBeforeSync);
            } else {
              throw new IllegalStateException("Unexpected state encountered");
            }

            if (!rowProcessor.identicalValues(orderedColumns, serverElementKeyToIndex, localRow, serverRow)) {
              if (rowProcessor.identicalValuesExceptRowETagAndFilterScope(orderedColumns,
                  serverElementKeyToIndex, localRow, serverRow)) {
                // just apply the server RowETag and filterScope to the
                // local row
                rowProcessor
                    .updateRowInDb(db, tableResource,
                        orderedColumns,
                        serverElementKeyToIndex,
                        fileAttachmentColumns,
                        localRow, serverRow,
                        true /* (state == SyncState.synced_pending_files) */,
                        tableLevelResult);

              } else {
                rowProcessor
                    .conflictRowInDb(db, tableResource,
                        orderedColumns,
                        serverRow,
                        localRowConflictType,
                        tableLevelResult);
              }
            } else {
              getLogger().w(TAG, "identical rows returned from server -- SHOULDN'T THESE NOT HAPPEN?");
            }
          }
        }

        // Now, go through the remaining serverRows in the rows map. That
        // map now contains only row changes that don't affect any existing
        // localRow. If the server change is not a row-deletion / revoke-row
        // action, then insert the serverRow locally.
        for (RowResource serverRow : changedServerRows.values()) {
          boolean isDeleted = serverRow.isDeleted();
          if (!isDeleted) {
            rowProcessor.insertRowInDb(db, tableResource,
                orderedColumns,
                serverElementKeyToIndex,
                fileAttachmentColumns,
                serverRow,
                tableLevelResult);
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
}
