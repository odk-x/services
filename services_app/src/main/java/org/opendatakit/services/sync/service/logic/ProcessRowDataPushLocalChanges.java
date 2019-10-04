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
import org.opendatakit.aggregate.odktables.rest.entity.RowOutcome;
import org.opendatakit.aggregate.odktables.rest.entity.RowOutcome.OutcomeType;
import org.opendatakit.aggregate.odktables.rest.entity.RowOutcomeList;
import org.opendatakit.aggregate.odktables.rest.entity.TableResource;
import org.opendatakit.database.data.BaseTable;
import org.opendatakit.database.data.ColumnDefinition;
import org.opendatakit.database.data.ColumnList;
import org.opendatakit.database.data.OrderedColumns;
import org.opendatakit.database.data.TableDefinitionEntry;
import org.opendatakit.database.data.TypedRow;
import org.opendatakit.database.data.UserTable;
import org.opendatakit.database.queries.BindArgs;
import org.opendatakit.database.service.DbHandle;
import org.opendatakit.exception.ServicesAvailabilityException;
import org.opendatakit.provider.DataTableColumns;
import org.opendatakit.services.R;
import org.opendatakit.services.sync.service.SyncExecutionContext;
import org.opendatakit.services.sync.service.exceptions.ClientDetectedVersionMismatchedServerResponseException;
import org.opendatakit.sync.service.SyncOutcome;
import org.opendatakit.sync.service.TableLevelResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * SyncProcessor implements the cloud synchronization logic for Tables.
 *
 * @author the.dylan.price@gmail.com
 * @author sudar.sam@gmail.com
 *
 */
class ProcessRowDataPushLocalChanges extends ProcessRowDataSharedBase {

  private static final String TAG = ProcessRowDataPushLocalChanges.class.getSimpleName();

  private static final double minPercentage = 50.0;
  private static final double maxPercentage = 75.0;

  private static final int UPSERT_BATCH_SIZE = 500;

  ProcessRowDataPushLocalChanges(SyncExecutionContext sharedContext) {
    super(sharedContext);

    setUpdateNotificationBounds(minPercentage, maxPercentage, 1);
  }

  /**
   * We pushed changes up to the server and now need to update the local rowETags to match
   * the rowETags assigned to those changes by the server.
   *
   * Does not update tableResult's syncOutcome
   *
   * @param resource        server schemaETag, dataETag etc. before(at) changes were pushed
   * @param tableLevelResult          for progress UI
   * @param orderedColumns            all user-defined columns in the table
   * @param fileAttachmentColumns     columns in the table that hold rowPath values
   * @param segmentAlter  changes that were pushed to server
   * @param outcomes      result of pushing those changes to server
   * @throws ServicesAvailabilityException
   */
  private void processRowOutcomes(TableResource resource,
      TableLevelResult tableLevelResult, OrderedColumns orderedColumns,
      ArrayList<ColumnDefinition> fileAttachmentColumns,
      List<TypedRow> segmentAlter, ArrayList<RowOutcome> outcomes)
      throws ServicesAvailabilityException, IOException {

    // For speed, do this all within a transaction. Processing is
    // all in-memory except when we are deleting a client row. In that
    // case, there may be SDCard access to delete the attachments for
    // the client row. But that is local access, and the commit will
    // be accessing the same device.
    //
    // i.e., no network access in this code, so we can place it all within
    // a transaction and not lock up the database for very long.
    //

    DbHandle db = null;
    try {
      db = sc.getDatabase();

      boolean specialCase = false;
      boolean badState = false;
      for (int i = 0; i < segmentAlter.size(); ++i) {
        RowOutcome serverRow = outcomes.get(i);
        TypedRow localRow = segmentAlter.get(i);
        String localRowId = localRow.getRawStringByKey(DataTableColumns.ID);
        if (!serverRow.getRowId().equals(localRowId)) {
          throw new ClientDetectedVersionMismatchedServerResponseException("Unexpected reordering of return");
        }
        if (serverRow.getOutcome() == OutcomeType.SUCCESS) {

          if (serverRow.isDeleted()) {

            // we should delete the LOCAL row because we have successfully deleted the server row.
            sc.getDatabaseService().privilegedDeleteRowWithId(
                sc.getAppName(), db, resource.getTableId(), orderedColumns,
                localRow.getRawStringByKey(DataTableColumns.ID));
            tableLevelResult.incLocalDeletes();
            publishUpdateNotification(R.string.sync_deleting_local_row, resource.getTableId());
            tableLevelResult.incServerDeletes();

          } else {

            boolean hasNonEmptyAttachmentColumns = false;
            for ( ColumnDefinition cd : fileAttachmentColumns ) {
              String uriFragment = localRow.getRawStringByKey(cd.getElementKey());
              if ( uriFragment != null ) {
                hasNonEmptyAttachmentColumns = true;
                break;
              }
            }
            SyncState newSyncState = hasNonEmptyAttachmentColumns
                ? SyncState.synced_pending_files : SyncState.synced;
            sc.getDatabaseService().privilegedUpdateRowETagAndSyncState(
                sc.getAppName(), db,
                resource.getTableId(),
                serverRow.getRowId(),
                serverRow.getRowETag(),
                newSyncState.name());

            publishUpdateNotification(R.string.sync_server_row_updated, resource.getTableId());
            // UPDATE or INSERT
            tableLevelResult.incServerUpserts();
          }

        } else if (serverRow.getOutcome() == OutcomeType.FAILED) {
          if (serverRow.getRowId() == null || !serverRow.isDeleted()) {
            // should never occur!!!
            badState = true;
          } else {
            // special case of a delete where server has no record of the row.
            // server should add row and mark it as deleted.
            // TODO: verify this is handled on server?
            // TODO: shouldn't we delete the local row?
          }

          publishUpdateNotification(R.string.sync_server_row_update_failed, resource.getTableId());

        } else if (serverRow.getOutcome() == OutcomeType.IN_CONFLICT) {
          // another device updated this record between the time we fetched
          // changes and the time we tried to update this record. Transition
          // the record locally into the conflicting state.
          // SyncState.deleted and server is not deleting
          // SyncState.new_row and record exists on server
          // SyncState.changed and new change on server
          // SyncState.in_conflict and new change on server

          // no need to worry about server in_conflict records.
          // any server in_conflict rows will be removed during
          // the update to the in_conflict state.

          ContentValues values = new ContentValues();

          // set up to insert the in_conflict row from the server
          for (DataKeyValue entry : serverRow.getValues()) {
            String colName = entry.column;
            values.put(colName, entry.value);
          }

          // insert in_conflict server row
          values.put(DataTableColumns.ROW_ETAG, serverRow.getRowETag());
          values.put(DataTableColumns.SYNC_STATE, (serverRow.isDeleted() ?
              SyncState.deleted.name() : SyncState.changed.name()));
          values.putNull(DataTableColumns.CONFLICT_TYPE);
          values.put(DataTableColumns.FORM_ID, serverRow.getFormId());
          values.put(DataTableColumns.LOCALE, serverRow.getLocale());
          values.put(DataTableColumns.SAVEPOINT_TIMESTAMP, serverRow.getSavepointTimestamp());
          values.put(DataTableColumns.SAVEPOINT_CREATOR, serverRow.getSavepointCreator());
          values.put(DataTableColumns.SAVEPOINT_TYPE, serverRow.getSavepointType());
          RowFilterScope.Access type = serverRow.getRowFilterScope().getDefaultAccess();
          values.put(DataTableColumns.DEFAULT_ACCESS,
              (type == null) ? RowFilterScope.Access.FULL.name() : type.name());
          values.put(DataTableColumns.ROW_OWNER, serverRow.getRowFilterScope().getRowOwner());

          values.put(DataTableColumns.GROUP_MODIFY, serverRow.getRowFilterScope().getGroupModify());
          values.put(DataTableColumns.GROUP_READ_ONLY, serverRow.getRowFilterScope().getGroupReadOnly());
          values.put(DataTableColumns.GROUP_PRIVILEGED, serverRow.getRowFilterScope().getGroupPrivileged());

          sc.getDatabaseService().privilegedPerhapsPlaceRowIntoConflictWithId(sc.getAppName(), db,
              resource.getTableId(),
              orderedColumns, values, serverRow.getRowId());

        } else if (serverRow.getOutcome() == OutcomeType.DENIED) {
          specialCase = true;
          // TODO: use different status message ???
          publishUpdateNotification(R.string.sync_server_row_update_denied, resource.getTableId());
        } else {
          // a new OutcomeType state was added!
          throw new IllegalStateException("Unexpected OutcomeType! " + serverRow.getOutcome().name());
        }
      }

      if (specialCase) {
        // user does not have privileges...
        throw new IllegalStateException(
            "update request rejected by the server -- do you have table synchronize privileges?");
      }

      if (badState) {
        // TODO: we could update all the other row state then throw this...
        throw new IllegalStateException(
            "Unexpected null rowId or OutcomeType.FAILED when not deleting row");
      }
    } finally {
      if (db != null) {
        sc.releaseDatabase(db);
        db = null;
      }
    }
  }

  /**
   * Push local changes up to the server.
   * <p>
   * This method does NOT synchronize files. It just sync's the table's rows.
   *
   * @param tableResource
   *          the table resource from the server, either from the getTables()
   *          call or from a createTable() response.
   * @param te
   *          definition of the table to synchronize
   * @param orderedColumns
   *          well-formed ordered list of columns in this table.
   * @param fileAttachmentColumns
   *          list of columns that contain file attachments.
   * @return true if changes need to be pulled from the server before continuing
   * @throws ServicesAvailabilityException
   */
  public boolean pushLocalChanges(TableResource tableResource,
      TableDefinitionEntry te, OrderedColumns orderedColumns,
      ArrayList<ColumnDefinition> fileAttachmentColumns)
      throws ServicesAvailabilityException {

    // Prepare the tableLevelResult. We'll start it as failure, and only update it
    // if we're successful at the end.
    String tableId = te.getTableId();
    TableLevelResult tableLevelResult = sc.getTableLevelResult(tableId);

    getLogger().i(TAG, "pushLocalChanges " + tableId);

    tableLevelResult.setMessage("scanning for changes to send to server");

    publishUpdateNotification(R.string.sync_calculating_rows_to_push_to_server, tableId, -1.0);

    String local_id_table = "L__" + tableId;

    // figure out how many rows there are to sync.
    int rowsToSyncCount = 0;
    {
      DbHandle db = null;
      try {
        db = sc.getDatabase();

        // We need to create a temporary table and fill it with all the IDs of
        // the rows that should be pushed to the server.
        //
        // This creates a static list of ids that we can then work off of.
        //
        // If we didn't do this, in the course of pushing the first batch of
        // local rows, we would be updating the field values that were used to
        // select the rows to push, and would lose track of where the second
        // batch of local rows begins.
        //
        List<Column> columns = new ArrayList<Column>();
        columns.add(
            new Column(ID_COLUMN, ID_COLUMN, ElementDataType.string.name(),
                "[]"));
        ColumnList columnList = new ColumnList(columns);

        // create the table (drop it first -- to get an empty table)
        sc.getDatabaseService().deleteLocalOnlyTable(sc.getAppName(), db, local_id_table);
        sc.getDatabaseService()
            .createLocalOnlyTableWithColumns(sc.getAppName(), db, local_id_table, columnList);


        String sqlCommand;
        BindArgs bindArgs = new BindArgs(new Object[]{
            SyncState.new_row.name(), SyncState.changed.name(), SyncState.deleted.name() });
        {
          StringBuilder sqlCommandBuilder = new StringBuilder();
          sqlCommandBuilder.append("INSERT INTO ").append(local_id_table)
              .append(" (").append(ID_COLUMN).append(" ) SELECT DISTINCT ")
              .append(DataTableColumns.ID).append(" FROM ").append(tableId)
              .append(" WHERE ").append(DataTableColumns.SYNC_STATE)
              .append(" IN (?, ?, ?) AND ")
              .append(DataTableColumns.ID).append(" NOT IN (SELECT DISTINCT ")
              .append(DataTableColumns.ID).append(" FROM ").append(tableId).append(" WHERE ")
              .append(DataTableColumns.SAVEPOINT_TYPE).append(" IS NULL)");
          sqlCommand = sqlCommandBuilder.toString();
        }

        // create the list of IDs
        sc.getDatabaseService().privilegedExecute(sc.getAppName(), db, sqlCommand, bindArgs);

        // now count the number
        StringBuilder b = new StringBuilder();
        b.append("SELECT COUNT(*) as rowCount FROM ").append(local_id_table);

        BaseTable bt = sc.getDatabaseService().arbitrarySqlQuery(sc.getAppName(), db, null,
            b.toString(), null, null, null);
        if ( bt.getNumberOfRows() != 1 || bt.getColumnIndexOfElementKey("rowCount") != 0 ) {
          tableLevelResult
              .setMessage("Unable to retrieve count of rows to send to server");
          tableLevelResult.setSyncOutcome(SyncOutcome.LOCAL_DATABASE_EXCEPTION);
          return false;
        }
        rowsToSyncCount = bt.getRowAtIndex(0).getDataType(0, Long.class).intValue();

      } finally {
        sc.releaseDatabase(db);
        db = null;
      }
    }

    if ( rowsToSyncCount != 0 ) {

      setUpdateNotificationBounds(minPercentage, maxPercentage, rowsToSyncCount);

      String whereClause;
      {
        StringBuilder whereClauseBuilder = new StringBuilder();
        whereClauseBuilder.append(DataTableColumns.ID).append(" IN (SELECT ")
            .append(ID_COLUMN).append(" FROM ").append(local_id_table)
            .append(" LIMIT ? OFFSET ? )");
        whereClause = whereClauseBuilder.toString();
      }

      // these are all the various actions we will need to take:
      int fetchOffset = 0;
      int fetchLimit = (orderedColumns.getColumnDefinitions().size() > maxColumnsToUseLargeFetchLimit)
          ? smallFetchLimit : largeFetchLimit;

      for (; ; ) {

        publishUpdateNotification(R.string.sync_anaylzing_local_row_changes, tableId, -1.0);

        UserTable localDataTable;
        try {
          // //////////////////////////////////////////////////
          // //////////////////////////////////////////////////
          // get fetchLimit number of rows in the data table
          {
            DbHandle db = null;
            try {
              db = sc.getDatabase();
              String[] empty = {};
              BindArgs bindArgs = new BindArgs(new Object[] {fetchLimit, fetchOffset});

              localDataTable = sc.getDatabaseService()
                  .privilegedSimpleQuery(sc.getAppName(), db, tableId, orderedColumns, whereClause,
                      bindArgs, empty, null, new String[] { DataTableColumns.ID },
                      new String[] { "ASC" }, null, null);
            } finally {
              sc.releaseDatabase(db);
              db = null;
            }
          }

          fetchOffset += localDataTable.getNumberOfRows();

          /**************************
           * PART 2: UPDATE THE DATA
           **************************/

          // /////////////////////////////////////
          // INFORM SERVER OF LOCAL CHANGES
          // INFORM SERVER OF LOCAL CHANGES
          // INFORM SERVER OF LOCAL CHANGES
          // INFORM SERVER OF LOCAL CHANGES
          // INFORM SERVER OF LOCAL CHANGES

          // idempotent interface means that the interactions
          // for inserts, updates and deletes are identical.
           if (localDataTable.getNumberOfRows() != 0) {
            tableLevelResult.setHadLocalDataChanges(true);

            int sendOffset = 0;
            while (sendOffset < localDataTable.getNumberOfRows()) {
              // alter UPSERT_BATCH_SIZE rows at a time to the server
              int max = sendOffset + UPSERT_BATCH_SIZE;
              if (max > localDataTable.getNumberOfRows()) {
                max = localDataTable.getNumberOfRows();
              }

              List<TypedRow> segmentAlter = new ArrayList<TypedRow>();
              for (int i = sendOffset; i < max; ++i) {
                segmentAlter.add(localDataTable.getRowAtIndex(i));
              }

              publishUpdateNotification(R.string.sync_pushing_local_row_changes_to_server,
                  tableId, -1.0);

              RowOutcomeList outcomes = sc.getSynchronizer()
                  .pushLocalRows(tableResource, orderedColumns, segmentAlter);
              if (outcomes == null) {
                // can't proceed because the server dataETag has changed.
                // Signal that we need to re-pull server updates then
                // try this request (more or less) again.
                return true;
              }

              if (outcomes.getRows().size() != segmentAlter.size()) {
                throw new IllegalStateException("Unexpected partial return?");
              }

              // process outcomes...
              processRowOutcomes(tableResource, tableLevelResult,
                  orderedColumns, fileAttachmentColumns, segmentAlter, outcomes.getRows());

              publishUpdateNotification(R.string.sync_updating_data_etag_after_push_to_server,
                  tableId, -1.0);

              // update our dataETag. Because the server will have failed with
              // a CONFLICT (409) if our dataETag did not match ours at the
              // time the update occurs, we are assured that there are no
              // interleaved changes we are unaware of.
              {
                DbHandle db = null;
                try {
                  db = sc.getDatabase();
                  // update the dataETag to the one returned by the server-update request.
                  sc.getDatabaseService().privilegedUpdateTableETags(sc.getAppName(), db, tableId,
                      tableResource.getSchemaETag(), outcomes.getDataETag());
                  // we won't be able to issue another server update unless we adopt
                  // this new etag value. Don't forget to update our in-memory values...
                  te.setSchemaETag(tableResource.getSchemaETag());
                  te.setLastDataETag(outcomes.getDataETag());
                  tableResource.setDataETag(outcomes.getDataETag());
                } finally {
                  sc.releaseDatabase(db);
                  db = null;
                }
              }

              // process next segment...
              sendOffset = max;
            }
          }
        } catch (Exception e) {
          exception("synchronizeTable - pushing data up to server", tableId, e, tableLevelResult);
          return false;
        }

        if (localDataTable.getNumberOfRows() < fetchLimit) {
          // done!
          // OK. Now we have pushed everything.
          // because of the 409 (CONFLICT) alterRows enforcement on the
          // server, we know that our data records are consistent and
          // our processing is complete.

          // And now update that we've pushed our changes to the server.
          tableLevelResult.setPushedLocalData(true);
          break;
        }
      }
    }

    publishUpdateNotification(R.string.sync_completed_push_to_server,
        tableId, maxPercentage);


    // if we got here, we reached some state of completion and
    // do not need to pull changes from the server.
    return false;
  }
}
