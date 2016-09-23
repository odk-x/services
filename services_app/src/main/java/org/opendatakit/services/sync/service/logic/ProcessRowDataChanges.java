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

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.opendatakit.aggregate.odktables.rest.ConflictType;
import org.opendatakit.aggregate.odktables.rest.ElementDataType;
import org.opendatakit.aggregate.odktables.rest.SyncState;
import org.opendatakit.aggregate.odktables.rest.entity.*;
import org.opendatakit.aggregate.odktables.rest.entity.RowOutcome.OutcomeType;
import org.opendatakit.database.data.ColumnDefinition;
import org.opendatakit.database.data.OrderedColumns;
import org.opendatakit.database.data.TableDefinitionEntry;
import org.opendatakit.database.data.UserTable;
import org.opendatakit.exception.ServicesAvailabilityException;
import org.opendatakit.provider.DataTableColumns;
import org.opendatakit.provider.FormsColumns;
import org.opendatakit.logging.WebLogger;
import org.opendatakit.logging.WebLoggerIf;
import org.opendatakit.database.service.DbHandle;
import org.opendatakit.database.data.Row;
import org.opendatakit.services.R;
import org.opendatakit.services.sync.service.SyncExecutionContext;
import org.opendatakit.sync.service.*;
import org.opendatakit.services.sync.service.data.SyncRow;
import org.opendatakit.services.sync.service.data.SyncRowDataChanges;
import org.opendatakit.services.sync.service.data.SyncRowPending;
import org.opendatakit.services.sync.service.exceptions.ClientDetectedVersionMismatchedServerResponseException;
import org.opendatakit.services.sync.service.exceptions.HttpClientWebException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * SyncProcessor implements the cloud synchronization logic for Tables.
 *
 * @author the.dylan.price@gmail.com
 * @author sudar.sam@gmail.com
 *
 */
public class ProcessRowDataChanges {

  private static final String TAG = ProcessRowDataChanges.class.getSimpleName();

  private static final int UPSERT_BATCH_SIZE = 500;
  private static final int ROWS_BETWEEN_PROGRESS_UPDATES = 10;
  private static final ObjectMapper mapper;

  static {
    mapper = new ObjectMapper();
    mapper.setVisibilityChecker(mapper.getVisibilityChecker().withFieldVisibility(Visibility.ANY));
  }

  private WebLoggerIf log;

  private final SyncExecutionContext sc;

  private Double perRowIncrement;
  private int rowsProcessed;
  private ProcessManifestContentAndFileChanges manifestProcessor;

  public ProcessRowDataChanges(SyncExecutionContext sharedContext) {
    this.sc = sharedContext;
    this.log = WebLogger.getLogger(sc.getAppName());
    this.manifestProcessor = new ProcessManifestContentAndFileChanges(sc);
  }

  /**
   * Common error reporting...
   *
   * @param method
   * @param tableId
   * @param e
   * @param tableLevelResult
   */
  private void exception(String method, String tableId, Exception e, TableLevelResult tableLevelResult) {
    String msg = e.getMessage();
    if (msg == null) {
      msg = e.toString();
    }

    String fmtMsg = String.format("Exception in %s on table: %s exception: %s", method, tableId,
        msg);

    log.e(TAG, fmtMsg);
    log.printStackTrace(e);

    SyncOutcome outcome = sc.exceptionEquivalentOutcome(e);
    tableLevelResult.setSyncOutcome(outcome);
    tableLevelResult.setMessage(fmtMsg);
  }

  /**
   * Synchronize all synchronized tables with the cloud.
   * <p>
   * This becomes more complicated with the ability to synchronize files. The
   * new order is as follows:
   * <ol>
   * <li>Synchronize app-level files. (i.e. those files under the appid
   * directory that are NOT then under the tables, instances, metadata, or
   * logging directories.) This is a multi-part process:
   * <ol>
   * <li>Get the app-level manifest, download any files that have changed
   * (differing hashes) or that do not exist.</li>
   * <li>Upload the files that you have that are not on the manifest. Note that
   * this could be suppressed if the user does not have appropriate permissions.
   * </li>
   * </ol>
   * </li>
   *
   * <li>Synchronize the static table files for those tables that are set to
   * sync. (i.e. those files under "appid/tables/tableid"). This follows the
   * same multi-part steps above (1a and 1b).</li>
   *
   * <li>Synchronize the table properties/metadata.</li>
   *
   * <li>Synchronize the table data. This includes the data in the db as well as
   * those files under "appid/instances/tableid". This file synchronization
   * follows the same multi-part steps above (1a and 1b).</li>
   *
   * <li>TODO: step four--the synchronization of instances files--should perhaps
   * also be allowed to be modular and permit things like ODK Submit to handle
   * data and files separately.</li>
   * </ol>
   * <p>
   * TODO: This should also somehow account for zipped files, exploding them or
   * what have you.
   * </p>
   *
   * @param workingListOfTables
   *          -- the list of tables we should sync with the server. This will be
   *          a subset of the available local tables -- if there was any error
   *          during the sync'ing of the table-level files, or if the table
   *          schema does not match, the local table will be omitted from this
   *          list.
   * @throws ServicesAvailabilityException
   */
  public void synchronizeDataRowsAndAttachments(List<TableResource> workingListOfTables,
      SyncAttachmentState attachmentState) throws ServicesAvailabilityException {
    log.i(TAG, "entered synchronize()");

    DbHandle db = null;

    // we can assume that all the local table properties should
    // sync with the server.
    for (TableResource tableResource : workingListOfTables) {
      // Sync the local media files with the server if the table
      // existed locally before we attempted downloading it.

      String tableId = tableResource.getTableId();
      TableDefinitionEntry te;
      OrderedColumns orderedDefns;
      String displayName;
      try {
        db = sc.getDatabase();
        te = sc.getDatabaseService().getTableDefinitionEntry(sc.getAppName(), db,
            tableId);
        orderedDefns = sc.getDatabaseService().getUserDefinedColumns(sc.getAppName(), db, tableId);
        displayName = sc.getTableDisplayName(tableId);
      } finally {
        sc.releaseDatabase(db);
        db = null;
      }

      synchronizeTableDataRowsAndAttachments(tableResource, te, orderedDefns, displayName,
          attachmentState);
      sc.incMajorSyncStep();
    }
  }

  /**
   * This and its callers do not set the tableResult sync outcome.
   *
   * @param tableResource
   * @param te
   * @param orderedColumns
   * @param displayName
   * @param attachmentState
   * @param fileAttachmentColumns
   * @param localDataTable
   * @param rows
   * @return
   * @throws IOException
   * @throws ServicesAvailabilityException
   */
  private UserTable updateLocalRowsFromServerChanges(TableResource tableResource,
      TableDefinitionEntry te, OrderedColumns orderedColumns, String displayName,
      SyncAttachmentState attachmentState, ArrayList<ColumnDefinition> fileAttachmentColumns,
      UserTable localDataTable, RowResourceList rows)
      throws IOException, ServicesAvailabilityException
  {
    String tableId = tableResource.getTableId();
    TableLevelResult tableLevelResult = sc.getTableLevelResult(tableId);

    if (rows.getRows().isEmpty()) {
      // nothing here -- let caller determine whether we are done or
      // whether we need to issue another request to the server.
      return localDataTable;
    }

    Map<String, SyncRow> changedServerRows = new HashMap<String, SyncRow>();
    for (RowResource row : rows.getRows()) {
      SyncRow syncRow = new SyncRow(row.getRowId(), row.getRowETag(), row.isDeleted(),
          row.getFormId(), row.getLocale(), row.getSavepointType(), row.getSavepointTimestamp(),
          row.getSavepointCreator(), row.getRowFilterScope(), row.getValues(), fileAttachmentColumns);
      changedServerRows.put(row.getRowId(), syncRow);
    }

    sc.updateNotification(SyncProgressState.ROWS, R.string.sync_applying_batch_server_row_changes,
        new Object[] { tableId }, 7.0, false);

    /**************************
     * PART 2: UPDATE THE DATA
     **************************/
    log.d(TAG, "updateDbFromServer setServerHadDataChanges(true)");
    tableLevelResult.setServerHadDataChanges(!changedServerRows.isEmpty());
    // these are all the various actions we will need to take:

    // serverRow updated; no matching localRow
    List<SyncRowDataChanges> rowsToInsertLocally = new ArrayList<SyncRowDataChanges>();

    // serverRow updated; localRow SyncState is synced or
    // synced_pending_files
    List<SyncRowDataChanges> rowsToUpdateLocally = new ArrayList<SyncRowDataChanges>();

    // serverRow deleted; localRow SyncState is synced or
    // synced_pending_files
    List<SyncRowDataChanges> rowsToDeleteLocally = new ArrayList<SyncRowDataChanges>();

    // serverRow updated or deleted; localRow SyncState is not synced or
    // synced_pending_files
    List<SyncRowDataChanges> rowsToMoveToInConflictLocally = new ArrayList<SyncRowDataChanges>();

    // loop through the localRow table
    for (int i = 0; i < localDataTable.getNumberOfRows(); i++) {
      Row localRow = localDataTable.getRowAtIndex(i);
      String stateStr = localRow.getDataByKey(DataTableColumns.SYNC_STATE);
      SyncState state = stateStr == null ? null : SyncState.valueOf(stateStr);

      String rowId = localDataTable.getRowId(i);

      // see if there is a change to this row from our current
      // server change set.
      SyncRow serverRow = changedServerRows.get(rowId);

      if (serverRow == null) {
        continue;
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
        localRowConflictTypeBeforeSync = localRowConflictTypeBeforeSyncStr == null ? null : Integer
            .parseInt(localRowConflictTypeBeforeSyncStr);
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

      // remove this server row from the map of changes reported by the
      // server.
      // the following decision tree will always place the row into one
      // of the
      // local action lists.
      changedServerRows.remove(rowId);

      // OK the record is either a simple local record or a local
      // in_conflict record
      if (state == SyncState.synced || state == SyncState.synced_pending_files) {
        // the server's change should be applied locally.
        //
        // the file attachments might be stale locally,
        // but those are dealt with separately.

        if (serverRow.isDeleted()) {
          rowsToDeleteLocally.add(new SyncRowDataChanges(serverRow, SyncRow.convertToSyncRow(
              orderedColumns, fileAttachmentColumns, localRow),
              (state == SyncState.synced_pending_files)));
        } else {
          // When a prior sync ends with conflicts, we will not update the table's "lastDataETag"
          // and when we next sync, we will pull the same server row updates as when the
          // conflicts were raised (elsewhere in the table).
          //
          // Therefore, we can expect many server row updates to have already been locally
          // applied (for the rows were not in conflict). Detect and ignore these already-
          // processed changes by testing for the server and device having identical field values.
          //
          SyncRowDataChanges syncRow = new SyncRowDataChanges(serverRow, SyncRow.convertToSyncRow(
              orderedColumns, fileAttachmentColumns, localRow), false, ConflictType.LOCAL_UPDATED_UPDATED_VALUES);

          if (!syncRow.identicalValues(orderedColumns)) {
            // Only add a local-update if the server and device rows have different values.
            //
            rowsToUpdateLocally.add(new SyncRowDataChanges(serverRow, SyncRow.convertToSyncRow(
                orderedColumns, fileAttachmentColumns, localRow),
                (state == SyncState.synced_pending_files)));
          }
        }
      } else if (serverRow.isDeleted()
          && (state == SyncState.deleted || (state == SyncState.in_conflict && localRowConflictTypeBeforeSync == ConflictType.LOCAL_DELETED_OLD_VALUES))) {
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
        rowsToDeleteLocally.add(new SyncRowDataChanges(serverRow, SyncRow.convertToSyncRow(
            orderedColumns, fileAttachmentColumns, localRow), false));
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
        if ( localRowETag != null && localRowETag.equals(serverRow.getRowETag()) ) {
          // ignore the server record. This is an update we will push to the server.
          continue;
        }

        // no need to worry about server in_conflict records.
        // any server in_conflict rows will be cleaned up during the
        // update of the in_conflict state.

        // figure out what the localRow conflict type should be...
        Integer localRowConflictType;
        if (state == SyncState.changed) {
          // SyncState.changed and new change on server
          localRowConflictType = ConflictType.LOCAL_UPDATED_UPDATED_VALUES;
          log.i(TAG, "local row was in sync state CHANGED, changing to "
              + "IN_CONFLICT and setting conflict type to: " + localRowConflictType);
        } else if (state == SyncState.new_row) {
          // SyncState.new_row and record exists on server
          // The 'new_row' case occurs if an insert is never ACKed but
          // completes successfully on the server.
          localRowConflictType = ConflictType.LOCAL_UPDATED_UPDATED_VALUES;
          log.i(TAG, "local row was in sync state NEW_ROW, changing to "
              + "IN_CONFLICT and setting conflict type to: " + localRowConflictType);
        } else if (state == SyncState.deleted) {
          // SyncState.deleted and server is not deleting
          localRowConflictType = ConflictType.LOCAL_DELETED_OLD_VALUES;
          log.i(TAG, "local row was in sync state DELETED, changing to "
              + "IN_CONFLICT and updating conflict type to: " + localRowConflictType);
        } else if (state == SyncState.in_conflict) {
          // SyncState.in_conflict and new change on server
          // leave the local conflict type unchanged (retrieve it and
          // use it).
          localRowConflictType = localRowConflictTypeBeforeSync;
          log.i(TAG, "local row was in sync state IN_CONFLICT, leaving as "
              + "IN_CONFLICT and leaving conflict type unchanged as: "
              + localRowConflictTypeBeforeSync);
        } else {
          throw new IllegalStateException("Unexpected state encountered");
        }
        SyncRowDataChanges syncRow = new SyncRowDataChanges(serverRow, SyncRow.convertToSyncRow(
            orderedColumns, fileAttachmentColumns, localRow), false, localRowConflictType);

        if (!syncRow.identicalValues(orderedColumns)) {
          if (syncRow.identicalValuesExceptRowETagAndFilterScope(orderedColumns)) {
            // just apply the server RowETag and filterScope to the
            // local row
            rowsToUpdateLocally.add(new SyncRowDataChanges(serverRow, SyncRow.convertToSyncRow(
                orderedColumns, fileAttachmentColumns, localRow), true));
          } else {
            rowsToMoveToInConflictLocally.add(syncRow);
          }
        } else {
          log.w(TAG, "identical rows returned from server -- SHOULDN'T THESE NOT HAPPEN?");
        }
      }
    }

    // Now, go through the remaining serverRows in the rows map. That
    // map now contains only row changes that don't affect any existing
    // localRow. If the server change is not a row-deletion / revoke-row
    // action, then insert the serverRow locally.
    for (SyncRow serverRow : changedServerRows.values()) {
      boolean isDeleted = serverRow.isDeleted();
      if (!isDeleted) {
        rowsToInsertLocally.add(new SyncRowDataChanges(serverRow, null, false));
      }
    }

    //
    // OK we have captured the local inserting, locally updating,
    // locally deleting and conflicting actions. And we know
    // the changes for the server. Determine the per-row percentage
    // for applying all these changes

    int totalChange = rowsToInsertLocally.size() + rowsToUpdateLocally.size()
        + rowsToDeleteLocally.size() + rowsToMoveToInConflictLocally.size();

    perRowIncrement = 70.0 / ((double) (totalChange + 1));
    rowsProcessed = 0;
    boolean hasAttachments = !fileAttachmentColumns.isEmpty();

    // i.e., we have created entries in the various action lists
    // for all the actions we should take.

    // ///////////////////////////////////////////////////
    // / PERFORM LOCAL DATABASE CHANGES
    // / PERFORM LOCAL DATABASE CHANGES
    // / PERFORM LOCAL DATABASE CHANGES
    // / PERFORM LOCAL DATABASE CHANGES
    // / PERFORM LOCAL DATABASE CHANGES

    {
      DbHandle db = null;
      boolean successful = false;
      try {
        db = sc.getDatabase();

        // this will individually move some files to the locally-deleted state
        // if we cannot sync file attachments in those rows.
        pushLocalAttachmentsBeforeDeleteRowsInDb(db, tableResource, rowsToDeleteLocally);

        deleteRowsInDb(db, tableResource, orderedColumns, rowsToDeleteLocally,
            tableLevelResult);

        insertRowsInDb(db, tableResource, orderedColumns, rowsToInsertLocally,
            hasAttachments, tableLevelResult);

        updateRowsInDb(db, tableResource, orderedColumns, rowsToUpdateLocally,
            hasAttachments, tableLevelResult);

        conflictRowsInDb(db, tableResource, orderedColumns, rowsToMoveToInConflictLocally,
            tableLevelResult);

        String[] empty = {};

        localDataTable = sc.getDatabaseService()
            .privilegedSimpleQuery(sc.getAppName(), db, tableId, orderedColumns, null, empty,
                empty, null,
                new String[] { DataTableColumns.ID }, new String[] { "ASC" }, null, null);

        // TODO: fix this for synced_pending_files
        // We likely need to relax this constraint on the
        // server?
        successful = true;
      } finally {
        if (db != null) {
          sc.releaseDatabase(db);
          db = null;
        }
      }
    }

    return localDataTable;
  }

  private int countRowToSyncFileAttachments(ArrayList<ColumnDefinition> fileAttachmentColumns,
      SyncState state) {
    if (state == SyncState.in_conflict) {
      if ( !fileAttachmentColumns.isEmpty() ) {
        return 1;
      }
    } else if (state == SyncState.synced_pending_files) {
      return 1;
    }
    return 0;
  }

  /**
   * Common routine to determine what to do w.r.t. constructing the set of rows requiring
   * syncing of file attachments.
   *
   * @param rowsToSyncFileAttachments
   * @param orderedColumns
   * @param fileAttachmentColumns
   * @param localRow
   * @param state
   */
  private void perhapsAddToRowsToSyncFileAttachments(List<SyncRowPending> rowsToSyncFileAttachments,
      OrderedColumns orderedColumns,
      ArrayList<ColumnDefinition> fileAttachmentColumns,
      Row localRow,
      SyncState state) {
    if (state == SyncState.in_conflict) {
      if ( !fileAttachmentColumns.isEmpty() ) {
        // fetch the file attachments for an in_conflict row but don't delete
        // anything and never update the state to synced (it must stay in in_conflict)
        rowsToSyncFileAttachments.add(new SyncRowPending(SyncRow.convertToSyncRow(
            orderedColumns, fileAttachmentColumns, localRow), true, false, false));
      }
    } else if (state == SyncState.synced_pending_files) {
      // if we succeed in fetching and deleting the local files to match the server
      // then update the state to synced.
      rowsToSyncFileAttachments.add(new SyncRowPending(SyncRow.convertToSyncRow(
          orderedColumns, fileAttachmentColumns, localRow), false, true, true));
    }
  }

  /**
   * Synchronize the table data rows.
   * <p>
   * Note that if the db changes under you when calling this method, the tp
   * parameter will become out of date. It should be refreshed after calling
   * this method.
   * <p>
   * This method does NOT synchronize any non-instance files; it assumes the
   * database schema has already been sync'd.
   *
   * @param tableResource
   *          the table resource from the server, either from the getTables()
   *          call or from a createTable() response.
   * @param te
   *          definition of the table to synchronize
   * @param orderedColumns
   *          well-formed ordered list of columns in this table.
   * @param displayName
   *          display name for this tableId - used in notifications
   * @param attachmentState
   * @throws ServicesAvailabilityException
   */
  private void synchronizeTableDataRowsAndAttachments(TableResource tableResource,
      TableDefinitionEntry te, OrderedColumns orderedColumns, String displayName,
      SyncAttachmentState attachmentState) throws ServicesAvailabilityException {
    boolean attachmentSyncSuccessful = false;
    boolean rowDataSyncSuccessful = false;

    ArrayList<ColumnDefinition> fileAttachmentColumns = new ArrayList<ColumnDefinition>();
    for (ColumnDefinition cd : orderedColumns.getColumnDefinitions()) {
      if (cd.getType().getDataType() == ElementDataType.rowpath) {
        fileAttachmentColumns.add(cd);
      }
    }

    log.i(
        TAG,
        "synchronizeTableDataRowsAndAttachments - attachmentState: "
            + attachmentState.toString());

    // Prepare the tableLevelResult. We'll start it as failure, and only update it
    // if we're successful at the end.
    String tableId = te.getTableId();
    TableLevelResult tableLevelResult = sc.getTableLevelResult(tableId);
    tableLevelResult.setTableDisplayName(displayName);
    if (tableLevelResult.getSyncOutcome() != SyncOutcome.WORKING) {
      // there was some sort of error...
      log.e(TAG, "Skipping data sync - error in table schema or file verification step " + tableId);
      sc.updateNotification(SyncProgressState.ROWS,
          R.string.sync_table_data_sync_skipped,
          new Object[] { tableId }, 100.0, false);
      return;
    }

    if (tableId.equals(FormsColumns.COMMON_BASE_FORM_ID)) {
      // do not sync the framework table
      tableLevelResult.setSyncOutcome(SyncOutcome.SUCCESS);
      sc.updateNotification(SyncProgressState.ROWS,
          R.string.sync_table_data_sync_complete,
          new Object[] { tableId }, 100.0, false);
      return;
    }

    boolean outstandingAttachmentsToSync = false;
    boolean containsConflicts = false;

    try {
      log.i(TAG, "REST " + tableId);

      SyncOutcome earlierFailure = SyncOutcome.WORKING;

      int passNumber = 1;
      while (passNumber <= 2) {
        // reset the table status to working...

        if ( tableLevelResult.getSyncOutcome() != SyncOutcome.WORKING ) {
          earlierFailure = tableLevelResult.getSyncOutcome();
          tableLevelResult.resetSyncOutcome();
        }
        tableLevelResult.setMessage((passNumber==1) ? "beginning row data sync" : "retrying row data sync");

        ++passNumber;

        sc.updateNotification(SyncProgressState.ROWS,
            R.string.sync_verifying_table_schema_on_server,
            new Object[] { tableId }, 0.0, false);

        // test that the schemaETag matches
        // if it doesn't, the user MUST sync app-level files and
        // configuration
        // syncing at the app level will adjust/set the local table
        // properties
        // schemaETag to match that on the server.
        String schemaETag = te.getSchemaETag();
        if (schemaETag == null || !tableResource.getSchemaETag().equals(schemaETag)) {
          // schemaETag is not identical
          tableLevelResult.setServerHadSchemaChanges(true);
          tableLevelResult
              .setMessage("Server schemaETag differs! Sync app-level files and configuration in order to sync this table.");
          tableLevelResult.setSyncOutcome(SyncOutcome.TABLE_REQUIRES_APP_LEVEL_SYNC);
          return;
        }

        int rowsToSyncCount = 0;
        boolean updateToServerSuccessful = false;
        for (; !updateToServerSuccessful;) {

          updateToServerSuccessful = false;

          if ( tableLevelResult.getSyncOutcome() != SyncOutcome.WORKING ) {
            earlierFailure = tableLevelResult.getSyncOutcome();
            tableLevelResult.resetSyncOutcome();
          }

          // always start with an empty synced-pending-files list.
          rowsToSyncCount = 0;

          try {
            // //////////////////////////////////////////////////
            // //////////////////////////////////////////////////
            // get all the rows in the data table -- we will iterate through
            // them all.
            UserTable localDataTable;
            {
              DbHandle db = null;
              try {
                db = sc.getDatabase();
                String[] empty = {};

                localDataTable = sc.getDatabaseService()
                    .privilegedSimpleQuery(sc.getAppName(), db, tableId, orderedColumns, null,
                        empty, empty,
                        null, new String[] {DataTableColumns.ID}, new String[]{"ASC"}, null, null);
              } finally {
                sc.releaseDatabase(db);
                db = null;
              }
            }

            containsConflicts = localDataTable.hasConflictRows();

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

            // //////////////////////////////////////////////////
            // //////////////////////////////////////////////////
            // Pull changes from the server...

            tableLevelResult.setPulledServerData(false);

            sc.updateNotification(SyncProgressState.ROWS,
                R.string.sync_getting_changed_rows_on_server,
                new Object[] { tableId }, 5.0, false);

            boolean pullCompletedSuccessfully = false;
            String firstDataETag = null;
            String websafeResumeCursor = null;

            // may set tableResult syncOutcome
            for (;;) {
              RowResourceList rows = null;

              try {
                rows = sc.getSynchronizer().getUpdates(tableResource, te.getLastDataETag(),
                    websafeResumeCursor);
                if (firstDataETag == null) {
                  firstDataETag = rows.getDataETag();
                }
              } catch (Exception e) {
                exception("synchronizeTable -  pulling data down from server", tableId, e,
                    tableLevelResult);
                break;
              }

              localDataTable = updateLocalRowsFromServerChanges(tableResource, te, orderedColumns,
                  displayName, attachmentState, fileAttachmentColumns, localDataTable, rows);

              containsConflicts = localDataTable.hasConflictRows();

              if (rows.isHasMoreResults()) {
                websafeResumeCursor = rows.getWebSafeResumeCursor();
              } else {
                // ////////////////////////////////
                // ////////////////////////////////
                // Success
                //
                // We have to update our dataETag here so that the server
                // knows we saw its changes. Otherwise it won't let us
                // put up new information.
                //
                // Note that we may have additional changes from
                // subsequent dataETags (changeSets). We only
                // break out of this loop if the dataETag on the
                // last request matches the firstDataETag. Otherwise,
                // we re-issue a fetch using the firstDataETag as
                // a starting point.
                {
                  DbHandle db = null;
                  try {
                    db = sc.getDatabase();
                    // update the dataETag to the one returned by the first
                    // of the fetch queries, above.
                    sc.getDatabaseService().privilegedUpdateDBTableETags(sc.getAppName(), db,
                        tableId,
                        tableResource.getSchemaETag(), firstDataETag);
                    // the above will throw a ServicesAvailabilityException if the change is not committed
                    // and be sure to update our in-memory objects...
                    te.setSchemaETag(tableResource.getSchemaETag());
                    te.setLastDataETag(firstDataETag);
                    tableResource.setDataETag(firstDataETag);
                  } finally {
                    sc.releaseDatabase(db);
                    db = null;
                  }
                }

                if ( (firstDataETag == rows.getDataETag()) ||
                    firstDataETag.equals(rows.getDataETag() ) ) {
                  // success -- exit the update loop...
                  pullCompletedSuccessfully = true;
                  break;
                } else {
                  // re-issue request...
                  websafeResumeCursor = null;
                }
              }
            }

            // If we made it here and there was data, then we successfully
            // updated the localDataTable from the server.
            tableLevelResult.setPulledServerData(pullCompletedSuccessfully);

            if (!pullCompletedSuccessfully) {
              // generally, the tableResult will have its syncOutcome set in this case.
              break;
            }

            // ////////////////////////////////
            // ////////////////////////////////
            // OK. We can now scan through the localDataTable for changes that
            // should be sent up to the server.

            sc.updateNotification(SyncProgressState.ROWS, R.string.sync_anaylzing_local_row_changes,
                new Object[] { tableId }, 70.0, false);

            /**************************
             * PART 2: UPDATE THE DATA
             **************************/
            // these are all the various actions we will need to take:

            // localRow SyncState.new_row no changes pulled from server
            // localRow SyncState.changed no changes pulled from server
            // localRow SyncState.deleted no changes pulled from server
            List<SyncRow> allAlteredRows = new ArrayList<SyncRow>();

            // loop through the localRow table
            for (int i = 0; i < localDataTable.getNumberOfRows(); i++) {
              Row localRow = localDataTable.getRowAtIndex(i);
              String stateStr = localRow
                  .getDataByKey(DataTableColumns.SYNC_STATE);
              SyncState state = (stateStr == null) ? null : SyncState.valueOf(stateStr);

              String rowId = localDataTable.getRowId(i);

              // the local row wasn't impacted by a server change
              // see if this local row should be pushed to the server.
              if (state == SyncState.new_row || state == SyncState.changed
                  || state == SyncState.deleted) {
                SyncRow sRow = SyncRow.convertToSyncRow(orderedColumns, fileAttachmentColumns, localRow);
                if (state == SyncState.deleted) {
                  sRow.setDeleted(true);
                }
                allAlteredRows.add(sRow);
              } else {
                rowsToSyncCount += countRowToSyncFileAttachments(fileAttachmentColumns, state);
              }
            }

            // We know the changes for the server. Determine the per-row
            // percentage for applying all these changes
            // Note: We are calculating percentages based on uploading the full row, but we may
            // only need to update a small portion of that row's files.
            // TODO: Improve size calculations

            int totalChange = allAlteredRows.size() + rowsToSyncCount;

            perRowIncrement = 90.0 / ((double) (totalChange + 1));
            rowsProcessed = 0;
            boolean hasAttachments = !fileAttachmentColumns.isEmpty();

            // i.e., we have created entries in the various action lists
            // for all the actions we should take.

            // /////////////////////////////////////
            // SERVER CHANGES
            // SERVER CHANGES
            // SERVER CHANGES
            // SERVER CHANGES
            // SERVER CHANGES
            // SERVER CHANGES

            if (allAlteredRows.size() != 0) {
              tableLevelResult.setHadLocalDataChanges(true);
            }

            // idempotent interface means that the interactions
            // for inserts, updates and deletes are identical.
            int count = 0;

            ArrayList<RowOutcome> specialCases = new ArrayList<RowOutcome>();

            if (!allAlteredRows.isEmpty()) {
              int offset = 0;
              while (offset < allAlteredRows.size()) {
                // alter UPSERT_BATCH_SIZE rows at a time to the server
                int max = offset + UPSERT_BATCH_SIZE;
                if (max > allAlteredRows.size()) {
                  max = allAlteredRows.size();
                }
                List<SyncRow> segmentAlter = allAlteredRows.subList(offset, max);

                // TODO: not yet handled dataETag change will report SC_CONFLICT outer retry
                // TODO: ...is an attempt to handle this (inadequate).
                RowOutcomeList outcomes = sc.getSynchronizer().alterRows(tableResource,
                    segmentAlter);

                if (outcomes.getRows().size() != segmentAlter.size()) {
                  throw new IllegalStateException("Unexpected partial return?");
                }

                // process outcomes...
                RowOutcomeSummary outcomeSummary = processRowOutcomes(te, tableResource,
                    tableLevelResult, orderedColumns,
                    fileAttachmentColumns, hasAttachments, count,
                    allAlteredRows.size(), segmentAlter, outcomes.getRows(), specialCases);

                count = outcomeSummary.countSoFar;
                containsConflicts = containsConflicts || outcomeSummary.hasNewConflicts;

                // NOTE: specialCases should probably be deleted?
                // This is the case if the user doesn't have permissions...
                // TODO: figure out whether these are benign or need reporting....
                if (!specialCases.isEmpty()) {
                  throw new IllegalStateException(
                      "update request rejected by the server -- do you have table synchronize privileges?");
                }

                // update our dataETag. Because the server will have failed with
                // a CONFLICT (409) if our dataETag did not match ours at the
                // time the update occurs, we are assured that there are no
                // interleaved changes we are unaware of.
                {
                  DbHandle db = null;
                  try {
                    db = sc.getDatabase();
                    // update the dataETag to the one returned by the first
                    // of the fetch queries, above.
                    sc.getDatabaseService().privilegedUpdateDBTableETags(sc.getAppName(), db,
                        tableId,
                        tableResource.getSchemaETag(), outcomes.getDataETag());
                    // the above will throw a ServicesAvailabilityException if the changed were not committed.
                    // and be sure to update our in-memory objects...
                    te.setSchemaETag(tableResource.getSchemaETag());
                    te.setLastDataETag(outcomes.getDataETag());
                    tableResource.setDataETag(outcomes.getDataETag());
                  } finally {
                    sc.releaseDatabase(db);
                    db = null;
                  }
                }

                // process next segment...
                offset = max;
              }
            }

            // And now update that we've pushed our changes to the server.
            tableLevelResult.setPushedLocalData(true);

            // OK. Now we have pushed everything.
            // because of the 409 (CONFLICT) alterRows enforcement on the
            // server, we know that our data records are consistent and
            // our processing is complete.
            updateToServerSuccessful = true;
          } catch (Exception e) {
            exception("synchronizeTable - pushing data up to server", tableId, e, tableLevelResult);
            break;
          }
        }
        // done with rowData sync. Either we were successful, or
        // there was an error. If there was an error, we will
        // try once more in the outer loop.

        rowDataSyncSuccessful = updateToServerSuccessful;

        // Our update may not have been successful. Only push files if it was...
        if (rowDataSyncSuccessful && (tableLevelResult.getSyncOutcome() == SyncOutcome.WORKING)) {
          ////////////////////////////////////////////////////////////////////////////////////////
          //
          // now compute the set of rows that require file attachments to be sync'd.
          // It is easiest to just re-fetch the localRowTable so...
          // get all the rows in the data table -- we will iterate through
          // them all.

          // file attachments we should sync with the server...
          List<SyncRowPending> rowsToSyncFileAttachments = new ArrayList<SyncRowPending>();
          {
            // place localDataTable in this scope so it can be garbage collected...
            UserTable localDataTable = null;
            {
              DbHandle db = null;
              try {
                db = sc.getDatabase();
                String[] empty = {};
                String[] syncStates = {SyncState.in_conflict.name(), SyncState.synced_pending_files.name()};

                localDataTable = sc.getDatabaseService()
                    .privilegedSimpleQuery(sc.getAppName(), db, tableId, orderedColumns,
                        DataTableColumns.SYNC_STATE + " IN (?,?)", syncStates, empty,
                        null, new String[] {DataTableColumns.ID}, new String[] {"ASC"}, null, null);
              } finally {
                sc.releaseDatabase(db);
                db = null;
              }
            }

            // loop through the localRow table
            for (int i = 0; i < localDataTable.getNumberOfRows(); i++) {
              Row localRow = localDataTable.getRowAtIndex(i);
              String stateStr = localRow
                  .getDataByKey(DataTableColumns.SYNC_STATE);
              SyncState state = (stateStr == null) ? null : SyncState.valueOf(stateStr);

              // the local row wasn't impacted by a server change
              // see if this local row should be pushed to the server.
              perhapsAddToRowsToSyncFileAttachments(rowsToSyncFileAttachments, orderedColumns,
                  fileAttachmentColumns, localRow, state);
            }
          }

          attachmentSyncSuccessful = (rowsToSyncFileAttachments.isEmpty());
          // And try to push the file attachments...
          int count = 0;
          boolean attachmentSyncFailed = false;
          SyncOutcome tableLevelSyncOutcome = SyncOutcome.WORKING;
          try {
            for (SyncRowPending syncRowPending : rowsToSyncFileAttachments) {
              try {
                boolean outcome = true;

                SyncAttachmentState filteredAttachmentState = (syncRowPending.onlyGetFiles() ?
                    SyncAttachmentState.DOWNLOAD : attachmentState);

                log.i(TAG, "synchronizeDataRowsAndAttachments beginning processing for " + syncRowPending.getRowId());

                outcome = manifestProcessor.syncRowLevelFileAttachments(
                    tableResource.getInstanceFilesUri(), tableId, syncRowPending, filteredAttachmentState);

                if (outcome) {
                  if (syncRowPending.updateSyncState()) {
                    // OK -- we succeeded in putting/getting all attachments
                    // update our state to the synced state.
                    DbHandle db = null;
                    try {
                      db = sc.getDatabase();
                      sc.getDatabaseService().privilegedUpdateRowETagAndSyncState(sc.getAppName
                              (), db, tableId,
                          syncRowPending.getRowId(), syncRowPending.getRowETag(), SyncState.synced.name());
                    } finally {
                      sc.releaseDatabase(db);
                      db = null;
                    }
                  }
                } else {
                  outstandingAttachmentsToSync = true;
                }
              } catch (Throwable e) {
                log.printStackTrace(e);
                tableLevelSyncOutcome = sc.exceptionEquivalentOutcome(e);
                attachmentSyncFailed = true;
                log.e(TAG, "[synchronizeTableRest] error synchronizing attachments " + e.toString());
              }
              tableLevelResult.incLocalAttachmentRetries();

              log.i(TAG, "synchronizeDataRowsAndAttachments completed processing for " + syncRowPending.getRowId());

              ++count;
              ++rowsProcessed;
              int idString;
              switch (attachmentState) {
              default:
              case NONE:
                idString = R.string.sync_skipping_attachments_server_row;
                break;
              case SYNC:
                idString = R.string.sync_syncing_attachments_server_row;
                break;
              case UPLOAD:
                idString = R.string.sync_uploading_attachments_server_row;
                break;
              case DOWNLOAD:
                idString = R.string.sync_downloading_attachments_server_row;
                break;
              }
              sc.updateNotification(SyncProgressState.ROWS, idString, new Object[]{tableId, count,
                      rowsToSyncFileAttachments.size()}, 10.0 + rowsProcessed * perRowIncrement,
                  false);
            }
          } catch ( Throwable e) {
            log.printStackTrace(e);
            tableLevelSyncOutcome = sc.exceptionEquivalentOutcome(e);
          } finally {
            attachmentSyncSuccessful = !attachmentSyncFailed;
            if ( tableLevelSyncOutcome != SyncOutcome.WORKING ) {
              tableLevelResult.setSyncOutcome(tableLevelSyncOutcome);
              tableLevelResult.setMessage("exception while syncing row-level attachments");
            }
          }
        }

        log.i(TAG, "synchronizeDataRowsAndAttachments completed processing for all attachments");

        if ( rowDataSyncSuccessful && attachmentSyncSuccessful ) {
          // no need to retry...
          break;
        }
      }

      if (rowDataSyncSuccessful) {
        // if the row data was sync'd
        // update the last-sync-time
        // NOTE: disregard whether
        // attachments were successfully
        // sync'd.
        DbHandle db = null;
        try {
          db = sc.getDatabase();
          sc.getDatabaseService().privilegedUpdateDBTableLastSyncTime(sc.getAppName(), db,
              tableId);
        } finally {
          sc.releaseDatabase(db);
          db = null;
        }
      }
    } finally {
      // Here we also want to add the TableLevelResult to the value.
      if (rowDataSyncSuccessful) {
        // Then we should have updated the db and shouldn't have set the
        // TableLevelResult to be exception.
        if (attachmentSyncSuccessful ) {
          if (tableLevelResult.getSyncOutcome() != SyncOutcome.WORKING) {
            log.e(TAG, "tableLevelResult status for table: " + tableId + " was "
                + tableLevelResult.getSyncOutcome().name()
                + ", and yet row and attachment sync was successful. This shouldn't be possible.");
          } else {
            if (containsConflicts) {
              tableLevelResult.setSyncOutcome(SyncOutcome.TABLE_CONTAINS_CONFLICTS);
              sc.updateNotification(SyncProgressState.ROWS,
                  R.string.sync_table_data_sync_with_conflicts,
                  new Object[]{tableId}, 100.0, false);
            } else if (outstandingAttachmentsToSync || !attachmentSyncSuccessful) {
              tableLevelResult.setSyncOutcome(SyncOutcome.TABLE_PENDING_ATTACHMENTS);
              sc.updateNotification(SyncProgressState.ROWS,
                  R.string.sync_table_data_sync_pending_attachments, new Object[]{tableId}, 100.0,
                  false);
            } else {
              tableLevelResult.setSyncOutcome(SyncOutcome.SUCCESS);
              sc.updateNotification(SyncProgressState.ROWS, R.string.sync_table_data_sync_complete,
                  new Object[]{tableId}, 100.0, false);
            }
          }
        } else {
          if (tableLevelResult.getSyncOutcome() == SyncOutcome.WORKING) {
            log.e(TAG, "tableLevelResult status for table: " + tableId + " was "
                + tableLevelResult.getSyncOutcome().name()
                + ", and yet attachmentSync was not successful. This shouldn't be possible.");
            tableLevelResult.setSyncOutcome(SyncOutcome.FAILURE);
          }
        }
      }
    }
  }

  private static class RowOutcomeSummary {
    final int countSoFar;
    final boolean hasNewConflicts;

    RowOutcomeSummary(int countSoFar, boolean hasNewConflicts) {
      this.countSoFar = countSoFar;
      this.hasNewConflicts = hasNewConflicts;
    }
  }

  /**
   * We pushed changes up to the server and now need to update the local rowETags to match
   * the rowETags assigned to those changes by the server.
   *
   * Does not update tableResult's syncOutcome
   *
   * @param te            local table entry (dataETag is suspect)
   * @param resource        server schemaETag, dataETag etc. before(at) changes were pushed
   * @param tableLevelResult          for progress UI
   * @param orderedColumns            all user-defined columns in the table
   * @param fileAttachmentColumns     columns in the table that hold rowPath values
   * @param hasAttachments            boolean indicating whether table has any rowPath columns
   * @param countSoFar           for progress UI
   * @param totalOutcomesSize    for progress UI
   * @param segmentAlter  changes that were pushed to server
   * @param outcomes      result of pushing those changes to server
   * @param specialCases
   * @return RowOutcomeSummary of new count and whether or not conflicts were introduced.
   * @throws ServicesAvailabilityException
   */
  private RowOutcomeSummary processRowOutcomes(TableDefinitionEntry te, TableResource resource,
      TableLevelResult tableLevelResult, OrderedColumns orderedColumns,
      ArrayList<ColumnDefinition> fileAttachmentColumns, boolean hasAttachments,
      int countSoFar, int totalOutcomesSize,
      List<SyncRow> segmentAlter, ArrayList<RowOutcome> outcomes, ArrayList<RowOutcome> specialCases) throws ServicesAvailabilityException {

    ArrayList<SyncRowDataChanges> rowsToMoveToInConflictLocally = new ArrayList<SyncRowDataChanges>();

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

      boolean badState = false;
      for (int i = 0; i < segmentAlter.size(); ++i) {
        RowOutcome r = outcomes.get(i);
        SyncRow syncRow = segmentAlter.get(i);
        if (!r.getRowId().equals(syncRow.getRowId())) {
          throw new ClientDetectedVersionMismatchedServerResponseException("Unexpected reordering of return");
        }
        if (r.getOutcome() == OutcomeType.SUCCESS) {

          if (r.isDeleted()) {
            // DELETE
            sc.getDatabaseService().privilegedDeleteRowWithId(sc.getAppName(), db,
                resource.getTableId(), orderedColumns, r.getRowId());
            // !!Important!! update the rowETag in our copy of this row.
            syncRow.setRowETag(r.getRowETag());
            tableLevelResult.incServerDeletes();
          } else {
            SyncState newSyncState = (hasAttachments && !syncRow.getUriFragments().isEmpty())
                ? SyncState.synced_pending_files : SyncState.synced;
            sc.getDatabaseService().privilegedUpdateRowETagAndSyncState(
                sc.getAppName(), db,
                resource.getTableId(),
                r.getRowId(),
                r.getRowETag(),
                newSyncState.name());
            // !!Important!! update the rowETag in our copy of this row.
            syncRow.setRowETag(r.getRowETag());
            // UPDATE or INSERT
            tableLevelResult.incServerUpserts();
          }
        } else if (r.getOutcome() == OutcomeType.FAILED) {
          if (r.getRowId() == null || !r.isDeleted()) {
            // should never occur!!!
            badState = true;
          } else {
            // special case of a delete where server has no record of the row.
            // server should add row and mark it as deleted.
          }
        } else if (r.getOutcome() == OutcomeType.IN_CONFLICT) {
          // another device updated this record between the time we fetched
          // changes
          // and the time we tried to update this record. Transition the record
          // locally into the conflicting state.
          // SyncState.deleted and server is not deleting
          // SyncState.new_row and record exists on server
          // SyncState.changed and new change on server
          // SyncState.in_conflict and new change on server

          // no need to worry about server in_conflict records.
          // any server in_conflict rows will be cleaned up during the
          // update of the in_conflict state.
          Integer localRowConflictType = syncRow.isDeleted() ? ConflictType.LOCAL_DELETED_OLD_VALUES
              : ConflictType.LOCAL_UPDATED_UPDATED_VALUES;

          Integer serverRowConflictType = r.isDeleted() ? ConflictType.SERVER_DELETED_OLD_VALUES
              : ConflictType.SERVER_UPDATED_UPDATED_VALUES;

          // figure out what the localRow conflict type sh
          SyncRow serverRow = new SyncRow(r.getRowId(), r.getRowETag(), r.isDeleted(),
              r.getFormId(), r.getLocale(), r.getSavepointType(), r.getSavepointTimestamp(),
              r.getSavepointCreator(), r.getRowFilterScope(), r.getValues(), fileAttachmentColumns);
          SyncRowDataChanges conflictRow = new SyncRowDataChanges(serverRow, syncRow, false,
              localRowConflictType);

          rowsToMoveToInConflictLocally.add(conflictRow);
          // we transition all of these later, outside this processing loop...
        } else if (r.getOutcome() == OutcomeType.DENIED) {
          // user does not have privileges...
          specialCases.add(r);
        } else {
          // a new OutcomeType state was added!
          throw new IllegalStateException("Unexpected OutcomeType! " + r.getOutcome().name());
        }

        ++countSoFar;
        ++rowsProcessed;
        sc.updateNotification(SyncProgressState.ROWS, R.string.sync_altering_server_row,
            new Object[] {
                resource.getTableId(), countSoFar, totalOutcomesSize }, 10.0 + rowsProcessed
                * perRowIncrement, false);
      }

      if (badState) {
        // TODO: we could update all the other row state then throw this...
        throw new IllegalStateException(
            "Unexpected null rowId or OutcomeType.FAILED when not deleting row");
      }

      // process the conflict rows, if any
      conflictRowsInDb(db, resource, orderedColumns, rowsToMoveToInConflictLocally,
          tableLevelResult);
    } finally {
      if (db != null) {
        sc.releaseDatabase(db);
        db = null;
      }
    }

    return new RowOutcomeSummary(countSoFar, !rowsToMoveToInConflictLocally.isEmpty());
  }

  /**
   * Delete any pre-existing server conflict records for the list of rows
   * (changes). If the server and local rows are both deletes, delete the local
   * row (and its attachments), thereby completing the deletion of the row
   * (entirely). Otherwise, change the local row to the in_conflict state, and
   * insert a copy of the server row locally, configured as a server conflict
   * record.
   *
   * @param db
   * @param resource
   * @param orderedColumns
   * @param changes
   * @param tableLevelResult
   * @throws ServicesAvailabilityException
   */
  private void conflictRowsInDb(DbHandle db, TableResource resource,
      OrderedColumns orderedColumns, List<SyncRowDataChanges> changes,
      TableLevelResult tableLevelResult) throws ServicesAvailabilityException {

    int count = 0;
    for (SyncRowDataChanges change : changes) {
      SyncRow serverRow = change.serverRow;
      log.i(TAG,
          "conflicting row, id=" + serverRow.getRowId() + " rowETag=" + serverRow.getRowETag());

      // update existing localRow

      // the localRow conflict type was determined when the
      // change was added to the changes list.
      Integer localRowConflictType = change.localRowConflictType;

      // Determine the type of change that occurred on the server.
      int serverRowConflictType;
      if (serverRow.isDeleted()) {
        serverRowConflictType = ConflictType.SERVER_DELETED_OLD_VALUES;
      } else {
        serverRowConflictType = ConflictType.SERVER_UPDATED_UPDATED_VALUES;
      }

      if (serverRowConflictType == ConflictType.SERVER_DELETED_OLD_VALUES
          && localRowConflictType == ConflictType.LOCAL_DELETED_OLD_VALUES) {

        // special case -- the server and local rows are both being deleted --
        // just delete them!

        // this is the same action as during conflict-resolution when we choose
        // to accept the server delete status over any local changes.
        sc.getDatabaseService().privilegedDeleteRowWithId(
            sc.getAppName(), db, resource.getTableId(), orderedColumns, serverRow.getRowId());

        tableLevelResult.incLocalDeletes();
      } else {
        ContentValues values = new ContentValues();

        // set up to insert the in_conflict row from the server
        for (DataKeyValue entry : serverRow.getValues()) {
          String colName = entry.column;
          values.put(colName, entry.value);
        }

        // insert in_conflict server row
        values.put(DataTableColumns.ROW_ETAG, serverRow.getRowETag());
        values.put(DataTableColumns.SYNC_STATE, SyncState.in_conflict.name());
        values.put(DataTableColumns.CONFLICT_TYPE, serverRowConflictType);
        values.put(DataTableColumns.FORM_ID, serverRow.getFormId());
        values.put(DataTableColumns.LOCALE, serverRow.getLocale());
        values.put(DataTableColumns.SAVEPOINT_TIMESTAMP, serverRow.getSavepointTimestamp());
        values.put(DataTableColumns.SAVEPOINT_CREATOR, serverRow.getSavepointCreator());
        values.put(DataTableColumns.SAVEPOINT_TYPE, serverRow.getSavepointType());
        RowFilterScope.Type type = serverRow.getRowFilterScope().getType();
        values.put(DataTableColumns.FILTER_TYPE,
            (type == null) ? RowFilterScope.Type.DEFAULT.name() : type.name());
        values.put(DataTableColumns.FILTER_VALUE, serverRow.getRowFilterScope().getValue());

        sc.getDatabaseService().privilegedPlaceRowIntoConflictWithId(sc.getAppName(), db,
            resource.getTableId(), orderedColumns, values, serverRow.getRowId(),
            localRowConflictType);

        // We're going to check our representation invariant here. A local and
        // a server version of the row should only ever be changed/changed,
        // deleted/changed, or changed/deleted. Anything else and we're in
        // trouble.
        if (localRowConflictType == ConflictType.LOCAL_DELETED_OLD_VALUES
            && serverRowConflictType != ConflictType.SERVER_UPDATED_UPDATED_VALUES) {
          log.e(TAG, "local row conflict type is local_deleted, but server "
              + "row conflict_type is not server_udpated. These states must"
              + " go together, something went wrong.");
        } else if (localRowConflictType != ConflictType.LOCAL_UPDATED_UPDATED_VALUES) {
          log.e(TAG, "localRowConflictType was not local_deleted or "
              + "local_updated! this is an error. local conflict type: " + localRowConflictType
              + ", server conflict type: " + serverRowConflictType);
        }

        tableLevelResult.incLocalConflicts();
      }
      ++count;
      ++rowsProcessed;
      sc.updateNotification(SyncProgressState.ROWS, R.string.sync_marking_conflicting_local_row,
          new Object[] { resource.getTableId(), count, changes.size() }, 10.0 + rowsProcessed
              * perRowIncrement, false);
    }
  }

  /**
   * Inserts the given list of rows (changes) into the local database. Adds
   * those rows to the rowsToPushFileAttachments list if they have any non-null
   * media attachments.
   *
   * @param db
   * @param resource
   * @param orderedColumns
   * @param changes
   * @param hasAttachments
   * @param tableLevelResult
   * @throws ServicesAvailabilityException
   */
  private void insertRowsInDb(DbHandle db, TableResource resource,
      OrderedColumns orderedColumns, List<SyncRowDataChanges> changes,
      boolean hasAttachments, TableLevelResult tableLevelResult) throws ServicesAvailabilityException {
    int count = 0;
    for (SyncRowDataChanges change : changes) {
      SyncRow serverRow = change.serverRow;
      ContentValues values = new ContentValues();

      // set all the metadata fields
      values.put(DataTableColumns.ID, serverRow.getRowId());
      values.put(DataTableColumns.ROW_ETAG, serverRow.getRowETag());
      values.put(DataTableColumns.SYNC_STATE, (hasAttachments && !serverRow.getUriFragments()
          .isEmpty()) ? SyncState.synced_pending_files.name() : SyncState.synced.name());
      values.put(DataTableColumns.FORM_ID, serverRow.getFormId());
      values.put(DataTableColumns.LOCALE, serverRow.getLocale());
      values.put(DataTableColumns.SAVEPOINT_TIMESTAMP, serverRow.getSavepointTimestamp());
      values.put(DataTableColumns.SAVEPOINT_CREATOR, serverRow.getSavepointCreator());
      values.put(DataTableColumns.SAVEPOINT_TYPE, serverRow.getSavepointType());
      values.put(DataTableColumns.FILTER_TYPE, serverRow.getRowFilterScope().getType().name());
      values.put(DataTableColumns.FILTER_VALUE, serverRow.getRowFilterScope().getValue());
      values.putNull(DataTableColumns.CONFLICT_TYPE);

      for (DataKeyValue entry : serverRow.getValues()) {
        String colName = entry.column;
        values.put(colName, entry.value);
      }

      sc.getDatabaseService().privilegedInsertRowWithId(sc.getAppName(), db,
          resource.getTableId(),
          orderedColumns, values, serverRow.getRowId(), false);
      tableLevelResult.incLocalInserts();

      ++count;
      ++rowsProcessed;
      sc.updateNotification(SyncProgressState.ROWS, R.string.sync_inserting_local_row, new
              Object[] {
              resource.getTableId(), count, changes.size() }, 10.0 + rowsProcessed * perRowIncrement,
          false);
    }
  }

  /**
   * Updates the given list of rows (changes) in the local database. Adds those
   * rows to the rowsToPushFileAttachments list if they have any non-null media
   * attachments.
   *
   * @param db
   * @param resource
   * @param orderedColumns
   * @param changes
   * @param hasAttachments
   * @param tableLevelResult
   * @throws ServicesAvailabilityException
   */
  private void updateRowsInDb(DbHandle db, TableResource resource,
      OrderedColumns orderedColumns, List<SyncRowDataChanges> changes,
      boolean hasAttachments,
      TableLevelResult tableLevelResult) throws ServicesAvailabilityException {
    int count = 0;
    for (SyncRowDataChanges change : changes) {
      // if the localRow sync state was synced_pending_files,
      // ensure that all those files are uploaded before
      // we update the row. This ensures that all attachments
      // are saved before we revise the local row value.
      if (change.isSyncedPendingFiles) {
        log.w(TAG,
            "file attachment at risk -- updating from server while in synced_pending_files state. rowId: "
                + change.localRow.getRowId() + " rowETag: " + change.localRow.getRowETag());
      }

      // update the row from the changes on the server
      SyncRow serverRow = change.serverRow;
      ContentValues values = new ContentValues();

      values.put(DataTableColumns.ROW_ETAG, serverRow.getRowETag());
      values.put(DataTableColumns.SYNC_STATE, (hasAttachments && !serverRow.getUriFragments()
          .isEmpty()) ? SyncState.synced_pending_files.name() : SyncState.synced.name());
      values.put(DataTableColumns.FORM_ID, serverRow.getFormId());
      values.put(DataTableColumns.LOCALE, serverRow.getLocale());
      values.put(DataTableColumns.SAVEPOINT_TYPE, serverRow.getSavepointType());
      values.put(DataTableColumns.SAVEPOINT_TIMESTAMP, serverRow.getSavepointTimestamp());
      values.put(DataTableColumns.SAVEPOINT_CREATOR, serverRow.getSavepointCreator());
      values.put(DataTableColumns.FILTER_TYPE, serverRow.getRowFilterScope().getType().name());
      values.put(DataTableColumns.FILTER_VALUE, serverRow.getRowFilterScope().getValue());
      values.putNull(DataTableColumns.CONFLICT_TYPE);

      for (DataKeyValue entry : serverRow.getValues()) {
        String colName = entry.column;
        values.put(colName, entry.value);
      }

      sc.getDatabaseService().privilegedUpdateRowWithId(sc.getAppName(), db,
          resource.getTableId(),
          orderedColumns, values, serverRow.getRowId(), false);
      tableLevelResult.incLocalUpdates();

      ++count;
      ++rowsProcessed;
      sc.updateNotification(SyncProgressState.ROWS, R.string.sync_updating_local_row, new Object[] {
              resource.getTableId(), count, changes.size() }, 10.0 + rowsProcessed * perRowIncrement,
          false);
    }
  }

  /**
   * Attempt to push all the attachments of the local rows up to the server
   * (before the row is locally deleted). If the attachments were pushed to the
   * server, the 'isSyncedPendingFiles' flag is cleared. This makes the local row
   * eligible for deletion. Otherwise, the localRow is removed from the
   * localRowplaced in the
   *
   * @param db
   * @param resource
   * @param changes
   * @throws HttpClientWebException
   * @throws IOException
   * @throws ServicesAvailabilityException
   */
  private void pushLocalAttachmentsBeforeDeleteRowsInDb(DbHandle db, TableResource resource,
      List<SyncRowDataChanges> changes) throws
      HttpClientWebException, IOException, ServicesAvailabilityException {

    // try first to push any attachments of the soon-to-be-deleted
    // local row up to the server
    for (int i = 0; i < changes.size();) {
      SyncRowDataChanges change = changes.get(i);
      if (change.isSyncedPendingFiles) {
        if (change.localRow.getUriFragments().isEmpty()) {
          // nothing to push
          change.isSyncedPendingFiles = false;
          ++i;
        } else {
          // since we are directly calling putFileAttachments, the flags in this
          // constructor are never accessed. Use false for their values.
          SyncRowPending srp = new SyncRowPending(change.localRow, false, false, false);
          boolean outcome = manifestProcessor.syncRowLevelFileAttachments(resource.getInstanceFilesUri(),
              resource.getTableId(), srp, SyncAttachmentState.UPLOAD);
          if (outcome) {
            // successful
            change.isSyncedPendingFiles = false;
            ++i;
          } else {
            // there are files that should be pushed that weren't.
            // change local state to deleted, and remove from the
            // this list. Whenever we next sync files, we will push
            // any local files that are not on the server then delete
            // the local record.
            sc.getDatabaseService().privilegedUpdateRowETagAndSyncState(sc.getAppName(), db,
                resource.getTableId(),
                change.localRow.getRowId(), change.serverRow.getRowETag(), SyncState.deleted.name());
            changes.remove(i);
          }
        }
      } else {
        ++i;
      }
    }
  }

  /**
   * Delete the rows that have had all of their (locally-available) attachments
   * pushed to the server. I.e., those with 'isSyncedPendingFiles' false.
   * Otherwise, leave these rows in the local database until their files are
   * pushed and they can safely be removed.
   *
   * @param db
   * @param resource
   * @param orderedColumns
   * @param changes
   * @param tableLevelResult
   * @throws IOException
   * @throws ServicesAvailabilityException
   */
  private void deleteRowsInDb(DbHandle db, TableResource resource, OrderedColumns orderedColumns,
      List<SyncRowDataChanges> changes, TableLevelResult tableLevelResult) throws IOException,
      ServicesAvailabilityException {
    int count = 0;

    // now delete the rows we can delete...
    for (SyncRowDataChanges change : changes) {
      if (!change.isSyncedPendingFiles) {
        // DELETE
        // this is equivalent to the conflict-resolution action where we accept the server delete
        // this ensures there are no server conflict rows, and that the local row is removed.
        sc.getDatabaseService().privilegedDeleteRowWithId(
            sc.getAppName(), db, resource.getTableId(), orderedColumns, change.serverRow.getRowId());
        tableLevelResult.incLocalDeletes();
      }

      ++count;
      ++rowsProcessed;
      sc.updateNotification(SyncProgressState.ROWS, R.string.sync_deleting_local_row, new Object[] {
              resource.getTableId(), count, changes.size() }, 10.0 + rowsProcessed * perRowIncrement,
          false);
    }
  }
}
