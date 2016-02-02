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
package org.opendatakit.sync.service.logic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpStatus;
import org.apache.wink.client.ClientWebException;
import org.json.JSONObject;
import org.opendatakit.aggregate.odktables.rest.ConflictType;
import org.opendatakit.aggregate.odktables.rest.ElementDataType;
import org.opendatakit.aggregate.odktables.rest.SyncState;
import org.opendatakit.aggregate.odktables.rest.entity.*;
import org.opendatakit.aggregate.odktables.rest.entity.RowOutcome.OutcomeType;
import org.opendatakit.common.android.data.ColumnDefinition;
import org.opendatakit.common.android.data.OrderedColumns;
import org.opendatakit.common.android.data.TableDefinitionEntry;
import org.opendatakit.common.android.data.UserTable;
import org.opendatakit.common.android.data.Row;
import org.opendatakit.common.android.provider.DataTableColumns;
import org.opendatakit.common.android.provider.FormsColumns;
import org.opendatakit.common.android.utilities.WebLogger;
import org.opendatakit.common.android.utilities.WebLoggerIf;
import org.opendatakit.core.R;
import org.opendatakit.database.service.OdkDbHandle;
import org.opendatakit.sync.service.SyncAttachmentState;
import org.opendatakit.sync.service.SyncExecutionContext;
import org.opendatakit.sync.service.data.SyncRow;
import org.opendatakit.sync.service.data.SyncRowDataChanges;
import org.opendatakit.sync.service.data.SyncRowPending;
import org.opendatakit.sync.service.data.SynchronizationResult.Status;
import org.opendatakit.sync.service.data.TableResult;
import org.opendatakit.sync.service.exceptions.InvalidAuthTokenException;
import org.opendatakit.sync.service.SyncProgressState;

import android.content.ContentValues;
import android.os.RemoteException;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.databind.ObjectMapper;

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

  public ProcessRowDataChanges(SyncExecutionContext sharedContext) {
    this.sc = sharedContext;
    this.log = WebLogger.getLogger(sc.getAppName());
  }

  /**
   * Common error reporting...
   * 
   * @param method
   * @param tableId
   * @param e
   * @param tableResult
   */
  private void clientAuthException(String method, String tableId, Exception e,
      TableResult tableResult) {
    String msg = e.getMessage();
    if (msg == null) {
      msg = e.toString();
    }
    log.e(TAG, String.format("ResourceAccessException in %s for table: %s exception: %s", method,
        tableId, msg));
    tableResult.setStatus(Status.AUTH_EXCEPTION);
    tableResult.setMessage(msg);
  }

  /**
   * Common error reporting...
   * 
   * @param method
   * @param tableId
   * @param e
   * @param tableResult
   */
  private void clientWebException(String method, String tableId, ClientWebException e,
      TableResult tableResult) {
    String msg = e.getMessage();
    if (msg == null) {
      msg = e.toString();
    }
    log.e(TAG, String.format("ResourceAccessException in %s for table: %s exception: %s", method,
        tableId, msg));
    tableResult.setStatus(Status.EXCEPTION);
    tableResult.setMessage(msg);
  }

  /**
   * Common error reporting...
   * 
   * @param method
   * @param tableId
   * @param e
   * @param tableResult
   */
  private void exception(String method, String tableId, Exception e, TableResult tableResult) {
    String msg = e.getMessage();
    if (msg == null) {
      msg = e.toString();
    }
    log.e(TAG, String.format("Unexpected exception in %s on table: %s exception: %s", method,
        tableId, msg));
    tableResult.setStatus(Status.EXCEPTION);
    tableResult.setMessage(msg);
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
   * @throws RemoteException 
   */
  public void synchronizeDataRowsAndAttachments(List<TableResource> workingListOfTables,
      SyncAttachmentState attachmentState) throws RemoteException {
    log.i(TAG, "entered synchronize()");

    OdkDbHandle db = null;

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

  private UserTable updateLocalRowsFromServerChanges(TableResource tableResource,
      TableDefinitionEntry te, OrderedColumns orderedColumns, String displayName,
      SyncAttachmentState attachmentState, ArrayList<ColumnDefinition> fileAttachmentColumns,
      List<SyncRowPending> rowsToPushFileAttachments, UserTable localDataTable, RowResourceList rows)
      throws IOException, ClientWebException, RemoteException {

    String tableId = tableResource.getTableId();
    TableResult tableResult = sc.getTableResult(tableId);

    if (rows.getRows().isEmpty()) {
      // nothing here -- let caller determine whether we are done or
      // whether we need to issue another request to the server.
      return localDataTable;
    }

    Map<String, SyncRow> changedServerRows = new HashMap<String, SyncRow>();
    for (RowResource row : rows.getRows()) {
      SyncRow syncRow = new SyncRow(row.getRowId(), row.getRowETag(), row.isDeleted(),
          row.getFormId(), row.getLocale(), row.getSavepointType(), row.getSavepointTimestamp(),
          row.getSavepointCreator(), row.getFilterScope(), row.getValues(), fileAttachmentColumns);
      changedServerRows.put(row.getRowId(), syncRow);
    }

    sc.updateNotification(SyncProgressState.ROWS, R.string.sync_anaylzing_row_changes,
        new Object[] { tableId }, 7.0, false);

    /**************************
     * PART 2: UPDATE THE DATA
     **************************/
    log.d(TAG, "updateDbFromServer setServerHadDataChanges(true)");
    tableResult.setServerHadDataChanges(!changedServerRows.isEmpty());
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
      String stateStr = localRow.getRawDataOrMetadataByElementKey(DataTableColumns.SYNC_STATE);
      SyncState state = stateStr == null ? null : SyncState.valueOf(stateStr);

      String rowId = localRow.getRowId();

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
            .getRawDataOrMetadataByElementKey(DataTableColumns.CONFLICT_TYPE);
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
          rowsToUpdateLocally.add(new SyncRowDataChanges(serverRow, SyncRow.convertToSyncRow(
              orderedColumns, fileAttachmentColumns, localRow),
              (state == SyncState.synced_pending_files)));
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
      OdkDbHandle db = null;
      boolean successful = false;
      try {
        db = sc.getDatabase();

        // this will individually move some files to the locally-deleted state
        // if we cannot sync file attachments in those rows.
        pushLocalAttachmentsBeforeDeleteRowsInDb(db, tableResource, rowsToDeleteLocally,
            fileAttachmentColumns, attachmentState, tableResult);

        deleteRowsInDb(db, tableResource, rowsToDeleteLocally, fileAttachmentColumns,
            attachmentState, tableResult);

        insertRowsInDb(db, tableResource, orderedColumns, rowsToInsertLocally,
            rowsToPushFileAttachments, hasAttachments, tableResult);

        updateRowsInDb(db, tableResource, orderedColumns, rowsToUpdateLocally,
            rowsToPushFileAttachments, hasAttachments, tableResult);

        conflictRowsInDb(db, tableResource, orderedColumns, rowsToMoveToInConflictLocally,
            rowsToPushFileAttachments, hasAttachments, tableResult);

        String[] empty = {};
        localDataTable = sc.getDatabaseService().rawSqlQuery(sc.getAppName(), db,
            tableId,
            orderedColumns, null, empty, empty, null, DataTableColumns.ID, "ASC");

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
   * @throws RemoteException
   */
  private void synchronizeTableDataRowsAndAttachments(TableResource tableResource,
      TableDefinitionEntry te, OrderedColumns orderedColumns, String displayName,
      SyncAttachmentState attachmentState) throws RemoteException {
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

    // Prepare the tableResult. We'll start it as failure, and only update it
    // if we're successful at the end.
    String tableId = te.getTableId();
    TableResult tableResult = sc.getTableResult(tableId);
    tableResult.setTableDisplayName(displayName);
    if (tableResult.getStatus() != Status.WORKING) {
      // there was some sort of error...
      log.e(TAG, "Skipping data sync - error in table schema or file verification step " + tableId);
      return;
    }

    if (tableId.equals(FormsColumns.COMMON_BASE_FORM_ID)) {
      // do not sync the framework table
      tableResult.setStatus(Status.SUCCESS);
      sc.updateNotification(SyncProgressState.ROWS,
          R.string.sync_table_data_sync_complete,
          new Object[] { tableId }, 100.0, false);
      return;
    }

    boolean containsConflicts = false;

    try {
      log.i(TAG, "REST " + tableId);

      int passNumber = 1;
      while (passNumber <= 2) {
        // reset the table status to working...
        tableResult.setStatus(Status.WORKING);
        tableResult.setMessage((passNumber==1) ? "beginning row data sync" : "retrying row data sync");

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
          tableResult.setServerHadSchemaChanges(true);
          tableResult
              .setMessage("Server schemaETag differs! Sync app-level files and configuration in order to sync this table.");
          tableResult.setStatus(Status.TABLE_REQUIRES_APP_LEVEL_SYNC);
          return;
        }

        // file attachments we should sync with the server...
        List<SyncRowPending> rowsToPushFileAttachments = new ArrayList<SyncRowPending>();

        boolean updateToServerSuccessful = false;
        for (; !updateToServerSuccessful;) {

          updateToServerSuccessful = false;

          // always start with an empty synced-pending-files list.
          rowsToPushFileAttachments.clear();

          try {
            // //////////////////////////////////////////////////
            // //////////////////////////////////////////////////
            // get all the rows in the data table -- we will iterate through
            // them all.
            UserTable localDataTable;
            {
              OdkDbHandle db = null;
              try {
                db = sc.getDatabase();
                String[] empty = {};
                localDataTable = sc.getDatabaseService().rawSqlQuery(sc.getAppName(), db, tableId,
                    orderedColumns, null, empty, empty, null, DataTableColumns.ID, "ASC");
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
              tableResult.setMessage(sc.getString(R.string.sync_table_contains_checkpoints));
              tableResult.setStatus(Status.TABLE_CONTAINS_CHECKPOINTS);
              return;
            }

            // //////////////////////////////////////////////////
            // //////////////////////////////////////////////////
            // Pull changes from the server...

            tableResult.setPulledServerData(false);

            sc.updateNotification(SyncProgressState.ROWS,
                R.string.sync_getting_changed_rows_on_server,
                new Object[] { tableId }, 5.0, false);

            boolean pullCompletedSuccessfully = false;
            String firstDataETag = null;
            String websafeResumeCursor = null;
            for (;;) {
              RowResourceList rows = null;

              try {
                rows = sc.getSynchronizer().getUpdates(tableResource, te.getLastDataETag(),
                    websafeResumeCursor);
                if (firstDataETag == null) {
                  firstDataETag = rows.getDataETag();
                }
              } catch (ClientWebException e) {
                if (e.getResponse() != null
                    && e.getResponse().getStatusCode() == HttpStatus.SC_UNAUTHORIZED) {
                  clientAuthException("synchronizeTable - pulling data down from server", tableId,
                      e, tableResult);
                } else {
                  clientWebException("synchronizeTable - pulling data down from server", tableId,
                      e, tableResult);
                }
                break;
              } catch (InvalidAuthTokenException e) {
                clientAuthException("synchronizeTable - pulling data down from server", tableId, e,
                    tableResult);
                break;
              } catch (Exception e) {
                exception("synchronizeTable -  pulling data down from server", tableId, e,
                    tableResult);
                break;
              }

              localDataTable = updateLocalRowsFromServerChanges(tableResource, te, orderedColumns,
                  displayName, attachmentState, fileAttachmentColumns,
                  rowsToPushFileAttachments, localDataTable, rows);

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
                  OdkDbHandle db = null;
                  try {
                    db = sc.getDatabase();
                    // update the dataETag to the one returned by the first
                    // of the fetch queries, above.
                    sc.getDatabaseService().updateDBTableETags(sc.getAppName(), db, 
                        tableId,
                        tableResource.getSchemaETag(), firstDataETag);
                    // the above will throw a RemoteException if the change is not committed
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
            tableResult.setPulledServerData(pullCompletedSuccessfully);

            if (!pullCompletedSuccessfully) {
              break;
            }

            // ////////////////////////////////
            // ////////////////////////////////
            // OK. We can now scan through the localDataTable for changes that
            // should be sent up to the server.

            sc.updateNotification(SyncProgressState.ROWS, R.string.sync_anaylzing_row_changes,
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
                  .getRawDataOrMetadataByElementKey(DataTableColumns.SYNC_STATE);
              SyncState state = (stateStr == null) ? null : SyncState.valueOf(stateStr);

              String rowId = localRow.getRowId();

              // the local row wasn't impacted by a server change
              // see if this local row should be pushed to the server.
              if (state == SyncState.new_row || state == SyncState.changed
                  || state == SyncState.deleted) {
                allAlteredRows.add(SyncRow.convertToSyncRow(orderedColumns, fileAttachmentColumns,
                    localRow));
              } else if (state == SyncState.synced_pending_files) {
                rowsToPushFileAttachments.add(new SyncRowPending(SyncRow.convertToSyncRow(
                    orderedColumns, fileAttachmentColumns, localRow), false, true, true));
              }
            }

            // We know the changes for the server. Determine the per-row
            // percentage for applying all these changes
            // Note: We are calculating percentages based on uploading the full row, but we may
            // only need to update a small portion of that row's files.
            // TODO: Improve size calculations

            int totalChange = allAlteredRows.size() + rowsToPushFileAttachments.size();

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
              tableResult.setHadLocalDataChanges(true);
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
                RowOutcomeList outcomes = sc.getSynchronizer().alterRows(tableResource,
                    segmentAlter);

                if (outcomes.getRows().size() != segmentAlter.size()) {
                  throw new IllegalStateException("Unexpected partial return?");
                }

                // process outcomes...
                count = processRowOutcomes(te, tableResource, tableResult, orderedColumns,
                    fileAttachmentColumns, hasAttachments, rowsToPushFileAttachments, count,
                    allAlteredRows.size(), segmentAlter, outcomes.getRows(), specialCases);

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
                  OdkDbHandle db = null;
                  try {
                    db = sc.getDatabase();
                    // update the dataETag to the one returned by the first
                    // of the fetch queries, above.
                    sc.getDatabaseService().updateDBTableETags(sc.getAppName(), db, 
                        tableId,
                        tableResource.getSchemaETag(), outcomes.getDataETag());
                    // the above will throw a RemoteException if the changed were not committed.
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
            tableResult.setPushedLocalData(true);

            // OK. Now we have pushed everything.
            // because of the 409 (CONFLICT) alterRows enforcement on the
            // server, we know that our data records are consistent and
            // our processing is complete.
            updateToServerSuccessful = true;
          } catch (ClientWebException e) {
            if (e.getResponse().getStatusCode() == HttpStatus.SC_CONFLICT) {
              // expected -- there were row updates by another client
              // re-pull changes from the server. Return to the start
              // of the for(;;) loop.
              continue;
            }
            // otherwise it is an error...
            if (e.getResponse() != null
                && e.getResponse().getStatusCode() == HttpStatus.SC_UNAUTHORIZED) {
              clientAuthException("synchronizeTable - pushing data up to server", tableId, e,
                  tableResult);
            } else {
              clientWebException("synchronizeTable - pushing data up to server", tableId, e,
                  tableResult);
            }
            break;
          } catch (InvalidAuthTokenException e) {
            clientAuthException("synchronizeTable - pushing data up to server", tableId, e, tableResult);
            break;
          } catch (Exception e) {
            exception("synchronizeTable - pushing data up to server", tableId, e, tableResult);
            break;
          }
        }
        // done with rowData sync. Either we were successful, or
        // there was an error. If there was an error, we will
        // try once more in the outer loop.

        rowDataSyncSuccessful = updateToServerSuccessful;

        // Our update may not have been successful. Only push files if it was...
        if (rowDataSyncSuccessful) {
          attachmentSyncSuccessful = (rowsToPushFileAttachments.isEmpty());
          // And try to push the file attachments...
          int count = 0;
          boolean attachmentSyncFailed = false;
          for (SyncRowPending syncRowPending : rowsToPushFileAttachments) {
            try {
              boolean outcome = true;

              SyncAttachmentState filteredAttachmentState = (syncRowPending.onlyGetFiles() ?
                  SyncAttachmentState.DOWNLOAD : attachmentState);

              outcome = sc.getSynchronizer().syncFileAttachments(
                  tableResource.getInstanceFilesUri(), tableId, syncRowPending, filteredAttachmentState);

              if (outcome) {

                if (syncRowPending.updateSyncState()) {
                  if (outcome) {
                    // OK -- we succeeded in putting/getting all attachments
                    // update our state to the synced state.
                    OdkDbHandle db = null;
                    try {
                      db = sc.getDatabase();
                      sc.getDatabaseService().updateRowETagAndSyncState(sc.getAppName(), db, tableId,
                          syncRowPending.getRowId(), syncRowPending.getRowETag(), SyncState.synced.name());
                    } finally {
                      sc.releaseDatabase(db);
                      db = null;
                    }
                  } else {
                    // only care about instance file status if we are trying
                    // to update state
                    attachmentSyncFailed = false;
                  }
                }
              }
            } catch (ClientWebException e) {
              if (e.getResponse() != null
                  && e.getResponse().getStatusCode() == HttpStatus.SC_UNAUTHORIZED) {
                clientAuthException("synchronizeTable - auth error synchronizing attachments", tableId, e, tableResult);
                log.e(TAG, "[synchronizeTableRest] auth failure synchronizing attachments " + e.toString());
              } else {
                clientWebException("synchronizeTableRest", tableId, e, tableResult);
                log.e(TAG, "[synchronizeTableRest] error synchronizing attachments " + e.toString());
              }
            } catch (Exception e) {
              exception("synchronizeTableRest", tableId, e, tableResult);
              log.e(TAG, "[synchronizeTableRest] error synchronizing attachments " + e.toString());
            }
            tableResult.incLocalAttachmentRetries();
            ++count;
            ++rowsProcessed;
            sc.updateNotification(SyncProgressState.ROWS,
                R.string.sync_uploading_attachments_server_row, new Object[] { tableId, count,
                    rowsToPushFileAttachments.size() }, 10.0 + rowsProcessed * perRowIncrement,
                false);
          }
          attachmentSyncSuccessful = !attachmentSyncFailed;
        }
        
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
        OdkDbHandle db = null;
        try {
          db = sc.getDatabase();
          sc.getDatabaseService().updateDBTableLastSyncTime(sc.getAppName(), db, tableId);
        } finally {
          sc.releaseDatabase(db);
          db = null;
        }
      }
    } finally {
      // Here we also want to add the TableResult to the value.
      if (rowDataSyncSuccessful) {
        // Then we should have updated the db and shouldn't have set the
        // TableResult to be exception.
        if (tableResult.getStatus() != Status.WORKING) {
          log.e(TAG, "tableResult status for table: " + tableId + " was "
              + tableResult.getStatus().name()
              + ", and yet success returned true. This shouldn't be possible.");
        } else {
          if (containsConflicts) {
            tableResult.setStatus(Status.TABLE_CONTAINS_CONFLICTS);
            sc.updateNotification(SyncProgressState.ROWS,
                R.string.sync_table_data_sync_with_conflicts,
                new Object[] { tableId }, 100.0, false);
          } else if (!attachmentSyncSuccessful) {
            tableResult.setStatus(Status.TABLE_PENDING_ATTACHMENTS);
            sc.updateNotification(SyncProgressState.ROWS,
                R.string.sync_table_data_sync_pending_attachments, new Object[] { tableId }, 100.0,
                false);
          } else {
            tableResult.setStatus(Status.SUCCESS);
            sc.updateNotification(SyncProgressState.ROWS, R.string.sync_table_data_sync_complete,
                new Object[] { tableId }, 100.0, false);
          }
        }
      }
    }
  }

  /**
   * We pushed changes up to the server and now need to update the local rowETags to match
   * the rowETags assigned to those changes by the server.
   *
   * @param te            local table entry (dataETag is suspect)
   * @param resource        server schemaETag, dataETag etc. before(at) changes were pushed
   * @param tableResult          for progress UI
   * @param orderedColumns            all user-defined columns in the table
   * @param fileAttachmentColumns     columns in the table that hold rowPath values
   * @param hasAttachments            boolean indicating whether table has any rowPath columns
   * @param rowsToPushFileAttachments the rows-pending-attachments list
   * @param countSoFar           for progress UI
   * @param totalOutcomesSize    for progress UI
   * @param segmentAlter  changes that were pushed to server
   * @param outcomes      result of pushing those changes to server
   * @param specialCases
   * @return
   * @throws RemoteException
   */
  private int processRowOutcomes(TableDefinitionEntry te, TableResource resource,
      TableResult tableResult, OrderedColumns orderedColumns,
      ArrayList<ColumnDefinition> fileAttachmentColumns, boolean hasAttachments,
      List<SyncRowPending> rowsToPushFileAttachments, int countSoFar, int totalOutcomesSize,
      List<SyncRow> segmentAlter, ArrayList<RowOutcome> outcomes, ArrayList<RowOutcome> specialCases) throws RemoteException {

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

    OdkDbHandle db = null;
    try {
      db = sc.getDatabase();

      boolean badState = false;
      for (int i = 0; i < segmentAlter.size(); ++i) {
        RowOutcome r = outcomes.get(i);
        SyncRow syncRow = segmentAlter.get(i);
        if (!r.getRowId().equals(syncRow.getRowId())) {
          throw new IllegalStateException("Unexpected reordering of return");
        }
        if (r.getOutcome() == OutcomeType.SUCCESS) {

          if (r.isDeleted()) {
            // DELETE
            // same as a conflict resolution to accept server-side changes
            sc.getDatabaseService().resolveServerConflictWithDeleteRowWithId(sc
                .getAppName(), db, resource.getTableId(), r.getRowId());
            // !!Important!! update the rowETag in our copy of this row.
            syncRow.setRowETag(r.getRowETag());
            tableResult.incServerDeletes();
          } else {
           sc.getDatabaseService().updateRowETagAndSyncState(
                     sc.getAppName(), db,
                    resource.getTableId(),
                    r.getRowId(),
                    r.getRowETag(),
                    ((hasAttachments && !syncRow.getUriFragments().isEmpty()) ? SyncState.synced_pending_files
                        : SyncState.synced).name());
            // !!Important!! update the rowETag in our copy of this row.
            syncRow.setRowETag(r.getRowETag());
            if (hasAttachments && !syncRow.getUriFragments().isEmpty()) {
              rowsToPushFileAttachments.add(new SyncRowPending(syncRow, false, true, true));
            }
            // UPDATE or INSERT
            tableResult.incServerUpserts();
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
              r.getSavepointCreator(), r.getFilterScope(), r.getValues(), fileAttachmentColumns);
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
          rowsToPushFileAttachments, hasAttachments, tableResult);
    } finally {
      if (db != null) {
        sc.releaseDatabase(db);
        db = null;
      }
    }

    return countSoFar;
  }

  /**
   * Delete any pre-existing server conflict records for the list of rows
   * (changes). If the server and local rows are both deletes, delete the local
   * row (and its attachments), thereby completing the deletion of the row
   * (entirely). Otherwise, change the local row to the in_conflict state, and
   * insert a copy of the server row locally, configured as a server conflict
   * record; in that case, add the server and client rows to
   * rowsToSyncFileAttachments.
   * 
   * @param db
   * @param resource
   * @param orderedColumns
   * @param changes
   * @param rowsToSyncFileAttachments
   * @param hasAttachments
   * @param tableResult
   * @throws ClientWebException
   * @throws RemoteException 
   */
  private void conflictRowsInDb(OdkDbHandle db, TableResource resource,
      OrderedColumns orderedColumns, List<SyncRowDataChanges> changes,
      List<SyncRowPending> rowsToSyncFileAttachments, boolean hasAttachments,
      TableResult tableResult) throws ClientWebException, RemoteException {

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
        sc.getDatabaseService().resolveServerConflictWithDeleteRowWithId(
            sc.getAppName(), db, resource.getTableId(), serverRow.getRowId());

        tableResult.incLocalDeletes();
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
        Scope.Type type = serverRow.getFilterScope().getType();
        values.put(DataTableColumns.FILTER_TYPE,
            (type == null) ? Scope.Type.DEFAULT.name() : type.name());
        values.put(DataTableColumns.FILTER_VALUE, serverRow.getFilterScope().getValue());

        sc.getDatabaseService().placeRowIntoServerConflictWithId(sc.getAppName(), db,
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

        tableResult.incLocalConflicts();

        // try to pull the file attachments for the in_conflict rows
        // it is OK if we can't get them, but they may be useful for
        // reconciliation
        if (hasAttachments) {
          if (!change.localRow.getUriFragments().isEmpty()) {
            rowsToSyncFileAttachments.add(new SyncRowPending(change.localRow, true, false, false));
          }
          if (!serverRow.getUriFragments().isEmpty()) {
            rowsToSyncFileAttachments.add(new SyncRowPending(serverRow, true, false, false));
          }
        }
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
   * @param rowsToPushFileAttachments
   * @param hasAttachments
   * @param tableResult
   * @throws ClientWebException
   * @throws RemoteException 
   */
  private void insertRowsInDb(OdkDbHandle db, TableResource resource,
      OrderedColumns orderedColumns, List<SyncRowDataChanges> changes,
      List<SyncRowPending> rowsToPushFileAttachments, boolean hasAttachments,
      TableResult tableResult) throws ClientWebException, RemoteException {
    int count = 0;
    for (SyncRowDataChanges change : changes) {
      SyncRow serverRow = change.serverRow;
      ContentValues values = new ContentValues();

      values.put(DataTableColumns.ID, serverRow.getRowId());
      values.put(DataTableColumns.ROW_ETAG, serverRow.getRowETag());
      values.put(DataTableColumns.SYNC_STATE, (hasAttachments && !serverRow.getUriFragments()
          .isEmpty()) ? SyncState.synced_pending_files.name() : SyncState.synced.name());
      values.put(DataTableColumns.FORM_ID, serverRow.getFormId());
      values.put(DataTableColumns.LOCALE, serverRow.getLocale());
      values.put(DataTableColumns.SAVEPOINT_TIMESTAMP, serverRow.getSavepointTimestamp());
      values.put(DataTableColumns.SAVEPOINT_CREATOR, serverRow.getSavepointCreator());

      for (DataKeyValue entry : serverRow.getValues()) {
        String colName = entry.column;
        values.put(colName, entry.value);
      }

      sc.getDatabaseService().insertRowWithId(sc.getAppName(), db,
          resource.getTableId(),
          orderedColumns, values, serverRow.getRowId());
      tableResult.incLocalInserts();

      if (hasAttachments && !serverRow.getUriFragments().isEmpty()) {
        rowsToPushFileAttachments.add(new SyncRowPending(serverRow, true, true, true));
      }
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
   * @param rowsToSyncFileAttachments
   * @param hasAttachments
   * @param tableResult
   * @throws ClientWebException
   * @throws RemoteException 
   */
  private void updateRowsInDb(OdkDbHandle db, TableResource resource,
      OrderedColumns orderedColumns, List<SyncRowDataChanges> changes,
      List<SyncRowPending> rowsToSyncFileAttachments, boolean hasAttachments,
      TableResult tableResult) throws ClientWebException, RemoteException {
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
      values.put(DataTableColumns.FILTER_TYPE, serverRow.getFilterScope().getType().name());
      values.put(DataTableColumns.FILTER_VALUE, serverRow.getFilterScope().getValue());
      values.put(DataTableColumns.FORM_ID, serverRow.getFormId());
      values.put(DataTableColumns.LOCALE, serverRow.getLocale());
      values.put(DataTableColumns.SAVEPOINT_TYPE, serverRow.getSavepointType());
      values.put(DataTableColumns.SAVEPOINT_TIMESTAMP, serverRow.getSavepointTimestamp());
      values.put(DataTableColumns.SAVEPOINT_CREATOR, serverRow.getSavepointCreator());

      for (DataKeyValue entry : serverRow.getValues()) {
        String colName = entry.column;
        values.put(colName, entry.value);
      }

      sc.getDatabaseService().updateRowWithId(sc.getAppName(), db,
          resource.getTableId(),
          orderedColumns, values, serverRow.getRowId());
      tableResult.incLocalUpdates();

      if (hasAttachments && !serverRow.getUriFragments().isEmpty()) {
        rowsToSyncFileAttachments.add(new SyncRowPending(serverRow, false, true, true));
      }

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
    * @param fileAttachmentColumns
    * @param attachmentState
    * @param tableResult
    * @throws IOException
    * @throws RemoteException
    */
  private void pushLocalAttachmentsBeforeDeleteRowsInDb(OdkDbHandle db, TableResource resource,
      List<SyncRowDataChanges> changes, ArrayList<ColumnDefinition> fileAttachmentColumns,
      SyncAttachmentState attachmentState, TableResult tableResult) throws IOException, RemoteException {

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
          boolean outcome = sc.getSynchronizer().syncFileAttachments(resource.getInstanceFilesUri(),
              resource.getTableId(), srp, attachmentState);
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
            sc.getDatabaseService().updateRowETagAndSyncState(sc.getAppName(), db, 
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
   * @param changes
   * @param fileAttachmentColumns
   * @param attachmentState
   * @param tableResult
   * @throws IOException
   * @throws RemoteException 
   */
  private void deleteRowsInDb(OdkDbHandle db, TableResource resource,
      List<SyncRowDataChanges> changes, ArrayList<ColumnDefinition> fileAttachmentColumns,
      SyncAttachmentState attachmentState, TableResult tableResult) throws IOException, RemoteException {
    int count = 0;

    // now delete the rows we can delete...
    for (SyncRowDataChanges change : changes) {
      if (!change.isSyncedPendingFiles) {
        // DELETE
        // this is equivalent to the conflict-resolution action where we accept the server delete
        // this ensures there are no server conflict rows, and that the local row is removed.
        sc.getDatabaseService().resolveServerConflictWithDeleteRowWithId(
            sc.getAppName(), db, resource.getTableId(), change.serverRow.getRowId());
        tableResult.incLocalDeletes();
      }
      ++count;
      ++rowsProcessed;
      sc.updateNotification(SyncProgressState.ROWS, R.string.sync_deleting_local_row, new Object[] {
          resource.getTableId(), count, changes.size() }, 10.0 + rowsProcessed * perRowIncrement,
          false);
    }
  }
}
