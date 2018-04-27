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

import org.opendatakit.aggregate.odktables.rest.ElementDataType;
import org.opendatakit.aggregate.odktables.rest.SyncState;
import org.opendatakit.aggregate.odktables.rest.entity.TableResource;
import org.opendatakit.database.data.BaseTable;
import org.opendatakit.database.data.ColumnDefinition;
import org.opendatakit.database.data.OrderedColumns;
import org.opendatakit.database.data.Row;
import org.opendatakit.database.data.TableDefinitionEntry;
import org.opendatakit.database.queries.BindArgs;
import org.opendatakit.database.service.DbHandle;
import org.opendatakit.exception.ServicesAvailabilityException;
import org.opendatakit.logging.WebLogger;
import org.opendatakit.logging.WebLoggerIf;
import org.opendatakit.provider.DataTableColumns;
import org.opendatakit.provider.FormsColumns;
import org.opendatakit.services.R;
import org.opendatakit.services.sync.service.SyncExecutionContext;
import org.opendatakit.sync.service.SyncAttachmentState;
import org.opendatakit.sync.service.SyncOutcome;
import org.opendatakit.sync.service.SyncProgressState;
import org.opendatakit.sync.service.TableLevelResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * SyncProcessor implements the cloud synchronization logic for Tables.
 *
 * @author the.dylan.price@gmail.com
 * @author sudar.sam@gmail.com
 *
 */
public class ProcessRowDataOrchestrateChanges {

  private static final String TAG = ProcessRowDataOrchestrateChanges.class.getSimpleName();

  private final WebLoggerIf log;

  private final SyncExecutionContext sc;

  private final ProcessRowDataPullServerUpdates serverUpdateProcessor;
  private final ProcessRowDataPushLocalChanges localChangesProcessor;
  private final ProcessRowDataSyncAttachments syncAttachmentsProcessor;

  public ProcessRowDataOrchestrateChanges(SyncExecutionContext sharedContext) {
    this.sc = sharedContext;
    this.log = WebLogger.getLogger(sc.getAppName());
    this.serverUpdateProcessor = new ProcessRowDataPullServerUpdates(sc);
    this.localChangesProcessor = new ProcessRowDataPushLocalChanges(sc);
    this.syncAttachmentsProcessor = new ProcessRowDataSyncAttachments(sc);
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
    log.i(TAG, "entered synchronizeDataRowsAndAttachments()");

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

      synchronizeTableDataRowsAndAttachments(te, orderedDefns, displayName,
          attachmentState);

      // report our table-level sync status up to the server.
      TableLevelResult tlr = sc.getTableLevelResult(tableId);
      try {

        int checkpoints = 0;
        int conflicts = 0;
        int rows = 0;
        try {
          db = sc.getDatabase();
          // get counts of checkpoints, conflicts and rows in the table
          BaseTable t = sc.getDatabaseService().arbitrarySqlQuery(sc.getAppName(), db, null,
              "SELECT sum(case when " + DataTableColumns.SAVEPOINT_TYPE +
                  " IS NULL THEN 1 ELSE 0 END) as n_checkpoints,"
                  + " sum(case when " + DataTableColumns.CONFLICT_TYPE +
                  " IS NOT NULL THEN 1 ELSE 0 END) as n_dblconflicts,"
                  + " count(*) as n_rows"
                  + " FROM " + tableId, null, null, null );
          if ( t.getNumberOfRows() == 1 ) {
            Row row = t.getRowAtIndex(0);
            String checkpointStr = row.getRawStringByKey("n_checkpoints");
            String dblconflictsStr = row.getRawStringByKey("n_dblconflicts");
            String rowsStr = row.getRawStringByKey("n_rows");
            checkpoints = (checkpointStr == null) ? 0 : Integer.valueOf(checkpointStr);
            conflicts = (dblconflictsStr == null) ? 0 : Integer.valueOf(dblconflictsStr) / 2;
            rows = (rowsStr == null) ? 0 : Integer.valueOf(rowsStr);
          }
        } finally {
          sc.releaseDatabase(db);
          db = null;
        }

        // get sync status details
        HashMap<String, Object> statusMap = tlr.getStatusMap();
        statusMap.put("localNumCheckpoints", checkpoints);
        statusMap.put("localNumConflicts", conflicts);
        statusMap.put("localNumRows", rows);
        sc.getSynchronizer().publishTableSyncStatus(tableResource, statusMap);
      } catch (Exception e) {
        log.e(
            TAG,
            "synchronizeDataRowsAndAttachments - unable to report sync status: "
                + tableId);
        log.printStackTrace(e);
        return;
      }

      sc.incMajorSyncStep();
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
   * @param te
   *          definition of the table to synchronize
   * @param orderedColumns
   *          well-formed ordered list of columns in this table.
   * @param displayName
   *          display name for this tableId - used in notifications
   * @param attachmentState
   * @throws ServicesAvailabilityException
   */
  private void synchronizeTableDataRowsAndAttachments(
      TableDefinitionEntry te, OrderedColumns orderedColumns, String displayName,
      SyncAttachmentState attachmentState) throws ServicesAvailabilityException {

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
      // there was some sort of error during config file syncing...
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

    try {
      log.i(TAG, "REST " + tableId);

      boolean refreshFromServer = false;
      do {
        tableLevelResult.setMessage("scanning for changes to send to server");

        sc.updateNotification(SyncProgressState.ROWS,
            R.string.sync_verifying_table_schema_on_server, new Object[] { tableId }, 0.0, false);


        TableResource tableResource = sc.getSynchronizer().getTable(tableId);

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
          tableLevelResult.setMessage(
              "Server schemaETag differs! Sync app-level files and configuration in order to sync this table.");
          tableLevelResult.setSyncOutcome(SyncOutcome.TABLE_REQUIRES_APP_LEVEL_SYNC);
          return;
        }

        try {
          serverUpdateProcessor
              .updateLocalRowsFromServer(tableResource, te, orderedColumns, fileAttachmentColumns);
        } catch (Exception e) {
          exception("synchronizeTableDataRowsAndAttachments -  pulling data down from server", tableId, e,
              tableLevelResult);
          return;
        }

        if (tableLevelResult.getSyncOutcome() != SyncOutcome.WORKING) {
          return;
        }

        try {
          refreshFromServer = localChangesProcessor
              .pushLocalChanges(tableResource, te, orderedColumns, fileAttachmentColumns);
        } catch (Exception e) {
          exception("synchronizeTableDataRowsAndAttachments -  pushing data up to server", tableId, e,
              tableLevelResult);
          return;
        }

        if (tableLevelResult.getSyncOutcome() != SyncOutcome.WORKING) {
          return;
        }

        if (!refreshFromServer) {
          try {
            syncAttachmentsProcessor
                .syncAttachments(tableResource, te, orderedColumns, fileAttachmentColumns, attachmentState);
          } catch (Exception e) {
            exception("synchronizeTableDataRowsAndAttachments -  syncing attachments with server", tableId, e,
                tableLevelResult);
            return;
          }
        }
      } while ( refreshFromServer );

      log.i(TAG, "synchronizeTableDataRowsAndAttachments completed processing for all attachments");

      if ( tableLevelResult.getSyncOutcome() != SyncOutcome.WORKING ) {
        return;
      }

      // //////////////////////////////////////////////////
      // //////////////////////////////////////////////////
      // fail the sync on this table if there are checkpoint rows.
      {
        StringBuilder b = new StringBuilder();
        b.append("SELECT ").append(DataTableColumns.ID).append(" FROM ").append(tableId)
            .append(" WHERE ").append(DataTableColumns.SAVEPOINT_TYPE).append(" IS NULL");
        DbHandle db = null;
        try {
          db = sc.getDatabase();
          BaseTable bt = sc.getDatabaseService().arbitrarySqlQuery(sc.getAppName(), db, null,
              b.toString(), null, 1, 0);
          if ( bt.getNumberOfRows() != 0 ) {
            tableLevelResult.setMessage(sc.getString(R.string.sync_table_contains_checkpoints));
            tableLevelResult.setSyncOutcome(SyncOutcome.TABLE_CONTAINS_CHECKPOINTS);
            return;
          }

        } finally {
          sc.releaseDatabase(db);
          db = null;
        }
      }

      // //////////////////////////////////////////////////
      // //////////////////////////////////////////////////
      // fail the sync on this table if there are conflict rows.
      {
        StringBuilder b = new StringBuilder();
        b.append("SELECT ").append(DataTableColumns.ID).append(" FROM ").append(tableId)
            .append(" WHERE ").append(DataTableColumns.SYNC_STATE).append(" = ?");
        BindArgs bindArgs = new BindArgs(new Object[]{ SyncState.in_conflict.name() });
        DbHandle db = null;
        try {
          db = sc.getDatabase();
          BaseTable bt = sc.getDatabaseService().arbitrarySqlQuery(sc.getAppName(), db, null,
              b.toString(), bindArgs, 1, 0);
          if ( bt.getNumberOfRows() != 0 ) {
            tableLevelResult.setSyncOutcome(SyncOutcome.TABLE_CONTAINS_CONFLICTS);
            tableLevelResult.setMessage(sc.getString(R.string.sync_table_data_sync_with_conflicts));
            sc.updateNotification(SyncProgressState.ROWS,
                R.string.sync_table_data_sync_with_conflicts,
                new Object[]{tableId}, 100.0, false);
            return;
          }

        } finally {
          sc.releaseDatabase(db);
          db = null;
        }
      }

    } catch (IOException e) {
      exception("synchronizeTableDataRowsAndAttachments -  pulling data down from server", tableId, e,
          tableLevelResult);
      return;
    } finally {
      if (tableLevelResult.getSyncOutcome() == SyncOutcome.WORKING) {
        tableLevelResult.setSyncOutcome(SyncOutcome.SUCCESS);
        sc.updateNotification(SyncProgressState.ROWS, R.string.sync_table_data_sync_complete,
            new Object[]{tableId}, 100.0, false);
      }
    }
  }
}
