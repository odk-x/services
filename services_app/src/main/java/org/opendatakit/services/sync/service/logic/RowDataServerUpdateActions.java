/*
 * Copyright (C) 2016 University of Washington
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
import org.opendatakit.aggregate.odktables.rest.SyncState;
import org.opendatakit.aggregate.odktables.rest.entity.DataKeyValue;
import org.opendatakit.aggregate.odktables.rest.entity.RowFilterScope;
import org.opendatakit.aggregate.odktables.rest.entity.TableResource;
import org.opendatakit.database.data.OrderedColumns;
import org.opendatakit.database.service.DbHandle;
import org.opendatakit.exception.ServicesAvailabilityException;
import org.opendatakit.logging.WebLogger;
import org.opendatakit.logging.WebLoggerIf;
import org.opendatakit.provider.DataTableColumns;
import org.opendatakit.services.R;
import org.opendatakit.services.sync.service.SyncExecutionContext;
import org.opendatakit.services.sync.service.data.SyncRow;
import org.opendatakit.services.sync.service.data.SyncRowDataChanges;
import org.opendatakit.services.sync.service.data.SyncRowPending;
import org.opendatakit.services.sync.service.exceptions.HttpClientWebException;
import org.opendatakit.sync.service.SyncAttachmentState;
import org.opendatakit.sync.service.TableLevelResult;

import java.io.IOException;
import java.util.List;

/**
 * RowDataServerUpdateActions contains the local database update actions that result from
 * changes on the server being retrieved and applied to the local database. These were part of
 * the original SyncProcessor class. These methods are invoked from ProcessRowDataServerUpdates.
 *
 * @author the.dylan.price@gmail.com
 * @author sudar.sam@gmail.com
 * @author mitchellsundt@gmail.com
 *
 */
public class RowDataServerUpdateActions {

  private static final String TAG = RowDataServerUpdateActions.class.getSimpleName();

  private WebLoggerIf log;

  private final SyncExecutionContext sc;

  private ProcessManifestContentAndFileChanges manifestProcessor;
  private ProcessRowDataServerUpdates manager;

  RowDataServerUpdateActions(ProcessRowDataServerUpdates manager) {
    this.manager = manager;
    this.sc = manager.sc;
    this.log = WebLogger.getLogger(sc.getAppName());
    this.manifestProcessor = new ProcessManifestContentAndFileChanges(sc);
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
  void conflictRowsInDb(DbHandle db, TableResource resource,
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
      count = manager.publishUpdateNotification(R.string.sync_marking_conflicting_local_row,
          resource.getTableId(), count, changes.size());
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
  void insertRowsInDb(DbHandle db, TableResource resource,
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

      count = manager.publishUpdateNotification(R.string.sync_inserting_local_row,
          resource.getTableId(), count, changes.size());
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
  void updateRowsInDb(DbHandle db, TableResource resource,
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

      count = manager.publishUpdateNotification(R.string.sync_updating_local_row,
          resource.getTableId(), count, changes.size());
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
  void deleteRowsInDb(DbHandle db, TableResource resource, OrderedColumns orderedColumns,
      List<SyncRowDataChanges> changes, TableLevelResult tableLevelResult) throws IOException,
      ServicesAvailabilityException {

    // this will remove some rows from the changes list
    // if we cannot sync file attachments in those rows.
    pushLocalAttachmentsBeforeDeleteRowsInDb(db, resource, changes);

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

      count = manager.publishUpdateNotification(R.string.sync_deleting_local_row,
          resource.getTableId(), count, changes.size());
    }
  }
}
