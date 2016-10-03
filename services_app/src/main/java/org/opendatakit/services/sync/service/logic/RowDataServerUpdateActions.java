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
import org.opendatakit.aggregate.odktables.rest.ElementDataType;
import org.opendatakit.aggregate.odktables.rest.SyncState;
import org.opendatakit.aggregate.odktables.rest.entity.DataKeyValue;
import org.opendatakit.aggregate.odktables.rest.entity.RowFilterScope;
import org.opendatakit.aggregate.odktables.rest.entity.RowOutcome;
import org.opendatakit.aggregate.odktables.rest.entity.RowResource;
import org.opendatakit.aggregate.odktables.rest.entity.TableResource;
import org.opendatakit.database.data.ColumnDefinition;
import org.opendatakit.database.data.OrderedColumns;
import org.opendatakit.database.data.Row;
import org.opendatakit.database.service.DbHandle;
import org.opendatakit.exception.ServicesAvailabilityException;
import org.opendatakit.logging.WebLogger;
import org.opendatakit.logging.WebLoggerIf;
import org.opendatakit.provider.DataTableColumns;
import org.opendatakit.services.R;
import org.opendatakit.services.sync.service.SyncExecutionContext;
import org.opendatakit.sync.service.SyncAttachmentState;
import org.opendatakit.sync.service.TableLevelResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * RowDataServerUpdateActions contains the local database update actions that result from
 * changes on the server being retrieved and applied to the local database. These were part of
 * the original SyncProcessor class. These methods are invoked from ProcessRowDataPullServerUpdates.
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
  private IProcessRowData manager;

  RowDataServerUpdateActions(IProcessRowData manager) {
    this.manager = manager;
    this.sc = manager.getSyncExecutionContext();
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
   * @param serverRow
   * @param localRowConflictType
   * @param tableLevelResult
   * @return updated currentAffectedRow value
   * @throws ServicesAvailabilityException
   */
  void conflictRowInDb(DbHandle db, TableResource resource,
      OrderedColumns orderedColumns,
      RowResource serverRow,
      int localRowConflictType,
      TableLevelResult tableLevelResult) throws ServicesAvailabilityException {

    log.i(TAG,
        "conflicting row, id=" + serverRow.getRowId() + " rowETag=" + serverRow.getRowETag());

    // update existing localRow

    // the localRow conflict type was determined when the
    // change was added to the changes list.

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
    manager.publishUpdateNotification(R.string.sync_marking_conflicting_local_row,
        resource.getTableId());
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
   * @param serverRow
   * @param localRowConflictType
   * @param tableLevelResult
   * @return updated currentAffectedRow value
   * @throws ServicesAvailabilityException
   */
  void conflictRowInDb(DbHandle db, TableResource resource,
      OrderedColumns orderedColumns,
      RowOutcome serverRow,
      int localRowConflictType,
      TableLevelResult tableLevelResult) throws ServicesAvailabilityException {

    log.i(TAG,
        "conflicting row, id=" + serverRow.getRowId() + " rowETag=" + serverRow.getRowETag());

    // update existing localRow

    // the localRow conflict type was determined when the
    // change was added to the changes list.

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
    manager.publishUpdateNotification(R.string.sync_marking_conflicting_local_row,
        resource.getTableId());
  }

  /**
   * Inserts the given list of rows (changes) into the local database. Adds
   * those rows to the rowsToPushFileAttachments list if they have any non-null
   * media attachments.
   *
   * @param db
   * @param resource
   * @param orderedColumns
   * @param serverElementKeyToIndex
   * @param fileAttachmentColumns
   * @param serverRow
   * @param tableLevelResult
   * @return updated currentAffectedRow value
   * @throws ServicesAvailabilityException
   */
  void insertRowInDb(DbHandle db, TableResource resource,
      OrderedColumns orderedColumns,
      HashMap<String,Integer> serverElementKeyToIndex,
      ArrayList<ColumnDefinition> fileAttachmentColumns,
      RowResource serverRow,
      TableLevelResult tableLevelResult) throws ServicesAvailabilityException {

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

    ContentValues values = new ContentValues();

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

    manager.publishUpdateNotification(R.string.sync_inserting_local_row, resource.getTableId());
  }

  /**
   * Updates the given list of rows (changes) in the local database. Adds those
   * rows to the rowsToPushFileAttachments list if they have any non-null media
   * attachments.
   *
   * @param db
   * @param resource
   * @param orderedColumns
   * @param serverElementKeyToIndex
   * @param fileAttachmentColumns
   * @param localRow
   * @param serverRow
   * @param tableLevelResult
   * @return updated currentAffectedRow value
   * @throws ServicesAvailabilityException
   */
  void updateRowInDb(DbHandle db, TableResource resource,
      OrderedColumns orderedColumns,
      HashMap<String,Integer> serverElementKeyToIndex,
      ArrayList<ColumnDefinition> fileAttachmentColumns,
      Row localRow, RowResource serverRow, boolean isSyncedPendingFiles,
      TableLevelResult tableLevelResult) throws ServicesAvailabilityException {

    // if the localRow sync state was synced_pending_files,
    // ensure that all those files are uploaded before
    // we update the row. This ensures that all attachments
    // are saved before we revise the local row value.
    if (isSyncedPendingFiles) {
      log.w(TAG,
          "file attachment at risk -- updating from server while in synced_pending_files state. rowId: "
              + localRow.getDataByKey(DataTableColumns.ID)
              + " rowETag: " + localRow.getDataByKey(DataTableColumns.ROW_ETAG));
    }

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

    // update the row from the changes on the server
    ContentValues values = new ContentValues();

    values.put(DataTableColumns.ROW_ETAG, serverRow.getRowETag());
    values.put(DataTableColumns.SYNC_STATE, (hasNonNullAttachments)
        ? SyncState.synced_pending_files.name() : SyncState.synced.name());
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

    manager.publishUpdateNotification(R.string.sync_updating_local_row, resource.getTableId());
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
   * @param fileAttachmentColumns
   * @param localRow
   * @param serverRowETag
   * @param isSyncedPendingFiles
   * @param tableLevelResult
   * @return updated currentAffectedRow value
   * @throws IOException
   * @throws ServicesAvailabilityException
   */
  void deleteRowInDb(DbHandle db, TableResource resource, OrderedColumns orderedColumns,
      ArrayList<ColumnDefinition> fileAttachmentColumns,
      Row localRow, String serverRowETag, boolean isSyncedPendingFiles,
      TableLevelResult tableLevelResult) throws IOException,
      ServicesAvailabilityException {

    // we should delete the row if it is not synced_pending_files
    boolean deleteLocalRow = !isSyncedPendingFiles;

    // if it is synced_pending_files
    if (isSyncedPendingFiles) {
        // attempt to upload all referenced attachments to the server.
      deleteLocalRow = manifestProcessor.syncRowLevelFileAttachments(
          resource.getInstanceFilesUri(),
          resource.getTableId(), localRow, fileAttachmentColumns, SyncAttachmentState.UPLOAD);
    }

    if (deleteLocalRow) {
      // DELETE
      // this is equivalent to the conflict-resolution action where we accept the server delete
      // this ensures there are no server conflict rows, and that the local row is removed.
      sc.getDatabaseService().privilegedDeleteRowWithId(
          sc.getAppName(), db, resource.getTableId(), orderedColumns,
          localRow.getDataByKey(DataTableColumns.ID));
      tableLevelResult.incLocalDeletes();

      manager.publishUpdateNotification(R.string.sync_deleting_local_row, resource.getTableId());
    } else {
      // there are files that should be uploaded that weren't.
      // change local state to deleted.
      // Whenever we next sync files, we will upload
      // any local files that are not on the server then delete
      // the local record.
      sc.getDatabaseService().privilegedUpdateRowETagAndSyncState(sc.getAppName(), db,
          resource.getTableId(),
          localRow.getDataByKey(DataTableColumns.ID),
          serverRowETag, SyncState.deleted.name());

      manager.publishUpdateNotification(R.string.sync_deferring_delete_local_row, resource.getTableId());
    }
  }

  /**
   * Compares user and metadata field values, but excludes sync_state, rowETag,
   * filter type and filter value.
   *
   * @param orderedDefns
   * @return true if the fields are identical
   */
  public boolean identicalValuesExceptRowETagAndFilterScope(OrderedColumns orderedDefns,
      HashMap<String,Integer> serverElementKeyToIndex,
      Row localRow, RowResource serverRow) {
    String value;

    value = localRow.getDataByKey(DataTableColumns.SAVEPOINT_TIMESTAMP);
    if ((serverRow.getSavepointTimestamp() == null) ? (value != null)
        : !serverRow.getSavepointTimestamp().equals(value)) {
      return false;
    }

    value = localRow.getDataByKey(DataTableColumns.SAVEPOINT_CREATOR);
    if ((serverRow.getSavepointCreator() == null) ? (value != null)
        : !serverRow.getSavepointCreator().equals(value)) {
      return false;
    }

    value = localRow.getDataByKey(DataTableColumns.FORM_ID);
    if ((serverRow.getFormId() == null) ? (value != null) : !serverRow.getFormId()
        .equals(value)) {
      return false;
    }

    value = localRow.getDataByKey(DataTableColumns.LOCALE);
    if ((serverRow.getLocale() == null) ? (value != null) : !serverRow.getLocale()
        .equals(value)) {
      return false;
    }

    value = localRow.getDataByKey(DataTableColumns.ID);
    if ((serverRow.getRowId() == null) ? (value != null) : !serverRow.getRowId()
        .equals(value)) {
      return false;
    }

    value = localRow.getDataByKey(DataTableColumns.SAVEPOINT_TYPE);
    if ((serverRow.getSavepointType() == null) ? (value != null)
        : !serverRow.getSavepointType().equals(value)) {
      return false;
    }

    if ( serverElementKeyToIndex.size() != orderedDefns.getColumnDefinitions().size() ) {
      return false;
    }

    for ( ColumnDefinition cd : orderedDefns.getColumnDefinitions() ) {
      Integer idx = serverElementKeyToIndex.get(cd.getElementKey());
      if ( idx == null ) {
        throw new IllegalStateException("Could not find index for element key!");
      }
      String localValue = localRow.getDataByKey(cd.getElementKey());
      String serverValue = serverRow.getValues().get(idx).value;

      if (localValue == null && serverValue == null) {
        continue;
      } else if (localValue == null || serverValue == null) {
        return false;
      } else if (localValue.equals(serverValue)) {
        continue;
      }

      // NOT textually identical.
      //
      // Everything must be textually identical except possibly number fields
      // which may have rounding due to different database implementations,
      // data representations, and marshaling libraries.
      //
      if (cd.getType().getDataType() == ElementDataType.number) {
        // !!Important!! Double.valueOf(str) handles NaN and +/-Infinity
        Double localNumber = Double.valueOf(localValue);
        Double serverNumber = Double.valueOf(serverValue);

        if (localNumber.equals(serverNumber)) {
          // simple case -- trailing zeros or string representation mix-up
          //
          continue;
        } else if (localNumber.isInfinite() && serverNumber.isInfinite()) {
          // if they are both plus or both minus infinity, we have a match
          if (Math.signum(localNumber) == Math.signum(serverNumber)) {
            continue;
          } else {
            return false;
          }
        } else if (localNumber.isNaN() || localNumber.isInfinite() || serverNumber.isNaN()
            || serverNumber.isInfinite()) {
          // one or the other is special1
          return false;
        } else {
          double localDbl = localNumber;
          double serverDbl = serverNumber;
          if (localDbl == serverDbl) {
            continue;
          }
          // OK. We have two values like 9.80 and 9.8
          // consider them equal if they are adjacent to each other.
          double localNear = localDbl;
          int idist = 0;
          int idistMax = 128;
          for (idist = 0; idist < idistMax; ++idist) {
            localNear = Math.nextAfter(localNear, serverDbl);
            if (localNear == serverDbl) {
              break;
            }
          }
          if (idist < idistMax) {
            continue;
          }
          return false;
        }
      } else {
        // textual identity is required!
        return false;
      }
    }
    // because we ensure that (localValues.size() == serverValues.size())
    // and we ensure inside the loop that (local.column.equals(server.column))
    // and we test for semantic equivalence of the fields (handling numbers specially)
    // we should NOT test for containment of the server field-value list in the
    // local field-value list.  That is too restrictive because it rejects
    // semantically-equivalent field-value entries (which is the entire point of
    // the above loop).
    return true;
  }

  /**
   * Compares user and metadata values, excluding the sync state.
   *
   * @param orderedDefns
   * @return
   */
  public boolean identicalValues(OrderedColumns orderedDefns,
      HashMap<String,Integer> serverElementKeyToIndex,
      Row localRow, RowResource serverRow) {

    String localFilterType = localRow.getDataByKey(DataTableColumns.FILTER_TYPE);
    String localFilterValue = localRow.getDataByKey(DataTableColumns.FILTER_VALUE);
    RowFilterScope localFilterScope = RowFilterScope.asRowFilter(localFilterType, localFilterValue);
    if ((serverRow.getRowFilterScope() == null) ? (localFilterScope != null) : !serverRow
        .getRowFilterScope().equals(localFilterScope)) {
      return false;
    }
    String localRowETag = localRow.getDataByKey(DataTableColumns.ROW_ETAG);
    if ((serverRow.getRowETag() == null) ? (localRowETag != null) : !serverRow
        .getRowETag().equals(localRowETag)) {
      return false;
    }
    return identicalValuesExceptRowETagAndFilterScope(orderedDefns, serverElementKeyToIndex,
        localRow, serverRow);
  }

}
