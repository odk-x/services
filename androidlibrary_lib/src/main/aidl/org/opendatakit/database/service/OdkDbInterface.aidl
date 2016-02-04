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
package org.opendatakit.database.service;

import java.util.List;

import org.opendatakit.common.android.data.OrderedColumns;
import org.opendatakit.common.android.data.ColumnList;
import org.opendatakit.common.android.data.TableDefinitionEntry;
import org.opendatakit.common.android.data.UserTable;
import org.opendatakit.common.android.data.RawUserTable;
import org.opendatakit.database.service.OdkDbHandle;
import org.opendatakit.database.service.TableHealthInfo;
import org.opendatakit.database.service.KeyValueStoreEntry;

interface OdkDbInterface {
    
  /**
   * Obtain a databaseHandleName
   *
   * @param appName
   *
   * @return dbHandleName
   */
  OdkDbHandle openDatabase(in String appName);

  /**
   * Release the databaseHandle. Will roll back any outstanding transactions
   * and release/close the database handle.
   *
   * @param appName
   * @param dbHandleName
   */
   void closeDatabase(in String appName, in OdkDbHandle dbHandleName); 

  /**
   * Call this when the schemaETag for the given tableId has changed on the server.
   *
   * This is a combination of:
   *
   * Clean up this table and set the dataETag to null.
   *
   * changeDataRowsToNewRowState(sc.getAppName(), db, tableId);
   *
   * we need to clear out the dataETag so
   * that we will pull all server changes and sync our properties.
   *
   * updateDBTableETags(sc.getAppName(), db, tableId, schemaETag, null);
   *
   * Although the server does not recognize this tableId, we can
   * keep our record of the ETags for the table-level files and
   * manifest. These may enable us to short-circuit the restoration
   * of the table-level files should another client be simultaneously
   * trying to restore those files to the server.
   *
   * However, we do need to delete all the instance-level files,
   * as these are tied to the schemaETag we hold, and that is now
   * invalid.
   *
   * if the local table ever had any server sync information for this
   * host then clear it. If the user changed the server URL, we have
   * already cleared this information.
   *
   * Clearing it here handles the case where an admin deleted the
   * table on the server and we are now re-pushing that table to
   * the server.
   *
   * We do not know whether the rows on the device match those on the server.
   * We will find out later, in the course of the sync.
   *
   * if (tableInstanceFilesUri != null) {
   *   deleteAllSyncETagsUnderServer(sc.getAppName(), db, tableInstanceFilesUri);
   * }
   */
  void serverTableSchemaETagChanged(in String appName, in OdkDbHandle dbHandleName,
    in String tableId, in String schemaETag, in String tableInstanceFilesUri);

  /**
   * Compute the app-global choiceListId for this choiceListJSON
   * and register the tuple of (choiceListId, choiceListJSON).
   * Return choiceListId.
   *
   * @param appName
   * @param dbHandleName
   * @param choiceListJSON -- the actual JSON choice list text.
   * @return choiceListId -- the unique code mapping to the choiceListJSON
   */
  String setChoiceList(in String appName, in OdkDbHandle dbHandleName,
   in String choiceListJSON );

  /**
   * Return the choice list JSON corresponding to the choiceListId
   *
   * @param appName
   * @param dbHandleName
   * @param choiceListId -- the md5 hash of the choiceListJSON
   * @return choiceListJSON -- the actual JSON choice list text.
   */
  String getChoiceList(in String appName, in OdkDbHandle dbHandleName, in String choiceListId );

  /**
   * If the tableId is not recorded in the TableDefinition metadata table, then
   * create the tableId with the indicated columns. This will synthesize
   * reasonable metadata KVS entries for table.
   * 
   * If the tableId is present, then this is a no-op.
   * 
   * @param appName
   * @param dbHandleName
   * @param tableId
   * @param columns simple transport wrapper for List<Columns>
   * @return the OrderedColumns of the user columns in the table.
   */
  OrderedColumns createOrOpenDBTableWithColumns(in String appName, in OdkDbHandle dbHandleName,
      in String tableId, in ColumnList columns);

	/**
   * If the tableId is not recorded in the TableDefinition metadata table, then
   * create the tableId with the indicated columns. And apply the supplied KVS
   * settings. If some are missing, this will synthesize reasonable metadata KVS
   * entries for table.
   *
   * If the table is present, this will delete and replace the KVS with the given KVS
   * entries if the clear flag is true
	 *
   * @param appName
   * @param dbHandleName
   * @param tableId
   * @param columns simple transport wrapper for List<Columns>
   * @param metaData a List<KeyValueStoreEntry>
   * @param clear if true then delete the existing set of values for this
   *          tableId before inserting or replacing with the new ones.
   * @return the OrderedColumns of the user columns in the table.
	 */
  OrderedColumns createOrOpenDBTableWithColumnsAndProperties(in String appName,
      in OdkDbHandle dbHandleName,
      in String tableId, in ColumnList columns,
      in List<KeyValueStoreEntry> metaData, in boolean clear);

  /**
   * Drop the given tableId and remove all the files (both configuration and
   * data attachments) associated with that table.
   * 
   * @param appName
   * @param dbHandleName
   * @param tableId
   */
  void deleteDBTableAndAllData(in String appName, in OdkDbHandle dbHandleName,
      in String tableId);
		
  /**
   * The deletion filter includes all non-null arguments. If all arguments
   * (except the db) are null, then all properties are removed.
   * 
   * @param appName
   * @param dbHandleName
   * @param tableId
   * @param partition
   * @param aspect
   * @param key
   */
  void deleteDBTableMetadata(in String appName, in OdkDbHandle dbHandleName,
      in String tableId, in String partition, in String aspect, in String key);

  /**
   * Return an array of the admin columns that must be present in
   * every database table.
   * 
   * @return
   */
  String[] getAdminColumns();
		
  /**
   * Return all the columns in the given table, including any metadata columns.
   * This does a direct query against the database and is suitable for accessing
   * non-managed tables. It does not access any metadata and therefore will not
   * report non-unit-of-retention (grouping) columns.
   * 
   * @param appName
   * @param dbHandleName
   * @param tableId
   * @return
   */
  String[] getAllColumnNames(in String appName, in OdkDbHandle dbHandleName,
      in String tableId);

  /**
   * Return all the tableIds in the database.
   * 
   * @param appName
   * @param dbHandleName
   * @return List<String> of tableIds
   */
  List<String> getAllTableIds(in String appName, in OdkDbHandle dbHandleName);
  
  /**
   * @param appName
   * @param dbHandleName
   * @param tableId
   * @param partition
   * @param aspect
   * @param key
   *
   * @return list of KeyValueStoreEntry values matching the filter criteria
   * @throws RemoteException
   */
  List<KeyValueStoreEntry> getDBTableMetadata(in String appName, in OdkDbHandle dbHandleName,
      in String tableId, in String partition, in String aspect, in String key);

  /**
   * Return an array of the admin columns that should be exported to
   * a CSV file. This list excludes the SYNC_STATE and CONFLICT_TYPE columns.
   * 
   * @return
   */
  String[] getExportColumns();

  /**
   * Get the table definition entry for a tableId. This specifies the schema
   * ETag, the data-modification ETag, and the date-time of the last successful
   * sync of the table to the server.
   * 
   * @param appName
   * @param dbHandleName
   * @param tableId
   * @return
   */
  TableDefinitionEntry getTableDefinitionEntry(in String appName, in OdkDbHandle dbHandleName,
      in String tableId);

  /**
   * Return the list of all tables and their health status.
   *
   * @param appName
   * @param dbHandleName
   *
   * @return the list of TableHealthInfo records for this appName
   */ 
  List<TableHealthInfo> getTableHealthStatuses(in String appName, in OdkDbHandle dbHandleName);
  
    /**
   * Retrieve the list of user-defined columns for a tableId using the metadata
   * for that table. Returns the unit-of-retention and non-unit-of-retention
   * (grouping) columns.
   * 
   * @param appName
   * @param dbHandleName
   * @param tableId
   * @return
   */
  OrderedColumns getUserDefinedColumns(in String appName, in OdkDbHandle dbHandleName, 
      in String tableId);
      
  /**
   * Verifies that the tableId exists in the database.
   *
   * @param appName
   * @param dbHandleName
   * @param tableId
   * @return true if table is listed in table definitions.
   */
  boolean hasTableId(in String appName, in OdkDbHandle dbHandleName, 
      in String tableId);

  /* rawQuery */

  /**
   * Get a {@link UserTable} for this table based on the given where clause. All
   * columns from the table are returned.
   * <p>
   * SELECT * FROM table WHERE whereClause GROUP BY groupBy[]s HAVING
   * havingClause ORDER BY orderbyElement orderByDirection
   * <p>
   * If any of the clause parts are omitted (null), then the appropriate
   * simplified SQL statement is constructed.
   * 
   * @param appName
   * @param dbHandleName
   * @param tableId
   * @param columnDefns
   * @param whereClause
   *          the whereClause for the selection, beginning with "WHERE". Must
   *          include "?" instead of actual values, which are instead passed in
   *          the selectionArgs.
   * @param selectionArgs
   *          an array of string values for bind parameters
   * @param groupBy
   *          an array of elementKeys
   * @param having
   * @param orderByElementKey
   *          elementKey to order the results by
   * @param orderByDirection
   *          either "ASC" or "DESC"
   * @return
   */
  UserTable rawSqlQuery(in String appName, in OdkDbHandle dbHandleName, 
      in String tableId,
      in OrderedColumns columnDefns, in String whereClause, in String[] selectionArgs,
      in String[] groupBy, in String having, in String orderByElementKey, in String orderByDirection);

  /**
   * Get a {@link RawUserTable} for the result set of an arbitrary sql query
   * and bind parameters. If the result set has an _id column, it is used as
   * the RowId of the RawRow. Otherwise, an ordinal number is generated and used.
   *
   * @param appName
   * @param dbHandleName
   * @param sqlCommand
   * @param sqlBindArgs
   * @return
   */
  RawUserTable arbitraryQuery(in String appName, in OdkDbHandle dbHandleName,
      in String sqlCommand, in String[] sqlBindArgs);
  /**
   * Insert or update a single table-level metadata KVS entry.
   * 
   * @param appName
   * @param dbHandleName
   * @param entry
   */
  void replaceDBTableMetadata(in String appName, in OdkDbHandle dbHandleName, 
      in KeyValueStoreEntry entry);

  /**
   * Insert or update a list of table-level metadata KVS entries. If clear is
   * true, then delete the existing set of values for this tableId before
   * inserting the new values.
   * 
   * @param appName
   * @param dbHandleName
   * @param tableId
   * @param metadata
   *          a List<KeyValueStoreEntry>
   * @param clear
   *          if true then delete the existing set of values for this tableId
   *          before inserting the new ones.
   */
  void replaceDBTableMetadataList(in String appName, in OdkDbHandle dbHandleName, 
      in String tableId,
      in List<KeyValueStoreEntry> metaData, in boolean clear);

  /**
   * Atomically delete all the fields under the given (tableId, partition, aspect)
   * and replace with the supplied values.
   *
   * @param appName
   * @param dbHandleName
   * @param tableId
   * @param partition
   * @param aspect
   * @param metadata
   *          a List<KeyValueStoreEntry>
   */
  void replaceDBTableMetadataSubList(in String appName, in OdkDbHandle dbHandleName,
      in String tableId, in String partition, in String aspect,
      in List<KeyValueStoreEntry> metaData);

    /**
   * Update the schema and data-modification ETags of a given tableId.
   *
   * @param appName
   * @param dbHandleName
   * @param tableId
   * @param schemaETag
   * @param lastDataETag
   */
  void updateDBTableETags(in String appName, in OdkDbHandle dbHandleName,
      in String tableId, in String schemaETag,
      in String lastDataETag);

  /**
   * Update the timestamp of the last entirely-successful synchronization
   * attempt of this table.
   *
   * @param appName
   * @param dbHandleName
   * @param tableId
   */
  void updateDBTableLastSyncTime(in String appName, in OdkDbHandle dbHandleName, in String tableId);

  /////////////////////////////////////////////////////////////////////////////////////
  // Row level changes
  /////////////////////////////////////////////////////////////////////////////////////

  /**
   *
   * @param appName
   * @param dbHandleName
   * @param tableId
   * @param rowId
   * @return the sync state of the row (use {@link SyncState.valueOf()} to reconstruct), or null if the
   *         row does not exist.
   */
  String getSyncState(in String appName, in OdkDbHandle dbHandleName,
      in String tableId, in String rowId);

  /**
   * Update the ETag and SyncState of a given rowId. There should be exactly one
   * record for this rowId in thed database (i.e., no conflicts or checkpoints).
   *
   * @param appName
   * @param dbHandleName
   * @param tableId
   * @param rowId
   * @param rowETag
   * @param syncState - the SyncState.name()
   */
  void updateRowETagAndSyncState(in String appName, in OdkDbHandle dbHandleName,
      in String tableId, in String rowId, in String rowETag, in String syncState);

  /**
   * Return the row(s) for the given tableId and rowId. If the row has
   * checkpoints or conflicts, the returned UserTable will have more than one
   * Row returned. Otherwise, it will contain a single row.
   *
   * @param appName
   * @param dbHandleName
   * @param tableId
   * @param orderedDefns
   * @param rowId
   * @return one or more rows (depending upon sync conflict and edit checkpoint states)
   */
  UserTable getRowsWithId(in String appName, in OdkDbHandle dbHandleName,
      in String tableId, in OrderedColumns orderedDefns, in String rowId);

  /**
   * Return the row with the most recent changes for the given tableId and rowId.
   * If the row has conflicts, it throws an exception. Otherwise, it returns the
   * most recent checkpoint or non-checkpoint value; it will contain a single row.
   *
   * @param appName
   * @param dbHandleName
   * @param tableId
   * @param orderedDefns
   * @param rowId
   * @return
   */
  UserTable getMostRecentRowWithId(in String appName,
      in OdkDbHandle dbHandleName,
      in String tableId, in OrderedColumns orderedDefns, in String rowId);

  /**
   * A combination of:
   *
   * deleteServerConflictRowWithId(appName, db, tableId, rowId)
   * placeRowIntoConflict(appName, db, tableId, rowId, localRowConflictType)
   * and, for the values which are the server row changes:
   * insertDataIntoExistingDBTableWithId( appName, db, tableId, orderedColumns, values, rowId)
   *
   * Change the conflictType for the given row from null (not in conflict) to
   * the specified one.
   *
   * @param appName
   * @param dbHandleName
   * @param tableId
   * @param orderedColumns
   * @param cvValues
   * @param rowId
   * @param localRowConflictType
   *          expected to be one of ConflictType.LOCAL_DELETED_OLD_VALUES (0) or
   *          ConflictType.LOCAL_UPDATED_UPDATED_VALUES (1)
   */
  UserTable placeRowIntoServerConflictWithId(in String appName, in OdkDbHandle dbHandleName,
      in String tableId, in OrderedColumns orderedColumns, in ContentValues cvValues,
      in String rowId, in int localRowConflictType);

  /**
   * Inserts a checkpoint row for the given rowId in the tableId. Checkpoint
   * rows are created by ODK Survey to hold intermediate values during the
   * filling-in of the form. They act as restore points in the Survey, should
   * the application die.
   *
   * @param appName
   * @param dbHandleName
   * @param tableId
   * @param orderedColumns
   * @param cvValues
   * @param rowId
   * @return single-row table with the content of the inserted checkpoint
   */
  UserTable insertCheckpointRowWithId(in String appName, in OdkDbHandle dbHandleName,
      in String tableId, in OrderedColumns orderedColumns, in ContentValues cvValues, in String rowId);

  /**
   * Insert the given rowId with the values in the cvValues. If certain metadata
   * values are not specified in the cvValues, then suitable default values may
   * be supplied for them.
   *
   * If a row with this rowId and certain matching metadata fields is present,
   * then an exception is thrown.
   *
   * @param appName
   * @param dbHandleName
   * @param tableId
   * @param orderedColumns
   * @param cvValues
   * @param rowId
   * @return single-row table with the content of the inserted row
   */
  UserTable insertRowWithId(in String appName, in OdkDbHandle dbHandleName,
  	  in String tableId, in OrderedColumns orderedColumns, in ContentValues cvValues, in String rowId);


  /**
   * Delete any checkpoint rows for the given rowId in the tableId. Checkpoint
   * rows are created by ODK Survey to hold intermediate values during the
   * filling-in of the form. They act as restore points in the Survey, should
   * the application die.
   *
   * @param appName
   * @param dbHandleName
   * @param tableId
   * @param orderedColumns
   * @param rowId
   */
  UserTable deleteAllCheckpointRowsWithId(in String appName, in OdkDbHandle dbHandleName,
      in String tableId, in OrderedColumns orderedColumns, in String rowId);

  /**
   * Delete any checkpoint rows for the given rowId in the tableId. Checkpoint
   * rows are created by ODK Survey to hold intermediate values during the
   * filling-in of the form. They act as restore points in the Survey, should
   * the application die.
   *
   * @param appName
   * @param dbHandleName
   * @param tableId
   * @param orderedColumns
   * @param rowId
   */
  UserTable deleteLastCheckpointRowWithId(in String appName, in OdkDbHandle dbHandleName,
      in String tableId, in OrderedColumns orderedColumns, in String rowId);

 /**
   * Delete the specified rowId in this tableId. Deletion respects sync
   * semantics. If the row is in the SyncState.new_row state, then the row and
   * its associated file attachments are immediately deleted. Otherwise, the row
   * is placed into the SyncState.deleted state and will be retained until the
   * device can delete the record on the server.
   * <p>
   * If you need to immediately delete a record that would otherwise sync to the
   * server, call updateRowETagAndSyncState(...) to set the row to
   * SyncState.new_row, and then call this method and it will be immediately
   * deleted (in this case, unless the record on the server was already deleted,
   * it will remain and not be deleted during any subsequent synchronizations).
   *
   * @param appName
   * @param dbHandleName
   * @param tableId
   * @param orderedColumns
   * @param rowId
   */
  UserTable deleteRowWithId(in String appName, in OdkDbHandle dbHandleName,
      in String tableId, in OrderedColumns orderedColumns, in String rowId);


  /**
   * Update all rows for the given rowId to SavepointType 'INCOMPLETE' and
   * remove all but the most recent row. When used with a rowId that has
   * checkpoints, this updates to the most recent checkpoint and removes any
   * earlier checkpoints, incomplete or complete savepoints. Otherwise, it has
   * the general effect of resetting the rowId to an INCOMPLETE state.
   * 
   * @param appName
   * @param dbHandleName
   * @param tableId
   * @param columnDefns
   * @param cvValues
   * @param rowId
   * @return single-row table with the content of the saved-as-incomplete row
   */
  UserTable saveAsIncompleteMostRecentCheckpointRowWithId(
  	  in String appName, in OdkDbHandle dbHandleName,
      in String tableId, in OrderedColumns columnDefns, in ContentValues cvValues, in String rowId);

 /**
   * Update all rows for the given rowId to SavepointType 'INCOMPLETE' and
   * remove all but the most recent row. When used with a rowId that has
   * checkpoints, this updates to the most recent checkpoint and removes any
   * earlier checkpoints, incomplete or complete savepoints. Otherwise, it has
   * the general effect of resetting the rowId to an INCOMPLETE state.
   *
   * @param appName
   * @param dbHandleName
   * @param tableId
   * @param columnDefns
   * @param cvValues
   * @param rowId
   * @return single-row table with the content of the saved-as-incomplete row
   */
  UserTable saveAsCompleteMostRecentCheckpointRowWithId(
  	  in String appName, in OdkDbHandle dbHandleName,
      in String tableId, in OrderedColumns columnDefns, in ContentValues cvValues, in String rowId);

  /**
   * Update the given rowId with the values in the cvValues. If certain metadata
   * values are not specified in the cvValues, then suitable default values may
   * be supplied for them. Furthermore, if the cvValues do not specify certain
   * metadata fields, then an exception may be thrown if there are more than one
   * row matching this rowId.
   * 
   * @param appName
   * @param dbHandleName
   * @param tableId
   * @param orderedColumns
   * @param cvValues
   * @param rowId
   * @return single-row table with the content of the saved-as-incomplete row
   */
  UserTable updateRowWithId(in String appName, in OdkDbHandle dbHandleName,
      in String tableId,
      in OrderedColumns orderedColumns, in ContentValues cvValues, in String rowId);

  /**
   * Delete the local and server conflict records to resolve a server conflict
   *
   * A combination of primitive actions, all performed in one transaction:
   *
   * // delete the record of the server row
   * deleteServerConflictRowWithId(appName, dbHandleName, tableId, rowId);
   *
   * // move the local record into the 'new_row' sync state
   * // so it can be physically deleted.
   * updateRowETagAndSyncState(appName, dbHandleName, tableId, rowId, null,
   *                           SyncState.new_row.name());
   * // move the local conflict back into the normal (null) state
   * deleteRowWithId(appName, dbHandleName, tableId, rowId);
   *
   * @param appName
   * @param dbHandleName
   * @param tableId
   * @param rowId
   */
  void resolveServerConflictWithDeleteRowWithId(in String appName,
        in OdkDbHandle dbHandleName, in String tableId,
  	    in String rowId);

  /**
   * Resolve the server conflict by taking the local changes.
   * If the local changes are to delete this record, the record will be deleted
   * upon the next successful sync.
   *
   * @param appName
   * @param dbHandleName
   * @param tableId
   * @param rowId
   */
  void resolveServerConflictTakeLocalRowWithId(in String appName,
        in OdkDbHandle dbHandleName, in String tableId,
  	    in String rowId);

  /**
   * Resolve the server conflict by taking the local changes plus a value map
   * of select server field values.  This map should not update any metadata
   * fields -- it should just contain user data fields.
   *
   * It is an error to call this if the local change is to delete the row.
   *
   * @param appName
   * @param dbHandleName
   * @param tableId
   * @param cvValues
   * @param rowId
   */
  void resolveServerConflictTakeLocalRowPlusServerDeltasWithId(in String appName,
        in OdkDbHandle dbHandleName, in String tableId, in ContentValues cvValues,
  	    in String rowId);

  /**
   * Resolve the server conflict by taking the server changes.  This may delete the local row.
   *
   * @param appName
   * @param dbHandleName
   * @param tableId
   * @param rowId
   */
  void resolveServerConflictTakeServerRowWithId(in String appName,
        in OdkDbHandle dbHandleName, in String tableId,
  	    in String rowId);

  /************************************
   * Sync Communications Tracking Tables.
   *
   * These APIs manipulate the table holding 
   * the most current ETag for any documents
   * transmitted between the server and client.
   *
   * By supplying an If-Modified: ETAG
   * header, the server is able to avoid
   * sending the document to the client
   * and instead return a NOT_MODIFIED
   * response.
   ************************************/

  /**
   * Forget the document ETag values for the given tableId on all servers.
   * Used when deleting a table. Exposed mainly for integration testing.
   *
   * @param appName
   * @param dbHandleName
   * @param tableId
   */
  void deleteAllSyncETagsForTableId(in String appName, in OdkDbHandle dbHandleName, in String tableId);

  /**
   * Forget the document ETag values for everything except the specified Uri.
   * Call this when the server URI we are syncing against has changed.
   *
   * @param appName
   * @param dbHandleName
   * @param verifiedUri (e.g., https://opendatakit-tablesdemo.appspot.com)
   */
  void deleteAllSyncETagsExceptForServer(in String appName, in OdkDbHandle dbHandleName,
  	  in String verifiedUri);

  /**
   * Forget the document ETag values for everything under the specified Uri.
   *
   * @param appName
   * @param dbHandleName
   * @param verifiedUri (e.g., https://opendatakit-tablesdemo.appspot.com)
   */
  void deleteAllSyncETagsUnderServer(in String appName, in OdkDbHandle dbHandleName,
  	  in String verifiedUri);

  /**
   * Get the document ETag values for the given file under the specified Uri.
   * The assumption is that the file system will update the modification timestamp
   * if the file has changed. This eliminates the need for computing an md5
   * hash on files that haven't changed. We can just retrieve that from the database.
   *
   * @param appName
   * @param dbHandleName
   * @param verifiedUri (e.g., https://opendatakit-tablesdemo.appspot.com)
   * @param tableId  (null if an application-level file)
   * @param modificationTimestamp timestamp of last file modification
   */
  String getFileSyncETag(in String appName, in OdkDbHandle dbHandleName,
  	  in String verifiedUri, in String tableId, in long modificationTimestamp);

  /**
   * Get the document ETag values for the given manifest under the specified Uri.
   *
   * @param appName
   * @param dbHandleName
   * @param verifiedUri (e.g., https://opendatakit-tablesdemo.appspot.com)
   * @param tableId  (null if an application-level manifest)
   */
  String getManifestSyncETag(in String appName, in OdkDbHandle dbHandleName,
  	  in String verifiedUri, in String tableId);

  /**
   * Update the document ETag values for the given file under the specified Uri.
   * The assumption is that the file system will update the modification timestamp
   * if the file has changed. This eliminates the need for computing an md5
   * hash on files that haven't changed. We can just retrieve that from the database.
   *
   * @param appName
   * @param dbHandleName
   * @param verifiedUri (e.g., https://opendatakit-tablesdemo.appspot.com)
   * @param tableId  (null if an application-level file)
   * @param modificationTimestamp timestamp of last file modification
   * @param eTag
   */
  void updateFileSyncETag(in String appName, in OdkDbHandle dbHandleName,
  	  in String verifiedUri, in String tableId, in long modificationTimestamp,
  	  in String eTag);

  /**
   * Update the document ETag values for the given manifest under the specified Uri.
   *
   * @param appName
   * @param dbHandleName
   * @param verifiedUri (e.g., https://opendatakit-tablesdemo.appspot.com)
   * @param tableId  (null if an application-level manifest)
   * @param eTag
   */
  void updateManifestSyncETag(in String appName, in OdkDbHandle dbHandleName,
  	  in String verifiedUri, in String tableId, in String eTag);
}
