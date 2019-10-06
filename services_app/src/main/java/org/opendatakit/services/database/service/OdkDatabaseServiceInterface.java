/*
 * Copyright (C) 2015 University of Washington
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.opendatakit.services.database.service;

import android.content.ContentValues;
import android.os.ParcelUuid;
import android.os.Parcelable;
import android.os.RemoteException;
import android.util.Log;

import org.opendatakit.database.DatabaseConstants;
import org.opendatakit.database.data.BaseTable;
import org.opendatakit.database.data.ColumnList;
import org.opendatakit.database.data.KeyValueStoreEntry;
import org.opendatakit.database.data.OrderedColumns;
import org.opendatakit.database.data.TableDefinitionEntry;
import org.opendatakit.database.data.TableMetaDataEntries;
import org.opendatakit.database.queries.BindArgs;
import org.opendatakit.database.queries.QueryBounds;
import org.opendatakit.database.service.DbChunk;
import org.opendatakit.database.service.DbHandle;
import org.opendatakit.database.service.IDbInterface;
import org.opendatakit.database.service.TableHealthInfo;
import org.opendatakit.database.utilities.DbChunkUtil;
import org.opendatakit.exception.ActionNotAuthorizedException;
import org.opendatakit.logging.WebLogger;
import org.opendatakit.services.database.AndroidConnectFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

class OdkDatabaseServiceInterface extends IDbInterface.Stub {

  private static final String TAG = OdkDatabaseServiceInterface.class.getSimpleName();

  /**
   *
   */
  private final OdkDatabaseService odkDatabaseService;
  private final OdkDatabaseServiceImpl odkDatabaseServiceImpl;

  /**
   * @param odkDatabaseService -- service under which this interface was created
   */
  public OdkDatabaseServiceInterface(OdkDatabaseService odkDatabaseService) {
    this.odkDatabaseService = odkDatabaseService;
    // Used to ensure that the singleton has been initialized properly
    AndroidConnectFactory.configure();

    this.odkDatabaseServiceImpl = new OdkDatabaseServiceImpl(odkDatabaseService);
  }

  private IllegalStateException createWrappingRemoteException(String appName,
                                                              DbHandle dbHandleName,
                                                              String methodName, Throwable e) {
    String msg = e.getLocalizedMessage();
    if (msg == null) {
      msg = e.getMessage();
    }
    if (msg == null) {
      msg = e.toString();
    }
    msg = "org.opendatakit|" + e.getClass().getName() + ": " + msg;
    WebLogger.getLogger(appName)
        .e(methodName, msg +
            ((dbHandleName != null) ? (" dbHandle: " + dbHandleName.getDatabaseHandle()) : ""));
    WebLogger.getLogger(appName).printStackTrace(e);
    return new IllegalStateException(msg);
  }

  /**
   * Return the active user or "anonymous" if the user
   * has not been authenticated against the server.
   *
   * @param appName
   *
   * @return the user reported from the server or "anonymous" if
   * server authentication has not been completed.
   */
  @Override
  public String getActiveUser(String appName) {
    try {
      return odkDatabaseServiceImpl.getActiveUser(appName);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, null, "getActiveUser", e);
    }
  }

  /**
   * Return the roles of a verified username or google account.
   * If the username or google account have not been verified,
   * or if the server settings specify to use an anonymous user,
   * then return an empty string.
   *
   * @param appName
   *
   * @return empty string or JSON serialization of an array of ROLES. See RoleConsts for possible values.
   */
  @Override public String getRolesList(String appName) throws RemoteException {
    try {
      return odkDatabaseServiceImpl.getRolesList(appName);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, null, "getRolesList", e);
    }
  }

  /**
   * Return the current user's default group.
   * This will be an empty string if the server does not support user-defined groups.
   *
   * @return empty string or the name of the default group.
   */
  @Override public String getDefaultGroup(String appName) throws RemoteException {
    try {
      return odkDatabaseServiceImpl.getDefaultGroup(appName);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, null, "getDefaultGroup", e);
    }
  }

  /**
   * Return the users configured on the server if the current
   * user is verified to have Tables Super-user, Administer Tables or
   * Site Administrator roles. Otherwise, returns information about
   * the current user. If the user is syncing anonymously with the
   * server, this returns an empty string.
   *
   * @param appName
   *
   * @return null or JSON serialization of an array of objects
   * structured as { "user_id": "...", "full_name": "...", "roles": ["...",...] }
   */
  @Override public DbChunk getUsersList(String appName) throws RemoteException {
    try {
      return getAndCacheChunksAllowNull(odkDatabaseServiceImpl.getUsersList(appName));
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, null, "getUsersList", e);
    }
  }

  @Override public DbHandle openDatabase(String appName) throws RemoteException {

    OdkDatabaseService.possiblyWaitForDatabaseServiceDebugger();

    try {
      return odkDatabaseServiceImpl.openDatabase(appName);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, null, "openDatabase", e);
    }
  }

  @Override public void closeDatabase(String appName, DbHandle dbHandleName)
      throws RemoteException {

    try {
      odkDatabaseServiceImpl.closeDatabase(appName, dbHandleName);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "closeDatabase", e);
    }
  }

  /**
   * Create a local only table and prepend the given id with an "L_"
   *
   * @param appName
   * @param dbHandleName
   * @param tableId
   * @param columns
   * @return
   */
  @Override public DbChunk createLocalOnlyTableWithColumns(String appName, DbHandle dbHandleName,
                                                             String tableId, ColumnList columns) throws RemoteException {

    try {
      OrderedColumns results = odkDatabaseServiceImpl.createLocalOnlyTableWithColumns(appName,
          dbHandleName, tableId, columns);
      return getAndCacheChunks(results);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "createLocalOnlyTableWithColumns", e);
    }
  }

  /**
   * Drop the given local only table
   *
   * @param appName
   * @param dbHandleName
   * @param tableId
   */
  @Override public void deleteLocalOnlyTable(String appName, DbHandle dbHandleName,
      String tableId) throws RemoteException {

    try {
      odkDatabaseServiceImpl.deleteLocalOnlyTable(appName, dbHandleName, tableId);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "deleteLocalOnlyTable", e);
    }
  }

  /**
   * Insert a row into a local only table
   *
   * @param appName
   * @param dbHandleName
   * @param tableId
   * @param rowValues
   * @throws ActionNotAuthorizedException
   */
  @Override
  public void insertLocalOnlyRow(String appName, DbHandle dbHandleName, String tableId,
      ContentValues rowValues) throws RemoteException {

    try {
      odkDatabaseServiceImpl.insertLocalOnlyRow(appName, dbHandleName, tableId, rowValues);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "insertLocalOnlyRow", e);
    }
  }

  /**
   * Update a row in a local only table
   *
   * @param appName
   * @param dbHandleName
   * @param tableId
   * @param rowValues
   * @param whereClause
   * @param sqlBindArgs
   * @throws ActionNotAuthorizedException
   */
  @Override public void updateLocalOnlyRow(String appName, DbHandle dbHandleName, String tableId,
      ContentValues rowValues, String whereClause, BindArgs sqlBindArgs)
      throws RemoteException {

    try {
      odkDatabaseServiceImpl.updateLocalOnlyRows(appName, dbHandleName, tableId, rowValues,
          whereClause, sqlBindArgs);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "updateLocalOnlyRow", e);
    }
  }

  /**
   * Delete a row in a local only table
   *
   * @param appName
   * @param dbHandleName
   * @param tableId
   * @param whereClause
   * @param sqlBindArgs
   * @throws ActionNotAuthorizedException
   */
  @Override public void deleteLocalOnlyRow(String appName, DbHandle dbHandleName, String tableId,
      String whereClause, BindArgs sqlBindArgs) throws RemoteException {

    try {
      odkDatabaseServiceImpl.deleteLocalOnlyRows(appName, dbHandleName, tableId, whereClause,
          sqlBindArgs);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "deleteLocalOnlyRow", e);
    }
  }

  /**
   * SYNC Only. ADMIN Privileges
   *
   * @param appName
   * @param dbHandleName
   * @param tableId
   * @param schemaETag
   * @param tableInstanceFilesUri
   * @throws RemoteException
     */
  @Override public void privilegedServerTableSchemaETagChanged(String appName, DbHandle
      dbHandleName,
      String tableId, String schemaETag, String tableInstanceFilesUri) throws RemoteException {

    try {
      odkDatabaseServiceImpl.privilegedServerTableSchemaETagChanged(appName, dbHandleName,
          tableId, schemaETag, tableInstanceFilesUri);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "privilegedServerTableSchemaETagChanged", e);
    }
  }

  /**
   * Compute the app-global choiceListId for this choiceListJSON
   * and register the tuple of (choiceListId, choiceListJSON).
   * Return choiceListId.
   *
   * @param appName -- application name
   * @param dbHandleName -- database handle
   * @param choiceListJSON -- the actual JSON choice list text.
   * @return choiceListId -- the unique code mapping to the choiceListJSON
   */
  @Override public String setChoiceList(String appName, DbHandle dbHandleName,
      String choiceListJSON) throws RemoteException {

    try {
      return odkDatabaseServiceImpl.setChoiceList(appName, dbHandleName, choiceListJSON);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "setChoiceList", e);
    }
  }

  /**
   * Return the choice list JSON corresponding to the choiceListId
   *
   * @param appName -- application name
   * @param dbHandleName -- database handle
   * @param choiceListId -- the md5 hash of the choiceListJSON
   * @return choiceListJSON -- the actual JSON choice list text.
   */
  @Override public DbChunk getChoiceList(String appName, DbHandle dbHandleName,
      String choiceListId) throws RemoteException {

    try {
      return getAndCacheChunksAllowNull(odkDatabaseServiceImpl.getChoiceList(appName, dbHandleName,
          choiceListId));
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "getChoiceList", e);
    }
  }

  @Override public DbChunk createOrOpenTableWithColumns(String appName,
      DbHandle dbHandleName, String tableId, ColumnList columns) throws RemoteException {

    try {
      OrderedColumns results = odkDatabaseServiceImpl.createOrOpenTableWithColumns(appName,
          dbHandleName, tableId, columns);
      return getAndCacheChunks(results);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "createOrOpenTableWithColumns", e);
    }
  }

  @Override public DbChunk createOrOpenTableWithColumnsAndProperties(String appName,
      DbHandle dbHandleName, String tableId, ColumnList columns,
      List<KeyValueStoreEntry> metaData, boolean clear) throws RemoteException {

    try {
      OrderedColumns results = odkDatabaseServiceImpl.createOrOpenTableWithColumnsAndProperties
          (appName, dbHandleName, tableId, columns, metaData, clear);
      return getAndCacheChunks(results);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "createOrOpenTableWithColumnsAndProperties", e);
    }
  }

  @Override public DbChunk deleteAllCheckpointRowsWithId(String appName, DbHandle dbHandleName,
      String tableId, String rowId) throws RemoteException {

    try {
      BaseTable t = odkDatabaseServiceImpl.deleteAllCheckpointRowsWithId(appName, dbHandleName,
          tableId, rowId);
      return getAndCacheChunks(t);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "deleteAllCheckpointRowsWithId", e);
    }
  }

  @Override public DbChunk deleteLastCheckpointRowWithId(String appName, DbHandle dbHandleName,
      String tableId, String rowId) throws RemoteException {

    try {
      BaseTable t = odkDatabaseServiceImpl.deleteLastCheckpointRowWithId(appName, dbHandleName,
          tableId, rowId);
      return getAndCacheChunks(t);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "deleteLastCheckpointRowWithId", e);
    }
  }

  @Override public void deleteTableAndAllData(String appName, DbHandle dbHandleName,
      String tableId) throws RemoteException {

    try {
      odkDatabaseServiceImpl.deleteTableAndAllData(appName, dbHandleName, tableId);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "deleteTableAndAllData", e);
    }
  }

  @Override public boolean rescanTableFormDefs(String appName, DbHandle dbHandleName,
                                              String tableId) throws RemoteException {

    try {
      return odkDatabaseServiceImpl.rescanTableFormDefs(appName, dbHandleName, tableId);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "rescanTableFormDefs", e);
    }
  }

  @Override public void deleteTableMetadata(String appName, DbHandle dbHandleName,
      String tableId, String partition, String aspect, String key) throws RemoteException {

    try {
      odkDatabaseServiceImpl.deleteTableMetadata(appName, dbHandleName, tableId, partition,
          aspect, key);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "deleteTableMetadata", e);
    }
  }

  @Override public DbChunk deleteRowWithId(String appName, DbHandle dbHandleName,
      String tableId, String rowId) throws RemoteException {

    try {
      BaseTable t = odkDatabaseServiceImpl.deleteRowWithId(appName, dbHandleName, tableId, rowId);
      return getAndCacheChunks(t);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "deleteRowWithId", e);
    }
  }

  /**
   * SYNC Only. ADMIN Privileges!
   *
   * @param appName
   * @param dbHandleName
   * @param tableId
   * @param rowId
   * @return
     * @throws RemoteException
     */
  @Override public DbChunk privilegedDeleteRowWithId(String appName,
                                              DbHandle dbHandleName,
                                              String tableId, String rowId) throws RemoteException {

    try {
      BaseTable t = odkDatabaseServiceImpl.privilegedDeleteRowWithId(appName, dbHandleName,
          tableId, rowId);
      return getAndCacheChunks(t);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "privilegedDeleteRowWithId", e);
    }
  }

  @Override public DbChunk getAdminColumns() throws RemoteException {

    String[] results = odkDatabaseServiceImpl.getAdminColumns();
    return getAndCacheChunks(results);
  }

  @Override public DbChunk getAllColumnNames(String appName, DbHandle dbHandleName,
      String tableId) throws RemoteException {

    try {
      String[] results = odkDatabaseServiceImpl.getAllColumnNames(appName, dbHandleName, tableId);
      return getAndCacheChunks(results);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "getAllColumnNames", e);
    }
  }

  @Override public DbChunk getAllTableIds(String appName, DbHandle dbHandleName)
      throws RemoteException {

    try {
      List<String> results = odkDatabaseServiceImpl.getAllTableIds(appName, dbHandleName);
      return getAndCacheChunks((Serializable)results);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "getAllTableIds", e);
    }
  }

  @Override public DbChunk getRowsWithId(String appName,
      DbHandle dbHandleName, String tableId, String rowId)
      throws RemoteException {

    try {
      BaseTable results = odkDatabaseServiceImpl.getRowsWithId(appName, dbHandleName, tableId,
          rowId);
      return getAndCacheChunks(results);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "getRowsWithId", e);
    }
  }

  @Override public DbChunk privilegedGetRowsWithId(String appName,
      DbHandle dbHandleName, String tableId, String rowId)
      throws RemoteException {

    try {
      BaseTable results = odkDatabaseServiceImpl.privilegedGetRowsWithId(appName, dbHandleName,
          tableId, rowId);
      return getAndCacheChunks(results);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "privilegedGetRowsWithId", e);
    }
  }

  @Override
  public DbChunk getMostRecentRowWithId(String appName, DbHandle dbHandleName, String
      tableId,
      String rowId)
      throws RemoteException {

    try {
      BaseTable results = odkDatabaseServiceImpl.getMostRecentRowWithId(appName, dbHandleName,
          tableId, rowId);
      return getAndCacheChunks(results);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "getMostRecentRowWithId", e);
    }
  }

  @Override public DbChunk getTableMetadata(String appName,
      DbHandle dbHandleName, String tableId, String partition, String aspect, String key)
      throws RemoteException {

    try {
      TableMetaDataEntries kvsEntries = odkDatabaseServiceImpl.getTableMetadata(appName,
          dbHandleName, tableId, partition, aspect, key);
      return getAndCacheChunks(kvsEntries);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "getTableMetadata", e);
    }
  }

  @Override
  public DbChunk getTableMetadataIfChanged(String appName, DbHandle dbHandleName, String tableId,
      String revId) throws RemoteException {

    try {
      TableMetaDataEntries kvsEntries = odkDatabaseServiceImpl.getTableMetadataIfChanged(appName,
          dbHandleName, tableId, revId);
      return getAndCacheChunks(kvsEntries);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "getTableMetadata", e);
    }
  }

  @Override public DbChunk getTableHealthStatus(String appName,
      DbHandle dbHandleName, String tableId) throws RemoteException {

    try {
      Parcelable healthInfo =
          odkDatabaseServiceImpl.getTableHealthStatus(appName, dbHandleName, tableId);
      return getAndCacheChunks(healthInfo);
    } catch (Throwable t) {
      throw createWrappingRemoteException(appName, dbHandleName, "getTableHealthStatus", t);
    }
  }

  @Override public DbChunk getTableHealthStatuses(String appName,
      DbHandle dbHandleName) throws RemoteException {

    try {
      ArrayList<TableHealthInfo> problems =
          odkDatabaseServiceImpl.getTableHealthStatuses(appName, dbHandleName);
      return getAndCacheChunks(problems);
    } catch (Throwable t) {
      throw createWrappingRemoteException(appName, dbHandleName, "getTableHealthStatuses", t);
    }
  }

  @Override public DbChunk getExportColumns() throws RemoteException {

    String[] results = odkDatabaseServiceImpl.getExportColumns();
    return getAndCacheChunks(results);
  }

  @Override public String getSyncState(String appName, DbHandle dbHandleName, String tableId,
      String rowId) throws RemoteException {

    try {
      return odkDatabaseServiceImpl.getSyncState(appName, dbHandleName, tableId, rowId);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "getSyncState", e);
    }
  }

  @Override public DbChunk getTableDefinitionEntry(String appName,
      DbHandle dbHandleName, String tableId) throws RemoteException {

    try {
      TableDefinitionEntry results = odkDatabaseServiceImpl.getTableDefinitionEntry(appName,
          dbHandleName, tableId);
      return getAndCacheChunks(results);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "getTableDefinitionEntry", e);
    }
  }

  @Override public DbChunk getUserDefinedColumns(String appName, DbHandle dbHandleName,
      String tableId) throws RemoteException {

    try {
      OrderedColumns results = odkDatabaseServiceImpl.getUserDefinedColumns(appName,
          dbHandleName, tableId);
      return getAndCacheChunks(results);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "getUserDefinedColumns", e);
    }
  }

  @Override public boolean hasTableId(String appName, DbHandle dbHandleName, String tableId)
      throws RemoteException {

    try {
      return odkDatabaseServiceImpl.hasTableId(appName, dbHandleName, tableId);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "hasTableId", e);
    }
  }

  @Override public DbChunk insertCheckpointRowWithId(String appName,
      DbHandle dbHandleName, String tableId,
      ContentValues cvValues, String rowId) throws RemoteException {

    try {
      BaseTable t = odkDatabaseServiceImpl.insertCheckpointRowWithId(appName, dbHandleName,
          tableId, cvValues, rowId);
      return getAndCacheChunks(t);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "insertCheckpointRowWithId", e);
    }
  }

  @Override public DbChunk insertRowWithId(String appName,
      DbHandle dbHandleName, String tableId,
      ContentValues cvValues, String rowId) throws RemoteException {

    try {
      BaseTable t = odkDatabaseServiceImpl.insertRowWithId(appName, dbHandleName, tableId,
          cvValues, rowId);
      return getAndCacheChunks(t);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "insertRowWithId", e);
    }
  }

  /**
   * SYNC Only. ADMIN Privileges!
   *
   * @param appName
   * @param dbHandleName
   * @param tableId
   * @param cvValues
   * @param rowId
   * @param asCsvRequestedChange
   * @return
   * @throws RemoteException
   */
  @Override public DbChunk privilegedInsertRowWithId(String appName, DbHandle dbHandleName,
                                                        String tableId, ContentValues cvValues,
                                                     String rowId, boolean asCsvRequestedChange)
      throws RemoteException {
    try {
      BaseTable t = odkDatabaseServiceImpl.privilegedInsertRowWithId(appName, dbHandleName,
          tableId, cvValues, rowId, asCsvRequestedChange);
      return getAndCacheChunks(t);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "privilegedInsertRowWithId", e);
    }
  }

  /**
   * SYNC Only. ADMIN Privileges!
   *
   * @param appName
   * @param dbHandleName
   * @param tableId
   * @param cvValues  server's field values for this row
   * @param rowId
   *          expected to be one of ConflictType.LOCAL_DELETED_OLD_VALUES (0) or
   * @return
   * @throws RemoteException
   */
  @Override public DbChunk privilegedPerhapsPlaceRowIntoConflictWithId(String appName, DbHandle
      dbHandleName, String tableId, ContentValues cvValues, String rowId) throws RemoteException {

    try {
      BaseTable t = odkDatabaseServiceImpl.privilegedPerhapsPlaceRowIntoConflictWithId(appName,
          dbHandleName, tableId, cvValues, rowId);
      return getAndCacheChunks(t);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "privilegedPerhapsPlaceRowIntoConflictWithId", e);
    }
  }

  @Override public DbChunk simpleQuery(String appName, DbHandle dbHandleName,
      String sqlCommand, BindArgs sqlBindArgs, QueryBounds sqlQueryBounds, String tableId) throws
      RemoteException {

    try {
      BaseTable result = odkDatabaseServiceImpl.simpleQuery(appName, dbHandleName, sqlCommand,
          sqlBindArgs, sqlQueryBounds, tableId);
      return getAndCacheChunks(result);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "simpleQuery", e);
    }
  }

  @Override
  public DbChunk privilegedSimpleQuery(String appName, DbHandle dbHandleName,
      String sqlCommand, BindArgs sqlBindArgs, QueryBounds sqlQueryBounds, String tableId) throws RemoteException {

    try {
      BaseTable result = odkDatabaseServiceImpl.privilegedSimpleQuery(appName, dbHandleName,
          sqlCommand, sqlBindArgs, sqlQueryBounds, tableId);
      return getAndCacheChunks(result);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "simpleQuery", e);
    }
  }

  @Override
  public void privilegedExecute(String appName, DbHandle dbHandleName,
      String sqlCommand, BindArgs sqlBindArgs) {

    try {
      odkDatabaseServiceImpl.privilegedExecute(appName, dbHandleName, sqlCommand, sqlBindArgs);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "privilegedExecute", e);
    }
  }

  @Override public void replaceTableMetadata(String appName, DbHandle dbHandleName,
      KeyValueStoreEntry entry) throws RemoteException {

    try {
      odkDatabaseServiceImpl.replaceTableMetadata(appName, dbHandleName, entry);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "replaceTableMetadata", e);
    }
  }

  @Override public void replaceTableMetadataList(String appName, DbHandle dbHandleName,
      String tableId, List<KeyValueStoreEntry> entries, boolean clear) throws RemoteException {

    try {
      odkDatabaseServiceImpl.replaceTableMetadataList(appName, dbHandleName, tableId, entries, clear);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "replaceTableMetadataList", e);
    }
  }

  @Override public void replaceTableMetadataSubList(String appName, DbHandle dbHandleName,
      String tableId, String partition, String aspect, List<KeyValueStoreEntry> entries)
      throws RemoteException {

    try {
      odkDatabaseServiceImpl.replaceTableMetadataSubList(appName, dbHandleName, tableId,
          partition, aspect, entries);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "replaceTableMetadataSubList", e);
    }
  }

  @Override public DbChunk saveAsIncompleteMostRecentCheckpointRowWithId(String appName,
      DbHandle dbHandleName, String tableId, String rowId)
      throws RemoteException {

    try {
      BaseTable t = odkDatabaseServiceImpl.saveAsIncompleteMostRecentCheckpointRowWithId(appName,
          dbHandleName, tableId, rowId);
      return getAndCacheChunks(t);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "saveAsIncompleteMostRecentCheckpointRowWithId", e);
    }
  }

  @Override public DbChunk saveAsCompleteMostRecentCheckpointRowWithId(String appName,
      DbHandle dbHandleName, String tableId, String rowId)
      throws RemoteException {

    try {
      BaseTable t = odkDatabaseServiceImpl.saveAsCompleteMostRecentCheckpointRowWithId(appName,
          dbHandleName, tableId, rowId);
      return getAndCacheChunks(t);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "saveAsCompleteMostRecentCheckpointRowWithId", e);
    }
  }

  /**
   * SYNC Only. ADMIN Privileges
   *
   * @param appName
   * @param dbHandleName
   * @param tableId
   * @param schemaETag
   * @param lastDataETag
   * @throws RemoteException
     */
  @Override public void privilegedUpdateTableETags(String appName, DbHandle dbHandleName,
      String tableId,
      String schemaETag, String lastDataETag) throws RemoteException {

    try {
      odkDatabaseServiceImpl.privilegedUpdateTableETags(appName, dbHandleName, tableId,
          schemaETag, lastDataETag);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "privilegedUpdateTableETags", e);
    }
  }

  /**
   * SYNC Only. ADMIN Privileges
   *
   * @param appName
   * @param dbHandleName
   * @param tableId
   * @throws RemoteException
     */
  @Override public void privilegedUpdateTableLastSyncTime(String appName, DbHandle
      dbHandleName,
      String tableId) throws RemoteException {

    try {
      odkDatabaseServiceImpl.privilegedUpdateTableLastSyncTime(appName, dbHandleName, tableId);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "privilegedUpdateTableLastSyncTime", e);
    }
  }

  @Override public DbChunk updateRowWithId(String appName,
      DbHandle dbHandleName, String tableId,
      ContentValues cvValues, String rowId) throws RemoteException {

    try {
      BaseTable t = odkDatabaseServiceImpl.updateRowWithId(appName, dbHandleName, tableId,
          cvValues, rowId);
      return getAndCacheChunks(t);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "updateRowWithId", e);
    }
  }

  @Override public void resolveServerConflictWithDeleteRowWithId(String appName,
      DbHandle dbHandleName, String tableId, String rowId) throws RemoteException {

    try {
      odkDatabaseServiceImpl.resolveServerConflictWithDeleteRowWithId(appName, dbHandleName,
          tableId, rowId);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "resolveServerConflictWithDeleteRowWithId", e);
    }
  }

  @Override public void resolveServerConflictTakeLocalRowWithId(String appName,
      DbHandle dbHandleName, String tableId, String rowId) throws RemoteException {

    try {
      odkDatabaseServiceImpl.resolveServerConflictTakeLocalRowWithId(appName, dbHandleName,
          tableId, rowId);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "resolveServerConflictTakeLocalRowWithId", e);
    }
  }

  @Override public void resolveServerConflictTakeLocalRowPlusServerDeltasWithId(String appName,
      DbHandle dbHandleName, String tableId, ContentValues cvValues, String rowId)
      throws  RemoteException {

    try {
      odkDatabaseServiceImpl.resolveServerConflictTakeLocalRowPlusServerDeltasWithId(appName,
          dbHandleName, tableId, cvValues, rowId);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "resolveServerConflictTakeLocalRowPlusServerDeltasWithId", e);
    }
  }

  @Override public void resolveServerConflictTakeServerRowWithId(String appName,
      DbHandle dbHandleName, String tableId, String rowId) throws RemoteException {

    try {
      odkDatabaseServiceImpl.resolveServerConflictTakeServerRowWithId(appName, dbHandleName,
          tableId, rowId);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "resolveServerConflictTakeServerRowWithId", e);
    }
  }

  /**
   * SYNC Only. ADMIN Privileges!
   *
   * @param appName
   * @param dbHandleName
   * @param tableId
   * @param rowId
   * @param rowETag
   * @param syncState - the SyncState.name()
     * @throws RemoteException
     */
  @Override public void privilegedUpdateRowETagAndSyncState(String appName, DbHandle
      dbHandleName,
      String tableId, String rowId, String rowETag, String syncState) throws RemoteException {

    try {
      odkDatabaseServiceImpl.privilegedUpdateRowETagAndSyncState(appName, dbHandleName, tableId,
          rowId, rowETag, syncState);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "privilegedUpdateRowETagAndSyncState", e);
    }
  }

  @Override public void deleteAppAndTableLevelManifestSyncETags(String appName, DbHandle dbHandleName)
          throws RemoteException {

    try {
      odkDatabaseServiceImpl.deleteAppAndTableLevelManifestSyncETags(appName, dbHandleName);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName,
              "deleteAppAndTableLevelManifestSyncETags", e);
    }
  }

  @Override public void deleteAllSyncETagsForTableId(String appName, DbHandle dbHandleName,
      String tableId) throws RemoteException {

    try {
      odkDatabaseServiceImpl.deleteAllSyncETagsForTableId(appName, dbHandleName, tableId);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "deleteAllSyncETagsForTableId", e);
    }
  }

  @Override public void deleteAllSyncETagsExceptForServer(String appName, DbHandle dbHandleName,
      String verifiedUri) throws RemoteException {

    try {
      odkDatabaseServiceImpl.deleteAllSyncETagsExceptForServer(appName, dbHandleName, verifiedUri);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "deleteAllSyncETagsExceptForServer", e);
    }
  }

  @Override public void deleteAllSyncETagsUnderServer(String appName, DbHandle dbHandleName,
      String verifiedUri) throws RemoteException {

    try {
      odkDatabaseServiceImpl.deleteAllSyncETagsUnderServer(appName, dbHandleName, verifiedUri);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "deleteAllSyncETagsUnderServer", e);
    }
  }

  @Override public String getFileSyncETag(String appName, DbHandle dbHandleName,
      String verifiedUri, String tableId, long modificationTimestamp) throws RemoteException {

    try {
      return odkDatabaseServiceImpl.getFileSyncETag(appName, dbHandleName, verifiedUri, tableId,
          modificationTimestamp);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "getFileSyncETag", e);
    }
  }

  @Override public String getManifestSyncETag(String appName, DbHandle dbHandleName,
      String verifiedUri, String tableId) throws RemoteException {

    try {
      return odkDatabaseServiceImpl.getManifestSyncETag(appName, dbHandleName, verifiedUri,
          tableId);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "getManifestSyncETag", e);
    }
  }

  @Override public void updateFileSyncETag(String appName, DbHandle dbHandleName,
      String verifiedUri, String tableId, long modificationTimestamp, String eTag)
      throws RemoteException {

    try {
      odkDatabaseServiceImpl.updateFileSyncETag(appName, dbHandleName, verifiedUri, tableId,
          modificationTimestamp, eTag);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "updateFileSyncETag", e);
    }
  }

  @Override public void updateManifestSyncETag(String appName, DbHandle dbHandleName,
      String verifiedUri, String tableId, String eTag) throws RemoteException {

    try {
      odkDatabaseServiceImpl.updateManifestSyncETag(appName, dbHandleName, verifiedUri, tableId,
          eTag);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "updateManifestSyncETag", e);
    }
  }


  @Override public DbChunk getChunk(ParcelUuid chunkID) {
    return odkDatabaseService.removeParceledChunk(chunkID.getUuid());
  }

  private DbChunk getAndCacheChunks(Parcelable data) {
    // Break the results into pieces that will fit over the wire
    List<DbChunk> chunkList = DbChunkUtil.convertToChunks(data, DatabaseConstants.PARCEL_SIZE);

    return getAndCacheChunksHelper(chunkList);
  }

  private DbChunk getAndCacheChunksAllowNull(Serializable data) {
    if ( data == null ) {
      return null;
    }
    return getAndCacheChunks(data);
  }

  private DbChunk getAndCacheChunks(Serializable data) {
    List<DbChunk> chunkList;
    try {
      chunkList = DbChunkUtil.convertToChunks(data, DatabaseConstants.PARCEL_SIZE);
    } catch (IOException e) {
      Log.e(TAG, "Invalid state. Failed to convert chunks");
      return null;
    }

    return getAndCacheChunksHelper(chunkList);
  }

  private DbChunk getAndCacheChunksHelper(List<DbChunk> chunkList) {

    if (chunkList == null || chunkList.size() == 0) {
      Log.e(TAG, "Invalid state. Failed to convert chunks");
      return null;
    }

    // Return the first chunk and store the rest for later retrieval
    DbChunk firstChunk = chunkList.remove(0);

    if (chunkList.size() > 0) {
      odkDatabaseService.putParceledChunks(chunkList);
    }

    return firstChunk;
  }

}
