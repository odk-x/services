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

import org.opendatakit.database.RoleConsts;
import org.opendatakit.aggregate.odktables.rest.SyncState;
import org.opendatakit.database.data.*;
import org.opendatakit.services.database.AndroidConnectFactory;
import org.opendatakit.services.database.OdkConnectionFactorySingleton;
import org.opendatakit.services.database.OdkConnectionInterface;
import org.opendatakit.database.queries.BindArgs;
import org.opendatakit.database.service.AidlDbInterface;
import org.opendatakit.database.service.DbChunk;
import org.opendatakit.database.service.DbHandle;
import org.opendatakit.database.service.TableHealthInfo;
import org.opendatakit.database.service.TableHealthStatus;
import org.opendatakit.exception.ActionNotAuthorizedException;
import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.database.utilities.CursorUtils;
import org.opendatakit.services.database.utlities.ODKDatabaseImplUtils;
import org.opendatakit.services.database.utlities.SyncETagsUtils;
import org.opendatakit.logging.WebLogger;
import org.opendatakit.database.DatabaseConstants;
import org.opendatakit.database.queries.QueryBounds;
import org.opendatakit.database.utilities.DbChunkUtil;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

class OdkDatabaseServiceInterface extends AidlDbInterface.Stub {

  private static final String TAG = OdkDatabaseServiceInterface.class.getSimpleName();

  /**
   *
   */
  private final OdkDatabaseService odkDatabaseService;

  /**
   * @param odkDatabaseService -- service under which this interface was created
   */
  public OdkDatabaseServiceInterface(OdkDatabaseService odkDatabaseService) {
    this.odkDatabaseService = odkDatabaseService;
    // Used to ensure that the singleton has been initialized properly
    AndroidConnectFactory.configure();
  }

  private String getActiveUser(String appName) {
    PropertiesSingleton props =
        CommonToolProperties.get(odkDatabaseService.getApplicationContext(), appName);
    return props.getActiveUser();
  }

  private String getInternalRolesList(String appName) {
    PropertiesSingleton props =
            CommonToolProperties.get(odkDatabaseService.getApplicationContext(), appName);
    return props.getProperty(CommonToolProperties.KEY_ROLES_LIST);
  }

  private String getInternalUsersList(String appName) {
    PropertiesSingleton props =
        CommonToolProperties.get(odkDatabaseService.getApplicationContext(), appName);
    return props.getProperty(CommonToolProperties.KEY_USERS_LIST);
  }

  private String getLocale(String appName) {
    PropertiesSingleton props =
        CommonToolProperties.get(odkDatabaseService.getApplicationContext(), appName);
    return props.getLocale();
  }

  private IllegalStateException createWrappingRemoteException(String appName,
                                                              DbHandle dbHandleName, String methodName, Throwable e) {
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
      return getInternalRolesList(appName);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, null, "getRolesList", e);
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
   * @return empty string or JSON serialization of an array of objects
   * structured as { "user_id": "...", "full_name": "...", "roles": ["...",...] }
   */
  @Override public String getUsersList(String appName) throws RemoteException {
    // TODO: This is generally unbounded in size. Perhaps it should be chunked? (max 2000 users)
    // Realistically, each user would be no more than 500 bytes. 1Mb = 2000 users.
    try {
      return getInternalUsersList(appName);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, null, "getUsersList", e);
    }
  }

  @Override public DbHandle openDatabase(String appName) throws RemoteException {

    OdkDatabaseService.possiblyWaitForDatabaseServiceDebugger();

    OdkConnectionInterface db = null;

    DbHandle dbHandleName = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
        .generateDatabaseServiceDbHandle();
    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      return dbHandleName;
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "openDatabase", e);
    } finally {
      if (db != null) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override public void closeDatabase(String appName, DbHandle dbHandleName)
      throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      boolean first = true;
      while (db != null && db.isOpen() && db.inTransaction()) {
        if (!first) {
          WebLogger.getLogger(appName).e("closeDatabase",
              appName + " " + dbHandleName.getDatabaseHandle() + " aborting transaction!");
        }
        first = false;
        // (presumably) abort the outstanding transaction
        db.endTransaction();
      }
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "closeDatabase", e);
    } finally {
      if (db != null) {
        try {
          // release the reference...
          // this will not close the db handle
          // the AppNameSharedStateContainer still holds a reference.
          db.releaseReference();
        } finally {
          // this will release the database from the AppNameSharedStateContainer...
          // this may also not close the connection -- it may be held open by a cursor
           // try {
              OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().removeConnection(appName,
                  dbHandleName);

           // }
           // Enabling dumpInfo will expose a bug in which
           // an open file handle on a directory that has been deleted
           // will prevent the directory to be created for the WebLogger
           // which will throw an exception
           // finally{
           //    OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().dumpInfo(true);
           // }
        }
      }
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

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      OrderedColumns results = ODKDatabaseImplUtils.get()
          .createLocalOnlyTableWithColumns(db, tableId, columns.getColumns());

      return getAndCacheChunks(results);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if (msg == null)
        msg = e.getMessage();
      if (msg == null)
        msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("createLocalOnlyTableWithColumns",
          appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if (db != null) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
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

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      ODKDatabaseImplUtils.get().deleteLocalOnlyTable(db, tableId);

    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if (msg == null)
        msg = e.getMessage();
      if (msg == null)
        msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("deleteLocalOnlyTable",
          appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if (db != null) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
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

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      ODKDatabaseImplUtils.get().insertLocalOnlyRow(db, tableId, rowValues);

    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if (msg == null)
        msg = e.getMessage();
      if (msg == null)
        msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("insertLocalOnlyRow",
          appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if (db != null) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
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

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      ODKDatabaseImplUtils.get()
          .updateLocalOnlyRow(db, tableId, rowValues, whereClause, sqlBindArgs.bindArgs);

    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if (msg == null)
        msg = e.getMessage();
      if (msg == null)
        msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("updateLocalOnlyRow",
          appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if (db != null) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
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

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      ODKDatabaseImplUtils.get()
          .deleteLocalOnlyRow(db, tableId, whereClause, sqlBindArgs.bindArgs);

    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if (msg == null)
        msg = e.getMessage();
      if (msg == null)
        msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("deleteLocalOnlyRow",
          appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if (db != null) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
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

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      ODKDatabaseImplUtils.get().serverTableSchemaETagChanged(db, tableId, schemaETag,
          tableInstanceFilesUri);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "privilegedServerTableSchemaETagChanged", e);
    } finally {
      if (db != null) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
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

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      return ODKDatabaseImplUtils.get().setChoiceList(db, choiceListJSON);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "setChoiceList", e);
    } finally {
      if (db != null) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
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
  @Override public String getChoiceList(String appName, DbHandle dbHandleName,
      String choiceListId) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      return ODKDatabaseImplUtils.get().getChoiceList(db, choiceListId);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "getChoiceList", e);
    } finally {
      if (db != null) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override public DbChunk createOrOpenTableWithColumns(String appName,
      DbHandle dbHandleName, String tableId, ColumnList columns) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      OrderedColumns results = ODKDatabaseImplUtils.get()
          .createOrOpenTableWithColumns(db, tableId, columns.getColumns());

      return getAndCacheChunks(results);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "createOrOpenTableWithColumns", e);
    } finally {
      if (db != null) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override public DbChunk createOrOpenTableWithColumnsAndProperties(String appName,
      DbHandle dbHandleName, String tableId, ColumnList columns,
      List<KeyValueStoreEntry> metaData, boolean clear) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      OrderedColumns results =
          ODKDatabaseImplUtils.get()
          .createOrOpenTableWithColumnsAndProperties(db, tableId, columns.getColumns(),
              metaData, clear);

      return getAndCacheChunks(results);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "createOrOpenTableWithColumnsAndProperties", e);
    } finally {
      if (db != null) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override public DbChunk deleteAllCheckpointRowsWithId(String appName, DbHandle dbHandleName,
      String tableId, String rowId) throws RemoteException {

    OdkConnectionInterface db = null;

    String activeUser = getActiveUser(appName);
    String rolesList = getInternalRolesList(appName);

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      db.beginTransactionExclusive();
      ODKDatabaseImplUtils.get().deleteAllCheckpointRowsWithId(db, tableId, rowId,
          activeUser, rolesList);
      BaseTable t = ODKDatabaseImplUtils.get().getMostRecentRowWithId(db, tableId, rowId,
          activeUser, rolesList);
      db.setTransactionSuccessful();
      return getAndCacheChunks(t);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "deleteAllCheckpointRowsWithId", e);
    } finally {
      if (db != null) {
        db.endTransaction();
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override public DbChunk deleteLastCheckpointRowWithId(String appName, DbHandle dbHandleName,
      String tableId, String rowId) throws RemoteException {

    OdkConnectionInterface db = null;

    String activeUser = getActiveUser(appName);
    String rolesList = getInternalRolesList(appName);

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      db.beginTransactionExclusive();
      ODKDatabaseImplUtils.get().deleteLastCheckpointRowWithId(db, tableId, rowId,
          activeUser, rolesList);
      BaseTable t = ODKDatabaseImplUtils.get().getMostRecentRowWithId(db, tableId, rowId,
          activeUser, rolesList);
      db.setTransactionSuccessful();
      return getAndCacheChunks(t);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "deleteLastCheckpointRowWithId", e);
    } finally {
      if (db != null) {
        db.endTransaction();
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override public void deleteTableAndAllData(String appName, DbHandle dbHandleName,
      String tableId) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      ODKDatabaseImplUtils.get().deleteTableAndAllData(db, tableId);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "deleteTableAndAllData", e);
    } finally {
      if (db != null) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override public void deleteTableMetadata(String appName, DbHandle dbHandleName,
      String tableId, String partition, String aspect, String key) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      ODKDatabaseImplUtils.get().deleteTableMetadata(db, tableId, partition, aspect, key);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "deleteTableMetadata", e);
    } finally {
      if (db != null) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override public DbChunk deleteRowWithId(String appName, DbHandle dbHandleName,
      String tableId, String rowId) throws RemoteException {

    OdkConnectionInterface db = null;

    String activeUser = getActiveUser(appName);
    String rolesList = getInternalRolesList(appName);

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      db.beginTransactionExclusive();
      ODKDatabaseImplUtils.get().deleteRowWithId(db, tableId, rowId,
          activeUser, rolesList);
      BaseTable t = ODKDatabaseImplUtils.get().getMostRecentRowWithId(db, tableId, rowId,
          activeUser, rolesList);
      db.setTransactionSuccessful();
      return getAndCacheChunks(t);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "deleteRowWithId", e);
    } finally {
      if (db != null) {
        db.endTransaction();
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
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

    OdkConnectionInterface db = null;

    String activeUser = getActiveUser(appName);

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
              .getConnection(appName, dbHandleName);
      db.beginTransactionExclusive();
      ODKDatabaseImplUtils.get().privilegedDeleteRowWithId(db, tableId, rowId, activeUser);
      BaseTable t = ODKDatabaseImplUtils.get().privilegedGetMostRecentRowWithId(db, tableId,
          rowId, activeUser);
      db.setTransactionSuccessful();
      return getAndCacheChunks(t);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "privilegedDeleteRowWithId", e);
    } finally {
      if (db != null) {
        db.endTransaction();
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override public DbChunk getAdminColumns() throws RemoteException {

    List<String> cols = ODKDatabaseImplUtils.get().getAdminColumns();
    String[] results = cols.toArray(new String[cols.size()]);

    return getAndCacheChunks(results);
  }

  @Override public DbChunk getAllColumnNames(String appName, DbHandle dbHandleName,
      String tableId) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      String[] results =  ODKDatabaseImplUtils.get().getAllColumnNames(db, tableId);

      return getAndCacheChunks(results);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "getAllColumnNames", e);
    } finally {
      if (db != null) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }

  }

  @Override public DbChunk getAllTableIds(String appName, DbHandle dbHandleName)
      throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      List<String> results = ODKDatabaseImplUtils.get().getAllTableIds(db);

      return getAndCacheChunks((Serializable)results);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "getAllTableIds", e);
    } finally {
      if (db != null) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override public DbChunk getRowsWithId(String appName,
      DbHandle dbHandleName, String tableId, String rowId)
      throws RemoteException {

    OdkConnectionInterface db = null;

    String activeUser = getActiveUser(appName);
    String rolesList = getInternalRolesList(appName);

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      BaseTable results = ODKDatabaseImplUtils.get()
          .getRowsWithId(db, tableId, rowId, activeUser, rolesList);

      return getAndCacheChunks(results);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "getRowsWithId", e);
    } finally {
      if (db != null) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override public DbChunk privilegedGetRowsWithId(String appName,
      DbHandle dbHandleName, String tableId, String rowId)
      throws RemoteException {

    OdkConnectionInterface db = null;

    String activeUser = getActiveUser(appName);

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      BaseTable results = ODKDatabaseImplUtils.get()
          .privilegedGetRowsWithId(db, tableId, rowId, activeUser);

      return getAndCacheChunks(results);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "privilegedGetRowsWithId", e);
    } finally {
      if (db != null) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override
  public DbChunk getMostRecentRowWithId(String appName, DbHandle dbHandleName, String
      tableId,
      String rowId)
      throws RemoteException {

    OdkConnectionInterface db = null;

    String activeUser = getActiveUser(appName);
    String rolesList = getInternalRolesList(appName);

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      BaseTable results = ODKDatabaseImplUtils.get()
          .getMostRecentRowWithId(db, tableId, rowId, activeUser, rolesList);

      return getAndCacheChunks(results);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "getMostRecentRowWithId", e);
    } finally {
      if (db != null) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override public DbChunk getTableMetadata(String appName,
      DbHandle dbHandleName, String tableId, String partition, String aspect, String key)
      throws RemoteException {

    OdkConnectionInterface db = null;

    TableMetaDataEntries kvsEntries = null;
    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      kvsEntries = ODKDatabaseImplUtils.get()
          .getTableMetadata(db, tableId, partition, aspect, key);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "getTableMetadata", e);
    } finally {
      if (db != null) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }

    return getAndCacheChunks(kvsEntries);
  }

  @Override
  public DbChunk getTableMetadataIfChanged(String appName, DbHandle dbHandleName, String tableId,
      String revId) throws RemoteException {

    OdkConnectionInterface db = null;

    TableMetaDataEntries kvsEntries = null;
    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      String currentRevId = ODKDatabaseImplUtils.get().getTableDefinitionRevId(db, tableId);
      if (revId != null && revId.equals(currentRevId)) {
        kvsEntries = new TableMetaDataEntries(tableId, revId);
      } else {
        kvsEntries = ODKDatabaseImplUtils.get().getTableMetadata(db, tableId, null, null, null);
      }
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "getTableMetadata", e);
    } finally {
      if (db != null) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }

    return getAndCacheChunks(kvsEntries);
  }

  @Override public DbChunk getTableHealthStatuses(String appName,
      DbHandle dbHandleName) throws RemoteException {

    long now = System.currentTimeMillis();
    WebLogger.getLogger(appName)
        .i("getTableHealthStatuses", appName + " " + dbHandleName.getDatabaseHandle() + " " +
            "getTableHealthStatuses -- searching for conflicts and checkpoints ");

    ArrayList<TableHealthInfo> problems = new ArrayList<>();
    OdkConnectionInterface db = null;
    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      ArrayList<String> tableIds = ODKDatabaseImplUtils.get().getAllTableIds(db);

      for (String tableId : tableIds) {
        int health = ODKDatabaseImplUtils.get().getTableHealth(db, tableId);
        if (health != CursorUtils.TABLE_HEALTH_IS_CLEAN) {
          TableHealthStatus status = TableHealthStatus.TABLE_HEALTH_IS_CLEAN;
          switch (health) {
          case CursorUtils.TABLE_HEALTH_HAS_CHECKPOINTS:
            status = TableHealthStatus.TABLE_HEALTH_HAS_CHECKPOINTS;
            break;
          case CursorUtils.TABLE_HEALTH_HAS_CONFLICTS:
            status = TableHealthStatus.TABLE_HEALTH_HAS_CONFLICTS;
            break;
          case CursorUtils.TABLE_HEALTH_HAS_CHECKPOINTS_AND_CONFLICTS:
            status = TableHealthStatus.TABLE_HEALTH_HAS_CHECKPOINTS_AND_CONFLICTS;
            break;
          }
          TableHealthInfo info = new TableHealthInfo(tableId, status);
          problems.add(info);
        }
      }

      long elapsed = System.currentTimeMillis() - now;
      WebLogger.getLogger(appName)
          .i("getTableHealthStatuses", appName + " " + dbHandleName.getDatabaseHandle() + " " +
              "getTableHealthStatuses -- full table scan completed: " + Long.toString(elapsed)
              + " ms");

      return getAndCacheChunks(problems);
    } catch (Throwable t) {
      throw createWrappingRemoteException(appName, dbHandleName, "getTableHealthStatuses", t);
    } finally {
      if (db != null) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override public DbChunk getExportColumns() throws RemoteException {
    List<String> exports = ODKDatabaseImplUtils.get().getExportColumns();
    String[] results =  exports.toArray(new String[exports.size()]);

    return getAndCacheChunks(results);
  }

  @Override public String getSyncState(String appName, DbHandle dbHandleName, String tableId,
      String rowId) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      SyncState state = ODKDatabaseImplUtils.get().getSyncState(db, tableId, rowId);
      return state.name();
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "getSyncState", e);
    } finally {
      if (db != null) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override public DbChunk getTableDefinitionEntry(String appName,
      DbHandle dbHandleName, String tableId) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      TableDefinitionEntry results = ODKDatabaseImplUtils.get()
          .getTableDefinitionEntry(db, tableId);

      return getAndCacheChunks(results);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "getTableDefinitionEntry", e);
    } finally {
      if (db != null) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override public DbChunk getUserDefinedColumns(String appName, DbHandle dbHandleName,
      String tableId) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      OrderedColumns results = ODKDatabaseImplUtils.get()
          .getUserDefinedColumns(db, tableId);

      return getAndCacheChunks(results);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "getUserDefinedColumns", e);
    } finally {
      if (db != null) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override public boolean hasTableId(String appName, DbHandle dbHandleName, String tableId)
      throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      return ODKDatabaseImplUtils.get().hasTableId(db, tableId);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "hasTableId", e);
    } finally {
      if (db != null) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override public DbChunk insertCheckpointRowWithId(String appName,
      DbHandle dbHandleName, String tableId, OrderedColumns orderedColumns,
      ContentValues cvValues, String rowId) throws RemoteException {

    OdkConnectionInterface db = null;

    String activeUser = getActiveUser(appName);
    String rolesList = getInternalRolesList(appName);
    String locale = getLocale(appName);

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      db.beginTransactionExclusive();
      ODKDatabaseImplUtils.get()
          .insertCheckpointRowWithId(db, tableId, orderedColumns, cvValues, rowId, activeUser,
              rolesList, locale);
      BaseTable t = ODKDatabaseImplUtils.get().getMostRecentRowWithId(db, tableId, rowId,
          activeUser, rolesList);
      db.setTransactionSuccessful();
      return getAndCacheChunks(t);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "insertCheckpointRowWithId", e);
    } finally {
      if (db != null) {
        db.endTransaction();
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override public DbChunk insertRowWithId(String appName,
      DbHandle dbHandleName, String tableId, OrderedColumns orderedColumns,
      ContentValues cvValues, String rowId) throws RemoteException {

    OdkConnectionInterface db = null;

    String activeUser = getActiveUser(appName);
    String rolesList = getInternalRolesList(appName);
    String locale = getLocale(appName);

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      db.beginTransactionExclusive();
      ODKDatabaseImplUtils.get()
          .insertRowWithId(db, tableId, orderedColumns, cvValues, rowId, activeUser, rolesList, locale);
      BaseTable t = ODKDatabaseImplUtils.get()
          .getMostRecentRowWithId(db, tableId, rowId, activeUser, rolesList);
      db.setTransactionSuccessful();
      return getAndCacheChunks(t);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "insertRowWithId", e);
    } finally {
      if (db != null) {
        db.endTransaction();
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  /**
   * SYNC Only. ADMIN Privileges!
   *
   * @param appName
   * @param dbHandleName
   * @param tableId
   * @param orderedColumns
   * @param cvValues
   * @param rowId
   * @param asCsvRequestedChange
   * @return
   * @throws RemoteException
   */
  @Override public DbChunk privilegedInsertRowWithId(String appName, DbHandle dbHandleName,
                                                        String tableId, OrderedColumns orderedColumns, ContentValues cvValues, String rowId, boolean asCsvRequestedChange)
      throws RemoteException {

    OdkConnectionInterface db = null;

    String activeUser = getActiveUser(appName);
    String locale = getLocale(appName);

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      db.beginTransactionExclusive();
      ODKDatabaseImplUtils.get()
          .privilegedInsertRowWithId(db, tableId, orderedColumns, cvValues, rowId, activeUser,
              locale, asCsvRequestedChange);
      BaseTable t = ODKDatabaseImplUtils.get().privilegedGetMostRecentRowWithId(db, tableId,
          rowId, activeUser);
      db.setTransactionSuccessful();
      return getAndCacheChunks(t);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "privilegedInsertRowWithId", e);
    } finally {
      if (db != null) {
        db.endTransaction();
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  /**
   * SYNC Only. ADMIN Privileges!
   *
   * @param appName
   * @param dbHandleName
   * @param tableId
   * @param orderedColumns
   * @param cvValues  server's field values for this row
   * @param rowId
   *          expected to be one of ConflictType.LOCAL_DELETED_OLD_VALUES (0) or
   * @return
   * @throws RemoteException
   */
  @Override public DbChunk privilegedPerhapsPlaceRowIntoConflictWithId(String appName, DbHandle
      dbHandleName,
      String tableId, OrderedColumns orderedColumns, ContentValues cvValues,
      String rowId) throws RemoteException {

    OdkConnectionInterface db = null;

    String activeUser = getActiveUser(appName);
    String rolesList = getRolesList(appName);
    String locale = getLocale(appName);

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      db.beginTransactionExclusive();

      ODKDatabaseImplUtils.get()
          .privilegedPerhapsPlaceRowIntoConflictWithId(db, tableId, orderedColumns, cvValues, rowId,
              activeUser, rolesList, locale);
      BaseTable t = ODKDatabaseImplUtils.get().privilegedGetRowsWithId(db, tableId,
          rowId, activeUser);
      db.setTransactionSuccessful();
      return getAndCacheChunks(t);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "privilegedPerhapsPlaceRowIntoConflictWithId", e);
    } finally {
      if (db != null) {
        db.endTransaction();
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }

  }

  @Override public DbChunk simpleQuery(String appName, DbHandle dbHandleName,
      String sqlCommand, BindArgs sqlBindArgs, QueryBounds sqlQueryBounds, String tableId) throws
      RemoteException {

    OdkConnectionInterface db = null;

    String activeUser = getActiveUser(appName);
    String rolesList = getInternalRolesList(appName);

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);


      ODKDatabaseImplUtils.AccessContext accessContext =
          ODKDatabaseImplUtils.get().getAccessContext(db, tableId, activeUser, rolesList);

      BaseTable result = ODKDatabaseImplUtils.get()
          .query(db, tableId, sqlCommand, sqlBindArgs.bindArgs, sqlQueryBounds, accessContext);

      return getAndCacheChunks(result);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "simpleQuery", e);
    } finally {
      if (db != null) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override
  public DbChunk privilegedSimpleQuery(String appName, DbHandle dbHandleName,
      String sqlCommand, BindArgs sqlBindArgs, QueryBounds sqlQueryBounds, String tableId) throws RemoteException {

    OdkConnectionInterface db = null;

    String activeUser = getActiveUser(appName);

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);

      ODKDatabaseImplUtils.AccessContext accessContext = ODKDatabaseImplUtils.get()
          .getAccessContext(db, tableId, activeUser, RoleConsts.ADMIN_ROLES_LIST);

      BaseTable result = ODKDatabaseImplUtils.get()
          .privilegedQuery(db, tableId, sqlCommand, sqlBindArgs.bindArgs, sqlQueryBounds,
              accessContext);

      return getAndCacheChunks(result);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "simpleQuery", e);
    } finally {
      if (db != null) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override
  public void privilegedExecute(String appName, DbHandle dbHandleName,
      String sqlCommand, BindArgs sqlBindArgs) {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);

      ODKDatabaseImplUtils.get()
          .privilegedExecute(db, sqlCommand, sqlBindArgs.bindArgs);

    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "privilegedExecute", e);
    } finally {
      if (db != null) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }

  }

  @Override public void replaceTableMetadata(String appName, DbHandle dbHandleName,
      KeyValueStoreEntry entry) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      ODKDatabaseImplUtils.get().replaceTableMetadata(db, entry);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "replaceTableMetadata", e);
    } finally {
      if (db != null) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override public void replaceTableMetadataList(String appName, DbHandle dbHandleName,
      String tableId, List<KeyValueStoreEntry> entries, boolean clear) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      ODKDatabaseImplUtils.get().replaceTableMetadata(db, tableId, entries, clear);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "replaceTableMetadataList", e);
    } finally {
      if (db != null) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override public void replaceTableMetadataSubList(String appName, DbHandle dbHandleName,
      String tableId, String partition, String aspect, List<KeyValueStoreEntry> entries)
      throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      ODKDatabaseImplUtils.get()
          .replaceTableMetadataSubList(db, tableId, partition, aspect, entries);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "replaceTableMetadataSubList", e);
    } finally {
      if (db != null) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override public DbChunk saveAsIncompleteMostRecentCheckpointRowWithId(String appName,
      DbHandle dbHandleName, String tableId, String rowId)
      throws RemoteException {

    OdkConnectionInterface db = null;

    String activeUser = getActiveUser(appName);
    String rolesList = getInternalRolesList(appName);

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      db.beginTransactionExclusive();
      ODKDatabaseImplUtils.get().saveAsIncompleteMostRecentCheckpointRowWithId(db, tableId,
          rowId);
      BaseTable t = ODKDatabaseImplUtils.get().getMostRecentRowWithId(db, tableId, rowId,
          activeUser, rolesList);
      db.setTransactionSuccessful();
      return getAndCacheChunks(t);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "saveAsIncompleteMostRecentCheckpointRowWithId", e);
    } finally {
      if (db != null) {
        db.endTransaction();
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override public DbChunk saveAsCompleteMostRecentCheckpointRowWithId(String appName,
      DbHandle dbHandleName, String tableId, String rowId)
      throws RemoteException {

    OdkConnectionInterface db = null;

    String activeUser = getActiveUser(appName);
    String rolesList = getInternalRolesList(appName);

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      db.beginTransactionExclusive();
      ODKDatabaseImplUtils.get().saveAsCompleteMostRecentCheckpointRowWithId(db, tableId, rowId);
      BaseTable t = ODKDatabaseImplUtils.get().getMostRecentRowWithId(db, tableId, rowId,
          activeUser, rolesList);
      db.setTransactionSuccessful();
      return getAndCacheChunks(t);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "saveAsCompleteMostRecentCheckpointRowWithId", e);
    } finally {
      if (db != null) {
        db.endTransaction();
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
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

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      ODKDatabaseImplUtils.get().privilegedUpdateTableETags(db, tableId, schemaETag,
          lastDataETag);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "privilegedUpdateTableETags", e);
    } finally {
      if (db != null) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
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

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      ODKDatabaseImplUtils.get().privilegedUpdateTableLastSyncTime(db, tableId);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "privilegedUpdateTableLastSyncTime", e);
    } finally {
      if (db != null) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override public DbChunk updateRowWithId(String appName,
      DbHandle dbHandleName, String tableId, OrderedColumns orderedColumns,
      ContentValues cvValues, String rowId) throws RemoteException {

    OdkConnectionInterface db = null;

    String activeUser = getActiveUser(appName);
    String rolesList = getInternalRolesList(appName);
    String locale = getLocale(appName);

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      db.beginTransactionExclusive();
      ODKDatabaseImplUtils.get()
          .updateRowWithId(db, tableId, orderedColumns, cvValues, rowId, activeUser, rolesList,
              locale);
      BaseTable t = ODKDatabaseImplUtils.get().getMostRecentRowWithId(db, tableId, rowId,
          activeUser, rolesList);
      db.setTransactionSuccessful();
      return getAndCacheChunks(t);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "updateRowWithId", e);
    } finally {
      if (db != null) {
        db.endTransaction();
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override public void resolveServerConflictWithDeleteRowWithId(String appName,
      DbHandle dbHandleName, String tableId, String rowId) throws RemoteException {

    OdkConnectionInterface db = null;

    String activeUser = getActiveUser(appName);
    String rolesList = getInternalRolesList(appName);

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);

      ODKDatabaseImplUtils.get()
          .resolveServerConflictWithDeleteRowWithId(db, tableId, rowId,
              activeUser, RoleConsts.ADMIN_ROLES_LIST);

    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "resolveServerConflictWithDeleteRowWithId", e);
    } finally {
      if (db != null) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override public void resolveServerConflictTakeLocalRowWithId(String appName,
      DbHandle dbHandleName, String tableId, String rowId) throws RemoteException {

    OdkConnectionInterface db = null;

    String activeUser = getActiveUser(appName);
    String rolesList = getInternalRolesList(appName);
    String locale = getLocale(appName);

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);

      ODKDatabaseImplUtils.get()
          .resolveServerConflictTakeLocalRowWithId(db, tableId, rowId, activeUser, rolesList, locale);

    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "resolveServerConflictTakeLocalRowWithId", e);
    } finally {
      if (db != null) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override public void resolveServerConflictTakeLocalRowPlusServerDeltasWithId(String appName,
      DbHandle dbHandleName, String tableId, ContentValues cvValues, String rowId)
      throws  RemoteException {

    OdkConnectionInterface db = null;

    String activeUser = getActiveUser(appName);
    String rolesList = getInternalRolesList(appName);
    String locale = getLocale(appName);

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);

      ODKDatabaseImplUtils.get()
          .resolveServerConflictTakeLocalRowPlusServerDeltasWithId(db, tableId, cvValues,
              rowId, activeUser, rolesList, locale);

    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "resolveServerConflictTakeLocalRowPlusServerDeltasWithId", e);
    } finally {
      if (db != null) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override public void resolveServerConflictTakeServerRowWithId(String appName,
      DbHandle dbHandleName, String tableId, String rowId) throws RemoteException {

    OdkConnectionInterface db = null;

    String activeUser = getActiveUser(appName);
    String locale = getLocale(appName);

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);

      // regardless of the roles available to the user, act as god.
      ODKDatabaseImplUtils.get()
          .resolveServerConflictTakeServerRowWithId(db, tableId, rowId, activeUser,
              locale);

    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "resolveServerConflictTakeServerRowWithId", e);
    } finally {
      if (db != null) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
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

    OdkConnectionInterface db = null;

    String activeUser = getActiveUser(appName);

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      if ( !ODKDatabaseImplUtils.get().privilegedUpdateRowETagAndSyncState(db, tableId, rowId, rowETag,
              SyncState.valueOf(syncState), activeUser) ) {
        throw new IllegalArgumentException(
            "row id " + rowId + " does not have exactly 1 row in table " + tableId);
      }
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "privilegedUpdateRowETagAndSyncState", e);
    } finally {
      if (db != null) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override public void deleteAppAndTableLevelManifestSyncETags(String appName, DbHandle dbHandleName)
          throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
              .getConnection(appName, dbHandleName);
      SyncETagsUtils seu = new SyncETagsUtils();
      seu.deleteAppAndTableLevelManifestSyncETags(db);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName,
              "deleteAppAndTableLevelManifestSyncETags", e);
    } finally {
      if (db != null) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override public void deleteAllSyncETagsForTableId(String appName, DbHandle dbHandleName,
      String tableId) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      SyncETagsUtils seu = new SyncETagsUtils();
      seu.deleteAllSyncETagsForTableId(db, tableId);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "deleteAllSyncETagsForTableId", e);
    } finally {
      if (db != null) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override public void deleteAllSyncETagsExceptForServer(String appName, DbHandle dbHandleName,
      String verifiedUri) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      SyncETagsUtils seu = new SyncETagsUtils();
      seu.deleteAllSyncETagsExceptForServer(db, verifiedUri);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "deleteAllSyncETagsExceptForServer", e);
    } finally {
      if (db != null) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override public void deleteAllSyncETagsUnderServer(String appName, DbHandle dbHandleName,
      String verifiedUri) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      SyncETagsUtils seu = new SyncETagsUtils();
      seu.deleteAllSyncETagsUnderServer(db, verifiedUri);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "deleteAllSyncETagsUnderServer", e);
    } finally {
      if (db != null) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override public String getFileSyncETag(String appName, DbHandle dbHandleName,
      String verifiedUri, String tableId, long modificationTimestamp) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      SyncETagsUtils seu = new SyncETagsUtils();
      return seu.getFileSyncETag(db, verifiedUri, tableId, modificationTimestamp);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "getFileSyncETag", e);
    } finally {
      if (db != null) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override public String getManifestSyncETag(String appName, DbHandle dbHandleName,
      String verifiedUri, String tableId) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      SyncETagsUtils seu = new SyncETagsUtils();
      return seu.getManifestSyncETag(db, verifiedUri, tableId);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "getManifestSyncETag", e);
    } finally {
      if (db != null) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override public void updateFileSyncETag(String appName, DbHandle dbHandleName,
      String verifiedUri, String tableId, long modificationTimestamp, String eTag)
      throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      SyncETagsUtils seu = new SyncETagsUtils();
      seu.updateFileSyncETag(db, verifiedUri, tableId, modificationTimestamp, eTag);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "updateFileSyncETag", e);
    } finally {
      if (db != null) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override public void updateManifestSyncETag(String appName, DbHandle dbHandleName,
      String verifiedUri, String tableId, String eTag) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      SyncETagsUtils seu = new SyncETagsUtils();
      seu.updateManifestSyncETag(db, verifiedUri, tableId, eTag);
    } catch (Exception e) {
      throw createWrappingRemoteException(appName, dbHandleName, "updateManifestSyncETag", e);
    } finally {
      if (db != null) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
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
