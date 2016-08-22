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

package org.opendatakit.database.service;

import android.content.ContentValues;
import android.os.ParcelUuid;
import android.os.Parcelable;
import android.os.RemoteException;
import android.util.Log;

import org.opendatakit.RoleConsts;
import org.opendatakit.aggregate.odktables.rest.SyncState;
import org.opendatakit.aggregate.odktables.rest.entity.Column;
import org.opendatakit.common.android.data.ColumnList;
import org.opendatakit.common.android.data.OrderedColumns;
import org.opendatakit.common.android.data.TableDefinitionEntry;
import org.opendatakit.common.android.database.AndroidConnectFactory;
import org.opendatakit.common.android.database.OdkConnectionFactorySingleton;
import org.opendatakit.common.android.database.OdkConnectionInterface;
import org.opendatakit.common.android.exception.ActionNotAuthorizedException;
import org.opendatakit.common.android.logic.CommonToolProperties;
import org.opendatakit.common.android.logic.PropertiesSingleton;
import org.opendatakit.common.android.utilities.ODKCursorUtils;
import org.opendatakit.common.android.utilities.ODKDatabaseImplUtils;
import org.opendatakit.common.android.utilities.SyncETagsUtils;
import org.opendatakit.common.android.utilities.WebLogger;
import org.opendatakit.database.DatabaseConsts;
import org.opendatakit.database.service.queries.QueryBounds;
import org.opendatakit.database.utilities.OdkDbChunkUtil;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class OdkDatabaseServiceInterface extends OdkDbInterface.Stub {

  private static final String TAG = OdkDatabaseServiceInterface.class.getSimpleName();

  /**
   *
   */
  private final OdkDatabaseService odkDatabaseService;

  /**
   * @param odkDatabaseService -- service under which this interface was created
   */
  OdkDatabaseServiceInterface(OdkDatabaseService odkDatabaseService) {
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
    return getInternalRolesList(appName);
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
    return getInternalUsersList(appName);
  }

  @Override public OdkDbHandle openDatabase(String appName) throws RemoteException {

    OdkDatabaseService.possiblyWaitForDatabaseServiceDebugger();

    OdkConnectionInterface db = null;

    OdkDbHandle dbHandleName = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
        .generateDatabaseServiceDbHandle();
    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      return dbHandleName;
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if (msg == null)
        msg = e.getMessage();
      if (msg == null)
        msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName)
          .e("openDatabase", appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
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

  @Override public void closeDatabase(String appName, OdkDbHandle dbHandleName)
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
      String msg = e.getLocalizedMessage();
      if (msg == null)
        msg = e.getMessage();
      if (msg == null)
        msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName)
          .e("closeDatabase", appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
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
  @Override public OdkDbChunk createLocalOnlyDbTableWithColumns(String appName, OdkDbHandle dbHandleName,
      String tableId, ColumnList columns) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      OrderedColumns results = ODKDatabaseImplUtils.get()
          .createLocalOnlyDBTableWithColumns(db, tableId, columns.getColumns());

      return getAndCacheChunks(results);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if (msg == null)
        msg = e.getMessage();
      if (msg == null)
        msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("createLocalOnlyDbTableWithColumns",
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
  @Override public void deleteLocalOnlyDBTable(String appName, OdkDbHandle dbHandleName,
      String tableId) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      ODKDatabaseImplUtils.get().deleteLocalOnlyDBTable(db, tableId);

    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if (msg == null)
        msg = e.getMessage();
      if (msg == null)
        msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("deleteLocalOnlyDbTable",
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
  public void insertLocalOnlyRow(String appName, OdkDbHandle dbHandleName, String tableId,
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
   * @param whereArgs
   * @throws ActionNotAuthorizedException
   */
  @Override public void updateLocalOnlyRow(String appName, OdkDbHandle dbHandleName, String tableId,
      ContentValues rowValues, String whereClause, String[] whereArgs)
      throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      ODKDatabaseImplUtils.get()
          .updateLocalOnlyRow(db, tableId, rowValues, whereClause, whereArgs);

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
   * @param whereArgs
   * @throws ActionNotAuthorizedException
   */
  @Override public void deleteLocalOnlyRow(String appName, OdkDbHandle dbHandleName, String tableId,
      String whereClause, String[] whereArgs) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      ODKDatabaseImplUtils.get()
          .deleteLocalOnlyRow(db, tableId, whereClause, whereArgs);

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
  @Override public void privilegedServerTableSchemaETagChanged(String appName, OdkDbHandle
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
      String msg = e.getLocalizedMessage();
      if (msg == null)
        msg = e.getMessage();
      if (msg == null)
        msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("serverTableSchemaETagChanged",
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
   * Compute the app-global choiceListId for this choiceListJSON
   * and register the tuple of (choiceListId, choiceListJSON).
   * Return choiceListId.
   *
   * @param appName -- application name
   * @param dbHandleName -- database handle
   * @param choiceListJSON -- the actual JSON choice list text.
   * @return choiceListId -- the unique code mapping to the choiceListJSON
   */
  @Override public String setChoiceList(String appName, OdkDbHandle dbHandleName,
      String choiceListJSON) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      return ODKDatabaseImplUtils.get().setChoiceList(db, choiceListJSON);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if (msg == null)
        msg = e.getMessage();
      if (msg == null)
        msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName)
          .e("setChoiceList", appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
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
   * Return the choice list JSON corresponding to the choiceListId
   *
   * @param appName -- application name
   * @param dbHandleName -- database handle
   * @param choiceListId -- the md5 hash of the choiceListJSON
   * @return choiceListJSON -- the actual JSON choice list text.
   */
  @Override public String getChoiceList(String appName, OdkDbHandle dbHandleName,
      String choiceListId) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      return ODKDatabaseImplUtils.get().getChoiceList(db, choiceListId);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if (msg == null)
        msg = e.getMessage();
      if (msg == null)
        msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName)
          .e("getChoiceList", appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
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

  @Override public OdkDbChunk createOrOpenDBTableWithColumns(String appName,
      OdkDbHandle dbHandleName, String tableId, ColumnList columns) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      OrderedColumns results = ODKDatabaseImplUtils.get()
          .createOrOpenDBTableWithColumns(db, tableId, columns.getColumns());

      return getAndCacheChunks(results);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if (msg == null)
        msg = e.getMessage();
      if (msg == null)
        msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("createOrOpenDBTableWithColumns",
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

  @Override public OdkDbChunk createOrOpenDBTableWithColumnsAndProperties(String appName,
      OdkDbHandle dbHandleName, String tableId, ColumnList columns,
      List<KeyValueStoreEntry> metaData, boolean clear) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      OrderedColumns results =
          ODKDatabaseImplUtils.get()
          .createOrOpenDBTableWithColumnsAndProperties(db, tableId, columns.getColumns(),
              metaData, clear);

      return getAndCacheChunks(results);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if (msg == null)
        msg = e.getMessage();
      if (msg == null)
        msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("createOrOpenDBTableWithColumnsAndProperties",
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

  @Override public OdkDbChunk deleteAllCheckpointRowsWithId(String appName, OdkDbHandle dbHandleName,
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
      OdkDbTable t = ODKDatabaseImplUtils.get().getMostRecentRowWithId(db, tableId, rowId);
      db.setTransactionSuccessful();
      return getAndCacheChunks(t);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if (msg == null)
        msg = e.getMessage();
      if (msg == null)
        msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("deleteAllCheckpointRowsWithId",
          appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
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

  @Override public OdkDbChunk deleteLastCheckpointRowWithId(String appName, OdkDbHandle dbHandleName,
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
      OdkDbTable t = ODKDatabaseImplUtils.get().getMostRecentRowWithId(db, tableId, rowId);
      db.setTransactionSuccessful();
      return getAndCacheChunks(t);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if (msg == null)
        msg = e.getMessage();
      if (msg == null)
        msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("deleteLastCheckpointRowWithId",
          appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
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

  @Override public void deleteDBTableAndAllData(String appName, OdkDbHandle dbHandleName,
      String tableId) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, tableId);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if (msg == null)
        msg = e.getMessage();
      if (msg == null)
        msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName)
          .e("deleteDBTableAndAllDat", appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
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

  @Override public void deleteDBTableMetadata(String appName, OdkDbHandle dbHandleName,
      String tableId, String partition, String aspect, String key) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      ODKDatabaseImplUtils.get().deleteDBTableMetadata(db, tableId, partition, aspect, key);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if (msg == null)
        msg = e.getMessage();
      if (msg == null)
        msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName)
          .e("deleteDBTableMetadata", appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
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

  @Override public OdkDbChunk deleteRowWithId(String appName, OdkDbHandle dbHandleName,
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
      OdkDbTable t = ODKDatabaseImplUtils.get().getMostRecentRowWithId(db, tableId, rowId);
      db.setTransactionSuccessful();
      return getAndCacheChunks(t);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if (msg == null)
        msg = e.getMessage();
      if (msg == null)
        msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("deleteRowWithId",
          appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
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
  @Override public OdkDbChunk privilegedDeleteRowWithId(String appName,
                                              OdkDbHandle dbHandleName,
                                              String tableId, String rowId) throws RemoteException {

    OdkConnectionInterface db = null;

    String activeUser = getActiveUser(appName);

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
              .getConnection(appName, dbHandleName);
      db.beginTransactionExclusive();
      ODKDatabaseImplUtils.get().privilegedDeleteRowWithId(db, tableId, rowId, activeUser);
      OdkDbTable t = ODKDatabaseImplUtils.get().getMostRecentRowWithId(db, tableId, rowId);
      db.setTransactionSuccessful();
      return getAndCacheChunks(t);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if (msg == null)
        msg = e.getMessage();
      if (msg == null)
        msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("deleteRowWithId",
              appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
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

  @Override public OdkDbChunk getAdminColumns() throws RemoteException {

    List<String> cols = ODKDatabaseImplUtils.get().getAdminColumns();
    String[] results = cols.toArray(new String[cols.size()]);

    return getAndCacheChunks(results);
  }

  @Override public OdkDbChunk getAllColumnNames(String appName, OdkDbHandle dbHandleName,
      String tableId) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      String[] results =  ODKDatabaseImplUtils.get().getAllColumnNames(db, tableId);

      return getAndCacheChunks(results);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if (msg == null)
        msg = e.getMessage();
      if (msg == null)
        msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName)
          .e("getAllColumnNames", appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
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

  @Override public OdkDbChunk getAllTableIds(String appName, OdkDbHandle dbHandleName)
      throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      List<String> results = ODKDatabaseImplUtils.get().getAllTableIds(db);

      return getAndCacheChunks((Serializable)results);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if (msg == null)
        msg = e.getMessage();
      if (msg == null)
        msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName)
          .e("getAllTableIds", appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
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

  @Override public OdkDbChunk getRowsWithId(String appName,
      OdkDbHandle dbHandleName, String tableId, String rowId)
      throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      OdkDbTable results = ODKDatabaseImplUtils.get()
          .getRowsWithId(db, tableId, rowId);

      return getAndCacheChunks(results);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if (msg == null)
        msg = e.getMessage();
      if (msg == null)
        msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("getRowsWithId",
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

  @Override
  public OdkDbChunk getMostRecentRowWithId(String appName, OdkDbHandle dbHandleName, String
      tableId,
      String rowId)
      throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      OdkDbTable results = ODKDatabaseImplUtils.get()
          .getMostRecentRowWithId(db, tableId, rowId);

      return getAndCacheChunks(results);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if (msg == null)
        msg = e.getMessage();
      if (msg == null)
        msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("getMostRecentRowWithId",
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

  @Override public OdkDbChunk getDBTableMetadata(String appName,
      OdkDbHandle dbHandleName, String tableId, String partition, String aspect, String key)
      throws RemoteException {

    OdkConnectionInterface db = null;

    ArrayList<KeyValueStoreEntry> kvsEntries = null;
    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      kvsEntries = ODKDatabaseImplUtils.get()
          .getDBTableMetadata(db, tableId, partition, aspect, key);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if (msg == null)
        msg = e.getMessage();
      if (msg == null)
        msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName)
          .e("getDBTableMetadata", appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
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

    return getAndCacheChunks(kvsEntries);
  }

  @Override public OdkDbChunk getTableHealthStatuses(String appName,
      OdkDbHandle dbHandleName) throws RemoteException {

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
        if (health != ODKCursorUtils.TABLE_HEALTH_IS_CLEAN) {
          TableHealthStatus status = TableHealthStatus.TABLE_HEALTH_IS_CLEAN;
          switch (health) {
          case ODKCursorUtils.TABLE_HEALTH_HAS_CHECKPOINTS:
            status = TableHealthStatus.TABLE_HEALTH_HAS_CHECKPOINTS;
            break;
          case ODKCursorUtils.TABLE_HEALTH_HAS_CONFLICTS:
            status = TableHealthStatus.TABLE_HEALTH_HAS_CONFLICTS;
            break;
          case ODKCursorUtils.TABLE_HEALTH_HAS_CHECKPOINTS_AND_CONFLICTS:
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
      WebLogger.getLogger(appName).e("getTableHealthStatuses", "exception during processing");
      WebLogger.getLogger(appName).printStackTrace(t);
      throw t;
    } finally {
      if (db != null) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override public OdkDbChunk getExportColumns() throws RemoteException {
    List<String> exports = ODKDatabaseImplUtils.get().getExportColumns();
    String[] results =  exports.toArray(new String[exports.size()]);

    return getAndCacheChunks(results);
  }

  @Override public String getSyncState(String appName, OdkDbHandle dbHandleName, String tableId,
      String rowId) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      SyncState state = ODKDatabaseImplUtils.get().getSyncState(db, tableId, rowId);
      return state.name();
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if (msg == null)
        msg = e.getMessage();
      if (msg == null)
        msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName)
          .e("getSyncState", appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
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

  @Override public OdkDbChunk getTableDefinitionEntry(String appName,
      OdkDbHandle dbHandleName, String tableId) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      TableDefinitionEntry results = ODKDatabaseImplUtils.get()
          .getTableDefinitionEntry(db, tableId);

      return getAndCacheChunks(results);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if (msg == null)
        msg = e.getMessage();
      if (msg == null)
        msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName)
          .e("getTableDefinitionEntry", appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
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

  @Override public OdkDbChunk getUserDefinedColumns(String appName, OdkDbHandle dbHandleName,
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
      String msg = e.getLocalizedMessage();
      if (msg == null)
        msg = e.getMessage();
      if (msg == null)
        msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName)
          .e("getUserDefinedColumns", appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
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

  @Override public boolean hasTableId(String appName, OdkDbHandle dbHandleName, String tableId)
      throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      return ODKDatabaseImplUtils.get().hasTableId(db, tableId);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if (msg == null)
        msg = e.getMessage();
      if (msg == null)
        msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName)
          .e("hasTableId", appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
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

  @Override public OdkDbChunk insertCheckpointRowWithId(String appName,
      OdkDbHandle dbHandleName, String tableId, OrderedColumns orderedColumns,
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
      OdkDbTable t = ODKDatabaseImplUtils.get().getMostRecentRowWithId(db, tableId, rowId);
      db.setTransactionSuccessful();
      return getAndCacheChunks(t);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if (msg == null)
        msg = e.getMessage();
      if (msg == null)
        msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("insertCheckpointRowWithId",
          appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
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

  @Override public OdkDbChunk insertRowWithId(String appName,
      OdkDbHandle dbHandleName, String tableId, OrderedColumns orderedColumns,
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
      OdkDbTable t = ODKDatabaseImplUtils.get()
          .getMostRecentRowWithId(db, tableId, rowId);
      db.setTransactionSuccessful();
      return getAndCacheChunks(t);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if (msg == null)
        msg = e.getMessage();
      if (msg == null)
        msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("insertRowWithId",
          appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
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
  @Override public OdkDbChunk privilegedInsertRowWithId(String appName, OdkDbHandle dbHandleName,
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
      OdkDbTable t = ODKDatabaseImplUtils.get().getMostRecentRowWithId(db, tableId, rowId);
      db.setTransactionSuccessful();
      return getAndCacheChunks(t);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if (msg == null)
        msg = e.getMessage();
      if (msg == null)
        msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("serverInsertRowWithId",
              appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
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
   * @param localRowConflictType
   *          expected to be one of ConflictType.LOCAL_DELETED_OLD_VALUES (0) or
   * @return
     * @throws RemoteException
     */
  @Override public OdkDbChunk privilegedPlaceRowIntoConflictWithId(String appName, OdkDbHandle
      dbHandleName,
      String tableId, OrderedColumns orderedColumns, ContentValues cvValues,
      String rowId, int localRowConflictType) throws RemoteException {

    OdkConnectionInterface db = null;

    String activeUser = getActiveUser(appName);
    String locale = getLocale(appName);

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      db.beginTransactionExclusive();

      ODKDatabaseImplUtils.get()
          .privilegedPlaceRowIntoConflictWithId(db, tableId, orderedColumns, cvValues, rowId,
              localRowConflictType, activeUser, locale);
      OdkDbTable t = ODKDatabaseImplUtils.get()
          .getConflictingRowsInExistingDBTableWithId(db, tableId, rowId);
      db.setTransactionSuccessful();
      return getAndCacheChunks(t);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if (msg == null)
        msg = e.getMessage();
      if (msg == null)
        msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("placeRowIntoServerConflictWithId",
          appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
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

  @Override public OdkDbChunk rawSqlQuery(String appName, OdkDbHandle dbHandleName,
      String sqlCommand, BindArgs sqlBindArgs, QueryBounds sqlQueryBounds) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      OdkDbTable result = ODKDatabaseImplUtils.get()
          .rawSqlQuery(db, sqlCommand, sqlBindArgs.bindArgs, sqlQueryBounds);

      return getAndCacheChunks(result);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if (msg == null)
        msg = e.getMessage();
      if (msg == null)
        msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName)
          .e("rawSqlQuery", appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
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

  @Override public void replaceDBTableMetadata(String appName, OdkDbHandle dbHandleName,
      KeyValueStoreEntry entry) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      ODKDatabaseImplUtils.get().replaceDBTableMetadata(db, entry);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if (msg == null)
        msg = e.getMessage();
      if (msg == null)
        msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("replaceDBTableMetadata",
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

  @Override public void replaceDBTableMetadataList(String appName, OdkDbHandle dbHandleName,
      String tableId, List<KeyValueStoreEntry> entries, boolean clear) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      ODKDatabaseImplUtils.get().replaceDBTableMetadata(db, tableId, entries, clear);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if (msg == null)
        msg = e.getMessage();
      if (msg == null)
        msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("replaceDBTableMetadataList",
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

  @Override public void replaceDBTableMetadataSubList(String appName, OdkDbHandle dbHandleName,
      String tableId, String partition, String aspect, List<KeyValueStoreEntry> entries)
      throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      ODKDatabaseImplUtils.get()
          .replaceDBTableMetadataSubList(db, tableId, partition, aspect, entries);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if (msg == null)
        msg = e.getMessage();
      if (msg == null)
        msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("replaceDBTableMetadataSubList",
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

  @Override public OdkDbChunk saveAsIncompleteMostRecentCheckpointRowWithId(String appName,
      OdkDbHandle dbHandleName, String tableId, String rowId)
      throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      db.beginTransactionExclusive();
      ODKDatabaseImplUtils.get().saveAsIncompleteMostRecentCheckpointRowWithId(db, tableId, rowId);
      OdkDbTable t = ODKDatabaseImplUtils.get().getMostRecentRowWithId(db, tableId, rowId);
      db.setTransactionSuccessful();
      return getAndCacheChunks(t);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if (msg == null)
        msg = e.getMessage();
      if (msg == null)
        msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("saveAsIncompleteMostRecentCheckpointRowWithId",
          appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
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

  @Override public OdkDbChunk saveAsCompleteMostRecentCheckpointRowWithId(String appName,
      OdkDbHandle dbHandleName, String tableId, String rowId)
      throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      db.beginTransactionExclusive();
      ODKDatabaseImplUtils.get().saveAsCompleteMostRecentCheckpointRowWithId(db, tableId, rowId);
      OdkDbTable t = ODKDatabaseImplUtils.get().getMostRecentRowWithId(db, tableId, rowId);
      db.setTransactionSuccessful();
      return getAndCacheChunks(t);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if (msg == null)
        msg = e.getMessage();
      if (msg == null)
        msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("saveAsCompleteMostRecentCheckpointRowWithId",
          appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
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
  @Override public void privilegedUpdateDBTableETags(String appName, OdkDbHandle dbHandleName,
      String tableId,
      String schemaETag, String lastDataETag) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      ODKDatabaseImplUtils.get().privilegedUpdateDBTableETags(db, tableId, schemaETag,
          lastDataETag);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if (msg == null)
        msg = e.getMessage();
      if (msg == null)
        msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName)
          .e("updateDBTableETags", appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
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
   * @throws RemoteException
     */
  @Override public void privilegedUpdateDBTableLastSyncTime(String appName, OdkDbHandle
      dbHandleName,
      String tableId) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      ODKDatabaseImplUtils.get().privilegedUpdateDBTableLastSyncTime(db, tableId);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if (msg == null)
        msg = e.getMessage();
      if (msg == null)
        msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("updateDBTableLastSyncTime",
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

  @Override public OdkDbChunk updateRowWithId(String appName,
      OdkDbHandle dbHandleName, String tableId, OrderedColumns orderedColumns,
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
      OdkDbTable t = ODKDatabaseImplUtils.get().getMostRecentRowWithId(db, tableId, rowId);
      db.setTransactionSuccessful();
      return getAndCacheChunks(t);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if (msg == null)
        msg = e.getMessage();
      if (msg == null)
        msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("updateRowWithId",
          appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
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
  @Override public OdkDbChunk privilegedUpdateRowWithId(String appName, OdkDbHandle dbHandleName,
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
              .privilegedUpdateRowWithId(db, tableId, orderedColumns, cvValues, rowId, activeUser,
                  locale, asCsvRequestedChange);
      OdkDbTable t = ODKDatabaseImplUtils.get().getMostRecentRowWithId(db, tableId, rowId);
      db.setTransactionSuccessful();
      return getAndCacheChunks(t);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if (msg == null)
        msg = e.getMessage();
      if (msg == null)
        msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("updateRowWithId",
              appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
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
      OdkDbHandle dbHandleName, String tableId, String rowId) throws RemoteException {

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
      String msg = e.getLocalizedMessage();
      if (msg == null)
        msg = e.getMessage();
      if (msg == null)
        msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("resolveServerConflictWithDeleteRowWithId",
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

  @Override public void resolveServerConflictTakeLocalRowWithId(String appName,
      OdkDbHandle dbHandleName, String tableId, String rowId) throws RemoteException {

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
      String msg = e.getLocalizedMessage();
      if (msg == null)
        msg = e.getMessage();
      if (msg == null)
        msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("resolveServerConflictTakeLocalRowWithId",
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

  @Override public void resolveServerConflictTakeLocalRowPlusServerDeltasWithId(String appName,
      OdkDbHandle dbHandleName, String tableId, ContentValues cvValues, String rowId)
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
      String msg = e.getLocalizedMessage();
      if (msg == null)
        msg = e.getMessage();
      if (msg == null)
        msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("resolveServerConflictTakeLocalRowWithId",
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

  @Override public void resolveServerConflictTakeServerRowWithId(String appName,
      OdkDbHandle dbHandleName, String tableId, String rowId) throws RemoteException {

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
      String msg = e.getLocalizedMessage();
      if (msg == null)
        msg = e.getMessage();
      if (msg == null)
        msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("resolveServerConflictTakeServerRowWithId",
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
  @Override public void privilegedUpdateRowETagAndSyncState(String appName, OdkDbHandle
      dbHandleName,
      String tableId, String rowId, String rowETag, String syncState) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      ODKDatabaseImplUtils.get()
          .privilegedUpdateRowETagAndSyncState(db, tableId, rowId, rowETag,
              SyncState.valueOf(syncState));
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if (msg == null)
        msg = e.getMessage();
      if (msg == null)
        msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("updateRowETagAndSyncState",
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

  @Override public void deleteAllSyncETagsForTableId(String appName, OdkDbHandle dbHandleName,
      String tableId) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      SyncETagsUtils seu = new SyncETagsUtils();
      seu.deleteAllSyncETagsForTableId(db, tableId);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if (msg == null)
        msg = e.getMessage();
      if (msg == null)
        msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("deleteAllSyncETagsForTableId",
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

  @Override public void deleteAllSyncETagsExceptForServer(String appName, OdkDbHandle dbHandleName,
      String verifiedUri) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      SyncETagsUtils seu = new SyncETagsUtils();
      seu.deleteAllSyncETagsExceptForServer(db, verifiedUri);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if (msg == null)
        msg = e.getMessage();
      if (msg == null)
        msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("deleteAllSyncETagsExceptForServer",
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

  @Override public void deleteAllSyncETagsUnderServer(String appName, OdkDbHandle dbHandleName,
      String verifiedUri) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      SyncETagsUtils seu = new SyncETagsUtils();
      seu.deleteAllSyncETagsUnderServer(db, verifiedUri);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if (msg == null)
        msg = e.getMessage();
      if (msg == null)
        msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("deleteAllSyncETagsUnderServer",
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

  @Override public String getFileSyncETag(String appName, OdkDbHandle dbHandleName,
      String verifiedUri, String tableId, long modificationTimestamp) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      SyncETagsUtils seu = new SyncETagsUtils();
      return seu.getFileSyncETag(db, verifiedUri, tableId, modificationTimestamp);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if (msg == null)
        msg = e.getMessage();
      if (msg == null)
        msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName)
          .e("getFileSyncETag", appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
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

  @Override public String getManifestSyncETag(String appName, OdkDbHandle dbHandleName,
      String verifiedUri, String tableId) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      SyncETagsUtils seu = new SyncETagsUtils();
      return seu.getManifestSyncETag(db, verifiedUri, tableId);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if (msg == null)
        msg = e.getMessage();
      if (msg == null)
        msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName)
          .e("getManifestSyncETag", appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
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

  @Override public void updateFileSyncETag(String appName, OdkDbHandle dbHandleName,
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
      String msg = e.getLocalizedMessage();
      if (msg == null)
        msg = e.getMessage();
      if (msg == null)
        msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName)
          .e("updateFileSyncETag", appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
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

  @Override public void updateManifestSyncETag(String appName, OdkDbHandle dbHandleName,
      String verifiedUri, String tableId, String eTag) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      SyncETagsUtils seu = new SyncETagsUtils();
      seu.updateManifestSyncETag(db, verifiedUri, tableId, eTag);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if (msg == null)
        msg = e.getMessage();
      if (msg == null)
        msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("updateManifestSyncETag",
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


  @Override public OdkDbChunk getChunk(ParcelUuid chunkID) {
    return odkDatabaseService.removeParceledChunk(chunkID.getUuid());
  }

  private OdkDbChunk getAndCacheChunks(Parcelable data) {
    // Break the results into pieces that will fit over the wire
    List<OdkDbChunk> chunkList = OdkDbChunkUtil.convertToChunks(data, DatabaseConsts.PARCEL_SIZE);

    return getAndCacheChunksHelper(chunkList);
  }

  private OdkDbChunk getAndCacheChunks(Serializable data) {
    List<OdkDbChunk> chunkList;
    try {
      chunkList = OdkDbChunkUtil.convertToChunks(data, DatabaseConsts.PARCEL_SIZE);
    } catch (IOException e) {
      Log.e(TAG, "Invalid state. Failed to convert chunks");
      return null;
    }

    return getAndCacheChunksHelper(chunkList);
  }

  private OdkDbChunk getAndCacheChunksHelper(List<OdkDbChunk> chunkList) {

    if (chunkList == null || chunkList.size() == 0) {
      Log.e(TAG, "Invalid state. Failed to convert chunks");
      return null;
    }

    // Return the first chunk and store the rest for later retrieval
    OdkDbChunk firstChunk = chunkList.remove(0);

    if (chunkList.size() > 0) {
      odkDatabaseService.putParceledChunks(chunkList);
    }

    return firstChunk;
  }

}
