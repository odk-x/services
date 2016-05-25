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
import org.opendatakit.aggregate.odktables.rest.SyncState;
import org.opendatakit.common.android.data.*;
import org.opendatakit.common.android.database.AndroidConnectFactory;
import org.opendatakit.common.android.database.OdkConnectionFactorySingleton;
import org.opendatakit.common.android.database.OdkConnectionInterface;
import org.opendatakit.common.android.logic.CommonToolProperties;
import org.opendatakit.common.android.logic.PropertiesSingleton;
import org.opendatakit.common.android.utilities.*;
import org.opendatakit.database.DatabaseConsts;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


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

  private String getLocale(String appName) {
    PropertiesSingleton props =
        CommonToolProperties.get(odkDatabaseService.getApplicationContext(), appName);
    return props.getLocale();
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

  @Override public void serverTableSchemaETagChanged(String appName, OdkDbHandle dbHandleName,
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
      return ODKDatabaseImplUtils.get().setChoiceList(db, appName, choiceListJSON);
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
      return ODKDatabaseImplUtils.get().getChoiceList(db, appName, choiceListId);
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
          .createOrOpenDBTableWithColumns(db, appName, tableId, columns.getColumns());

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
          .createOrOpenDBTableWithColumnsAndProperties(db, appName, tableId, columns.getColumns(),
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
      String tableId, OrderedColumns orderedDefns, String rowId) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      db.beginTransactionExclusive();
      ODKDatabaseImplUtils.get().deleteAllCheckpointRowsWithId(db, appName, tableId, rowId);
      UserTable t = ODKDatabaseImplUtils.get().getMostRecentRowWithId(db, appName, tableId,
          orderedDefns, rowId);
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
      String tableId, OrderedColumns orderedDefns, String rowId) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      db.beginTransactionExclusive();
      ODKDatabaseImplUtils.get().deleteLastCheckpointRowWithId(db, appName, tableId, rowId);
      UserTable t = ODKDatabaseImplUtils.get().getMostRecentRowWithId(db, appName, tableId,
          orderedDefns, rowId);
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
      ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, appName, tableId);
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

  @Override public OdkDbChunk deleteRowWithId(String appName,
      OdkDbHandle dbHandleName,
      String tableId, OrderedColumns orderedDefns, String rowId) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      db.beginTransactionExclusive();
      ODKDatabaseImplUtils.get().deleteRowWithId(db, appName, tableId, rowId);
      UserTable t = ODKDatabaseImplUtils.get().getMostRecentRowWithId(db, appName, tableId,
          orderedDefns, rowId);
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
      OdkDbHandle dbHandleName, String tableId, OrderedColumns orderedDefns, String rowId)
      throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      UserTable results = ODKDatabaseImplUtils.get()
          .getRowsWithId(db, appName, tableId, orderedDefns, rowId);

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

  @Override public OdkDbChunk getMostRecentRowWithId(String appName,
      OdkDbHandle dbHandleName, String tableId, OrderedColumns orderedDefns, String rowId)
      throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      UserTable results =  ODKDatabaseImplUtils.get()
          .getMostRecentRowWithId(db, appName, tableId, orderedDefns, rowId);

      return getAndCacheChunks(results);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if (msg == null)
        msg = e.getMessage();
      if (msg == null)
        msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName)
          .e("getMostRecentRowWithId", appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
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
      SyncState state = ODKDatabaseImplUtils.get().getSyncState(db, appName, tableId, rowId);
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
          .getUserDefinedColumns(db, appName, tableId);

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
    String locale = getLocale(appName);

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      db.beginTransactionExclusive();
      ODKDatabaseImplUtils.get()
          .insertCheckpointRowWithId(db, tableId, orderedColumns, cvValues, rowId, activeUser,
              locale);
      UserTable t = ODKDatabaseImplUtils.get()
          .getMostRecentRowWithId(db, appName, tableId, orderedColumns, rowId);
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
    String locale = getLocale(appName);

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      db.beginTransactionExclusive();
      ODKDatabaseImplUtils.get()
          .insertRowWithId(db, tableId, orderedColumns, cvValues, rowId, activeUser, locale);
      UserTable t = ODKDatabaseImplUtils.get()
          .getMostRecentRowWithId(db, appName, tableId, orderedColumns, rowId);
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

  @Override public OdkDbChunk placeRowIntoServerConflictWithId(String appName, OdkDbHandle
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

      ODKDatabaseImplUtils.get().placeRowIntoServerConflictWithId(db, tableId, orderedColumns,
          cvValues, rowId, localRowConflictType, activeUser, locale);
      UserTable t = ODKDatabaseImplUtils.get().getConflictingRowsInExistingDBTableWithId(db,
          appName, tableId, orderedColumns, rowId);
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

  @Override public OdkDbChunk rawSqlQuery(String appName, OdkDbHandle dbHandleName, String tableId,
      OrderedColumns columnDefns, String whereClause, String[] selectionArgs, String[] groupBy,
      String having, String orderByElementKey, String orderByDirection) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      UserTable results = ODKDatabaseImplUtils.get()
          .rawSqlQuery(db, appName, tableId, columnDefns, whereClause, selectionArgs, groupBy,
              having, orderByElementKey, orderByDirection);

      return getAndCacheChunks(results);
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

  @Override public OdkDbChunk arbitraryQuery(String appName, OdkDbHandle dbHandleName,
      String sqlCommand, String[] sqlBindArgs) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      RawUserTable result = ODKDatabaseImplUtils.get()
          .arbitraryQuery(db, appName, sqlCommand, sqlBindArgs);

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
      OdkDbHandle dbHandleName, String tableId, OrderedColumns orderedColumns,
      ContentValues cvValues, String rowId) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      db.beginTransactionExclusive();
      ODKDatabaseImplUtils.get()
          .saveAsIncompleteMostRecentCheckpointRowWithId(db, tableId, rowId);
      UserTable t = ODKDatabaseImplUtils.get()
          .getMostRecentRowWithId(db, appName, tableId, orderedColumns, rowId);
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
      OdkDbHandle dbHandleName, String tableId, OrderedColumns orderedColumns,
      ContentValues cvValues, String rowId) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      db.beginTransactionExclusive();
      ODKDatabaseImplUtils.get()
          .saveAsCompleteMostRecentCheckpointRowWithId(db, tableId, rowId);
      UserTable t = ODKDatabaseImplUtils.get()
          .getMostRecentRowWithId(db, appName, tableId, orderedColumns, rowId);
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

  @Override public void updateDBTableETags(String appName, OdkDbHandle dbHandleName, String tableId,
      String schemaETag, String lastDataETag) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      ODKDatabaseImplUtils.get().updateDBTableETags(db, tableId, schemaETag, lastDataETag);
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

  @Override public void updateDBTableLastSyncTime(String appName, OdkDbHandle dbHandleName,
      String tableId) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      ODKDatabaseImplUtils.get().updateDBTableLastSyncTime(db, tableId);
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
    String locale = getLocale(appName);

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      db.beginTransactionExclusive();
      ODKDatabaseImplUtils.get()
          .updateRowWithId(db, tableId, orderedColumns, cvValues, rowId, activeUser, locale);
      UserTable t = ODKDatabaseImplUtils.get()
          .getMostRecentRowWithId(db, appName, tableId, orderedColumns, rowId);
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

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);

      ODKDatabaseImplUtils.get()
          .resolveServerConflictWithDeleteRowWithId(db, appName, tableId, rowId);

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
    String locale = getLocale(appName);

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);

      ODKDatabaseImplUtils.get()
          .resolveServerConflictTakeLocalRowWithId(db, appName, tableId, rowId, activeUser, locale);

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
    String locale = getLocale(appName);

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);

      ODKDatabaseImplUtils.get()
          .resolveServerConflictTakeLocalRowPlusServerDeltasWithId(db, appName, tableId, cvValues,
              rowId, activeUser, locale);

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

      ODKDatabaseImplUtils.get()
          .resolveServerConflictTakeServerRowWithId(db, appName, tableId, rowId, activeUser, locale);

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

  @Override public void updateRowETagAndSyncState(String appName, OdkDbHandle dbHandleName,
      String tableId, String rowId, String rowETag, String syncState) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      ODKDatabaseImplUtils.get()
          .updateRowETagAndSyncState(db, tableId, rowId, rowETag, SyncState.valueOf(syncState));
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
    return OdkDatabaseService.removeParceledChunk(chunkID.getUuid());
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
      OdkDatabaseService.putParceledChunks(chunkList);
    }

    return firstChunk;
  }

}
