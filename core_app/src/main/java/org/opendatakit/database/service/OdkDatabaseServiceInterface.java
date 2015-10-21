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
import android.os.RemoteException;

import org.opendatakit.aggregate.odktables.rest.SyncState;
import org.opendatakit.common.android.data.ColumnList;
import org.opendatakit.common.android.data.OrderedColumns;
import org.opendatakit.common.android.data.TableDefinitionEntry;
import org.opendatakit.common.android.data.UserTable;
import org.opendatakit.common.android.database.AndroidConnectFactory;
import org.opendatakit.common.android.database.OdkConnectionFactorySingleton;
import org.opendatakit.common.android.database.OdkConnectionInterface;
import org.opendatakit.common.android.utilities.ODKCursorUtils;
import org.opendatakit.common.android.utilities.ODKDatabaseImplUtils;
import org.opendatakit.common.android.utilities.SyncETagsUtils;
import org.opendatakit.common.android.utilities.WebLogger;
import org.opendatakit.core.application.Core;

import java.util.ArrayList;
import java.util.List;

public class OdkDatabaseServiceInterface extends OdkDbInterface.Stub {
  
  private static final String LOGTAG = OdkDatabaseServiceInterface.class.getSimpleName();

  /**
   * 
   */
  private final OdkDatabaseService odkDatabaseService;

  /**
   * @param odkDatabaseService
   */
  OdkDatabaseServiceInterface(OdkDatabaseService odkDatabaseService) {
    this.odkDatabaseService = odkDatabaseService;
    // Used to ensure that the singleton has been initialized properly
    AndroidConnectFactory.configure();
  }

  @Override
  public OdkDbHandle openDatabase(String appName, boolean beginTransaction) throws RemoteException {

    Core.getInstance().possiblyWaitForDatabaseServiceDebugger();

    OdkConnectionInterface db = null;

    OdkDbHandle dbHandleName = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().generateDatabaseServiceDbHandle();
    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().getConnection(appName, dbHandleName);
      if ( beginTransaction ) {
        db.beginTransactionNonExclusive();
      }
      return dbHandleName;
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("openDatabase", appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null ) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }
  
  @Override
  public void beginTransaction(String appName, OdkDbHandle dbHandleName) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().getConnection(appName, dbHandleName);
      db.beginTransactionNonExclusive();
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("beginTransaction", appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null ) {
        // release the reference...
        // this does not necessarily close the db handle 
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override
  public void closeDatabase(String appName, OdkDbHandle dbHandleName) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().getConnection(appName, dbHandleName);
      boolean first = true;
      while ( db != null && db.isOpen() && db.inTransaction()) {
        if ( !first ) {
          WebLogger.getLogger(appName).e("closeDatabase", appName + " " + dbHandleName.getDatabaseHandle() + " aborting transaction!");
        }
        first = false;
        // (presumably) abort the outstanding transaction
        db.endTransaction();
      }
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("closeDatabase", appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null ) {
        try {
          // release the reference...
          // this will not close the db handle
          // the AppNameSharedStateContainer still holds a reference.
          db.releaseReference();
        } finally {
          // this will release the database from the AppNameSharedStateContainer...
          // this may also not close the connection -- it may be held open by a cursor
          OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().releaseDatabase(appName, dbHandleName);
        }
      }
    }
  }

  @Override
  public void closeTransaction(String appName, OdkDbHandle dbHandleName, boolean successful) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().getConnection(appName, dbHandleName);
      if ( successful ) {
        db.setTransactionSuccessful();
      }
      db.endTransaction();
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("closeTransaction", appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null ) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override
  public void closeTransactionAndDatabase(String appName, OdkDbHandle dbHandleName, boolean successful)
      throws RemoteException {

     // helpful RPC wrapper for calling two methods in succession
     closeTransaction(appName, dbHandleName, successful);
     closeDatabase(appName, dbHandleName);
  }

  @Override
  public void changeDataRowsToNewRowState(String appName, OdkDbHandle dbHandleName, String tableId)
      throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().getConnection(appName, dbHandleName);
      ODKDatabaseImplUtils.get().changeDataRowsToNewRowState(db, tableId);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("changeDataRowsToNewRowState", appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null ) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override
  public OrderedColumns createOrOpenDBTableWithColumns(String appName, OdkDbHandle dbHandleName, String tableId,
      ColumnList columns) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().getConnection(appName, dbHandleName);
      return ODKDatabaseImplUtils.get().createOrOpenDBTableWithColumns(db, appName, tableId, columns.getColumns());
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("createOrOpenDBTableWithColumns", appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null ) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override
  public void deleteCheckpointRowsWithId(String appName, OdkDbHandle dbHandleName, String tableId, String rowId)
      throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().getConnection(appName, dbHandleName);
      ODKDatabaseImplUtils.get().deleteCheckpointRowsWithId(db, appName, tableId, rowId);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("deleteCheckpointRowsWithId", appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null ) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override
  public void deleteLastCheckpointRowWithId(String appName, OdkDbHandle dbHandleName, String tableId, String rowId)
          throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().getConnection(appName, dbHandleName);
      ODKDatabaseImplUtils.get().deleteLastCheckpointRowWithId(db, tableId, rowId);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("deleteLastCheckpointRowWithId", appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null ) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override
  public void deleteDBTableAndAllData(String appName, OdkDbHandle dbHandleName, String tableId) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().getConnection(appName, dbHandleName);
      ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, appName, tableId);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("deleteDBTableAndAllDat", appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null ) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override
  public void deleteDBTableMetadata(String appName, OdkDbHandle dbHandleName, String tableId, String partition,
      String aspect, String key) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().getConnection(appName, dbHandleName);
      ODKDatabaseImplUtils.get().deleteDBTableMetadata(db, tableId, partition, aspect, key);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("deleteDBTableMetadata", appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null ) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override
  public void deleteDataInExistingDBTableWithId(String appName, OdkDbHandle dbHandleName, String tableId, String rowId)
      throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().getConnection(appName, dbHandleName);
      ODKDatabaseImplUtils.get().deleteDataInExistingDBTableWithId(db, appName, tableId, rowId);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("deleteDataInExistingDBTableWithId", appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null ) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override
  public void deleteServerConflictRowWithId(String appName, OdkDbHandle dbHandleName, String tableId, String rowId)
      throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().getConnection(appName, dbHandleName);
      ODKDatabaseImplUtils.get().deleteServerConflictRowWithId(db, tableId, rowId);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("deleteServerConflictRowWithId", appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null ) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override
  public void enforceTypesDBTableMetadata(String appName, OdkDbHandle dbHandleName, String tableId)
      throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().getConnection(appName, dbHandleName);
      ODKDatabaseImplUtils.get().enforceTypesDBTableMetadata(db, tableId);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("enforceTypesDBTableMetadata", appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null ) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
    
  }

  @Override
  public String[] getAdminColumns() throws RemoteException {

    List<String> cols = ODKDatabaseImplUtils.get().getAdminColumns();
    return cols.toArray(new String[cols.size()]);

  }

  @Override
  public String[] getAllColumnNames(String appName, OdkDbHandle dbHandleName, String tableId) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().getConnection(appName, dbHandleName);
      return ODKDatabaseImplUtils.get().getAllColumnNames(db, tableId);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("getAllColumnNames", appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null ) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }

  }

  @Override
  public List<String> getAllTableIds(String appName, OdkDbHandle dbHandleName) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().getConnection(appName, dbHandleName);
      return ODKDatabaseImplUtils.get().getAllTableIds(db);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("getAllTableIds", appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null ) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override
  public UserTable getDataInExistingDBTableWithId(String appName, OdkDbHandle dbHandleName,
      String tableId, OrderedColumns orderedDefns, String rowId) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().getConnection(appName, dbHandleName);
      return ODKDatabaseImplUtils.get().getDataInExistingDBTableWithId(db, appName, tableId, orderedDefns, rowId);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("getDataInExistingDBTableWithId", appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null ) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override
  public List<KeyValueStoreEntry> getDBTableMetadata(String appName, OdkDbHandle dbHandleName,
      String tableId, String partition, String aspect, String key) throws RemoteException {

    OdkConnectionInterface db = null;

    ArrayList<KeyValueStoreEntry> kvsEntries = null;
    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().getConnection(appName, dbHandleName);
      kvsEntries = ODKDatabaseImplUtils.get().getDBTableMetadata(db, tableId, partition, aspect, key);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("getDBTableMetadata", appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null ) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }

    return kvsEntries;
  }

  @Override
  public List<TableHealthInfo> getTableHealthStatuses(String appName, OdkDbHandle dbHandleName) throws RemoteException {

    long now = System.currentTimeMillis();
    WebLogger.getLogger(appName).i("getTableHealthStatuses", appName + " " + dbHandleName.getDatabaseHandle() + " " +
            "getTableHealthStatuses -- searching for conflicts and checkpoints ");

    ArrayList<TableHealthInfo> problems = new ArrayList<TableHealthInfo>();
    OdkConnectionInterface db = null;
    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().getConnection(appName, dbHandleName);
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
      WebLogger.getLogger(appName).i("getTableHealthStatuses", appName + " " + dbHandleName.getDatabaseHandle() + " " +
              "getTableHealthStatuses -- full table scan completed: " + Long.toString(elapsed) + " ms");

      return problems;
    } catch (Throwable t) {
      WebLogger.getLogger(appName).e("getTableHealthStatuses", "exception during processing");
      WebLogger.getLogger(appName).printStackTrace(t);
      throw t;
    } finally {
      if ( db != null ) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override
  public String[] getExportColumns() throws RemoteException {
    List<String> exports = ODKDatabaseImplUtils.get().getExportColumns();
    return exports.toArray(new String[exports.size()]);
  }

  @Override
  public String getSyncState(String appName, OdkDbHandle dbHandleName, String tableId, String rowId)
      throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().getConnection(appName, dbHandleName);
      SyncState state = ODKDatabaseImplUtils.get().getSyncState(db, appName, tableId, rowId);
      return state.name();
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("getSyncState", appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null ) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override
  public TableDefinitionEntry getTableDefinitionEntry(String appName, OdkDbHandle dbHandleName, String tableId)
      throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().getConnection(appName, dbHandleName);
      return ODKDatabaseImplUtils.get().getTableDefinitionEntry(db, tableId);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("getTableDefinitionEntry", appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null ) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override
  public OrderedColumns getUserDefinedColumns(String appName, OdkDbHandle dbHandleName, String tableId)
      throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().getConnection(appName, dbHandleName);
      return ODKDatabaseImplUtils.get().getUserDefinedColumns(db, appName, tableId);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("getUserDefinedColumns", appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null ) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override
  public boolean hasTableId(String appName, OdkDbHandle dbHandleName, String tableId) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().getConnection(appName, dbHandleName);
      return ODKDatabaseImplUtils.get().hasTableId(db, tableId);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("hasTableId", appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null ) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override
  public void insertCheckpointRowIntoExistingDBTableWithId(String appName, OdkDbHandle dbHandleName,
      String tableId, OrderedColumns orderedColumns, ContentValues cvValues, String rowId) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().getConnection(appName, dbHandleName);
      ODKDatabaseImplUtils.get().insertCheckpointRowIntoExistingDBTableWithId(db, tableId, orderedColumns, cvValues, rowId);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("insertCheckpointRowIntoExistingDBTableWithId", appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null ) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override
  public void insertDataIntoExistingDBTableWithId(String appName, OdkDbHandle dbHandleName, String tableId,
      OrderedColumns orderedColumns, ContentValues cvValues, String rowId) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().getConnection(appName, dbHandleName);
      ODKDatabaseImplUtils.get().insertDataIntoExistingDBTableWithId(db, tableId, orderedColumns, cvValues, rowId);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("insertDataIntoExistingDBTableWithId", appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null ) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override
  public void placeRowIntoConflict(String appName, OdkDbHandle dbHandleName, String tableId, String rowId, int conflictType)
      throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().getConnection(appName, dbHandleName);
      ODKDatabaseImplUtils.get().placeRowIntoConflict(db, tableId, rowId, conflictType);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("placeRowIntoConflict", appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null ) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override
  public UserTable rawSqlQuery(String appName, OdkDbHandle dbHandleName, String tableId,
      OrderedColumns columnDefns, String whereClause, String[] selectionArgs,
      String[] groupBy, String having, String orderByElementKey, String orderByDirection)
      throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().getConnection(appName, dbHandleName);
      return ODKDatabaseImplUtils.get().rawSqlQuery(db, appName, tableId,
          columnDefns, whereClause, selectionArgs,
          groupBy, having, orderByElementKey, orderByDirection);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("rawSqlQuery", appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null ) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override
  public void replaceDBTableMetadata(String appName, OdkDbHandle dbHandleName, KeyValueStoreEntry entry)
      throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().getConnection(appName, dbHandleName);
      ODKDatabaseImplUtils.get().replaceDBTableMetadata(db, entry);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("replaceDBTableMetadata", appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null ) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override
  public void replaceDBTableMetadataList(String appName, OdkDbHandle dbHandleName, String tableId,
      List<KeyValueStoreEntry> entries, boolean clear) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().getConnection(appName, dbHandleName);
      ODKDatabaseImplUtils.get().replaceDBTableMetadata(db, tableId, entries, clear);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("replaceDBTableMetadataList", appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null ) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override
  public void restoreRowFromConflict(String appName, OdkDbHandle dbHandleName, String tableId, String rowId,
      String syncState, int conflictType) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().getConnection(appName, dbHandleName);
      ODKDatabaseImplUtils.get().restoreRowFromConflict(db, tableId, rowId,
              SyncState.valueOf(syncState), conflictType);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("restoreRowFromConflict", appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null ) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override
  public void saveAsIncompleteMostRecentCheckpointDataInDBTableWithId(String appName, OdkDbHandle dbHandleName,
      String tableId, String rowId) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().getConnection(appName, dbHandleName);
      ODKDatabaseImplUtils.get().saveAsIncompleteMostRecentCheckpointDataInDBTableWithId(db, tableId, rowId);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("saveAsIncompleteMostRecentCheckpointDataInDBTableWithId", appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null ) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override
  public void saveAsCompleteMostRecentCheckpointDataInDBTableWithId(String appName, OdkDbHandle dbHandleName,
                                                                      String tableId, String rowId) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().getConnection(appName, dbHandleName);
      ODKDatabaseImplUtils.get().saveAsCompleteMostRecentCheckpointDataInDBTableWithId(db, tableId, rowId);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("saveAsCompleteMostRecentCheckpointDataInDBTableWithId", appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null ) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override
  public void updateDBTableETags(String appName, OdkDbHandle dbHandleName, String tableId, String schemaETag,
      String lastDataETag)
      throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().getConnection(appName, dbHandleName);
      ODKDatabaseImplUtils.get().updateDBTableETags(db, tableId, schemaETag, lastDataETag);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("updateDBTableETags", appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null ) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override
  public void updateDBTableLastSyncTime(String appName, OdkDbHandle dbHandleName, String tableId)
      throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().getConnection(appName, dbHandleName);
      ODKDatabaseImplUtils.get().updateDBTableLastSyncTime(db, tableId);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("updateDBTableLastSyncTime", appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null ) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override
  public void updateDataInExistingDBTableWithId(String appName, OdkDbHandle dbHandleName, String tableId,
      OrderedColumns orderedColumns, ContentValues cvValues, String rowId) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().getConnection(appName, dbHandleName);
      ODKDatabaseImplUtils.get().updateDataInExistingDBTableWithId(db, tableId, orderedColumns, cvValues, rowId);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("updateDataInExistingDBTableWithId", appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null ) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override
  public void updateRowETagAndSyncState(String appName, OdkDbHandle dbHandleName, String tableId, String rowId,
      String rowETag, String syncState) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().getConnection(appName, dbHandleName);
      ODKDatabaseImplUtils.get().updateRowETagAndSyncState(db, tableId, rowId, rowETag, SyncState.valueOf(syncState));
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("updateRowETagAndSyncState", appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null ) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override
  public void deleteAllSyncETagsForTableId(String appName, OdkDbHandle dbHandleName, String tableId) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().getConnection(appName, dbHandleName);
      SyncETagsUtils seu = new SyncETagsUtils();
      seu.deleteAllSyncETagsForTableId(db, tableId);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("deleteAllSyncETagsForTableId", appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null ) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override
  public void deleteAllSyncETagsExceptForServer(String appName, OdkDbHandle dbHandleName, String verifiedUri)
      throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().getConnection(appName, dbHandleName);
      SyncETagsUtils seu = new SyncETagsUtils();
      seu.deleteAllSyncETagsExceptForServer(db, verifiedUri);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("deleteAllSyncETagsExceptForServer", appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null ) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override
  public void deleteAllSyncETagsUnderServer(String appName, OdkDbHandle dbHandleName, String verifiedUri)
      throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().getConnection(appName, dbHandleName);
      SyncETagsUtils seu = new SyncETagsUtils();
      seu.deleteAllSyncETagsUnderServer(db, verifiedUri);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("deleteAllSyncETagsUnderServer", appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null ) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override
  public String getFileSyncETag(String appName, OdkDbHandle dbHandleName, String verifiedUri, String tableId, long modificationTimestamp)
      throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().getConnection(appName, dbHandleName);
      SyncETagsUtils seu = new SyncETagsUtils();
      return seu.getFileSyncETag(db, verifiedUri, tableId, modificationTimestamp);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("getFileSyncETag", appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null ) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override
  public String getManifestSyncETag(String appName, OdkDbHandle dbHandleName, String verifiedUri, String tableId)
      throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().getConnection(appName, dbHandleName);
      SyncETagsUtils seu = new SyncETagsUtils();
      return seu.getManifestSyncETag(db, verifiedUri, tableId);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("getManifestSyncETag", appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null ) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override
  public void updateFileSyncETag(String appName, OdkDbHandle dbHandleName, String verifiedUri, String tableId,
      long modificationTimestamp, String eTag) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().getConnection(appName, dbHandleName);
      SyncETagsUtils seu = new SyncETagsUtils();
      seu.updateFileSyncETag(db, verifiedUri, tableId, modificationTimestamp, eTag);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("updateFileSyncETag", appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null ) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

  @Override
  public void updateManifestSyncETag(String appName, OdkDbHandle dbHandleName, String verifiedUri, String tableId,
      String eTag) throws RemoteException {

    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().getConnection(appName, dbHandleName);
      SyncETagsUtils seu = new SyncETagsUtils();
      seu.updateManifestSyncETag(db, verifiedUri, tableId, eTag);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("updateManifestSyncETag", appName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null ) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
  }

}
