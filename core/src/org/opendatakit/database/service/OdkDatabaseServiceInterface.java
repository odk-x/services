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

import java.util.ArrayList;
import java.util.List;

import org.opendatakit.aggregate.odktables.rest.SyncState;
import org.opendatakit.common.android.data.ColumnList;
import org.opendatakit.common.android.data.OrderedColumns;
import org.opendatakit.common.android.data.TableDefinitionEntry;
import org.opendatakit.common.android.data.UserTable;
import org.opendatakit.common.android.database.DatabaseFactory;
import org.opendatakit.common.android.database.OdkDatabase;
import org.opendatakit.common.android.utilities.ODKCursorUtils;
import org.opendatakit.common.android.utilities.ODKDatabaseImplUtils;
import org.opendatakit.common.android.utilities.SyncETagsUtils;
import org.opendatakit.common.android.utilities.WebLogger;
import org.opendatakit.core.application.Core;

import android.content.ContentValues;
import android.os.RemoteException;

public class OdkDatabaseServiceInterface extends OdkDbInterface.Stub {
  
  private static final String LOGTAG = OdkDatabaseServiceInterface.class.getSimpleName();

  /**
   * 
   */
  private final OdkDatabaseService odkDatabaseService;

  /**
   * @param odkDbShimService
   */
  OdkDatabaseServiceInterface(OdkDatabaseService odkDatabaseService) {
    this.odkDatabaseService = odkDatabaseService;
  }

  @Override
  public OdkDbHandle openDatabase(String appName, boolean beginTransaction) throws RemoteException {

    if (Core.getInstance().shouldWaitForDebugger()) {
      android.os.Debug.waitForDebugger();
    }

    OdkDatabase db = null;

    OdkDbHandle dbHandleName = DatabaseFactory.get().generateDatabaseServiceDbHandle();
    try {
      db = DatabaseFactory.get().getDatabase(odkDatabaseService.getApplicationContext(), appName, dbHandleName);
      if ( db.isOpen() ) {
        if ( beginTransaction ) {
          ODKDatabaseImplUtils.get().beginTransactionNonExclusive(db);
        }
        return dbHandleName;
      }
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("openDatabase", msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    }
    throw new RemoteException("Unable to open database");
  }
  
  @Override
  public void beginTransaction(String appName, OdkDbHandle dbHandleName) throws RemoteException {

    OdkDatabase db = null;

    try {
      db = DatabaseFactory.get().getDatabase(odkDatabaseService.getApplicationContext(), appName, dbHandleName);
      ODKDatabaseImplUtils.get().beginTransactionNonExclusive(db);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("openDatabase", msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null && db.isOpen() ) {
        // release the reference...
        // this does not necessarily close the db handle 
        // or terminate any pending transaction
        db.close();
      }
    }
  }

  @Override
  public void closeDatabase(String appName, OdkDbHandle dbHandleName) throws RemoteException {

    OdkDatabase db = null;

    try {
      db = DatabaseFactory.get().getDatabase(odkDatabaseService.getApplicationContext(), appName, dbHandleName);
      db.close();
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("changeDataRowsToNewRowState", msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      while ( db != null && db.isOpen() ) {
        // release the reference...
        // this does not necessarily close the db handle 
        // or terminate any pending transaction
        db.close();
      }
    }
    DatabaseFactory.get().releaseDatabase(odkDatabaseService.getApplicationContext(), appName, dbHandleName);
  }

  @Override
  public void closeTransaction(String appName, OdkDbHandle dbHandleName, boolean successful) throws RemoteException {

    OdkDatabase db = null;

    try {
      db = DatabaseFactory.get().getDatabase(odkDatabaseService.getApplicationContext(), appName, dbHandleName);
      if ( successful ) {
        db.setTransactionSuccessful();
      }
      db.endTransaction();
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("changeDataRowsToNewRowState", msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null && db.isOpen() ) {
        // release the reference...
        // this does not necessarily close the db handle 
        // or terminate any pending transaction
        db.close();
      }
    }
  }

  @Override
  public void closeTransactionAndDatabase(String appName, OdkDbHandle dbHandleName, boolean successful)
      throws RemoteException {

    OdkDatabase db = null;

    try {
      db = DatabaseFactory.get().getDatabase(odkDatabaseService.getApplicationContext(), appName, dbHandleName);
      if ( successful ) {
        db.setTransactionSuccessful();
      }
      db.endTransaction();
      db.close();
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("changeDataRowsToNewRowState", msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      while ( db != null && db.isOpen() ) {
        // release the reference...
        // this does not necessarily close the db handle 
        // or terminate any pending transaction
        db.close();
      }
    }
    DatabaseFactory.get().releaseDatabase(odkDatabaseService.getApplicationContext(), appName, dbHandleName);
  }

  @Override
  public void changeDataRowsToNewRowState(String appName, OdkDbHandle dbHandleName, String tableId)
      throws RemoteException {

    OdkDatabase db = null;

    try {
      db = DatabaseFactory.get().getDatabase(odkDatabaseService.getApplicationContext(), appName, dbHandleName);
      ODKDatabaseImplUtils.get().changeDataRowsToNewRowState(db, tableId);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("changeDataRowsToNewRowState", msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null && db.isOpen() ) {
        // release the reference...
        // this does not necessarily close the db handle 
        // or terminate any pending transaction
        db.close();
      }
    }
  }

  @Override
  public OrderedColumns createOrOpenDBTableWithColumns(String appName, OdkDbHandle dbHandleName, String tableId,
      ColumnList columns) throws RemoteException {

    OdkDatabase db = null;

    try {
      db = DatabaseFactory.get().getDatabase(odkDatabaseService.getApplicationContext(), appName, dbHandleName);
      return ODKDatabaseImplUtils.get().createOrOpenDBTableWithColumns(db, appName, tableId, columns.getColumns());
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("createOrOpenDBTableWithColumns", msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null && db.isOpen() ) {
        // release the reference...
        // this does not necessarily close the db handle 
        // or terminate any pending transaction
        db.close();
      }
    }
  }

  @Override
  public void deleteCheckpointRowsWithId(String appName, OdkDbHandle dbHandleName, String tableId, String rowId)
      throws RemoteException {

    OdkDatabase db = null;

    try {
      db = DatabaseFactory.get().getDatabase(odkDatabaseService.getApplicationContext(), appName, dbHandleName);
      ODKDatabaseImplUtils.get().deleteCheckpointRowsWithId(db, appName, tableId, rowId);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("deleteCheckpointRowsWithId", msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null && db.isOpen() ) {
        // release the reference...
        // this does not necessarily close the db handle 
        // or terminate any pending transaction
        db.close();
      }
    }
  }

  @Override
  public void deleteDBTableAndAllData(String appName, OdkDbHandle dbHandleName, String tableId) throws RemoteException {

    OdkDatabase db = null;

    try {
      db = DatabaseFactory.get().getDatabase(odkDatabaseService.getApplicationContext(), appName, dbHandleName);
      ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, appName, tableId);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("deleteDBTableAndAllData", msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null && db.isOpen() ) {
        // release the reference...
        // this does not necessarily close the db handle 
        // or terminate any pending transaction
        db.close();
      }
    }
  }

  @Override
  public void deleteDBTableMetadata(String appName, OdkDbHandle dbHandleName, String tableId, String partition,
      String aspect, String key) throws RemoteException {

    OdkDatabase db = null;

    try {
      db = DatabaseFactory.get().getDatabase(odkDatabaseService.getApplicationContext(), appName, dbHandleName);
      ODKDatabaseImplUtils.get().deleteDBTableMetadata(db, tableId, partition, aspect, key);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("deleteDBTableMetadata", msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null && db.isOpen() ) {
        // release the reference...
        // this does not necessarily close the db handle 
        // or terminate any pending transaction
        db.close();
      }
    }
  }

  @Override
  public void deleteDataInExistingDBTableWithId(String appName, OdkDbHandle dbHandleName, String tableId, String rowId)
      throws RemoteException {

    OdkDatabase db = null;

    try {
      db = DatabaseFactory.get().getDatabase(odkDatabaseService.getApplicationContext(), appName, dbHandleName);
      ODKDatabaseImplUtils.get().deleteDataInExistingDBTableWithId(db, appName, tableId, rowId);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("deleteDataInExistingDBTableWithId", msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null && db.isOpen() ) {
        // release the reference...
        // this does not necessarily close the db handle 
        // or terminate any pending transaction
        db.close();
      }
    }
  }

  @Override
  public void deleteServerConflictRowWithId(String appName, OdkDbHandle dbHandleName, String tableId, String rowId)
      throws RemoteException {

    OdkDatabase db = null;

    try {
      db = DatabaseFactory.get().getDatabase(odkDatabaseService.getApplicationContext(), appName, dbHandleName);
      ODKDatabaseImplUtils.get().deleteServerConflictRowWithId(db, tableId, rowId);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("deleteServerConflictRowWithId", msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null && db.isOpen() ) {
        // release the reference...
        // this does not necessarily close the db handle 
        // or terminate any pending transaction
        db.close();
      }
    }
  }

  @Override
  public void enforceTypesDBTableMetadata(String appName, OdkDbHandle dbHandleName, String tableId)
      throws RemoteException {

    OdkDatabase db = null;

    try {
      db = DatabaseFactory.get().getDatabase(odkDatabaseService.getApplicationContext(), appName, dbHandleName);
      ODKDatabaseImplUtils.get().enforceTypesDBTableMetadata(db, tableId);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("enforceTypesDBTableMetadata", msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null && db.isOpen() ) {
        // release the reference...
        // this does not necessarily close the db handle 
        // or terminate any pending transaction
        db.close();
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

    OdkDatabase db = null;

    try {
      db = DatabaseFactory.get().getDatabase(odkDatabaseService.getApplicationContext(), appName, dbHandleName);
      return ODKDatabaseImplUtils.get().getAllColumnNames(db, tableId);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("getAllColumnNames", msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null && db.isOpen() ) {
        // release the reference...
        // this does not necessarily close the db handle 
        // or terminate any pending transaction
        db.close();
      }
    }

  }

  @Override
  public List<String> getAllTableIds(String appName, OdkDbHandle dbHandleName) throws RemoteException {

    OdkDatabase db = null;

    try {
      db = DatabaseFactory.get().getDatabase(odkDatabaseService.getApplicationContext(), appName, dbHandleName);
      return ODKDatabaseImplUtils.get().getAllTableIds(db);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("getAllTableIds", msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null && db.isOpen() ) {
        // release the reference...
        // this does not necessarily close the db handle 
        // or terminate any pending transaction
        db.close();
      }
    }
  }

  @Override
  public UserTable getDataInExistingDBTableWithId(String appName, OdkDbHandle dbHandleName,
      String tableId, OrderedColumns orderedDefns, String rowId) throws RemoteException {

    OdkDatabase db = null;

    try {
      db = DatabaseFactory.get().getDatabase(odkDatabaseService.getApplicationContext(), appName, dbHandleName);
      return ODKDatabaseImplUtils.get().getDataInExistingDBTableWithId(db, appName, tableId, orderedDefns, rowId);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("getDataInExistingDBTableWithId", msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null && db.isOpen() ) {
        // release the reference...
        // this does not necessarily close the db handle 
        // or terminate any pending transaction
        db.close();
      }
    }
  }

  @Override
  public List<KeyValueStoreEntry> getDBTableMetadata(String appName, OdkDbHandle dbHandleName,
      String tableId, String partition, String aspect, String key) throws RemoteException {

    OdkDatabase db = null;

    ArrayList<KeyValueStoreEntry> kvsEntries = null;
    try {
      db = DatabaseFactory.get().getDatabase(odkDatabaseService.getApplicationContext(), appName, dbHandleName);
      kvsEntries = ODKDatabaseImplUtils.get().getDBTableMetadata(db, tableId, partition, aspect, key);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("getDBTableMetadata", msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null && db.isOpen() ) {
        db.close();
      }
    }

    return kvsEntries;
  }

  @Override
  public List<TableHealthInfo> getTableHealthStatuses(String appName, OdkDbHandle dbHandleName) throws RemoteException {

    long now = System.currentTimeMillis();
    WebLogger.getLogger(appName).i(LOGTAG,
        "getTableHealthStatuses -- searching for conflicts and checkpoints ");

    ArrayList<TableHealthInfo> problems = new ArrayList<TableHealthInfo>();
    OdkDatabase db = null;
    try {
      db = DatabaseFactory.get().getDatabase(odkDatabaseService.getApplicationContext(), appName, dbHandleName);
      ArrayList<String> tableIds = ODKDatabaseImplUtils.get().getAllTableIds(db);

      for (String tableId : tableIds) {
        int health = ODKDatabaseImplUtils.get().getTableHealth(db, tableId);
        if ( health != ODKCursorUtils.TABLE_HEALTH_IS_CLEAN ) {
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
      WebLogger.getLogger(appName).i(LOGTAG,
          "getTableHealthStatuses -- full table scan completed: " + Long.toString(elapsed) + " ms");

      return problems;
    } finally {
      if ( db != null && db.isOpen() ) {
        db.close();
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

    OdkDatabase db = null;

    try {
      db = DatabaseFactory.get().getDatabase(odkDatabaseService.getApplicationContext(), appName, dbHandleName);
      SyncState state = ODKDatabaseImplUtils.get().getSyncState(db, appName, tableId, rowId);
      return state.name();
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("getSyncState", msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null && db.isOpen() ) {
        db.close();
      }
    }
  }

  @Override
  public TableDefinitionEntry getTableDefinitionEntry(String appName, OdkDbHandle dbHandleName, String tableId)
      throws RemoteException {

    OdkDatabase db = null;

    try {
      db = DatabaseFactory.get().getDatabase(odkDatabaseService.getApplicationContext(), appName, dbHandleName);
      return ODKDatabaseImplUtils.get().getTableDefinitionEntry(db, tableId);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("getTableDefinitionEntry", msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null && db.isOpen() ) {
        db.close();
      }
    }
  }

  @Override
  public OrderedColumns getUserDefinedColumns(String appName, OdkDbHandle dbHandleName, String tableId)
      throws RemoteException {

    OdkDatabase db = null;

    try {
      db = DatabaseFactory.get().getDatabase(odkDatabaseService.getApplicationContext(), appName, dbHandleName);
      return ODKDatabaseImplUtils.get().getUserDefinedColumns(db, appName, tableId);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("getUserDefinedColumns", msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null && db.isOpen() ) {
        db.close();
      }
    }
  }

  @Override
  public boolean hasTableId(String appName, OdkDbHandle dbHandleName, String tableId) throws RemoteException {

    OdkDatabase db = null;

    try {
      db = DatabaseFactory.get().getDatabase(odkDatabaseService.getApplicationContext(), appName, dbHandleName);
      return ODKDatabaseImplUtils.get().hasTableId(db, tableId);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("hasTableId", msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null && db.isOpen() ) {
        db.close();
      }
    }
  }

  @Override
  public void insertDataIntoExistingDBTableWithId(String appName, OdkDbHandle dbHandleName, String tableId,
      OrderedColumns orderedColumns, ContentValues cvValues, String rowId) throws RemoteException {

    OdkDatabase db = null;

    try {
      db = DatabaseFactory.get().getDatabase(odkDatabaseService.getApplicationContext(), appName, dbHandleName);
      ODKDatabaseImplUtils.get().insertDataIntoExistingDBTableWithId(db, tableId, orderedColumns, cvValues, rowId);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("insertDataIntoExistingDBTableWithId", msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null && db.isOpen() ) {
        db.close();
      }
    }
  }

  @Override
  public void placeRowIntoConflict(String appName, OdkDbHandle dbHandleName, String tableId, String rowId, int conflictType)
      throws RemoteException {

    OdkDatabase db = null;

    try {
      db = DatabaseFactory.get().getDatabase(odkDatabaseService.getApplicationContext(), appName, dbHandleName);
      ODKDatabaseImplUtils.get().placeRowIntoConflict(db, tableId, rowId, conflictType);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("placeRowIntoConflict", msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null && db.isOpen() ) {
        db.close();
      }
    }
  }

  @Override
  public UserTable rawSqlQuery(String appName, OdkDbHandle dbHandleName, String tableId,
      OrderedColumns columnDefns, String whereClause, String[] selectionArgs,
      String[] groupBy, String having, String orderByElementKey, String orderByDirection)
      throws RemoteException {

    OdkDatabase db = null;

    try {
      db = DatabaseFactory.get().getDatabase(odkDatabaseService.getApplicationContext(), appName, dbHandleName);
      return ODKDatabaseImplUtils.get().rawSqlQuery(db, appName, tableId,
          columnDefns, whereClause, selectionArgs,
          groupBy, having, orderByElementKey, orderByDirection);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("rawSqlQuery", msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null && db.isOpen() ) {
        db.close();
      }
    }
  }

  @Override
  public void replaceDBTableMetadata(String appName, OdkDbHandle dbHandleName, KeyValueStoreEntry entry)
      throws RemoteException {

    OdkDatabase db = null;

    try {
      db = DatabaseFactory.get().getDatabase(odkDatabaseService.getApplicationContext(), appName, dbHandleName);
      ODKDatabaseImplUtils.get().replaceDBTableMetadata(db, entry);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("replaceDBTableMetadata", msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null && db.isOpen() ) {
        db.close();
      }
    }
  }

  @Override
  public void replaceDBTableMetadataList(String appName, OdkDbHandle dbHandleName, String tableId,
      List<KeyValueStoreEntry> entries, boolean clear) throws RemoteException {

    OdkDatabase db = null;

    try {
      db = DatabaseFactory.get().getDatabase(odkDatabaseService.getApplicationContext(), appName, dbHandleName);
      ODKDatabaseImplUtils.get().replaceDBTableMetadata(db, tableId, entries, clear);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("replaceDBTableMetadataList", msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null && db.isOpen() ) {
        db.close();
      }
    }
  }

  @Override
  public void restoreRowFromConflict(String appName, OdkDbHandle dbHandleName, String tableId, String rowId,
      String syncState, int conflictType) throws RemoteException {

    OdkDatabase db = null;

    try {
      db = DatabaseFactory.get().getDatabase(odkDatabaseService.getApplicationContext(), appName, dbHandleName);
      ODKDatabaseImplUtils.get().restoreRowFromConflict(db, tableId, rowId,
          SyncState.valueOf(syncState), conflictType);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("restoreRowFromConflict", msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null && db.isOpen() ) {
        db.close();
      }
    }
  }

  @Override
  public void saveAsIncompleteMostRecentCheckpointDataInDBTableWithId(String appName, OdkDbHandle dbHandleName,
      String tableId, String rowId) throws RemoteException {

    OdkDatabase db = null;

    try {
      db = DatabaseFactory.get().getDatabase(odkDatabaseService.getApplicationContext(), appName, dbHandleName);
      ODKDatabaseImplUtils.get().saveAsIncompleteMostRecentCheckpointDataInDBTableWithId(db, tableId, rowId);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("saveAsIncompleteMostRecentCheckpointDataInDBTableWithId", msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null && db.isOpen() ) {
        db.close();
      }
    }
  }

  @Override
  public void updateDBTableETags(String appName, OdkDbHandle dbHandleName, String tableId, String schemaETag,
      String lastDataETag)
      throws RemoteException {

    OdkDatabase db = null;

    try {
      db = DatabaseFactory.get().getDatabase(odkDatabaseService.getApplicationContext(), appName, dbHandleName);
      ODKDatabaseImplUtils.get().updateDBTableETags(db, tableId, schemaETag, lastDataETag);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("updateDBTableETags", msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null && db.isOpen() ) {
        db.close();
      }
    }
  }

  @Override
  public void updateDBTableLastSyncTime(String appName, OdkDbHandle dbHandleName, String tableId)
      throws RemoteException {

    OdkDatabase db = null;

    try {
      db = DatabaseFactory.get().getDatabase(odkDatabaseService.getApplicationContext(), appName, dbHandleName);
      ODKDatabaseImplUtils.get().updateDBTableLastSyncTime(db, tableId);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("updateDBTableLastSyncTime", msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null && db.isOpen() ) {
        db.close();
      }
    }
  }

  @Override
  public void updateDataInExistingDBTableWithId(String appName, OdkDbHandle dbHandleName, String tableId,
      OrderedColumns orderedColumns, ContentValues cvValues, String rowId) throws RemoteException {

    OdkDatabase db = null;

    try {
      db = DatabaseFactory.get().getDatabase(odkDatabaseService.getApplicationContext(), appName, dbHandleName);
      ODKDatabaseImplUtils.get().updateDataInExistingDBTableWithId(db, tableId, orderedColumns, cvValues, rowId);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("updateDataInExistingDBTableWithId", msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null && db.isOpen() ) {
        db.close();
      }
    }
  }

  @Override
  public void updateRowETagAndSyncState(String appName, OdkDbHandle dbHandleName, String tableId, String rowId,
      String rowETag, String syncState) throws RemoteException {

    OdkDatabase db = null;

    try {
      db = DatabaseFactory.get().getDatabase(odkDatabaseService.getApplicationContext(), appName, dbHandleName);
      ODKDatabaseImplUtils.get().updateRowETagAndSyncState(db, tableId, rowId, rowETag, SyncState.valueOf(syncState));
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("updateRowETagAndSyncState", msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null && db.isOpen() ) {
        db.close();
      }
    }
  }

  @Override
  public void deleteAllSyncETagsForTableId(String appName, OdkDbHandle dbHandleName, String tableId) throws RemoteException {

    OdkDatabase db = null;

    try {
      SyncETagsUtils seu = new SyncETagsUtils();
      db = DatabaseFactory.get().getDatabase(odkDatabaseService.getApplicationContext(), appName, dbHandleName);
      seu.deleteAllSyncETagsForTableId(db, tableId);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("deleteAllSyncETags", msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null && db.isOpen() ) {
        db.close();
      }
    }
  }

  @Override
  public void deleteAllSyncETagsExceptForServer(String appName, OdkDbHandle dbHandleName, String verifiedUri)
      throws RemoteException {

    OdkDatabase db = null;

    try {
      SyncETagsUtils seu = new SyncETagsUtils();
      db = DatabaseFactory.get().getDatabase(odkDatabaseService.getApplicationContext(), appName, dbHandleName);
      seu.deleteAllSyncETagsExceptForServer(db, verifiedUri);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("deleteAllSyncETags", msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null && db.isOpen() ) {
        db.close();
      }
    }
  }

  @Override
  public void deleteAllSyncETagsUnderServer(String appName, OdkDbHandle dbHandleName, String verifiedUri)
      throws RemoteException {

    OdkDatabase db = null;

    try {
      SyncETagsUtils seu = new SyncETagsUtils();
      db = DatabaseFactory.get().getDatabase(odkDatabaseService.getApplicationContext(), appName, dbHandleName);
      seu.deleteAllSyncETagsUnderServer(db, verifiedUri);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("deleteAllSyncETags", msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null && db.isOpen() ) {
        db.close();
      }
    }
  }

  @Override
  public String getFileSyncETag(String appName, OdkDbHandle dbHandleName, String verifiedUri, String tableId, long modificationTimestamp)
      throws RemoteException {

    OdkDatabase db = null;

    try {
      SyncETagsUtils seu = new SyncETagsUtils();
      db = DatabaseFactory.get().getDatabase(odkDatabaseService.getApplicationContext(), appName, dbHandleName);
      return seu.getFileSyncETag(db, verifiedUri, tableId, modificationTimestamp);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("deleteAllSyncETags", msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null && db.isOpen() ) {
        db.close();
      }
    }
  }

  @Override
  public String getManifestSyncETag(String appName, OdkDbHandle dbHandleName, String verifiedUri, String tableId)
      throws RemoteException {

    OdkDatabase db = null;

    try {
      SyncETagsUtils seu = new SyncETagsUtils();
      db = DatabaseFactory.get().getDatabase(odkDatabaseService.getApplicationContext(), appName, dbHandleName);
      return seu.getManifestSyncETag(db, verifiedUri, tableId);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("deleteAllSyncETags", msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null && db.isOpen() ) {
        db.close();
      }
    }
  }

  @Override
  public void updateFileSyncETag(String appName, OdkDbHandle dbHandleName, String verifiedUri, String tableId,
      long modificationTimestamp, String eTag) throws RemoteException {

    OdkDatabase db = null;

    try {
      SyncETagsUtils seu = new SyncETagsUtils();
      db = DatabaseFactory.get().getDatabase(odkDatabaseService.getApplicationContext(), appName, dbHandleName);
      seu.updateFileSyncETag(db, verifiedUri, tableId, modificationTimestamp, eTag);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("deleteAllSyncETags", msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null && db.isOpen() ) {
        db.close();
      }
    }
  }

  @Override
  public void updateManifestSyncETag(String appName, OdkDbHandle dbHandleName, String verifiedUri, String tableId,
      String eTag) throws RemoteException {

    OdkDatabase db = null;

    try {
      SyncETagsUtils seu = new SyncETagsUtils();
      db = DatabaseFactory.get().getDatabase(odkDatabaseService.getApplicationContext(), appName, dbHandleName);
      seu.updateManifestSyncETag(db, verifiedUri, tableId, eTag);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if ( msg == null ) msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(appName).e("deleteAllSyncETags", msg);
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new RemoteException(msg);
    } finally {
      if ( db != null && db.isOpen() ) {
        db.close();
      }
    }
  }

}
