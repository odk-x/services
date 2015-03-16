/*
 * Copyright (C) 2014 University of Washington
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
package org.opendatakit.dbshim.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.opendatakit.common.android.database.DatabaseFactory;
import org.opendatakit.common.android.utilities.ODKCursorUtils;
import org.opendatakit.common.android.utilities.ODKFileUtils;
import org.opendatakit.common.android.utilities.WebLogger;
import org.opendatakit.core.application.Core;
import org.sqlite.database.sqlite.SQLiteDatabase;

import android.annotation.SuppressLint;
import android.app.Service;
import android.content.Intent;
import android.database.Cursor;
import android.os.IBinder;
import android.os.IBinder.DeathRecipient;
import android.os.RemoteException;
import android.util.Log;
import android.util.SparseArray;

public class OdkDbShimService extends Service {

  public static final String LOGTAG = OdkDbShimService.class.getSimpleName();

  /**
   * Open database transactions by appName and database generation.
   */
  private static class DbShimAppContext {
    // for debugging
    @SuppressWarnings("unused")
    final String appName;
    final String generation;
    final SparseArray<SQLiteDatabase> transactions = new SparseArray<SQLiteDatabase>();
    
    DbShimAppContext( String appName, String generation ) {
      this.appName = appName;
      this.generation = generation;
    }
  }

  /**
   * Map of appName to open database transactions
   */
  private Map<String, DbShimAppContext> appContexts = new HashMap<String, DbShimAppContext>();

  /**
   * Type of action to be performed on an appName and open database
   */
  private enum Action {
    INITIALIZE, ROLLBACK, COMMIT, STMT
  };

  /**
   * Action and its parameters
   */
  private class DbAction implements DeathRecipient {
    boolean isActive = true;
    String appName;
    String generation;
    DbShimCallback callback;
    Action theAction;
    int transactionGeneration;
    int actionIdx;
    String sqlStmt;
    String strBinds;
    
    @Override
    public void binderDied() {
      isActive = false;
      OdkDbShimService.this.appNameDied(appName);
    }
  };

  /**
   * Queue of actions to be performed
   */
  private final LinkedBlockingQueue<DbAction> actions = new LinkedBlockingQueue<DbAction>(10);

  /**
   * Work unit that is enqueued on the worker single-threaded executor service.
   * Picks an action off the actions queue and processes it.
   */
  private final Runnable workUnit = new Runnable() {
    @Override
    public void run() {
      try {
        DbAction action;
        action = actions.poll();
        if (action != null) {
          processAction(action);
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  };

  /**
   * The single-threaded executor service that manages the processing of the
   * actions.
   */
  private ExecutorService worker = null;

  /**
   * Report a SQLError outcome related to a transaction
   *
   * @param generation
   * @param transactionGeneration
   * @param actionIdx
   * @param code
   * @param message
   * @throws RemoteException
   */
  private void errorResult(String generation, int transactionGeneration, int code, String message,
      DbShimCallback callback) throws RemoteException {
    Map<String, Object> outcome = new HashMap<String, Object>();
    outcome.put("error", message);
    outcome.put("errorCode", code);
    String quotedResultString = null;
    try {
      String resultString = ODKFileUtils.mapper.writeValueAsString(outcome);
      quotedResultString = ODKFileUtils.mapper.writeValueAsString(resultString);
    } catch (Exception e) {
      quotedResultString = "\'{\"error\":\"Internal Error\",\"errorCode\":\"0\"}\'";
    }
    final String fullCommand = "javascript:window.dbif.dbshimTransactionCallback(\"" + generation
        + "\"," + transactionGeneration + "," + quotedResultString + ");";

    callback.fireCallback(fullCommand);
  }

  /**
   * Report a SQLError outcome related to a sqlStatement
   *
   * @param generation
   * @param transactionGeneration
   * @param actionIdx
   * @param code
   * @param message
   * @throws RemoteException
   */
  private void errorResult(String generation, int transactionGeneration, int actionIdx, int code,
      String message, DbShimCallback callback) throws RemoteException {
    Map<String, Object> outcome = new HashMap<String, Object>();
    outcome.put("error", message);
    outcome.put("errorCode", code);
    String quotedResultString = null;
    try {
      String resultString = ODKFileUtils.mapper.writeValueAsString(outcome);
      quotedResultString = ODKFileUtils.mapper.writeValueAsString(resultString);
    } catch (Exception e) {
      quotedResultString = "\'{\"error\":\"Internal Error\",\"errorCode\":\"0\"}\'";
    }
    final String fullCommand = "javascript:window.dbif.dbshimCallback(\"" + generation + "\","
        + transactionGeneration + "," + actionIdx + "," + quotedResultString + ");";

    callback.fireCallback(fullCommand);
  }

  /**
   * Assert that the database connections for the appName are for the specified
   * generation. If not, flush them and make it so.
   *
   * @param appName
   * @param thisGeneration
   * @param contextName
   * @return
   */
  private DbShimAppContext assertGeneration(String appName, String thisGeneration,
      String contextName) {
    WebLogger log = WebLogger.getLogger(appName);

    DbShimAppContext appContext = appContexts.get(appName);
    if (appContext != null) {
      if (!appContext.generation.equals(thisGeneration)) {
        // copy out the outstanding transactions in the old generation...
        String oldGeneration = appContext.generation;
        ArrayList<SQLiteDatabase> thisTransactions = new ArrayList<SQLiteDatabase>();
        for ( int i = 0 ; i < appContext.transactions.size(); ++i ) {
          thisTransactions.add(appContext.transactions.valueAt(i));
        }
        
        // and replace the old generation with a new object.
        appContext = new DbShimAppContext(appName, thisGeneration);
        appContexts.put(appName, appContext);
        log.i(LOGTAG, contextName + " -- creating DbShimAppContext gen: " + thisGeneration);
        appContext.transactions.clear();

        // then go through the old transactions and end them (rolling them back)
        for (SQLiteDatabase db : thisTransactions) {
          try {
            log.e(LOGTAG, contextName + " -- Wrong Generation! gen: " + thisGeneration
                + " purging transaction!");
            if ( db.inTransaction() ) {
              db.endTransaction();
            }
            db.close();
          } catch (Exception e) {
            log.e(LOGTAG, contextName + " -- exception: " + e.toString());
          }
        }
        
        DatabaseFactory.get().releaseDatabase(getApplicationContext(), appName, oldGeneration);

        log.i(LOGTAG, contextName + " -- updating DbShimAppContext gen: " + thisGeneration
            + " prior gen: " + oldGeneration);
      }
    } else {
      appContext = new DbShimAppContext(appName, thisGeneration);
      appContexts.put(appName, appContext);
      log.i(LOGTAG, contextName + " -- creating DbShimAppContext gen: " + thisGeneration);
    }

    return appContext;
  }

  /**
   * Called to clear any database connections for appName that do not match the
   * generation.
   *
   * @param appName
   * @param generation
   * @param callback
   * @throws RemoteException
   */
  private void initializeDatabaseConnections(String appName, String generation,
      DbShimCallback callback) throws RemoteException {
    WebLogger log = WebLogger.getLogger(appName);
    log.i(LOGTAG, "initializeDatabaseConnections -- gen: " + generation);

    String oldGeneration = "-";
    DbShimAppContext appContext = appContexts.get(appName);
    if (appContext != null) {
      oldGeneration = appContext.generation;
      if (oldGeneration.equals(generation)) {
        oldGeneration = "-";
      }
    }

    appContext = assertGeneration(appName, generation, "initializeDatabaseConnections");

    String fullCommand = "javascript:window.dbif.dbshimCleanupCallback(\"" + oldGeneration + "\");";
    callback.fireCallback(fullCommand);
  }

  /**
   * Rolls back the indicated transaction.
   *
   * @param appName
   * @param thisGeneration
   * @param thisTransactionGeneration
   * @param callback
   * @throws RemoteException
   */
  private void runRollback(String appName, String thisGeneration, int thisTransactionGeneration,
      DbShimCallback callback) throws RemoteException {
    WebLogger log = WebLogger.getLogger(appName);

    DbShimAppContext appContext = assertGeneration(appName, thisGeneration, "runRollback");

    SQLiteDatabase db = appContext.transactions.get(thisTransactionGeneration);
    if (db != null) {
      log.i(LOGTAG, "rollback gen: " + thisGeneration + " transaction: "
          + thisTransactionGeneration);
      try {
        db.endTransaction();
        db.close();
        appContext.transactions.remove(thisTransactionGeneration);
      } catch (Exception e) {
        log.e(LOGTAG, "rollback gen: " + thisGeneration + " transaction: "
            + thisTransactionGeneration + " - exception: " + e.toString());
        try {
          appContext.transactions.remove(thisTransactionGeneration);
          if ( db.isOpen() ) {
            if ( db.inTransaction() ) {
              db.endTransaction();
            }
            db.close();
          }
        } catch ( Exception ex ) {
          // ignore
        }
        errorResult(thisGeneration, thisTransactionGeneration, 0,
            "rollback - exception: " + e.toString(), callback);
        return;
      }
    } else {
      log.w(LOGTAG, "rollback -- Transaction Not Found! gen: " + thisGeneration + " transaction: "
          + thisTransactionGeneration);
    }

    String fullCommand = "javascript:window.dbif.dbshimTransactionCallback(\"" + thisGeneration
        + "\"," + thisTransactionGeneration + ", '{}');";

    callback.fireCallback(fullCommand);
  }

  /**
   * Commits the indicated transaction.
   *
   * @param appName
   * @param thisGeneration
   * @param thisTransactionGeneration
   * @param callback
   * @throws RemoteException
   */
  private void runCommit(String appName, String thisGeneration, int thisTransactionGeneration,
      DbShimCallback callback) throws RemoteException {
    WebLogger log = WebLogger.getLogger(appName);

    DbShimAppContext appContext = assertGeneration(appName, thisGeneration, "runCommit");

    SQLiteDatabase db = appContext.transactions.get(thisTransactionGeneration);
    if (db != null) {
      log.i(LOGTAG, "commit gen: " + thisGeneration + " transaction: " + thisTransactionGeneration);
      try {
        db.setTransactionSuccessful();
        db.endTransaction();
        db.close();
        appContext.transactions.remove(thisTransactionGeneration);
      } catch (Exception e) {
        log.e(LOGTAG, "commit gen: " + thisGeneration + " transaction: "
            + thisTransactionGeneration + " - exception: " + e.toString());
        try {
          appContext.transactions.remove(thisTransactionGeneration);
          if ( db.isOpen() ) {
            if ( db.inTransaction() ) {
              db.endTransaction();
            }
            db.close();
          }
        } catch ( Exception ex ) {
          // ignore
        }
        errorResult(thisGeneration, thisTransactionGeneration, 0,
            "commit - exception: " + e.toString(), callback);
        return;
      }
    } else {
      log.w(LOGTAG, "commit -- Transaction Not Found! gen: " + thisGeneration + " transaction: "
          + thisTransactionGeneration);
    }

    String fullCommand = "javascript:window.dbif.dbshimTransactionCallback(\"" + thisGeneration
        + "\"," + thisTransactionGeneration + ", '{}');";

    callback.fireCallback(fullCommand);
  }

  /**
   * Execute an arbitrary SQL statement.
   *
   * This either calls back with a SQLError (via errorResult(...)) or calls back
   * with a SQLResultSet.
   *
   * NOTE: the SQLResultSet does not properly record insertId or rowsAffected.
   * 
   * @throws RemoteException
   */
  @SuppressLint("NewApi")
  private void runStmt(String appName, String thisGeneration, int thisTransactionGeneration,
      int thisActionIdx, String sqlStmt, String strBinds, DbShimCallback callback)
      throws RemoteException {
    WebLogger log = WebLogger.getLogger(appName);

    sqlStmt = sqlStmt.trim();
    // doesn't matter...
    String sqlVerb = sqlStmt.substring(0, sqlStmt.indexOf(' ')).toUpperCase(Locale.US);

    DbShimAppContext appContext = assertGeneration(appName, thisGeneration, "runStmt");

    log.i(LOGTAG, "executeSqlStmt -- gen: " + thisGeneration + " transaction: "
        + thisTransactionGeneration + " action: " + thisActionIdx + " sqlVerb: " + sqlVerb);

    String[] bindArray = null;
    try {
      if (strBinds != null) {
        ArrayList<Object> binds = new ArrayList<Object>();
        binds = ODKFileUtils.mapper.readValue(strBinds, binds.getClass());
        bindArray = new String[binds.size()];
        // convert the bindings to string values for SQLiteDatabase interface
        for (int i = 0; i < binds.size(); ++i) {
          Object o = binds.get(i);
          if (o == null) {
            bindArray[i] = null;
          } else {
            bindArray[i] = o.toString();
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      log.e(LOGTAG, "executeSqlStmt - exception parsing binds: " + e.toString());
      errorResult(thisGeneration, thisTransactionGeneration, thisActionIdx, 0,
          "exception parsing binds!", callback);
      return;
    }

    SQLiteDatabase db = appContext.transactions.get(thisTransactionGeneration);
    if (thisActionIdx == 0) {
      if (db != null) {
        log.e(LOGTAG,
            "executeSqlStmt - unexpectedly matching transactionGen with actionIdx of zero!");
        try {
          appContext.transactions.remove(thisTransactionGeneration);
          if ( db.isOpen() ) {
            if ( db.inTransaction() ) {
              db.endTransaction();
            }
            db.close();
          }
        } catch ( Exception ex ) {
          // ignore
        }
        errorResult(thisGeneration, thisTransactionGeneration, thisActionIdx, 0,
            "unexpectedly matching transactionGen with actionIdx of zero!", callback);
        return;
      }

      db = DatabaseFactory.get().getDatabase(getApplicationContext(), appName, thisGeneration);
      appContext.transactions.put(thisTransactionGeneration, db);
      if ( !db.inTransaction() ) {
        db.beginTransactionNonExclusive();
      }
    } else {
      if (db == null) {
        log.e(LOGTAG,
            "executeSqlStmt - could not find matching transactionGen with non-zero actionIdx!");
        errorResult(thisGeneration, thisTransactionGeneration, thisActionIdx, 0,
            "could not find matching transactionGen with non-zero actionIdx!", callback);
        return;
      }
    }

    if (sqlVerb.equals("SELECT")) {
      try {
        Cursor c = db.rawQuery(sqlStmt, bindArray);
        Map<String, Object> resultSet = new HashMap<String, Object>();
        ArrayList<Map<String, Object>> rowSet = new ArrayList<Map<String, Object>>();
        resultSet.put("rowsAffected", 0);
        resultSet.put("rows", rowSet);

        while (c.moveToNext()) {
          Map<String, Object> row = new HashMap<String, Object>();
          int nCols = c.getColumnCount();
          for (int i = 0; i < nCols; ++i) {
            String name = c.getColumnName(i);

            Object v = ODKCursorUtils.getIndexAsType(c,
                ODKCursorUtils.getIndexDataType(c, i), i);
            row.put(name, v);
          }
          rowSet.add(row);
        }

        String resultString = ODKFileUtils.mapper.writeValueAsString(resultSet);
        String quotedResultString = ODKFileUtils.mapper.writeValueAsString(resultString);
        StringBuilder b = new StringBuilder();
        b.append("javascript:window.dbif.dbshimCallback(\"").append(thisGeneration).append("\",")
            .append(thisTransactionGeneration).append(",").append(thisActionIdx).append(",")
            .append(quotedResultString).append(");");
        String fullCommand = b.toString();
        log.i(LOGTAG, "executeSqlStmt return sqlVerb: " + sqlVerb);
        //db.yieldIfContendedSafely();
        callback.fireCallback(fullCommand);
        return;
      } catch (Exception e) {
        log.e(LOGTAG, "executeSqlStmt - exception: " + e.toString());
        //db.yieldIfContendedSafely();
        errorResult(thisGeneration, thisTransactionGeneration, thisActionIdx, 0,
            "exception: " + e.toString(), callback);
        return;
      }
    } else {
      try {
        db.execSQL(sqlStmt, bindArray);
        Map<String, Object> resultSet = new HashMap<String, Object>();
        String resultString = ODKFileUtils.mapper.writeValueAsString(resultSet);
        String quotedResultString = ODKFileUtils.mapper.writeValueAsString(resultString);
        StringBuilder b = new StringBuilder();
        b.append("javascript:window.dbif.dbshimCallback(\"").append(thisGeneration).append("\",")
            .append(thisTransactionGeneration).append(",").append(thisActionIdx).append(",")
            .append(quotedResultString).append(");");
        String fullCommand = b.toString();
        log.i(LOGTAG, "executeSqlStmt return sqlVerb: " + sqlVerb);
        //db.yieldIfContendedSafely();
        callback.fireCallback(fullCommand);
      } catch (Exception e) {
        log.e(LOGTAG, "executeSqlStmt - exception: " + e.toString());
        //db.yieldIfContendedSafely();
        errorResult(thisGeneration, thisTransactionGeneration, thisActionIdx, 0,
            "exception: " + e.toString(), callback);
        return;
      }
    }
  }

  private synchronized void processAction(DbAction actionDefn) throws RemoteException {
    if ( !actionDefn.isActive ) {
      return;
    }
    switch (actionDefn.theAction) {
    case INITIALIZE:
      initializeDatabaseConnections(actionDefn.appName, actionDefn.generation, actionDefn.callback);
      break;
    case ROLLBACK:
      runRollback(actionDefn.appName, actionDefn.generation, actionDefn.transactionGeneration,
          actionDefn.callback);
      break;
    case COMMIT:
      runCommit(actionDefn.appName, actionDefn.generation, actionDefn.transactionGeneration,
          actionDefn.callback);
      break;
    case STMT:
      runStmt(actionDefn.appName, actionDefn.generation, actionDefn.transactionGeneration,
          actionDefn.actionIdx, actionDefn.sqlStmt, actionDefn.strBinds, actionDefn.callback);
      break;
    }
  }

  private OdkDbShimServiceInterface servInterface;

  @Override
  public void onCreate() {
    super.onCreate();
    servInterface = new OdkDbShimServiceInterface(this);

    // start a new executor...
    worker = Executors.newSingleThreadExecutor();
  }

  @Override
  public IBinder onBind(Intent intent) {
    Log.i(LOGTAG, "onBind -- returning interface.");
    if (Core.getInstance().shouldWaitForDebugger()) {
      android.os.Debug.waitForDebugger();
    }
    return servInterface;
  }

  public synchronized void appNameDied(String appName) {
    DbShimAppContext appContext = appContexts.get(appName);
    appContexts.remove(appName);
    if (appContext != null) {
      // copy out the outstanding transactions in the old generation...
      String oldGeneration = appContext.generation;
      ArrayList<SQLiteDatabase> thisTransactions = new ArrayList<SQLiteDatabase>();
      for ( int i = 0 ; i < appContext.transactions.size(); ++i ) {
        thisTransactions.add(appContext.transactions.valueAt(i));
      }
      
      // then go through the old transactions and end them (rolling them back)
      for (SQLiteDatabase db : thisTransactions) {
        try {
          WebLogger.getLogger(appName).e(LOGTAG, "appNameDied -- purging transaction!");
          if ( db.inTransaction() ) {
            db.endTransaction();
          }
          db.close();
        } catch (Exception e) {
          WebLogger.getLogger(appName).e(LOGTAG, "appNameDied -- exception: " + e.toString());
        }
      }
      DatabaseFactory.get().releaseDatabase(getApplicationContext(), appContext.appName, appContext.generation);
    }
  }
  
  @Override
  public synchronized void onDestroy() {
    Log.w(LOGTAG, "onDestroy -- shutting down worker (zero interfaces)!");
    // clear out the work items
    actions.clear();
    // drain the active work queue
    worker.shutdown();
    try {
      worker.awaitTermination(2000L, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    worker = null;

    // and release any transactions we are holding...
    String contextName = "onDestroy";
    ArrayList<DbShimAppContext> tmpContexts = new ArrayList<DbShimAppContext>(appContexts.values());
    appContexts.clear();
    for (DbShimAppContext appContext : tmpContexts ) {
      
      // copy out the outstanding transactions...
      ArrayList<SQLiteDatabase> thisTransactions = new ArrayList<SQLiteDatabase>();
      for ( int i = 0 ; i < appContext.transactions.size(); ++i ) {
        thisTransactions.add(appContext.transactions.valueAt(i));
      }
      appContext.transactions.clear();
  
      // then go through the old transactions and end them (rolling them back)
      for (SQLiteDatabase db : thisTransactions) {
        try {
          WebLogger.getLogger(appContext.appName)
          .e(LOGTAG, contextName + " -- purging transaction!");
          if ( db.inTransaction() ) {
            db.endTransaction();
          }
          db.close();
        } catch (Exception e) {
          WebLogger.getLogger(appContext.appName)
          .e(LOGTAG, contextName + " -- exception: " + e.toString());
        }
      }
      DatabaseFactory.get().releaseDatabase(getApplicationContext(), appContext.appName, appContext.generation);
    }
    Log.i(LOGTAG, "onDestroy - done");
    super.onDestroy();
  }

  public synchronized void queueAction(DbAction action) throws RemoteException {
    try {
      // throws exception if queue length would be exceeded
      actions.add(action);
      worker.execute(workUnit);
    } catch ( Exception e ) {
      WebLogger.getLogger(action.appName).e(LOGTAG, "queueAction Failed: " + e.toString());
      WebLogger.getLogger(action.appName).printStackTrace(e);
      throw new RemoteException();
    }
  }
  
  public void queueInitializeDatabaseConnections(String appName, String generation,
      DbShimCallback callback) throws RemoteException {
    DbAction action = new DbAction();
    action.appName = appName;
    action.generation = generation;
    action.callback = callback;
    action.theAction = Action.INITIALIZE;
    // throws exception if queue length would be exceeded
    queueAction(action);
  }

  public void queueRunCommit(String appName, String generation, int transactionGeneration,
      DbShimCallback callback) throws RemoteException {
    DbAction action = new DbAction();
    action.appName = appName;
    action.generation = generation;
    action.transactionGeneration = transactionGeneration;
    action.callback = callback;
    action.theAction = Action.COMMIT;
    // throws exception if queue length would be exceeded
    queueAction(action);
  }

  public void queueRunRollback(String appName, String generation, int transactionGeneration,
      DbShimCallback callback) throws RemoteException {
    DbAction action = new DbAction();
    action.appName = appName;
    action.generation = generation;
    action.transactionGeneration = transactionGeneration;
    action.callback = callback;
    action.theAction = Action.ROLLBACK;
    // throws exception if queue length would be exceeded
    queueAction(action);
  }

  public void queueRunStmt(String appName, String generation, int transactionGeneration,
      int actionIdx, String sqlStmt, String strBinds, DbShimCallback callback) throws RemoteException {
    DbAction action = new DbAction();
    action.appName = appName;
    action.generation = generation;
    action.transactionGeneration = transactionGeneration;
    action.actionIdx = actionIdx;
    action.sqlStmt = sqlStmt;
    action.strBinds = strBinds;
    action.callback = callback;
    action.theAction = Action.STMT;
    // throws exception if queue length would be exceeded
    queueAction(action);
  }

}
