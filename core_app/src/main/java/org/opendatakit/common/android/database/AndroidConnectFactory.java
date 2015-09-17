/*
 * Copyright (C) 2014 University of Washington
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

package org.opendatakit.common.android.database;

import android.content.Context;
import android.util.Log;

import org.opendatakit.common.android.utilities.ODKDataUtils;
import org.opendatakit.common.android.utilities.ODKDatabaseImplUtils;
import org.opendatakit.common.android.utilities.ODKFileUtils;
import org.opendatakit.common.android.utilities.StaticStateManipulator;
import org.opendatakit.common.android.utilities.StaticStateManipulator.IStaticFieldManipulator;
import org.opendatakit.common.android.utilities.WebLogger;
import org.opendatakit.database.service.OdkDbHandle;
import org.sqlite.database.sqlite.SQLiteException;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;

public class AndroidConnectFactory extends OdkConnectionFactorySingleton{

  private static OdkConnectionFactorySingleton connectionFactory = new AndroidConnectFactory();

  private static final String GROUP_TYPE_DIVIDER = "--";
  public static final String INTERNAL_TYPE_SUFFIX = "-internal";

  /** the version that the application expects */
  private final int mNewVersion = 1;

  private boolean mIsInitializing = false;
  
  static {

    // loads our custom libsqliteX.so
    System.loadLibrary("sqliteX");

    OdkConnectionFactorySingleton.set(new AndroidConnectFactory());

    // register a state-reset manipulator for 'connectionFactory' field.
    StaticStateManipulator.get().register(50, new IStaticFieldManipulator() {

      @Override
      public void reset() {
        connectionFactory = new AndroidConnectFactory();
      }

    });
  }

  public OdkConnectionFactorySingleton get() {
    return connectionFactory;
  }

  public static void configure() {
    // just to get the static initialization block (above) to run
  }

  /**
   * For mocking -- supply a mocked object.
   * 
   * @param factory
   */
  public static void set(AndroidConnectFactory factory) {
    connectionFactory = factory;
  }

  protected AndroidConnectFactory() {
  }

  // map of appName -TO-
  // map of dbHandleTag -TO- dabase handle
  //
  // The dbHandleTag enables different sections of code to isolate themselves
  // from
  // the transactions that are in-progress in other sections of code.
  private final Map<String, Map<String, OdkConnectionInterface>> dbHelpers = new HashMap<String, Map<String, OdkConnectionInterface>>();

  /**
   * This handle is suitable for non-service uses.
   * 
   * @return
   */
  public OdkDbHandle generateInternalUseDbHandle() {
    return new OdkDbHandle(ODKDataUtils.genUUID() + AndroidConnectFactory.INTERNAL_TYPE_SUFFIX);
  }

  /**
   * This handle is suitable for database service use.
   * 
   * @return
   */
  public OdkDbHandle generateDatabaseServiceDbHandle() {
    return new OdkDbHandle(ODKDataUtils.genUUID());
  }
  
  /**
   * Shared accessor to get a database handle.
   *
   * @param appName
   * @param sessionQualifier
   * @return an entry in dbHelpers
   */
  private synchronized OdkConnectionInterface getDbHelper(Context context, String appName,
      String sessionQualifier) {
    WebLogger log = null;

    try {
      ODKFileUtils.verifyExternalStorageAvailability();
      ODKFileUtils.assertDirectoryStructure(appName);
      log = WebLogger.getLogger(appName);
    } catch (Exception e) {
      if (log != null) {
        log.e("AndroidConnectFactory",
            "External storage not available -- purging dbHelpers");
      }
      releaseAllDatabases(context);
      return null;
    }

    if (sessionQualifier == null) {
      sessionQualifier = appName;
    }

    OdkConnectionInterface dbConnection = null;
    Map<String, OdkConnectionInterface> dbHelperSet = dbHelpers.get(appName);
    if (dbHelperSet == null) {
      dbHelperSet = new HashMap<String, OdkConnectionInterface>();
      dbHelpers.put(appName, dbHelperSet);

      // If initialization takes a while, we may have a problem ...
      initializeDatabase(sessionQualifier, appName);
    }

    // verify that the base database handle is not initializing
    dbConnection = dbHelperSet.get(sessionQualifier);
    if (dbConnection != null && dbConnection.isOpen()) {
      dbConnection.acquireReference();
      WebLogger.getLogger(appName).i("AndroidContentFactory",
              "getDbHelper -- obtaining reference to already-open database");
      return dbConnection; // The database is already open for business
    }

    if (mIsInitializing) {
      throw new IllegalStateException("getDbHelper called recursively");
    }

    WebLogger.getLogger(appName).i("AndroidContentFactory", "getDbHelper -- opening database");
    boolean success = false;
    AndroidOdkConnection db = null;
    try {
      db = AndroidOdkConnection.openDatabase(appName, getDbFilePath(appName), sessionQualifier);
      success = true;
    } finally {
      if (success) {
        if (dbConnection != null) {
          try {
            dbConnection.close();
          } catch (Exception e) {
          }
        }
        dbConnection = db;
        dbConnection = dbHelperSet.put(sessionQualifier, dbConnection);
      } else {
        if (db != null)
          db.close();
      }
    }

    return dbConnection;
  }

  protected synchronized void initializeDatabase(String sessionQualifier, String appName) {
    // Unnecessary until proven otherwise
    //if (mDatabase != null) {
    //  throw new IllegalStateException("initializeDatabase called multiple times!");
    //}

    mIsInitializing = true;
    WebLogger.getLogger(appName).i("AndroidContentFactory", "initializeDatabase -- initializing database");


    AndroidOdkConnection db = null;
    try {
      db = AndroidOdkConnection.openDatabase(appName, getDbFilePath(appName), sessionQualifier);

      int version = db.getVersion();
      if (version != mNewVersion) {
        ODKDatabaseImplUtils.get().beginTransactionNonExclusive(db);
        try {
          if (version == 0) {
            onCreate(db);
          } else {
            onUpgrade(db, version, mNewVersion);
          }
          db.setVersion(mNewVersion);
          db.setTransactionSuccessful();
        } finally {
          db.endTransaction();
        }
      }

    } finally {
      if (db != null) {
        db.close();
      }
    }

    mIsInitializing = false;
  }

  public OdkConnectionInterface getConnection(Context context, String appName, OdkDbHandle dbHandleName) {
    if (dbHandleName != null) {
      return getConnectionImpl(context, appName, dbHandleName.getDatabaseHandle());
    } else {
      return getConnectionImpl(context, appName, null);
    }

  }

  public void releaseDatabase(Context context, String appName, OdkDbHandle dbHandleName) {
    releaseDatabase(context, appName, dbHandleName.getDatabaseHandle());
  }

  public synchronized void releaseAllDatabases(Context context) {
    for (Entry<String, Map<String, OdkConnectionInterface>> e : dbHelpers.entrySet()) {
      String appName = e.getKey();
      Map<String, OdkConnectionInterface> dbHelperSet = e.getValue();
      HashSet<String> sessionQualifiers = new HashSet<String>();
      sessionQualifiers.addAll(dbHelperSet.keySet());
      for (String sessionQualifier : sessionQualifiers) {
        WebLogger.getLogger(appName).i("AndroidConnectFactory",
            "releaseAllDatabases: releasing " + sessionQualifier);
        OdkConnectionInterface dbConnection = dbHelperSet.get(sessionQualifier);
        dbHelperSet.remove(dbConnection);
        try {
          dbConnection.releaseConnection();
        } catch (Exception ex) {
          // ignore
        }
      }
    }
  }

  private void releaseDatabase(Context context, String appName, String sessionQualifier) {
    Map<String, OdkConnectionInterface> dbHelperSet = dbHelpers.get(appName);
    if ( dbHelperSet == null ) {
      // no record of the appName
      return;
    }

    if (sessionQualifier == null) {
      sessionQualifier = appName;
    }
    OdkConnectionInterface dbHelper = dbHelperSet.get(sessionQualifier);
    if (dbHelper != null) {
      try {
        // ensure that the database is released
        dbHelper.releaseConnection();
      } finally {
        dbHelperSet.remove(sessionQualifier);
      }
    }
  }

  public OdkConnectionInterface getDatabaseGroupInstance(Context context, String appName,
      String sessionGroupQualifier, int instanceQualifier) {
    String sessionQualifier = sessionGroupQualifier + GROUP_TYPE_DIVIDER + Integer.toString(instanceQualifier);
    return getConnectionImpl(context, appName, sessionQualifier);
  }

  public void releaseDatabaseGroupInstance(Context context, String appName,
      String sessionGroupQualifier, int instanceQualifier) {
    String sessionQualifier = sessionGroupQualifier + GROUP_TYPE_DIVIDER + Integer.toString(instanceQualifier);
    releaseDatabase(context, appName, sessionQualifier);
  }

  /**
   * Release any database handles for groups that don't match or match the indicated
   * session group qualifier (depending upon the value of releaseNonMatchingGroupsOnly).
   * 
   * Database sessions are group sessions if they contain '--'. Everything up
   * to the last occurrence of '--' in the sessionQualifier is considered the session
   * group qualifier.
   * 
   * @param context
   * @param appName
   * @param sessionGroupQualifier
   * @param releaseNonMatchingGroupsOnly
   * @return true if anything was released.
   */
  public boolean releaseDatabaseGroupInstances(Context context, String appName,
      String sessionGroupQualifier, boolean releaseNonMatchingGroupsOnly) {
    Map<String, OdkConnectionInterface> dbHelperSet = dbHelpers.get(appName);
    if ( dbHelperSet == null ) {
      // no record of the appName
      return false;
    }
    HashSet<String> sessionQualifiers = new HashSet<String>();
    for (String sessionQualifier : dbHelperSet.keySet()) {
      int idx = sessionQualifier.lastIndexOf(GROUP_TYPE_DIVIDER);
      if ( idx != -1 ) {
        String sessionGroup = sessionQualifier.substring(0,idx);
        if ( releaseNonMatchingGroupsOnly ) {
          if ( !sessionGroup.equals(sessionGroupQualifier) ) {
            sessionQualifiers.add(sessionQualifier);
          }
        } else {
          if ( sessionGroup.equals(sessionGroupQualifier) ) {
            sessionQualifiers.add(sessionQualifier);
          }
        }
      }
    }

    File appLogger = new File(ODKFileUtils.getLoggingFolder(appName));
    for (String sessionQualifier : sessionQualifiers) {
      if ( appLogger.exists() && appLogger.isDirectory() ) {
        WebLogger.getLogger(appName).i("AndroidConnectFactory",
            "releaseDatabaseGroupInstances: releasing " + appName + 
            " session " + sessionQualifier);
      } else {
        Log.i("AndroidConnectFactory",
            "releaseDatabaseGroupInstances: releasing " + 
            appName + " session " + sessionQualifier);
      }
      OdkConnectionInterface dbHelper = dbHelperSet.get(sessionQualifier);
      dbHelperSet.remove(dbHelper);
      try {
        dbHelper.releaseConnection();
      } catch (Exception ex) {
        // ignore
        if ( appLogger.exists() && appLogger.isDirectory() ) {
          WebLogger.getLogger(appName).printStackTrace(ex);
        } else {
          ex.printStackTrace();
        }
      }
    }
    return !sessionQualifiers.isEmpty();
  }

  /**
   * Releases all database handles that are not created by the
   * internal content providers or by the dbshim service.
   * 
   * @param context
   * @param appName
   * @return
   */
  private boolean releaseDatabaseNonGroupNonInternalInstances(Context context, String appName) {
    Map<String, OdkConnectionInterface> dbHelperSet = dbHelpers.get(appName);
    if ( dbHelperSet == null ) {
      // no record of the appName
      return false;
    }
    HashSet<String> sessionQualifiers = new HashSet<String>();
    for (String sessionQualifier : dbHelperSet.keySet()) {
      int idx = sessionQualifier.lastIndexOf(GROUP_TYPE_DIVIDER);
      if ( idx == -1 && !sessionQualifier.endsWith(INTERNAL_TYPE_SUFFIX)) {
        sessionQualifiers.add(sessionQualifier);
      }
    }

    File appLogger = new File(ODKFileUtils.getLoggingFolder(appName));
    for (String sessionQualifier : sessionQualifiers) {
      if ( appLogger.exists() && appLogger.isDirectory() ) {
        WebLogger.getLogger(appName).i("AndroidConnectFactory",
            "releaseDatabaseNonGroupNonInternalInstances: releasing " + appName + 
            " session " + sessionQualifier);
      } else {
        Log.i("AndroidConnectFactory",
            "releaseDatabaseNonGroupNonInternalInstances: releasing " + 
            appName + " session " + sessionQualifier);
      }
      OdkConnectionInterface dbHelper = dbHelperSet.get(sessionQualifier);
      dbHelperSet.remove(dbHelper);
      try {
        dbHelper.releaseConnection();
      } catch (Exception ex) {
        // ignore
      }
    }
    return !sessionQualifiers.isEmpty();
  }

  public boolean releaseAllDatabaseGroupInstances(Context context) {
    HashSet<String> appNames = new HashSet<String>();
    for ( Entry<String, Map<String, OdkConnectionInterface>> dbHelperSetEntry : dbHelpers.entrySet() ) {
      String appName = dbHelperSetEntry.getKey();
      Map<String, OdkConnectionInterface> dbHelperSet = dbHelperSetEntry.getValue();
      for (String sessionQualifier : dbHelperSet.keySet()) {
        int idx = sessionQualifier.lastIndexOf(GROUP_TYPE_DIVIDER);
        if ( idx != -1 ) {
            appNames.add(appName);
            break;
        }
      }
    }
    for ( String appName : appNames ) {
      releaseDatabaseGroupInstances(context, appName, null, true);
    }
    return !appNames.isEmpty();
  }

  public boolean releaseAllDatabaseNonGroupNonInternalInstances(Context context) {
    HashSet<String> appNames = new HashSet<String>();
    for ( Entry<String, Map<String, OdkConnectionInterface>> dbHelperSetEntry : dbHelpers.entrySet() ) {
      String appName = dbHelperSetEntry.getKey();
      Map<String, OdkConnectionInterface> dbHelperSet = dbHelperSetEntry.getValue();
      for (String sessionQualifier : dbHelperSet.keySet()) {
        int idx = sessionQualifier.lastIndexOf(GROUP_TYPE_DIVIDER);
        if ( idx == -1 && !sessionQualifier.endsWith(INTERNAL_TYPE_SUFFIX)) {
            appNames.add(appName);
            break;
        }
      }
    }
    for ( String appName : appNames ) {
      releaseDatabaseNonGroupNonInternalInstances(context, appName);
    }
    return !appNames.isEmpty();
  }

  public void dumpInfo() {
    for ( Entry<String, Map<String, OdkConnectionInterface>> dbHelperSetEntry : dbHelpers.entrySet() ) {
      String appName = dbHelperSetEntry.getKey();
      Map<String, OdkConnectionInterface> dbHelperSet = dbHelperSetEntry.getValue();
      for (String sessionQualifier : dbHelperSet.keySet()) {
        OdkConnectionInterface dbHelper = dbHelperSet.get(sessionQualifier);
        WebLogger.getLogger(appName).i("AndroidConnectFactory:dumpInfo", "appName " + appName +
            " sessionQualifier " + sessionQualifier + 
            " lastAction: " + dbHelper.getLastAction() );
        
      }
    }
  }

  private OdkConnectionInterface getConnectionImpl(Context context, String appName, String sessionQualifier) {
    OdkConnectionInterface db = null;
    int count = 1;
    for (; db == null; ++count) {
      try {
        db = getDbHelper(context, appName, sessionQualifier);
      } catch (SQLiteException e) {
        if (count == 201) {
          // give up after 200 * 20 + 180*50 = 13000 milliseconds...
          WebLogger.getLogger(appName).e("AndroidConnectFactory",
              "database exception " + e.toString() + " -- abandoning attempt!");
          throw e;
        } else if (count % 10 != 0) {
          try {
            WebLogger.getLogger(appName).i("AndroidConnectFactory",
                "database exception " + e.toString() + " -- sleeping 50ms");
            Thread.sleep(50L);
          } catch (InterruptedException ex) {
            // ignore
          }
        } else {
          try {
            WebLogger.getLogger(appName).w("AndroidConnectFactory",
                "database exception " + e.toString() + " -- sleeping 200ms");
            Thread.sleep(200L);
          } catch (InterruptedException ex) {
            // ignore
          }
        }
      }
    }
    return db;
  }

  private String getDbFilePath(String appName) {
    File dbFile = new File(ODKFileUtils.getWebDbFolder(appName),
            ODKFileUtils.getNameOfSQLiteDatabase());
    String dbFilePath = dbFile.getAbsolutePath();
    return dbFilePath;
  }

  /**
   * Called when the database is created for the first time. This is where the
   * creation of tables and the initial population of the tables should happen.
   *
   * @param db
   *          The database.
   */
  protected void onCreate(AndroidOdkConnection db) {
    ODKDatabaseImplUtils.initializeDatabase(db);
  }

  /**
   * Called when the database needs to be upgraded. The implementation should
   * use this method to drop tables, add tables, or do anything else it needs to
   * upgrade to the new schema version.
   * <p>
   * The SQLite ALTER TABLE documentation can be found <a
   * href="http://sqlite.org/lang_altertable.html">here</a>. If you add new
   * columns you can use ALTER TABLE to insert them into a live table. If you
   * rename or remove columns you can use ALTER TABLE to rename the old table,
   * then create the new table and then populate the new table with the contents
   * of the old table.
   *
   * @param db
   *          The database.
   * @param oldVersion
   *          The old database version.
   * @param newVersion
   *          The new database version.
   */
  protected void onUpgrade(AndroidOdkConnection db, int oldVersion, int newVersion) {
    // for now, upgrade and creation use the same codepath...
    ODKDatabaseImplUtils.initializeDatabase(db);
  }
}
