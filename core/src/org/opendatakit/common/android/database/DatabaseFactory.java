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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;

import org.opendatakit.common.android.utilities.ODKFileUtils;
import org.opendatakit.common.android.utilities.StaticStateManipulator;
import org.opendatakit.common.android.utilities.StaticStateManipulator.IStaticFieldManipulator;
import org.opendatakit.common.android.utilities.WebLogger;
import org.opendatakit.database.service.OdkDbHandle;
import org.sqlite.database.sqlite.SQLiteDatabase;
import org.sqlite.database.sqlite.SQLiteException;

import android.content.Context;

public class DatabaseFactory {
  
  private static DatabaseFactory databaseFactory = new DatabaseFactory();

  static {

    // loads our custom libsqliteX.so 
    System.loadLibrary("sqliteX");

    // register a state-reset manipulator for 'databaseFactory' field.
    StaticStateManipulator.get().register(50, new IStaticFieldManipulator() {

      @Override
      public void reset() {
        databaseFactory = new DatabaseFactory();
      }
      
    });
  }

  public static DatabaseFactory get() {
    return databaseFactory;
  }
 
  /**
   * For mocking -- supply a mocked object.
   * 
   * @param databaseFactory
   */
  public static void set(DatabaseFactory factory) {
    databaseFactory = factory;
  }

  protected DatabaseFactory() {}

  // map of appName -TO- 
  //     map of dbHandleTag -TO- dabase handle
  // 
  // The dbHandleTag enables different sections of code to isolate themselves from 
  // the transactions that are in-progress in other sections of code.
  private final Map<String, Map<String, DataModelDatabaseHelper>> dbHelpers 
    = new HashMap<String, Map<String, DataModelDatabaseHelper>>();

  /**
   * Shared accessor to get a database handle.
   *
   * @param appName
   * @param sessionQualifier
   * @return an entry in dbHelpers
   */
  private synchronized DataModelDatabaseHelper getDbHelper(Context context, String appName, String sessionQualifier) {
    WebLogger log = null;

    try {
      ODKFileUtils.verifyExternalStorageAvailability();
      ODKFileUtils.assertDirectoryStructure(appName);
      log = WebLogger.getLogger(appName);
    } catch ( Exception e ) {
      if ( log != null ) {
        log.e("DataModelDatabaseHelperFactory", "External storage not available -- purging dbHelpers");
      }
      dbHelpers.clear();
      return null;
    }

    DataModelDatabaseHelper dbHelper = null;
    Map<String, DataModelDatabaseHelper> dbHelperSet = dbHelpers.get(appName);
    if ( dbHelperSet == null ) {
      dbHelperSet = new HashMap<String, DataModelDatabaseHelper>();
      dbHelpers.put(appName, dbHelperSet);

      dbHelper = new DataModelDatabaseHelper(appName);
      dbHelperSet.put(appName, dbHelper);
      
      dbHelper.initializeDatabase();
    }
    
    // verify that the base database handle is not initializing
    dbHelper = dbHelperSet.get(appName);
    if ( dbHelper.isInitializing() ) {
      throw new IllegalStateException("getDatabase called recursively during database initialization / upgrade");
    }
    
    if ( sessionQualifier == null ) {
      sessionQualifier = appName;
    }
    dbHelper = dbHelperSet.get(sessionQualifier);
    if (dbHelper == null) {
      dbHelper = new DataModelDatabaseHelper(appName);
      dbHelperSet.put(sessionQualifier, dbHelper);
    }
    return dbHelper;
  }

  public SQLiteDatabase getDatabase(Context context, String appName, OdkDbHandle dbHandleName) {
    return getDatabase(context, appName, dbHandleName.getDatabaseHandle());
  }
  
  public void releaseDatabase(Context context, String appName, OdkDbHandle dbHandleName) {
    releaseDatabase(context, appName, dbHandleName.getDatabaseHandle());
  }

  public synchronized void releaseAllDatabases(Context context) {
    for ( Entry<String, Map<String, DataModelDatabaseHelper>> e : dbHelpers.entrySet()) {
      String appName = e.getKey();
      Map<String, DataModelDatabaseHelper> dbHelperSet = e.getValue();
      HashSet<String> sessionQualifiers = new HashSet<String>();
      sessionQualifiers.addAll(dbHelperSet.keySet());
      for ( String sessionQualifier : sessionQualifiers ) {
        WebLogger.getLogger(appName).i("DatabaseFactory",
            "releaseAllDatabases: releasing " + sessionQualifier);
        DataModelDatabaseHelper dbHelper = dbHelperSet.get(sessionQualifier);
        dbHelperSet.remove(dbHelper);
        try {
          dbHelper.releaseDatabase();
        } catch ( Exception ex ) {
          // ignore
        }
      }
    }
  }
  
  public void releaseDatabase(Context context, String appName, String sessionQualifier) {
    Map<String, DataModelDatabaseHelper> dbHelperSet = dbHelpers.get(appName);
    if ( dbHelperSet == null ) {
      return;
    }
    
    if ( sessionQualifier == null ) {
      sessionQualifier = appName;
    }
    DataModelDatabaseHelper dbHelper = dbHelperSet.get(sessionQualifier);
    if (dbHelper != null) {
      // ensure that the database is released
      dbHelper.releaseDatabase();
      dbHelperSet.remove(sessionQualifier);
    }
  }

  public SQLiteDatabase getDatabase(Context context, String appName, String sessionQualifier) {
    SQLiteDatabase db = null;
    int count = 1;
    for (;db == null;++count) {
      try {
        db = getDbHelper(context, appName, sessionQualifier).getWritableDatabase();
      } catch ( SQLiteException e ) {
        if ( count == 201 ) {
          // give up after 200 * 20 + 180*50 = 13000 milliseconds...
          WebLogger.getLogger(appName).e("DatabaseFactory", 
              "database exception " + e.toString() + " -- abandoning attempt!");
          throw e;
        } else if ( count % 10 != 0 ) {
          try {
            WebLogger.getLogger(appName).i("DatabaseFactory", 
                "database exception " + e.toString() + " -- sleeping 50ms");
            Thread.sleep(50L);
          } catch (InterruptedException ex) {
            // ignore
          }
        } else {
          try {
            WebLogger.getLogger(appName).w("DatabaseFactory", 
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
}
