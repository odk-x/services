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

import org.opendatakit.common.android.utilities.ODKDatabaseImplUtils;
import org.opendatakit.common.android.utilities.ODKFileUtils;
import org.opendatakit.common.android.utilities.StaticStateManipulator;
import org.opendatakit.common.android.utilities.StaticStateManipulator.IStaticFieldManipulator;
import org.opendatakit.common.android.utilities.WebLogger;

import java.io.File;

public final class AndroidConnectFactory  extends OdkConnectionFactoryInterface {

  /** the version that the application expects */
  private final int mNewVersion = 1;

  static {

    // loads our custom libsqliteX.so
    System.loadLibrary("sqliteX");

    OdkConnectionFactorySingleton.set(new AndroidConnectFactory());

    // register a state-reset manipulator for 'connectionFactory' field.
    StaticStateManipulator.get().register(50, new IStaticFieldManipulator() {

      @Override
      public void reset() {
        OdkConnectionFactorySingleton.set(new AndroidConnectFactory());
      }

    });
  }

  public static void configure() {
    // just to get the static initialization block (above) to run
  }

  private AndroidConnectFactory() {
  }

  @Override
  protected void logInfo(String appName, String message) {
    WebLogger.getLogger(appName).i("AndroidConnectFactory", message);
  }

  @Override
  protected void logWarn(String appName, String message) {
    WebLogger.getLogger(appName).w("AndroidConnectFactory", message);
  }

  @Override
  protected void logError(String appName, String message) {
    WebLogger.getLogger(appName).e("AndroidConnectFactory", message);
  }

  @Override
  protected void printStackTrace(String appName, Throwable e) {
    WebLogger.getLogger(appName).printStackTrace(e);
  }

  /**
   * Shared accessor to get a database handle.
   *
   * @param context
   * @param appNameMutex
   * @param appName
   * @param sessionQualifier
   * @param shouldInitialize
   * @param manipulator
   * @return an entry in appConnectionsMap
   */
  @Override
  protected OdkConnectionInterface getDbConnection(Context context, Object appNameMutex, String appName,
                                                                String sessionQualifier,
                                                                boolean shouldInitialize,
                                                                SessionQualifierToConnectionMapManipulator manipulator) {
    WebLogger logger = null;

    try {
      ODKFileUtils.verifyExternalStorageAvailability();
      ODKFileUtils.assertDirectoryStructure(appName);
      logger = WebLogger.getLogger(appName);
    } catch (Exception e) {
      if (logger != null) {
        logger.e("AndroidConnectFactory",
                "External storage not available -- purging appConnectionsMap for " + appName + " " + sessionQualifier);
      }
      releaseDatabaseInstances(context, appName);
      return null;
    }

    OdkConnectionInterface dbConnection = null;
    OdkConnectionInterface dbConnectionExisting = null;
    logger.i("AndroidConnectFactory", "getDbConnection -- opening database for " + appName + " " + sessionQualifier);

    // this throws an exception if the db cannot be opened
    dbConnection = AndroidOdkConnection.openDatabase(appNameMutex, appName, getDbFilePath(appName), sessionQualifier);
    if ( dbConnection != null ) {
      boolean success = false;
      try {
        dbConnectionExisting = manipulator.put(sessionQualifier, dbConnection);
      } finally {
        if ( dbConnection != dbConnectionExisting ) {
          // we hold 1 reference count -- release it to destroy connection
          dbConnection.releaseReference();
          if ( sessionQualifier.equals(appName) ) {
            logger.w("AndroidConnectFactory", "Successfully resolved Contention when initially opening database for " + appName + " " + sessionQualifier);
            return dbConnectionExisting;
          } else {
            dbConnectionExisting.releaseReference();
            throw new IllegalStateException("Unexpected contention when opening database for " + appName + " " + sessionQualifier);
          }
        }
      }
    }

    // upon success, we hold two references to the connection, one in map, one here on stack
    if ( shouldInitialize ) {
      boolean initSuccessful = false;
      try {
        dbConnection.beginTransactionNonExclusive();
        try {
          int version = dbConnection.getVersion();
          if (version != mNewVersion) {
            if (version == 0) {
              logger.i("AndroidConnectionFactory","Invoking onCreate for " + appName + " " + sessionQualifier);
              onCreate(dbConnection);
            } else {
              logger.i("AndroidConnectionFactory","Invoking onUpgrade for " + appName + " " + sessionQualifier);
              onUpgrade(dbConnection, version, mNewVersion);
            }
            dbConnection.setVersion(mNewVersion);
            dbConnection.setTransactionSuccessful();
          } else {
            dbConnection.setTransactionSuccessful();
          }
        } finally {
          dbConnection.endTransaction();
        }
        initSuccessful = true;
      } finally {
        dumpInfo();
        if ( !initSuccessful ) {
          try {
            manipulator.remove(sessionQualifier);
          } finally {
            // we hold 1 reference count -- release it to destroy connection
            OdkConnectionInterface tmp = dbConnection;
            dbConnection = null;
            // signal first -- releasing the reference may release this object...
            tmp.signalInitializationComplete(false);
            tmp.releaseReference();
          }
        } else {
          dbConnection.signalInitializationComplete(true);
        }
      }
    }
    return dbConnection;
  }

  private String getDbFilePath(String appName) {
    File dbFile = new File(ODKFileUtils.getWebDbFolder(appName),
            ODKFileUtils.getNameOfSQLiteDatabase());
    String dbFilePath = dbFile.getAbsolutePath();
    return dbFilePath;
  }
}
