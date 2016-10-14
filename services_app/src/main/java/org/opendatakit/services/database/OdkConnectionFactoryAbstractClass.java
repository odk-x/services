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

package org.opendatakit.services.database;

import org.opendatakit.utilities.LocalizationUtils;
import org.opendatakit.services.database.utlities.ODKDatabaseImplUtils;
import org.opendatakit.utilities.ODKFileUtils;
import org.opendatakit.database.service.DbHandle;
import org.sqlite.database.sqlite.SQLiteCantOpenDatabaseException;
import org.sqlite.database.sqlite.SQLiteDatabaseLockedException;
import org.sqlite.database.sqlite.SQLiteException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.OverlappingFileLockException;
import java.util.*;
import java.nio.channels.FileLock;

/**
 * Factory interface that provides database connections and manages them.
 * This abstract class implements most of the management tracking involved.
 *
 * Derived classes should implement logging and openDatabase() APIs.
 *
 * @author clarlars@gmail.com
 * @author mitchellsundt@gmail.com
 */
public abstract class OdkConnectionFactoryAbstractClass implements OdkConnectionFactoryInterface {

  /**
   * the database schema version that the application expects
   */
  private static final int mNewVersion = 1;

  /**
   * object for guarding appNameSharedStateMap
   * <p/>
   * That map never has its contents removed, so once an AppNameSharedStateContainer
   * is obtained, we can hold it and interact with it outside of this mutex. The
   * mutex only ensures that the puts/gets are not corrupted in the TreeMap.
   */
  private final Object mutex = new Object();

  /**
   * This map MUST BE ACCESSED AND UPDATED after gaining the above mutex.
   * <p/>
   * map of appName -TO- AppNameSharedStateContainer
   * <p/>
   * AppNameSharedStateContainer contains the shared state for the given appName.
   * i.e., the appNameMutex and the sessionMutex generator, and the map of
   * sessionQualifier to DbHandles. Callers should obtain the sessionMutex
   * once and save it for later use, as it is not recoverable.
   * <p/>
   * The AppNameSharedStateContainer does its own multithreaded access protection,
   * and this map never removes items, so we can safely hold AppNameSharedStateContainer
   * entries outside of this map.
   */
  private final Map<String, AppNameSharedStateContainer> appNameSharedStateMap = new TreeMap<String, AppNameSharedStateContainer>();

  /**
   * Log the given Info message for the specified appName
   *
   * @param appName
   * @param message
   */
  protected abstract void logInfo(String appName, String message);

  /**
   * Log the given Warn message for the specified appName
   *
   * @param appName
   * @param message
   */
  protected abstract void logWarn(String appName, String message);

  /**
   * Log the given Error message for the specified appName
   *
   * @param appName
   * @param message
   */
  protected abstract void logError(String appName, String message);

  /**
   * Log the stack trace for the specified appName
   *
   * @param appName
   * @param throwable
   */
  protected abstract void printStackTrace(String appName, Throwable throwable);

  /**
   * Open a database connection under the given appName shared-state container with the given
   * qualifier.
   *
   * @param appNameSharedStateContainer
   * @param sessionQualifier
   * @return
   */
  protected abstract OdkConnectionInterface openDatabase(
          AppNameSharedStateContainer appNameSharedStateContainer, String sessionQualifier);

  /**
   * This handle is suitable for non-service uses.
   *
   * @return sessionQualifier appropriate for 'internal uses'
   */
  public final DbHandle generateInternalUseDbHandle() {
    return new DbHandle(LocalizationUtils.genUUID() + INTERNAL_TYPE_SUFFIX);
  }

  /**
   * This handle is suitable for database service use.
   *
   * @return sessionQualifier appropriate for 'database service uses'
   */
  public final DbHandle generateDatabaseServiceDbHandle() {
    return new DbHandle(LocalizationUtils.genUUID());
  }

  /**
   * Dump the state and history of the database layer.
   * Useful for debugging and understanding
   * cross-thread interactions.
   */
  public final void dumpInfo(boolean asError) {
    ArrayList<AppNameSharedStateContainer> containers = new ArrayList<AppNameSharedStateContainer>();
    synchronized (mutex) {
      for (String appName : appNameSharedStateMap.keySet()) {
        containers.add(appNameSharedStateMap.get(appName));
      }
    }

    for (AppNameSharedStateContainer container : containers) {
      StringBuilder b = new StringBuilder();
      container.dumpInfo(b);
      if ( asError ) {
        logError(container.getAppName(), b.toString());
      } else {
        logInfo(container.getAppName(), b.toString());
      }
    }
  }

  private static String getDbFilePath(String appName) {
    File dbFile = new File(ODKFileUtils.getWebDbFolder(appName),
            ODKFileUtils.getNameOfSQLiteDatabase());
    String dbFilePath = dbFile.getAbsolutePath();
    return dbFilePath;
  }

  private static String getDbLockFilePath(String appName) {
    File dbLockFile = new File(ODKFileUtils.getDataFolder(appName),
            ODKFileUtils.getNameOfSQLiteDatabaseLockFile());
    String dbLockFilePath = dbLockFile.getAbsolutePath();
    return dbLockFilePath;
  }

  /**
   * Create a new connection and insert it into the connection-map.
   * If the initialization logic should be run on this connection,
   * execute it. Otherwise, simply +1 reference count the connection
   * and return it.
   *
   * @param appNameSharedStateContainer
   * @param sessionQualifier
   * @return
   * @throws SQLiteException
   */
  private final OdkConnectionInterface getNewConnectionImpl(
          AppNameSharedStateContainer appNameSharedStateContainer, String sessionQualifier)
          throws SQLiteException, IllegalAccessException {

    if (sessionQualifier == null) {
      throw new IllegalArgumentException(
              "getNewConnectionImpl: null sessionQualifier");
    }

    OdkConnectionInterface dbConnection = null;
    String appName = appNameSharedStateContainer.getAppName();

    // Get file lock
    String dbLockFile = getDbLockFilePath(appName);

    File lockfile = new File(dbLockFile);

    File dbFile = null;
    FileLock fileLock = null;
    try {
      if (!lockfile.exists()) {
        lockfile.createNewFile();
      }

      RandomAccessFile raf = new RandomAccessFile(lockfile, "rw");

      fileLock = raf.getChannel().lock();

      if (fileLock != null) {
        if (fileLock.isShared()) {
          throw new IllegalArgumentException("DB lock file cannot be shared");
        }

        // Now write a number to the file
        long ms = System.currentTimeMillis();
        raf.writeLong(ms);

        // Now that we know we have a good lock
        // check that sqlite.db exists
        String dbFileName = getDbFilePath(appName);
        dbFile = new File(dbFileName);

        // If it doesn't exist, then we need to
        // create it and initialize it!!
        if (!dbFile.exists()) {
          // Attempt to open the database
          dbConnection = attemptToOpenDb(appNameSharedStateContainer, sessionQualifier);

          // Now run initialization
          dbConnection = initDatabase(sessionQualifier, dbConnection, appName);
        } else {
          // We add connection to map and are done with it
          dbConnection = attemptToOpenDb(appNameSharedStateContainer, sessionQualifier);
        }
      }

      if (dbConnection != null) {
        OdkConnectionInterface dbConnectionExisting = null;
        dbConnectionExisting = appNameSharedStateContainer.atomicSetOrGetExisting(sessionQualifier, dbConnection);

        if (dbConnectionExisting != null) {
          throw new IllegalAccessException("An exising db connection should not be found in getNewConnectionImpl");
        }
      }
    } catch (FileNotFoundException fnfe) {
      fnfe.printStackTrace();
    } catch (IOException ioe) {
      ioe.printStackTrace();
    } catch (OverlappingFileLockException ofle) {
      ofle.printStackTrace();
    }  catch (NonWritableChannelException nwce) {
      nwce.printStackTrace();
    } finally {
      try {
        if (fileLock != null) {
          fileLock.release();
        }
      } catch (IOException ioe) {
        ioe.printStackTrace();
      }
    }

    return dbConnection;
  }

  private OdkConnectionInterface initDatabase(String sessionQualifier, OdkConnectionInterface dbConnection, String appName) {
    boolean initSuccessful = false;
    try {
      dbConnection.beginTransactionExclusive();
      try {
        int version = dbConnection.getVersion();
        if (version != mNewVersion) {
          if (version == 0) {
            logInfo(appName, "getNewConnectionImpl -- Invoking onCreate for " + appName +
                    " " + sessionQualifier);
            onCreate(dbConnection);
          } else {
            logInfo(appName, "getNewConnectionImpl -- Invoking onUpgrade for " + appName +
                    " " + sessionQualifier);
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
      if ( !initSuccessful ) {
        try {
          logInfo(appName,
                  "getNewConnectionImpl -- " + appName +
                          " " + sessionQualifier + " -- removing session on database");
          // -1 to let go of +1 from creation or retrieval via getExisting()
          dbConnection.releaseReference();
        } finally {
          // and the connection map holds 1 reference count
          // release it to destroy connection
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
    return dbConnection;
  }

  private OdkConnectionInterface attemptToOpenDb(AppNameSharedStateContainer appNameSharedStateContainer, String sessionQualifier)
  throws SQLiteDatabaseLockedException{

    // Exception will be thrown if the db cannot be opened
    String appName = appNameSharedStateContainer.getAppName();
    OdkConnectionInterface dbConnection = null;
    SQLiteDatabaseLockedException ex = null;
    int MAX_OPEN_RETRY_COUNT = 3;
    int retryCount = 1;
    for (; retryCount <= MAX_OPEN_RETRY_COUNT ; ++retryCount ) {
      try {
        dbConnection = openDatabase(appNameSharedStateContainer, sessionQualifier);
        break;
      } catch ( SQLiteDatabaseLockedException e ) {
        if ( retryCount == MAX_OPEN_RETRY_COUNT ) {
          StringBuilder b = new StringBuilder();
          b.append("openDatabase Attempt ").append(retryCount)
              .append(" Failed: throwing  SQLiteDatabaseLockedException");
          appNameSharedStateContainer.dumpInfo(b);
          logError(appName, b.toString());
        } else {
          logWarn(appName, "openDatabase Attempt " + retryCount +
                  " Failed: will throw an exception after attempt " + MAX_OPEN_RETRY_COUNT);
        }
        ex = e;
        long retryDelay = (retryCount+1)*300L;
        try {
          Thread.sleep(retryDelay);
        } catch (InterruptedException e1) {
          // ignore
        }
      }
    }
    if ( dbConnection == null ) {
      throw ex;
    } else {
      return dbConnection;
    }
  }

  /**
   * Attempt to retrieve an existing connection for the sessionQualifier.
   *
   * Tests whether the sessionQualifier database connection exists.
   *
   * If it does not, the connection is created and verified. The database schema
   * is updated on that connection.
   *
   * Each connection checks for the existence of the sqlite.db file.  If this file
   * does not exist, it is created, and the database is initialized.  A file lock is used
   * to prevent issues with multiple threads.  This throws an exception
   * if the opening of the connection or initialization fails.
   *
   * Following this, the existing connection for sessionQualifier is
   * returned with a +1 reference count or, if there is no existing
   * connection, a new connection is opened, inserted into the connection-map
   * and returned with a +1 reference count.  This throws an exception
   * if the connection could not be opened.
   *
   * @param appName
   * @param sessionQualifier
   * @throws SQLiteCantOpenDatabaseException
   * @return
   */
  private final OdkConnectionInterface getConnectionImpl(final String appName,
                                                         String sessionQualifier) {

    if (sessionQualifier == null) {
      throw new IllegalArgumentException("session qualifier cannot be null");
    }
    if (sessionQualifier.equals(appName)) {
      throw new IllegalArgumentException("session qualifier cannot be the same as the appName");
    }

    OdkConnectionInterface dbConnection = null;

    AppNameSharedStateContainer appNameSharedStateContainer = null;
    boolean hasBeenInitialized = false;
    {
      synchronized (mutex) {
        appNameSharedStateContainer = appNameSharedStateMap.get(appName);
        if (appNameSharedStateContainer == null) {
          appNameSharedStateContainer = new AppNameSharedStateContainer(appName);
          appNameSharedStateMap.put(appName, appNameSharedStateContainer);
        }
      }

      dbConnection = appNameSharedStateContainer.getExisting(sessionQualifier);

      if (dbConnection != null) {
        // logInfo(appName, "getConnectionImpl -- " + sessionQualifier +
        //        " -- successfully obtained reference to already-open database for " + appName +
        //        " when getting " + sessionQualifier);
        return dbConnection;
      }

    }
    // otherwise, create a connection for the sessionQualifier and return that
    logInfo(appName, "getConnectionImpl -- " + sessionQualifier +
            " -- creating new connection for " + appName + " when getting " + sessionQualifier);
    OdkConnectionInterface db = null;
    try {
      db = getNewConnectionImpl(appNameSharedStateContainer,
              sessionQualifier);
    } catch (IllegalAccessException e) {
      e.printStackTrace();
      throw new SQLiteCantOpenDatabaseException("Found existing conn on new - WHAAT?");
    }

    if ( db == null ) {
      throw new SQLiteCantOpenDatabaseException("unable to initialize session database for "
              + appName + " when getting " + sessionQualifier);
    } else {
      return db;
    }
  }

  @Override
  public final OdkConnectionInterface getConnection(String appName,
                                                    DbHandle dbHandleName) {
    if (dbHandleName != null) {
      return getConnectionImpl(appName, dbHandleName.getDatabaseHandle());
    } else {
      throw new IllegalArgumentException("null dbHandleName " + appName);
    }
  }

  @Override
  public final void removeConnection(String appName, DbHandle dbHandleName) {
    removeConnectionImpl(appName, dbHandleName.getDatabaseHandle());
  }

  /**
   * Remove the connection for the indicated sessionQualifier from the connection-map and
   * release the reference held on it by that map once we are outside of the map mutex.
   * <p/>
   * This will generally trigger closure of the connection.
   *
   * @param appName
   * @param sessionQualifier
   */
  private final void removeConnectionImpl(String appName, String sessionQualifier) {
    boolean releaseTwice = false;
    OdkConnectionInterface dbConnection = null;
    if (appName == null) {
      throw new IllegalArgumentException("appName cannot be null!");
    }
    if (sessionQualifier == null) {
      throw new IllegalArgumentException("sessionQualifier cannot be null!");
    }
    try {
      AppNameSharedStateContainer appNameSharedStateContainer = null;
      synchronized (mutex) {
        appNameSharedStateContainer = appNameSharedStateMap.get(appName);
      }
      if (appNameSharedStateContainer == null) {
        // nothing to do...
        return;
      }
      // +1 reference count (or null)
      dbConnection = appNameSharedStateContainer.getExisting(sessionQualifier);
      if (dbConnection != null) {
        // no change in reference count
        releaseTwice = appNameSharedStateContainer.moveIntoPendingDestruction(dbConnection);
      }
    } finally {
      if (dbConnection != null) {
        // -1 for getExisting
        dbConnection.releaseReference();
        if (releaseTwice) {
          // -1 for removal from sessionQualifierConnectionMap
          dbConnection.releaseReference();
        }
      }
    }
  }

  /**
   * Removes all database handles under the specified appName
   * that are created for the database service; i.e., have a
   * {OdkConnectionFactoryInterface.generateDatabaseServiceDbHandle()}
   *
   * @param appName
   * @return true if we removed something. false otherwise.
   */
  private final boolean removeDatabaseServiceConnections(String appName) {
    AppNameSharedStateContainer appNameSharedStateContainer = null;
    synchronized (mutex) {
      appNameSharedStateContainer = appNameSharedStateMap.get(appName);
    }
    if (appNameSharedStateContainer == null) {
      // nothing to do...
      return false;
    }
    TreeSet<String> allSessionQualifiers = appNameSharedStateContainer.getAllSessionQualifiers();
    if ( allSessionQualifiers.isEmpty() ) {
      // nothing to do...
      return false;
    }

    TreeSet<String> sessionQualifiers = new TreeSet<String>();
    for (String sessionQualifier : allSessionQualifiers) {
      // not the base (appName) session
      // not an internal session
      if (!sessionQualifier.equals(appName) &&
          !sessionQualifier.endsWith(INTERNAL_TYPE_SUFFIX)) {
        sessionQualifiers.add(sessionQualifier);
      }
    }

    for (String sessionQualifier : sessionQualifiers) {
      logInfo(appName, "removeDatabaseServiceConnections " + sessionQualifier);
      try {
        removeConnectionImpl(appName, sessionQualifier);
      } catch (Exception e) {
        logError(appName,
                "removeDatabaseServiceConnections when releasing " + sessionQualifier);
        printStackTrace(appName, e);
      }
    }
    return !sessionQualifiers.isEmpty();
  }

  /**
   * Remove all connections for the given appName
   * @param appName
   * @return true if anything was removed.
   */
  private final boolean removeAllConnections(String appName) {
    AppNameSharedStateContainer appNameSharedStateContainer = null;
    synchronized (mutex) {
      appNameSharedStateContainer = appNameSharedStateMap.get(appName);
    }
    if (appNameSharedStateContainer == null) {
      // nothing to do...
      return false;
    }
    TreeSet<String> sessionQualifiers = appNameSharedStateContainer.getAllSessionQualifiers();
    if ( sessionQualifiers.isEmpty() ) {
      // nothing to do...
      return false;
    }

    boolean hasAppNameQualifier = false;
    for (String sessionQualifier : sessionQualifiers) {
      if (sessionQualifier.equals(appName)) {
        // release the appName one last...
        hasAppNameQualifier = true;
        continue;
      }
      logInfo(appName, "removeAllConnections " + sessionQualifier);
      try {
        removeConnectionImpl(appName, sessionQualifier);
      } catch (Exception e) {
        logError(appName, "removeAllConnections when releasing " + sessionQualifier);
        printStackTrace(appName, e);
      }
    }

    if (hasAppNameQualifier) {
      logInfo(appName, "removeAllConnections " + appName);
      try {
        removeConnectionImpl(appName, appName);
      } catch (Exception e) {
        logError(appName, "removeAllConnections when releasing " + appName);
        printStackTrace(appName, e);
      }
    }
    return !sessionQualifiers.isEmpty();
  }

  @Override
  public final boolean removeAllDatabaseServiceConnections() {
    HashSet<String> appNames = new HashSet<String>();
    synchronized (mutex) {
      appNames.addAll(appNameSharedStateMap.keySet());
    }
    for (String appName : appNames) {
      removeDatabaseServiceConnections(appName);
    }
    return !appNames.isEmpty();
  }

  @Override
  public final void removeAllConnections() {
    HashSet<String> appNames = new HashSet<String>();
    synchronized (mutex) {
      appNames.addAll(appNameSharedStateMap.keySet());
    }
    for (String appName : appNames) {
      removeAllConnections(appName);
    }
  }

  /**
   * Called when the database is created for the first time. This is where the
   * creation of tables and the initial population of the tables should happen.
   *
   * @param db The database.
   */
  protected final void onCreate(OdkConnectionInterface db) {
    ODKDatabaseImplUtils.initializeDatabase(db);
  }

  /**
   * Called when the database needs to be upgraded. The implementation should
   * use this method to drop tables, add tables, or do anything else it needs to
   * upgrade to the new schema version.
   * <p/>
   * The SQLite ALTER TABLE documentation can be found <a
   * href="http://sqlite.org/lang_altertable.html">here</a>. If you add new
   * columns you can use ALTER TABLE to insert them into a live table. If you
   * rename or remove columns you can use ALTER TABLE to rename the old table,
   * then create the new table and then populate the new table with the contents
   * of the old table.
   *
   * @param db         The database.
   * @param oldVersion The old database version.
   * @param newVersion The new database version.
   */
  protected final void onUpgrade(OdkConnectionInterface db, int oldVersion, int newVersion) {
    // for now, upgrade and creation use the same codepath...
    ODKDatabaseImplUtils.initializeDatabase(db);
  }

}
