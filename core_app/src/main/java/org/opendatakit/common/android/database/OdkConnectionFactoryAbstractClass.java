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

package org.opendatakit.common.android.database;

import org.opendatakit.common.android.utilities.ODKDataUtils;
import org.opendatakit.common.android.utilities.ODKDatabaseImplUtils;
import org.opendatakit.database.service.OdkDbHandle;
import org.sqlite.database.sqlite.SQLiteCantOpenDatabaseException;
import org.sqlite.database.sqlite.SQLiteException;

import java.util.*;

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
    * sessionQualifier to OdkDbHandles. Callers should obtain the sessionMutex
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
   public final OdkDbHandle generateInternalUseDbHandle() {
      return new OdkDbHandle(ODKDataUtils.genUUID() + AndroidConnectFactory.INTERNAL_TYPE_SUFFIX);
   }

   /**
    * This handle is suitable for database service use.
    *
    * @return sessionQualifier appropriate for 'database service uses'
    */
   public final OdkDbHandle generateDatabaseServiceDbHandle() {
      return new OdkDbHandle(ODKDataUtils.genUUID());
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

   /**
    * Create a new connection and insert it into the connection-map.
    * If the initialization logic should be run on this connection,
    * execute it. Otherwise, simply +1 reference count the connection
    * and return it.
    *
    * @param appNameSharedStateContainer
    * @param sessionQualifier
    * @param shouldInitialize
    * @return
    * @throws SQLiteException
    */
   private final OdkConnectionInterface getNewConnectionImpl(
       AppNameSharedStateContainer appNameSharedStateContainer, String sessionQualifier,
       boolean shouldInitialize) throws SQLiteException {

      if (sessionQualifier == null) {
         throw new IllegalArgumentException(
             "getNewConnectionImpl: null sessionQualifier");
      }

      String appName = appNameSharedStateContainer.getAppName();

      OdkConnectionInterface dbConnection = null;
      OdkConnectionInterface dbConnectionExisting = null;
      logInfo(appName, "getNewConnectionImpl -- opening database for " +
          appName + " " + sessionQualifier);

      // this throws an exception if the db cannot be opened
      dbConnection = openDatabase(appNameSharedStateContainer, sessionQualifier);
      if ( dbConnection != null ) {
         boolean success = false;
         try {
            logInfo(appName,
                "getNewConnectionImpl -- " + sessionQualifier + " -- opening new session on database");

            for (;;) {
               dbConnectionExisting = null;
               // +1 of either dbConnectionExisting (!= null) or else of dbConnection
               dbConnectionExisting = appNameSharedStateContainer
                   .atomicSetOrGetExisting(sessionQualifier, dbConnection);

               if (dbConnectionExisting == null) {
                  // return this connection (no conflict)
                  dbConnectionExisting = dbConnection;
                  break;
               }

               // block waiting for the existing connection in the map to complete initialization.
               boolean outcome = dbConnectionExisting.waitForInitializationComplete();
               if (outcome) {
                  // success -- release the dbConnection we opened
                  // and return the existing connection
                  boolean releaseTwice =
                      appNameSharedStateContainer.moveIntoPendingDestruction(dbConnection);
                  // by returning the other connection, caller knows to -1 dbConnection
                  break;
               } else {
                  // release the existing connection (because init failed)
                  dbConnectionExisting.releaseReference();
                  // give another thread time to become aware that the connection
                  // is bad and remove it.
                  try {
                     Thread.sleep(50L);
                  } catch (InterruptedException e) {
                     e.printStackTrace();
                  }
                  // and loop, trying to insert our connection
               }
            }
         } finally {
            if ( dbConnection != dbConnectionExisting ) {
               // we hold 1 reference count -- release it to destroy connection
               dbConnection.releaseReference();
               if ( sessionQualifier.equals(appName) ) {
                  logWarn(appName,
                      "getNewConnectionImpl -- Successfully resolved Contention when " +
                          "initially opening database for "
                          + appName + " " + sessionQualifier);
                  return dbConnectionExisting;
               } else {
                  dbConnectionExisting.releaseReference();
                  throw new IllegalStateException("Unexpected contention when opening database for "
                      + appName + " " + sessionQualifier);
               }
            }
         }
      }

      // upon success, we hold two references to the connection, one in map, one here on stack
      if ( shouldInitialize ) {
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
      }
      return dbConnection;
   }

   /**
    * Attempt to retrieve an existing connection for the sessionQualifier.
    *
    * Tests whether the base 'appName' database connection exists.
    *
    * If it does not, it creates it and verifies and updates the database schema
    * on that 'appName' connection.
    *
    * Otherwise, it tests whether initialization is complete.
    *
    * If the initialization is not complete and there is no already-open
    * connection for this sessionQualifier, an 'appName' connection is
    * initiated to open and initialize the database.  This throws an exception
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
         OdkConnectionInterface dbConnectionAppName = null;
         try {
            synchronized (mutex) {
               appNameSharedStateContainer = appNameSharedStateMap.get(appName);
               if (appNameSharedStateContainer == null) {
                  appNameSharedStateContainer = new AppNameSharedStateContainer(appName);
                  appNameSharedStateMap.put(appName, appNameSharedStateContainer);
               }
            }
            // +1 reference count (or null)
            dbConnectionAppName = appNameSharedStateContainer.getExisting(appName);
            // +1 reference count (or null)
            dbConnection = appNameSharedStateContainer.getExisting(sessionQualifier);

            if (dbConnectionAppName != null) {
               logInfo(appName, "getConnectionImpl -- " + sessionQualifier +
                   " -- obtaining reference to base database for " + appName +
                   " when getting " + sessionQualifier);
            }

            if (dbConnection != null) {
               logInfo(appName, "getConnectionImpl -- " + sessionQualifier +
                   " -- obtaining reference to already-open database for " + appName +
                   " when getting " + sessionQualifier);
            }

            if (dbConnectionAppName != null) {
               hasBeenInitialized = dbConnectionAppName.waitForInitializationComplete();
            }
         } finally {
            if (dbConnectionAppName != null) {
               // -1 reference count
               // dbConnectionAppName should not be used below this point...
               dbConnectionAppName.releaseReference();
            }
         }
      }

      // see if the base database was successfully initialized.
      //
      // If it was not, and we don't have an existing connection for this
      // sessionQualifier, then initialize the base database.
      if (!hasBeenInitialized && dbConnection == null) {
         logInfo(appName, "getConnectionImpl -- " + sessionQualifier +
             " -- triggering initialization of base database for " + appName +
             " when getting " + sessionQualifier);

         // the "appName" database qualifier  is created once and left open
         OdkConnectionInterface dbConnectionAppName = getNewConnectionImpl(
             appNameSharedStateContainer, appName, true);

         if ( dbConnectionAppName == null ) {
            throw new SQLiteCantOpenDatabaseException("unable to initialize base database for "
                + appName + " when getting " + sessionQualifier);
         } else {
            // we aren't going to do anything with this, so we can release it.
            // It is retained in the connection map.
            dbConnectionAppName.releaseReference();
         }
      }

      // and if we do have an existing connection,
      // it already has +1 on it, so just return it.
      if (dbConnection != null) {
         logInfo(appName, "getConnectionImpl -- " + sessionQualifier +
             " -- returning an existing session for " + appName +
             " when getting " + sessionQualifier);
         return dbConnection;
      }

      // otherwise, create a connection for the sessionQualifier and return that
      logInfo(appName, "getConnectionImpl -- " + sessionQualifier +
          " -- creating new connection for " + appName + " when getting " + sessionQualifier);
      OdkConnectionInterface db = getNewConnectionImpl(appNameSharedStateContainer,
          sessionQualifier, false);

      if ( db == null ) {
         throw new SQLiteCantOpenDatabaseException("unable to initialize session database for "
             + appName + " when getting " + sessionQualifier);
      } else {
         return db;
      }
   }

   @Override
   public final OdkConnectionInterface getConnection(String appName,
       OdkDbHandle dbHandleName) {
      if (dbHandleName != null) {
         return getConnectionImpl(appName, dbHandleName.getDatabaseHandle());
      } else {
         throw new IllegalArgumentException("null dbHandleName " + appName);
      }
   }

   @Override
   public final void removeConnection(String appName, OdkDbHandle dbHandleName) {
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

   @Override
   public final OdkConnectionInterface getSessionGroupInstanceConnection(String appName,
       String sessionGroupQualifier, int instanceQualifier) {
      String sessionQualifier =
          sessionGroupQualifier + GROUP_TYPE_DIVIDER + Integer.toString(instanceQualifier);
      return getConnectionImpl(appName, sessionQualifier);
   }

   @Override
   public final void removeSessionGroupInstanceConnection(String appName,
       String sessionGroupQualifier, int instanceQualifier) {
      String sessionQualifier =
          sessionGroupQualifier + GROUP_TYPE_DIVIDER + Integer.toString(instanceQualifier);
      removeConnectionImpl(appName, sessionQualifier);
   }

   @Override
   public final boolean removeSessionGroupConnections(String appName, String sessionGroupQualifier,
       boolean removeNonMatchingGroupsOnly) {
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
         int idx = sessionQualifier.lastIndexOf(GROUP_TYPE_DIVIDER);
         if (idx != -1) {
            String sessionGroup = sessionQualifier.substring(0, idx);
            if (removeNonMatchingGroupsOnly) {
               if (sessionGroupQualifier == null || !sessionGroup.equals(sessionGroupQualifier)) {
                  sessionQualifiers.add(sessionQualifier);
               }
            } else {
               if (sessionGroupQualifier != null && sessionGroup.equals(sessionGroupQualifier)) {
                  sessionQualifiers.add(sessionQualifier);
               }
            }
         }
      }

      for (String sessionQualifier : sessionQualifiers) {
         logInfo(appName, "removeSessionGroupConnections " + sessionQualifier);
         try {
            removeConnectionImpl(appName, sessionQualifier);
         } catch (Exception e) {
            logError(appName, "removeSessionGroupConnections when releasing " + sessionQualifier);
            printStackTrace(appName, e);
         }
      }
      return !sessionQualifiers.isEmpty();
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
         // not a group (no group divider)
         // not the base (appName) session
         // not an internal session
         if ((sessionQualifier.lastIndexOf(GROUP_TYPE_DIVIDER) == -1) &&
             !sessionQualifier.equals(appName) &&
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
   public final boolean removeAllSessionGroupConnections(String appName) {
      return removeSessionGroupConnections(appName, null, true);
   }

   @Override
   public final boolean removeAllSessionGroupConnections() {
      HashSet<String> appNames = new HashSet<String>();
      synchronized (mutex) {
         appNames.addAll(appNameSharedStateMap.keySet());
      }
      for (String appName : appNames) {
         removeSessionGroupConnections(appName, null, true);
      }
      return !appNames.isEmpty();
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
