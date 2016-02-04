package org.opendatakit.common.android.database;

import android.content.Context;
import android.database.sqlite.SQLiteCantOpenDatabaseException;
import android.database.sqlite.SQLiteDatabaseLockedException;
import android.database.sqlite.SQLiteException;

import org.opendatakit.common.android.utilities.ODKDataUtils;
import org.opendatakit.common.android.utilities.ODKDatabaseImplUtils;
import org.opendatakit.common.android.utilities.StaticStateManipulator;
import org.opendatakit.common.android.utilities.WebLogger;
import org.opendatakit.database.service.OdkDbHandle;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Created by clarice on 1/27/16.
 */
public class AndroidConvConnectFactory extends OdkConnectionFactoryAbstractClass {
  static {
    OdkConnectionFactorySingleton.set(new AndroidConvConnectFactory());

    // register a state-reset manipulator for 'connectionFactory' field.
    StaticStateManipulator.get().register(50, new StaticStateManipulator.IStaticFieldManipulator() {

      @Override
      public void reset() {
        OdkConnectionFactorySingleton.set(new AndroidConvConnectFactory());
      }

    });
  }

  public static void configure() {
    // just to get the static initialization block (above) to run
  }

  private AndroidConvConnectFactory() {
  }

  @Override
  protected void logInfo(String appName, String message) {
    WebLogger.getLogger(appName).i("AndroidConvConnectFactory", message);
  }

  @Override
  protected void logWarn(String appName, String message) {
    WebLogger.getLogger(appName).w("AndroidConvConnectFactory", message);
  }

  @Override
  protected void logError(String appName, String message) {
    WebLogger.getLogger(appName).e("AndroidConvConnectFactory", message);
  }

  @Override
  protected void printStackTrace(String appName, Throwable e) {
    WebLogger.getLogger(appName).printStackTrace(e);
  }

  @Override
  protected OdkConnectionInterface openDatabase(AppNameSharedStateContainer appNameSharedStateContainer,
                                                String sessionQualifier, Context context) {
    return AndroidConvOdkConnection.openDatabase(appNameSharedStateContainer, sessionQualifier,
            context);
  }

  /**
   * the database schema version that the application expects
   */
  private static final int mNewVersion = 1;

  /**
   * object for guarding appNameSharedStateMap
   * <p>
   * That map never has its contents removed, so once an AppNameSharedStateContainer
   * is obtained, we can hold it and interact with it outside of this mutex. The
   * mutex only ensures that the puts/gets are not corrupted in the TreeMap.
   */
  private final Object mutex = new Object();

  /**
   * This map MUST BE ACCESSED AND UPDATED after gaining the above mutex.
   * <p>
   * map of appName -TO- AppNameSharedStateContainer
   * <p>
   * AppNameSharedStateContainer contains the shared state for the given appName.
   * i.e., the appNameMutex and the sessionMutex generator, and the map of
   * sessionQualifier to OdkDbHandles. Callers should obtain the sessionMutex
   * once and save it for later use, as it is not recoverable.
   * <p>
   * The AppNameSharedStateContainer does its own multithreaded access protection,
   * and this map never removes items, so we can safely hold AppNameSharedStateContainer
   * entries outside of this map.
   */
  private final Map<String, AppNameSharedStateContainer> appNameSharedStateMap = new TreeMap<String, AppNameSharedStateContainer>();

  /**
   * Dump the state and history of the database layer.
   * Useful for debugging and understanding
   * cross-thread interactions.
   */
  public final void dumpInfo(boolean asError) {
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
   * @param context
   * @return
   * @throws SQLiteException
   */
  private OdkConnectionInterface getNewConnectionImpl(
            AppNameSharedStateContainer appNameSharedStateContainer, String sessionQualifier,
            boolean shouldInitialize, Context context) throws SQLiteException {

    if (sessionQualifier == null) {
      throw new IllegalArgumentException("getNewConnectionImpl: null sessionQualifier");
    }

    String appName = appNameSharedStateContainer.getAppName();

    OdkConnectionInterface dbConnection = null;
    //OdkConnectionInterface dbConnectionExisting = null;
    logInfo(appName, "getNewConnectionImpl -- opening database for " +
            appName + " " + sessionQualifier + " shouldInitialize=" + shouldInitialize);

    // this throws an exception if the db cannot be opened
    SQLiteDatabaseLockedException ex = null;
    try {
      dbConnection = openDatabase(appNameSharedStateContainer, sessionQualifier, context);
      logInfo(appName, "getNewConnectionImpl -- opening database for " +
              appName + " " + sessionQualifier + " dbConnection=" + dbConnection);
    } catch (SQLiteDatabaseLockedException e) {
      StringBuilder b = new StringBuilder();
      b.append("openDatabase Attempt ")
      .append(" Failed: throwing  SQLiteDatabaseLockedException");
      //appNameSharedStateContainer.dumpInfo(b);

    }
    if (dbConnection == null) {
      logInfo(appName, "getNewConnectionImpl -- dbConnection is null - going to throw exception");
      throw ex;
    }

    // upon success, we hold two references to the connection, one in map, one here on stack
    if (shouldInitialize) {
      boolean initSuccessful = false;
      try {
        // CAL: Does this need to be exclusive?
        //dbConnection.beginTransactionExclusive();
        dbConnection.beginTransactionNonExclusive();

        int version = dbConnection.getVersion();
        logInfo(appName, "getNewConnectionImpl -- Invoking onCreate for " + appName + " " +
                sessionQualifier);
        onCreate(dbConnection);
        dbConnection.setVersion(mNewVersion);
        dbConnection.setTransactionSuccessful();

        initSuccessful = true;
      } catch (Exception e) {
        System.out.println("Exception while initializing database");
        e.printStackTrace();
      } finally {
        dbConnection.endTransaction();
        if (!initSuccessful) {
          throw new SQLiteException
                  ("AndroidConvConnectFactory: Database was not initialized successfully!!");
        }
      }
    }
    return dbConnection;
  }

  /**
   * Attempt to retrieve an existing connection for the sessionQualifier.
   * <p>
   * Tests whether the base 'appName' database connection exists.
   * <p>
   * If it does not, it creates it and verifies and updates the database schema
   * on that 'appName' connection.
   * <p>
   * Otherwise, it tests whether initialization is complete.
   * <p>
   * If the initialization is not complete and there is no already-open
   * connection for this sessionQualifier, an 'appName' connection is
   * initiated to open and initialize the database.  This throws an exception
   * if the opening of the connection or initialization fails.
   * <p>
   * Following this, the existing connection for sessionQualifier is
   * returned with a +1 reference count or, if there is no existing
   * connection, a new connection is opened, inserted into the connection-map
   * and returned with a +1 reference count.  This throws an exception
   * if the connection could not be opened.
   *
   * @param appName
   * @param sessionQualifier
   * @param context
   * @return
   * @throws SQLiteCantOpenDatabaseException
   */
  private OdkConnectionInterface getConnectionImpl(final String appName,
                                                   String sessionQualifier, Context context) {

    if (sessionQualifier == null) {
      throw new IllegalArgumentException("session qualifier cannot be null");
    }
    if (sessionQualifier.equals(appName)) {
      throw new IllegalArgumentException("session qualifier cannot be the same as the appName");
    }

    OdkConnectionInterface dbConnection = null;

    AppNameSharedStateContainer appNameSharedStateContainer = null;
    boolean hasBeenInitialized = true;

    // otherwise, create a connection for the sessionQualifier and return that
    appNameSharedStateContainer = appNameSharedStateMap.get(appName);
    if (appNameSharedStateContainer == null) {
      appNameSharedStateContainer = new AppNameSharedStateContainer(appName);
      appNameSharedStateMap.put(appName, appNameSharedStateContainer);
    } else {
      hasBeenInitialized = false;
    }

    logInfo(appName, "getConnectionImpl -- " + sessionQualifier +
            " -- creating new connection for " + appName + " when getting " + sessionQualifier);
    OdkConnectionInterface db = getNewConnectionImpl(appNameSharedStateContainer,
                sessionQualifier, hasBeenInitialized, context);

    if (db == null) {
      throw new SQLiteCantOpenDatabaseException("unable to initialize session database for "
                    + appName + " when getting " + sessionQualifier);
    } else {
      return db;
    }
  }

  @Override
  public OdkConnectionInterface getConnection(String appName,
                                              OdkDbHandle dbHandleName, Context context) {
    if (dbHandleName != null) {
      return getConnectionImpl(appName, dbHandleName.getDatabaseHandle(), context);
    } else {
      throw new IllegalArgumentException("null dbHandleName " + appName);

    }
  }

  @Override
  public void removeConnection(String appName, OdkDbHandle dbHandleName) {
    removeConnectionImpl(appName, dbHandleName.getDatabaseHandle());
  }

  /**
   * Remove the connection for the indicated sessionQualifier from the connection-map and
   * release the reference held on it by that map once we are outside of the map mutex.
   * <p>
   * This will generally trigger closure of the connection.
   *
   * @param appName
   * @param sessionQualifier
   */
  private void removeConnectionImpl(String appName, String sessionQualifier) {
    boolean releaseTwice = false;
    OdkConnectionInterface dbConnection = null;
    if (appName == null) {
      throw new IllegalArgumentException("appName cannot be null!");
    }
//    if (sessionQualifier == null) {
//      throw new IllegalArgumentException("sessionQualifier cannot be null!");
//    }
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
//      dbConnection = appNameSharedStateContainer.getExisting(sessionQualifier);
//      if (dbConnection != null) {
//        // no change in reference count
//        releaseTwice = appNameSharedStateContainer.moveIntoPendingDestruction(dbConnection);
//      }

    } catch (Exception e) {
      e.printStackTrace();
      logInfo(appName, "removeConnectionImpl -- database for " +
              appName);
    }
    finally {
//      if (dbConnection != null) {
//        // -1 for getExisting
//        dbConnection.releaseReference();
//        if (releaseTwice) {
//          // -1 for removal from sessionQualifierConnectionMap
//          dbConnection.releaseReference();
//        }
//      }
    }
  }

  @Override
  public OdkConnectionInterface getSessionGroupInstanceConnection(String appName,
                                                                  String sessionGroupQualifier,
                                                                  int instanceQualifier,
                                                                  Context context) {
    String sessionQualifier =
                sessionGroupQualifier + GROUP_TYPE_DIVIDER + Integer.toString(instanceQualifier);
    return getConnectionImpl(appName, sessionQualifier, context);
  }

  @Override
  public void removeSessionGroupInstanceConnection(String appName,
                                                     String sessionGroupQualifier,
                                                   int instanceQualifier) {
    String sessionQualifier = sessionGroupQualifier + GROUP_TYPE_DIVIDER +
            Integer.toString(instanceQualifier);
  }

  @Override
  public boolean removeSessionGroupConnections(String appName, String sessionGroupQualifier,
                                                 boolean removeNonMatchingGroupsOnly) {

    return true;
  }

  /**
   * Removes all database handles under the specified appName
   * that are created for the database service; i.e., have a
   * {OdkConnectionFactoryInterface.generateDatabaseServiceDbHandle()}
   *
   * @param appName
   * @return true if we removed something. false otherwise.
   */
  private boolean removeDatabaseServiceConnections(String appName) {
        return true;
    }

  /**
   * Remove all connections for the given appName
   *
   * @param appName
   * @return true if anything was removed.
   */
  private boolean removeAllConnections(String appName) {
        return true;
    }

  @Override
  public boolean removeAllSessionGroupConnections(String appName) {
    return removeSessionGroupConnections(appName, null, true);
  }

  @Override
  public boolean removeAllSessionGroupConnections() {
        return true;
    }

  @Override
  public boolean removeAllDatabaseServiceConnections() {
        return true;
    }

  @Override
  public void removeAllConnections() {
  }

  /**
   * Called when the database is created for the first time. This is where the
   * creation of tables and the initial population of the tables should happen.
   *
   * @param db The database.
   */
  protected void onCreate(OdkConnectionInterface db) {
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
   * @param db         The database.
   * @param oldVersion The old database version.
   * @param newVersion The new database version.
   */
  protected void onUpgrade(OdkConnectionInterface db, int oldVersion, int newVersion) {
    // for now, upgrade and creation use the same codepath...
    ODKDatabaseImplUtils.initializeDatabase(db);
  }
}
