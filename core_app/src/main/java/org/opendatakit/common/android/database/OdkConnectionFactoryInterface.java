package org.opendatakit.common.android.database;

// the Android Context is mocked on the desktop...
import android.content.Context;

import android.util.StringBuilderPrinter;
import org.opendatakit.common.android.utilities.ODKDataUtils;
import org.opendatakit.common.android.utilities.ODKDatabaseImplUtils;
import org.opendatakit.common.android.utilities.WebLogger;
import org.opendatakit.database.service.OdkDbHandle;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.WeakHashMap;

/**
 * Created by clarice on 9/14/15.
 */
public abstract class OdkConnectionFactoryInterface {

  /** used by subclasses to modify the appConnectionsMap */
  protected interface SessionQualifierToConnectionMapManipulator {
    /**
     * Insert the connection into the session map.
     * The connection already has referenceCount == 1
     * It should not be incremented inside this routine.
     *
     * @param sessionQualifier
     * @param dbConnection
     * @return  returns dbConnection if successful, existing connection if one already exists (race condition).
     */
    OdkConnectionInterface put(String sessionQualifier, OdkConnectionInterface dbConnection);

    /**
     * Remove the connection from the session map.
     * The connection referenceCount is not modified.
     *
     * @param sessionQualifier
     */
    void remove(String sessionQualifier);
  }

    public static final String GROUP_TYPE_DIVIDER = "--";
    public static final String INTERNAL_TYPE_SUFFIX = "-internal";

  /** object for guarding appNameMutexMap, appConnectionsMap, pendingDestruction */
  private final Object mutex = new Object();

  /**
   * Holds the mutex that should be used for all access to the given
   * appName's database.
   */
  private final HashMap<String,Object> appNameMutexMap = new HashMap<String, Object>();
  /**
   * This map MUST BE ACCESSED AND UPDATED after gaining the mutex.
   *
   * map of appName -TO-
   * map of sessionQualifier -TO- dabase handle
   *
   * The sessionQualifier enables different sections of code to isolate themselves
   * from the transactions that are in-progress in other sections of code.
   */
  private final Map<String, Map<String, OdkConnectionInterface>>
          appConnectionsMap = new HashMap<String, Map<String, OdkConnectionInterface>>();

  /**
   * holds connections that are in process of being destroyed.
   */
  private final WeakHashMap<OdkConnectionInterface, Long>
          pendingDestruction = new WeakHashMap<OdkConnectionInterface, Long>();

  protected abstract void logInfo(String appName, String message);

  protected abstract void logWarn(String appName, String message);

  protected abstract void logError(String appName, String message);

  protected abstract void printStackTrace(String appName, Throwable e);

  protected abstract OdkConnectionInterface getDbConnection(Context context, Object appNameMutex, String appName,
                                                            String sessionQualifier,
                                                            boolean shouldInitialize,
                                                            SessionQualifierToConnectionMapManipulator mapManipulator);

  public final void dumpInfo() {
    Map<String, StringBuilder> dumps = new HashMap<String, StringBuilder>();
    synchronized (mutex) {
      for (Map.Entry<String, Map<String, OdkConnectionInterface>>
              appNameToConnectionsMapEntry : appConnectionsMap.entrySet()) {
        String appName = appNameToConnectionsMapEntry.getKey();
        StringBuilder b = dumps.get(appName);
        if ( b == null ) {
          b = new StringBuilder();
          b.append("----------------" + appName + "-------------\n");
          dumps.put(appName, b);
        }
        Map<String, OdkConnectionInterface> sessionQualifierToConnectionMap = appNameToConnectionsMapEntry.getValue();
        for (String sessionQualifier : sessionQualifierToConnectionMap.keySet()) {
          OdkConnectionInterface dbConnection = sessionQualifierToConnectionMap.get(sessionQualifier);
          b.append("dumpInfo: refCount: " + dbConnection.getReferenceCount() + " appName " + appName + " sessionQualifier " + sessionQualifier + " lastAction " + dbConnection.getLastAction());
          b.append("\n");
          StringBuilder bpb = new StringBuilder();
          StringBuilderPrinter pb = new StringBuilderPrinter(bpb);
          dbConnection.dumpDetail(pb);
          b.append(bpb.toString());
          b.append("\n");
        }
      }

      for ( StringBuilder b : dumps.values()) {
        b.append("\n-----pendingDestruction------------\n");
      }

      for (WeakHashMap.Entry<OdkConnectionInterface, Long> dbconnectionPD : pendingDestruction.entrySet()) {
        OdkConnectionInterface dbKey = dbconnectionPD.getKey();

        if (dbKey != null) {
          String appName = dbKey.getAppName();
          StringBuilder b = dumps.get(appName);
          if ( b == null ) {
            b = new StringBuilder();
            dumps.put(appName, b);
          }

          String sessionQualifier = dbKey.getSessionQualifier();
          Long value = dbconnectionPD.getValue();

          b.append("dumpInfo: appName " + appName + " sessionQualifier " + sessionQualifier + " lastAction " + dbKey.getLastAction());
          b.append(" -- closed at " + value + "\n");
          StringBuilder bpb = new StringBuilder();
          StringBuilderPrinter pb = new StringBuilderPrinter(bpb);
          dbKey.dumpDetail(pb);
          b.append(bpb.toString());
          b.append("\n-------\n");
        }
      }

      for ( StringBuilder b : dumps.values()) {
        b.append("\n-----done------------\n");
      }
    }

    for ( String appName : dumps.keySet()) {
      logWarn(appName, dumps.get(appName).toString());
    }
  }

  /**
   * This handle is suitable for non-service uses.
   *
   * @return
   */
  public final OdkDbHandle generateInternalUseDbHandle() {
    return new OdkDbHandle(ODKDataUtils.genUUID() + AndroidConnectFactory.INTERNAL_TYPE_SUFFIX);
  }

  /**
   * This handle is suitable for database service use.
   *
   * @return
   */
  public final OdkDbHandle generateDatabaseServiceDbHandle() {
    return new OdkDbHandle(ODKDataUtils.genUUID());
  }

  public final OdkConnectionInterface getConnection(Context context, String appName, OdkDbHandle dbHandleName) {
    if (dbHandleName != null) {
      return getConnectionImpl(context, appName, dbHandleName.getDatabaseHandle());
    } else {
      throw new IllegalArgumentException("null sessionQualifier " + appName);
    }
  }

  private final class AndroidSessionQualifierToConnectionMapManipulator
          implements SessionQualifierToConnectionMapManipulator {

    private String appName;

    AndroidSessionQualifierToConnectionMapManipulator(String appName) {
      this.appName = appName;
    }

    @Override
    public OdkConnectionInterface put(String sessionQualifier, OdkConnectionInterface dbConnection) {
      if (sessionQualifier == null || dbConnection == null) {
        throw new IllegalArgumentException(
                "getConnectionInnerImpl: null arg to SessionQualifierToConnectionMapManipulator.add");
      }
      logInfo(appName,
              "getDbConnection -- " + sessionQualifier + " -- opening new session on database");

      for(;;) {
        OdkConnectionInterface dbConnectionExisting = null;
        synchronized (mutex) {
          Map<String, OdkConnectionInterface> dbConnectionMap = appConnectionsMap.get(appName);
          if (dbConnectionMap == null) {
            dbConnectionMap = new HashMap<String, OdkConnectionInterface>();
            appConnectionsMap.put(appName, dbConnectionMap);
          }
          dbConnectionExisting = dbConnectionMap.get(sessionQualifier);

          if (dbConnectionExisting == null) {
            dbConnectionMap.put(sessionQualifier, dbConnection);
            // this map now holds a reference
            dbConnection.acquireReference();
          } else {
            // signal that getDbConnection() should release reference (do not call remove)
            // +1 reference to retain this
            dbConnectionExisting.acquireReference();
          }
        }

        if (dbConnectionExisting == null ) {
          return dbConnection;
        }

        boolean outcome = dbConnectionExisting.waitForInitializationComplete();
        if ( outcome ) {
          synchronized (mutex) {
            // add the newly-created connection to the pending destruction list
            pendingDestruction.put(dbConnection, System.currentTimeMillis());
          }
          return dbConnectionExisting;
        } else {
          // release the existing connection (because init failed)
          dbConnectionExisting.releaseReference();
          // give another thread time to process connection
          try {
            Thread.sleep(50L);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          // and loop, trying to insert our connection
        }
      }
    }

    @Override
    public void remove(String sessionQualifier) {
      if ( sessionQualifier == null ) {
        throw new IllegalArgumentException(
                "getConnectionInnerImpl: null arg to SessionQualifierToConnectionMapManipulator.remove");
      }
      logInfo(appName,
              "getDbConnection -- " + sessionQualifier + " -- removing session on database");
      synchronized (mutex) {
        Map<String, OdkConnectionInterface> dbConnectionMap = appConnectionsMap.get(appName);
        if (dbConnectionMap == null) {
          return;
        }
        OdkConnectionInterface dbConnectionExisting = dbConnectionMap.get(sessionQualifier);
        if ( dbConnectionExisting != null ) {
          // add the removed connection to the pending destruction list
          pendingDestruction.put(dbConnectionExisting, System.currentTimeMillis());
          dbConnectionMap.remove(sessionQualifier);
        }
      }
    }
  };

  private final OdkConnectionInterface getConnectionImpl(Context context, final String appName, String sessionQualifier) {

    if (sessionQualifier == null) {
      throw new IllegalArgumentException("session qualifier cannot be null");
    }
    if (sessionQualifier.equals(appName)) {
      throw new IllegalArgumentException("session qualifier cannot be the same as the appName");
    }

    OdkConnectionInterface dbConnectionAppName = null;
    OdkConnectionInterface dbConnection = null;
    Object appNameMutex = null;

    synchronized (mutex) {
      appNameMutex = appNameMutexMap.get(appName);
      if ( appNameMutex == null ) {
        appNameMutex = new Object();
        appNameMutexMap.put(appName, appNameMutex);
      }
      Map<String, OdkConnectionInterface> dbConnectionMap = appConnectionsMap.get(appName);
      if (dbConnectionMap == null) {
        dbConnectionMap = new HashMap<String, OdkConnectionInterface>();
        appConnectionsMap.put(appName, dbConnectionMap);
      }
      dbConnectionAppName = dbConnectionMap.get(appName);
      if ( dbConnectionAppName != null ) {
        dbConnectionAppName.acquireReference();
        logInfo(appName,
                "getDbConnection -- " + sessionQualifier + " -- obtaining reference to base database");
      }
      dbConnection = dbConnectionMap.get(sessionQualifier);

      if (dbConnection != null) {
        dbConnection.acquireReference();
        logInfo(appName,
                "getDbConnection -- " + sessionQualifier + " -- obtaining reference to already-open database");
      }
    }


    AndroidSessionQualifierToConnectionMapManipulator manipulator =
            new AndroidSessionQualifierToConnectionMapManipulator(appName);

    boolean hasBeenInitialized = false;
    if ( dbConnectionAppName != null ) {
      hasBeenInitialized = dbConnectionAppName.waitForInitializationComplete();
      dbConnectionAppName.releaseReference();
    }

    if ( !hasBeenInitialized && dbConnection == null ) {
      logInfo(appName,
              "getDbConnection -- " + sessionQualifier + " -- triggering initialization of base database");

      // the "appName" database qualifier  is created once and left open
      OdkConnectionInterface db = getDbConnection(context, appNameMutex, appName, appName, true,
              manipulator);
      // TODO: verify that we should release the reference here...
      db.releaseReference();
    }

    if ( dbConnection != null ) {
      logInfo(appName,
              "getDbConnection -- " + sessionQualifier + " -- returning this existing session");
      return dbConnection;
    }

    logInfo(appName,
            "getDbConnection -- " + sessionQualifier + " -- creating this session now");
    OdkConnectionInterface db = getDbConnection(context, appNameMutex, appName, sessionQualifier, false,
            manipulator);

    return db;
  }

  public final void releaseDatabase(Context context, String appName, OdkDbHandle dbHandleName) {
    releaseDatabaseImpl(context, appName, dbHandleName.getDatabaseHandle());
  }


  /**
   * Remove the indicated session from the session map and release the reference held on it
   * by that map once we are outside of the map mutex.
   *
   * This will generally trigger closure of the connection.
   *
   * @param context
   * @param appName
   * @param sessionQualifier
   */
  private final void releaseDatabaseImpl(Context context, String appName, String sessionQualifier) {
    OdkConnectionInterface dbConnection = null;
    try {
      synchronized (mutex) {
        Map<String, OdkConnectionInterface> sessionQualifierToConnectionMap = appConnectionsMap.get(appName);
        if (sessionQualifierToConnectionMap == null) {
          // no record of the appName
          return;
        }

        if (sessionQualifier == null) {
          throw new IllegalArgumentException("sessionQualifier cannot be null!");
        }

        if (sessionQualifier.equals(appName) ) {
          WebLogger.getLogger(appName).w("releaseDatabaseImpl", "releasing the base database handle");
        }

        dbConnection = sessionQualifierToConnectionMap.get(sessionQualifier);
        if (dbConnection != null) {
          // add the removed connection to the pending destruction list
          pendingDestruction.put(dbConnection, System.currentTimeMillis());
          sessionQualifierToConnectionMap.remove(sessionQualifier);
        }
      }
    } finally {
      // after the dbConnection has been removed from map, we can release it
      if (dbConnection != null) {
        // release the reference count
        dbConnection.releaseReference();
      }
    }
  }

  public final OdkConnectionInterface getDatabaseGroupInstance(Context context, String appName,
                                                         String sessionGroupQualifier, int instanceQualifier) {
    String sessionQualifier = sessionGroupQualifier + GROUP_TYPE_DIVIDER + Integer.toString(instanceQualifier);
    return getConnectionImpl(context, appName, sessionQualifier);
  }

  public final void releaseDatabaseGroupInstance(Context context, String appName,
                                           String sessionGroupQualifier, int instanceQualifier) {
    String sessionQualifier = sessionGroupQualifier + GROUP_TYPE_DIVIDER + Integer.toString(instanceQualifier);
    releaseDatabaseImpl(context, appName, sessionQualifier);
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
  public final boolean releaseDatabaseGroupInstances(Context context, String appName,
                                               String sessionGroupQualifier, boolean releaseNonMatchingGroupsOnly) {
    HashSet<String> sessionQualifiers = new HashSet<String>();
    synchronized (mutex) {
      Map<String, OdkConnectionInterface> sessionQualifierToConnectionMap = appConnectionsMap.get(appName);
      if (sessionQualifierToConnectionMap == null) {
        // no record of the appName
        return false;
      }
      for (String sessionQualifier : sessionQualifierToConnectionMap.keySet()) {
        int idx = sessionQualifier.lastIndexOf(GROUP_TYPE_DIVIDER);
        if (idx != -1) {
          String sessionGroup = sessionQualifier.substring(0, idx);
          if (releaseNonMatchingGroupsOnly) {
            if (!sessionGroup.equals(sessionGroupQualifier)) {
              sessionQualifiers.add(sessionQualifier);
            }
          } else {
            if (sessionGroup.equals(sessionGroupQualifier)) {
              sessionQualifiers.add(sessionQualifier);
            }
          }
        }
      }
    }

    for (String sessionQualifier : sessionQualifiers) {
      logInfo(appName, "releaseDatabaseGroupInstances " + sessionQualifier);
      try {
        releaseDatabaseImpl(context, appName, sessionQualifier);
      } catch ( Exception e ) {
        logError( appName, "releaseDatabaseGroupInstances when releasing " + sessionQualifier);
        printStackTrace(appName, e);
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
  private final boolean releaseDatabaseNonGroupNonInternalInstances(Context context, String appName) {
    HashSet<String> sessionQualifiers = new HashSet<String>();
    synchronized (mutex) {
      Map<String, OdkConnectionInterface> sessionQualifierToConnectionMap = appConnectionsMap.get(appName);
      if (sessionQualifierToConnectionMap == null) {
        // no record of the appName
        return false;
      }
      for (String sessionQualifier : sessionQualifierToConnectionMap.keySet()) {
        int idx = sessionQualifier.lastIndexOf(GROUP_TYPE_DIVIDER);
        if (idx == -1 && !sessionQualifier.endsWith(INTERNAL_TYPE_SUFFIX)) {
          sessionQualifiers.add(sessionQualifier);
        }
      }
    }

    for (String sessionQualifier : sessionQualifiers) {
      logInfo(appName, "releaseDatabaseNonGroupNonInternalInstances " + sessionQualifier);
      try {
        releaseDatabaseImpl(context, appName, sessionQualifier);
      } catch ( Exception e ) {
        logError( appName, "releaseDatabaseNonGroupNonInternalInstances when releasing " + sessionQualifier);
        printStackTrace(appName, e);
      }
    }
    return !sessionQualifiers.isEmpty();
  }

  public final boolean releaseDatabaseInstances(Context context, String appName) {
    HashSet<String> sessionQualifiers = new HashSet<String>();
    synchronized (mutex) {
      Map<String, OdkConnectionInterface> sessionQualifierToConnectionMap = appConnectionsMap.get(appName);
      if (sessionQualifierToConnectionMap == null) {
        // no record of appName
        return false;
      }
      sessionQualifiers.addAll(sessionQualifierToConnectionMap.keySet());
    }

    for (String sessionQualifier : sessionQualifiers) {
      logInfo(appName, "releaseDatabaseInstances " + sessionQualifier);
      try {
        releaseDatabaseImpl(context, appName, sessionQualifier);
      } catch ( Exception e ) {
        logError( appName, "releaseDatabaseInstances when releasing " + sessionQualifier);
        printStackTrace(appName, e);
      }
    }
    return !sessionQualifiers.isEmpty();
  }

  public final boolean releaseAllDatabaseGroupInstances(Context context) {
    HashSet<String> appNames = new HashSet<String>();
    synchronized (mutex) {
      for (Map.Entry<String, Map<String, OdkConnectionInterface>> appNameToConnectionsMapEntry : appConnectionsMap.entrySet()) {
        String appName = appNameToConnectionsMapEntry.getKey();
        Map<String, OdkConnectionInterface> sessionQualifierToConnectionMap = appNameToConnectionsMapEntry.getValue();
        for (String sessionQualifier : sessionQualifierToConnectionMap.keySet()) {
          int idx = sessionQualifier.lastIndexOf(GROUP_TYPE_DIVIDER);
          if (idx != -1) {
            appNames.add(appName);
            break;
          }
        }
      }
    }
    for ( String appName : appNames ) {
      releaseDatabaseGroupInstances(context, appName, null, true);
    }
    return !appNames.isEmpty();
  }

  public final boolean releaseAllDatabaseNonGroupNonInternalInstances(Context context) {
    HashSet<String> appNames = new HashSet<String>();
    synchronized (mutex) {
      for (Map.Entry<String, Map<String, OdkConnectionInterface>> appNameToConnectionsMapEntry : appConnectionsMap.entrySet()) {
        String appName = appNameToConnectionsMapEntry.getKey();
        Map<String, OdkConnectionInterface> sessionQualifierToConnectionMap = appNameToConnectionsMapEntry.getValue();
        for (String sessionQualifier : sessionQualifierToConnectionMap.keySet()) {
          int idx = sessionQualifier.lastIndexOf(GROUP_TYPE_DIVIDER);
          if (idx == -1 && !sessionQualifier.endsWith(INTERNAL_TYPE_SUFFIX)) {
            appNames.add(appName);
            break;
          }
        }
      }
    }
    for ( String appName : appNames ) {
      releaseDatabaseNonGroupNonInternalInstances(context, appName);
    }
    return !appNames.isEmpty();
  }

  public final void releaseAllDatabases(Context context) {
    HashSet<String> appNames = new HashSet<String>();
    synchronized (mutex) {
      appNames.addAll(appConnectionsMap.keySet());
    }
    for ( String appName : appNames ) {
      releaseDatabaseInstances(context, appName);
    }
  }

  /**
   * Called when the database is created for the first time. This is where the
   * creation of tables and the initial population of the tables should happen.
   *
   * @param db
   *          The database.
   */
  protected final void onCreate(OdkConnectionInterface db) {
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
  protected final void onUpgrade(OdkConnectionInterface db, int oldVersion, int newVersion) {
    // for now, upgrade and creation use the same codepath...
    ODKDatabaseImplUtils.initializeDatabase(db);
  }

}
