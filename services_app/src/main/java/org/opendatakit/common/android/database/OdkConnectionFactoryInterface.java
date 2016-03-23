package org.opendatakit.common.android.database;

import org.opendatakit.database.service.OdkDbHandle;

/**
 * Factory interface that provides database connections and manages them.
 * Implementors should derive from OdkConnectionFactoryAbstractClass
 * which implements all of this interface.
 *
 * @author clarlars@gmail.com
 * @author mitchellsundt@gmail.com
 */
public interface OdkConnectionFactoryInterface {

   /**
    * A suffix that identifies a sessionQualifier as having been
    * created for internal use (e.g., for content providers).
    */
   static final String INTERNAL_TYPE_SUFFIX = "-internal";

   /**
    * the divider used to construct the session qualifier from
    * a session group qualifier and an instance qualifier.
    * This is currently only used through the DbShim interface,
    * and will presumably go away once that interface is removed.
    * @deprecated will be removed when DbShim interface goes away
    */
   static final String GROUP_TYPE_DIVIDER = "--";

   /**
    * Generate a database handle (a wrapper for a session qualifier)
    * that is suitable for non-service uses. e.g., content providers.
    *
    * These are not impacted by calls to
    * {removeAllDatabaseServiceConnections()}
    *
    * @return sessionQualifier appropriate for 'internal uses'
    */
   public OdkDbHandle generateInternalUseDbHandle();

   /**
    * This handle is suitable for database service use.
    *
    * @return sessionQualifier appropriate for 'database service uses'
    */
   public OdkDbHandle generateDatabaseServiceDbHandle();

   /**
    * Dump the state and history of the database layer.
    * Useful for debugging and understanding
    * cross-thread interactions.
    *
    * @param asError true if it should be logged as an error
    */
   public void dumpInfo(boolean asError);

   /**
    * Get a connection to the database for the given dbHandleName.
    * If no connection exists, this will create a new connection for that dbHandleName
    * and insert it into the active-connection map (so that it can be retrieved later).
    * The connection has a +1 reference count upon return.
    * <p>
    * Callers are responsible for calling {@link OdkConnectionInterface#releaseReference()}
    * </p>
    *
    * @param appName
    * @param dbHandleName
    * @return
    */
   public OdkConnectionInterface getConnection(String appName, OdkDbHandle dbHandleName);

   /**
    * Remove the connection to the database for the given dbHandleName from the
    * active-connection map. The database connection may still be held open by
    * an open cursor or other action that has incremented the connection's reference
    * count, but it is no longer retrievable via {@link this.getConnection(String, OdkDbHandle)}
    * after this call. Once all outstanding cursors are closed (or become GC'd), the
    * connection will be closed.
    *
    * @param appName
    * @param dbHandleName
    */
   public void removeConnection(String appName, OdkDbHandle dbHandleName);

   /**
    * Remove all database connections having {generateDatabaseServiceDbHandle()} session
    * qualifiers. See {removeConnection(String, OdkDbHandle)}
    *
    * @return true if anything was removed
    */
   public boolean removeAllDatabaseServiceConnections();

   /**
    * Remove all open database connections.
    * See {removeConnection(String, OdkDbHandle)}
    */
   public void removeAllConnections();

   /**
    * Retrieve (or open) the connection for the given sessionGroupQualifier and instanceQualifier.
    *
    * @param appName
    * @param sessionGroupQualifier
    * @param instanceQualifier
    * @return
    * @deprecated will be removed when DbShim interface goes away
    */
   public OdkConnectionInterface getSessionGroupInstanceConnection(String appName,
       String sessionGroupQualifier, int instanceQualifier);

   /**
    * Remove the connection for the given sessionGroupQualifier and instanceQualifier
    *
    * @param appName
    * @param sessionGroupQualifier
    * @param instanceQualifier
    * @deprecated will be removed when DbShim interface goes away
    */
   public void removeSessionGroupInstanceConnection(String appName, String sessionGroupQualifier,
       int instanceQualifier);

   /**
    * Remove any database handles for groups that don't match or that do match the indicated
    * session group qualifier (depending upon the value of removeNonMatchingGroupsOnly).
    * <p/>
    * Database sessions are group sessions if they contain {GROUP_TYPE_DIVIDER}.
    * Everything up to the last occurrence of {GROUP_TYPE_DIVIDER} in the
    * sessionQualifier is considered the session group qualifier.
    *
    * @param appName
    * @param sessionGroupQualifier may be null
    * @param removeNonMatchingGroupsOnly true if only removing non-matching
    *                                     sessionGroupQualifiers.
    *                                     false if only removing matching sessionGroupQualifiers.
    * @return true if anything was removed. false otherwise.
    * @deprecated will be removed when DbShim interface goes away
    */
   public boolean removeSessionGroupConnections(String appName, String sessionGroupQualifier,
       boolean removeNonMatchingGroupsOnly);

   /**
    * Remove all session group connections for the given appName.
    *
    * @param appName
    * @return true if we removed something. false otherwise.
    * @deprecated will be removed when DbShim interface goes away
    */
   public boolean removeAllSessionGroupConnections(String appName);

   /**
    * Remove all session group connections across all appNames.
    *
    * @return true if we removed something. false otherwise.
    * @deprecated will be removed when DbShim interface goes away
    */
   public boolean removeAllSessionGroupConnections();

}
