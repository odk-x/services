package org.opendatakit.services.database;

import org.opendatakit.database.service.DbHandle;

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
   String INTERNAL_TYPE_SUFFIX = "-internal";

   /**
    * Generate a database handle (a wrapper for a session qualifier)
    * that is suitable for non-service uses. e.g., content providers.
    *
    * These are not impacted by calls to
    * {removeAllDatabaseServiceConnections()}
    *
    * @return sessionQualifier appropriate for 'internal uses'
    */
   DbHandle generateInternalUseDbHandle();

   /**
    * This handle is suitable for database service use.
    *
    * @return sessionQualifier appropriate for 'database service uses'
    */
   DbHandle generateDatabaseServiceDbHandle();

   /**
    * Dump the state and history of the database layer.
    * Useful for debugging and understanding
    * cross-thread interactions.
    *
    * @param asError true if it should be logged as an error
    */
   void dumpInfo(boolean asError);

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
   OdkConnectionInterface getConnection(String appName, DbHandle dbHandleName);

   /**
    * Remove the connection to the database for the given dbHandleName from the
    * active-connection map. The database connection may still be held open by
    * an open cursor or other action that has incremented the connection's reference
    * count, but it is no longer retrievable via {@link this.getConnection(String, DbHandle)}
    * after this call. Once all outstanding cursors are closed (or become GC'd), the
    * connection will be closed.
    *
    * @param appName
    * @param dbHandleName
    */
   void removeConnection(String appName, DbHandle dbHandleName);

   /**
    * Remove all database connections having {generateDatabaseServiceDbHandle()} session
    * qualifiers. See {removeConnection(String, DbHandle)}
    *
    * @return true if anything was removed
    */
   boolean removeAllDatabaseServiceConnections();

   /**
    * Remove all open database connections.
    * See {removeConnection(String, DbHandle)}
    */
   void removeAllConnections();
}
