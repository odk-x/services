package org.opendatakit.services.database;

import android.database.Cursor;

import org.sqlite.database.SQLException;

import java.util.Map;

/**
 *  @author clarlars@gmail.com
 *  @author mitchellsundt@gmail.com
 */
public interface OdkConnectionInterface {

    // This should be static!!
    // Not allowed in Java
    //public OdkConnectionInterface openDatabase(String appName, String dbFilePath, String sessionQualifier);

  /**
   * This should only be called for the main database initialization.
   *
   * @return true if initialization is successful.
   */
    boolean waitForInitializationComplete();


  /**
   * Signal that initialization is complete with the given outcome
   * @param outcome true if successful
   */
    void signalInitializationComplete(boolean outcome);

    int getReferenceCount();

    String getAppName();

    String getSessionQualifier();

    void dumpDetail(StringBuilder b);

   /**
    * This is called within
    * {OdkConnectionFactoryInterface.getConnection(String appName, DbHandle dbHandleName)}
    * before the connection is returned.
    *
    * Only call this if you need to store the connection in your own data structures.
    * In general, however, that is a bad idea. Just use the above function to retrieve
    * the connection each time you need it.
    */
    void acquireReference();

   /**
    * Call this when you no longer need to use the connection.
    * The connection remains open until it is removed from
    * the connection map via:
    * {OdkConnectionFactoryInterface.removeConnection(String appName, DbHandle dbHandleName)}
    *
    * @throws SQLException
    */
    void releaseReference() throws SQLException;

   /**
    * Get the schema version in the database.
    * This should only be called by the {OdkConnectionFactoryAbstractClass}.
    *
    * @return
    * @throws SQLException
    */
    int getVersion() throws SQLException;

   /**
    * Set the schema version in the database.
    * This should only be called by the {OdkConnectionFactoryAbstractClass}.
    *
    * @param version
    * @throws SQLException
    */
    void setVersion(int version) throws SQLException;

    boolean isOpen() throws SQLException;

  /**
   * close() is not implemented.
   *
   * To effect a close:
   * (1) call releaseReference() (because the referenceCount is +1 when you obtain this interface)
   * (2) then call:
   * {OdkConnectionFactoryInterface.removeConnection(String appName, DbHandle dbHandleName)}
   * or one of its variants. Those methods will ensure that this interface is removed from the
   * set of actively managed interfaces, causing its reference count to -1, and, if there are no
   * open cursors, then trigger a close. Otherwise, we wait for a GC cycle to detect an unreachable
   * connection and terminate it during the finalize() action (which should be considered a logic
   * error).
   */
    void close();

   /**
    * Take an immediate exclusive lock on the database.
    *
    * This should only be called by the {OdkConnectionFactoryAbstractClass}.
    *
    * It is used during database initialization and upgrade.
    *
    * @throws SQLException
    */
    void beginTransactionExclusive() throws SQLException;

   /**
    * The normal lock is a non-exclusive deferred lock that gains
    * a read lock and then only gains a write lock when a DDL or DML
    * action occurs.
    *
    * @throws SQLException
    */
    void beginTransactionNonExclusive() throws SQLException;

    boolean inTransaction() throws SQLException;

    void setTransactionSuccessful() throws SQLException;

    void endTransaction() throws SQLException;

    int update(String table, Map<String,Object> values, String whereClause, Object[] whereArgs) throws SQLException;

    int delete(String table, String whereClause, Object[] whereArgs) throws SQLException;

    void replaceOrThrow(String table, String nullColumnHack, Map<String,Object> initialValues)
            throws SQLException;

    void insertOrThrow(String table, String nullColumnHack, Map<String,Object> values)
            throws SQLException;

    void execSQL(String sql, Object[] bindArgs) throws SQLException;

    Cursor rawQuery(String sql, Object[] selectionArgs) throws SQLException;

    Cursor query(String table, String[] columns, String selection, Object[] selectionArgs,
                           String groupBy, String having, String orderBy, String limit) throws SQLException;

    Cursor queryDistinct(String table, String[] columns, String selection,
        Object[] selectionArgs, String groupBy, String having, String orderBy, String limit) throws SQLException;
}
