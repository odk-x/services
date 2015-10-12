package org.opendatakit.common.android.database;

import android.content.ContentValues;
import android.database.Cursor;
import android.util.Printer;
import org.sqlite.database.SQLException;

/**
 * Created by clarice on 9/14/15.
 */
public interface OdkConnectionInterface {

    // This should be static!!
    // Not allowed in Java
    //public OdkConnectionInterface openDatabase(String appName, String dbFilePath, String sessionQualifier);

    public String getLastAction();

  /**
   * This should only be called for the main database initialization.
   *
   * @return true if initialization is successful.
   */
    public boolean waitForInitializationComplete();


  /**
   * Signal that initialization is complete with the given outcome
   * @param outcome true if successful
   */
    public void signalInitializationComplete(boolean outcome);

    public int getReferenceCount();

    public String getAppName();

    public long getLastThreadId();

    public String getSessionQualifier();

    public void dumpDetail(Printer printer);

    public void acquireReference();

    public void releaseReference();

    public int getVersion();

    public void setVersion(int version);

    public boolean isOpen();

  /*
   * close() is not implemented. Instead, users should call:
   *
   * {@link OdkConnectionFactoryInterface.releaseDatabase(String appName, OdkDbHandle dbHandleName)}
   *
   * That method or one of its variants will ensure that this interface is removed from the set of active
   * interfaces managed by the connection factory.
   *
   * To effect a close:
   * (1) call releaseReference() (because the referenceCount is +1 when you obtain this interface)
   * (2) then call the above method or one of its variants.
   */
    public void close();

    public void beginTransactionNonExclusive();

    public boolean inTransaction();

    public void setTransactionSuccessful();

    public void endTransaction();

    public int update(String table, ContentValues values, String whereClause, String[] whereArgs);

    public int delete(String table, String whereClause, String[] whereArgs);

    public long replaceOrThrow(String table, String nullColumnHack, ContentValues initialValues)
            throws SQLException;

    public long insertOrThrow(String table, String nullColumnHack, ContentValues values)
            throws SQLException;

    public void execSQL(String sql, Object[] bindArgs) throws SQLException;

    public void execSQL(String sql) throws SQLException;

    public Cursor rawQuery(String sql, String[] selectionArgs);

    public Cursor query(String table, String[] columns, String selection, String[] selectionArgs,
                           String groupBy, String having, String orderBy, String limit);

    public Cursor queryDistinct(String table, String[] columns, String selection,
                                   String[] selectionArgs, String groupBy, String having, String orderBy, String limit);
}
