package org.opendatakit.common.android.database;

import android.content.ContentValues;
import android.database.Cursor;
import org.sqlite.database.SQLException;

/**
 * Created by clarice on 9/14/15.
 */
public interface OdkConnectionInterface {

    // This should be static!!
    // Not allowed in Java
    //public OdkConnectionInterface openDatabase(String appName, String dbFilePath, String sessionQualifier);

    public String getLastAction();

    public void acquireReference();

    public void releaseReference();

    public boolean isWriteAheadLoggingEnabled();

    public boolean enableWriteAheadLogging();

    public int getVersion();

    public void setVersion(int version);

    public boolean isOpen();

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

    // Added after taking out DataModelDatabaseHelper
    public void releaseConnection();
}
