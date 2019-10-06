package org.sqlite.database.sqlite;

import android.database.Cursor;
import android.os.CancellationSignal;

import org.opendatakit.logging.WebLoggerIf;
import org.sqlite.database.SQLException;

import java.util.Map;

public abstract class SQLiteConnectionBase extends SQLiteClosable {


   /**
    * Transaction mode: Deferred.
    * <p>
    * In a deferred transaction, no locks are acquired on the database
    * until the first operation is performed.  If the first operation is
    * read-only, then a <code>SHARED</code> lock is acquired, otherwise
    * a <code>RESERVED</code> lock is acquired.
    * </p><p>
    * While holding a <code>SHARED</code> lock, this session is only allowed to
    * read but other sessions are allowed to read or write.
    * While holding a <code>RESERVED</code> lock, this session is allowed to read
    * or write but other sessions are only allowed to read.
    * </p><p>
    * Because the lock is only acquired when needed in a deferred transaction,
    * it is possible for another session to write to the database first before
    * this session has a chance to do anything.
    * </p><p>
    * Corresponds to the SQLite <code>BEGIN DEFERRED</code> transaction mode.
    * </p>
    */
   public static final int TRANSACTION_MODE_DEFERRED = 0;

   /**
    * Transaction mode: Immediate.
    * <p>
    * When an immediate transaction begins, the session acquires a
    * <code>RESERVED</code> lock.
    * </p><p>
    * While holding a <code>RESERVED</code> lock, this session is allowed to read
    * or write but other sessions are only allowed to read.
    * </p><p>
    * Corresponds to the SQLite <code>BEGIN IMMEDIATE</code> transaction mode.
    * </p>
    */
   public static final int TRANSACTION_MODE_IMMEDIATE = 1;

   /**
    * Transaction mode: Exclusive.
    * <p>
    * When an exclusive transaction begins, the session acquires an
    * <code>EXCLUSIVE</code> lock.
    * </p><p>
    * While holding an <code>EXCLUSIVE</code> lock, this session is allowed to read
    * or write but no other sessions are allowed to access the database.
    * </p><p>
    * Corresponds to the SQLite <code>BEGIN EXCLUSIVE</code> transaction mode.
    * </p>
    */
   public static final int TRANSACTION_MODE_EXCLUSIVE = 2;

   /**
    * When a constraint violation occurs, an immediate ROLLBACK occurs,
    * thus ending the current transaction, and the command aborts with a
    * return code of SQLITE_CONSTRAINT. If no transaction is active
    * (other than the implied transaction that is created on every command)
    * then this algorithm works the same as ABORT.
    */
   public static final int CONFLICT_ROLLBACK = 1;

   /**
    * When a constraint violation occurs,no ROLLBACK is executed
    * so changes from prior commands within the same transaction
    * are preserved. This is the default behavior.
    */
   public static final int CONFLICT_ABORT = 2;

   /**
    * When a constraint violation occurs, the command aborts with a return
    * code SQLITE_CONSTRAINT. But any changes to the database that
    * the command made prior to encountering the constraint violation
    * are preserved and are not backed out.
    */
   public static final int CONFLICT_FAIL = 3;

   /**
    * When a constraint violation occurs, the one row that contains
    * the constraint violation is not inserted or changed.
    * But the command continues executing normally. Other rows before and
    * after the row that contained the constraint violation continue to be
    * inserted or updated normally. No error is returned.
    */
   public static final int CONFLICT_IGNORE = 4;

   /**
    * When a UNIQUE constraint violation occurs, the pre-existing rows that
    * are causing the constraint violation are removed prior to inserting
    * or updating the current row. Thus the insert or update always occurs.
    * The command continues executing normally. No error is returned.
    * If a NOT NULL constraint violation occurs, the NULL value is replaced
    * by the default value for that column. If the column has no default
    * value, then the ABORT algorithm is used. If a CHECK constraint
    * violation occurs then the IGNORE algorithm is used. When this conflict
    * resolution strategy deletes rows in order to satisfy a constraint,
    * it does not invoke delete triggers on those rows.
    * This behavior might change in a future release.
    */
   public static final int CONFLICT_REPLACE = 5;

   /**
    * Use the following when no conflict action is specified.
    */
   public static final int CONFLICT_NONE = 0;

   /**
    * Maximum Length Of A LIKE Or GLOB Pattern
    * The pattern matching algorithm used in the default LIKE and GLOB implementation
    * of SQLite can exhibit O(N^2) performance (where N is the number of characters in
    * the pattern) for certain pathological cases. To avoid denial-of-service attacks
    * the length of the LIKE or GLOB pattern is limited to SQLITE_MAX_LIKE_PATTERN_LENGTH bytes.
    * The default value of this limit is 50000. A modern workstation can evaluate
    * even a pathological LIKE or GLOB pattern of 50000 bytes relatively quickly.
    * The denial of service problem only comes into play when the pattern length gets
    * into millions of bytes. Nevertheless, since most useful LIKE or GLOB patterns
    * are at most a few dozen bytes in length, paranoid application developers may
    * want to reduce this parameter to something in the range of a few hundred
    * if they know that external users are able to generate arbitrary patterns.
    */
   public static final int SQLITE_MAX_LIKE_PATTERN_LENGTH = 50000;

   /**
    * Open flag: Flag for {@link SQLiteDatabaseConfiguration} to open the database for reading and writing.
    * If the disk is full, this may fail even before you actually write anything.
    *
    * {@more} Note that the value of this flag is 0, so it is the default.
    */
   public static final int OPEN_READWRITE = 0x00000000;          // update native code if changing

   /**
    * Open flag: Flag for {@link #openDatabase} to open the database for reading only.
    * This is the only reliable way to open a database if the disk may be full.
    */
   // public static final int OPEN_READONLY = 0x00000001;           // update native code if changing

   // private static final int OPEN_READ_MASK = 0x00000001;         // update native code if changing

   /**
    * Open flag: Flag for {@link SQLiteDatabaseConfiguration} to open the database without support for
    * localized collators.
    *
    * {@more} This causes the collator <code>LOCALIZED</code> not to be created.
    * You must be consistent when using this flag to use the setting the database was
    * created with.  If this is set, setLocale will do nothing.
    */
   public static final int NO_LOCALIZED_COLLATORS = 0x00000010;  // update native code if changing

   /**
    * Open flag: Flag for {@link SQLiteDatabaseConfiguration} to create the database file if it does not
    * already exist.
    */
   public static final int CREATE_IF_NECESSARY = 0x10000000;     // update native code if changing

   /**
    * Open flag: Flag for {@link SQLiteDatabaseConfiguration} to open the database file with
    * write-ahead logging enabled by default.
    */
   public static final int ENABLE_WRITE_AHEAD_LOGGING = 0x20000000;

   public abstract String getAppName();

   public abstract WebLoggerIf getLogger();

   public abstract void open();

   public abstract boolean isOpen();

   public abstract int getVersion();

   public abstract void setVersion(int version);

   public abstract void beginTransaction(int transactionMode,
                                         CancellationSignal cancellationSignal);

   public abstract boolean inTransaction();

   public abstract void setTransactionSuccessful();

   public abstract void endTransaction();

   public abstract int update(String table, Map<String,Object> values, String whereClause, Object[] whereArgs);

   public abstract int delete(String table, String whereClause, Object[] whereArgs);

   public abstract void replaceOrThrow(String table, String nullColumnHack,
                                       Map<String,Object> initialValues) throws SQLException;

   public abstract void insertOrThrow(String table, String nullColumnHack, Map<String,Object> values)
       throws SQLException;

   public abstract void execSQL(String sql, Object[] bindArgs) throws SQLException;

   public abstract Cursor rawQuery(String sql, Object[] selectionArgs, CancellationSignal cancellationSignal);

   public abstract Cursor query(String table, String[] columns, String selection,
                                Object[] selectionArgs, String groupBy, String having,
                                String orderBy, String limit);

   public abstract Cursor query(boolean distinct, String table, String[] columns,
                                String selection, Object[] selectionArgs, String groupBy,
                                String having, String orderBy, String limit, CancellationSignal cancellationSignal);

   public abstract void dump(StringBuilder b, boolean verbose);

}
