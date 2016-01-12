/*
 * org_sqlite_database_sqlite_SQLiteCommon.h
 *
 *  Created on: Jan 8, 2016
 *      Author: Admin
 */

#ifndef ORG_SQLITE_DATABASE_SQLITE_SQLITECOMMON_H_
#define ORG_SQLITE_DATABASE_SQLITE_SQLITECOMMON_H_

// Special log tags defined in SQLiteDebug.java.
#define SQLITE_LOG_TAG "SQLiteLog"
#define SQLITE_TRACE_TAG "SQLiteStatements"
#define SQLITE_PROFILE_TAG "SQLiteTime"

#include "jni.h"
#include <string>
#include "ALog-priv.h"
#include "sqlite3.h"
#include "ScopedLocalRef.h"

namespace org_opendatakit {

/* Busy timeout in milliseconds.
 * If another connection (possibly in another process) has the database locked for
 * longer than this amount of time then SQLite will generate a SQLITE_BUSY error.
 * The SQLITE_BUSY error is then raised as a SQLiteDatabaseLockedException.
 *
 * In ordinary usage, busy timeouts are quite rare.  Most databases only ever
 * have a single open connection at a time unless they are using WAL.  When using
 * WAL, a timeout could occur if one connection is busy performing an auto-checkpoint
 * operation.  The busy timeout needs to be long enough to tolerate slow I/O write
 * operations but not so long as to cause the application to hang indefinitely if
 * there is a problem acquiring a database lock.
 */
const int BUSY_TIMEOUT_MS = 2500;

// Limit heap to 8MB for now.  This is 4 times the maximum cursor window
// size, as has been used by the original code in SQLiteDatabase for
// a long time.
const int SOFT_HEAP_LIMIT = 8 * 1024 * 1024;

struct SQLiteConnection {

    sqlite3* db;
    const int openFlags;
    std::string path;
    std::string label;

    volatile bool canceled;

    SQLiteConnection(sqlite3* db, int openFlags, const std::string& path, const std::string& label) :
        db(db), openFlags(openFlags), path(path), label(label), canceled(false) { }
};

typedef struct SQLiteConnection SQLiteConnection;

/*
** Note: The following symbols must be in the same order as the corresponding
** elements in the aMethod[] array in function nativeExecuteForCursorWindow().
*/
enum CWMethodNames {
  CW_CLEAR         = 0,
  CW_SETNUMCOLUMNS = 1,
  CW_ALLOCROW      = 2,
  CW_FREELASTROW   = 3,
  CW_PUTNULL       = 4,
  CW_PUTLONG       = 5,
  CW_PUTDOUBLE     = 6,
  CW_PUTSTRING     = 7,
  CW_PUTBLOB       = 8
};

/*
** An instance of this structure represents a single CursorWindow java method.
*/
struct CWMethod {
  jmethodID id;                   /* Method id */
  const char *zName;              /* Method name */
  const char *zSig;               /* Method JNI signature */
};

int jniThrowExceptionFmt(JNIEnv* env, const char* className, const char* fmt, ...);

void throw_sqlite3_exception(JNIEnv* env, int errcode,
                             const char* sqlite3Message, const char* message);
/* throw a SQLiteException with a message appropriate for the error in handle
   concatenated with the given message
 */
void throw_sqlite3_exception_db(JNIEnv* env, sqlite3* handle, const char* message);

/* throw a SQLiteException for a given error code
 * should only be used when the database connection is not available because the
 * error information will not be quite as rich */
void throw_sqlite3_exception_errcode(JNIEnv* env, int errcode, const char* message);

/* throw a SQLiteException with a message appropriate for the error in handle */
void throw_sqlite3_exception_db_unspecified(JNIEnv* env, sqlite3* handle);

void sqliteLogCallback(void* data, int iErrCode, const char* zMsg);

void sqliteInitialize(JNIEnv* env);

void sqliteTraceCallback(void *data, const char *sql);

void sqliteProfileCallback(void *data, const char *sql, sqlite3_uint64 tm);

int sqliteProgressHandlerCallback(void* data);

int coll_localized(
  void *not_used,
  int nKey1, const void *pKey1,
  int nKey2, const void *pKey2
);

void sqliteCustomFunctionCallback(sqlite3_context *context,
        int argc, sqlite3_value **argv);

void sqliteCustomFunctionDestructor(void* data);

int executeNonQuery(JNIEnv* env, SQLiteConnection* connection, sqlite3_stmt* statement);

int executeOneRowQuery(JNIEnv* env, SQLiteConnection* connection, sqlite3_stmt* statement);

int createAshmemRegionWithData(JNIEnv* env, const void* data, size_t length);

jboolean copyRowToWindow(
  JNIEnv *pEnv,
  jobject win,
  int iRow,
  sqlite3_stmt *pStmt,
  CWMethod *aMethod);

jboolean setWindowNumColumns(
  JNIEnv *pEnv,
  jobject win,
  sqlite3_stmt *pStmt,
  CWMethod *aMethod);

}

#endif /* ORG_SQLITE_DATABASE_SQLITE_SQLITECOMMON_H_ */

