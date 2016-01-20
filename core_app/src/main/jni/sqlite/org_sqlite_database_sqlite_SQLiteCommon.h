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

#define nullptr 0
#include "jni.h"
#include <string>
#include "ALog-priv.h"
#include "sqlite3.h"
#include "ScopedLocalRef.h"

#define GET_METHOD_ID(var, clazz, methodName, fieldDescriptor) \
        var = env->GetMethodID(clazz, methodName, fieldDescriptor); \
        LOG_FATAL_IF((var == nullptr), "Unable to find method" methodName);

#define GET_FIELD_ID(var, clazz, fieldName, fieldDescriptor) \
        var = env->GetFieldID(clazz, fieldName, fieldDescriptor); \
        LOG_FATAL_IF((var == nullptr), "Unable to find field " fieldName);

namespace org_opendatakit {

typedef struct SQLiteConnection SQLiteConnection;

/* throw a SQLiteException with a message appropriate for the error in handle
   concatenated with the given message
 */
void throw_sqlite3_exception_db(JNIEnv* env, sqlite3* handle, const char* message);

void sqliteInitialize(JNIEnv* env);

SQLiteConnection* openConnection(JNIEnv* env,
          std::string path, jint openFlags, std::string label,
          jboolean enableTrace, jboolean enableProfile);

void closeConnection(JNIEnv* env, SQLiteConnection* connection);

void registerCustomFunction(JNIEnv* env, SQLiteConnection* connection,
        const char * name, int numArgs, jobject functionObj);

sqlite3_stmt* prepareStatement(JNIEnv* env, SQLiteConnection* connection, jstring sqlString);

void finalizeStatement(JNIEnv* env, SQLiteConnection* connection, sqlite3_stmt* statement);

jint bindParameterCount(JNIEnv* env, SQLiteConnection* connection, sqlite3_stmt* statment);

jboolean statementIsReadOnly(JNIEnv* env, SQLiteConnection* connection, sqlite3_stmt* statement);

jint getColumnCount(JNIEnv* env, SQLiteConnection* connection, sqlite3_stmt* statement);

jstring getColumnName(JNIEnv* env, SQLiteConnection* connection, sqlite3_stmt* statement,
      int index);

void bindNull(JNIEnv* env, SQLiteConnection* connection, sqlite3_stmt* statement, int index);

void bindLong(JNIEnv* env, SQLiteConnection* connection, sqlite3_stmt* statement, int index,
      jlong value);

void bindDouble(JNIEnv* env, SQLiteConnection* connection, sqlite3_stmt* statement, int index,
      jdouble value);

void bindString(JNIEnv* env, SQLiteConnection* connection, sqlite3_stmt* statement, int index,
      const jchar* value, size_t valueLength);

void bindBlob(JNIEnv* env, SQLiteConnection* connection, sqlite3_stmt* statement, int index,
      const jbyte* value, size_t valueLength);

void resetAndClearBindings(JNIEnv* env, SQLiteConnection* connection, sqlite3_stmt* statement);

// internal sqlite error code
int executeNonQuery(JNIEnv* env, SQLiteConnection* connection, sqlite3_stmt* statement);

jlong executeForLong(JNIEnv* env, SQLiteConnection* connection, sqlite3_stmt* statement);

jstring executeForString(JNIEnv* env, SQLiteConnection* connection, sqlite3_stmt* statement);

jint executeForChangedRowCount(JNIEnv* env, SQLiteConnection* connection, sqlite3_stmt* statement);

jlong executeForLastInsertedRowId(JNIEnv* env, SQLiteConnection* connection, sqlite3_stmt* statement);

int executeOneRowQuery(JNIEnv* env, SQLiteConnection* connection, sqlite3_stmt* statement);

jstring getColumnStringValue(JNIEnv* env, SQLiteConnection* connection, sqlite3_stmt* statement,
       int index);

jlong executeIntoCursorWindow(JNIEnv* env, SQLiteConnection* connection, sqlite3_stmt* statement,
                          jobject win,
                          jint startPos,                  /* First row to add (advisory) */
                          jint iRowRequired,              /* Required row */
                          jboolean countAllRows);

jint getDbLookasideUsed(JNIEnv* env, SQLiteConnection* connection);

void cancel(JNIEnv* env, SQLiteConnection* connection);

void resetCancel(JNIEnv* env, SQLiteConnection* connection, jboolean cancelable);

jboolean hasCodec(JNIEnv* env);

// used by SQLiteGlobal
jint releaseMemory();

// used by SQLiteDebug
void getStatus(JNIEnv* env, jint* memoryUsed, jint* largestMemAlloc, jint *pageCacheOverflow);

}

#endif /* ORG_SQLITE_DATABASE_SQLITE_SQLITECOMMON_H_ */

