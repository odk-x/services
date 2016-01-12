/*
 * Copyright (C) 2011 The Android Open Source Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
** Modified to support SQLite extensions by the SQLite developers: 
** sqlite-dev@sqlite.org.
*/

#define LOG_TAG "SQLiteConnection"

#include <assert.h>

#include "org_sqlite_database_sqlite_SQLiteConnection.h"
#include "org_sqlite_database_sqlite_SQLiteCommon.h"


#define GET_FIELD_ID(var, clazz, fieldName, fieldDescriptor) \
        var = env->GetFieldID(clazz, fieldName, fieldDescriptor); \
        LOG_FATAL_IF(! var, "Unable to find field " fieldName);

using org_opendatakit::ScopedLocalRef;
using org_opendatakit::SQLiteConnection;
using org_opendatakit::CWMethod;
using org_opendatakit::jniThrowExceptionFmt;
using org_opendatakit::throw_sqlite3_exception_db;
using org_opendatakit::throw_sqlite3_exception_db_unspecified;
using org_opendatakit::throw_sqlite3_exception_errcode;
using org_opendatakit::coll_localized;
using org_opendatakit::BUSY_TIMEOUT_MS;
using org_opendatakit::sqliteInitialize;
using org_opendatakit::sqliteTraceCallback;
using org_opendatakit::sqliteProfileCallback;
using org_opendatakit::sqliteProgressHandlerCallback;
using org_opendatakit::sqliteCustomFunctionCallback;
using org_opendatakit::sqliteCustomFunctionDestructor;
using org_opendatakit::executeNonQuery;
using org_opendatakit::executeOneRowQuery;
using org_opendatakit::createAshmemRegionWithData;

/*
 * Class:     org_sqlite_database_sqlite_SQLiteConnection
 * Method:    nativeInit
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_org_sqlite_database_sqlite_SQLiteConnection_nativeInit
  (JNIEnv* env, jclass clazz) {
  sqliteInitialize(env);
}

/*
 * Class:     org_sqlite_database_sqlite_SQLiteConnection
 * Method:    nativeOpen
 * Signature: (Ljava/lang/String;ILjava/lang/String;ZZ)J
 */
JNIEXPORT jlong JNICALL Java_org_sqlite_database_sqlite_SQLiteConnection_nativeOpen
  (JNIEnv* env, jclass clazz, jstring pathStr, jint openFlags,
        jstring labelStr, jboolean enableTrace, jboolean enableProfile) {
    int sqliteFlags;
    if (openFlags & org_sqlite_database_sqlite_SQLiteConnection_CREATE_IF_NECESSARY) {
        sqliteFlags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
    } else {
        sqliteFlags = SQLITE_OPEN_READWRITE;
    }

    const char* pathChars = env->GetStringUTFChars(pathStr, nullptr);
    std::string path(pathChars);
    env->ReleaseStringUTFChars(pathStr, pathChars);

    const char* labelChars = env->GetStringUTFChars(labelStr, nullptr);
    std::string label(labelChars);
    env->ReleaseStringUTFChars(labelStr, labelChars);

    sqlite3* db;
    int err = sqlite3_open_v2(path.c_str(), &db, sqliteFlags, nullptr);
    if (err != SQLITE_OK) {
        throw_sqlite3_exception_errcode(env, err, "Could not open database");
        return 0L;
    }
    err = sqlite3_create_collation(db, "localized", SQLITE_UTF8, nullptr,
    		coll_localized);
    if (err != SQLITE_OK) {
        throw_sqlite3_exception_errcode(env, err, "Could not register collation");
        sqlite3_close_v2(db);
        return 0L;
    }

    // Check that the database is really read/write when that is what we asked for.
    if ((sqliteFlags & SQLITE_OPEN_READWRITE) && sqlite3_db_readonly(db, nullptr)) {
        throw_sqlite3_exception_db(env, db, "Could not open the database in read/write mode.");
        sqlite3_close_v2(db);
        return 0L;
    }

    // Set the default busy handler to retry automatically before returning SQLITE_BUSY.
    err = sqlite3_busy_timeout(db, BUSY_TIMEOUT_MS);
    if (err != SQLITE_OK) {
        throw_sqlite3_exception_db(env, db, "Could not set busy timeout");
        sqlite3_close_v2(db);
        return 0L;
    }

    // Register custom Android functions.
#if 0
    err = register_android_functions(db, UTF16_STORAGE);
    if (err) {
        throw_sqlite3_exception_db(env, db, "Could not register Android SQL functions.");
        sqlite3_close_v2(db);
        return 0L;
    }
#endif

    // Create wrapper object.
    SQLiteConnection* connection = new SQLiteConnection(db, openFlags, path, label);

    // Enable tracing and profiling if requested.
    if (enableTrace) {
        sqlite3_trace(db, &sqliteTraceCallback, connection);
    }
    if (enableProfile) {
        sqlite3_profile(db, &sqliteProfileCallback, connection);
    }

    ALOGV("Opened connection %p with label '%s'", db, label.c_str());
    return reinterpret_cast<jlong>(connection);
}

/*
 * Class:     org_sqlite_database_sqlite_SQLiteConnection
 * Method:    nativeClose
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_sqlite_database_sqlite_SQLiteConnection_nativeClose
  (JNIEnv* env, jclass clazz, jlong connectionPtr) {
    SQLiteConnection* connection = reinterpret_cast<SQLiteConnection*>(connectionPtr);

    if (connection) {
        ALOGV("Closing connection %p", connection->db);
        int err = sqlite3_close_v2(connection->db);
        if (err != SQLITE_OK) {
            // This can happen if sub-objects aren't closed first.  Make sure the caller knows.
            ALOGE("sqlite3_close_v2(%p) failed: %d", connection->db, err);
            throw_sqlite3_exception_db(env, connection->db, "Count not close db.");
            return;
        }

        delete connection;
    }
}

/*
 * Class:     org_sqlite_database_sqlite_SQLiteConnection
 * Method:    nativeRegisterCustomFunction
 * Signature: (JLorg/sqlite/database/sqlite/SQLiteCustomFunction;)V
 */
JNIEXPORT void JNICALL Java_org_sqlite_database_sqlite_SQLiteConnection_nativeRegisterCustomFunction
  (JNIEnv* env, jclass clazz, jlong connectionPtr, jobject functionObj) {
    SQLiteConnection* connection = reinterpret_cast<SQLiteConnection*>(connectionPtr);


    ScopedLocalRef<jclass> customFunctionClass(env, env->FindClass("org/sqlite/database/sqlite/SQLiteCustomFunction"));
    if (customFunctionClass.get() == nullptr) {
        // unable to locate the class -- silently exit
        ALOGE("Unable to find class org/sqlite/database/sqlite/SQLiteCustomFunction");
        return;
    }

    jfieldID f_name;
    GET_FIELD_ID(f_name, customFunctionClass.get(), "name", "Ljava/lang/String;");
    jfieldID f_numArgs;
    GET_FIELD_ID(f_numArgs, customFunctionClass.get(), "numArgs", "I");

    jstring nameStr = jstring(env->GetObjectField(functionObj, f_name));
    jint numArgs = env->GetIntField(functionObj, f_numArgs);

    // this is important -- the sqlite3_user_data() function will return functionObjGlobal
    // so it needs to be a global ref.
    jobject functionObjGlobal = env->NewGlobalRef(functionObj);

    const char* name = env->GetStringUTFChars(nameStr, nullptr);
    // this will copy the name, so we don't have to worry about localref reloc of the buffer.
    int err = sqlite3_create_function_v2(connection->db, name, numArgs, SQLITE_UTF16,
            reinterpret_cast<void*>(functionObjGlobal),
            &sqliteCustomFunctionCallback, nullptr, nullptr, &sqliteCustomFunctionDestructor);
    // and release the lock on name (nameStr), allowing it to reloc.
    env->ReleaseStringUTFChars(nameStr, name);

    if (err != SQLITE_OK) {
        ALOGE("sqlite3_create_function returned %d", err);
        env->DeleteGlobalRef(functionObjGlobal);
        throw_sqlite3_exception_db_unspecified(env, connection->db);
        return;
    }
}

/*
 * Class:     org_sqlite_database_sqlite_SQLiteConnection
 * Method:    nativeRegisterLocalizedCollators
 * Signature: (JLjava/lang/String;)V
 */
JNIEXPORT void JNICALL Java_org_sqlite_database_sqlite_SQLiteConnection_nativeRegisterLocalizedCollators
  (JNIEnv* env, jclass clazz, jlong connectionPtr, jstring localeStr) {
    SQLiteConnection* connection = reinterpret_cast<SQLiteConnection*>(connectionPtr);

    const char* locale = env->GetStringUTFChars(localeStr, nullptr);
    int err = SQLITE_OK;
#if 0
    err = register_localized_collators(connection->db, locale, UTF16_STORAGE);
#endif
    env->ReleaseStringUTFChars(localeStr, locale);

    if (err != SQLITE_OK) {
        throw_sqlite3_exception_db_unspecified(env, connection->db);
    }
}

/*
 * Class:     org_sqlite_database_sqlite_SQLiteConnection
 * Method:    nativePrepareStatement
 * Signature: (JLjava/lang/String;)J
 */
JNIEXPORT jlong JNICALL Java_org_sqlite_database_sqlite_SQLiteConnection_nativePrepareStatement
  (JNIEnv* env, jclass clazz, jlong connectionPtr, jstring sqlString) {
    SQLiteConnection* connection = reinterpret_cast<SQLiteConnection*>(connectionPtr);

    jsize sqlLength = env->GetStringLength(sqlString);
    const jchar* sql = env->GetStringChars(sqlString, nullptr);
    sqlite3_stmt* statement;
    int err = sqlite3_prepare16_v2(connection->db,
            sql, sqlLength * sizeof(jchar), &statement, nullptr);
    env->ReleaseStringChars(sqlString, sql);

    if (err != SQLITE_OK) {
        // Error messages like 'near ")": syntax error' are not
        // always helpful enough, so construct an error string that
        // includes the query itself.
        const char *query = env->GetStringUTFChars(sqlString, nullptr);
        char *message = (char*) malloc(strlen(query) + 50);
        if (message) {
            strcpy(message, ", while compiling: "); // less than 50 chars
            strcat(message, query);
        }
        env->ReleaseStringUTFChars(sqlString, query);
        throw_sqlite3_exception_db(env, connection->db, message);
        free(message);
        return 0L;
    }

    ALOGV("Prepared statement %p on connection %p", statement, connection->db);
    return reinterpret_cast<jlong>(statement);
}

/*
 * Class:     org_sqlite_database_sqlite_SQLiteConnection
 * Method:    nativeFinalizeStatement
 * Signature: (JJ)V
 */
JNIEXPORT void JNICALL Java_org_sqlite_database_sqlite_SQLiteConnection_nativeFinalizeStatement
  (JNIEnv* env, jclass clazz, jlong connectionPtr, jlong statementPtr) {
    SQLiteConnection* connection = reinterpret_cast<SQLiteConnection*>(connectionPtr);
    sqlite3_stmt* statement = reinterpret_cast<sqlite3_stmt*>(statementPtr);

    // We ignore the result of sqlite3_finalize because it is really telling us about
    // whether any errors occurred while executing the statement.  The statement itself
    // is always finalized regardless.
    ALOGV("Finalized statement %p on connection %p", statement, connection->db);
    sqlite3_finalize(statement);
}

/*
 * Class:     org_sqlite_database_sqlite_SQLiteConnection
 * Method:    nativeGetParameterCount
 * Signature: (JJ)I
 */
JNIEXPORT jint JNICALL Java_org_sqlite_database_sqlite_SQLiteConnection_nativeGetParameterCount
  (JNIEnv* env, jclass clazz, jlong connectionPtr, jlong statementPtr) {
    SQLiteConnection* connection = reinterpret_cast<SQLiteConnection*>(connectionPtr);
    sqlite3_stmt* statement = reinterpret_cast<sqlite3_stmt*>(statementPtr);

    return sqlite3_bind_parameter_count(statement);
}

/*
 * Class:     org_sqlite_database_sqlite_SQLiteConnection
 * Method:    nativeIsReadOnly
 * Signature: (JJ)Z
 */
JNIEXPORT jboolean JNICALL Java_org_sqlite_database_sqlite_SQLiteConnection_nativeIsReadOnly
  (JNIEnv* env, jclass clazz, jlong connectionPtr, jlong statementPtr) {
    SQLiteConnection* connection = reinterpret_cast<SQLiteConnection*>(connectionPtr);
    sqlite3_stmt* statement = reinterpret_cast<sqlite3_stmt*>(statementPtr);

    return sqlite3_stmt_readonly(statement) != 0;
}

/*
 * Class:     org_sqlite_database_sqlite_SQLiteConnection
 * Method:    nativeGetColumnCount
 * Signature: (JJ)I
 */
JNIEXPORT jint JNICALL Java_org_sqlite_database_sqlite_SQLiteConnection_nativeGetColumnCount
  (JNIEnv* env, jclass clazz, jlong connectionPtr, jlong statementPtr) {
    SQLiteConnection* connection = reinterpret_cast<SQLiteConnection*>(connectionPtr);
    sqlite3_stmt* statement = reinterpret_cast<sqlite3_stmt*>(statementPtr);

    return sqlite3_column_count(statement);
}

/*
 * Class:     org_sqlite_database_sqlite_SQLiteConnection
 * Method:    nativeGetColumnName
 * Signature: (JJI)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_org_sqlite_database_sqlite_SQLiteConnection_nativeGetColumnName
  (JNIEnv* env, jclass clazz, jlong connectionPtr,
        jlong statementPtr, jint index) {
    SQLiteConnection* connection = reinterpret_cast<SQLiteConnection*>(connectionPtr);
    sqlite3_stmt* statement = reinterpret_cast<sqlite3_stmt*>(statementPtr);

    const jchar* name = static_cast<const jchar*>(sqlite3_column_name16(statement, index));
    if (name) {
        size_t length = 0;
        while (name[length]) {
            length += 1;
        }
        return env->NewString(name, length);
    }
    return nullptr;
}

/*
 * Class:     org_sqlite_database_sqlite_SQLiteConnection
 * Method:    nativeBindNull
 * Signature: (JJI)V
 */
JNIEXPORT void JNICALL Java_org_sqlite_database_sqlite_SQLiteConnection_nativeBindNull
  (JNIEnv* env, jclass clazz, jlong connectionPtr, jlong statementPtr, jint index) {
    SQLiteConnection* connection = reinterpret_cast<SQLiteConnection*>(connectionPtr);
    sqlite3_stmt* statement = reinterpret_cast<sqlite3_stmt*>(statementPtr);

    int err = sqlite3_bind_null(statement, index);
    if (err != SQLITE_OK) {
        throw_sqlite3_exception_db_unspecified(env, connection->db);
    }
}

/*
 * Class:     org_sqlite_database_sqlite_SQLiteConnection
 * Method:    nativeBindLong
 * Signature: (JJIJ)V
 */
JNIEXPORT void JNICALL Java_org_sqlite_database_sqlite_SQLiteConnection_nativeBindLong
  (JNIEnv* env, jclass clazz, jlong connectionPtr, jlong statementPtr, jint index, jlong value) {
    SQLiteConnection* connection = reinterpret_cast<SQLiteConnection*>(connectionPtr);
    sqlite3_stmt* statement = reinterpret_cast<sqlite3_stmt*>(statementPtr);

    int err = sqlite3_bind_int64(statement, index, value);
    if (err != SQLITE_OK) {
        throw_sqlite3_exception_db_unspecified(env, connection->db);
    }
}

/*
 * Class:     org_sqlite_database_sqlite_SQLiteConnection
 * Method:    nativeBindDouble
 * Signature: (JJID)V
 */
JNIEXPORT void JNICALL Java_org_sqlite_database_sqlite_SQLiteConnection_nativeBindDouble
  (JNIEnv* env, jclass clazz, jlong connectionPtr, jlong statementPtr, jint index, jdouble value) {
    SQLiteConnection* connection = reinterpret_cast<SQLiteConnection*>(connectionPtr);
    sqlite3_stmt* statement = reinterpret_cast<sqlite3_stmt*>(statementPtr);

    int err = sqlite3_bind_double(statement, index, value);
    if (err != SQLITE_OK) {
        throw_sqlite3_exception_db_unspecified(env, connection->db);
    }
}

/*
 * Class:     org_sqlite_database_sqlite_SQLiteConnection
 * Method:    nativeBindString
 * Signature: (JJILjava/lang/String;)V
 */
JNIEXPORT void JNICALL Java_org_sqlite_database_sqlite_SQLiteConnection_nativeBindString
  (JNIEnv* env, jclass clazz, jlong connectionPtr, jlong statementPtr, jint index, jstring valueString) {
    SQLiteConnection* connection = reinterpret_cast<SQLiteConnection*>(connectionPtr);
    sqlite3_stmt* statement = reinterpret_cast<sqlite3_stmt*>(statementPtr);

    jsize valueLength = env->GetStringLength(valueString);
    const jchar* value = env->GetStringChars(valueString, nullptr);
    int err = sqlite3_bind_text16(statement, index, value, valueLength * sizeof(jchar),
            SQLITE_TRANSIENT);
    env->ReleaseStringChars(valueString, value);
    if (err != SQLITE_OK) {
        throw_sqlite3_exception_db_unspecified(env, connection->db);
    }
}

/*
 * Class:     org_sqlite_database_sqlite_SQLiteConnection
 * Method:    nativeBindBlob
 * Signature: (JJI[B)V
 */
JNIEXPORT void JNICALL Java_org_sqlite_database_sqlite_SQLiteConnection_nativeBindBlob
  (JNIEnv* env, jclass clazz, jlong connectionPtr,
        jlong statementPtr, jint index, jbyteArray valueArray) {
    SQLiteConnection* connection = reinterpret_cast<SQLiteConnection*>(connectionPtr);
    sqlite3_stmt* statement = reinterpret_cast<sqlite3_stmt*>(statementPtr);

    jsize valueLength = env->GetArrayLength(valueArray);
    jbyte* value = env->GetByteArrayElements(valueArray, nullptr);
    int err = sqlite3_bind_blob(statement, index, value, valueLength, SQLITE_TRANSIENT);
    env->ReleaseByteArrayElements(valueArray, value, JNI_ABORT);
    if (err != SQLITE_OK) {
        throw_sqlite3_exception_db_unspecified(env, connection->db);
    }
}

/*
 * Class:     org_sqlite_database_sqlite_SQLiteConnection
 * Method:    nativeResetStatementAndClearBindings
 * Signature: (JJ)V
 */
JNIEXPORT void JNICALL Java_org_sqlite_database_sqlite_SQLiteConnection_nativeResetStatementAndClearBindings
  (JNIEnv* env, jclass clazz, jlong connectionPtr, jlong statementPtr) {
    SQLiteConnection* connection = reinterpret_cast<SQLiteConnection*>(connectionPtr);
    sqlite3_stmt* statement = reinterpret_cast<sqlite3_stmt*>(statementPtr);

    int err = sqlite3_reset(statement);
    if (err == SQLITE_OK) {
        err = sqlite3_clear_bindings(statement);
    }
    if (err != SQLITE_OK) {
        throw_sqlite3_exception_db_unspecified(env, connection->db);
    }
}

/*
 * Class:     org_sqlite_database_sqlite_SQLiteConnection
 * Method:    nativeExecute
 * Signature: (JJ)V
 */
JNIEXPORT void JNICALL Java_org_sqlite_database_sqlite_SQLiteConnection_nativeExecute
  (JNIEnv* env, jclass clazz, jlong connectionPtr, jlong statementPtr) {
    SQLiteConnection* connection = reinterpret_cast<SQLiteConnection*>(connectionPtr);
    sqlite3_stmt* statement = reinterpret_cast<sqlite3_stmt*>(statementPtr);

    executeNonQuery(env, connection, statement);
}

/*
 * Class:     org_sqlite_database_sqlite_SQLiteConnection
 * Method:    nativeExecuteForLong
 * Signature: (JJ)J
 */
JNIEXPORT jlong JNICALL Java_org_sqlite_database_sqlite_SQLiteConnection_nativeExecuteForLong
  (JNIEnv* env, jclass clazz, jlong connectionPtr, jlong statementPtr) {
    SQLiteConnection* connection = reinterpret_cast<SQLiteConnection*>(connectionPtr);
    sqlite3_stmt* statement = reinterpret_cast<sqlite3_stmt*>(statementPtr);

    int err = executeOneRowQuery(env, connection, statement);
    if (err == SQLITE_ROW && sqlite3_column_count(statement) >= 1) {
        return sqlite3_column_int64(statement, 0);
    }
    return -1;
}

/*
 * Class:     org_sqlite_database_sqlite_SQLiteConnection
 * Method:    nativeExecuteForString
 * Signature: (JJ)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_org_sqlite_database_sqlite_SQLiteConnection_nativeExecuteForString
  (JNIEnv* env, jclass clazz, jlong connectionPtr, jlong statementPtr) {
    SQLiteConnection* connection = reinterpret_cast<SQLiteConnection*>(connectionPtr);
    sqlite3_stmt* statement = reinterpret_cast<sqlite3_stmt*>(statementPtr);

    int err = executeOneRowQuery(env, connection, statement);
    if (err == SQLITE_ROW && sqlite3_column_count(statement) >= 1) {
        const jchar* text = static_cast<const jchar*>(sqlite3_column_text16(statement, 0));
        if (text) {
            size_t length = sqlite3_column_bytes16(statement, 0) / sizeof(jchar);
            return env->NewString(text, length);
        }
    }
    return nullptr;
}

/*
 * Class:     org_sqlite_database_sqlite_SQLiteConnection
 * Method:    nativeExecuteForBlobFileDescriptor
 * Signature: (JJ)I
 */
JNIEXPORT jint JNICALL Java_org_sqlite_database_sqlite_SQLiteConnection_nativeExecuteForBlobFileDescriptor
  (JNIEnv* env, jclass clazz, jlong connectionPtr, jlong statementPtr) {
    SQLiteConnection* connection = reinterpret_cast<SQLiteConnection*>(connectionPtr);
    sqlite3_stmt* statement = reinterpret_cast<sqlite3_stmt*>(statementPtr);

    int err = executeOneRowQuery(env, connection, statement);
    if (err == SQLITE_ROW && sqlite3_column_count(statement) >= 1) {
        const void* blob = sqlite3_column_blob(statement, 0);
        if (blob) {
            int length = sqlite3_column_bytes(statement, 0);
            if (length >= 0) {
                return createAshmemRegionWithData(env, blob, length);
            }
        }
    }
    return -1;
}

/*
 * Class:     org_sqlite_database_sqlite_SQLiteConnection
 * Method:    nativeExecuteForChangedRowCount
 * Signature: (JJ)I
 */
JNIEXPORT jint JNICALL Java_org_sqlite_database_sqlite_SQLiteConnection_nativeExecuteForChangedRowCount
  (JNIEnv* env, jclass clazz, jlong connectionPtr, jlong statementPtr) {
    SQLiteConnection* connection = reinterpret_cast<SQLiteConnection*>(connectionPtr);
    sqlite3_stmt* statement = reinterpret_cast<sqlite3_stmt*>(statementPtr);

    int err = executeNonQuery(env, connection, statement);
    return err == SQLITE_DONE ? sqlite3_changes(connection->db) : -1;
}

/*
 * Class:     org_sqlite_database_sqlite_SQLiteConnection
 * Method:    nativeExecuteForLastInsertedRowId
 * Signature: (JJ)J
 */
JNIEXPORT jlong JNICALL Java_org_sqlite_database_sqlite_SQLiteConnection_nativeExecuteForLastInsertedRowId
  (JNIEnv* env, jclass clazz, jlong connectionPtr, jlong statementPtr) {
    SQLiteConnection* connection = reinterpret_cast<SQLiteConnection*>(connectionPtr);
    sqlite3_stmt* statement = reinterpret_cast<sqlite3_stmt*>(statementPtr);

    int err = executeNonQuery(env, connection, statement);
    return err == SQLITE_DONE && sqlite3_changes(connection->db) > 0
            ? sqlite3_last_insert_rowid(connection->db) : -1;
}

/*
** This method has been rewritten for org.sqlite.database.*. The original 
** android implementation used the C++ interface to populate a CursorWindow
** object. Since the NDK does not export this interface, we invoke the Java
** interface using standard JNI methods to do the same thing.
**
** This function executes the SQLite statement object passed as the 4th 
** argument and copies one or more returned rows into the CursorWindow
** object passed as the 5th argument. The set of rows copied into the 
** CursorWindow is always contiguous.
**
** The only row that *must* be copied into the CursorWindow is row 
** iRowRequired. Ideally, all rows from iRowStart through to the end
** of the query are copied into the CursorWindow. If this is not possible
** (CursorWindow objects have a finite capacity), some compromise position
** is found (see comments embedded in the code below for details).
**
** The return value is a 64-bit integer calculated as follows:
**
**      (iStart << 32) | nRow
**
** where iStart is the index of the first row copied into the CursorWindow.
** If the countAllRows argument is true, nRow is the total number of rows
** returned by the query. Otherwise, nRow is one greater than the index of 
** the last row copied into the CursorWindow.
*/
/*
 * Class:     org_sqlite_database_sqlite_SQLiteConnection
 * Method:    nativeExecuteForCursorWindow
 * Signature: (JJLandroid/database/CursorWindow;IIZ)J
 */
JNIEXPORT jlong JNICALL Java_org_sqlite_database_sqlite_SQLiteConnection_nativeExecuteForCursorWindow
  (
  JNIEnv *pEnv, 
  jclass clazz,
  jlong connectionPtr,             /* Pointer to SQLiteConnection C++ object */
  jlong statementPtr,              /* Pointer to sqlite3_stmt object */
  jobject win,                    /* The CursorWindow object to populate */
  jint startPos,                  /* First row to add (advisory) */
  jint iRowRequired,              /* Required row */
  jboolean countAllRows
) {
  SQLiteConnection *pConnection = reinterpret_cast<SQLiteConnection*>(connectionPtr);
  sqlite3_stmt *pStmt = reinterpret_cast<sqlite3_stmt*>(statementPtr);

  CWMethod aMethod[] = {
    {0, "clear",         "()V"},
    {0, "setNumColumns", "(I)Z"},
    {0, "allocRow",      "()Z"},
    {0, "freeLastRow",   "()V"},
    {0, "putNull",       "(II)Z"},
    {0, "putLong",       "(JII)Z"},
    {0, "putDouble",     "(DII)Z"},
    {0, "putString",     "(Ljava/lang/String;II)Z"},
    {0, "putBlob",       "([BII)Z"},
  };
  int i;                          /* Iterator variable */
  int nCol;                       /* Number of columns returned by pStmt */
  int nRow;
  jboolean bOk;
  int iStart;                     /* First row copied to CursorWindow */

  
  /* Class android.database.CursorWindow */
  ScopedLocalRef<jclass> cls(pEnv, pEnv->FindClass("android/database/CursorWindow"));
  /* Locate all required CursorWindow methods. */
  for(i=0; i<(sizeof(aMethod)/sizeof(struct CWMethod)); i++){
    aMethod[i].id = pEnv->GetMethodID(cls.get(), aMethod[i].zName, aMethod[i].zSig);
    if( aMethod[i].id==nullptr ){
      jniThrowExceptionFmt(pEnv, "java/lang/Exception", 
          "Failed to find method CursorWindow.%s()", aMethod[i].zName
      );
      return 0;
    }
  }

  /* Set the number of columns in the window */
  bOk = setWindowNumColumns(pEnv, win, pStmt, aMethod);
  if( bOk==0 ) return 0;

  nRow = 0;
  iStart = startPos;
  while( sqlite3_step(pStmt)==SQLITE_ROW ){
    /* Only copy in rows that occur at or after row index iStart. */
    if( nRow>=iStart && bOk ){
      bOk = copyRowToWindow(pEnv, win, (nRow - iStart), pStmt, aMethod);
      if( bOk==0 ){
        /* The CursorWindow object ran out of memory. If row iRowRequired was
        ** not successfully added before this happened, clear the CursorWindow
        ** and try to add the current row again.  */
        if( nRow<=iRowRequired ){
          bOk = setWindowNumColumns(pEnv, win, pStmt, aMethod);
          if( bOk==0 ){
            sqlite3_reset(pStmt);
            return 0;
          }
          iStart = nRow;
          bOk = copyRowToWindow(pEnv, win, (nRow - iStart), pStmt, aMethod);
        }

        /* If the CursorWindow is still full and the countAllRows flag is not
        ** set, break out of the loop here. If countAllRows is set, continue
        ** so as to set variable nRow correctly.  */
        if( bOk==0 && countAllRows==0 ) break;
      }
    }

    nRow++;
  }

  /* Finalize the statement. If this indicates an error occurred, throw an
  ** SQLiteException exception.  */
  int rc = sqlite3_reset(pStmt);
  if( rc!=SQLITE_OK ){
    throw_sqlite3_exception_db_unspecified(pEnv, sqlite3_db_handle(pStmt));
    return 0;
  }

  jlong lRet = jlong(iStart) << 32 | jlong(nRow);
  return lRet;
}

/*
 * Class:     org_sqlite_database_sqlite_SQLiteConnection
 * Method:    nativeGetDbLookaside
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_org_sqlite_database_sqlite_SQLiteConnection_nativeGetDbLookaside
  (JNIEnv* env, jclass clazz, jlong connectionPtr) {
    SQLiteConnection* connection = reinterpret_cast<SQLiteConnection*>(connectionPtr);

    int cur = -1;
    int unused;
    sqlite3_db_status(connection->db, SQLITE_DBSTATUS_LOOKASIDE_USED, &cur, &unused, 0);
    return cur;
}

/*
 * Class:     org_sqlite_database_sqlite_SQLiteConnection
 * Method:    nativeCancel
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_sqlite_database_sqlite_SQLiteConnection_nativeCancel
  (JNIEnv* env, jclass clazz, jlong connectionPtr) {
    SQLiteConnection* connection = reinterpret_cast<SQLiteConnection*>(connectionPtr);
    connection->canceled = true;
}

/*
 * Class:     org_sqlite_database_sqlite_SQLiteConnection
 * Method:    nativeResetCancel
 * Signature: (JZ)V
 */
JNIEXPORT void JNICALL Java_org_sqlite_database_sqlite_SQLiteConnection_nativeResetCancel
  (JNIEnv* env, jclass clazz, jlong connectionPtr, jboolean cancelable) {
    SQLiteConnection* connection = reinterpret_cast<SQLiteConnection*>(connectionPtr);
    connection->canceled = false;

    if (cancelable) {
        sqlite3_progress_handler(connection->db, 4, sqliteProgressHandlerCallback,
                connection);
    } else {
        sqlite3_progress_handler(connection->db, 0, nullptr, nullptr);
    }
}

/*
 * Class:     org_sqlite_database_sqlite_SQLiteConnection
 * Method:    nativeHasCodec
 * Signature: ()Z
 */
JNIEXPORT jboolean JNICALL Java_org_sqlite_database_sqlite_SQLiteConnection_nativeHasCodec
(JNIEnv* env, jobject clazz){
#ifdef SQLITE_HAS_CODEC
  return true;
#else
  return false;
#endif
}
