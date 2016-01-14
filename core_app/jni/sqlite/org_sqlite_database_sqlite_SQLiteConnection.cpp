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


using org_opendatakit::ScopedLocalRef;
using org_opendatakit::SQLiteConnection;
using org_opendatakit::sqliteInitialize;
using org_opendatakit::openConnection;
using org_opendatakit::closeConnection;
using org_opendatakit::prepareStatement;
using org_opendatakit::finalizeStatement;
using org_opendatakit::getColumnCount;
using org_opendatakit::getColumnName;
using org_opendatakit::bindNull;
using org_opendatakit::bindLong;
using org_opendatakit::bindDouble;
using org_opendatakit::bindString;
using org_opendatakit::bindBlob;
using org_opendatakit::resetAndClearBindings;
using org_opendatakit::executeNonQuery;
using org_opendatakit::executeForLong;
using org_opendatakit::executeForString;
using org_opendatakit::executeForChangedRowCount;
using org_opendatakit::executeForLastInsertedRowId;
using org_opendatakit::executeIntoCursorWindow;
using org_opendatakit::getDbLookasideUsed;
using org_opendatakit::cancel;
using org_opendatakit::resetCancel;
using org_opendatakit::hasCodec;

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

    const char* pathChars = env->GetStringUTFChars(pathStr, nullptr);
    std::string path(pathChars);
    env->ReleaseStringUTFChars(pathStr, pathChars);

    const char* labelChars = env->GetStringUTFChars(labelStr, nullptr);
    std::string label(labelChars);
    env->ReleaseStringUTFChars(labelStr, labelChars);

    SQLiteConnection* connection = nullptr;
    connection = openConnection(env, path, openFlags, label, enableTrace, enableProfile);

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
    closeConnection(env, connection);
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
    jfieldID f_numArgs;
    GET_FIELD_ID(f_name, customFunctionClass.get(), "name", "Ljava/lang/String;");
    GET_FIELD_ID(f_numArgs, customFunctionClass.get(), "numArgs", "I");

    jstring nameStr = jstring(env->GetObjectField(functionObj, f_name));
    jint numArgs = env->GetIntField(functionObj, f_numArgs);

    const char* name = env->GetStringUTFChars(nameStr, nullptr);

    registerCustomFunction(env, connection, name, numArgs, functionObj);

    // and release the lock on name (nameStr), allowing it to reloc.
    env->ReleaseStringUTFChars(nameStr, name);
}

/*
 * Class:     org_sqlite_database_sqlite_SQLiteConnection
 * Method:    nativePrepareStatement
 * Signature: (JLjava/lang/String;)J
 */
JNIEXPORT jlong JNICALL Java_org_sqlite_database_sqlite_SQLiteConnection_nativePrepareStatement
  (JNIEnv* env, jclass clazz, jlong connectionPtr, jstring sqlString) {
    SQLiteConnection* connection = reinterpret_cast<SQLiteConnection*>(connectionPtr);

    sqlite3_stmt* statement = prepareStatement(env, connection, sqlString);

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

    finalizeStatement(env, connection, statement);
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

    return bindParameterCount(env, connection, statement);
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

    return statementIsReadOnly(env, connection, statement);
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

    return getColumnCount(env, connection, statement);
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

    return getColumnName(env, connection, statement, index);
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

    bindNull(env, connection, statement, index);
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

    bindLong(env, connection, statement, index, value);
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

    bindDouble(env, connection, statement, index, value);
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

    bindString(env, connection, statement, index, value, valueLength);

    env->ReleaseStringChars(valueString, value);
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

    bindBlob(env, connection, statement, index, value, valueLength);

    env->ReleaseByteArrayElements(valueArray, value, JNI_ABORT);
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

    resetAndClearBindings(env, connection, statement);
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

    return executeForLong(env, connection, statement);
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

    return executeForString(env, connection, statement);
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

    return executeForChangedRowCount(env, connection, statement);
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

    return executeForLastInsertedRowId(env, connection, statement);
}

/*
 * Class:     org_sqlite_database_sqlite_SQLiteConnection
 * Method:    nativeExecuteForCursorWindow
 * Signature: (JJLandroid/database/CursorWindow;IIZ)J
 */
JNIEXPORT jlong JNICALL Java_org_sqlite_database_sqlite_SQLiteConnection_nativeExecuteForCursorWindow
  (
  JNIEnv *env, 
  jclass clazz,
  jlong connectionPtr,             /* Pointer to SQLiteConnection C++ object */
  jlong statementPtr,              /* Pointer to sqlite3_stmt object */
  jobject win,                    /* The CursorWindow object to populate */
  jint startPos,                  /* First row to add (advisory) */
  jint iRowRequired,              /* Required row */
  jboolean countAllRows
) {
  SQLiteConnection *connection = reinterpret_cast<SQLiteConnection*>(connectionPtr);
  sqlite3_stmt *statement = reinterpret_cast<sqlite3_stmt*>(statementPtr);

  jlong lResult = executeIntoCursorWindow(env, connection, statement,
                          win, startPos, iRowRequired, countAllRows);
  return lResult;
}

/*
 * Class:     org_sqlite_database_sqlite_SQLiteConnection
 * Method:    nativeGetDbLookaside
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_org_sqlite_database_sqlite_SQLiteConnection_nativeGetDbLookaside
  (JNIEnv* env, jclass clazz, jlong connectionPtr) {
    SQLiteConnection* connection = reinterpret_cast<SQLiteConnection*>(connectionPtr);

    jint cur = getDbLookasideUsed(env, connection);
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
    cancel(env, connection);
}

/*
 * Class:     org_sqlite_database_sqlite_SQLiteConnection
 * Method:    nativeResetCancel
 * Signature: (JZ)V
 */
JNIEXPORT void JNICALL Java_org_sqlite_database_sqlite_SQLiteConnection_nativeResetCancel
  (JNIEnv* env, jclass clazz, jlong connectionPtr, jboolean cancelable) {
    SQLiteConnection* connection = reinterpret_cast<SQLiteConnection*>(connectionPtr);
    resetCancel(env, connection, cancelable);
}

/*
 * Class:     org_sqlite_database_sqlite_SQLiteConnection
 * Method:    nativeHasCodec
 * Signature: ()Z
 */
JNIEXPORT jboolean JNICALL Java_org_sqlite_database_sqlite_SQLiteConnection_nativeHasCodec
(JNIEnv* env, jobject clazz){
    return hasCodec(env);
}
