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

#include <cstddef>

#include "org_sqlite_database_sqlite_SQLiteConnection.h"
#include "org_sqlite_database_sqlite_SQLiteCommon.h"

using org_opendatakit::sqliteInitialize;
using org_opendatakit::openConnection;
using org_opendatakit::closeConnection;
using org_opendatakit::prepareStatement;
using org_opendatakit::finalizeStatement;
using org_opendatakit::bindParameterCount;
using org_opendatakit::statementIsReadOnly;
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
using org_opendatakit::executeIntoObjectArray;
using org_opendatakit::cancel;
using org_opendatakit::resetCancel;

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

    return openConnection(env, pathStr, openFlags, labelStr, enableTrace, enableProfile);
}

/*
 * Class:     org_sqlite_database_sqlite_SQLiteConnection
 * Method:    nativeClose
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_sqlite_database_sqlite_SQLiteConnection_nativeClose
  (JNIEnv* env, jclass clazz, jlong connectionPtr) {

    closeConnection(env, connectionPtr);
}

/*
 * Class:     org_sqlite_database_sqlite_SQLiteConnection
 * Method:    nativePrepareStatement
 * Signature: (JLjava/lang/String;)J
 */
JNIEXPORT jlong JNICALL Java_org_sqlite_database_sqlite_SQLiteConnection_nativePrepareStatement
  (JNIEnv* env, jclass clazz, jlong connectionPtr, jstring sqlString) {

    return prepareStatement(env, connectionPtr, sqlString);
}

/*
 * Class:     org_sqlite_database_sqlite_SQLiteConnection
 * Method:    nativeFinalizeStatement
 * Signature: (JJ)V
 */
JNIEXPORT void JNICALL Java_org_sqlite_database_sqlite_SQLiteConnection_nativeFinalizeStatement
  (JNIEnv* env, jclass clazz, jlong connectionPtr, jlong statementPtr) {

    finalizeStatement(env, connectionPtr, statementPtr);
}

/*
 * Class:     org_sqlite_database_sqlite_SQLiteConnection
 * Method:    nativeGetParameterCount
 * Signature: (JJ)I
 */
JNIEXPORT jint JNICALL Java_org_sqlite_database_sqlite_SQLiteConnection_nativeGetParameterCount
  (JNIEnv* env, jclass clazz, jlong connectionPtr, jlong statementPtr) {

    return bindParameterCount(env, connectionPtr, statementPtr);
}

/*
 * Class:     org_sqlite_database_sqlite_SQLiteConnection
 * Method:    nativeIsReadOnly
 * Signature: (JJ)Z
 */
JNIEXPORT jboolean JNICALL Java_org_sqlite_database_sqlite_SQLiteConnection_nativeIsReadOnly
  (JNIEnv* env, jclass clazz, jlong connectionPtr, jlong statementPtr) {

    return statementIsReadOnly(env, connectionPtr, statementPtr);
}

/*
 * Class:     org_sqlite_database_sqlite_SQLiteConnection
 * Method:    nativeBindNull
 * Signature: (JJI)V
 */
JNIEXPORT void JNICALL Java_org_sqlite_database_sqlite_SQLiteConnection_nativeBindNull
  (JNIEnv* env, jclass clazz, jlong connectionPtr, jlong statementPtr, jint index) {

    bindNull(env, connectionPtr, statementPtr, index);
}

/*
 * Class:     org_sqlite_database_sqlite_SQLiteConnection
 * Method:    nativeBindLong
 * Signature: (JJIJ)V
 */
JNIEXPORT void JNICALL Java_org_sqlite_database_sqlite_SQLiteConnection_nativeBindLong
  (JNIEnv* env, jclass clazz, jlong connectionPtr, jlong statementPtr, jint index, jlong value) {

    bindLong(env, connectionPtr, statementPtr, index, value);
}

/*
 * Class:     org_sqlite_database_sqlite_SQLiteConnection
 * Method:    nativeBindDouble
 * Signature: (JJID)V
 */
JNIEXPORT void JNICALL Java_org_sqlite_database_sqlite_SQLiteConnection_nativeBindDouble
  (JNIEnv* env, jclass clazz, jlong connectionPtr, jlong statementPtr, jint index, jdouble value) {

    bindDouble(env, connectionPtr, statementPtr, index, value);
}

/*
 * Class:     org_sqlite_database_sqlite_SQLiteConnection
 * Method:    nativeBindString
 * Signature: (JJILjava/lang/String;)V
 */
JNIEXPORT void JNICALL Java_org_sqlite_database_sqlite_SQLiteConnection_nativeBindString
  (JNIEnv* env, jclass clazz, jlong connectionPtr, jlong statementPtr, jint index, jstring valueString) {

    bindString(env, connectionPtr, statementPtr, index, valueString);
}

/*
 * Class:     org_sqlite_database_sqlite_SQLiteConnection
 * Method:    nativeBindBlob
 * Signature: (JJI[B)V
 */
JNIEXPORT void JNICALL Java_org_sqlite_database_sqlite_SQLiteConnection_nativeBindBlob
  (JNIEnv* env, jclass clazz, jlong connectionPtr,
        jlong statementPtr, jint index, jbyteArray valueArray) {

    bindBlob(env, connectionPtr, statementPtr, index, valueArray);
}

/*
 * Class:     org_sqlite_database_sqlite_SQLiteConnection
 * Method:    nativeResetStatementAndClearBindings
 * Signature: (JJ)V
 */
JNIEXPORT void JNICALL Java_org_sqlite_database_sqlite_SQLiteConnection_nativeResetStatementAndClearBindings
  (JNIEnv* env, jclass clazz, jlong connectionPtr, jlong statementPtr) {

    resetAndClearBindings(env, connectionPtr, statementPtr);
}

/*
 * Class:     org_sqlite_database_sqlite_SQLiteConnection
 * Method:    nativeExecute
 * Signature: (JJ)V
 */
JNIEXPORT void JNICALL Java_org_sqlite_database_sqlite_SQLiteConnection_nativeExecute
  (JNIEnv* env, jclass clazz, jlong connectionPtr, jlong statementPtr) {

    executeNonQuery(env, connectionPtr, statementPtr);
}

/*
 * Class:     org_sqlite_database_sqlite_SQLiteConnection
 * Method:    nativeExecuteForLong
 * Signature: (JJ)J
 */
JNIEXPORT jlong JNICALL Java_org_sqlite_database_sqlite_SQLiteConnection_nativeExecuteForLong
  (JNIEnv* env, jclass clazz, jlong connectionPtr, jlong statementPtr) {

    return executeForLong(env, connectionPtr, statementPtr);
}

/*
 * Class:     org_sqlite_database_sqlite_SQLiteConnection
 * Method:    nativeExecuteForString
 * Signature: (JJ)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_org_sqlite_database_sqlite_SQLiteConnection_nativeExecuteForString
  (JNIEnv* env, jclass clazz, jlong connectionPtr, jlong statementPtr) {

    return executeForString(env, connectionPtr, statementPtr);
}

/*
 * Class:     org_sqlite_database_sqlite_SQLiteConnection
 * Method:    nativeExecuteForChangedRowCount
 * Signature: (JJ)I
 */
JNIEXPORT jint JNICALL Java_org_sqlite_database_sqlite_SQLiteConnection_nativeExecuteForChangedRowCount
  (JNIEnv* env, jclass clazz, jlong connectionPtr, jlong statementPtr) {

    return executeForChangedRowCount(env, connectionPtr, statementPtr);
}

/*
 * Class:     org_sqlite_database_sqlite_SQLiteConnection
 * Method:    nativeExecuteForLastInsertedRowId
 * Signature: (JJ)J
 */
JNIEXPORT jlong JNICALL Java_org_sqlite_database_sqlite_SQLiteConnection_nativeExecuteForLastInsertedRowId
  (JNIEnv* env, jclass clazz, jlong connectionPtr, jlong statementPtr) {

    return executeForLastInsertedRowId(env, connectionPtr, statementPtr);
}

/*
 * Class:     org_sqlite_database_sqlite_SQLiteConnection
 * Method:    nativeExecuteForObjectArray
 * Signature: (JJ)[Ljava/lang/Object;
 */
JNIEXPORT jobjectArray JNICALL Java_org_sqlite_database_sqlite_SQLiteConnection_nativeExecuteForObjectArray
        (
                JNIEnv *env,
                jclass clazz,
                jlong connectionPtr,             /* Pointer to SQLiteConnection C++ object */
                jlong statementPtr               /* Pointer to sqlite3_stmt object */
        ) {

    return executeIntoObjectArray(env, connectionPtr, statementPtr);
}

/*
 * Class:     org_sqlite_database_sqlite_SQLiteConnection
 * Method:    nativeCancel
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_sqlite_database_sqlite_SQLiteConnection_nativeCancel
  (JNIEnv* env, jclass clazz, jlong connectionPtr) {

    cancel(env, connectionPtr);
}

/*
 * Class:     org_sqlite_database_sqlite_SQLiteConnection
 * Method:    nativeResetCancel
 * Signature: (JZ)V
 */
JNIEXPORT void JNICALL Java_org_sqlite_database_sqlite_SQLiteConnection_nativeResetCancel
  (JNIEnv* env, jclass clazz, jlong connectionPtr, jboolean cancelable) {

    resetCancel(env, connectionPtr, cancelable);
}
