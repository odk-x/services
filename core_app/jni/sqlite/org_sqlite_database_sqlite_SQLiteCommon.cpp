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

#define LOG_TAG "SQLiteCommon"

#include <stdio.h>
#include <assert.h>
#include <pthread.h>

#include "MutexRegion.h"

#include "org_sqlite_database_sqlite_SQLiteCommon.h"

namespace org_opendatakit {

// FIX:
static JavaVM *gpJavaVM = 0;


#define GET_METHOD_ID(var, clazz, methodName, fieldDescriptor) \
        var = env->GetMethodID(clazz, methodName, fieldDescriptor); \
        LOG_FATAL_IF(! var, "Unable to find method" methodName);

#define GET_FIELD_ID(var, clazz, fieldName, fieldDescriptor) \
        var = env->GetFieldID(clazz, fieldName, fieldDescriptor); \
        LOG_FATAL_IF(! var, "Unable to find field " fieldName);

// Called each time a message is logged.
void sqliteLogCallback(void* data, int iErrCode, const char* zMsg) {
    bool verboseLog = !!data;
    if (iErrCode == 0 || iErrCode == SQLITE_CONSTRAINT || iErrCode == SQLITE_SCHEMA) {
        if (verboseLog) {
            ALOG(LOG_VERBOSE, SQLITE_LOG_TAG, "(%d) %s\n", iErrCode, zMsg);
        }
    } else {
        ALOG(LOG_ERROR, SQLITE_LOG_TAG, "(%d) %s\n", iErrCode, zMsg);
    }
}

pthread_mutex_t g_init_mutex = PTHREAD_MUTEX_INITIALIZER;

// Sets the global SQLite configuration.
// This must be called before any other SQLite functions are called.
void sqliteInitialize(JNIEnv* env) {
    MutexRegion guard(&g_init_mutex);

    env->GetJavaVM(&gpJavaVM);

    // Enable multi-threaded mode.  In this mode, SQLite is safe to use by multiple
    // threads as long as no two threads use the same database connection at the same
    // time (which we guarantee in the SQLite database wrappers).
    sqlite3_config(SQLITE_CONFIG_MULTITHREAD);

    // Redirect SQLite log messages to the Android log.
#if 0
    bool verboseLog = android_util_Log_isVerboseLogEnabled(SQLITE_LOG_TAG);
#endif
    bool verboseLog = false;
    void * verboseLogging = (void*) 1L;
    void * quietLogging = (void*) 0L;
    sqlite3_config(SQLITE_CONFIG_LOG, &sqliteLogCallback, verboseLog ? verboseLogging : quietLogging);

    // The soft heap limit prevents the page cache allocations from growing
    // beyond the given limit, no matter what the max page cache sizes are
    // set to. The limit does not, as of 3.5.0, affect any other allocations.
    sqlite3_soft_heap_limit(SOFT_HEAP_LIMIT);

    // Initialize SQLite.
    sqlite3_initialize();
}

/*
 * Returns a human-readable summary of an exception object.  The buffer will
 * be populated with the "binary" class name and, if present, the
 * exception message.
 */
static bool getExceptionSummary(JNIEnv* env, jthrowable exception, std::string& result) {

    /* get the name of the exception's class */
	// can't fail
	ScopedLocalRef<jclass> exceptionClass(env, env->GetObjectClass(exception));
    // java.lang.Class, can't fail
    ScopedLocalRef<jclass> classClass(env, env->GetObjectClass(exceptionClass.get()));
    jmethodID classGetNameMethod =
            env->GetMethodID(classClass.get(), "getName", "()Ljava/lang/String;");
    ScopedLocalRef<jstring> classNameStr(env,
            (jstring) env->CallObjectMethod(exceptionClass.get(), classGetNameMethod));
    if (classNameStr.get() == nullptr) {
        env->ExceptionClear();
        result = "<error getting class name>";
        return false;
    }
    const char* classNameChars = env->GetStringUTFChars(classNameStr.get(), nullptr);
    if (classNameChars == nullptr) {
        env->ExceptionClear();
        result = "<error getting class name UTF-8>";
        return false;
    }
    result += classNameChars;
    env->ReleaseStringUTFChars(classNameStr.get(), classNameChars);

    /* if the exception has a detail message, get that */
    jmethodID getMessage =
            env->GetMethodID(exceptionClass.get(), "getMessage", "()Ljava/lang/String;");
    ScopedLocalRef<jstring> messageStr(env,
            (jstring) env->CallObjectMethod(exception, getMessage));
    if (messageStr.get() == nullptr) {
        return true;
    }

    result += ": ";

    const char* messageChars = env->GetStringUTFChars(messageStr.get(), nullptr);
    if (messageChars != nullptr) {
        result += messageChars;
        env->ReleaseStringUTFChars(messageStr.get(), messageChars);
    } else {
        result += "<error getting message>";
        env->ExceptionClear(); // clear OOM
    }

    return true;
}

static int jniThrowException(JNIEnv* env, const char* className, const char* msg) {
   if (env->ExceptionCheck()) {
       /* TODO: consider creating the new exception with this as "cause" */
       ScopedLocalRef<jthrowable> exception(env, env->ExceptionOccurred());
       env->ExceptionClear();

       if (exception.get() != nullptr) {
           std::string text;
           getExceptionSummary(env, exception.get(), text);
           ALOGW("Discarding pending exception (%s) to throw %s", text.c_str(), className);
       }
   }

   ScopedLocalRef<jclass> exceptionClass(env, env->FindClass(className));
   if (exceptionClass.get() == nullptr) {
       ALOGE("Unable to find exception class %s", className);
       /* ClassNotFoundException now pending */
       return -1;
   }

   if (env->ThrowNew(exceptionClass.get(), msg) != JNI_OK) {
       ALOGE("Failed throwing '%s' '%s'", className, msg);
       /* an exception, most likely OOM, will now be pending */
       return -1;
   }

   return 0;
}
static const char* jniStrError(int errnum, char* buf, size_t buflen) {
    int rc = strerror_r(errnum, buf, buflen);
    if (rc != 0) {
        // (POSIX only guarantees a value other than 0. The safest
        // way to implement this function is to use C++ and overload on the
        // type of strerror_r to accurately distinguish GNU from POSIX.)
        snprintf(buf, buflen, "errno %d", errnum);
        // make sure we zero-terminate
        buf[buflen-1] = '\0';
    }
    return buf;
}

int jniThrowIOException(JNIEnv* env, int errnum) {
    char buffer[80];
    const char* message = jniStrError(errnum, buffer, sizeof(buffer));
    return jniThrowException(env, "java/io/IOException", message);
}

int jniThrowExceptionFmt(JNIEnv* env, const char* className, const char* fmt, ...) {
    va_list args;
    va_start(args, fmt);
    char msgBuf[512];
    vsnprintf(msgBuf, sizeof(msgBuf), fmt, args);
    msgBuf[sizeof(msgBuf)-1] = '\0';
    return jniThrowException(env, className,msgBuf);
    va_end(args);
}


/* throw a SQLiteException for a given error code, sqlite3message, and
   user message
 */
void throw_sqlite3_exception(JNIEnv* env, int errcode,
                             const char* sqlite3Message, const char* message) {
    const char* exceptionClass;
    switch (errcode & 0xff) { /* mask off extended error code */
        case SQLITE_IOERR:
            exceptionClass = "org/sqlite/database/sqlite/SQLiteDiskIOException";
            break;
        case SQLITE_CORRUPT:
        case SQLITE_NOTADB: // treat "unsupported file format" error as corruption also
            exceptionClass = "org/sqlite/database/sqlite/SQLiteDatabaseCorruptException";
            break;
        case SQLITE_CONSTRAINT:
            exceptionClass = "org/sqlite/database/sqlite/SQLiteConstraintException";
            break;
        case SQLITE_ABORT:
            exceptionClass = "org/sqlite/database/sqlite/SQLiteAbortException";
            break;
        case SQLITE_DONE:
            exceptionClass = "org/sqlite/database/sqlite/SQLiteDoneException";
            sqlite3Message = nullptr; // SQLite error message is irrelevant in this case
            break;
        case SQLITE_FULL:
            exceptionClass = "org/sqlite/database/sqlite/SQLiteFullException";
            break;
        case SQLITE_MISUSE:
            exceptionClass = "org/sqlite/database/sqlite/SQLiteMisuseException";
            break;
        case SQLITE_PERM:
            exceptionClass = "org/sqlite/database/sqlite/SQLiteAccessPermException";
            break;
        case SQLITE_BUSY:
            exceptionClass = "org/sqlite/database/sqlite/SQLiteDatabaseLockedException";
            break;
        case SQLITE_LOCKED:
            exceptionClass = "org/sqlite/database/sqlite/SQLiteTableLockedException";
            break;
        case SQLITE_READONLY:
            exceptionClass = "org/sqlite/database/sqlite/SQLiteReadOnlyDatabaseException";
            break;
        case SQLITE_CANTOPEN:
            exceptionClass = "org/sqlite/database/sqlite/SQLiteCantOpenDatabaseException";
            break;
        case SQLITE_TOOBIG:
            exceptionClass = "org/sqlite/database/sqlite/SQLiteBlobTooBigException";
            break;
        case SQLITE_RANGE:
            exceptionClass = "org/sqlite/database/sqlite/SQLiteBindOrColumnIndexOutOfRangeException";
            break;
        case SQLITE_NOMEM:
            exceptionClass = "org/sqlite/database/sqlite/SQLiteOutOfMemoryException";
            break;
        case SQLITE_MISMATCH:
            exceptionClass = "org/sqlite/database/sqlite/SQLiteDatatypeMismatchException";
            break;
        case SQLITE_INTERRUPT:
            exceptionClass = "android/os/OperationCanceledException";
            break;
        default:
            exceptionClass = "org/sqlite/database/sqlite/SQLiteException";
            break;
    }

    if (sqlite3Message) {
        char *zFullmsg = sqlite3_mprintf(
            "%s (code %d)%s%s", sqlite3Message, errcode, 
            (message ? ": " : ""), (message ? message : "")
        );
        jniThrowException(env, exceptionClass, zFullmsg);
        sqlite3_free(zFullmsg);
    } else {
        jniThrowException(env, exceptionClass, message);
    }
}

/* throw a SQLiteException with a message appropriate for the error in handle
   concatenated with the given message
 */
void throw_sqlite3_exception_db(JNIEnv* env, sqlite3* handle, const char* message) {
    if (handle) {
        // get the error code and message from the SQLite connection
        // the error message may contain more information than the error code
        // because it is based on the extended error code rather than the simplified
        // error code that SQLite normally returns.
    	int extendedErrCode = sqlite3_extended_errcode(handle);
    	const char * extendedMsg = sqlite3_errmsg(handle);
        throw_sqlite3_exception(env, extendedErrCode, extendedMsg, message);
    } else {
        // we use SQLITE_OK so that a generic SQLiteException is thrown;
        // any code not specified in the switch statement below would do.
        throw_sqlite3_exception(env, SQLITE_OK, "unknown error", message);
    }
}

/* throw a SQLiteException for a given error code
 * should only be used when the database connection is not available because the
 * error information will not be quite as rich */
void throw_sqlite3_exception_errcode(JNIEnv* env, int errcode, const char* message) {
    throw_sqlite3_exception(env, errcode, "unknown error", message);
}

/* throw a SQLiteException with a message appropriate for the error in handle */
void throw_sqlite3_exception_db_unspecified(JNIEnv* env, sqlite3* handle) {
	const char * unspecified = "unspecified";
	throw_sqlite3_exception_db(env, handle, unspecified);
}

// Called each time a statement begins execution, when tracing is enabled.
void sqliteTraceCallback(void *data, const char *sql) {
    SQLiteConnection* connection = static_cast<SQLiteConnection*>(data);
    ALOG(LOG_VERBOSE, SQLITE_TRACE_TAG, "%s: \"%s\"\n",
            connection->label.c_str(), sql);
}

// Called each time a statement finishes execution, when profiling is enabled.
void sqliteProfileCallback(void *data, const char *sql, sqlite3_uint64 tm) {
    SQLiteConnection* connection = static_cast<SQLiteConnection*>(data);
    ALOG(LOG_VERBOSE, SQLITE_PROFILE_TAG, "%s: \"%s\" took %0.3f ms\n",
            connection->label.c_str(), sql, tm * 0.000001f);
}

// Called after each SQLite VM instruction when cancelation is enabled.
int sqliteProgressHandlerCallback(void* data) {
    SQLiteConnection* connection = static_cast<SQLiteConnection*>(data);
    return connection->canceled;
}

/*
** This function is a collation sequence callback equivalent to the built-in
** BINARY sequence.
**
** Stock Android uses a modified version of sqlite3.c that calls out to a module
** named "sqlite3_android" to add extra built-in collations and functions to
** all database handles. Specifically, collation sequence "LOCALIZED". For now,
** this module does not include sqlite3_android (since it is difficult to build
** with the NDK only). Instead, this function is registered as "LOCALIZED" for all
** new database handles.
*/
int coll_localized(
  void *not_used,
  int nKey1, const void *pKey1,
  int nKey2, const void *pKey2
){
  int rc, n;
  n = nKey1<nKey2 ? nKey1 : nKey2;
  rc = memcmp(pKey1, pKey2, n);
  if( rc==0 ){
    rc = nKey1 - nKey2;
  }
  return rc;
}

// Called each time a custom function is evaluated.
void sqliteCustomFunctionCallback(sqlite3_context *context,
        int argc, sqlite3_value **argv) {

    JNIEnv* env = 0;
    gpJavaVM->GetEnv((void**)&env, JNI_VERSION_1_6);

    // Get the callback function object.
    // Create a new local reference to it in case the callback tries to do something
    // dumb like unregister the function (thereby destroying the global ref) while it is running.
    jobject functionObjGlobal = reinterpret_cast<jobject>(sqlite3_user_data(context));
    ScopedLocalRef<jobject> functionObj(env, env->NewLocalRef(functionObjGlobal));

    // FIX: ScopedLocalRef<jobjectArray> argsArray(env, env->NewObjectArray(argc, JniConstants::stringClass, nullptr));
    ScopedLocalRef<jclass> stringClass(env, env->FindClass("java/lang/String"));
    ScopedLocalRef<jobjectArray> argsArray(env, env->NewObjectArray(argc, stringClass.get(), nullptr));
    if (argsArray.get() != nullptr) {
        for (int i = 0; i < argc; i++) {
            const jchar* arg = static_cast<const jchar*>(sqlite3_value_text16(argv[i]));
            if (!arg) {
                ALOGW("NULL argument in custom_function_callback.  This should not happen.");
            } else {
                size_t argLen = sqlite3_value_bytes16(argv[i]) / sizeof(jchar);
                ScopedLocalRef<jstring> argStr(env, env->NewString(arg, argLen));
                if (argStr.get() == nullptr) {
                    goto error; // out of memory error
                }
                env->SetObjectArrayElement(argsArray.get(), i, argStr.get());
            }
        }

        {


            ScopedLocalRef<jclass> customFunctionClass(env, env->FindClass("org/sqlite/database/sqlite/SQLiteCustomFunction"));
            if (customFunctionClass.get() == nullptr) {
                // unable to locate the class -- silently exit
                ALOGE("Unable to find class org/sqlite/database/sqlite/SQLiteCustomFunction");
                goto error;
            }

            jmethodID dispatchCallback;
            GET_METHOD_ID(dispatchCallback, customFunctionClass.get(), "dispatchCallback", "([Ljava/lang/String;)V");

            // TODO: Support functions that return values.
            env->CallVoidMethod(functionObj.get(), dispatchCallback, argsArray.get());
        }

    }

error:
    // drop out -- will release the local ref

    if (env->ExceptionCheck()) {
        ALOGE("An exception was thrown by custom SQLite function.");
        /* LOGE_EX(env); */
        env->ExceptionClear();
    }
}

// Called when a custom function is destroyed.
void sqliteCustomFunctionDestructor(void* data) {
    jobject functionObjGlobal = reinterpret_cast<jobject>(data);
    JNIEnv* env = 0;
    gpJavaVM->GetEnv((void**)&env, JNI_VERSION_1_6);
    env->DeleteGlobalRef(functionObjGlobal);
}

int executeNonQuery(JNIEnv* env, SQLiteConnection* connection, sqlite3_stmt* statement) {
    int err = sqlite3_step(statement);
    if (err == SQLITE_ROW) {
        throw_sqlite3_exception_db(env, connection->db,
                "Queries can be performed using SQLiteDatabase query or rawQuery methods only.");
    } else if (err != SQLITE_DONE) {
        throw_sqlite3_exception_db_unspecified(env, connection->db);
    }
    return err;
}

int executeOneRowQuery(JNIEnv* env, SQLiteConnection* connection, sqlite3_stmt* statement) {
    int err = sqlite3_step(statement);
    if (err != SQLITE_ROW) {
        throw_sqlite3_exception_db_unspecified(env, connection->db);
    }
    return err;
}


int createAshmemRegionWithData(JNIEnv* env, const void* data, size_t length) {
#if 0
    int error = 0;
    int fd = ashmem_create_region(nullptr, length);
    if (fd < 0) {
        error = errno;
        ALOGE("ashmem_create_region failed: %s", strerror(error));
    } else {
        if (length > 0) {
            void* ptr = mmap(nullptr, length, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
            if (ptr == MAP_FAILED) {
                error = errno;
                ALOGE("mmap failed: %s", strerror(error));
            } else {
                memcpy(ptr, data, length);
                munmap(ptr, length);
            }
        }

        if (!error) {
            if (ashmem_set_prot_region(fd, PROT_READ) < 0) {
                error = errno;
                ALOGE("ashmem_set_prot_region failed: %s", strerror(errno));
            } else {
                return fd;
            }
        }

        close(fd);
    }

#endif
    jniThrowIOException(env, -1);
    return -1;
}

/*
** Append the contents of the row that SQL statement pStmt currently points to
** to the CursorWindow object passed as the second argument. The CursorWindow
** currently contains iRow rows. Return true on success or false if an error
** occurs.
*/
jboolean copyRowToWindow(
  JNIEnv *pEnv,
  jobject win,
  int iRow,
  sqlite3_stmt *pStmt,
  CWMethod *aMethod
){
  int nCol = sqlite3_column_count(pStmt);
  int i;
  jboolean bOk;

  bOk = pEnv->CallBooleanMethod(win, aMethod[CW_ALLOCROW].id);
  for(i=0; bOk && i<nCol; i++){
    switch( sqlite3_column_type(pStmt, i) ){
      case SQLITE_NULL: {
        bOk = pEnv->CallBooleanMethod(win, aMethod[CW_PUTNULL].id, iRow, i);
        break;
      }

      case SQLITE_INTEGER: {
        jlong val = sqlite3_column_int64(pStmt, i);
        bOk = pEnv->CallBooleanMethod(win, aMethod[CW_PUTLONG].id, val, iRow, i);
        break;
      }

      case SQLITE_FLOAT: {
        jdouble val = sqlite3_column_double(pStmt, i);
        bOk = pEnv->CallBooleanMethod(win, aMethod[CW_PUTDOUBLE].id, val, iRow, i);
        break;
      }

      case SQLITE_TEXT: {
        jchar *pStr = (jchar*)sqlite3_column_text16(pStmt, i);
        int nStr = sqlite3_column_bytes16(pStmt, i) / sizeof(jchar);
        ScopedLocalRef<jstring> val(pEnv, pEnv->NewString(pStr, nStr));
        bOk = pEnv->CallBooleanMethod(win, aMethod[CW_PUTSTRING].id, val.get(), iRow, i);
        break;
      }

      default: {
        assert( sqlite3_column_type(pStmt, i)==SQLITE_BLOB );
        const jbyte *p = (const jbyte*)sqlite3_column_blob(pStmt, i);
        int n = sqlite3_column_bytes(pStmt, i);
        ScopedLocalRef<jbyteArray> val(pEnv, pEnv->NewByteArray(n));
        pEnv->SetByteArrayRegion(val.get(), 0, n, p);
        bOk = pEnv->CallBooleanMethod(win, aMethod[CW_PUTBLOB].id, val.get(), iRow, i);
        break;
      }
    }

    if( bOk==0 ){
      pEnv->CallVoidMethod(win, aMethod[CW_FREELASTROW].id);
    }
  }

  return bOk;
}

jboolean setWindowNumColumns(
  JNIEnv *pEnv,
  jobject win,
  sqlite3_stmt *pStmt,
  CWMethod *aMethod
){
  int nCol;

  pEnv->CallVoidMethod(win, aMethod[CW_CLEAR].id);
  nCol = sqlite3_column_count(pStmt);
  return pEnv->CallBooleanMethod(win, aMethod[CW_SETNUMCOLUMNS].id, (jint)nCol);
}

} // namespace android
