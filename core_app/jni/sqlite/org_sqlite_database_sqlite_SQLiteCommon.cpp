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
#include <unistd.h>
#include <assert.h>
#include <pthread.h>

#include "MutexRegion.h"

#include "org_sqlite_database_sqlite_SQLiteCommon.h"
#include "org_sqlite_database_sqlite_SQLiteConnection.h"

namespace org_opendatakit {

// Limit heap to 8MB for now.  This is 4 times the maximum cursor window
// size, as has been used by the original code in SQLiteDatabase for
// a long time.
const int SOFT_HEAP_LIMIT = 8 * 1024 * 1024;

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

/*
** Note: The following symbols must be in the same order as the corresponding
** elements in the aMethod[] array in function executeIntoCursorWindow().
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

struct SQLiteConnection {

    sqlite3* db;
    const int openFlags;
    std::string path;
    std::string label;

    volatile bool canceled;

    SQLiteConnection(sqlite3* db, int openFlags, const std::string& path, const std::string& label) :
        db(db), openFlags(openFlags), path(path), label(label), canceled(false) { }
};

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

static pthread_mutex_t g_init_mutex = PTHREAD_MUTEX_INITIALIZER;

// this is initialized within the above mutex
static JavaVM *gpJavaVM = nullptr;

// Sets the global SQLite configuration.
// This must be called before any other SQLite functions are called.
void sqliteInitialize(JNIEnv* env) {
    pid_t tid = getpid();
    ALOGV("sqliteInitialize 0x%.8x -- entered", tid);

    MutexRegion guard(&g_init_mutex);
    ALOGV("sqliteInitialize 0x%.8x -- gained mutex", tid);

    if ( gpJavaVM == nullptr ) {
      ALOGV("sqliteInitialize 0x%.8x -- executing sqlite3_config statements", tid);

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

      // finally, get the VM pointer
      env->GetJavaVM(&gpJavaVM);
    }
    ALOGV("sqliteInitialize 0x%.8x -- done!", tid);
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
static void sqliteTraceCallback(void *data, const char *sql) {
    SQLiteConnection* connection = static_cast<SQLiteConnection*>(data);
    ALOG(LOG_VERBOSE, SQLITE_TRACE_TAG, "%s: \"%s\"\n",
            connection->label.c_str(), sql);
}

// Called each time a statement finishes execution, when profiling is enabled.
static void sqliteProfileCallback(void *data, const char *sql, sqlite3_uint64 tm) {
    SQLiteConnection* connection = static_cast<SQLiteConnection*>(data);
    ALOG(LOG_VERBOSE, SQLITE_PROFILE_TAG, "%s: \"%s\" took %0.3f ms\n",
            connection->label.c_str(), sql, tm * 0.000001f);
}

// Called after each SQLite VM instruction when cancelation is enabled.
static int sqliteProgressHandlerCallback(void* data) {
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
static int coll_localized(
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

SQLiteConnection* openConnection(JNIEnv* env,
          std::string path, jint openFlags, std::string label,
          jboolean enableTrace, jboolean enableProfile) {
              
    pid_t tid = getpid();
    int sqliteFlags;

    if (openFlags & org_sqlite_database_sqlite_SQLiteConnection_CREATE_IF_NECESSARY) {
        sqliteFlags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
    } else {
        sqliteFlags = SQLITE_OPEN_READWRITE;
    }

    sqlite3* db;
    int err = sqlite3_open_v2(path.c_str(), &db, sqliteFlags, nullptr);
    if (err != SQLITE_OK) {
        ALOGV("openConnection 0x%.8x -- failed sqlite3_open_v2", tid);
        throw_sqlite3_exception_errcode(env, err, "Could not open database");
        return 0L;
    }
    err = sqlite3_create_collation(db, "localized", SQLITE_UTF8, nullptr,
            coll_localized);
    if (err != SQLITE_OK) {
        ALOGV("openConnection 0x%.8x -- failed sqlite3_create_collation", tid);
        throw_sqlite3_exception_errcode(env, err, "Could not register collation");
        sqlite3_close_v2(db);
        return 0L;
    }

    // Check that the database is really read/write when that is what we asked for.
    if ((sqliteFlags & SQLITE_OPEN_READWRITE) && sqlite3_db_readonly(db, nullptr)) {
        ALOGV("openConnection 0x%.8x -- failed sqlite3_db_readonly", tid);
        throw_sqlite3_exception_db(env, db, "Could not open the database in read/write mode.");
        sqlite3_close_v2(db);
        return 0L;
    }

    // Set the default busy handler to retry automatically before returning SQLITE_BUSY.
    err = sqlite3_busy_timeout(db, BUSY_TIMEOUT_MS);
    if (err != SQLITE_OK) {
        ALOGV("openConnection 0x%.8x -- failed sqlite3_busy_timeout", tid);
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
    ALOGV("openConnection 0x%.8x -- creating connection", tid);

    // Create wrapper object.
    SQLiteConnection* connection = new SQLiteConnection(db, openFlags, path, label);

    // Enable tracing and profiling if requested.
    if (enableTrace) {
        sqlite3_trace(db, &sqliteTraceCallback, connection);
    }
    if (enableProfile) {
        sqlite3_profile(db, &sqliteProfileCallback, connection);
    }

    return connection;
}

void closeConnection(JNIEnv *env, SQLiteConnection* connection) {
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

// Called each time a custom function is evaluated.
static void sqliteCustomFunctionCallback(sqlite3_context *context,
        int argc, sqlite3_value **argv) {

    JNIEnv* env = nullptr;
    gpJavaVM->GetEnv((void**)&env, JNI_VERSION_1_6);

    // Get the callback function object.
    // Create a new local reference to it in case the callback tries to do something
    // dumb like unregister the function (thereby destroying the global ref) while it is running.
    jobject functionObjGlobal = reinterpret_cast<jobject>(sqlite3_user_data(context));
    ScopedLocalRef<jobject> functionObj(env, env->NewLocalRef(functionObjGlobal));

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
static void sqliteCustomFunctionDestructor(void* data) {
    jobject functionObjGlobal = reinterpret_cast<jobject>(data);
    JNIEnv* env = nullptr;
    gpJavaVM->GetEnv((void**)&env, JNI_VERSION_1_6);
    env->DeleteGlobalRef(functionObjGlobal);
}

void registerCustomFunction(JNIEnv* env, SQLiteConnection* connection,
        const char * name, int numArgs, jobject functionObj) {

    // this is important -- the sqlite3_user_data() function will return functionObjGlobal
    // so it needs to be a global ref.
    jobject functionObjGlobal = env->NewGlobalRef(functionObj);

    // this will copy the name, so we don't have to worry about localref reloc of the buffer.
    // should an error occur, the destructor function will be called, which will have called:
    //    env->DeleteGlobalRef(functionObjGlobal);
    int err = sqlite3_create_function_v2(connection->db, name, numArgs, SQLITE_UTF16,
            reinterpret_cast<void*>(functionObjGlobal),
            &sqliteCustomFunctionCallback, nullptr, nullptr, &sqliteCustomFunctionDestructor);

    if (err != SQLITE_OK) {
        ALOGE("sqlite3_create_function returned %d", err);
        throw_sqlite3_exception_db(env, connection->db, "Error while registering custom function");
    }
}

sqlite3_stmt* prepareStatement(JNIEnv* env, SQLiteConnection* connection, jstring sqlString) {

    sqlite3_stmt* stmt = nullptr;

    jsize sqlLength = env->GetStringLength(sqlString);
    const jchar* sql = env->GetStringChars(sqlString, nullptr);

    int err = sqlite3_prepare16_v2(connection->db,
            sql, sqlLength * sizeof(jchar), &stmt, nullptr);

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

    ALOGV("Prepared statement %p on connection %p", stmt, connection->db);
    return stmt;
}

void finalizeStatement(JNIEnv* env, SQLiteConnection* connection, sqlite3_stmt* statement) {

    // We ignore the result of sqlite3_finalize because it is really telling us about
    // whether any errors occurred while executing the statement.  The statement itself
    // is always finalized regardless.
    ALOGV("Finalized statement %p on connection %p", statement, connection->db);
    sqlite3_finalize(statement);
}

jint bindParameterCount(JNIEnv* env, SQLiteConnection* connection, sqlite3_stmt* statement) {
    return sqlite3_bind_parameter_count(statement);
}

jboolean statementIsReadOnly(JNIEnv* env, SQLiteConnection* connection, sqlite3_stmt* statement) {
    return sqlite3_stmt_readonly(statement) != 0;
}

jint getColumnCount(JNIEnv* env, SQLiteConnection* connection, sqlite3_stmt* statement) {
    return sqlite3_column_count(statement);
}

jstring getColumnName(JNIEnv* env, SQLiteConnection* connection, sqlite3_stmt* statement, int index) {
    // sqlite3_column_name16 returns a null-terminated UTF-16 string.
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

void bindNull(JNIEnv* env, SQLiteConnection* connection, sqlite3_stmt* statement, int index) {
    int err = sqlite3_bind_null(statement, index);

    if (err != SQLITE_OK) {
        throw_sqlite3_exception_db(env, connection->db, "Error while binding null value");
    }
}

void bindLong(JNIEnv* env, SQLiteConnection* connection, sqlite3_stmt* statement, int index,
      jlong value) {
    int err = sqlite3_bind_int64(statement, index, value);

    if (err != SQLITE_OK) {
        throw_sqlite3_exception_db(env, connection->db, "Error while binding long value");
    }
}


void bindDouble(JNIEnv* env, SQLiteConnection* connection, sqlite3_stmt* statement, int index,
      jdouble value) {
    int err = sqlite3_bind_double(statement, index, value);

    if (err != SQLITE_OK) {
        throw_sqlite3_exception_db(env, connection->db, "Error while binding double value");
    }
}

void bindString(JNIEnv* env, SQLiteConnection* connection, sqlite3_stmt* statement, int index,
      const jchar* value, size_t valueLength) {
    int err = sqlite3_bind_text16(statement, index, value, valueLength * sizeof(jchar),
        SQLITE_TRANSIENT);

    if (err != SQLITE_OK) {
        throw_sqlite3_exception_db(env, connection->db, "Error while binding string value");
    }
}

void bindBlob(JNIEnv* env, SQLiteConnection* connection, sqlite3_stmt* statement, int index,
      const jbyte* value, size_t valueLength) {
    int err = sqlite3_bind_blob(statement, index, value, valueLength, SQLITE_TRANSIENT);

    if (err != SQLITE_OK) {
        throw_sqlite3_exception_db(env, connection->db, "Error while binding blob value");
    }
}

void resetAndClearBindings(JNIEnv* env, SQLiteConnection* connection, sqlite3_stmt* statement) {
    int err = sqlite3_reset(statement);
    if (err == SQLITE_OK) {
        err = sqlite3_clear_bindings(statement);
    }

    if (err != SQLITE_OK) {
        throw_sqlite3_exception_db(env, connection->db, "Error during resetAndClearBindings");
    }
}

jstring getColumnStringValue(JNIEnv* env, SQLiteConnection* connection, sqlite3_stmt* statement, int index) {
    // Strings returned by sqlite3_column_text16() are always null terminated.
    const jchar* text = static_cast<const jchar*>(sqlite3_column_text16(statement, index));
    if (text) {
        size_t length = 0;
        while (text[length]) {
            length += 1;
        }
        return env->NewString(text, length);
    }
    return nullptr;
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

jint executeForChangedRowCount(JNIEnv* env, SQLiteConnection* connection, sqlite3_stmt* statement) {

    int err = executeNonQuery(env, connection, statement);
    return err == SQLITE_DONE ? sqlite3_changes(connection->db) : -1;
}

jlong executeForLastInsertedRowId(JNIEnv* env, SQLiteConnection* connection, sqlite3_stmt* statement) {

    int err = executeNonQuery(env, connection, statement);
    return err == SQLITE_DONE && sqlite3_changes(connection->db) > 0
            ? sqlite3_last_insert_rowid(connection->db) : -1L;
}

int executeOneRowQuery(JNIEnv* env, SQLiteConnection* connection, sqlite3_stmt* statement) {
    int err = sqlite3_step(statement);
    if (err != SQLITE_ROW) {
        throw_sqlite3_exception_db_unspecified(env, connection->db);
    }
    return err;
}


jlong executeForLong(JNIEnv* env, SQLiteConnection* connection, sqlite3_stmt* statement) {
    int err = executeOneRowQuery(env, connection, statement);
    if (err == SQLITE_ROW && sqlite3_column_count(statement) >= 1) {
        return sqlite3_column_int64(statement, 0);
    }
    return -1L;
}

jstring executeForString(JNIEnv* env, SQLiteConnection* connection, sqlite3_stmt* statement) {
    int err = executeOneRowQuery(env, connection, statement);
    if (err == SQLITE_ROW && sqlite3_column_count(statement) >= 1) {
        return getColumnStringValue(env, connection, statement, 0);
    }
    return nullptr;
}

/*
** Append the contents of the row that SQL statement statement currently points to
** to the CursorWindow object passed as the second argument. The CursorWindow
** currently contains iRow rows. Return true on success or false if an error
** occurs.
*/
static jboolean copyRowToWindow(
  JNIEnv *env,
  jobject win,
  int iRow,
  sqlite3_stmt *statement,
  CWMethod *aMethod
){
  int nCol = sqlite3_column_count(statement);
  int i;
  jboolean bOk;

  bOk = env->CallBooleanMethod(win, aMethod[CW_ALLOCROW].id);
  for(i=0; bOk && i<nCol; i++){
    switch( sqlite3_column_type(statement, i) ){
      case SQLITE_NULL: {
        bOk = env->CallBooleanMethod(win, aMethod[CW_PUTNULL].id, iRow, i);
        break;
      }

      case SQLITE_INTEGER: {
        jlong val = sqlite3_column_int64(statement, i);
        bOk = env->CallBooleanMethod(win, aMethod[CW_PUTLONG].id, val, iRow, i);
        break;
      }

      case SQLITE_FLOAT: {
        jdouble val = sqlite3_column_double(statement, i);
        bOk = env->CallBooleanMethod(win, aMethod[CW_PUTDOUBLE].id, val, iRow, i);
        break;
      }

      case SQLITE_TEXT: {
        // Strings returned by sqlite3_column_text16() are always null terminated.
        jchar *pStr = (jchar*)sqlite3_column_text16(statement, i);
        if (pStr) {
            size_t nStr = 0;
            while (pStr[nStr]) {
                nStr += 1;
            }
            ScopedLocalRef<jstring> val(env, env->NewString(pStr, nStr));
            bOk = env->CallBooleanMethod(win, aMethod[CW_PUTSTRING].id, val.get(), iRow, i);
        } else {
          bOk = env->CallBooleanMethod(win, aMethod[CW_PUTNULL].id, iRow, i);
        }
        break;
      }

      default: {
        assert( sqlite3_column_type(statement, i)==SQLITE_BLOB );
        const jbyte *p = (const jbyte*)sqlite3_column_blob(statement, i);
        if (p) {
          int n = sqlite3_column_bytes(statement, i);
          ScopedLocalRef<jbyteArray> val(env, env->NewByteArray(n));
          env->SetByteArrayRegion(val.get(), 0, n, p);
          bOk = env->CallBooleanMethod(win, aMethod[CW_PUTBLOB].id, val.get(), iRow, i);
        } else {
          bOk = env->CallBooleanMethod(win, aMethod[CW_PUTNULL].id, iRow, i);
        }
        break;
      }
    }

    if( bOk==0 ){
      env->CallVoidMethod(win, aMethod[CW_FREELASTROW].id);
    }
  }

  return bOk;
}

static jboolean setWindowNumColumns(
  JNIEnv *env,
  jobject win,
  sqlite3_stmt *statement,
  CWMethod *aMethod
){
  int nCol;

  env->CallVoidMethod(win, aMethod[CW_CLEAR].id);
  nCol = sqlite3_column_count(statement);
  return env->CallBooleanMethod(win, aMethod[CW_SETNUMCOLUMNS].id, (jint)nCol);
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
jlong executeIntoCursorWindow(JNIEnv* env, SQLiteConnection* connection, sqlite3_stmt* statement,
                          jobject win,
                          jint startPos,                  /* First row to add (advisory) */
                          jint iRowRequired,              /* Required row */
                          jboolean countAllRows) {

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

  /* Class android.database.CursorWindow */
  ScopedLocalRef<jclass> cls(env, env->FindClass("android/database/CursorWindow"));
  /* Locate all required CursorWindow methods. */
  for(int i=0; i<(sizeof(aMethod)/sizeof(struct CWMethod)); i++){
    aMethod[i].id = env->GetMethodID(cls.get(), aMethod[i].zName, aMethod[i].zSig);
    if( aMethod[i].id==nullptr ){
      jniThrowExceptionFmt(env, "java/lang/Exception",
          "Failed to find method CursorWindow.%s()", aMethod[i].zName
      );
      return 0L;
    }
  }

  /* Set the number of columns in the window */
  jboolean bOk = setWindowNumColumns(env, win, statement, aMethod);
  if( bOk==0 ) {
    return 0L;
  }

  int nRow = 0;
  int iStart = startPos;
  while( sqlite3_step(statement)==SQLITE_ROW ){
    /* Only copy in rows that occur at or after row index iStart. */
    if( nRow>=iStart && bOk ){
      bOk = copyRowToWindow(env, win, (nRow - iStart), statement, aMethod);
      if( bOk==0 ){
        /* The CursorWindow object ran out of memory. If row iRowRequired was
        ** not successfully added before this happened, clear the CursorWindow
        ** and try to add the current row again.  */
        if( nRow<=iRowRequired ){
          bOk = setWindowNumColumns(env, win, statement, aMethod);
          if( bOk==0 ){
            sqlite3_reset(statement);
            return 0L;
          }
          iStart = nRow;
          bOk = copyRowToWindow(env, win, (nRow - iStart), statement, aMethod);
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
  int rc = sqlite3_reset(statement);
  if( rc!=SQLITE_OK ){
    throw_sqlite3_exception_db_unspecified(env, connection->db);
    return 0L;
  }

  jlong lRet = jlong(iStart) << 32 | jlong(nRow);
  return lRet;
}

jint getDbLookasideUsed(JNIEnv* env, SQLiteConnection* connection) {
    int cur = -1;
    int unused;
    sqlite3_db_status(connection->db, SQLITE_DBSTATUS_LOOKASIDE_USED, &cur, &unused, 0);
    return cur;
}

void cancel(JNIEnv* env, SQLiteConnection* connection) {
    connection->canceled = true;
}

void resetCancel(JNIEnv* env, SQLiteConnection* connection, jboolean cancelable) {
    connection->canceled = false;

    if (cancelable) {
        sqlite3_progress_handler(connection->db, 4, sqliteProgressHandlerCallback,
                connection);
    } else {
        sqlite3_progress_handler(connection->db, 0, nullptr, nullptr);
    }
}

jboolean hasCodec(JNIEnv* env) {
#ifdef SQLITE_HAS_CODEC
  return true;
#else
  return false;
#endif
}

// Called by SQLiteGlobal
jint releaseMemory() {
    return sqlite3_release_memory(org_opendatakit::SOFT_HEAP_LIMIT);
}

// Called by SQLiteDebug
void getStatus(JNIEnv* env, jint* memoryUsed, jint* largestMemAlloc, jint *pageCacheOverflow) {
    int imemoryUsed;
    int ipageCacheOverflow;
    int ilargestMemAlloc;
    int iunused;

    sqlite3_status(SQLITE_STATUS_MEMORY_USED, &imemoryUsed, &iunused, 0);
    sqlite3_status(SQLITE_STATUS_MALLOC_SIZE, &iunused, &ilargestMemAlloc, 0);
    sqlite3_status(SQLITE_STATUS_PAGECACHE_OVERFLOW, &ipageCacheOverflow, &iunused, 0);

    *memoryUsed = imemoryUsed;
    *pageCacheOverflow = ipageCacheOverflow;
    *largestMemAlloc = ilargestMemAlloc;
}

} // namespace android
